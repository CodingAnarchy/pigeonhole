//! Near-duplicate dedup (D311 §4, T3): before `remember`'s transaction, the nearest live
//! memory in the namespace, and what to do when it's close enough.

use crate::error::{Error, Result};
use crate::layout::*;
use crate::store::{Memory, Store};

/// What [`Store::remember`] does with a memory whose embedding is near a live one's
/// ([`NearDedup`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum DedupPolicy {
    /// Writes nothing, returning [`Remembered::Similar`](crate::Remembered::Similar) with
    /// the match.
    Skip,
    /// Merges into the match as an exact duplicate would: the larger importance, the
    /// source, and a use. A match that's gone or superseded by the time the transaction
    /// reads it is ignored, and the memory is written as new.
    Merge,
    /// Writes the memory and links it to the match with
    /// [`LinkType::SIMILAR_TO`](crate::LinkType::SIMILAR_TO), weighted by their similarity.
    #[default]
    Link,
}

impl DedupPolicy {
    fn code(self) -> u8 {
        match self {
            DedupPolicy::Skip => 0,
            DedupPolicy::Merge => 1,
            DedupPolicy::Link => 2,
        }
    }

    fn from_code(c: u8) -> Option<Self> {
        Some(match c {
            0 => DedupPolicy::Skip,
            1 => DedupPolicy::Merge,
            2 => DedupPolicy::Link,
            _ => return None,
        })
    }
}

/// A store's near-duplicate dedup ([`StoreOptions::near_dedup`](crate::StoreOptions::near_dedup),
/// [`Store::set_near_dedup`]): a memory whose embedding is at least `similarity` (cosine)
/// to a live, unsuperseded memory of its namespace is handled by the policy.
///
/// The check runs before [`Store::remember`]'s transaction, so two near duplicates written
/// at once can both be inserted (S2): recall can return both until a consolidation folds
/// them. [`Store::remember_in`] doesn't check, since its caller's transaction is already
/// open and the check couldn't be part of it: call [`Store::near_duplicate`] before it.
/// An exact duplicate merges as always, whatever this says.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct NearDedup {
    similarity: f32,
    policy: DedupPolicy,
}

impl NearDedup {
    /// Near duplicates at `similarity` (cosine, above 0 and up to 1) or above, linked
    /// ([`DedupPolicy::Link`]). 0 or below is refused: almost every write would be a near
    /// duplicate.
    pub fn new(similarity: f32) -> Self {
        NearDedup {
            similarity,
            policy: DedupPolicy::Link,
        }
    }

    /// What to do with a near duplicate: [`DedupPolicy::Link`] by default.
    pub fn policy(mut self, p: DedupPolicy) -> Self {
        self.policy = p;
        self
    }

    /// The similarity a match needs.
    pub fn get_similarity(&self) -> f32 {
        self.similarity
    }

    /// What's done with a near duplicate.
    pub fn get_policy(&self) -> DedupPolicy {
        self.policy
    }

    pub(crate) fn check(&self) -> Result<()> {
        if !(self.similarity > 0.0 && self.similarity <= 1.0) {
            return Err(Error::InvalidArgument(
                "a near dedup similarity is above 0 and at most 1".into(),
            ));
        }
        Ok(())
    }

    /// As `G` stores it: a version byte, the similarity (f32, little endian), the policy.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut b = vec![1u8];
        b.extend_from_slice(&self.similarity.to_le_bytes());
        b.push(self.policy.code());
        b
    }

    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        match b {
            [1, s0, s1, s2, s3, p] => Ok(NearDedup {
                similarity: f32::from_le_bytes([*s0, *s1, *s2, *s3]),
                policy: DedupPolicy::from_code(*p)
                    .ok_or_else(|| crate::error::corrupt("a near dedup policy"))?,
            }),
            _ => Err(crate::error::corrupt("the near dedup setting")),
        }
    }
}

/// A near match a `remember` found: the memory, its similarity, and the policy.
#[derive(Debug, Clone)]
pub(crate) struct Near {
    pub(crate) row: Vec<u8>,
    pub(crate) similarity: f32,
    pub(crate) policy: DedupPolicy,
}

impl Store {
    /// The store's near-duplicate dedup, if it has one.
    pub fn near_dedup(&self) -> Result<Option<NearDedup>> {
        match self.table().get(GLOBAL, META, NEAR_DEDUP)? {
            Some(c) => Ok(Some(NearDedup::decode(c.value())?)),
            None => Ok(None),
        }
    }

    /// Sets the store's near-duplicate dedup (`None` turns it off). `InvalidArgument` in a
    /// store without an embedding model.
    pub fn set_near_dedup(&self, near: Option<NearDedup>) -> Result<()> {
        let t = self.table();
        match near {
            Some(n) => {
                n.check()?;
                if self.embedding_model().is_none() {
                    return Err(Error::InvalidArgument(
                        "near dedup compares embeddings, and the store has no model".into(),
                    ));
                }
                t.mutate(GLOBAL)
                    .put(META, NEAR_DEDUP, &n.encode())
                    .commit()?;
            }
            None => {
                t.mutate(GLOBAL).delete_column(META, NEAR_DEDUP).commit()?;
            }
        }
        Ok(())
    }

    /// The live, unsuperseded memory of `ns` nearest to `memory`'s embedding, with its
    /// similarity, if it reaches the store's near dedup similarity: what
    /// [`remember`](Self::remember) checks before its transaction, for a caller about to
    /// [`remember_in`](Self::remember_in). `None` without an embedding or a match;
    /// `InvalidArgument` if the store has no near dedup.
    pub fn near_duplicate(
        &self,
        ns: &str,
        memory: &Memory,
    ) -> Result<Option<(crate::MemoryId, f32)>> {
        let Some(setting) = self.near_dedup()? else {
            return Err(Error::InvalidArgument(
                "the store has no near dedup: set one with Store::set_near_dedup".into(),
            ));
        };
        Ok(self.near(ns, memory, setting)?.map(|n| {
            let id = crate::MemoryId::from_row(&n.row).expect("a memory row");
            (id, n.similarity)
        }))
    }

    /// The near match for `memory` under `setting`, read at a fresh snapshot.
    pub(crate) fn near(
        &self,
        ns: &str,
        memory: &Memory,
        setting: NearDedup,
    ) -> Result<Option<Near>> {
        check_namespace(ns)?;
        let (Some(e), Some(model)) = (memory.embedding.as_deref(), self.embedding_model()) else {
            return Ok(None);
        };
        let spec = model.spec();
        let t = self.table();
        let s = self.db().snapshot()?;
        let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
        // A few, so a superseded nearest doesn't hide a live one just behind it.
        let found = t
            .scan_prefix(&prefix)
            .family(EMB)
            .snapshot(&s)
            .nearest_f32(e, 8)
            .run()?;
        for n in found {
            let Some(row) = t
                .row(&n.row)
                .snapshot(&s)
                .families([CONTENT, META, EMB])
                .read()?
            else {
                continue;
            };
            if row.get(CONTENT, TEXT).is_none() || row.get(META, SUPERSEDED_BY).is_some() {
                continue;
            }
            let Some(cell) = row.get(EMB, V) else {
                continue;
            };
            let Some(v) = cell.as_vector() else {
                continue;
            };
            let similarity = (1.0 - spec.distance_f32(e, v)?) as f32;
            if similarity >= setting.similarity {
                return Ok(Some(Near {
                    row: n.row,
                    similarity,
                    policy: setting.policy,
                }));
            }
            // Nearest first: the rest are farther.
            break;
        }
        Ok(None)
    }
}
