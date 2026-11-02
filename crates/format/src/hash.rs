//! The workspace's one fast hasher for in-memory maps (#320): `foldhash`, seeded randomly
//! per process, behind [`FastMap`] and [`FastSet`].
//!
//! For maps keyed by ids and other engine-chosen values on hot paths, where std's SipHash
//! costs more than the lookup (a point get made four SipHash lookups, about 600
//! instructions). The seed keeps it out of reach of hash flooding, so it is also fine for
//! keys holding user bytes; an unseeded hasher must never see those.
//!
//! **Seeds** (#1273). Every map's hasher is `foldhash`'s [`SeedableRandomState`]: a seed of
//! its own (from its stack address and a per-thread counter) and the process's random shared
//! seed, exactly what `foldhash::fast::RandomState` uses. The callgrind measurements need one
//! instruction count per commit, so the bench alone can fix both seeds to [`FIXED_SEED`]
//! before the first map is built (`fix_seed_for_measurement`, behind the hidden
//! `__unstable-fixed-seed` feature). Either way the same code hashes; only the seed values
//! differ. Nothing else, and no environment variable, can choose the fixed seeds.
//!
//! ```
//! use pigeonhole_format::hash::FastMap;
//!
//! let mut m: FastMap<u32, &str> = FastMap::default();
//! m.insert(7, "seven");
//! assert_eq!(m.get(&7), Some(&"seven"));
//! ```

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::OnceLock;

use foldhash::SharedSeed;
use foldhash::fast::{FoldHasher, SeedableRandomState};

/// The seed every map gets when the bench fixes them (#1273): the ASCII bytes of
/// `"PIGEONHO"`, picked once and never tuned. A seed chosen for short probes would set the
/// instruction ceilings below what production sees, so changing it means re-deriving every
/// ceiling, in a D195 ceilings commit (CONTRIBUTING.md, "Fixed hash seeds").
pub const FIXED_SEED: u64 = u64::from_le_bytes(*b"PIGEONHO");

/// The shared seed of the fixed mode, from [`FIXED_SEED`].
static FIXED_SHARED: SharedSeed = SharedSeed::from_u64(FIXED_SEED);

/// How this process seeds its maps, chosen once: by the first map built, or before it by
/// `fix_seed_for_measurement`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Random,
    Fixed,
}

static MODE: OnceLock<Mode> = OnceLock::new();

/// The hasher factory: `foldhash`'s fast hasher, with a per-map seed and the process's random
/// shared seed (or both fixed, in the bench's measurements: see the module notes).
#[derive(Debug, Clone)]
pub struct FastBuildHasher(SeedableRandomState);

impl Default for FastBuildHasher {
    #[inline]
    fn default() -> Self {
        Self(match MODE.get_or_init(|| Mode::Random) {
            Mode::Random => SeedableRandomState::random(),
            Mode::Fixed => SeedableRandomState::with_seed(FIXED_SEED, &FIXED_SHARED),
        })
    }
}

impl BuildHasher for FastBuildHasher {
    type Hasher = FoldHasher<'static>;

    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        self.0.build_hasher()
    }
}

/// `fix_seed_for_measurement` came after this process had already chosen its seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedAlreadyChosen;

impl std::fmt::Display for SeedAlreadyChosen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a hash map was built before the seed was fixed")
    }
}

impl std::error::Error for SeedAlreadyChosen {}

/// Fixes every map's seeds to [`FIXED_SEED`] for the rest of the process, so a callgrind
/// run counts the same instructions on every run of one commit (#1273). Only the bench's
/// measured examples call it, first thing. It fails if a map was built first, since this
/// process's maps would then mix seeds, and it never weakens a build that doesn't call it.
///
/// Not part of the API and outside the semver promise; never enable the feature in an
/// application: fixed seeds give up the flooding resistance the random ones give.
#[cfg(feature = "__unstable-fixed-seed")]
#[doc(hidden)]
pub fn fix_seed_for_measurement() -> Result<(), SeedAlreadyChosen> {
    MODE.set(Mode::Fixed).map_err(|_| SeedAlreadyChosen)
}

/// A `HashMap` with the [`FastBuildHasher`]. Build it with `FastMap::default()` (or
/// `with_capacity_and_hasher`): `HashMap::new` is only for std's hasher.
pub type FastMap<K, V> = HashMap<K, V, FastBuildHasher>;

/// A `HashSet` with the [`FastBuildHasher`].
pub type FastSet<T> = HashSet<T, FastBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_are_seeded_randomly_unless_the_seed_was_fixed_first() {
        // Two maps on one thread hash a key differently: each has a seed of its own (equal
        // by chance with probability 2^-64).
        let (a, b) = (FastBuildHasher::default(), FastBuildHasher::default());
        assert_ne!(a.hash_one(42u64), b.hash_one(42u64));
        assert_eq!(MODE.get(), Some(&Mode::Random));
        #[cfg(feature = "__unstable-fixed-seed")]
        assert_eq!(fix_seed_for_measurement(), Err(SeedAlreadyChosen));
    }
}
