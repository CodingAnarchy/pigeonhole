//! The workspace's one fast hasher for in-memory maps (#320): `foldhash`, seeded randomly
//! per process, behind [`FastMap`] and [`FastSet`].
//!
//! For maps keyed by ids and other engine-chosen values on hot paths, where std's SipHash
//! costs more than the lookup (a point get made four SipHash lookups, about 600
//! instructions). The seed keeps it out of reach of hash flooding, so it is also fine for
//! keys holding user bytes; an unseeded hasher must never see those.
//!
//! ```
//! use pigeonhole_format::hash::FastMap;
//!
//! let mut m: FastMap<u32, &str> = FastMap::default();
//! m.insert(7, "seven");
//! assert_eq!(m.get(&7), Some(&"seven"));
//! ```

use std::collections::{HashMap, HashSet};

/// The hasher factory: `foldhash`'s fast hasher with a per-process random seed.
pub type FastBuildHasher = foldhash::fast::RandomState;

/// A `HashMap` with the [`FastBuildHasher`]. Build it with `FastMap::default()` (or
/// `with_capacity_and_hasher`): `HashMap::new` is only for std's hasher.
pub type FastMap<K, V> = HashMap<K, V, FastBuildHasher>;

/// A `HashSet` with the [`FastBuildHasher`].
pub type FastSet<T> = HashSet<T, FastBuildHasher>;
