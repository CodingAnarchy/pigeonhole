//! The server's caches of open handles by name (vector and text indexes), with the
//! generation that keeps a stale open out of them (#1632).
//!
//! A handle is opened outside the cache's lock (a cached lookup never waits behind another
//! request's I/O), then kept. A catalog change or a re-bootstrap swap forgets every handle in
//! between, and the open may have read the catalog, or the engine, from before it: kept, it
//! would be served until the next forget. So [`Handles::forget`] bumps a generation, an
//! opener notes the generation before it opens, and keeps its handle only if the generation
//! is unchanged under the same lock; otherwise it opens again (cli692's D1 on #1618, the same
//! rule as the memory slot's).

use std::collections::HashMap;
use std::sync::Arc;

/// Open handles by name, and the generation [`forget`](Self::forget) bumps.
#[derive(Debug)]
pub(crate) struct Handles<V> {
    generation: u64,
    map: HashMap<String, Arc<V>>,
}

impl<V> Default for Handles<V> {
    fn default() -> Self {
        Self {
            generation: 0,
            map: HashMap::new(),
        }
    }
}

impl<V> Handles<V> {
    /// The handle kept for `name`.
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<V>> {
        self.map.get(name)
    }

    /// The generation an open starts at: keep its handle only while it's still this one.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether a handle opened at `generation` may be kept: no forget since.
    pub(crate) fn current(&self, generation: u64) -> bool {
        self.generation == generation
    }

    /// Keeps `handle`, opened at `generation`, under `name`, unless a handle is kept there
    /// already (then that one is returned). `None`, keeping nothing, if a forget came since
    /// `generation`. The only way in: nothing is kept past the generation check (cli692's
    /// N1 on #1647).
    pub(crate) fn keep(&mut self, generation: u64, name: &str, handle: Arc<V>) -> Option<Arc<V>> {
        if !self.current(generation) {
            return None;
        }
        Some(Arc::clone(
            self.map.entry(name.to_owned()).or_insert(handle),
        ))
    }

    /// Drops every handle, and makes every open that started before this one stale.
    pub(crate) fn forget(&mut self) {
        self.generation += 1;
        self.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::Handles;

    #[test]
    fn an_open_overtaken_by_a_forget_is_not_current() {
        let mut h = Handles::<u32>::default();
        let started = h.generation();
        assert!(h.current(started));
        h.forget();
        assert!(
            !h.current(started),
            "a forget during the open makes it stale"
        );
        assert!(
            h.current(h.generation()),
            "an open started after it is current"
        );
        // Kept only at the current generation; the first kept wins.
        assert!(h.keep(started, "a", std::sync::Arc::new(1)).is_none());
        assert!(h.get("a").is_none());
        let now = h.generation();
        assert_eq!(
            h.keep(now, "a", std::sync::Arc::new(2)).as_deref(),
            Some(&2)
        );
        assert_eq!(
            h.keep(now, "a", std::sync::Arc::new(3)).as_deref(),
            Some(&2)
        );
    }
}
