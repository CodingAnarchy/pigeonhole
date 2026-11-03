//! The 1.1 reads: multi-get (D299), traversal (D304), walking a wide row in pages (D297) and
//! nearest-neighbour search (D296), with D308's byte budget on nearest.
//!
//! Results with no engine object behind them (a traversal, its hops, a nearest search's
//! neighbours and page) come back as plain Arrays, which `lib/pigeonhole/api.rb` turns into
//! frozen `Data` objects: no wrapped struct here holds a Ruby value.

use std::collections::HashMap;
use std::ops::Bound as KeyBound;
use std::sync::Mutex;

use magnus::{Error, IntoValue, RArray, Ruby, TryConvert, Value, prelude::*};
use pigeonhole::KeySet;

use crate::classes::{Pages, PagesState, Row, RowPosition, Snapshot, Table};
use crate::convert::{self, Bytes, Name, arg_error, binary, lock};
use crate::error::{OrRb, engine};
use crate::fork::ForkSafe;
use crate::read::snapshot_arg;
use crate::table::either;
use crate::vector::{self, Vector};

/// An Array of byte Strings (`keys`, `seeds`).
pub(crate) fn byte_list(a: RArray) -> Result<Vec<Vec<u8>>, Error> {
    a.into_iter()
        .map(|v| Bytes::try_convert(v).map(|b| b.0))
        .collect()
}

/// Binary Strings for `keys`.
fn strings(ruby: &Ruby, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) -> RArray {
    ruby.ary_from_iter(keys.into_iter().map(|k| binary(ruby, k.as_ref())))
}

impl Table {
    /// `multi_get(keys, families, qualifier_prefix, versions, column_limit, snapshot)`: the
    /// rows of `keys`, aligned with `keys` (one entry per key given, in that order, `nil`
    /// for a key whose row has no cell the read keeps, a repeated key repeated). The read
    /// itself is one multi-get in key order.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn multi_get(
        ruby: &Ruby,
        rb_self: &Self,
        keys: RArray,
        families: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Snapshot>,
    ) -> Result<RArray, Error> {
        let given = byte_list(keys)?;
        let families = convert::names(families)?;
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        let h = rb_self.handle(ruby)?.clone();
        let rows = engine(ruby, move || {
            let set = KeySet::from_unsorted(&given);
            let mut found: HashMap<Vec<u8>, pigeonhole::Row> = HashMap::new();
            either!(&h, |t| {
                let mut r = t.keys(&set).versions(versions);
                if let Some(f) = &families {
                    r = r.families(f.iter().map(String::as_str));
                }
                if let Some(q) = &qp {
                    r = r.qualifier_prefix(q);
                }
                if let Some(n) = column_limit {
                    r = r.column_limit(n);
                }
                if let Some(s) = &snap {
                    r = r.snapshot(s);
                }
                let mut rows = r.read()?;
                while let Some((_, row)) = rows.next()? {
                    found.insert(row.key().to_vec(), row.to_owned());
                }
                Ok::<_, pigeonhole::Error>(())
            })?;
            Ok(given
                .iter()
                .map(|k| found.get(k).cloned())
                .collect::<Vec<_>>())
        })?;
        let out = ruby.ary_new_capa(rows.len());
        for r in rows {
            out.push(r.map(Row))?;
        }
        Ok(out)
    }

    /// `traverse(seeds, family, hops, per_node_limit, hop_limit, max_nodes, byte_budget,
    /// parents, qualifier_prefix, snapshot, parallel)`: a breadth-first traversal (D304),
    /// as `[seeds, [[nodes, parents], ...], rows, edges, hops_cut, cut_by_max_nodes,
    /// cut_by_budget]`. `seeds` are sorted and deduplicated: hop 1's parents index them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn traverse(
        ruby: &Ruby,
        rb_self: &Self,
        seeds: RArray,
        family: Name,
        hops: u32,
        per_node_limit: Option<u32>,
        hop_limit: Option<u64>,
        max_nodes: Option<u64>,
        byte_budget: Option<u64>,
        parents: bool,
        qualifier_prefix: Option<Bytes>,
        snapshot: Option<&Snapshot>,
        parallel: Option<usize>,
    ) -> Result<RArray, Error> {
        let seeds = KeySet::from_unsorted(byte_list(seeds)?);
        if byte_budget == Some(0) {
            return Err(arg_error(
                ruby,
                "byte_budget: must be at least 1 (nil sets no budget)",
            ));
        }
        let byte_budget = byte_budget.unwrap_or(0);
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        let h = rb_self.handle(ruby)?.clone();
        let family = family.0;
        let (sorted, t) = engine(ruby, move || {
            let t = either!(&h, |t| {
                let mut r = t
                    .traverse(&seeds)
                    .family(&family)
                    .hops(hops)
                    .byte_budget(byte_budget)
                    .parents(parents);
                if let Some(n) = per_node_limit {
                    r = r.per_node_limit(n);
                }
                if let Some(n) = hop_limit {
                    r = r.hop_limit(n);
                }
                if let Some(n) = max_nodes {
                    r = r.max_nodes(n);
                }
                if let Some(q) = &qp {
                    r = r.qualifier_prefix(q);
                }
                if let Some(s) = &snap {
                    r = r.snapshot(s);
                }
                if let Some(n) = parallel {
                    r = r.parallel(n);
                }
                r.run()
            })?;
            Ok((seeds.iter().map(<[u8]>::to_vec).collect::<Vec<_>>(), t))
        })?;
        let stats = t.stats();
        let out_hops = ruby.ary_new_capa(t.hops().len());
        for hop in t.hops() {
            let parents = hop.parents().map_or_else(
                || ruby.qnil().as_value(),
                |p| p.to_vec().into_value_with(ruby),
            );
            out_hops.push((strings(ruby, hop.nodes().iter()), parents))?;
        }
        let out = ruby.ary_new_capa(7);
        out.push(strings(ruby, &sorted))?;
        out.push(out_hops)?;
        out.push(stats.rows)?;
        out.push(stats.edges)?;
        out.push(stats.hops_cut)?;
        out.push(stats.cut_by_max_nodes)?;
        out.push(stats.cut_by_budget)?;
        Ok(out)
    }

    /// `pages(row, size, families, qualifier_prefix, versions, snapshot, after)`: a walk of
    /// `row`'s pages; nothing is read until the first page is asked for.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pages(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        size: usize,
        families: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        versions: u32,
        snapshot: Option<&Snapshot>,
        after: Option<&RowPosition>,
    ) -> Result<Pages, Error> {
        if size == 0 {
            return Err(arg_error(ruby, "size must be at least 1"));
        }
        Ok(Pages(ForkSafe::new(Mutex::new(PagesState {
            table: rb_self.handle(ruby)?.clone(),
            row: row.0,
            size,
            families: convert::names(families)?,
            qualifier_prefix: convert::opt_bytes(qualifier_prefix),
            versions,
            snapshot: snapshot_arg(ruby, snapshot)?,
            position: after.map(|p| p.0.clone()),
            done: false,
        }))))
    }

    /// `nearest(family, query, k, prefix, start, end, keys, qualifier_prefix, snapshot,
    /// parallel, within)`: the `k` cells of vector family `family` nearest `query`, as
    /// `[row, qualifier, distance, ts]` Arrays in (distance, row, qualifier) order; with
    /// `within` (D308), `[neighbours, bytes, next_bytes]`: the nearest that fit that many
    /// bytes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn nearest(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        query: Value,
        k: usize,
        prefix: Option<Bytes>,
        start: Option<Bytes>,
        end: Option<Bytes>,
        keys: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        snapshot: Option<&Snapshot>,
        parallel: Option<usize>,
        within: Option<u64>,
    ) -> Result<Value, Error> {
        let h = rb_self.handle(ruby)?.clone();
        let family = family.0;
        let spec = vector::family_spec(ruby, &h, &family, "nearest searches")?;
        let query = vector::vector(ruby, query, spec.element())?;
        let (prefix, start, end) = (
            convert::opt_bytes(prefix),
            convert::opt_bytes(start),
            convert::opt_bytes(end),
        );
        let ranged = prefix.is_some() || start.is_some() || end.is_some();
        if prefix.is_some() && (start.is_some() || end.is_some()) {
            return Err(arg_error(
                ruby,
                "nearest takes a prefix or start:/end:, not both",
            ));
        }
        let keys = keys.map(byte_list).transpose()?.map(KeySet::from_unsorted);
        if keys.is_some() && ranged {
            return Err(arg_error(ruby, "nearest takes keys: or a range, not both"));
        }
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        macro_rules! search {
            ($read:expr) => {{
                let r = $read;
                let mut n = match &query {
                    Vector::F32(q) => r.nearest_f32(q, k),
                    Vector::I8(q) => r.nearest_i8(q, k),
                    Vector::Bits(q) => r.nearest_bits(q, k),
                };
                if let Some(p) = parallel {
                    n = n.parallel(p);
                }
                match within {
                    Some(b) => n.run_within(b).map(|p| {
                        let (bytes, next) = (p.bytes(), p.next_bytes());
                        (p.into_neighbours(), Some((bytes, next)))
                    }),
                    None => n.run().map(|v| (v, None)),
                }
            }};
        }
        let (found, page) = engine(ruby, move || {
            either!(&h, |t| {
                if let Some(keys) = &keys {
                    let mut r = t.keys(keys).family(&family);
                    if let Some(q) = &qp {
                        r = r.qualifier_prefix(q);
                    }
                    if let Some(s) = &snap {
                        r = r.snapshot(s);
                    }
                    search!(r)
                } else {
                    let mut r = match &prefix {
                        Some(p) => t.scan_prefix(p),
                        None => t.scan_bounds(
                            start
                                .as_deref()
                                .map_or(KeyBound::Unbounded, KeyBound::Included),
                            end.as_deref()
                                .map_or(KeyBound::Unbounded, KeyBound::Excluded),
                        ),
                    }
                    .family(&family);
                    if let Some(q) = &qp {
                        r = r.qualifier_prefix(q);
                    }
                    if let Some(s) = &snap {
                        r = r.snapshot(s);
                    }
                    search!(r)
                }
            })
        })?;
        let neighbours = ruby.ary_new_capa(found.len());
        for n in found {
            neighbours.push((
                binary(ruby, &n.row),
                binary(ruby, &n.qualifier),
                n.distance,
                n.ts,
            ))?;
        }
        Ok(match page {
            None => neighbours.as_value(),
            Some((bytes, next)) => (neighbours, bytes, next).into_value_with(ruby),
        })
    }
}

impl Pages {
    /// The next page, or `nil` once the row is done. Each page is one read with the GVL
    /// released; none is held open between pages.
    pub(crate) fn next_page(ruby: &Ruby, rb_self: &Self) -> Result<Option<Row>, Error> {
        let state = rb_self.0.get(ruby)?;
        // The state's mutex is taken only inside the call, so a second thread waiting on it
        // never holds the GVL.
        let page = engine(ruby, || {
            let mut s = lock(state);
            if s.done {
                return Ok(None);
            }
            let page = either!(&s.table, |t| {
                let mut r = t.row(&s.row).versions(s.versions);
                if let Some(f) = &s.families {
                    r = r.families(f.iter().map(String::as_str));
                }
                if let Some(q) = &s.qualifier_prefix {
                    r = r.qualifier_prefix(q);
                }
                if let Some(sn) = &s.snapshot {
                    r = r.snapshot(sn);
                }
                if let Some(p) = &s.position {
                    r = r.resume_after(p);
                }
                let mut pages = r.pages(s.size)?;
                let page = pages.next()?.map(|p| p.to_owned());
                Ok::<_, pigeonhole::Error>(page.map(|p| (p, pages.position())))
            })?;
            Ok(match page {
                Some((p, position)) => {
                    s.position = position;
                    Some(p)
                }
                None => {
                    s.done = true;
                    None
                }
            })
        })?;
        Ok(page.map(Row))
    }

    /// Where the last page ended (`nil` before the first page, unless the walk started from
    /// one).
    pub(crate) fn position(ruby: &Ruby, rb_self: &Self) -> Result<Option<RowPosition>, Error> {
        let state = rb_self.0.get(ruby)?;
        let p = crate::gvl::without_gvl(ruby, || lock(state).position.clone())?;
        Ok(p.map(RowPosition))
    }
}

impl RowPosition {
    /// The position as bytes: stable, so a client can hand them back later.
    pub(crate) fn to_bytes(ruby: &Ruby, rb_self: &Self) -> magnus::RString {
        binary(ruby, &rb_self.0.to_bytes())
    }

    /// The position `to_bytes` wrote.
    pub(crate) fn from_bytes(ruby: &Ruby, b: Bytes) -> Result<Self, Error> {
        pigeonhole::RowPosition::from_bytes(&b.0)
            .or_rb(ruby)
            .map(Self)
    }

    /// The same position.
    pub(crate) fn eq(&self, other: Value) -> bool {
        <&RowPosition>::try_convert(other).is_ok_and(|o| o.0.to_bytes() == self.0.to_bytes())
    }

    /// A hash consistent with `==`.
    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        self.0.to_bytes().hash(&mut h);
        h.finish().cast_signed()
    }
}
