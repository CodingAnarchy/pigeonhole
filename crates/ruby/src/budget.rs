//! D308's byte-budgeted reads (#1308): a scan's or a multi-get's page within a byte budget,
//! and the positions to resume them from. A page comes back as `[rows, bytes,
//! cut_by_budget, next_bytes, resume]`, which `lib/pigeonhole/api.rb` turns into a frozen
//! `ScanPage` or `KeysPage`.

use std::ops::Bound as KeyBound;

use magnus::{Error, IntoValue, RArray, RString, Ruby, TryConvert, Value, prelude::*};
use pigeonhole::KeySet;

use crate::classes::{KeysPosition, Row, ScanPosition, Snapshot, Table};
use crate::convert::{self, Bytes, arg_error, binary};
use crate::error::{OrRb, engine};
use crate::read::snapshot_arg;
use crate::table::either;
use crate::walk::byte_list;

/// A position class's methods: its bytes, `from_bytes`, its key, equality and hashing.
macro_rules! position {
    ($name:ident, $inner:ty, $key:ident) => {
        impl $name {
            /// The position as bytes: versioned and stable, so a client can hand them back
            /// later.
            pub(crate) fn to_bytes(ruby: &Ruby, rb_self: &Self) -> RString {
                binary(ruby, &rb_self.0.to_bytes())
            }

            /// The position `to_bytes` wrote; anything else (another kind of position
            /// included) raises `InvalidArgument`.
            pub(crate) fn from_bytes(ruby: &Ruby, b: Bytes) -> Result<Self, Error> {
                <$inner>::from_bytes(&b.0).or_rb(ruby).map(Self)
            }

            /// The row (or key) the position is in, at or after.
            pub(crate) fn $key(ruby: &Ruby, rb_self: &Self) -> RString {
                binary(ruby, rb_self.0.$key())
            }

            /// The same position.
            pub(crate) fn eq(&self, other: Value) -> bool {
                <&$name>::try_convert(other).is_ok_and(|o| o.0 == self.0)
            }

            /// A hash consistent with `==`.
            pub(crate) fn hash(&self) -> i64 {
                use std::hash::{Hash, Hasher};
                let mut h = std::hash::DefaultHasher::new();
                self.0.hash(&mut h);
                h.finish().cast_signed()
            }
        }
    };
}

position!(ScanPosition, pigeonhole::ScanPosition, row);
position!(KeysPosition, pigeonhole::KeysPosition, key);

/// A page's fields as an Array: `[rows, bytes, cut_by_budget, next_bytes, resume]`.
fn page(
    ruby: &Ruby,
    rows: Vec<pigeonhole::Row>,
    bytes: u64,
    cut: bool,
    next: Option<u64>,
    resume: Value,
) -> Result<RArray, Error> {
    let out = ruby.ary_new_capa(5);
    out.push(ruby.ary_from_iter(rows.into_iter().map(Row)))?;
    out.push(bytes)?;
    out.push(cut)?;
    out.push(next)?;
    out.push(resume)?;
    Ok(out)
}

impl Table {
    /// `scan_within(bytes, prefix, start, end, families, qualifier_prefix, versions, limit,
    /// column_limit, snapshot, after)`: a scan's page of at most `bytes` (D308), resumed
    /// `after` a position.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_within(
        ruby: &Ruby,
        rb_self: &Self,
        bytes: u64,
        prefix: Option<Bytes>,
        start: Option<Bytes>,
        end: Option<Bytes>,
        families: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        versions: u32,
        limit: Option<u64>,
        column_limit: Option<u32>,
        snapshot: Option<&Snapshot>,
        after: Option<&ScanPosition>,
    ) -> Result<RArray, Error> {
        let (prefix, start, end) = (
            convert::opt_bytes(prefix),
            convert::opt_bytes(start),
            convert::opt_bytes(end),
        );
        if prefix.is_some() && (start.is_some() || end.is_some()) {
            return Err(arg_error(
                ruby,
                "scan_within takes a prefix or start:/end:, not both",
            ));
        }
        let families = convert::names(families)?;
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        let after = after.map(|a| a.0.clone());
        let h = rb_self.handle(ruby)?.clone();
        let p = engine(ruby, move || {
            either!(&h, |t| {
                let mut s = match &prefix {
                    Some(p) => t.scan_prefix(p),
                    None => t.scan_bounds(
                        start
                            .as_deref()
                            .map_or(KeyBound::Unbounded, KeyBound::Included),
                        end.as_deref()
                            .map_or(KeyBound::Unbounded, KeyBound::Excluded),
                    ),
                }
                .versions(versions);
                if let Some(f) = &families {
                    s = s.families(f.iter().map(String::as_str));
                }
                if let Some(q) = &qp {
                    s = s.qualifier_prefix(q);
                }
                if let Some(n) = limit {
                    s = s.limit(n);
                }
                if let Some(n) = column_limit {
                    s = s.column_limit(n);
                }
                if let Some(sn) = &snap {
                    s = s.snapshot(sn);
                }
                if let Some(a) = &after {
                    s = s.resume_after(a);
                }
                s.read_within(bytes)
            })
        })?;
        let (bytes, cut, next) = (p.bytes(), p.cut_by_budget(), p.next_bytes());
        let resume = p.resume().cloned().map_or_else(
            || ruby.qnil().as_value(),
            |r| ScanPosition(r).into_value_with(ruby),
        );
        page(ruby, p.into_rows(), bytes, cut, next, resume)
    }

    /// `multi_get_within(keys, bytes, families, qualifier_prefix, versions, column_limit,
    /// snapshot, after)`: a page of `multi_get`'s cells in key order, at most `bytes` long
    /// (D308), resumed `after` a position.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn multi_get_within(
        ruby: &Ruby,
        rb_self: &Self,
        keys: RArray,
        bytes: u64,
        families: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Snapshot>,
        after: Option<&KeysPosition>,
    ) -> Result<RArray, Error> {
        let given = byte_list(keys)?;
        let families = convert::names(families)?;
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        let after = after.map(|a| a.0.clone());
        let h = rb_self.handle(ruby)?.clone();
        let p = engine(ruby, move || {
            let set = KeySet::from_unsorted(&given);
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
                if let Some(a) = &after {
                    r = r.resume_after(a);
                }
                r.read_within(bytes)
            })
        })?;
        let (bytes, cut, next) = (p.bytes(), p.cut_by_budget(), p.next_bytes());
        let resume = p.resume().cloned().map_or_else(
            || ruby.qnil().as_value(),
            |r| KeysPosition(r).into_value_with(ruby),
        );
        page(ruby, p.into_rows(), bytes, cut, next, resume)
    }
}
