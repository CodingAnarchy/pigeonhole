//! The change feed (D310, #1322): `Table#feed`, and `Feed`'s waits.
//!
//! A commit comes back as `[seqno, timestamp, [[table, family, kind, row, qualifier,
//! timestamp, value], ...]]`, which `lib/pigeonhole/api.rb` turns into frozen `Commit` and
//! `Change` objects.

use std::time::Duration;

use magnus::{Error, RArray, RString, Ruby, TryConvert, prelude::*};
use pigeonhole::ChangeKind;

use crate::classes::{Feed, Snapshot, Table};
use crate::convert::{Bytes, Secs, arg_error, binary, frozen};
use crate::error::engine;
use crate::fork::ForkSafe;
use crate::read::{snapshot_arg, typed};
use crate::table::{Handle, either};

/// A feed as a stream's source (`crate::waits`): its sync reads, the fiber wait's poll and
/// its token, shared with the memory store's watch.
pub(crate) struct FeedSource(pigeonhole::Feed);

impl crate::waits::Source for FeedSource {
    type Item = pigeonhole::Commit;
    type Error = pigeonhole::Error;
    const NAME: &'static str = "feed";

    fn next(&mut self, d: Duration) -> pigeonhole::Result<Option<pigeonhole::Commit>> {
        self.0.next(d).map(Option::<&pigeonhole::Commit>::cloned)
    }

    fn try_next(&mut self) -> pigeonhole::Result<Option<pigeonhole::Commit>> {
        self.0.try_next().map(Option::<&pigeonhole::Commit>::cloned)
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<pigeonhole::Result<pigeonhole::Commit>> {
        self.0.poll_next(cx).map(|r| r.cloned())
    }

    fn token(&self) -> Vec<u8> {
        self.0.token().as_bytes().to_vec()
    }

    fn oldest_retained(&self) -> Option<u64> {
        self.0.oldest_retained()
    }

    fn to_rb(ruby: &Ruby, item: pigeonhole::Commit) -> Result<magnus::Value, Error> {
        commit(ruby, &item).map(|a| a.as_value())
    }

    fn error(ruby: &Ruby, e: pigeonhole::Error) -> Error {
        crate::error::to_rb(ruby, &e)
    }
}

impl Table {
    /// `feed(tables, prefix, visible, from_snapshot, from_oldest, resume)`: subscribes to
    /// this table's committed changes (and `tables`'). A reader process's table feeds from
    /// the running writer's history (D310 PR 4, #1361).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn feed(
        ruby: &Ruby,
        rb_self: &Self,
        tables: Option<RArray>,
        prefix: Option<Bytes>,
        visible: bool,
        from_snapshot: Option<&Snapshot>,
        from_oldest: bool,
        resume: Option<Bytes>,
    ) -> Result<Feed, Error> {
        let starts = usize::from(from_snapshot.is_some())
            + usize::from(from_oldest)
            + usize::from(resume.is_some());
        if starts > 1 {
            return Err(arg_error(
                ruby,
                "one of from_snapshot:, from_oldest: and resume:, not several",
            ));
        }
        let mut others = Vec::new();
        for v in tables.into_iter().flatten() {
            others.push(<&Table>::try_convert(v)?.handle(ruby)?.clone());
        }
        let snap = snapshot_arg(ruby, from_snapshot)?;
        let h = rb_self.handle(ruby)?.clone();
        let feed = engine(ruby, move || {
            let mut b = either!(&h, |t| t.feed());
            for other in &others {
                b = match other {
                    Handle::Writer(t) => b.table(t),
                    Handle::Reader(t) => b.read_table(t),
                };
            }
            if let Some(p) = &prefix {
                b = b.prefix(&p.0);
            }
            if visible {
                b = b.visible();
            }
            if let Some(s) = &snap {
                b = b.from_snapshot(s);
            }
            if from_oldest {
                b = b.from_oldest();
            }
            if let Some(r) = resume {
                b = b.resume(&pigeonhole::FeedToken::from_bytes(r.0));
            }
            b.open()
        })?;
        Ok(Feed(ForkSafe::new(crate::waits::Waits::new(FeedSource(
            feed,
        )))))
    }
}

/// A change's kind as a Symbol's name.
fn kind_name(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Put => "put",
        ChangeKind::Merge => "merge",
        ChangeKind::DeleteCell => "delete_cell",
        ChangeKind::DeleteColumn => "delete_column",
        ChangeKind::DeleteFamily => "delete_family",
        // A kind this build doesn't name.
        _ => "unknown",
    }
}

/// A commit as `[seqno, timestamp, changes]`. Every String and Array inside a change is
/// frozen, as its `Data` is (ai1249's P1 on #1411): mutating one would change the change's
/// `==` and `hash`.
fn commit(ruby: &Ruby, c: &pigeonhole::Commit) -> Result<RArray, Error> {
    let name = |s: &str| {
        let s = ruby.str_new(s);
        s.freeze();
        s
    };
    let changes = ruby.ary_new_capa(c.changes().len());
    for ch in c.changes() {
        let a = ruby.ary_new_capa(7);
        a.push(name(ch.table()))?;
        a.push(name(ch.family()))?;
        a.push(ruby.to_symbol(kind_name(ch.kind())))?;
        a.push(frozen(ruby, ch.row()))?;
        a.push(frozen(ruby, ch.qualifier()))?;
        a.push(ch.timestamp())?;
        let value = ch
            .value()
            .map_or_else(|| ruby.qnil().as_value(), |v| typed(ruby, v));
        value.freeze();
        a.push(value)?;
        changes.push(a)?;
    }
    let out = ruby.ary_new_capa(3);
    out.push(c.seqno())?;
    out.push(c.timestamp())?;
    out.push(changes)?;
    Ok(out)
}

impl Feed {
    /// `native_next(timeout)`: the next commit, waiting up to `timeout` seconds (`nil`
    /// waits until one comes), in slices with an interrupt check between them; `nil` if
    /// none came in time.
    pub(crate) fn next(
        ruby: &Ruby,
        rb_self: &Self,
        timeout: Option<Secs>,
    ) -> Result<Option<RArray>, Error> {
        let state = rb_self.0.get(ruby)?;
        state
            .wait(ruby, timeout.map(|t| t.0))?
            .map(|c| commit(ruby, &c))
            .transpose()
    }

    /// `native_try_next`: the next commit if one is ready, else `nil`. Never waits.
    pub(crate) fn try_next(ruby: &Ruby, rb_self: &Self) -> Result<Option<RArray>, Error> {
        rb_self
            .0
            .get(ruby)?
            .try_next(ruby)?
            .map(|c| commit(ruby, &c))
            .transpose()
    }

    /// `native_next_op(bridge)`: the next commit as a fiber op (#1574 PR 2).
    #[cfg(unix)]
    pub(crate) fn next_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
    ) -> Result<crate::fiber::FiberOp, Error> {
        rb_self.0.get(ruby)?.next_op(ruby, bridge)
    }

    /// Where the feed is, as bytes: `Table#feed(resume:)` continues after the last commit
    /// delivered, in this open of the database or a later one.
    pub(crate) fn token(ruby: &Ruby, rb_self: &Self) -> Result<RString, Error> {
        let t = rb_self.0.get(ruby)?.token(ruby)?;
        Ok(binary(ruby, &t))
    }

    /// Once the feed expired: the oldest commit seqno its history still holds, when known.
    pub(crate) fn oldest_retained(ruby: &Ruby, rb_self: &Self) -> Result<Option<u64>, Error> {
        rb_self.0.get(ruby)?.oldest_retained(ruby)
    }

    /// Ends the subscription now. Calls afterwards raise `Closed`; closing twice does
    /// nothing.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        rb_self.0.get(ruby)?.close(ruby)
    }

    /// Whether `close` was called.
    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        rb_self.0.get(ruby)?.is_closed(ruby)
    }
}

/// The feed options of `Pigeonhole.open`: `change_feed` (the history's size in bytes),
/// `change_feed_off`, `feed_age` and `feed_tables`.
pub(crate) fn options(
    ruby: &Ruby,
    mut o: pigeonhole::Options,
    change_feed: Option<u64>,
    change_feed_off: bool,
    feed_age: Option<Secs>,
    feed_tables: Option<RArray>,
) -> Result<pigeonhole::Options, Error> {
    if change_feed_off {
        if change_feed.is_some() || feed_age.is_some() {
            return Err(arg_error(
                ruby,
                "change_feed_off: can't take change_feed: or feed_age:",
            ));
        }
        o = o.change_feed_off();
    }
    match (change_feed, feed_age) {
        (Some(n), age) => {
            let mut r = pigeonhole::FeedRetention::bytes(n);
            if let Some(a) = age {
                r = r.age(a.0);
            }
            o = o.change_feed(r);
        }
        (None, Some(_)) => {
            return Err(arg_error(
                ruby,
                "feed_age: needs change_feed: (the history's size in bytes)",
            ));
        }
        (None, None) => {}
    }
    if let Some(t) = crate::convert::names(feed_tables)? {
        o = o.feed_tables(t);
    }
    Ok(o)
}
