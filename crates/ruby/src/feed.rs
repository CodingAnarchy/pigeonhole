//! The change feed (D310, #1322): `Table#feed`, and `Feed`'s waits.
//!
//! A commit comes back as `[seqno, timestamp, [[table, family, kind, row, qualifier,
//! timestamp, value], ...]]`, which `lib/pigeonhole/api.rb` turns into frozen `Commit` and
//! `Change` objects.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use magnus::{Error, RArray, RString, Ruby, TryConvert, prelude::*};
use pigeonhole::{ChangeKind, ErrorCode};

use crate::classes::{Feed, Snapshot, Table};
use crate::convert::{Bytes, Secs, arg_error, binary, lock};
use crate::error::{engine, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::read::{snapshot_arg, typed};
use crate::table::{Handle, either};

/// The longest a wait runs with the GVL released before it checks for an interrupt
/// (`Thread#raise`, `Thread#kill`, Ctrl-C), so a consumer blocked in `next` stays
/// interruptible although one engine call isn't.
const SLICE: Duration = Duration::from_millis(100);

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
        Ok(Feed(ForkSafe::new(Mutex::new(Some(feed)))))
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

/// A commit as `[seqno, timestamp, changes]`.
fn commit(ruby: &Ruby, c: &pigeonhole::Commit) -> Result<RArray, Error> {
    let changes = ruby.ary_new_capa(c.changes().len());
    for ch in c.changes() {
        let a = ruby.ary_new_capa(7);
        a.push(ch.table())?;
        a.push(ch.family())?;
        a.push(ruby.to_symbol(kind_name(ch.kind())))?;
        a.push(binary(ruby, ch.row()))?;
        a.push(binary(ruby, ch.qualifier()))?;
        a.push(ch.timestamp())?;
        a.push(
            ch.value()
                .map_or_else(|| ruby.qnil().as_value(), |v| typed(ruby, v)),
        )?;
        changes.push(a)?;
    }
    let out = ruby.ary_new_capa(3);
    out.push(c.seqno())?;
    out.push(c.timestamp())?;
    out.push(changes)?;
    Ok(out)
}

impl Feed {
    /// One wait of at most `slice` (`None`: no wait), with the GVL released. `Closed` for a
    /// closed feed.
    fn poll(
        ruby: &Ruby,
        rb_self: &Self,
        slice: Option<Duration>,
    ) -> Result<Option<pigeonhole::Commit>, Error> {
        let feed = rb_self.0.get(ruby)?;
        // The mutex is taken only inside the call, so a thread waiting on it (behind a
        // consumer's slice) never holds the GVL.
        let polled = engine(ruby, || {
            let mut guard = lock(feed);
            let Some(f) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match slice {
                Some(d) => f.next(d)?,
                None => f.try_next()?,
            };
            Ok(Some(next.cloned()))
        })?;
        polled.ok_or_else(|| with_code(ruby, ErrorCode::Closed, "the feed is closed"))
    }

    /// `native_next(timeout)`: the next commit, waiting up to `timeout` seconds (`nil`
    /// waits until one comes), in slices with an interrupt check between them; `nil` if
    /// none came in time.
    pub(crate) fn next(
        ruby: &Ruby,
        rb_self: &Self,
        timeout: Option<Secs>,
    ) -> Result<Option<RArray>, Error> {
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t.0));
        loop {
            let slice = deadline.map_or(SLICE, |d| {
                d.saturating_duration_since(Instant::now()).min(SLICE)
            });
            if let Some(c) = Self::poll(ruby, rb_self, Some(slice))? {
                return commit(ruby, &c).map(Some);
            }
            ruby.thread_check_ints()?;
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }

    /// `native_try_next`: the next commit if one is ready, else `nil`. Never waits.
    pub(crate) fn try_next(ruby: &Ruby, rb_self: &Self) -> Result<Option<RArray>, Error> {
        Self::poll(ruby, rb_self, None)?
            .map(|c| commit(ruby, &c))
            .transpose()
    }

    /// Where the feed is, as bytes: `Table#feed(resume:)` continues after the last commit
    /// delivered, in this open of the database or a later one.
    pub(crate) fn token(ruby: &Ruby, rb_self: &Self) -> Result<RString, Error> {
        let feed = rb_self.0.get(ruby)?;
        // The lock may be held by a thread waiting in `next`, for up to a slice.
        let token = without_gvl(ruby, || lock(feed).as_ref().map(pigeonhole::Feed::token))?
            .ok_or_else(|| with_code(ruby, ErrorCode::Closed, "the feed is closed"))?;
        Ok(binary(ruby, token.as_bytes()))
    }

    /// Once the feed expired: the oldest commit seqno its history still holds, when known.
    pub(crate) fn oldest_retained(ruby: &Ruby, rb_self: &Self) -> Result<Option<u64>, Error> {
        let feed = rb_self.0.get(ruby)?;
        without_gvl(ruby, || {
            lock(feed)
                .as_ref()
                .and_then(pigeonhole::Feed::oldest_retained)
        })
    }

    /// Ends the subscription now. Calls afterwards raise `Closed`; closing twice does
    /// nothing.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let feed = rb_self.0.get(ruby)?;
        without_gvl(ruby, || drop(lock(feed).take()))
    }

    /// Whether `close` was called.
    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        let feed = rb_self.0.get(ruby)?;
        without_gvl(ruby, || lock(feed).is_none())
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
                "change_feed_off: or change_feed:, not both",
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
