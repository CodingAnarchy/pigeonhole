//! A direct stream's record writes (D324, #1621): the stream's bytes go to the file through a
//! handle opened for direct I/O (`O_DIRECT`), whole aligned blocks at a time, one write in
//! flight per stream. A buffered rewrite of the tail page waits on its folio while the
//! previous group's sync writes that page back; a direct write takes no folio.
//!
//! The driver hands each segment's bytes over in order ([`Direct::push`]). While a write is
//! in flight, later bytes wait in the segment's image and go out together as the next write
//! when it completes, which the completion submits itself: the driver never waits. Two direct
//! writes of the same block never overlap (the device may reorder them), which is why there is
//! one in flight. A write rewrites the tail block's earlier bytes from the image, and pads
//! past the last byte with zeros: what a prepared slot already holds there (D157), and what
//! recovery reads as the end of the segment.
//!
//! [`Direct::written`] moves when a write completes, never at its submission, so a record is
//! in the file, readable by another handle (the change feed, D310's M1), once it passes.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::Lsn;
use pigeonhole_io::{Error, ErrorKind, FileRef, IoBuf};

use crate::stream::lock;

/// Something to run once every byte before a position is written, or with the error of the
/// write that failed first.
pub(crate) type Waiter = Box<dyn FnOnce(pigeonhole_io::Result<()>) + Send>;

/// The error a waiter gets once a write failed with `kind`.
fn failed(kind: ErrorKind) -> Error {
    Error::new(kind, "wal direct write")
}

/// One segment's bytes not yet written ([`Direct::push`]).
struct Job {
    /// The segment's slot and epoch.
    slot: usize,
    epoch: u32,
    /// Segment offset of `image[0]`: aligned, at or below the first unwritten byte.
    base: u64,
    /// The segment's bytes from `base`: the tail block's written prefix, then the bytes
    /// not yet written.
    image: Vec<u8>,
    /// Segment offset past the last byte submitted.
    sent: u64,
}

impl Job {
    fn end(&self) -> u64 {
        self.base + self.image.len() as u64
    }
}

#[derive(Default)]
struct State {
    /// Segments in order; the front one is being written.
    jobs: VecDeque<Job>,
    in_flight: bool,
    /// The first write that failed (or was refused at a close): nothing is written any more.
    failed: Option<ErrorKind>,
    /// Run once [`Direct::written`] reaches the position (in order of position).
    waiters: Vec<(Lsn, Waiter)>,
}

/// A direct stream's writer ([`crate::stream`]'s `Shared::direct`).
pub(crate) struct Direct {
    /// The handle opened for direct I/O.
    file: FileRef,
    /// The alignment its I/O needs ([`pigeonhole_io::File::direct_align`]).
    align: u64,
    segment_size: u64,
    state: Mutex<State>,
    /// Position past the last written byte (an [`Lsn`]).
    written: AtomicU64,
}

/// A write to submit, worked out under the lock and submitted outside it (a backend may
/// complete it inline, and its completion takes the lock).
pub(crate) struct Submit {
    buf: IoBuf,
    at: u64,
    /// Position past the write's last byte.
    pub(crate) end: Lsn,
}

impl Direct {
    pub(crate) fn new(file: FileRef, align: u64, segment_size: u64) -> Self {
        Self {
            file,
            align,
            segment_size,
            state: Mutex::new(State::default()),
            written: AtomicU64::new(0),
        }
    }

    /// Position past the last written byte.
    pub(crate) fn written(&self) -> Lsn {
        Lsn(self.written.load(Ordering::Acquire))
    }

    /// The driver starts segment `epoch` in `slot`: its bytes follow from segment offset
    /// `from` (0, or past a header written on its own). Bytes of earlier segments still
    /// unwritten go first. Starting the segment just started, with nothing handed over yet,
    /// moves its start.
    pub(crate) fn start(&self, slot: usize, epoch: u32, from: u64) {
        let mut st = lock(&self.state);
        if let Some(job) = st.jobs.back_mut()
            && job.epoch == epoch
            && job.image.is_empty()
            && job.sent == job.base
        {
            job.base = from;
            job.sent = from;
            return;
        }
        st.jobs.push_back(Job {
            slot,
            epoch,
            base: from,
            image: Vec::new(),
            sent: from,
        });
    }

    /// Hands over `bytes` of the current segment, starting at segment offset `at` (where the
    /// previous ones ended). Returns the write to submit now, if none is in flight.
    #[must_use]
    pub(crate) fn push(&self, at: u64, bytes: &[u8]) -> Option<Submit> {
        let mut st = lock(&self.state);
        let job = st.jobs.back_mut().expect("a segment is started");
        debug_assert_eq!(job.end(), at, "bytes handed over in order");
        let _ = at;
        job.image.extend_from_slice(bytes);
        self.next(&mut st)
    }

    /// The next write, if none is in flight and the front segment has unsent bytes. Drops
    /// segments that are fully written.
    fn next(&self, st: &mut State) -> Option<Submit> {
        if st.in_flight || st.failed.is_some() {
            return None;
        }
        loop {
            let job = st.jobs.front_mut()?;
            if job.sent < job.end() {
                break;
            }
            if st.jobs.len() == 1 {
                return None;
            }
            st.jobs.pop_front();
        }
        let job = st.jobs.front_mut().expect("checked above");
        let start = job.sent / self.align * self.align;
        let data_end = job.end();
        let end = data_end.div_ceil(self.align) * self.align;
        let mut buf = IoBuf::zeroed((end - start) as usize);
        let from = (start - job.base) as usize;
        let n = (data_end - start) as usize;
        buf[..n].copy_from_slice(&job.image[from..from + n]);
        job.sent = data_end;
        st.in_flight = true;
        Some(Submit {
            buf,
            at: job.slot as u64 * self.segment_size + start,
            end: Lsn::new(job.epoch, data_end as u32),
        })
    }

    /// Submits `s` (from [`Direct::push`] or [`Direct::finish`]); `then` runs with the
    /// write's outcome, and must pass it to [`Direct::finish`]. Returns at once.
    pub(crate) fn submit(
        &self,
        s: Submit,
        then: impl FnOnce(Result<(), ErrorKind>, Lsn) + Send + 'static,
    ) {
        let end = s.end;
        // The continuation's own completion is not needed.
        drop(self.file.submit_write(s.buf, s.at).map(move |r| {
            then(r.map(|_| ()).map_err(|e| e.kind), end);
            Ok(())
        }));
    }

    /// A write ending at `end` finished (or was refused): on success, moves `written`, trims
    /// the image, runs the waiters it satisfied and returns the next write; on failure, fails
    /// every waiter and writes nothing more.
    /// `fail` runs once if it failed, before the waiters (it poisons the stream).
    pub(crate) fn finish(
        &self,
        r: Result<(), ErrorKind>,
        end: Lsn,
        fail: impl FnOnce(),
    ) -> Option<Submit> {
        let (ready, next) = {
            let mut st = lock(&self.state);
            st.in_flight = false;
            if let Err(kind) = r {
                st.failed.get_or_insert(kind);
                (std::mem::take(&mut st.waiters), None)
            } else {
                self.written.fetch_max(end.0, Ordering::AcqRel);
                if let Some(job) = st.jobs.front_mut()
                    && job.epoch == end.epoch()
                {
                    // Keep the written tail block's prefix: the next write rewrites it.
                    let keep = u64::from(end.offset()) / self.align * self.align;
                    let cut = (keep - job.base) as usize;
                    job.image.drain(..cut);
                    job.base = keep;
                }
                let (ready, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut st.waiters)
                    .into_iter()
                    .partition(|(at, _)| at.0 <= end.0);
                st.waiters = rest;
                let next = self.next(&mut st);
                (ready, next)
            }
        };
        if r.is_err() {
            fail();
        }
        for (_, w) in ready {
            w(r.map_err(failed));
        }
        next
    }

    /// The error of the first write that failed, if one did.
    pub(crate) fn failure(&self) -> Option<Error> {
        lock(&self.state).failed.map(failed)
    }

    /// Runs `w` once every byte before `upto` is written (at once if it is), or with the
    /// error of the first write that failed.
    pub(crate) fn after(&self, upto: Lsn, w: Waiter) {
        {
            let mut st = lock(&self.state);
            if let Some(kind) = st.failed {
                drop(st);
                return w(Err(failed(kind)));
            }
            if self.written().0 < upto.0 {
                st.waiters.push((upto, w));
                return;
            }
        }
        w(Ok(()));
    }
}
