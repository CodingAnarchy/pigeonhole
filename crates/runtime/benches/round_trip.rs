//! Submit → handle → notify round trip: a caller thread submits a message carrying a
//! `Notifier`, the shard handles it and notifies, and the caller wakes from `wait()` (sync)
//! or from its waker (async).

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use criterion::{Criterion, criterion_group, criterion_main};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_runtime::{
    Notifier, Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId, completion,
};

struct Echo;

impl ShardHandler for Echo {
    type Msg = (u64, Notifier<u64>);
    fn handle(&mut self, _ctx: &mut ShardContext<'_, Self::Msg>, (v, n): Self::Msg) {
        n.notify(v);
    }
    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Self::Msg>) {}
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        thread::park();
    }
}

fn round_trip(c: &mut Criterion) {
    let mut config = RuntimeConfig::new(PreadVfs::new(1));
    config.shards = 1;
    config.pin_threads = false;
    let rt = Runtime::start(config, vec![Echo]).unwrap();
    let sub = rt.submitter(ShardId(0));

    let mut g = c.benchmark_group("runtime");
    g.bench_function("submit_handle_notify_sync", |b| {
        b.iter(|| {
            let (n, w) = completion();
            sub.submit((1, n)).unwrap();
            w.wait()
        })
    });
    g.bench_function("submit_handle_notify_async", |b| {
        b.iter(|| {
            let (n, w) = completion();
            sub.submit((1, n)).unwrap();
            block_on(w)
        })
    });
    g.finish();
    rt.shutdown().unwrap();
}

criterion_group!(benches, round_trip);
criterion_main!(benches);
