"""The GIL is released during engine calls: Python threads run while one waits on a commit."""

import threading
import time

import pigeonhole as ph


def test_other_threads_run_during_engine_calls(db):
    t = db.table("t").family("f").create()
    stop = threading.Event()
    ticks = [0]

    def count():
        while not stop.is_set():
            ticks[0] += 1

    counter = threading.Thread(target=count)
    counter.start()
    try:
        # Sync commits wait on fsync: with the GIL held, the counter would stall meanwhile.
        before = ticks[0]
        started = time.monotonic()
        for i in range(50):
            t.put(f"r{i}", "f", b"q", b"v" * 1024, durability=ph.Durability.SYNC)
        elapsed = time.monotonic() - started
        progressed = ticks[0] - before
    finally:
        stop.set()
        counter.join()
    assert progressed > 0, f"no progress in {elapsed:.3f}s of commits"
    assert len(list(t.scan())) == 50
