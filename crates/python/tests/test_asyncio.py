"""Native asyncio (#1479, ICR 0121): the `*_async` forms of the calls that wait, over the
async front door (D196, D210). They mirror the sync suites' commit, conditional, returning,
transact and feed cases, then the bridge's own: cancellation (perf2's C1, C2), many loops,
the loop never blocking, and the feed's hand-back slot (B5)."""

import asyncio
import threading
import time

import pytest

import pigeonhole as ph

FEED = 16 << 20


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def t(db):
    return db.table("t").family("f").family("n", ph.Family.counter()).create()


@pytest.fixture
def fdb(path):
    d = ph.open(path, create=True, shards=1, change_feed=FEED)
    yield d
    d.close()


@pytest.fixture
def ft(fdb):
    return fdb.table("t").family("f").create()


# --- commits ---


def test_async_commits_return_what_the_sync_ones_do(db, t):
    async def main():
        s1 = await t.mutate(b"a").put("f", b"q", b"1").commit_async()
        s2 = await t.mutate(b"b").put("f", b"q", b"2").commit_async()
        assert s2 > s1
        wb = db.write_batch()
        wb.put(t, b"c", "f", b"q", b"3")
        assert await wb.commit_async(durability=ph.Durability.SYNC) > s2

    run(main())
    assert [t.get(k, "f", b"q").value for k in (b"a", b"b", b"c")] == [b"1", b"2", b"3"]


def test_conditional_commits(db, t):
    """api.rs: conditional_commits_sync, awaited"""

    async def main():
        absent = ph.Condition.absent("f", b"lock")
        assert await t.mutate(b"r").put("f", b"lock", b"me").commit_if_async(absent) is not None
        assert await t.mutate(b"r").put("f", b"lock", b"you").commit_if_async(absent) is None

    run(main())
    assert t.get(b"r", "f", b"lock").value == b"me"


def test_returning_commits_and_their_errors(db, t):
    async def main():
        r = await t.mutate(b"o").incr("n", b"next", 5).commit_returning_async()
        assert r.value("n", b"next") == 5
        assert await t.incr_returning_async(b"o", "n", b"next", 2) == 7
        # A commit the engine refuses raises its usual class, not ReturnedValueError.
        with pytest.raises(ph.FamilyNotFound) as e:
            await t.mutate(b"o").put("missing", b"q", b"v").commit_async()
        assert not isinstance(e.value, ph.ReturnedValueError)

    run(main())


def test_concurrent_incr_returning_hands_out_each_value_once(t):
    """D314 on one loop: 200 increments in flight at once get exactly 1..=200"""

    async def main():
        return await asyncio.gather(
            *[t.incr_returning_async(b"orders", "n", b"next", 1) for _ in range(200)]
        )

    assert sorted(run(main())) == list(range(1, 201))


def test_a_mutation_commits_once_and_needs_a_running_loop(t):
    m = t.mutate(b"r").put("f", b"q", b"v")
    # No running loop: refused before the mutation is spent.
    with pytest.raises(RuntimeError):
        m.commit_async()

    async def main():
        await m.commit_async()
        with pytest.raises(ValueError):
            m.commit_async()

    run(main())


def test_a_future_is_an_asyncio_future_of_its_loop(t):
    """B1/B2: a real asyncio.Future, created on the loop it was called on."""

    async def main():
        f = t.mutate(b"r").put("f", b"q", b"v").commit_async()
        assert isinstance(f, asyncio.Future)
        assert f.get_loop() is asyncio.get_running_loop()
        await f

    run(main())


# --- transactions ---


def test_transact_async_retries_a_conflict(db, t):
    """transact.rs: a_conflicting_attempt_is_retried_and_counted, with an async body"""
    t.put(b"a", "f", b"v", 0)
    attempts = []

    async def body(txn):
        attempts.append(1)
        n = (await txn.get_async(t, b"a", "f", b"v")).as_int()
        if len(attempts) == 1:
            await t.mutate(b"a").put("f", b"v", 100).commit_async()
        txn.put(t, b"a", "f", b"v", n + 1)
        return n

    assert run(db.transact_async(body)) == 100
    assert len(attempts) == 2
    assert t.get(b"a", "f", b"v").as_int() == 101


def test_transact_async_stops_at_the_attempt_cap_and_on_the_apps_error(db, t):
    t.put(b"a", "f", b"v", 0)
    attempts = []

    async def always_conflicts(txn):
        attempts.append(1)
        await txn.get_async(t, b"a", "f", b"v")
        t.put(b"a", "f", b"v", len(attempts))
        txn.put(t, b"a", "f", b"v", -1)

    with pytest.raises(ph.Conflict):
        run(db.transact_async(always_conflicts, max_attempts=3))
    assert len(attempts) == 3

    async def fails(txn):
        txn.put(t, b"b", "f", b"v", 1)
        raise KeyError("mine")

    with pytest.raises(KeyError):
        run(db.transact_async(fails))
    assert t.get(b"b", "f", b"v") is None


def test_concurrent_async_increments_lose_no_update(db, t):
    """transact.rs: concurrent_increments_lose_no_update, as 100 tasks on one loop"""
    t.put(b"a", "f", b"v", 0)

    async def bump(txn):
        cell = await txn.get_async(t, b"a", "f", b"v")
        txn.put(t, b"a", "f", b"v", cell.as_int() + 1)

    async def main():
        await asyncio.gather(*[db.transact_async(bump, max_attempts=1000) for _ in range(100)])

    run(main())
    assert t.get(b"a", "f", b"v").as_int() == 100


def test_a_body_of_the_wrong_kind_is_refused(db, t):
    """B3: an async body to transact, a sync one to transact_async."""

    async def abody(txn):
        txn.put(t, b"x", "f", b"q", b"v")

    with pytest.raises(TypeError, match="transact_async"):
        db.transact(abody)
    with pytest.raises(TypeError, match="async def"):
        run(db.transact_async(lambda txn: txn.put(t, b"x", "f", b"q", b"v")))
    assert t.get(b"x", "f", b"q") is None


def test_a_read_still_pending_at_commit_counts(db, t):
    """B4: get_async records its read at the call, so a commit while it's pending still
    checks it."""
    t.put(b"a", "f", b"v", 1)

    async def main():
        txn = db.transaction()
        pending = txn.get_async(t, b"a", "f", b"v")
        t.put(b"a", "f", b"v", 2)  # changes the cell the read covers
        txn.put(t, b"b", "f", b"v", 1)
        with pytest.raises(ph.Conflict):
            await txn.commit_async()
        assert (await pending).as_int() == 1  # it still resolves, at the snapshot

    run(main())


# --- the change feed ---


def test_feed_next_async_waits_and_times_out_without_losing_a_commit(fdb, ft):
    async def main():
        feed = ft.feed()
        waiting = asyncio.ensure_future(feed.next_async())
        await asyncio.sleep(0.05)
        s = await ft.mutate(b"r1").put("f", b"q", b"1").commit_async()
        assert (await asyncio.wait_for(waiting, 5)).seqno == s
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(feed.next_async(), 0.1)
        s2 = await ft.mutate(b"r2").put("f", b"q", b"2").commit_async()
        assert (await asyncio.wait_for(feed.next_async(), 5)).seqno == s2
        feed.close()

    run(main())


def test_one_feed_wait_at_a_time(fdb, ft):
    async def main():
        feed = ft.feed()
        w = asyncio.ensure_future(feed.next_async())
        await asyncio.sleep(0.01)
        with pytest.raises(ValueError, match="in progress"):
            feed.try_next()
        with pytest.raises(ValueError, match="in progress"):
            feed.next_async()
        w.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w
        # Cancelled and awaited at once: the next call isn't refused.
        assert feed.try_next() is None
        feed.close()

    run(main())


def test_a_commit_taken_after_its_wait_was_cancelled_is_handed_back(fdb, ft):
    """B5: the poll returned the commit, then the wait was cancelled before the loop
    claimed it. The commit is the next any call returns, and token() is from before it."""

    async def main():
        feed = ft.feed()
        before = feed.token()
        w = feed.next_async()
        ft.mutate(b"r").put("f", b"q", b"v").commit()  # sync: the loop doesn't run
        time.sleep(0.3)  # the bridge takes it; its claim waits for the loop
        w.cancel()
        assert feed.token() == before
        c = feed.try_next()
        assert c is not None and c.changes[0].row == b"r"
        assert feed.token() != before
        await asyncio.sleep(0.05)  # the cancelled claim runs, and takes nothing
        assert feed.try_next() is None
        feed.close()

    run(main())


# --- the bridge ---


def gate():
    g = getattr(ph._native, "_Gate", None)
    if g is None:
        pytest.skip("needs the test-hooks build (maturin develop --features test-hooks)")
    return g()


def test_a_cancelled_wait_is_dropped_at_once_and_never_polled_again():
    """perf2's C2 and C1: the cancel drops the Rust future without waiting for any other
    completion (a waiting commit would hold its shard's admission queue), and opening the
    gate afterwards polls nothing."""

    async def main():
        g = gate()
        fut = g.future()
        assert g.polls == 1 and g.drops == 0
        fut.cancel()
        await asyncio.sleep(0)  # the done callback runs
        assert g.drops == 1
        g.open()
        await asyncio.sleep(0.05)
        assert g.polls == 1

    run(main())


def test_a_wake_from_another_thread_completes_on_the_loop():
    async def main():
        g = gate()
        fut = g.future()
        threading.Timer(0.05, g.open).start()
        assert await asyncio.wait_for(fut, 5) is None
        assert g.polls == 2 and g.drops == 1

    run(main())


def test_a_panicking_conversion_fails_its_future_and_the_bridge_goes_on():
    """perf2's F1: a panic converting one result fails that future with RuntimeError; the
    bridge thread survives, so later awaits still complete."""

    async def main():
        bad, good = gate(), gate()
        f = bad.panicking_future()
        bad.open()
        with pytest.raises(RuntimeError, match="internal error"):
            await asyncio.wait_for(f, 5)
        g = good.future()
        good.open()
        assert await asyncio.wait_for(g, 5) is None

    run(main())


def test_many_loops_in_threads_and_a_closed_one(t):
    """B2: each future completes on its own loop; a loop closed with a commit pending drops
    that completion and breaks nothing."""
    errors = []

    def worker(i):
        try:

            async def main():
                return await asyncio.gather(
                    *[t.mutate(f"w{i}-{k}").put("f", b"q", b"v").commit_async() for k in range(50)]
                )

            assert len(asyncio.run(main())) == 50
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert errors == []

    loop = asyncio.new_event_loop()

    async def start_one():
        return t.mutate(b"closed").put("f", b"q", b"v").commit_async()

    loop.run_until_complete(start_one())
    loop.close()  # its commit's completion arrives at a closed loop
    time.sleep(0.1)

    async def after():
        return await t.mutate(b"after").put("f", b"q", b"v").commit_async()

    assert asyncio.run(after()) > 0


def test_the_loop_keeps_running_while_commits_wait(t):
    """No event-loop thread blocks on an engine call: a ticker keeps its interval while
    Sync commits are awaited."""

    async def main():
        gaps = []
        stop = False

        async def ticker():
            last = time.perf_counter()
            while not stop:
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                gaps.append(now - last)
                last = now

        tick = asyncio.ensure_future(ticker())
        await asyncio.gather(
            *[
                t.mutate(f"s{k}").put("f", b"q", b"v").durability(ph.Durability.SYNC).commit_async()
                for k in range(200)
            ]
        )
        stop = True
        await tick
        return max(gaps)

    # Generous: a blocked loop would show the whole batch's sync time as one gap.
    assert run(main()) < 0.5


def test_uvloop():
    """B6: the bridge only calls call_soon_threadsafe, so uvloop works too."""
    uvloop = pytest.importorskip("uvloop")

    async def main(path):
        db = ph.open(path, create=True, shards=1)
        t = db.table("t").family("f").create()
        assert await t.mutate(b"r").put("f", b"q", b"v").commit_async() > 0
        db.close()

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        uvloop.run(main(Path(d) / "u.phdb"))


def test_a_sync_next_never_stalls_the_bridge(fdb, ft):
    """cli692's P1 (a): after a cancelled next_async, a sync next(timeout=2) on another
    thread waits with the feed's lock released, so the bridge (and an unrelated commit on
    the loop) isn't held up behind it."""

    async def main():
        feed = ft.feed()
        w = feed.next_async()
        await asyncio.sleep(0.01)
        w.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w
        th = threading.Thread(target=lambda: feed.next(timeout=2))
        th.start()
        await asyncio.sleep(0.05)
        s = time.perf_counter()
        await asyncio.wait_for(ft.mutate(b"other").put("f", b"q", b"v").commit_async(), 5)
        took = time.perf_counter() - s
        await asyncio.to_thread(th.join)
        feed.close()
        return took

    assert run(main()) < 1.0


def test_next_async_during_a_sync_next_is_refused_at_once(fdb, ft):
    """cli692's P1 (b): the loop never blocks on a sync read's wait."""

    async def main():
        feed = ft.feed()
        th = threading.Thread(target=lambda: feed.next(timeout=1))
        th.start()
        await asyncio.sleep(0.05)
        s = time.perf_counter()
        with pytest.raises(ValueError, match="sync next"):
            feed.next_async()
        took = time.perf_counter() - s
        await asyncio.to_thread(th.join)
        feed.close()
        return took

    assert run(main()) < 0.1


def test_two_sync_nexts_on_one_feed_still_take_turns(fdb, ft):
    """Unchanged sync behaviour: a second thread's next waits for the first, it isn't
    refused."""
    feed = ft.feed()
    got = []
    ths = [threading.Thread(target=lambda: got.append(feed.next(timeout=2))) for _ in range(2)]
    for th in ths:
        th.start()
    time.sleep(0.05)
    ft.mutate(b"a").put("f", b"q", b"1").commit()
    ft.mutate(b"b").put("f", b"q", b"2").commit()
    for th in ths:
        th.join()
    assert sorted(c.changes[0].row for c in got) == [b"a", b"b"]
    feed.close()


def test_both_transacts_count_their_retries(db, t):
    """cli692's N1: a retried attempt counts in EngineMetrics.transaction_retries, from the
    sync and the async transact alike."""
    retries = getattr(ph._native, "_transaction_retries", None)
    if retries is None:
        pytest.skip("needs the test-hooks build")
    t.put(b"a", "f", b"v", 0)
    attempts = []

    def body(txn):
        attempts.append(1)
        n = txn.get(t, b"a", "f", b"v").as_int()
        if len(attempts) == 1:
            t.put(b"a", "f", b"v", 100)
        txn.put(t, b"a", "f", b"v", n + 1)

    before = retries(db)
    db.transact(body)
    assert retries(db) == before + 1

    attempts.clear()

    async def abody(txn):
        attempts.append(1)
        n = (await txn.get_async(t, b"a", "f", b"v")).as_int()
        if len(attempts) == 1:
            t.put(b"a", "f", b"v", 200)
        txn.put(t, b"a", "f", b"v", n + 1)

    run(db.transact_async(abody))
    assert retries(db) == before + 2


def test_a_batch_called_with_no_loop_keeps_its_writes(db, t):
    """cli692's N2: a commit_async that fails before submitting (no running loop) leaves
    the batch's writes, so it can still be committed."""
    wb = db.write_batch()
    wb.put(t, b"x", "f", b"q", b"v")
    with pytest.raises(RuntimeError):
        wb.commit_async()
    assert len(wb) == 1
    wb.commit()
    assert t.get(b"x", "f", b"q").value == b"v"
