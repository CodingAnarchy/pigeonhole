"""The memory store under asyncio (#1479 PR 2, ICR 0121 amendment 2, ICR 0123): the awaited
writes mirror test_memory.py's cases, `Watch` and `ConsolidateLoop` mirror the Ruby
bindings', and the waits are cancel-safe (cli692's C1, perf287's W6)."""

import asyncio

import pytest

import pigeonhole as ph
from pigeonhole import memory as mem

FEED = 16 << 20


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fdb(path):
    d = ph.open(path, create=True, shards=1, change_feed=FEED)
    yield d
    d.close()


def model(dim=4):
    return mem.EmbeddingModel("m", dim, "f32")


# --- the awaited writes ---


def test_remember_async_and_exact_dedup(db):
    """store.rs: remember, an exact duplicate merges, verify is consistent."""

    async def main(s):
        r = await s.remember_async("n", "Dana prefers tabs", kind="semantic", session="s1")
        assert r.outcome == "new"
        again = await s.remember_async("n", "  Dana prefers   tabs", source="chat-2")
        assert (again.outcome, again.id) == ("merged", r.id)
        assert s.get(r.id).session == "s1"
        assert s.verify().is_consistent()

    with mem.Store.create(db, "agents") as s:
        run(main(s))


def test_concurrent_remember_async_of_one_text_dedups_to_one_memory(db):
    async def main(s):
        rs = await asyncio.gather(*[s.remember_async("n", "the same fact") for _ in range(20)])
        assert len({r.id for r in rs}) == 1
        assert sorted(r.outcome for r in rs).count("new") == 1

    with mem.Store.create(db, "agents") as s:
        run(main(s))
        assert s.verify().is_consistent()


def test_near_dedup_runs_in_remember_async(db):
    """The near-dedup lookup still runs: a near duplicate merges, as `remember` does."""

    async def main(s):
        a = await s.remember_async("n", "first", embedding=[1.0, 0.0, 0.0, 0.0])
        b = await s.remember_async("n", "second", embedding=[0.999, 0.01, 0.0, 0.0])
        return a, b

    with mem.Store.create(db, "agents", embedding=model()) as s:
        s.set_near_dedup(mem.NearDedup(0.95, policy="merge"))
        a, b = run(main(s))
        assert b.id == a.id and b.outcome != "new"


def test_links_supersede_mark_used_and_delete_async(db):
    async def main(s):
        a = (await s.remember_async("n", "the deploy runs on Fridays")).id
        b = (await s.remember_async("n", "the deploy runs on Thursdays")).id
        await s.link_async(b, a, mem.LinkType.DERIVED_FROM, 0.5)
        assert [(l.id, l.type, l.weight) for l in s.get(b).links] == [(a, 1, 0.5)]
        with pytest.raises(ph.InvalidArgument):
            await s.link_async(a, b, 5)
        await s.supersede_async(a, b)
        assert s.get(a).superseded_by == b
        m = await s.mark_used_async([b, str(a)])
        assert m.marked == 2 and m.missing == []
        assert await s.delete_async(b) and not await s.delete_async(b)
        # A link to a memory that's gone is refused, as `link` refuses it.
        with pytest.raises(ph.InvalidArgument, match="not found"):
            await s.link_async(a, b, mem.LinkType.SIMILAR_TO)

    with mem.Store.create(db, "agents") as s:
        run(main(s))
        assert s.verify().is_consistent()


def test_the_in_forms_are_the_sync_methods_in_a_transaction(db):
    with mem.Store.create(db, "agents") as s:
        a, b = s.remember("n", "alpha").id, s.remember("n", "beta").id
        with db.transaction() as txn:
            s.link_in(txn, a, b, mem.LinkType.SIMILAR_TO)
            s.supersede_in(txn, a, b)
            assert s.mark_used_in(txn, [a, a]).marked == 1
            assert s.get(a).links == []  # not before the commit
        assert len(s.get(a).links) == 1 and s.get(a).superseded_by == b
        s.settle()  # nothing parked without IVF: harmless


def test_a_link_against_a_concurrent_delete_conflicts_then_finds_it_gone(db):
    """perf287 on the addendum: the link's reads of both ends are in its transaction, so a
    delete between them and the commit conflicts, and the next attempt finds the end gone."""

    async def main(s):
        a = (await s.remember_async("n", "alpha")).id
        b = (await s.remember_async("n", "beta")).id
        txn = db.transaction()
        s.link_in(txn, a, b, mem.LinkType.SIMILAR_TO)
        assert s.delete(b)  # between the link's reads and its commit
        with pytest.raises(ph.Conflict):
            await txn.commit_async()
        with pytest.raises(ph.InvalidArgument, match="not found"):
            await s.link_async(a, b, mem.LinkType.SIMILAR_TO)

    with mem.Store.create(db, "agents") as s:
        run(main(s))


# --- the watch ---


def test_a_watch_delivers_through_next_async_and_next(fdb):
    async def main(s, w):
        r = await s.remember_async("n", "hello")
        c = await asyncio.wait_for(w.next_async(), 5)
        assert [(e.kind, e.id) for e in c.events] == [("remembered", r.id)]
        assert c.seqno > 0 and c.timestamp_us > 0 and len(c) == 1
        await s.delete_async(r.id)
        c = w.next(timeout=5)
        assert [e.kind for e in c.events] == ["deleted"]

    with mem.Store.create(fdb, "agents") as s:
        w = s.watch("n")
        assert w.oldest_retained is None and not w.closed
        run(main(s, w))
        w.close()
        assert w.closed


def test_a_cancelled_watch_wait_loses_nothing_and_one_waits_at_a_time(fdb):
    async def main(s, w):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(w.next_async(), 0.1)
        r = await s.remember_async("n", "after the cancel")
        c = await asyncio.wait_for(w.next_async(), 5)
        assert c.events[0].id == r.id
        pending = w.next_async()
        await asyncio.sleep(0.01)
        with pytest.raises(ValueError, match="in progress"):
            w.next_async()
        pending.cancel()

    with mem.Store.create(fdb, "agents") as s, s.watch(["n", "m"]) as w:
        run(main(s, w))


def test_closing_a_watch_ends_a_pending_next_async(fdb):
    """#1606: as for the feed, the close wakes a pending wait, which fails with Closed."""

    async def main(w):
        waiting = asyncio.ensure_future(w.next_async())
        await asyncio.sleep(0.05)
        w.close()
        with pytest.raises(ph.Closed):
            await asyncio.wait_for(waiting, 1)

    with mem.Store.create(fdb, "agents") as s:
        run(main(s.watch("n")))


# --- the consolidation loop ---


class Concat:
    def __init__(self):
        self.calls = 0

    def __call__(self, group):
        self.calls += 1
        return [mem.Memory("summary: " + " / ".join(m.text for m in group))]


def pair(s, ns):
    s.remember(ns, f"{ns} first note", session="s")
    s.remember(ns, f"{ns} second note", session="s")


def test_run_once_async_waits_then_runs_the_due_passes(fdb):
    c = Concat()
    policy = mem.ConsolidatePolicy(min_age=10, group_similarity=0.0)

    async def main(s, loop):
        step = await loop.run_once_async(0.1)  # the first step starts the loop
        assert step.passes == []
        pair(s, "n")
        step = await loop.run_once_async(2.0)
        assert step.events >= 1 and step.passes == []  # too young
        assert loop.next_due is not None and loop.next_due > loop._now_us()

    with mem.Store.create(fdb, "agents") as s:
        with s.consolidate_loop("main", c, policy=policy, namespaces="n") as loop:
            run(main(s, loop))
        # clock_offset moves the loop's clock past min_age.
        with s.consolidate_loop("main", c, policy=policy, namespaces="n", clock_offset=60) as loop:
            step = run(loop.run_once_async(0.1))
            assert [(ns, p.committed) for ns, p in step.passes] == [("n", 1)]
            assert c.calls == 1
            assert isinstance(step.token, bytes)


def test_a_dropped_loop_wait_loses_no_events(fdb):
    """cli692's C1, perf287's W6: run_once_async drops its wait on every timeout; the
    commits it took are still recorded, so every namespace is consolidated."""
    c = Concat()
    policy = mem.ConsolidatePolicy(min_age=10, group_similarity=0.0)

    async def main(s, loop):
        await loop.run_once_async(0)
        for i in range(10):
            pair(s, f"n{i}")
            await loop.run_once_async(0.001)  # a short wait, dropped at its timeout

    with mem.Store.create(fdb, "agents") as s:
        with s.consolidate_loop("main", c, policy=policy) as loop:
            run(main(s, loop))
            loop.run_once(0.5)
        with s.consolidate_loop("main", c, policy=policy, clock_offset=60) as loop:
            step = loop.run_once(0)
            assert sorted(ns for ns, _ in step.passes) == [f"n{i}" for i in range(10)]


def test_a_sync_run_once_and_an_async_wait_take_turns(fdb):
    c = Concat()

    async def main(s, loop):
        await loop.run_once_async(0)
        waiting = asyncio.ensure_future(loop.run_once_async(5.0))
        await asyncio.sleep(0.05)
        # A sync step on a worker thread meanwhile: it runs, and the wait goes on.
        step = await asyncio.to_thread(loop.run_once, 0)
        assert step.passes == []
        s.remember("n", "wakes the wait")
        assert (await asyncio.wait_for(waiting, 5)).events >= 1

    with mem.Store.create(fdb, "agents") as s, s.consolidate_loop("main", c) as loop:
        run(main(s, loop))
        loop.interrupt()  # from any thread; the next step returns early
        assert loop.run_once(5).interrupted
    with pytest.raises(ph.Closed):
        loop.run_once(0)
    assert loop.next_due is None  # nothing is due on a closed loop


def test_the_event_loop_keeps_running_while_a_sync_step_has_the_loop(fdb):
    """cli692's L1 on #1563: run_once_async reads next_due on the event loop's thread; while
    a sync run_once on another thread has the loop out (for its whole timeout), that read
    must not wait, so a ticker on the loop keeps its interval."""
    c = Concat()
    with mem.Store.create(fdb, "agents") as s, s.consolidate_loop("main", c) as loop:
        loop.run_once(0)

        async def main():
            gaps, stop = [], False

            async def ticker():
                last = asyncio.get_running_loop().time()
                while not stop:
                    await asyncio.sleep(0.01)
                    now = asyncio.get_running_loop().time()
                    gaps.append(now - last)
                    last = now

            tick = asyncio.ensure_future(ticker())
            sync = asyncio.ensure_future(asyncio.to_thread(loop.run_once, 1.0))
            await asyncio.sleep(0.05)  # the sync step has the loop out now
            for _ in range(5):
                await loop.run_once_async(0.05)
            await sync
            stop = True
            await tick
            return max(gaps)

        assert run(main()) < 0.5


def test_a_reader_process_watch_cant_await(fdb, path):
    """cli692's R1: next_async is the writer process's only; a reader's watch raises
    Unsupported (its sync next works)."""
    with mem.Store.create(fdb, "agents") as s:
        s.remember("n", "hello")
        reader = ph.open_reader(path)
        try:
            with mem.Store.open_reader(reader, "agents", model=None) as rs, rs.watch() as w:

                async def main():
                    with pytest.raises(ph.Unsupported):
                        await w.next_async()

                run(main())
        finally:
            reader.close()


# --- recall (#1560 PR 4, ICR 0124) ---


def _hit(h):
    sig = h.signals
    signals = None if sig is None else tuple(
        None if x is None else (x.rank, x.value)
        for x in (sig.vector, sig.keyword, sig.recency, sig.importance, sig.uses, sig.graph)
    )
    return (h.id, h.text, h.kind, h.importance, h.created_us, h.superseded_by, h.score,
            signals, h.embedding)


def _stats(st):
    return (st.candidates, st.truncated, st.expanded, st.expansion_truncated)


def _fill(s, n=60):
    words = ["deploy", "postgres", "cache", "memory", "agent", "index", "budget", "window"]
    ids = []
    for i in range(n):
        text = f"{words[i % 8]} {words[(i * 3) % 8]} {words[(i * 5) % 8]} note {i}"
        emb = [((i * 7) % 11) / 10 - 0.5, ((i * 3) % 5) / 4 - 0.5, (i % 4) / 3, 1.0]
        r = s.remember("ns" if i % 3 else "other", text, embedding=emb,
                       session="s1" if i % 2 else "s2", importance=(i % 10) / 10)
        ids.append(r.id)
    # A hub in "ns" (links stay within a namespace).
    in_ns = [ids[i] for i in range(n) if i % 3]
    for j, to in enumerate(in_ns[1::2]):
        s.link(in_ns[0], to, 16, (j % 9 + 1) / 10)
    for i in range(0, n, 7):
        s.mark_used([ids[i]])
    return ids


_CASES = [
    dict(text="deploy cache"),
    dict(embedding=[0.2, -0.1, 0.5, 1.0]),
    dict(text="memory agent", embedding=[0.1, 0.3, -0.2, 1.0], k=5, with_signals=True),
    dict(text="postgres", session="s1", include_superseded=True, candidates=3),
    dict(text="index budget", fusion="linear", with_embeddings=True),
    dict(text="deploy", expand=mem.Expand(2, max=10), with_signals=True),
    dict(embedding=[0.0, 0.0, 1.0, 1.0], expand=mem.Expand(3), byte_budget=300),
    dict(text="window note", byte_budget=120, k=20),
]


def test_recall_async_returns_what_recall_returns(db):
    """Every case's hits (signals and embeddings included) and stats, async against sync,
    over one namespace and two, at a snapshot and at the latest."""

    async def main(s, snap):
        for case in _CASES:
            for ns in ("ns", ["ns", "other"]):
                for at in (None, snap):
                    q = dict(case, now_us=1_900_000_000_000_000, snapshot=at)
                    want, wst = s.recall_with_stats(ns, **q)
                    got, gst = await s.recall_with_stats_async(ns, **q)
                    ctx = (case, ns, at is not None)
                    assert [_hit(h) for h in got] == [_hit(h) for h in want], ctx
                    assert _stats(gst) == _stats(wst), ctx
                    hits = await s.recall_async(ns, **q)
                    assert [_hit(h) for h in hits] == [_hit(h) for h in want], ctx

    with mem.Store.create(db, "agents", embedding=model()) as s:
        _fill(s)
        snap = db.snapshot()
        run(main(s, snap))
        snap.release()


def test_recall_async_starts_on_the_bridge_not_at_the_call(db):
    """ai1249's B1: a recall isn't polled inline, so even a warm recall's future is pending
    when the call returns, and the loop runs meanwhile."""

    async def main(s):
        s.recall("ns", text="deploy")  # warm
        f = s.recall_async("ns", text="deploy")
        assert isinstance(f, asyncio.Future) and not f.done()
        ticks = 0

        async def tick():
            nonlocal ticks
            while not f.done():
                ticks += 1
                await asyncio.sleep(0)

        await asyncio.gather(f, tick())
        assert ticks >= 1
        assert [h.id for h in f.result()] == [h.id for h in s.recall("ns", text="deploy")]

    with mem.Store.create(db, "agents", embedding=model()) as s:
        _fill(s)
        run(main(s))


def test_concurrent_recall_async_each_return_what_recall_does(db):
    async def main(s):
        qs = [dict(c, now_us=1_900_000_000_000_000) for c in _CASES] * 4
        got = await asyncio.gather(*[s.recall_async("ns", **q) for q in qs])
        for q, hits in zip(qs, got):
            assert [_hit(h) for h in hits] == [_hit(h) for h in s.recall("ns", **q)], q

    with mem.Store.create(db, "agents", embedding=model()) as s:
        _fill(s)
        run(main(s))


def test_recall_async_raises_what_recall_raises_and_survives_a_cancel(db):
    async def main(s):
        # Refused at the call, as the sync form refuses: a bad keyword.
        with pytest.raises(ph.InvalidArgument):
            s.recall_async("ns", text="x", fusion="nope")
        # Refused by the recall itself: neither text nor an embedding.
        with pytest.raises(ph.InvalidArgument):
            await s.recall_async("ns")
        with pytest.raises(ph.InvalidArgument):
            await s.recall_async("ns", expand=mem.Expand(9), text="x")
        # A cancelled recall leaves the bridge serving the next one.
        f = s.recall_async("ns", text="deploy", expand=mem.Expand(3))
        f.cancel()
        with pytest.raises(asyncio.CancelledError):
            await f
        assert len(await s.recall_async("ns", text="deploy")) > 0

    with mem.Store.create(db, "agents", embedding=model()) as s:
        _fill(s)
        run(main(s))
    with pytest.raises(ph.Closed):
        s.recall_async("ns", text="x")


def test_a_reader_store_recalls_async(db, path):
    async def main(rs):
        for case in _CASES:
            q = dict(case, now_us=1_900_000_000_000_000)
            want, wst = rs.recall_with_stats("ns", **q)
            got, gst = await rs.recall_with_stats_async("ns", **q)
            assert [_hit(h) for h in got] == [_hit(h) for h in want], case
            assert _stats(gst) == _stats(wst), case

    with mem.Store.create(db, "agents", embedding=model()) as s:
        _fill(s)
        with ph.open_reader(path) as reader:
            with mem.Store.open_reader(reader, "agents", model="m") as rs:
                run(main(rs))
