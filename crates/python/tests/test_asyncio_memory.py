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
