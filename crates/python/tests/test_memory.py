"""`pigeonhole.memory` (D311 T6): mirrors crates/memory/tests (store, recall, readers,
forgetting, consolidation, near dedup), plus the binding's own rules: lifecycle, a Python
consolidator, and errors."""

import threading
from datetime import timedelta

import pytest

import pigeonhole as ph
from pigeonhole import memory as mem


def model(dim=4):
    return mem.EmbeddingModel("m", dim, "f32")


def test_remember_get_and_exact_dedup(db):
    """store.rs: remember, get, an exact duplicate merges, verify is consistent."""
    with mem.Store.create(db, "agents") as s:
        assert s.embedding_model is None
        r = s.remember("user:dana", "Dana prefers tabs", kind="semantic", importance=0.8,
                       source="chat-1", session="s1")
        assert r.outcome == "new" and isinstance(r.id, mem.MemoryId)
        again = s.remember("user:dana", "  Dana prefers   tabs", source="chat-2")
        assert (again.outcome, again.id) == ("merged", r.id)
        m = s.get(r.id)
        assert m.text == "Dana prefers tabs"
        assert m.kind == mem.Kind.SEMANTIC == "semantic"
        assert m.session == "s1" and m.importance == pytest.approx(0.8)
        assert sorted(x.name for x in m.sources) == ["chat-1", "chat-2"]
        # The text form round-trips, and every id argument takes it.
        assert mem.MemoryId(str(r.id)) == r.id and s.get(str(r.id)).id == r.id
        assert mem.MemoryId.from_bytes(r.id.to_bytes()) == r.id
        assert r.id.namespace == "user:dana" and r.id.created_us > 0
        assert len({r.id, mem.MemoryId(str(r.id))}) == 1
        assert s.verify().is_consistent()


def test_ids_that_are_not_ids_are_invalid_argument(db):
    with mem.Store.create(db, "agents") as s:
        for bad in ["nope", ""]:
            with pytest.raises(ph.InvalidArgument):
                mem.MemoryId(bad)
            with pytest.raises(ph.InvalidArgument):
                s.get(bad)
        with pytest.raises(ph.InvalidArgument):
            mem.MemoryId.from_bytes(b"\x05\x00ab")
        with pytest.raises(TypeError):
            s.get(42)


def test_links_supersede_and_mark_used(db):
    with mem.Store.create(db, "agents") as s:
        a = s.remember("n", "the deploy runs on Fridays").id
        b = s.remember("n", "the deploy runs on Thursdays").id
        c = s.remember("other", "unrelated").id
        s.link(b, a, mem.LinkType.DERIVED_FROM, 0.5)
        s.link(a, b, 20)
        got = s.get(b)
        assert [(l.id, l.type, l.weight) for l in got.links] == [(a, 1, 0.5)]
        assert [(l.id, l.type) for l in got.backlinks] == [(a, 20)]
        for bad in (5, 256, -1, 1 << 70):  # below the first custom type, or not a u8
            with pytest.raises(ph.InvalidArgument):
                s.link(a, b, bad)
        with pytest.raises(ph.InvalidArgument):
            s.link(a, c, mem.LinkType.SIMILAR_TO)  # across namespaces
        s.supersede(a, b)
        assert s.get(a).superseded_by == b
        hits = s.recall("n", text="deploy")
        assert [h.id for h in hits] == [b]
        both = s.recall(["n"], text="deploy", include_superseded=True)
        assert {h.id for h in both} == {a, b}
        assert next(h for h in both if h.id == a).superseded_by == b
        m = s.mark_used([b, str(a)])
        assert m.marked == 2 and m.missing == []
        assert s.get(b).uses == 1
        assert s.delete(c) and not s.delete(c)
        assert s.mark_used([c]).missing == [c]


def test_recall_ranks_signals_budgets_and_snapshots(db):
    """recall.rs: the vector signal, keyword-only recall on a pinned store, signals,
    the byte budget, weights, and one snapshot."""
    with mem.Store.create(db, "agents", embedding=model()) as s:
        assert s.embedding_model == model()
        a = s.remember("n", "the cache sits in front of the database",
                       embedding=[1.0, 0.0, 0.0, 0.0], importance=0.9).id
        b = s.remember("n", "lunch is at noon", embedding=[0.0, 1.0, 0.0, 0.0]).id
        with pytest.raises(ph.InvalidArgument):
            s.remember("n", "no embedding on a pinned store")
        with pytest.raises(ph.InvalidArgument):
            s.remember("n", "wrong dimension", embedding=[1.0, 0.0])
        assert s.recall("n", embedding=[0.9, 0.1, 0.0, 0.0])[0].id == a
        assert s.recall("n", embedding=[0.1, 0.9, 0.0, 0.0])[0].id == b
        assert s.recall("n", text="lunch")[0].id == b
        hit = s.recall("n", text="cache", with_signals=True, with_embeddings=True)[0]
        assert hit.signals.keyword.rank == 1 and hit.signals.keyword.value > 0
        assert hit.signals.vector is None  # no query embedding
        assert hit.embedding == [1.0, 0.0, 0.0, 0.0]
        hits, stats = s.recall_with_stats("n", text="noon", byte_budget=1)
        assert len(hits) == 1 and stats.truncated  # the first hit even past the budget
        assert stats.candidates >= 1
        # Weights that only count recency put the newer memory first.
        only_recency = mem.Weights(vector=0, keyword=0, importance=0, uses=0)
        assert s.recall("n", text="cache", weights=only_recency)[0].id == b
        assert only_recency.recency == 1.0 and mem.Weights() == mem.Weights()
        with pytest.raises(ph.InvalidArgument):
            s.recall("n")  # neither text nor embedding
        with pytest.raises(ph.InvalidArgument):
            s.recall("n", text="x", fusion="nope")
        snap = db.snapshot()
        s.remember("n", "a later cache note", embedding=[1.0, 0.0, 0.0, 0.0])
        at = s.recall("n", text="cache", snapshot=snap)
        assert all(h.text != "a later cache note" for h in at)
        assert len(at) >= 1
        assert s.recall("n", text="cache", snapshot=snap, fusion="linear")[0].id == a
        snap.release()
        with pytest.raises(ph.SnapshotExpired):
            s.recall("n", text="cache", snapshot=snap)


def test_recall_expands_through_links(db):
    """recall.rs's graph expansion (T4): Expand, the graph signal and weight, the stats."""
    with mem.Store.create(db, "graph", embedding=model()) as s:
        far = s.remember("n", "the reason behind it", embedding=[0.0, 0.0, 1.0, 0.0]).id
        seed = s.remember("n", "the decision", embedding=[1.0, 0.0, 0.0, 0.0],
                          links=[(far, 16, 0.8)]).id
        q = dict(embedding=[1.0, 0.0, 0.0, 0.0], candidates=1, with_signals=True)
        # Without expand there's no graph signal, whatever its weight.
        plain, st = s.recall_with_stats("n", weights=mem.Weights(graph=5.0), **q)
        assert [h.id for h in plain] == [seed]
        assert all(h.signals.graph is None for h in plain)
        assert (st.expanded, st.expansion_truncated) == (0, False)
        hits, st = s.recall_with_stats("n", expand=mem.Expand(1), **q)
        by = {h.id: h for h in hits}
        assert by[seed].signals.graph is None  # a seed
        g = by[far].signals.graph
        assert g.rank == 1 and abs(g.value - 0.8) < 1e-6  # seed 1's value (1) times 0.8
        assert (st.expanded, st.expansion_truncated) == (1, False)
        # max=0 gives nothing; the budget's cut is reported.
        _, st = s.recall_with_stats("n", expand=mem.Expand(1, max=0), **q)
        assert st.expanded == 0
        _, st = s.recall_with_stats("n", expand=mem.Expand(), byte_budget=1, **q)
        assert (st.expanded, st.expansion_truncated) == (0, True)
        x = mem.Expand(2, max=5)
        assert (x.hops, x.max) == (2, 5) and x == mem.Expand(2, max=5)
        assert repr(x) == "Expand(hops=2, max=5)" and mem.Expand.MAX_HOPS == 4
        assert mem.Expand().max == 20 and mem.Weights().graph == 0.5
        for bad in (0, mem.Expand.MAX_HOPS + 1):
            with pytest.raises(ph.InvalidArgument):
                s.recall("n", expand=mem.Expand(bad), **q)


def test_open_checks_the_model(db):
    mem.Store.create(db, "agents", embedding=model()).close()
    with pytest.raises(mem.ModelMismatch) as e:
        mem.Store.open(db, "agents", model="other")
    assert isinstance(e.value, ph.InvalidArgument) and e.value.code == "InvalidArgument"
    with mem.Store.open(db, "agents", model="m") as s:
        s.remember("n", "x", embedding=[1.0, 0.0, 0.0, 0.0])
    with mem.Store.open(db, "agents", model=None) as s:
        assert s.recall("n", text="x")[0].text == "x"
        with pytest.raises(ph.InvalidArgument):
            s.remember("n", "a handle without the model can't remember")


def test_in_transactions(db):
    """remember_in / delete_in commit with the transaction; remember_in skips near dedup
    (simp724's P3 on #1340); inside Database.transact, a retry re-reads."""
    with mem.Store.create(db, "agents", embedding=model(),
                          near_dedup=mem.NearDedup(0.9, policy="skip")) as s:
        first = s.remember("n", "the timeout is 5 s", embedding=[1.0, 0.0, 0.0, 0.0]).id
        near = s.remember("n", "the timeout is five seconds", embedding=[0.99, 0.01, 0.0, 0.0])
        assert (near.outcome, near.id) == ("similar", first)
        assert s.near_duplicate("n", "x", embedding=[0.98, 0.02, 0.0, 0.0])[0] == first

        def body(txn):
            return s.remember_in(txn, "n", "the timeout is 6 s",
                                 embedding=[0.98, 0.02, 0.0, 0.0])

        r = db.transact(body)
        assert r.outcome == "new"
        txn = db.transaction()
        assert s.delete_in(txn, r.id)
        assert s.get(r.id) is not None  # not committed yet
        txn.commit()
        assert s.get(r.id) is None
        txn = db.transaction()
        txn.rollback()
        with pytest.raises(ValueError):
            s.remember_in(txn, "n", "x", embedding=[1.0, 0.0, 0.0, 0.0])


def test_forgetting(db):
    """forget.rs: a pass under the persisted policy or an override, resume, forget_in."""
    with mem.Store.create(db, "agents") as s:
        with pytest.raises(ph.InvalidArgument):
            s.forget()  # no policy anywhere
        assert s.forget_policy() is None
        old = s.remember("n", "an old episode").id
        keep = s.remember("n", "a pinned rule", pinned=True).id
        later = old.created_us + 10 * 86_400_000_000
        p = mem.ForgetPolicy(max_age={"episodic": timedelta(days=1)}, max_per_pass=10,
                             half_life=timedelta(days=3), use_weight=0.2, min_retention=0.01,
                             interval=60)
        assert p.max_age == [("episodic", timedelta(days=1))]
        assert (p.half_life, p.use_weight, p.max_per_pass, p.interval) == (
            timedelta(days=3), 0.2, 10, timedelta(seconds=60))
        passed = s.forget(policy=p, now_us=later)
        assert (passed.deleted, passed.protected) == (1, 1) and passed.resume is None
        assert s.get(old) is None and s.get(keep) is not None
        s.set_forget_policy(p)
        assert s.forget_policy() == p
        other = s.remember("n", "another episode").id
        txn = db.transaction()
        assert s.forget_in(txn, other, other.created_us) == "not_due"
        assert s.forget_in(txn, keep, later) == "protected"
        assert s.forget_in(txn, other, later) == "deleted"
        txn.commit()
        assert s.get(other) is None
        s.set_forget_policy(None)
        assert s.forget_policy() is None
        with pytest.raises(ph.InvalidArgument):
            s.forget(policy=mem.ForgetPolicy(interval=0.1))


def test_consolidation_with_a_python_consolidator(db):
    """consolidate.rs: a Python consolidator folds a group; it may call the store
    (simp724's P2 on #1340); its exception stops the pass with the pass so far."""
    with mem.Store.create(db, "agents") as s:
        for t in ["met Dana at 9", "Dana asked about tabs", "Dana prefers tabs"]:
            s.remember("n", t, session="s1")
        seen = []

        def summarise(group):
            seen.append([m.text for m in group])
            # Re-entrancy: the consolidator may read the store.
            assert s.recall("n", text="Dana")
            return [mem.Memory("Dana prefers tabs (summary)", kind="semantic",
                               importance=0.9)]

        p = s.consolidate("n", summarise, policy=mem.ConsolidatePolicy(min_age=0),
                          now_us=None)
        assert (p.groups, p.committed, p.created, p.skipped) == (1, 1, 1, 0)
        assert len(seen) == 1 and len(seen[0]) == 3
        summary = s.recall("n", text="summary")[0]
        assert summary.kind == "semantic"
        got = s.get(summary.id)
        assert {l.type for l in got.links} == {mem.LinkType.DERIVED_FROM}
        # Consolidated memories aren't grouped again.
        assert s.consolidate("n", summarise, policy=mem.ConsolidatePolicy(min_age=0)).groups == 0

        for t in ["first", "second"]:
            s.remember("m", t, session="s2")

        class Boom(Exception):
            pass

        def fails(group):
            raise Boom("no model")

        with pytest.raises(mem.ConsolidateError) as e:
            s.consolidate("m", fails, policy=mem.ConsolidatePolicy(min_age=0))
        assert isinstance(e.value, ph.MergeFailed) and e.value.code == "MergeFailed"
        assert isinstance(e.value.__cause__, Boom)
        assert e.value.consolidated.committed == 0
        with pytest.raises(mem.ConsolidateError) as e:
            s.consolidate("m", lambda g: "not a list", policy=mem.ConsolidatePolicy(min_age=0))
        assert isinstance(e.value.__cause__, TypeError)
        with pytest.raises(mem.ConsolidateError) as e:
            s.consolidate("m", lambda g: [1], policy=mem.ConsolidatePolicy(min_age=0))
        assert isinstance(e.value.__cause__, TypeError)
        with pytest.raises(TypeError):
            s.consolidate("m", "not callable")
        # Returning nothing keeps the group, marked consolidated.
        p = s.consolidate("m", lambda g: [], policy=mem.ConsolidatePolicy(min_age=0))
        assert (p.committed, p.created) == (1, 0)
        assert all(s.get(h.id).consolidated for h in s.recall("m", text="first second"))
        assert s.verify().is_consistent()


def test_a_consolidator_runs_while_another_thread_recalls(db):
    """The GIL is handed back around the callback (simp724's P2): a second thread recalls
    while the pass runs and the consolidator itself recalls."""
    with mem.Store.create(db, "agents") as s:
        for i in range(6):
            s.remember("n", f"note {i}", session="s")
        stop = threading.Event()
        counts = []

        def recall_loop():
            n = 0
            while not stop.is_set():
                s.recall("n", text="note")
                n += 1
            counts.append(n)

        t = threading.Thread(target=recall_loop)
        t.start()
        try:
            def summarise(group):
                assert s.recall("n", text="note")
                return [mem.Memory("all the notes")]

            p = s.consolidate("n", summarise, policy=mem.ConsolidatePolicy(min_age=0))
        finally:
            stop.set()
            t.join()
        assert p.committed == 1 and counts[0] > 0


def test_close_and_database_close(path):
    """simp724's P1 on #1340: Store.close and `with`, then Closed; Database.close closes the
    stores opened on it first, forgetter included."""
    db = ph.open(path, create=True, shards=1)
    p = mem.ForgetPolicy(max_age={"episodic": timedelta(days=1)}, interval=1)
    s = mem.Store.create(db, "agents", forget=p)
    s.remember("n", "x")
    s.close()
    s.close()  # idempotent
    with pytest.raises(ph.Closed):
        s.recall("n", text="x")
    s2 = mem.Store.open(db, "agents", model=None)
    s3 = mem.Store.open(db, "agents", model=None)
    db.close()
    for h in (s2, s3):
        with pytest.raises(ph.Closed):
            h.remember("n", "y")
    # The file reopens cleanly: nothing held it.
    db = ph.open(path, shards=1)
    with mem.Store.open(db, "agents", model=None) as s:
        assert s.recall("n", text="x")[0].text == "x"
    db.close()


def test_a_reader_recalls(db, path):
    with mem.Store.create(db, "agents") as s:
        rid = s.remember("n", "the build runs nightly").id
        with ph.open_reader(path) as reader:
            with mem.Store.open_reader(reader, "agents", model=None) as rs:
                assert rs.recall("n", text="build")[0].id == rid
                assert rs.get(rid).text == "the build runs nightly"
                assert rs.verify().memories == 1
                assert not hasattr(rs, "remember")
            with pytest.raises(ph.Closed):
                rs.recall("n", text="build")
            rs2 = mem.Store.open_reader(reader, "agents", model=None)
        with pytest.raises(ph.Closed):
            rs2.get(rid)


def test_near_dedup_settings(db):
    with mem.Store.create(db, "agents", embedding=model()) as s:
        assert s.near_dedup() is None
        n = mem.NearDedup(0.95, policy="merge")
        s.set_near_dedup(n)
        assert s.near_dedup() == n and n.policy == "merge" and n.similarity == pytest.approx(0.95)
        s.set_near_dedup(None)
        assert s.near_dedup() is None
        with pytest.raises(ph.InvalidArgument):
            s.set_near_dedup(mem.NearDedup(0.0))
        with pytest.raises(ph.InvalidArgument):
            mem.NearDedup(0.9, policy="nope")
    with mem.Store.create(db, "plain") as s, pytest.raises(ph.InvalidArgument):
        s.set_near_dedup(mem.NearDedup(0.9))


def test_a_close_during_a_consolidation_ends_it_with_closed(path):
    """simp724's P1: a call already running ends with Closed, not a hang. The consolidator
    closes the database mid-pass; the group's commit then fails with Closed."""
    db = ph.open(path, create=True, shards=1)
    s = mem.Store.create(db, "agents")
    for t in ["one", "two"]:
        s.remember("n", t, session="s")

    def closes(group):
        db.close()
        return [mem.Memory("summary")]

    with pytest.raises(ph.Closed):
        s.consolidate("n", closes, policy=mem.ConsolidatePolicy(min_age=0))
    with pytest.raises(ph.Closed):
        s.recall("n", text="one")


def test_keyboard_interrupt_in_a_consolidator_stops_the_pass(db):
    """simp724's P2: KeyboardInterrupt stops the pass like any exception, with the pass so
    far; the groups before it stay committed."""
    with mem.Store.create(db, "agents") as s:
        for session in ["a", "b"]:
            for t in ["x one", "x two"]:
                s.remember("n", f"{session} {t}", session=session)
        calls = []

        def second_interrupts(group):
            calls.append(group[0].session)
            if len(calls) == 2:
                raise KeyboardInterrupt
            return [mem.Memory(f"summary {group[0].session}")]

        with pytest.raises(mem.ConsolidateError) as e:
            s.consolidate("n", second_interrupts, policy=mem.ConsolidatePolicy(min_age=0))
        assert isinstance(e.value.__cause__, KeyboardInterrupt)
        assert e.value.consolidated.committed == 1
        assert s.recall("n", text="summary")[0].text == f"summary {calls[0]}"


def test_stores_opened_while_the_database_closes_never_outlive_it(path):
    """simp724's L1 on #1395: a store opened during Database.close is either closed by it or
    refused with Closed; none is left working on a closed database."""
    for _ in range(20):
        db = ph.open(path, create=True, shards=1)
        mem.Store.create(db, "agents").close() if "mem_agents" not in db.tables() else None
        opened, errors = [], []
        go = threading.Event()

        def opener():
            go.wait()
            for _ in range(50):
                try:
                    opened.append(mem.Store.open(db, "agents", model=None))
                except ph.Closed:
                    errors.append(1)
                    return

        t = threading.Thread(target=opener)
        t.start()
        go.set()
        db.close()
        t.join()
        for s in opened:
            with pytest.raises(ph.Closed):
                s.recall("n", text="x")
