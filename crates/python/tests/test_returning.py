"""Increments that return the counter's value after the commit (D314, ICR 0113):
`Mutation.commit_returning()` and `Table.incr_returning()`. Each test names its Rust
counterpart where there is one."""

import threading
import time

import pytest

import pigeonhole as ph

HOUR_US = 3_600_000_000


@pytest.fixture
def t(db):
    return (
        db.table("seq")
        .family("n", ph.Family.counter())
        .family("b", ph.Family.counter(ttl=3600))
        .family("f")
        .create()
    )


def test_commit_returning_gives_each_counter_after_the_commit(t):
    t.incr(b"r", "n", b"a", 5)
    now = time.time_ns() // 1000
    r = (
        t.mutate(b"r")
        .incr("n", b"a", 2)
        .incr("n", "c", 1)
        .put("n", b"set", 40)
        .incr("n", b"set", 2)
        .incr("b", b"hits", 3, ts=now)
        .put("f", b"plain", b"not a counter")
        .commit_returning()
    )
    assert isinstance(r, ph.Incremented)
    assert r.value("n", b"a") == 7
    assert r.value("n", "c") == 1
    assert r.value("n", b"set") == 42
    assert r.value("b", b"hits", ts=now) == 3
    assert r["n", b"a"] == 7
    assert r["b", b"hits", now] == 3
    assert ("n", b"a") in r and ("b", b"hits", now) in r
    assert ("f", b"plain") not in r and ("n", b"a", 1) not in r and "n" not in r
    # In the order the mutation first wrote each cell; the plain family's put isn't one.
    assert list(r) == [("n", b"a", 0), ("n", b"c", 0), ("n", b"set", 0), ("b", b"hits", now)]
    assert len(r) == 4
    assert r.cells() == [
        ("n", b"a", 0, 7),
        ("n", b"c", 0, 1),
        ("n", b"set", 0, 42),
        ("b", b"hits", now, 3),
    ]
    assert 0 < r.seqno < t.incr(b"r", "n", b"a", 1)
    assert repr(r) == f"Incremented(seqno={r.seqno}, cells=4)"


def test_a_cell_the_commit_did_not_write_is_refused(t):
    r = t.mutate(b"r").incr("n", b"a", 1).commit_returning()
    with pytest.raises(ph.InvalidArgument):
        r.value("n", b"other")
    with pytest.raises(ph.InvalidArgument):
        r.value("n", b"a", ts=7)
    with pytest.raises(ph.InvalidArgument):
        r.value("nope", b"a")
    with pytest.raises(KeyError):
        r["n", b"other"]
    for key in ["n", ("n",), ("n", b"a", 0, 1), ("n", b"a", "ts")]:
        with pytest.raises(TypeError):
            r[key]


def test_a_mutation_commits_once(t):
    m = t.mutate(b"r").incr("n", b"a", 1)
    assert m.commit_returning().value("n", b"a") == 1
    with pytest.raises(ValueError, match="already committed"):
        m.commit_returning()
    with pytest.raises(ValueError, match="already committed"):
        m.commit()


def test_a_mutation_without_counters_returns_none_of_them(t):
    r = t.mutate(b"r").put("f", b"x", b"1").commit_returning()
    assert len(r) == 0 and list(r) == [] and r.cells() == []
    assert t.get(b"r", "f", b"x").value == b"1"


def test_a_counter_the_mutation_deleted_reads_as_none(t):
    t.incr(b"r", "n", b"gone", 3)
    r = t.mutate(b"r").incr("n", b"a", 1).delete("n", b"gone", ts=0).commit_returning()
    assert r.value("n", b"gone") is None
    assert r.cells() == [("n", b"a", 0, 1), ("n", b"gone", 0, None)]
    assert t.get(b"r", "n", b"gone") is None


def test_incr_returning_counts_and_returns_the_value(t):
    """write.rs: commit_returning's doc example"""
    assert t.incr_returning(b"orders", "n", b"next", 1) == 1
    assert t.incr_returning(b"orders", "n", b"next", 1, durability=ph.Durability.SYNC) == 2
    assert t.incr_returning(b"orders", "n", b"next", -5) == -3
    assert t.get(b"orders", "n", b"next").as_int() == -3


def test_incr_returning_a_bucket(t):
    now = time.time_ns() // 1000
    assert t.incr_returning(b"r", "b", b"hits", 2, ts=now) == 2
    assert t.incr_returning(b"r", "b", b"hits", 2, ts=now) == 4
    # A bucket already past the family's TTL (an hour) reads as nothing.
    assert t.incr_returning(b"r", "b", b"hits", 1, ts=now - 2 * HOUR_US) is None


def test_concurrent_incr_returning_hands_out_each_value_once(db, t):
    """D314: N callers adding 1 from zero get exactly 1..=N (threads: the GIL is released)"""
    got = []
    lock = threading.Lock()

    def worker():
        mine = [t.incr_returning(b"orders", "n", b"next", 1) for _ in range(50)]
        with lock:
            got.extend(mine)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert sorted(got) == list(range(1, 201))



def test_a_failed_commit_and_a_failed_read_after_it_raise_apart(db, t):
    """perf2's S1 on #1473: a read that fails after the commit applied raises
    ReturnedValueError (the commit landed: don't retry), never the commit's own classes. The
    bindings can't make that read fail, so this pins the split from the other side: commit
    failures and unwritten cells keep their usual classes."""
    assert issubclass(ph.ReturnedValueError, ph.Error)
    assert not issubclass(ph.ReturnedValueError, (ph.Io, ph.InvalidArgument, ph.Closed))
    # A commit the engine refuses: a TTL counter family takes only buckets.
    with pytest.raises(ph.InvalidArgument) as e:
        t.incr_returning(b"r", "b", b"hits", 1)
    assert not isinstance(e.value, ph.ReturnedValueError)
    # A cell the commit didn't write is the caller's mistake, not a failed read.
    r = t.mutate(b"r").incr("n", b"a", 1).commit_returning()
    with pytest.raises(ph.InvalidArgument) as e:
        r.value("n", b"other")
    assert not isinstance(e.value, ph.ReturnedValueError)
    # A commit on a closed database.
    db.close()
    with pytest.raises(ph.Closed) as e:
        t.incr_returning(b"r", "n", b"a", 1)
    assert not isinstance(e.value, ph.ReturnedValueError)


# ---- return_previous (#1445, ICR 0113 amendment) ----


@pytest.fixture
def leases(db):
    return db.table("leases").family("s").family("n", ph.Family.counter()).create()


def test_a_lease_handoff_returns_the_holder_it_replaced(leases):
    """The guide's example: one call takes the lease and says who held it."""
    leases.mutate(b"lease:42").put("s", b"holder", b"worker-3").commit()
    r = (
        leases.mutate(b"lease:42")
        .put("s", b"holder", b"worker-7")
        .return_previous("s", b"holder")
        .commit_returning()
    )
    old = r.previous("s", b"holder")
    assert isinstance(old, ph.Cell) and old.value == b"worker-3"
    assert leases.get(b"lease:42", "s", b"holder").value == b"worker-7"
    # A column with nothing before the commit.
    r = (
        leases.mutate(b"lease:43")
        .put("s", "holder", b"worker-1")
        .return_previous("s", "holder")
        .commit_returning()
    )
    assert r.previous("s", b"holder") is None


def test_previous_values_sit_beside_the_counters(leases):
    leases.mutate(b"r").put("s", b"state", b"packed").incr("n", b"v", 4).commit()
    r = (
        leases.mutate(b"r")
        .put("s", b"state", b"shipped")
        .incr("n", b"v", 1)
        .return_previous("s", b"state")
        .return_previous("n", b"v")
        .return_previous("s", b"state")  # a repeat is one request
        .commit_returning()
    )
    assert r.value("n", b"v") == 5
    assert r.previous("s", b"state").value == b"packed"
    assert r.previous("n", b"v").as_int() == 4
    assert len(r) == 1  # the counters only: previous values aren't keys


def test_return_previous_is_refused_outside_commit_returning(leases):
    condition = ph.Condition.absent("s", b"x")
    for commit in [lambda m: m.commit(), lambda m: m.commit_if(condition)]:
        m = leases.mutate(b"r").put("s", b"x", b"1").return_previous("s", b"x")
        with pytest.raises(ph.InvalidArgument):
            commit(m)
    assert leases.get(b"r", "s", b"x") is None, "nothing was written"
    # A mutation that writes nothing.
    with pytest.raises(ph.InvalidArgument):
        leases.mutate(b"r").return_previous("s", b"x").commit_returning()
    # A column the mutation didn't ask for is the caller's mistake, not a failed read.
    r = leases.mutate(b"r").put("s", b"x", b"1").return_previous("s", b"x").commit_returning()
    with pytest.raises(ph.InvalidArgument) as e:
        r.previous("s", b"other")
    assert not isinstance(e.value, ph.ReturnedValueError)
    # Asking after the commit.
    m = leases.mutate(b"r").put("s", b"x", b"2")
    m.commit_returning()
    with pytest.raises(ValueError, match="already committed"):
        m.return_previous("s", b"x")


def test_returned_is_incremented(leases):
    assert ph.Returned is ph.Incremented
    r = leases.mutate(b"r").incr("n", b"v", 1).commit_returning()
    assert isinstance(r, ph.Returned)


def test_return_previous_works_on_the_async_form(leases):
    import asyncio

    leases.mutate(b"lease:9").put("s", b"holder", b"a").commit()

    async def go():
        return await (
            leases.mutate(b"lease:9")
            .put("s", b"holder", b"b")
            .return_previous("s", b"holder")
            .commit_returning_async()
        )

    r = asyncio.run(go())
    assert r.previous("s", b"holder").value == b"a"
