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

