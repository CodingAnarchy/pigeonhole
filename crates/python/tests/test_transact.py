"""Mirrors crates/pigeonhole/tests/transact.rs, and api.rs's
transactions_commit_or_conflict_sync: commits, conflicts and transact's retries."""

import threading

import pytest

import pigeonhole as ph


@pytest.fixture
def t(db):
    return db.table("t").family("f").family("g").family("n", ph.Family.counter()).create()


def test_transactions_commit_or_conflict(db, t):
    """api.rs: transactions_commit_or_conflict_sync"""
    t.put(b"a", "f", b"v", 1)
    first = db.transaction()
    second = db.transaction()
    first.put(t, b"a", "f", b"v", first.get(t, b"a", "f", b"v").as_int() + 1)
    second.put(t, b"a", "f", b"v", second.get(t, b"a", "f", b"v").as_int() + 10)
    first.commit()
    with pytest.raises(ph.Conflict):
        second.commit()
    assert t.get(b"a", "f", b"v").as_int() == 2
    with pytest.raises(ValueError):
        second.commit()


def test_a_transaction_block_commits_or_rolls_back(db, t):
    """The binding's context manager: commit on a clean exit, nothing on an exception."""
    with db.transaction() as txn:
        txn.put(t, b"r", "f", b"q", b"v")
    assert t.get(b"r", "f", b"q").value == b"v"
    with pytest.raises(RuntimeError):
        with db.transaction() as txn:
            txn.put(t, b"r", "f", b"q", b"w")
            raise RuntimeError
    assert t.get(b"r", "f", b"q").value == b"v"


def test_a_conflicting_attempt_is_retried(db, t):
    """transact.rs: a_conflicting_attempt_is_retried_and_counted"""
    t.put(b"a", "f", b"v", 0)
    attempts = []

    def body(txn):
        attempts.append(1)
        n = txn.get(t, b"a", "f", b"v").as_int()
        if len(attempts) == 1:
            t.put(b"a", "f", b"v", 100)  # a concurrent writer, between the read and commit
        txn.put(t, b"a", "f", b"v", n + 1)
        return n

    assert db.transact(body) == 100
    assert len(attempts) == 2
    assert t.get(b"a", "f", b"v").as_int() == 101


def test_retrying_stops_at_the_attempt_cap(db, t):
    """transact.rs: retrying_stops_at_the_attempt_cap_or_the_deadline"""
    t.put(b"a", "f", b"v", 0)
    attempts = []

    def always_conflicts(txn):
        attempts.append(1)
        txn.get(t, b"a", "f", b"v")
        t.put(b"a", "f", b"v", len(attempts))
        txn.put(t, b"a", "f", b"v", -1)

    with pytest.raises(ph.Conflict):
        db.transact(always_conflicts, max_attempts=3)
    assert len(attempts) == 3


def test_the_applications_own_error_ends_the_call(db, t):
    """transact.rs: the_applications_own_error_ends_the_call"""
    attempts = []

    def fails(txn):
        attempts.append(1)
        txn.put(t, b"a", "f", b"v", 1)
        raise KeyError("mine")

    with pytest.raises(KeyError):
        db.transact(fails)
    assert len(attempts) == 1
    assert t.get(b"a", "f", b"v") is None


def test_concurrent_increments_lose_no_update(db, t):
    """transact.rs: concurrent_increments_lose_no_update (threads: the GIL is released)"""
    t.put(b"a", "f", b"v", 0)

    def bump(txn):
        cell = txn.get(t, b"a", "f", b"v")
        txn.put(t, b"a", "f", b"v", cell.as_int() + 1)

    def worker():
        for _ in range(25):
            db.transact(bump, max_attempts=1000)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert t.get(b"a", "f", b"v").as_int() == 100


def test_get_row_records_a_read_of_each_family(db, t):
    """transact.rs: get_row_records_a_read_of_each_family_it_covers"""
    t.mutate(b"r").put("f", b"x", b"1").put("g", b"y", b"1").commit()
    txn = db.transaction()
    row = txn.get_row(t, b"r", ["f", "g"])
    assert len(row) == 2
    t.put(b"r", "g", b"z", b"2")  # another column of a family the transaction read
    txn.put(t, b"r", "f", b"x", b"3")
    with pytest.raises(ph.Conflict):
        txn.commit()


def test_every_row_mutation_write_is_available_in_a_transaction(db, t):
    """transact.rs: every_row_mutation_write_is_available_in_a_transaction"""
    with db.transaction() as txn:
        txn.put(t, b"r", "f", b"b", b"x").put(t, b"r", "f", b"i", 3).put_u64(t, b"r", "f", b"u", 7)
        txn.incr(t, b"r", "n", b"c", 2).put(t, b"r", "g", b"gone", b"x")
    with db.transaction() as txn:
        txn.delete(t, b"r", "g")
    row = t.row(b"r")
    assert sorted(q for _, q, _ in row) == [b"b", b"c", b"i", b"u"]
