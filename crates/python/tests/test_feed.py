"""The change feed (D310, #1322) through the bindings: whole commits in order, typed changes,
resuming from a token across a reopen, expiry, and waiting with the GIL released. Mirrors
crates/pigeonhole/tests/feed.rs at the API's surface."""

import threading
import time

import pytest

import pigeonhole as ph

FEED = 16 << 20


@pytest.fixture
def fdb(path):
    d = ph.open(path, create=True, shards=1, change_feed=FEED)
    yield d
    d.close()


@pytest.fixture
def t(fdb):
    return fdb.table("t").family("f").family("n", ph.Family.aggregate("sum")).create()


def test_changes_arrive_typed_and_in_commit_order(fdb, t):
    feed = t.feed()
    t.put(b"r", "f", b"q", b"v")
    t.incr(b"r", "n", b"hits", 3)
    t.mutate(b"r").delete("f", b"q", ts=5).delete("f", b"q").delete("f").commit()
    put = feed.next(timeout=5)
    assert len(put) == 1
    (c,) = put.changes
    assert (c.table, c.family, c.kind, c.row, c.qualifier, c.value) == ("t", "f", "put", b"r", b"q", b"v")
    assert c.timestamp > 0 and put.timestamp > 0
    merge = feed.next(timeout=5)
    assert merge.seqno > put.seqno
    assert (merge.changes[0].kind, merge.changes[0].value) == ("merge", 3)
    deletes = feed.next(timeout=5)
    assert [ch.kind for ch in deletes.changes] == ["delete_cell", "delete_column", "delete_family"]
    assert all(ch.value is None for ch in deletes.changes)
    assert deletes.changes[2].qualifier == b""
    assert feed.try_next() is None
    assert feed.next(timeout=0.05) is None


def test_a_batch_across_tables_is_one_commit(fdb, t):
    u = fdb.table("u").family("f").create()
    feed = t.feed(tables=[u])
    fdb.write_batch().put(t, b"a", "f", b"q", b"1").put(u, b"b", "f", b"q", b"2").commit()
    c = feed.next(timeout=5)
    assert sorted((ch.table, ch.row) for ch in c.changes) == [("t", b"a"), ("u", b"b")]


def test_a_prefix_and_a_snapshot_start(fdb, t):
    t.put(b"user:1", "f", b"q", b"seen by the snapshot")
    snap = fdb.snapshot()
    t.put(b"other", "f", b"q", b"not under the prefix")
    t.put(b"user:2", "f", b"q", b"after the snapshot")
    feed = t.feed(prefix=b"user:", from_snapshot=snap)
    c = feed.next(timeout=5)
    assert [ch.row for ch in c.changes] == [b"user:2"]
    assert feed.try_next() is None
    with pytest.raises(ValueError):
        t.feed(from_snapshot=snap, from_oldest=True)


def test_a_token_resumes_after_a_reopen_and_a_feed_off_open_expires_it(path):
    db = ph.open(path, create=True, shards=1, change_feed=FEED)
    t = db.table("t").family("f").create()
    feed = t.feed()
    t.put(b"a", "f", b"q", b"1")
    assert feed.next(timeout=5).changes[0].row == b"a"
    token = feed.token()
    assert isinstance(token, bytes)
    t.put(b"b", "f", b"q", b"2")
    feed.close()
    db.close()

    db = ph.open(path, shards=1, change_feed=FEED)
    t = db.table("t").open()
    feed = t.feed(resume=token)
    assert [c.changes[0].row for c in feed.iter(timeout=0.5)] == [b"b"]
    later = feed.token()
    feed.close()
    db.close()

    # An open that doesn't mention the feed inherits it: the token still resumes.
    ph.open(path, shards=1).close()
    db = ph.open(path, shards=1, change_feed=FEED)
    t = db.table("t").open()
    with t.feed(resume=later) as feed:
        assert feed.try_next() is None
    db.close()

    # An open that turns the feed off ends its history: the token expired.
    ph.open(path, shards=1, change_feed_off=True).close()
    db = ph.open(path, shards=1, change_feed=FEED)
    t = db.table("t").open()
    feed = t.feed(resume=later)
    with pytest.raises(ph.FeedExpired) as e:
        feed.next(timeout=5)
    assert e.value.code_num == 31
    with pytest.raises(ph.FeedExpired):
        feed.try_next()
    feed.close()
    db.close()


def test_refusals(tmp_path, t):
    plain_db = ph.open(tmp_path / "plain.phdb", create=True, shards=1)
    plain = plain_db.table("p").family("f").create()
    with pytest.raises(ph.InvalidArgument):
        plain.feed()  # this database's feed is off
    plain_db.close()
    with pytest.raises(ValueError):
        ph.open(tmp_path / "age.phdb", create=True, feed_age=1)  # an age needs a size
    with pytest.raises(ph.InvalidArgument):
        t.feed(resume=b"not a token")
    feed = t.feed()
    feed.close()
    with pytest.raises(ph.Closed):
        feed.try_next()


def test_waiting_releases_the_gil(fdb, t):
    feed = t.feed()
    got = []

    def consume():
        got.append(feed.next(timeout=10))

    waiter = threading.Thread(target=consume)
    waiter.start()
    # The writer thread runs (and commits) while the consumer waits in next().
    time.sleep(0.2)
    t.put(b"w", "f", b"q", b"v")
    waiter.join(timeout=10)
    assert not waiter.is_alive()
    assert got[0].changes[0].row == b"w"


def test_iterating_waits_for_each_commit(fdb, t):
    feed = t.feed()

    def write():
        for i in range(3):
            time.sleep(0.05)
            t.put(f"r{i}", "f", b"q", b"v")

    writer = threading.Thread(target=write)
    writer.start()
    rows = []
    for commit in feed:
        rows.append(commit.changes[0].row)
        if len(rows) == 3:
            break
    writer.join()
    assert rows == [b"r0", b"r1", b"r2"]


def test_a_reader_process_follows_the_writers_feed(path, fdb, t):
    # D310 PR 4 (#1361): a reader's table subscribes to the running writer's feed.
    with ph.open_reader(path) as r:
        rt = r.table("t")
        with rt.feed() as feed:
            t.put(b"seen by the reader", "f", b"q", b"v")
            c = feed.next(timeout=5)
            assert c is not None and c.changes[0].row == b"seen by the reader"
            token = feed.token()
        t.put(b"after the token", "f", b"q", b"v")
        with rt.feed(resume=token) as feed:
            assert feed.next(timeout=5).changes[0].row == b"after the token"
