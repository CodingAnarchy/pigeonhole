"""The 1.1 reads through the bindings: multi-get (mirrors key_reads.rs), traversal (mirrors
traverse.rs), walking a wide row in pages from a position (mirrors row_walk.rs), and closing
a scan."""

import pytest

import pigeonhole as ph


@pytest.fixture
def g(db):
    g = db.table("g").family("out").family("props").create()
    for a, b in [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e"), ("d", "a")]:
        g.mutate(f"n:{a}").put("out", f"n:{b}", b"").commit()
    g.put(b"n:a", "props", b"name", "A")
    return g


def test_multi_get_aligns_its_rows_with_the_keys_given(g):
    """key_reads.rs's rows, aligned with the keys as given (cli692's S1 on #1276): one entry
    per key, None for a miss, a repeated key repeated."""
    rows = g.multi_get((b"n:d", b"n:zz", "n:a", b"n:a"), families=["out"])
    assert [r.key if r else None for r in rows] == [b"n:d", None, b"n:a", b"n:a"]
    assert [q for _, q, _ in rows[2]] == [b"n:b", b"n:c"]
    assert g.multi_get([b"n:a"], families=["props"])[0].get("props", "name").value == b"A"
    assert g.multi_get([]) == []


def test_traverse_takes_hops_with_parents(g):
    """traverse.rs: hops in order, each sorted, parents indexing the previous hop."""
    t = g.traverse([b"n:a"], "out", hops=3, parents=True)
    assert [h.nodes for h in t.hops] == [[b"n:b", b"n:c"], [b"n:d"], [b"n:e"]]
    assert t.seeds == [b"n:a"]
    assert t.hops[0].parents == [0, 0]
    # e's parent is d (index 0 of hop 2), d's is b (index 0 of hop 1).
    assert (t.hops[2].parents, t.hops[1].parents) == ([0], [0])
    assert t.edges >= 5 and not t.cut_by_max_nodes
    assert g.traverse([b"n:a"], "out").hops[0].parents is None
    capped = g.traverse([b"n:a"], "out", per_node_limit=1)
    assert capped.hops[0].nodes == [b"n:b"]
    limited = g.traverse([b"n:a"], "out", hops=3, max_nodes=2)
    assert limited.cut_by_max_nodes


def test_pages_walk_a_wide_row_and_resume_from_a_position(db):
    """row_walk.rs: pages of whole columns; a position resumes in a later walk."""
    t = db.table("w").family("e").create()
    m = t.mutate(b"hub")
    for i in range(25):
        m.put("e", f"{i:03}", i)
    m.commit()
    walk = t.pages(b"hub", 10)
    assert walk.position is None
    first = next(walk)
    assert len(first) == 10
    token = walk.position.to_bytes()
    rest = list(t.pages(b"hub", 10, after=ph.RowPosition.from_bytes(token)))
    assert [len(p) for p in rest] == [10, 5]
    qualifiers = [q for p in [first, *rest] for _, q, _ in p]
    assert qualifiers == [f"{i:03}".encode() for i in range(25)]
    assert ph.RowPosition.from_bytes(token) == walk.position
    assert len({ph.RowPosition.from_bytes(token), walk.position}) == 1
    with db.snapshot() as s:
        t.put(b"hub", "e", b"zzz", 1)
        assert sum(len(p) for p in t.pages(b"hub", 7, snapshot=s)) == 25
    with pytest.raises(ValueError):
        t.pages(b"hub", 0)


def test_a_scan_closes_early_and_as_a_context_manager(db):
    """ScanIter.close() releases the scan; a closed database fails the next read."""
    t = db.table("t").family("f").create()
    with db.write_batch() as wb:
        for i in range(100):
            wb.put(t, f"r{i:03}", "f", b"q", i)
    it = t.scan(chunk=4)
    next(it)
    it.close()
    it.close()
    assert list(it) == []
    with t.scan() as it:
        assert next(it).key == b"r000"
    assert list(it) == []


def test_a_live_scan_after_the_database_closes_raises_closed(path):
    """Database.close() with a live iterator: nothing hangs; once the rows it had read are
    handed out, the next read raises Closed, as a Rust RowIter's does."""
    db = ph.open(path, create=True, shards=1)
    t = db.table("t").family("f").create()
    with db.write_batch() as wb:
        for i in range(100):
            wb.put(t, f"r{i:03}", "f", b"q", i)
    it = t.scan(chunk=4)
    next(it)
    db.close()
    with pytest.raises(ph.Closed):
        for _ in it:
            pass
