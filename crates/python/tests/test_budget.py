"""D308's byte-budgeted reads through the bindings (#1308): scan and multi-get pages against
a model of D308's size rule, resumed chains, positions' bytes, limit against the budget, a
traversal's byte budget, and nearest_within."""

import random

import pytest

import pigeonhole as ph

OVERHEAD = 16


def cells(rows):
    """A read's cells, in order: (row key, family, qualifier, value)."""
    return [(r.key, f, q, c.value) for r in rows for f, q, c in r]


def model_page(flat, budget):
    """D308's page by hand over a read's cells (as `cells` lists them): the longest prefix
    of columns whose sizes fit (qualifier, value and 16 each, and a row's key once), and the
    size of the first that didn't (its key included)."""
    out, used = [], 0
    for key, f, q, v in flat:
        size = len(q) + len(v) + OVERHEAD
        first = not out or out[-1][0] != key
        need = size + (len(key) if first else 0)
        if used + need > budget:
            return out, used, size + len(key)
        out.append((key, f, q, v))
        used += need
    return out, used, None


@pytest.fixture
def docs(db):
    t = db.table("docs").family("a").family("b").create()
    rng = random.Random(1308)
    for i in range(60):
        m = t.mutate(f"doc:{i:03}")
        for q in range(1 + rng.randrange(4)):
            m = m.put(rng.choice("ab"), f"q{q}", bytes(rng.randrange(1, 300)))
        m.commit()
    return t


def test_scan_pages_follow_the_size_rule_and_chain_to_the_whole_read(docs):
    """Each page equals the model's prefix of what's left; resumed after each page in turn,
    the pages concatenate to the whole scan, exactly once."""
    whole = cells(docs.scan(b"doc:"))
    for budget in [0, 40, 100, 333, 1000, 5000, 1 << 20]:
        got, after = [], None
        while True:
            page = docs.scan_within(budget, b"doc:", after=after)
            want, used, nxt = model_page(whole[len(got) :], budget)
            assert cells(page.rows) == want, budget
            assert page.bytes == used <= budget
            assert page.cut_by_budget == (nxt is not None)
            assert page.next_bytes == nxt
            got += cells(page.rows)
            # An empty page cut by the budget would resume at its own start, forever.
            if page.resume is None or not page.rows:
                break
            after = page.resume
        if budget >= 1000:
            assert got == whole


def test_an_empty_page_says_what_the_next_column_needs(docs):
    page = docs.scan_within(10, b"doc:")
    assert page.rows == [] and page.bytes == 0 and page.cut_by_budget
    first = next(iter(docs.scan(b"doc:")))
    _, q, c = next(iter(first))
    assert page.next_bytes == len(q) + len(c.value) + OVERHEAD + len(first.key)
    # The page resumes at its own start: with next_bytes, the read returns that column.
    again = docs.scan_within(page.next_bytes, b"doc:", after=page.resume)
    assert len(cells(again.rows)) == 1


def test_limit_and_the_budget_stop_the_read_whichever_comes_first(docs):
    page = docs.scan_within(1 << 20, b"doc:", limit=3)
    assert len(page.rows) == 3 and not page.cut_by_budget
    assert page.resume is not None
    assert page.resume.row == page.rows[-1].key
    small = docs.scan_within(50, b"doc:", limit=3)
    assert small.cut_by_budget and len(small.rows) <= 1


def test_multi_get_pages_resume_by_key_even_when_the_keys_change(docs):
    keys = [f"doc:{i:03}" for i in range(0, 60, 3)] + ["doc:zzz"]
    whole = cells(r for r in docs.multi_get(keys) if r)
    got, after = [], None
    while True:
        page = docs.multi_get_within(keys, 500, after=after)
        assert page.bytes <= 500
        got += cells(page.rows)
        if page.resume is None:
            break
        after = page.resume
    assert got == whole
    # A set rebuilt between pages resumes at the first key at or after the position's.
    first = docs.multi_get_within(keys, 1500)
    at = first.resume.key
    assert b"doc:001" < at < b"doc:zzz"
    # Keys dropped and one added before the position: the next page starts at the position
    # (inside its row when the page cut it, after it when the page ended on it).
    later = [k for k in keys if k.encode() >= at][1:] + ["doc:001", at.decode()]
    nxt = docs.multi_get_within(later, 1 << 20, after=first.resume)
    assert nxt.rows[0].key >= at
    assert b"doc:001" not in [r.key for r in nxt.rows]
    assert cells(first.rows) + cells(nxt.rows) == whole


def test_positions_round_trip_and_are_refused_across_kinds_and_tables(db, docs):
    sp = docs.scan_within(300, b"doc:").resume
    kp = docs.multi_get_within(["doc:000", "doc:001", "doc:002"], 200).resume
    assert sp is not None and kp is not None
    assert ph.ScanPosition.from_bytes(sp.to_bytes()) == sp
    assert hash(ph.ScanPosition.from_bytes(sp.to_bytes())) == hash(sp)
    assert ph.KeysPosition.from_bytes(kp.to_bytes()) == kp
    with pytest.raises(ph.InvalidArgument):
        ph.ScanPosition.from_bytes(kp.to_bytes())
    with pytest.raises(ph.InvalidArgument):
        ph.KeysPosition.from_bytes(sp.to_bytes())
    with pytest.raises(ph.InvalidArgument):
        ph.ScanPosition.from_bytes(b"not a position")
    other = db.table("other").family("a").create()
    other.put(b"doc:000", "a", b"q", b"v")
    with pytest.raises(ph.InvalidArgument):
        other.scan_within(1000, after=sp)
    with pytest.raises(ph.InvalidArgument):
        other.multi_get_within(["doc:000"], 1000, after=kp)


def test_a_traversal_stops_at_its_byte_budget(db):
    g = db.table("g").family("out").create()
    for b in "bcdefgh":
        g.mutate("n:a").put("out", f"n:{b}", b"").commit()
    full = g.traverse([b"n:a"], "out")
    assert not full.cut_by_budget and len(full.hops[0]) == 7
    # Each node counts its 3-byte key and 16: 3 fit in 60 bytes.
    cut = g.traverse([b"n:a"], "out", byte_budget=60)
    assert cut.cut_by_budget
    assert cut.hops[0].nodes == full.hops[0].nodes[:3]


def test_nearest_within_keeps_the_nearest_that_fit(db):
    v = (
        db.table("v")
        .family("e", ph.Family.vector(2, ph.Element.F32, ph.Metric.L2))
        .create()
    )
    for i in range(10):
        v.mutate(f"r{i}").put_vector("e", b"a", [float(i), 0.0]).commit()
    near = v.nearest("e", [0.0, 0.0], 5)
    # Each neighbour is a 2-byte key, a 1-byte qualifier and 16: 3 fit in 60.
    page = v.nearest_within("e", [0.0, 0.0], 5, 60)
    assert page.neighbours == near[:3]
    assert (page.bytes, page.cut_by_budget, page.next_bytes) == (57, True, 19)
    whole = v.nearest_within("e", [0.0, 0.0], 5, 1 << 20)
    assert whole.neighbours == near and not whole.cut_by_budget and whole.next_bytes is None
