"""Mirrors crates/pigeonhole/tests/delete_markers.rs: deletes hide cells in every source
(memtable and flushed files alike)."""

import pigeonhole as ph


def test_a_get_never_sees_a_row_deleted_before_it_started(db):
    """delete_markers.rs: a_get_never_sees_a_row_deleted_before_it_started"""
    t = db.table("t").family("f").family("g").create()
    t.mutate(b"r").put("f", b"q", b"v").put("g", b"q", b"v").commit()
    db.flush()
    t.delete(b"r")
    assert t.get(b"r", "f", b"q") is None
    assert t.row(b"r") is None


def test_a_get_never_sees_a_family_deleted_before_it_started(db):
    """delete_markers.rs: a_get_never_sees_a_family_deleted_before_it_started"""
    t = db.table("t").family("f").family("g").create()
    t.mutate(b"r").put("f", b"q", b"v").put("g", b"q", b"v").commit()
    t.delete(b"r", "f")
    db.flush()
    assert t.get(b"r", "f", b"q") is None
    assert t.get(b"r", "g", b"q").value == b"v"


def test_markers_in_other_sources_still_hide_cells(db):
    """delete_markers.rs: markers_in_other_sources_still_hide_cells"""
    t = db.table("t").family("f").create()
    t.put(b"r", "f", b"q", b"v", ts=10)
    db.flush()
    t.delete(b"r", "f", b"q")
    db.flush()
    t.put(b"r", "f", b"other", b"w")
    db.compact()
    assert [q for _, q, _ in t.row(b"r")] == [b"other"]
