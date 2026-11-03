# frozen_string_literal: true

# Mirrors crates/python/tests/test_delete_markers.py (crates/pigeonhole/tests/delete_markers.rs):
# deletes hide cells in every source (memtable and flushed files alike).
require_relative "test_helper"

class TestDeleteMarkers < Minitest::Test
  include PigeonholeTest

  # delete_markers.rs: a_get_never_sees_a_row_deleted_before_it_started
  def test_a_get_never_sees_a_row_deleted_before_it_started
    t = db.table("t").family("f").family("g").create
    t.mutate("r").put("f", "q", "v").put("g", "q", "v").commit
    db.flush
    t.delete("r")
    assert_nil t.get("r", "f", "q")
    assert_nil t.row("r")
  end

  # delete_markers.rs: a_get_never_sees_a_family_deleted_before_it_started
  def test_a_get_never_sees_a_family_deleted_before_it_started
    t = db.table("t").family("f").family("g").create
    t.mutate("r").put("f", "q", "v").put("g", "q", "v").commit
    t.delete("r", "f")
    db.flush
    assert_nil t.get("r", "f", "q")
    assert_equal "v", t.get("r", "g", "q").value
  end

  # delete_markers.rs: markers_in_other_sources_still_hide_cells
  def test_markers_in_other_sources_still_hide_cells
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "v", ts: 10)
    db.flush
    t.delete("r", "f", "q")
    db.flush
    t.put("r", "f", "other", "w")
    db.compact
    assert_equal ["other"], t.row("r").map { |_, q, _| q }
  end
end
