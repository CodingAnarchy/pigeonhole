# frozen_string_literal: true

# simp724's R3 on #697: a scan Enumerator reads every chunk of one enumeration at one
# snapshot, however lazily it is consumed; a new enumeration (`rewind`, another `each`)
# takes a new one; a given `snapshot:` is used as it is.
require_relative "test_helper"

class TestScanSnapshot < Minitest::Test
  include PigeonholeTest

  def setup
    super
    @t = db.table("t").family("f").create
    10.times { |i| @t.put(format("k%02d", i), "f", "q", "v1") }
  end

  def test_external_enumeration_sees_one_snapshot_across_chunks
    e = @t.scan(chunk: 2)
    first = e.next
    assert_equal "k00", first.key
    # Writes mid-enumeration: new rows, changed rows and a deleted row.
    @t.put("k05", "f", "q", "v2")
    @t.put("k99", "f", "q", "v2")
    @t.delete("k07")
    rest = []
    loop { rest << e.next }
    keys = [first, *rest].map(&:key)
    assert_equal (0...10).map { |i| format("k%02d", i) }, keys
    assert(rest.all? { |r| r.get("f", "q").value == "v1" })
    # A new enumeration takes a new snapshot.
    e.rewind
    keys = []
    values = {}
    loop do
      r = e.next
      keys << r.key
      values[r.key] = r.get("f", "q").value
    end
    assert_includes keys, "k99"
    refute_includes keys, "k07"
    assert_equal "v2", values["k05"]
  end

  def test_internal_enumeration_sees_one_snapshot_and_each_call_a_new_one
    e = @t.scan(chunk: 3)
    seen = []
    e.each do |r|
      @t.put("k#{r.key}", "f", "q", "new") if seen.empty? # a key past the scan's end
      seen << r.key
    end
    assert_equal 10, seen.size
    assert_equal 11, e.count
  end

  def test_a_given_snapshot_is_used
    s = db.snapshot
    e = @t.scan(snapshot: s, chunk: 4)
    @t.put("k50", "f", "q", "later")
    assert_equal 10, e.count
    assert_equal 10, e.count # every enumeration: the given snapshot
    s.release
    assert_raises(Pigeonhole::SnapshotExpired) { e.first }
  end

  def test_lazy_enumeration_stops_early_and_releases
    e = @t.scan(chunk: 1)
    assert_equal %w[k00 k01], e.lazy.map(&:key).first(2)
    db.close
    @db = nil
    assert_raises(Pigeonhole::Closed) { e.first }
  end
end
