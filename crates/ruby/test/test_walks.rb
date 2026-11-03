# frozen_string_literal: true

# Mirrors crates/python/tests/test_walks.py: multi-get (key_reads.rs), traversal
# (traverse.rs) and walking a wide row in pages from a position (row_walk.rs).
require_relative "test_helper"

class TestWalks < Minitest::Test
  include PigeonholeTest

  def g
    @g ||= begin
      g = db.table("g").family("out").family("props").create
      [%w[a b], %w[a c], %w[b d], %w[c d], %w[d e], %w[d a]].each do |a, b|
        g.mutate("n:#{a}").put("out", "n:#{b}", "").commit
      end
      g.put("n:a", "props", "name", "A")
      g
    end
  end

  # key_reads.rs's rows, aligned with the keys as given: one entry per key, nil for a miss,
  # a repeated key repeated.
  def test_multi_get_aligns_its_rows_with_the_keys_given
    rows = g.multi_get(["n:d", "n:zz", "n:a", "n:a"], families: ["out"])
    assert_equal ["n:d", nil, "n:a", "n:a"], rows.map { |r| r&.key }
    assert_equal %w[n:b n:c], rows[2].map { |_, q, _| q }
    assert_equal "A", g.multi_get(["n:a"], families: [:props])[0].get("props", "name").value
    assert_equal [], g.multi_get([])
    assert_equal ["n:a"], g.multi_get(Set["n:a"]).map(&:key)
    assert_bad_call(TypeError) { g.multi_get(["n:a", 1]) }
    snap = db.snapshot
    g.put("n:new", "out", "n:a", "")
    assert_equal [nil], g.multi_get(["n:new"], snapshot: snap)
    snap.release
  end

  # traverse.rs: hops in order, each sorted, parents indexing the previous hop.
  def test_traverse_takes_hops_with_parents
    t = g.traverse(["n:a"], "out", hops: 3, parents: true)
    assert_equal [%w[n:b n:c], ["n:d"], ["n:e"]], t.hops.map(&:nodes)
    assert_equal ["n:a"], t.seeds
    assert_equal [0, 0], t.hops[0].parents
    # e's parent is d (index 0 of hop 2), d's is b (index 0 of hop 1).
    assert_equal [[0], [0]], [t.hops[2].parents, t.hops[1].parents]
    assert_operator t.edges, :>=, 5
    refute t.cut_by_max_nodes
    assert t.frozen? && t.hops.frozen? && t.hops[0].frozen?
    # simp724's N1 on #1401: the Strings inside are frozen too, so `==` and `hash` hold.
    assert(t.seeds.all?(&:frozen?) && t.hops[0].nodes.all?(&:frozen?))
    assert_equal 2, t.hops[0].size
    assert_nil g.traverse(["n:a"], "out").hops[0].parents
    assert_equal ["n:b"], g.traverse(["n:a"], "out", per_node_limit: 1).hops[0].nodes
    assert g.traverse(["n:a"], :out, hops: 3, max_nodes: 2).cut_by_max_nodes
    # Seeds come back sorted and deduplicated.
    assert_equal %w[n:a n:b], g.traverse(%w[n:b n:a n:b], "out").seeds
  end

  # row_walk.rs: pages of whole columns; a position resumes in a later walk.
  def test_pages_walk_a_wide_row_and_resume_from_a_position
    t = db.table("w").family("e").create
    m = t.mutate("hub")
    25.times { |i| m.put("e", format("%03d", i), i) }
    m.commit
    walk = t.pages("hub", 10)
    assert_nil walk.position
    first = walk.first
    assert_equal 10, first.size
    token = walk.position.to_bytes
    assert_equal Encoding::BINARY, token.encoding
    rest = t.pages("hub", 10, after: Pigeonhole::RowPosition.from_bytes(token)).to_a
    assert_equal [10, 5], rest.map(&:size)
    qualifiers = [first, *rest].flat_map { |p| p.map { |_, q, _| q } }
    assert_equal (0...25).map { |i| format("%03d", i) }, qualifiers
    assert_equal walk.position, Pigeonhole::RowPosition.from_bytes(token)
    assert_equal 1, [Pigeonhole::RowPosition.from_bytes(token), walk.position].uniq.size
    # The walk continues where it stopped: what's left after the first page.
    assert_equal [10, 5], walk.map(&:size)
    assert_equal [], walk.to_a
    db.snapshot do |s|
      t.put("hub", "e", "zzz", 1)
      assert_equal 25, t.pages("hub", 7, snapshot: s).sum(&:size)
    end
    assert_equal 26, t.pages("hub", 7).sum(&:size)
    assert_raises(ArgumentError) { t.pages("hub", 0) }
    assert_raises(Pigeonhole::InvalidArgument) { Pigeonhole::RowPosition.from_bytes("nope") }
    # External enumeration reads one page at a time. (`to_enum`, not a blockless `each`:
    # `rbs test` checks an Enumerator a hooked method returns by iterating it, which would
    # use up this one-shot walk.)
    e = t.pages("hub", 20).to_enum
    assert_equal 20, e.next.size
    assert_equal 6, e.next.size
    assert_raises(StopIteration) { e.next }
  end
end
