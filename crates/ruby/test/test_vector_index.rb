# frozen_string_literal: true

# The vector index (D306, ICR 0100; #697 PR 3b): inserts, searches against a brute force,
# the element forms, writes in a transaction with Placed and after_commit, prefixed
# searches, bulk loads, readers and a Ruby-driven Rebalancer. Mirrors crates/vector/tests.
require_relative "test_helper"

class TestVectorIndex < Minitest::Test
  include PigeonholeTest

  V = Pigeonhole::Vector

  def spec(dim = 4, element = :f32, metric = :l2) = Pigeonhole::VectorSpec.new(dim, element, metric)

  def l2(a, b) = Math.sqrt(a.zip(b).sum { |x, y| (x - y)**2 })

  def f32(v) = v.pack("e*").unpack("e*")

  def test_inserts_and_searches_equal_a_brute_force
    rng = Random.new(697)
    idx = V::Index.create(db, "emb", spec(8), target_list_len: 8)
    vecs = (0...120).to_h { |i| [format("v%03d", i), Array.new(8) { rng.rand(-1.0..1.0) }] }
    vecs.each { |id, v| idx.insert(id, v) }
    q = Array.new(8) { rng.rand(-1.0..1.0) }
    want = vecs.keys.sort_by { |id| [l2(f32(vecs[id]), f32(q)), id] }.first(5)
    hits, stats = idx.search_with_stats(q, 5, nprobe: 1_000)
    assert_equal want, hits.map(&:id)
    assert_equal stats.lists, stats.lists_probed
    assert_operator stats.lists, :>, 1
    assert hits.frozen? && hits[0].frozen? && hits[0].id.frozen?
    assert_equal hits.map(&:distance).sort, hits.map(&:distance)
    v = idx.verify
    assert_equal [120, 120], [v.vectors, v.list_lens.sum]
    assert idx.delete("v000")
    refute idx.delete("v000")
    assert_equal 119, idx.verify.vectors
    assert_equal spec(8), idx.spec
    assert_equal :inline, idx.rebalance
    assert_operator idx.max_list_len, :>=, idx.min_list_len
    reopened = V::Index.open(db, "emb")
    assert_equal (want - ["v000"]).first(4), reopened.search(q, 4, nprobe: 1_000).map(&:id)
  end

  def test_element_forms
    i8 = V::Index.create(db, "q8", spec(3, :i8))
    i8.insert("a", [1, 2, 3])
    i8.insert("b", [-100, 0, 100])
    assert_equal "a", i8.search([1, 2, 2], 1)[0].id
    assert_raises(RangeError) { i8.insert("c", [1, 2, 300]) }
    bits = V::Index.create(db, "sig", spec(16, :bit, :hamming))
    bits.insert("a", "\xFF\x00".b)
    bits.insert("b", "\x00\xFF".b)
    assert_equal "a", bits.search("\xFE\x00".b, 1)[0].id
    assert_bad_call(TypeError) { bits.insert("c", [1, 0]) }
    half = V::Index.create(db, "h16", spec(2, :f16))
    half.insert("a", [1.5, -0.25]) # f32 values; the index rounds them
    assert_equal "a", half.search([1.5, -0.25], 1)[0].id
    assert_raises(Pigeonhole::InvalidArgument) { half.insert("x", [1.0, 2.0, 3.0]) } # dimension
  end

  # ai1249's P1 on #697: a Placed made inside the transaction's block.
  def test_writes_in_a_transaction
    idx = V::Index.create(db, "tx", spec(2), target_list_len: 4)
    docs = db.table("docs").family("f").create
    placed = db.transact do |txn|
      p = V::Placed.new
      txn.put(docs, "d1", "f", "", "text")
      idx.insert_in(txn, p, "d1", [1.0, 0.0])
      idx.insert_in(txn, p, "d1", [0.0, 1.0]) # an upsert of it, in the same transaction
      p
    end
    idx.after_commit(placed)
    assert_raises(Pigeonhole::Error) { idx.after_commit(placed) } # used up
    assert_equal 1, idx.verify.vectors
    assert_in_delta 0.0, idx.search([0.0, 1.0], 1)[0].distance
    txn = db.transaction
    p = V::Placed.new
    assert idx.delete_in(txn, p, "d1")
    refute idx.delete_in(txn, p, "d1") # the record says it's gone
    txn.rollback
    assert_equal 1, idx.verify.vectors
    other = V::Index.create(db, "other", spec(2))
    txn = db.transaction
    p = V::Placed.new
    idx.insert_in(txn, p, "d2", [1.0, 1.0])
    assert_raises(Pigeonhole::InvalidArgument) { other.insert_in(txn, p, "d3", [1.0, 1.0]) }
    txn.commit
    idx.after_commit(p)
    assert_equal 2, idx.verify.vectors
  end

  def test_prefixed_searches_widen
    idx = V::Index.create(db, "pre", spec(2), target_list_len: 4, nprobe: 1)
    rng = Random.new(1412)
    40.times { |i| idx.insert(format("a:%02d", i), [rng.rand, rng.rand]) }
    3.times { |i| idx.insert("b:#{i}", [rng.rand + 10, rng.rand + 10]) }
    hits, stats = idx.search_with_stats([0.5, 0.5], 3, id_prefix: "b:")
    assert_equal %w[b:0 b:1 b:2].sort, hits.map(&:id).sort
    assert_operator stats.lists_probed, :>, 1
    capped, st = idx.search_with_stats([0.5, 0.5], 3, id_prefix: "b:", max_probe: 1)
    assert_operator capped.size, :<, 3
    assert_operator st.lists_probed, :<, st.lists
  end

  # ai1249's P2 on #697: a bulk load streams its pairs a batch at a time.
  def test_bulk_load
    idx = V::Index.create(db, "bulk", spec(4), target_list_len: 16)
    rng = Random.new(306)
    pairs = (0...500).lazy.map { |i| [format("v%04d", i), Array.new(4) { rng.rand }] }
    stats = idx.bulk_load(pairs, expected: 500, batch: 64)
    assert_equal 500, stats.vectors
    assert_operator stats.lists, :>, 1
    assert_equal 500, idx.verify.vectors
    assert_raises(Pigeonhole::InvalidArgument) { idx.bulk_load([["x", [1, 2, 3, 4]]], expected: 1) } # not empty
    fresh = V::Index.create(db, "bulk2", spec(4))
    boom = Class.new(StandardError)
    each_then_raise = Enumerator.new do |y|
      3.times { |i| y << ["v#{i}", [i, 0, 0, 0]] }
      raise boom
    end
    assert_raises(boom) { fresh.bulk_load(each_then_raise, expected: 10, batch: 2) }
    assert_equal 2, fresh.verify.vectors # the batch sent before the failure loaded
    assert_bad_call(TypeError) { V::Index.create(db, "bulk3", spec(4)).bulk_load([[1]], expected: 1) }
  end

  def test_a_reader_searches
    idx = V::Index.create(db, "rd", spec(2))
    idx.insert("a", [1.0, 0.0])
    idx.insert("b", [0.0, 1.0])
    Pigeonhole.open_reader(@path) do |r|
      ri = V::Index.open_reader(r, "rd")
      assert_equal spec(2), ri.spec
      assert_equal "b", ri.search([0.1, 0.9], 1)[0].id
      assert_equal 2, ri.search_with_stats([0.1, 0.9], 2)[0].size
    end
  end

  # ai1249 and perf2's Q3 on #697: background rebalancing driven by a Ruby thread.
  def test_a_ruby_driven_rebalancer
    idx, reb = V::Index.create(db, "bg", spec(2), target_list_len: 4, rebalance: :background, rebalancer: true)
    assert_equal :background, idx.rebalance
    refute_predicate reb, :closed?
    rng = Random.new(1)
    30.times { |i| idx.insert("v#{i}", [rng.rand, rng.rand]) }
    worked = false
    20.times do
      worked |= reb.run_once(0.05)
      break unless reb.run_once(0.05)
    end
    assert worked
    reb.wait(0.05) # nothing marked: returns once the timeout passes
    assert_equal 30, idx.verify.vectors
    assert_operator idx.verify.list_lens.size, :>, 1
    _, again = V::Index.open(db, "bg", rebalancer: true)
    refute_predicate again, :closed?
  end
end
