# frozen_string_literal: true

# Mirrors crates/python/tests/test_returning.py: increments that return the counter's value
# after the commit (D314, ICR 0113), Mutation#commit_returning and Table#incr_returning.
require_relative "test_helper"

class TestReturning < Minitest::Test
  include PigeonholeTest

  HOUR_US = 3_600_000_000

  def t
    @t ||= db.table("seq")
             .family("n", Pigeonhole::Family.counter)
             .family("b", Pigeonhole::Family.counter(ttl: 3600))
             .family("f")
             .create
  end

  def now_us = Process.clock_gettime(Process::CLOCK_REALTIME, :microsecond)

  def test_commit_returning_gives_each_counter_after_the_commit
    t.incr("r", "n", "a", 5)
    now = now_us
    r = t.mutate("r")
         .incr("n", "a", 2)
         .incr(:n, "c", 1)
         .put("n", "set", 40)
         .incr("n", "set", 2)
         .incr("b", "hits", 3, ts: now)
         .put("f", "plain", "not a counter")
         .commit_returning
    assert_instance_of Pigeonhole::Incremented, r
    assert_predicate r, :frozen?
    assert_equal 7, r.value("n", "a")
    assert_equal 1, r.value(:n, "c")
    assert_equal 42, r.value("n", "set")
    assert_equal 3, r.value("b", "hits", ts: now)
    # In the order the mutation first wrote each cell; the plain family's put isn't one.
    h = r.to_h
    assert_equal({ ["n", "a", 0] => 7, ["n", "c", 0] => 1, ["n", "set", 0] => 42, ["b", "hits", now] => 3 }, h)
    assert_equal [["n", "a", 0], ["n", "c", 0], ["n", "set", 0], ["b", "hits", now]], h.keys
    assert(h.keys.all? { |k| k.frozen? && k[0].frozen? && k[1].frozen? })
    assert_equal Encoding::BINARY, h.keys[0][1].encoding
    assert_equal 4, r.size
    assert_operator r.seqno, :<, t.incr("r", "n", "a", 1)
    assert_equal "#<Pigeonhole::Incremented seqno=#{r.seqno} cells=4>", r.inspect
  end

  def test_a_cell_the_commit_did_not_write_is_refused
    r = t.mutate("r").incr("n", "a", 1).commit_returning
    assert_raises(Pigeonhole::InvalidArgument) { r.value("n", "other") }
    assert_raises(Pigeonhole::InvalidArgument) { r.value("n", "a", ts: 7) }
    assert_raises(Pigeonhole::InvalidArgument) { r.value("nope", "a") }
    assert_bad_call(TypeError) { r.value("n", 5) }
  end

  def test_an_incremented_cannot_be_made_from_ruby
    assert_raises(NoMethodError, TypeError) { Pigeonhole::Incremented.new }
    assert_raises(TypeError) { Pigeonhole::Incremented.allocate }
  end

  def test_a_mutation_commits_once
    m = t.mutate("r").incr("n", "a", 1)
    assert_equal 1, m.commit_returning.value("n", "a")
    assert_raises(Pigeonhole::SpentError) { m.commit_returning }
    assert_raises(Pigeonhole::SpentError) { m.commit }
  end

  def test_a_mutation_without_counters_returns_none_of_them
    r = t.mutate("r").put("f", "x", "1").commit_returning
    assert_equal 0, r.size
    assert_empty r.to_h
    assert_equal "1", t.get("r", "f", "x").value
  end

  def test_a_counter_the_mutation_deleted_reads_as_nil
    t.incr("r", "n", "gone", 3)
    r = t.mutate("r").incr("n", "a", 1).delete("n", "gone", ts: 0).commit_returning
    assert_nil r.value("n", "gone")
    assert_equal({ ["n", "a", 0] => 1, ["n", "gone", 0] => nil }, r.to_h)
    assert_nil t.get("r", "n", "gone")
  end

  # write.rs: commit_returning's doc example
  def test_incr_returning_counts_and_returns_the_value
    assert_equal 1, t.incr_returning("orders", "n", "next", 1)
    assert_equal 2, t.incr_returning("orders", "n", "next", 1, durability: :sync)
    assert_equal(-3, t.incr_returning("orders", "n", "next", -5))
    assert_equal(-3, t.get("orders", "n", "next").as_int)
  end

  def test_incr_returning_a_bucket
    now = now_us
    assert_equal 2, t.incr_returning("r", "b", "hits", 2, ts: now)
    assert_equal 4, t.incr_returning("r", "b", "hits", 2, ts: now)
    # A bucket already past the family's TTL (an hour) reads as nothing.
    assert_nil t.incr_returning("r", "b", "hits", 1, ts: now - (2 * HOUR_US))
  end

  # perf2's S1 on #1473: a read that fails after the commit applied raises
  # ReturnedValueError (the commit landed: don't retry), never the commit's own classes. The
  # binding can't make that read fail, so this pins the split from the other side: commit
  # failures and unwritten cells keep their usual classes.
  def test_a_failed_commit_and_a_failed_read_after_it_raise_apart
    assert_operator Pigeonhole::ReturnedValueError, :<, Pigeonhole::Error
    refute Pigeonhole::Native::ERROR_CODES.key?("ReturnedValueError")
    e = Pigeonhole::ReturnedValueError.new("m", "Io", 1, 7)
    assert_equal [7, "Io", 1], [e.seqno, e.code, e.code_num]
    # A commit the engine refuses: a TTL counter family takes only buckets.
    e = assert_raises(Pigeonhole::InvalidArgument) { t.incr_returning("r", "b", "hits", 1) }
    refute_kind_of Pigeonhole::ReturnedValueError, e
    # A cell the commit didn't write is the caller's mistake, not a failed read.
    r = t.mutate("r").incr("n", "a", 1).commit_returning
    e = assert_raises(Pigeonhole::InvalidArgument) { r.value("n", "other") }
    refute_kind_of Pigeonhole::ReturnedValueError, e
    # A commit on a closed database.
    db.close
    @db = nil
    e = assert_raises(Pigeonhole::Closed) { t.incr_returning("r", "n", "a", 1) }
    refute_kind_of Pigeonhole::ReturnedValueError, e
  end

  # D314: N callers adding 1 from zero get exactly 1..N (threads: the GVL is released).
  def test_concurrent_incr_returning_hands_out_each_value_once
    t
    got = Array.new(4) { Thread.new { Array.new(50) { t.incr_returning("orders", "n", "next", 1) } } }
                .flat_map(&:value)
    assert_equal (1..200).to_a, got.sort
  end
end
