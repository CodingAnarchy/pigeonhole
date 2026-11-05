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

  # ---- return_previous (#1445, ICR 0113 amendment) ----

  def leases
    @leases ||= db.table("leases").family("s").family("n", Pigeonhole::Family.counter).create
  end

  # The guide's example: an unconditional takeover says whom it displaced, with a fencing
  # epoch, and the displaced holder's next guarded write finds it lost the lease (simp724's
  # L1 and L2 on #1552).
  def test_a_lease_handoff_returns_the_holder_it_replaced
    mine = Pigeonhole::Condition.equals("s", "holder", "worker-3")
    leases.mutate("lease:42").put("s", "holder", "worker-3").incr("n", "epoch", 1).commit
    refute_nil leases.mutate("lease:42").put("s", "beat", "1").commit_if(mine)
    r = leases.mutate("lease:42")
              .put("s", "holder", "worker-7")
              .incr("n", "epoch", 1)
              .return_previous("s", "holder")
              .commit_returning
    old = r.previous("s", "holder")
    assert_instance_of Pigeonhole::Cell, old
    assert_equal "worker-3", old.value
    assert_equal 2, r.value("n", "epoch"), "the new holder's fencing token"
    assert_equal "worker-7", leases.get("lease:42", "s", "holder").value
    # The displaced holder's guarded write: refused, nothing written.
    assert_nil leases.mutate("lease:42").put("s", "beat", "2").commit_if(mine)
    assert_equal "1", leases.get("lease:42", "s", "beat").value
    # A column with nothing before the commit.
    r = leases.mutate("lease:43").put(:s, "holder", "worker-1").return_previous(:s, "holder").commit_returning
    assert_nil r.previous("s", "holder")
  end

  def test_previous_values_sit_beside_the_counters
    leases.mutate("r").put("s", "state", "packed").incr("n", "v", 4).commit
    r = leases.mutate("r")
              .put("s", "state", "shipped")
              .incr("n", "v", 1)
              .return_previous("s", "state")
              .return_previous("n", "v")
              .return_previous("s", "state") # a repeat is one request
              .commit_returning
    assert_equal 5, r.value("n", "v")
    assert_equal "packed", r.previous("s", "state").value
    assert_equal 4, r.previous("n", "v").as_int
    assert_equal 1, r.size # the counters only
  end

  def test_return_previous_is_refused_outside_commit_returning
    condition = Pigeonhole::Condition.absent("s", "x")
    [->(m) { m.commit }, ->(m) { m.commit_if(condition) }].each do |commit|
      m = leases.mutate("r").put("s", "x", "1").return_previous("s", "x")
      assert_raises(Pigeonhole::InvalidArgument) { commit.call(m) }
    end
    assert_nil leases.get("r", "s", "x"), "nothing was written"
    # A mutation that writes nothing.
    assert_raises(Pigeonhole::InvalidArgument) do
      leases.mutate("r").return_previous("s", "x").commit_returning
    end
    # A column the mutation didn't ask for is the caller's mistake, not a failed read.
    r = leases.mutate("r").put("s", "x", "1").return_previous("s", "x").commit_returning
    e = assert_raises(Pigeonhole::InvalidArgument) { r.previous("s", "other") }
    refute_kind_of Pigeonhole::ReturnedValueError, e
    # Asking after the commit.
    m = leases.mutate("r").put("s", "x", "2")
    m.commit_returning
    assert_raises(Pigeonhole::SpentError) { m.return_previous("s", "x") }
  end

  def test_returned_is_incremented
    assert_same Pigeonhole::Incremented, Pigeonhole::Returned
    assert_kind_of Pigeonhole::Returned, leases.mutate("r").incr("n", "v", 1).commit_returning
  end

  # ---- commit_if_returning (#1446, D314 amendment 2) ----

  def ck
    @ck ||= db.table("ck")
              .family("s")
              .family("c", Pigeonhole::Family.counter)
              .family("b", Pigeonhole::Family.new(blob_threshold: 64))
              .create
  end

  def test_both_outcomes_report_the_row
    ck.mutate("r").put("s", "state", "packed").put("s", "note", "fragile").incr("c", "n", 5).commit
    packed = Pigeonhole::Condition.equals("s", "state", "packed")
    r = ck.mutate("r")
          .put("s", "state", "shipped")
          .incr("c", "n", 1)
          .return_previous("s", "state")
          .return_current("s", "note")
          .commit_if_returning(packed)
    assert_instance_of Pigeonhole::Checked, r
    assert_predicate r, :frozen?
    assert_predicate r, :applied?
    assert_nil r.seen
    ret = r.returned
    assert_kind_of Pigeonhole::Returned, ret
    assert_predicate ret, :frozen?
    assert_equal "shipped", ret.current("s", "state").value # the new value
    assert_equal "packed", ret.previous("s", "state").value # what it replaced
    assert_equal "fragile", ret.current(:s, "note").value
    assert_equal 6, ret.value("c", "n")
    e = assert_raises(Pigeonhole::InvalidArgument) { ret.current("s", "other") }
    refute_kind_of Pigeonhole::ReturnedValueError, e
    assert_equal "#<Pigeonhole::Checked applied=true>", r.inspect
    # Now it fails: "shipped" is not "packed", and nothing is written.
    r = ck.mutate("r").put("s", "state", "lost").return_current("s", "note").commit_if_returning(packed)
    refute_predicate r, :applied?
    assert_nil r.returned
    seen = r.seen
    assert_instance_of Pigeonhole::Seen, seen
    assert_predicate seen, :frozen?
    assert_equal "shipped", seen.current("s", "state").value
    assert_equal "fragile", seen.current("s", "note").value
    assert_raises(Pigeonhole::InvalidArgument) { seen.current("s", "other") }
    assert_equal "shipped", ck.get("r", "s", "state").value
  end

  def test_a_missing_cell_is_reported_missing
    r = ck.mutate("m").put("s", "q", "v").commit_if_returning(Pigeonhole::Condition.exists("s", "q"))
    refute_predicate r, :applied?
    assert_nil r.seen.current("s", "q")
    r = ck.mutate("m").put("s", "q", "v").commit_if_returning(Pigeonhole::Condition.absent("s", "q"))
    assert_predicate r, :applied?
    assert_equal "v", r.returned.current("s", "q").value
  end

  def test_a_separated_value_is_compared_and_returned_as_the_value
    big = "\x07".b * 5000
    ck.mutate("r").put("b", "q", big).commit
    db.flush
    r = ck.mutate("r").put("b", "q", "small").commit_if_returning(Pigeonhole::Condition.equals("b", "q", "other"))
    refute_predicate r, :applied?
    assert_equal big, r.seen.current("b", "q").value
    r = ck.mutate("r").put("b", "q", "small").commit_if_returning(Pigeonhole::Condition.equals("b", "q", big))
    assert_predicate r, :applied?
  end

  # Each attempt swaps from the value it last saw; a failed one retries from its Seen, with
  # no extra read. Every value is taken by exactly one success.
  def test_a_compare_and_swap_loop_hands_out_each_value_once
    ck.mutate("seq").put("s", "v", "0").commit
    threads = Array.new(4) do
      Thread.new do
        got = []
        seen = 0
        while got.size < 25
          r = ck.mutate("seq")
                .put("s", "v", (seen + 1).to_s)
                .commit_if_returning(Pigeonhole::Condition.equals("s", "v", seen.to_s))
          if r.applied?
            got << seen
            seen += 1
          else
            seen = Integer(r.seen.current("s", "v").value)
          end
        end
        got
      end
    end
    assert_equal (0...100).to_a, threads.flat_map(&:value).sort
  end

  def test_an_empty_mutation_is_a_check_and_read
    %w[e/a e/b].each { |row| ck.mutate(row).put("s", "state", "paid").commit }
    [["paid", true], ["packed", false]].each do |cond, want|
      c = Pigeonhole::Condition.equals("s", "state", cond)
      assert_equal want, !ck.mutate("e/a").commit_if(c).nil?
      r = ck.mutate("e/b").commit_if_returning(c)
      assert_equal want, r.applied?
      side = r.applied? ? r.returned : r.seen
      assert_equal "paid", side.current("s", "state").value
    end
  end

  def test_return_current_is_refused_elsewhere
    c = Pigeonhole::Condition.equals("s", "q", "x")
    e = assert_raises(Pigeonhole::InvalidArgument) do
      ck.mutate("r").put("s", "q", "a").return_current("s", "q").commit
    end
    assert_match(/use a transaction/, e.message)
    assert_raises(Pigeonhole::InvalidArgument) do
      ck.mutate("r").incr("c", "n", 1).return_current("s", "q").commit_returning
    end
    assert_raises(Pigeonhole::InvalidArgument) do
      ck.mutate("r").put("s", "q", "a").return_current("s", "q").commit_if(c)
    end
    assert_nil ck.get("r", "s", "q"), "nothing committed"
    assert_raises(Pigeonhole::FamilyNotFound) do
      ck.mutate("r").put("s", "q", "a").return_current("nope", "q").commit_if_returning(c)
    end
    m = ck.mutate("r").put("s", "q", "a")
    m.commit
    assert_raises(Pigeonhole::SpentError) { m.return_current("s", "q") }
    assert_raises(Pigeonhole::SpentError) { m.commit_if_returning(c) }
  end

  def test_a_seen_cannot_be_made_from_ruby
    assert_raises(NoMethodError, TypeError) { Pigeonhole::Seen.new }
    assert_raises(TypeError) { Pigeonhole::Seen.allocate }
  end
end
