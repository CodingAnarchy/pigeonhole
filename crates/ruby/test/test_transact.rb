# frozen_string_literal: true

# Mirrors crates/python/tests/test_transact.py (crates/pigeonhole/tests/transact.rs, and
# api.rs's transactions_commit_or_conflict_sync): commits, conflicts and transact's retries;
# plus simp724's R4 on #697, a transaction shared between threads.
require_relative "test_helper"

class TestTransact < Minitest::Test
  include PigeonholeTest

  def t
    @t ||= db.table("t").family("f").family("g").family("n", Pigeonhole::Family.counter).create
  end

  # api.rs: transactions_commit_or_conflict_sync
  def test_transactions_commit_or_conflict
    t.put("a", "f", "v", 1)
    first = db.transaction
    second = db.transaction
    first.put(t, "a", "f", "v", first.get(t, "a", "f", "v").as_int + 1)
    second.put(t, "a", "f", "v", second.get(t, "a", "f", "v").as_int + 10)
    assert_kind_of Integer, first.commit
    assert_raises(Pigeonhole::Conflict) { second.commit }
    assert_equal 2, t.get("a", "f", "v").as_int
    # R4: a call after commit (or a failed commit) raises the binding's Pigeonhole::Error.
    e = assert_raises(Pigeonhole::Error) { second.commit }
    assert_nil e.code
    assert_raises(Pigeonhole::Error) { first.get(t, "a", "f", "v") }
    assert_raises(Pigeonhole::Error) { first.put(t, "a", "f", "v", 0) }
    assert_raises(Pigeonhole::Error) { first.rollback }
  end

  # The binding's block form: commit on a normal return, nothing on an exception.
  def test_a_transaction_block_commits_or_rolls_back
    db.transaction { |txn| txn.put(t, "r", "f", "q", "v") }
    assert_equal "v", t.get("r", "f", "q").value
    assert_raises(RuntimeError) do
      db.transaction do |txn|
        txn.put(t, "r", "f", "q", "w")
        raise "boom"
      end
    end
    assert_equal "v", t.get("r", "f", "q").value
    # Committing or rolling back inside the block is fine: the block form doesn't again.
    db.transaction { |txn| txn.put(t, "r", "f", "q", "x").commit }
    db.transaction { |txn| txn.put(t, "r", "f", "q", "y").rollback }
    assert_equal "x", t.get("r", "f", "q").value
  end

  # transact.rs: a_conflicting_attempt_is_retried_and_counted
  def test_a_conflicting_attempt_is_retried
    t.put("a", "f", "v", 0)
    attempts = 0
    out = db.transact do |txn|
      attempts += 1
      n = txn.get(t, "a", "f", "v").as_int
      t.put("a", "f", "v", 100) if attempts == 1 # a concurrent writer, between read and commit
      txn.put(t, "a", "f", "v", n + 1)
      n
    end
    assert_equal 100, out
    assert_equal 2, attempts
    assert_equal 101, t.get("a", "f", "v").as_int
  end

  # transact.rs: retrying_stops_at_the_attempt_cap_or_the_deadline
  def test_retrying_stops_at_the_attempt_cap
    t.put("a", "f", "v", 0)
    attempts = 0
    assert_raises(Pigeonhole::Conflict) do
      db.transact(max_attempts: 3) do |txn|
        attempts += 1
        txn.get(t, "a", "f", "v")
        t.put("a", "f", "v", attempts)
        txn.put(t, "a", "f", "v", -1)
      end
    end
    assert_equal 3, attempts
  end

  # transact.rs: retrying_stops_at_the_attempt_cap_or_the_deadline (the deadline)
  def test_retrying_stops_at_the_deadline
    t.put("a", "f", "v", 0)
    attempts = 0
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert_raises(Pigeonhole::Conflict) do
      db.transact(max_attempts: 1_000_000, deadline: 0.2) do |txn|
        attempts += 1
        txn.get(t, "a", "f", "v")
        t.put("a", "f", "v", attempts)
        txn.put(t, "a", "f", "v", -1)
      end
    end
    assert_operator attempts, :>, 1
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 10
    assert_raises(ArgumentError) { db.transact(deadline: -1) { nil } }
    assert_bad_call(ArgumentError) { db.transact }
  end

  # transact.rs: the_applications_own_error_ends_the_call
  def test_the_applications_own_error_ends_the_call
    attempts = 0
    assert_raises(KeyError) do
      db.transact do |txn|
        attempts += 1
        txn.put(t, "a", "f", "v", 1)
        raise KeyError, "mine"
      end
    end
    assert_equal 1, attempts
    assert_nil t.get("a", "f", "v")
  end

  # transact.rs: concurrent_increments_lose_no_update (threads: the GVL is released)
  def test_concurrent_increments_lose_no_update
    t.put("a", "f", "v", 0)
    threads = Array.new(4) do
      Thread.new do
        25.times do
          db.transact(max_attempts: 1000) do |txn|
            txn.put(t, "a", "f", "v", txn.get(t, "a", "f", "v").as_int + 1)
          end
        end
      end
    end
    threads.each(&:join)
    assert_equal 100, t.get("a", "f", "v").as_int
  end

  # transact.rs: get_row_records_a_read_of_each_family_it_covers
  def test_get_row_records_a_read_of_each_family
    t.mutate("r").put("f", "x", "1").put("g", "y", "1").commit
    txn = db.transaction
    assert_equal 2, txn.get_row(t, "r", %w[f g]).size
    t.put("r", "g", "z", "2") # another column of a family the transaction read
    txn.put(t, "r", "f", "x", "3")
    assert_raises(Pigeonhole::Conflict) { txn.commit }
  end

  # transact.rs: every_row_mutation_write_is_available_in_a_transaction
  def test_every_row_mutation_write_is_available_in_a_transaction
    db.transaction do |txn|
      txn.put(t, "r", "f", "b", "x").put(t, "r", "f", "i", 3).put_u64(t, "r", "f", "u", 7)
      txn.incr(t, "r", "n", "c", 2).put(t, "r", "g", "gone", "x")
    end
    db.transaction { |txn| txn.delete(t, "r", "g") }
    assert_equal %w[b c i u], t.row("r").map { |_, q, _| q }.sort
  end

  # simp724's R4: two threads hammering one transaction, then a commit. The transaction's
  # mutex serialises them: every write lands, none is torn, and calls after the commit
  # raise.
  def test_threads_sharing_a_transaction_serialise
    t # the table exists before the transaction's snapshot
    txn = db.transaction
    writers = Array.new(2) do |w|
      Thread.new do
        200.times do |i|
          txn.put(t, "row#{w}", "f", "q#{i}", "#{w}:#{i}")
          txn.incr(t, "shared", "n", "c", 1)
          txn.get(t, "row#{w}", "f", "q#{i}")
        end
      end
    end
    writers.each(&:join)
    txn.commit
    2.times do |w|
      row = t.row("row#{w}")
      assert_equal 200, row.size
      200.times { |i| assert_equal "#{w}:#{i}", row.get("f", "q#{i}").value }
    end
    assert_equal 400, t.get("shared", "n", "c").as_int
    assert_raises(Pigeonhole::Error) { txn.put(t, "x", "f", "q", "v") }
  end

  # R4 again: a thread using the transaction while another commits it either lands before
  # the commit or raises Pigeonhole::Error; nothing is half-applied.
  def test_a_call_racing_the_commit_lands_before_it_or_raises
    t
    txn = db.transaction
    landed = []
    refused = 0
    th = Thread.new do
      1000.times do |i|
        txn.put(t, "race", "f", "q#{i}", "v")
        landed << i
      rescue Pigeonhole::Error => e
        raise if e.code # an engine error would be a bug here

        refused += 1
      end
    end
    Thread.pass until landed.size > 10
    txn.commit
    th.join
    assert_equal 1000, landed.size + refused
    assert_equal landed.size, t.row("race")&.size.to_i
  end
end
