# frozen_string_literal: true

# The GVL is released during engine calls (crates/python/tests/test_gil.py's counterpart),
# and simp724's G1 on #697: an interrupt (`Thread#raise`, `Thread#kill`) that arrives while a
# thread is inside an engine call lands after the call returns. The call completes (its
# commit is visible), and nothing stays locked.
require_relative "test_helper"

class TestGvl < Minitest::Test
  include PigeonholeTest

  class Poke < StandardError; end

  # Ruby threads run while one waits on sync commits.
  def test_other_threads_run_during_engine_calls
    t = db.table("t").family("f").create
    stop = false
    ticks = 0
    counter = Thread.new { ticks += 1 until stop }
    begin
      before = ticks
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      10.times { |i| t.put("r#{i}", "f", "q", "v" * 1024, durability: :sync) }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      progressed = ticks - before
    ensure
      stop = true
      counter.join
    end
    assert_operator progressed, :>, 0, "no progress in #{elapsed}s of commits"
    assert_equal 10, t.scan.count
  end

  # A batch big enough that its commit takes a while (under 10,000 rows: `rbs test` reads
  # up to that many of a returned Enumerator to check its element type).
  def big_batch(t, rows, tag)
    b = db.write_batch
    value = "x" * 1024
    rows.times { |i| b.put(t, format("#{tag}%07d", i), "f", "q", value) }
    b
  end

  # Starts `work` in a thread and waits until the thread is inside a native call without
  # the GVL (its status reads "sleep"), then interrupts it with `interrupt`. Returns the
  # thread, whether the interrupt was sent while inside, and the thread's report.
  def interrupt_inside(interrupt, &work)
    report = Queue.new
    th = Thread.new { work.call(report) }
    sent = false
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
    while th.alive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      if th.status == "sleep"
        interrupt.call(th)
        sent = true
        break
      end
      Thread.pass
    end
    [th, sent, report]
  end

  def test_thread_raise_inside_an_engine_call_lands_after_the_commit
    t = db.table("t").family("f").create
    rows = 8_000
    b = big_batch(t, rows, "a")
    th, sent, report = interrupt_inside(->(x) { x.raise(Poke, "poked") }) do |r|
      b.commit(durability: :sync)
      r << :returned
      sleep 5 # the interrupt lands at a later safe point if not already
      r << :slept
    rescue Poke
      r << :poked
    end
    th.join
    skip "the commit finished before the test saw it inside the call" unless sent
    outcome = []
    outcome << report.pop until report.empty?
    # The interrupt arrived after the call returned: never instead of its effect.
    assert_includes outcome, :poked
    refute_includes outcome, :slept
    # The commit happened, whole.
    assert_equal rows, t.scan("a").count
    # Nothing is left locked: the batch is spent, and the database still commits and
    # closes.
    assert_raises(Pigeonhole::Error) { b.commit }
    t.put("after", "f", "q", "v", durability: :sync)
    assert_equal "v", t.get("after", "f", "q").value
  end

  def test_thread_kill_inside_an_engine_call_still_commits
    t = db.table("t").family("f").create
    rows = 8_000
    b = big_batch(t, rows, "k")
    th, sent, = interrupt_inside(->(x) { x.kill }) { |_| b.commit(durability: :sync) }
    th.join
    skip "the commit finished before the test saw it inside the call" unless sent
    refute_predicate th, :alive?
    assert_equal rows, t.scan("k").count
    db.flush
  end

  # Compaction is one long engine call: the interrupt lands after it, and the database is
  # whole and usable.
  def test_thread_raise_inside_compaction_leaves_the_database_usable
    t = db.table("t").family("f").create
    big_batch(t, 8_000, "c").commit
    db.flush
    big_batch(t, 8_000, "c").commit
    th, sent, report = interrupt_inside(->(x) { x.raise(Poke) }) do |r|
      db.compact
      r << :returned
      sleep 5 # the interrupt lands at a later safe point if not already
      r << :slept
    rescue Poke
      r << :poked
    end
    th.join
    skip "compaction finished before the test saw it inside the call" unless sent
    outcome = []
    outcome << report.pop until report.empty?
    assert_includes outcome, :poked
    refute_includes outcome, :slept
    assert_equal 8_000, t.scan("c").count
    db.compact # nothing held: compaction runs again
    db.close
    Pigeonhole.open(@path, shards: 1) { |d| assert_equal 8_000, d.table("t").open.scan("c").count }
  end
end
