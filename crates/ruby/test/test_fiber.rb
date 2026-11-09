# frozen_string_literal: true

# The waiting calls under a fiber scheduler (#1574, ICR 0126). Each case runs under Async and
# under a minimal `Fiber::Scheduler` (test/support/minimal_scheduler.rb), so nothing assumes
# Async's behaviour beyond the scheduler interface.
require_relative "test_helper"
require_relative "support/minimal_scheduler"
require "async"

module FiberCases
  include PigeonholeTest

  WINDOWS = RbConfig::CONFIG["host_os"].match?(/mswin|mingw|cygwin/)

  def setup
    super
    skip "the waiting calls block on Windows (ICR 0126)" if WINDOWS
    @t = db.table("t").family("f").family("c", Pigeonhole::Family.counter).create
  end

  def teardown
    Pigeonhole.fiber_scheduler = true
    super
  end

  # This thread's bridge, if a call under the scheduler made one.
  def bridge
    Thread.current.thread_variable_get(:__pigeonhole_fiber_bridge)
  end

  def test_the_waiting_calls_give_their_blocking_results
    on_scheduler do
      assert_kind_of Integer, @t.put("r", "f", "a", "1", durability: :sync)
      assert_kind_of Integer, @t.incr("r", "c", "n", 2, durability: :sync)
      assert_equal 5, @t.incr_returning("r", "c", "n", 3, durability: :sync)
      assert_kind_of Integer, @t.delete("r", "f", "a", durability: :sync)
      assert_nil @t.get("r", "f", "a")

      m = @t.mutate("r").put("f", "b", "2").durability(:sync)
      assert_kind_of Integer, m.commit
      assert_raises(Pigeonhole::SpentError) { m.commit }
      assert_nil @t.mutate("r").put("f", "b", "3").commit_if(Pigeonhole::Condition.absent("f", "b"))
      assert_equal 6, @t.mutate("r").incr("c", "n", 1).commit_returning.value("c", "n")
      checked = @t.mutate("r").put("f", "b", "4").commit_if_returning(Pigeonhole::Condition.exists("f", "b"))
      assert_predicate checked, :applied?
      assert_predicate checked, :frozen?

      db.write_batch do |b|
        b.put(@t, "s", "f", "a", "x")
        b.put(@t, "s", "f", "b", "y")
      end
      assert_equal "x", @t.get("s", "f", "a").value
      b = db.write_batch.put(@t, "s", "f", "c", "z")
      assert_kind_of Integer, b.commit(durability: :sync)

      assert_equal :done, db.transact { |txn| txn.put(@t, "u", "f", "a", "1") && :done }
    end
    assert_equal 0, bridge.native.pending
  end

  def test_other_fibers_run_while_commits_wait
    ticks = 0
    on_scheduler do |spawn|
      done = false
      spawn.call do
        until done
          ticks += 1
          sleep 0.0005
        end
      end
      before = ticks
      200.times { |i| @t.put("r#{i}", "f", "a", "v", durability: :sync) }
      done = true
      assert_operator ticks - before, :>, 0, "no other fiber ran during 200 sync commits"
    end
    assert_operator bridge.parks, :>, 0, "no commit parked: the test proves nothing"
  end

  def test_many_fibers_commit_at_once_and_the_drainer_role_passes
    on_scheduler do |spawn|
      fibers = Array.new(32) do |f|
        spawn.call { 20.times { |i| @t.incr("ctr", "c", "n", 1, durability: :sync) && @t.put("r#{f}-#{i}", "f", "a", "v") } }
      end
      fibers # each finishes before on_scheduler returns
    end
    assert_equal 640, @t.get("ctr", "c", "n").as_int
    assert_equal 0, bridge.native.pending
  end

  def test_a_conflict_raises_through_the_bridge_and_transact_retries
    on_scheduler do
      t1 = db.transaction
      t1.get(@t, "k", "f", "a")
      @t.put("k", "f", "a", "other", durability: :sync)
      t1.put(@t, "k", "f", "a", "mine")
      assert_raises(Pigeonhole::Conflict) { t1.commit(durability: :sync) }

      attempts = 0
      db.transact(durability: :sync) do |txn|
        attempts += 1
        txn.get(@t, "k", "f", "a")
        @t.put("k", "f", "a", "racer#{attempts}") if attempts == 1
        txn.put(@t, "k", "f", "a", "won")
      end
      assert_equal 2, attempts
      assert_equal "won", @t.get("k", "f", "a").value
    end
  end

  def test_both_opt_outs_keep_the_calls_blocking
    on_scheduler do
      # Async's tests share the main thread, whose bridge an earlier test may have made: no
      # call may park on it now.
      parks = -> { Thread.current.thread_variable_get(:__pigeonhole_fiber_bridge)&.parks }
      before = parks.call
      Fiber.blocking { 20.times { @t.put("b", "f", "a", "v", durability: :sync) } }
      assert before == parks.call, "a call parked inside Fiber.blocking"
      Pigeonhole.fiber_scheduler = false
      refute Pigeonhole.fiber_scheduler
      20.times { @t.put("b", "f", "a", "v", durability: :sync) }
      assert before == parks.call, "a call parked with fiber_scheduler = false"
      Pigeonhole.fiber_scheduler = true
      20.times { @t.put("b", "f", "a", "v", durability: :sync) }
      refute_equal before, parks.call, "no call parked with the switch back on"
    end
  end

  def test_a_stopped_fiber_drops_its_op_and_the_others_finish
    stopped = 0
    on_scheduler do |spawn|
      victims = Array.new(8) do |v|
        spawn.call do
          50.times { |i| @t.put("v#{v}-#{i}", "f", "a", "v", durability: :sync) }
        rescue *STOPS
          stopped += 1
        end
      end
      survivors = Array.new(4) do |s|
        spawn.call { 50.times { |i| @t.put("s#{s}-#{i}", "f", "a", "v", durability: :sync) } }
      end
      sleep 0.002
      victims.each { |f| stop(f) }
      survivors # finish before on_scheduler returns
    end
    assert_operator stopped, :>, 0
    # The survivors all landed; a stopped fiber's last commit may have (D210), its later ones not.
    4.times { |s| 50.times { |i| refute_nil @t.get("s#{s}-#{i}", "f", "a") } }
    assert_equal 0, bridge.native.pending
  end

  def test_a_stopped_fiber_gets_no_stray_resume_later
    on_scheduler do |spawn|
      victim = spawn.call do
        loop { @t.put("x", "f", "a", "v", durability: :sync) }
      rescue *STOPS
        started = clock
        sleep 0.2
        assert_operator clock - started, :>=, 0.19, "a stray resume cut the sleep short"
      end
      20.times { |i| @t.put("y#{i}", "f", "a", "v", durability: :sync) }
      stop(victim)
    end
  end

  # D210 / ICR 0126 §4 through the hold seam (test-hooks; cli692 and the coordinator on
  # #1592): a raised waiter whose op is still waiting (a commit waiting for room) writes
  # nothing, even when its op is woken afterwards while another fiber drains. Arranged so
  # the victim is raised while the drainer role is being handed to it: its ensure then hands
  # the role on (to W) and absorbs that resume, and the op must already be gone, or W's
  # drain would poll it to "submission".
  def test_a_raised_waiter_whose_op_still_waits_writes_nothing
    skip "needs the test-hooks build (PIGEONHOLE_TEST_HOOKS=1 rake compile)" unless defined?(Pigeonhole::Native::Gate)

    drainer, held, other = Array.new(3) { Pigeonhole::Native::Gate.new }
    rescued = false
    at_absorb = nil
    on_scheduler do |spawn|
      b = Pigeonhole::Native::FiberBridge.current
      # The minimal scheduler shows the victim's absorb as it starts: its op must be dropped
      # by then, or a drain during the absorb could submit it.
      sched = Fiber.scheduler
      if sched.respond_to?(:on_block=)
        sched.on_block = lambda do |_blocker, timeout|
          at_absorb ||= held.counts if timeout # the absorb is the only timed block
        end
      end
      spawn.call { b.await(drainer.op(b.native)) } # parks first: the drainer
      victim = spawn.call do
        b.await(held.op(b.native))
      rescue *STOPS
        rescued = true
      end
      spawn.call { b.await(other.op(b.native)) }
      sleep 0.01 # all three parked
      drainer.open # its op is done: the role goes to the victim
      sleep 0
      stop(victim)
      held.open # the victim's op is woken now: had it survived, the next drain submits it
      sleep 0.05
      other.open
    end
    assert rescued
    if is_a?(TestFiberMinimal)
      refute_nil at_absorb, "the victim never absorbed a resume: the test arranged nothing"
      assert_equal 1, at_absorb[2], "the victim's op was still live while it absorbed"
    end
    polls, submits, drops = held.counts
    assert_equal [1, 0, 1], [polls, submits, drops], "the held op was polled after its waiter was raised"
    assert_equal 0, bridge.native.pending
  end

  def test_a_forked_child_gets_its_own_bridge
    on_scheduler { @t.put("p", "f", "a", "v", durability: :sync) }
    parent = bridge
    child_pid = fork do
      fresh = nil
      run_scheduler { fresh = Pigeonhole::Native::FiberBridge.current }
      exit!(fresh && !fresh.equal?(parent) && fresh.pid == Process.pid ? 0 : 1)
    end
    _, status = Process.wait2(child_pid)
    assert_predicate status, :success?
  end

  class StopTest < StandardError; end
  # What stopping a fiber raises in it: Async's `Task#stop` raises Async::Stop.
  STOPS = [StopTest, Async::Stop].freeze

  private

  def clock = Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

class TestFiberAsync < Minitest::Test
  include FiberCases

  # Runs the block in an Async reactor; `spawn` starts a sibling task, waited for at the end.
  def on_scheduler
    Sync do |task|
      tasks = []
      spawn = lambda do |&blk|
        t = task.async(&blk)
        tasks << t
        t
      end
      yield spawn
      tasks.each(&:wait)
    end
  end

  def run_scheduler(&) = Sync(&)

  def stop(task) = task.stop
end

class TestFiberMinimal < Minitest::Test
  include FiberCases

  # Runs the block on a fresh MinimalScheduler in a non-blocking fiber; `spawn` starts
  # another, and the scheduler runs until every fiber has finished.
  def on_scheduler(&blk)
    run_scheduler do
      spawn = ->(&b) { Fiber.schedule(&b) }
      blk.call(spawn)
    end
  end

  # The scheduler's thread's bridge, kept as that thread ends.
  def bridge
    @last_bridge
  end

  def run_scheduler(&blk)
    Thread.new do
      Fiber.set_scheduler(MinimalScheduler.new)
      Fiber.schedule(&blk)
      Fiber.scheduler.close # runs every fiber to its end
      @last_bridge = Thread.current.thread_variable_get(:__pigeonhole_fiber_bridge)
    end.join
    nil
  end

  def stop(fiber) = fiber.raise(StopTest)

end

# Where the bridge exists (ICR 0126, cli692's Q2): everywhere but Windows, where every call
# blocks as before, scheduler or not.
class TestFiberPlatforms < Minitest::Test
  include PigeonholeTest

  def test_windows_keeps_blocking_and_unix_has_the_bridge
    windows = RbConfig::CONFIG["host_os"].match?(/mswin|mingw|cygwin/)
    assert_equal !windows, Pigeonhole::Native::FiberBridge::SUPPORTED
    t = db.table("t").family("f").create
    seen = :unset
    Thread.new do
      Fiber.set_scheduler(MinimalScheduler.new)
      Fiber.schedule do
        seen = Pigeonhole::Native::FiberBridge.current
        t.put("r", "f", "a", "v", durability: :sync)
      end
      Fiber.scheduler.close
    end.join
    windows ? assert_nil(seen) : assert_kind_of(Pigeonhole::Native::FiberBridge, seen)
    assert_equal "v", t.get("r", "f", "a").value
  end
end
