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

  # --- the change feed and the memory watch (#1574 PR 2) ---

  # A database with the change feed on, closed by teardown.
  def fdb
    @fdb ||= Pigeonhole.open(File.join(@dir, "feed.phdb"), create: true, shards: 1, change_feed: 16 << 20)
  end

  def teardown
    @fdb&.close
    super
  end

  def test_a_feed_wait_lets_other_fibers_run_and_delivers
    t = fdb.table("t").family("f").create
    feed = t.feed
    got = nil
    ticks = 0
    on_scheduler do |spawn|
      spawn.call { got = feed.next(timeout: 5) }
      spawn.call do
        5.times do
          ticks += 1
          sleep 0.01
        end
        t.put("r", "f", "q", "v")
      end
    end
    assert_equal ["r"], got.changes.map(&:row)
    assert_equal 5, ticks, "the writer fiber ran while the reader waited"
    assert_operator bridge.parks, :>, 0
    feed.close
  end

  def test_a_feed_wait_times_out_and_loses_nothing
    t = fdb.table("t").family("f").create
    feed = t.feed
    first = second = nil
    took = nil
    on_scheduler do
      started = clock
      first = feed.next(timeout: 0.1)
      took = clock - started
      t.put("r", "f", "q", "v")
      second = feed.next(timeout: 5)
    end
    assert_nil first
    assert_operator took, :>=, 0.09
    assert_equal ["r"], second.changes.map(&:row)
    feed.close
  end

  def test_one_fiber_waits_on_a_feed_at_a_time
    t = fdb.table("t").family("f").create
    feed = t.feed
    refused = nil
    on_scheduler do |spawn|
      spawn.call { feed.next(timeout: 0.3) }
      spawn.call do
        sleep 0.05
        feed.next(timeout: 0.1)
      rescue Pigeonhole::Error => e
        refused = e
      end
    end
    assert_match(/one consumer at a time/, refused&.message.to_s)
    feed.close
  end

  # The audit's S-B1: an interrupt pending as a fiber's `next` starts its wait is raised by
  # the bridge's start before the wait exists (the test hook refuses the start the same
  # way). The stream must not stay marked "waiting": the next call works.
  def test_a_feed_wait_refused_at_its_start_leaves_the_feed_usable
    skip "needs the test-hooks build (PIGEONHOLE_TEST_HOOKS=1 rake compile)" unless Pigeonhole::Native.respond_to?(:refuse_next_bridge_start)

    t = fdb.table("t").family("f").create
    feed = t.feed
    raised = after = nil
    on_scheduler do
      Pigeonhole::Native.refuse_next_bridge_start
      feed.next(timeout: 5)
    rescue Pigeonhole::Error => e
      raised = e
      t.put("r", "f", "q", "v")
      after = feed.next(timeout: 5)
    end
    assert_match(/refused start/, raised&.message.to_s)
    assert_equal ["r"], after.changes.map(&:row)
    feed.close
  end

  # A stopped waiter loses no commit: whichever way the race goes (it took the commit before
  # it was stopped, or the commit waits in the slot), the commit is delivered exactly once.
  # Under the minimal scheduler the order is fixed: the commit's wake is in the pipe before
  # any fiber runs, the drainer marks the victim's op done and queues its resume behind this
  # fiber's, and this fiber stops it first, so the commit must come from the slot, with the
  # token from before it.
  def test_a_stopped_feed_waiter_loses_no_commit
    skip "needs the test-hooks build (PIGEONHOLE_TEST_HOOKS=1 rake compile)" unless defined?(Pigeonhole::Native::Gate)

    t = fdb.table("t").family("f").create
    feed = t.feed
    before = feed.token
    gate = Pigeonhole::Native::Gate.new
    delivered = []
    from_slot = false
    on_scheduler do |spawn|
      b = Pigeonhole::Native::FiberBridge.current
      spawn.call { b.await(gate.op(b.native)) } # parks first: the drainer
      victim = spawn.call do
        delivered << feed.next(timeout: 5)
      rescue *STOPS
        nil
      end
      begin
        sleep 0.01 # both parked
        Fiber.blocking do
          t.put("r", "f", "q", "v")
          sleep 0.05 # the feed's wake reaches the pipe
        end
        sleep 0 # the drainer runs: the victim's op is done
        if victim.alive? && delivered.empty?
          stop(victim)
          assert_equal before, feed.token, "the slot's commit isn't delivered yet"
          from_slot = true
          delivered << feed.next(timeout: 5)
        end
      ensure
        gate.open
      end
    end
    assert_equal [["r"]], delivered.compact.map { |c| c.changes.map(&:row) }
    assert_nil feed.try_next, "delivered once"
    assert from_slot, "the victim was stopped with its commit in the slot" if is_a?(TestFiberMinimal)
    feed.close
  end

  # cli692's R1 on #1605: a fiber parked in `next` with no timeout is woken by the stream's
  # close, and raises `Closed`, rather than waiting for a commit that can't come.
  def test_closing_a_stream_ends_a_parked_fiber_wait
    t = fdb.table("t").family("f").create
    feed = t.feed
    raised = nil
    took = nil
    on_scheduler do |spawn|
      spawn.call do
        started = clock
        feed.next
      rescue Pigeonhole::Closed => e
        raised = e
        took = clock - started
      end
      spawn.call do
        sleep 0.05
        feed.close
      end
    end
    assert_kind_of Pigeonhole::Closed, raised
    assert_operator took, :<, 1.0
  end

  def test_a_memory_watch_waits_under_the_scheduler
    s = Pigeonhole::Memory::Store.create(fdb, "agents")
    w = s.watch("n")
    got = nil
    id = nil
    on_scheduler do |spawn|
      spawn.call { got = w.next(timeout: 5) }
      spawn.call do
        sleep 0.02
        id = s.remember("n", "hello").id
      end
    end
    assert_equal [[:remembered, id]], got.events.map { |e| [e.kind, e.id] }
    w.close
    s.close
  end

  def test_a_reader_process_watch_cannot_wait_on_a_fiber
    s = Pigeonhole::Memory::Store.create(fdb, "agents")
    s.remember("n", "hello")
    Pigeonhole.open_reader(File.join(@dir, "feed.phdb")) do |r|
      Pigeonhole::Memory::Store.open_reader(r, "agents", model: nil) do |rs|
        rs.watch do |rw|
          raised = nil
          on_scheduler do
            rw.next(timeout: 1)
          rescue Pigeonhole::Unsupported => e
            raised = e
          end
          assert raised, "a reader's fiber wait is Unsupported (it can wait on a thread)"
        end
      end
    end
    s.close
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

  # #1574 PR 3: the memory store's writes and recall under the scheduler give the blocking
  # results, through every path: near dedup's lookup and an IVF store's settle on the
  # offload pool, and the writes' commits through `transact`.
  def test_memory_writes_and_recall_give_their_blocking_results
    m = Pigeonhole::Memory
    s = m::Store.create(db, "agents", embedding: m::EmbeddingModel.new("m", 4, :f32),
                                      near_dedup: m::NearDedup.new(0.9, policy: :skip),
                                      ivf: m::IvfOptions.new(min_namespace_len: 20))
    rng = Random.new(1574)
    first = other = newer = nil
    on_scheduler do
      first = s.remember("n", "the timeout is 5 s", embedding: [1.0, 0, 0, 0])
      assert_equal :new, first.outcome
      near = s.remember("n", "the timeout is five seconds", embedding: [0.99, 0.01, 0, 0])
      assert_equal [:similar, first.id], [near.outcome, near.id]
      other = s.remember("n", "tabs, not spaces", embedding: [0, 1.0, 0, 0])
      assert_nil s.link(first.id, other.id, m::LinkType::SIMILAR_TO, 0.5)
      newer = s.remember("n", "the timeout is 7 s", embedding: [0, 0, 1.0, 0])
      assert_nil s.supersede(first.id, newer.id)
      assert_equal 1, s.mark_used([other.id]).marked
      s.near_dedup = nil # random 4-d vectors are mostly near duplicates: keep all 30
      30.times { |i| s.remember("big", "note #{i}", embedding: Array.new(4) { rng.rand }) }
      hits, stats = s.recall_with_stats("big", embedding: [0.5, 0.5, 0.5, 0.5], k: 5)
      assert_equal [5, 1], [hits.size, stats.ivf_namespaces]
      assert s.delete(other.id)
      refute s.delete(other.id)
    end
    assert_nil s.get(other.id)
    assert_equal newer.id, s.get(first.id).superseded_by
    assert_predicate s.verify, :consistent?
    s.close
  end

  # A recall waits on the offload pool, not the thread: another fiber runs while the pool
  # is held (the test-hooks seam).
  def test_a_recall_parks_on_a_worker_while_other_fibers_run
    skip "needs the test-hooks build (PIGEONHOLE_TEST_HOOKS=1 rake compile)" unless defined?(Pigeonhole::Native::Gate)
    s = Pigeonhole::Memory::Store.create(db, "agents")
    3.times { |i| s.remember("n", "note #{i}") }
    want = s.recall("n", text: "note").map(&:id)
    got = nil
    ran_meanwhile = false
    Pigeonhole::Native.offload_hold(true)
    begin
      on_scheduler do |spawn|
        spawn.call { got = s.recall("n", text: "note").map(&:id) }
        spawn.call do
          sleep 0.02
          ran_meanwhile = got.nil?
          Pigeonhole::Native.offload_hold(false)
        end
      end
    ensure
      Pigeonhole::Native.offload_hold(false)
    end
    assert ran_meanwhile, "no other fiber ran while the recall waited on its worker"
    assert_equal want, got
    s.close
  end

  # A loop's step under the scheduler: its wait for new memories and the block's groups
  # yield to the thread's other fibers, and the block may itself yield.
  def test_a_consolidation_loop_steps_under_the_scheduler
    m = Pigeonhole::Memory
    s = m::Store.create(fdb, "agents")
    ticks = 0
    in_block = []
    lp = s.consolidate_loop("main", policy: m::ConsolidatePolicy.new(min_age: 0)) do |group|
      before = ticks
      sleep 0.05 # yields to the ticker
      in_block << (ticks - before)
      [m::Memory.new("summary of #{group.size}")]
    end
    step = nil
    waited_ticks = nil
    on_scheduler do |spawn|
      done = false
      spawn.call do
        until done
          ticks += 1
          sleep 0.001
        end
      end
      assert_empty lp.run_once(0).passes # starts the loop
      spawn.call do
        sleep 0.05
        fdb.transact { |t| 3.times { |i| s.remember_in(t, "n", "note #{i}", session: "s") } }
      end
      before = ticks
      step = lp.run_once(5)
      waited_ticks = ticks - before
      done = true
    end
    assert_equal ["n"], step.passes.map(&:first)
    assert_equal 3, step.events
    assert_equal 1, step.passes[0][1].committed
    assert_operator waited_ticks, :>, 0
    assert in_block.all?(&:positive?), "the ticker didn't run while the block slept: #{in_block}"
    lp.close
    s.close
  end

  # #1606's lesson for the loop: a fiber's step waiting for new memories ends when the loop
  # is closed, rather than at its timeout.
  def test_closing_a_loop_ends_a_fiber_step_waiting
    s = Pigeonhole::Memory::Store.create(fdb, "agents")
    lp = s.consolidate_loop("main") { |_| [] }
    lp.run_once(0)
    raised = took = nil
    on_scheduler do |spawn|
      spawn.call do
        started = clock
        lp.run_once(30)
      rescue Pigeonhole::Error => e
        raised = e
        took = clock - started
      end
      spawn.call do
        sleep 0.05
        lp.close
      end
    end
    assert_kind_of Pigeonhole::Error, raised
    assert_operator took, :<, 1.0
    s.close
  end

  # The audit's S-B2: a step whose wait is refused at its start (as above) gives the loop
  # back, rather than leaving it "waiting" for ever.
  def test_a_loop_step_refused_at_its_start_gives_the_loop_back
    skip "needs the test-hooks build (PIGEONHOLE_TEST_HOOKS=1 rake compile)" unless Pigeonhole::Native.respond_to?(:refuse_next_bridge_start)

    s = Pigeonhole::Memory::Store.create(fdb, "agents")
    lp = s.consolidate_loop("main") { |_| [] }
    lp.run_once(0)
    raised = nil
    on_scheduler do
      Pigeonhole::Native.refuse_next_bridge_start
      lp.run_once(5)
    rescue Pigeonhole::Error => e
      raised = e
    end
    assert_match(/refused start/, raised&.message.to_s)
    lp.run_once(0.1) # the loop is back: no "a fiber's step of this loop is waiting"
    lp.close
    s.close
  end

  # A step stopped while its pass runs: the worker ends and is joined, and the loop is
  # back for the next step.
  def test_a_step_stopped_in_its_passes_gives_the_loop_back
    m = Pigeonhole::Memory
    s = m::Store.create(fdb, "agents")
    in_block = false
    lp = s.consolidate_loop("main", policy: m::ConsolidatePolicy.new(min_age: 0)) do |_|
      in_block = true
      sleep 0.2
      [m::Memory.new("summary")]
    end
    lp.run_once(0)
    fdb.transact { |t| 3.times { |i| s.remember_in(t, "n", "note #{i}", session: "s") } }
    ended = false
    on_scheduler do |spawn|
      stepper = spawn.call do
        lp.run_once(1)
      rescue *STOPS
        nil
      ensure
        ended = true
      end
      spawn.call do
        sleep 0.005 until in_block
        stop(stepper)
      end
    end
    assert ended
    lp.run_once(0.1) # the loop is back: no "running on another thread"
    lp.close
    s.close
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
