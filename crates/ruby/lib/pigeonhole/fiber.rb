# frozen_string_literal: true

# The waiting calls under a fiber scheduler (#1574, ICR 0126). Under `Fiber.current_scheduler`
# (Async, Falcon, any `Fiber::Scheduler`), a commit lets the thread's other fibers run while
# it waits; without one, or inside `Fiber.blocking { }`, or with
# `Pigeonhole.fiber_scheduler = false`, it blocks as before.
#
# How (the design note on #1574): the call polls the engine's future once, at the call, so it
# submits there (D210). If that isn't enough, the fiber parks. The engine's waker can't take
# the GVL, so it writes one byte to the thread's pipe; one of the parked fibers holds the
# drainer role, waits on the pipe through the scheduler, polls every woken op in one call,
# and resumes the fibers whose ops are done. The role passes to another parked fiber when its
# holder's own op is done or it's stopped: no task outlives its own call (cli692's R1).
# Every resume of a parked fiber comes from the drainer, once, on the same thread; `block`
# gets no timeout (R2), and a return from it that isn't that resume parks again (R3).
module Pigeonhole
  @fiber_scheduler = true

  class << self
    # Whether the waiting calls yield to a fiber scheduler when one is running (default
    # true). `false` keeps them blocking, process-wide, as before 1.2; `Fiber.blocking { }`
    # does the same for one block.
    def fiber_scheduler
      @fiber_scheduler
    end

    def fiber_scheduler=(on)
      @fiber_scheduler = on ? true : false
    end
  end

  module Native
    # The fiber bridge of one thread (internal: `Native` isn't public API).
    class FiberBridge
      # Native on Unix only: elsewhere every call blocks.
      SUPPORTED = Pigeonhole.respond_to?(:native_fiber_bridge, true)
      KEY = :__pigeonhole_fiber_bridge
      private_constant :KEY

      # A parked call: its op, its fiber, and whether it's done, has been handed the drainer
      # role, or has a resume on its way.
      Entry = Struct.new(:op, :fiber, :done, :drain, :resuming)
      private_constant :Entry

      # This thread's bridge when the calling fiber should yield, else nil.
      def self.current
        return nil unless SUPPORTED && Pigeonhole.fiber_scheduler && Fiber.current_scheduler

        b = Thread.current.thread_variable_get(KEY)
        if b.nil? || b.pid != Process.pid
          # A forked child gets its own; the parent's pipe is left open (ICR 0126).
          b = new
          Thread.current.thread_variable_set(KEY, b)
        end
        b
      end

      attr_reader :pid, :native
      # Calls that parked here (their first poll wasn't enough): read by the tests.
      attr_reader :parks

      def initialize
        @pid = Process.pid
        @native = Pigeonhole.__send__(:native_fiber_bridge)
        @io = IO.for_fd(@native.fd, autoclose: false)
        @parked = {}
        @drainer = nil
        @parks = 0
      end

      # Waits for `op` (started on this bridge) and returns its result. Raised into while it
      # waits (`Async::Task#stop`, a timeout, `Thread#raise`), it drops the op's future: a
      # commit still waiting for room writes nothing, and one already submitted may land (D210).
      def await(op)
        return op.take if op.done?

        scheduler = Fiber.current_scheduler
        entry = Entry.new(op, Fiber.current, false, false, false)
        @parks += 1
        @parked[op.id] = entry
        begin
          until entry.done
            if @drainer.nil? || entry.drain
              entry.drain = false
              @drainer = entry
              drain_until(entry, scheduler)
            else
              scheduler.block(op, nil)
              entry.resuming = false
            end
          end
        ensure
          @parked.delete(op.id)
          hand_off(scheduler) if @drainer.equal?(entry)
          op.cancel unless entry.done
        end
        op.take
      end

      private

      # The drainer's loop: waits on the pipe through the scheduler, polls the woken ops, and
      # resumes the fibers whose ops are done, until its own is.
      def drain_until(me, scheduler)
        until me.done
          @io.wait_readable
          @native.drain.each do |id|
            e = @parked[id] or next
            e.done = true
            next if e.equal?(me) || e.resuming

            e.resuming = true
            scheduler.unblock(e.op, e.fiber)
          end
        end
      end

      # Passes the drainer role to a parked fiber that isn't done and has no resume coming:
      # it's resumed, sees `drain`, and drains. With none, the role lapses until the next wait.
      def hand_off(scheduler)
        @drainer = nil
        e = @parked.each_value.find { |x| !x.done && !x.resuming }
        return unless e

        e.drain = true
        e.resuming = true
        @drainer = e
        scheduler.unblock(e.op, e.fiber)
      end
    end
  end
end
