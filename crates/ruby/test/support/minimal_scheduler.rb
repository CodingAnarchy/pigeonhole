# frozen_string_literal: true

# A minimal `Fiber::Scheduler` for the tests (#1574): just the hooks Ruby requires, over
# `IO.select`, so the fiber tests assume nothing about Async. It follows Ruby's own test
# scheduler (test/fiber/scheduler.rb): `unblock` may come from any thread, so it goes through
# a queue and a wake-up pipe.
class MinimalScheduler
  # Called as `on_block.call(blocker, timeout)` as each `block` starts: the tests' window
  # into a wait (test_fiber.rb's absorb check).
  attr_accessor :on_block

  def initialize
    @readable = {}
    @writable = {}
    @waiting = {}
    @blocked = 0
    @ready = []
    @lock = Thread::Mutex.new
    @urgent = IO.pipe
    @closed = false
  end

  def run
    while @readable.any? || @writable.any? || @waiting.any? || @blocked.positive? || ready?
      readable, writable = IO.select(@readable.keys + [@urgent.first], @writable.keys, [], timeout)
      readable&.each do |io|
        if io == @urgent.first
          io.read_nonblock(1024, exception: false)
        elsif (fiber = @readable.delete(io))
          fiber.resume(IO::READABLE)
        end
      end
      writable&.each { |io| (f = @writable.delete(io)) && f.resume(IO::WRITABLE) }
      now = clock
      @waiting.select { |_, at| at <= now }.each_key do |fiber|
        @waiting.delete(fiber)
        fiber.resume(false) if fiber.alive?
      end
      ready = @lock.synchronize { @ready.slice!(0..) }
      ready.each { |fiber| fiber.resume if fiber.alive? }
    end
  end

  def close
    return if @closed

    run
    @closed = true
    @urgent.each(&:close)
  end

  def fiber(&)
    f = Fiber.new(blocking: false, &)
    f.resume
    f
  end

  def io_wait(io, events, duration)
    @readable[io] = Fiber.current if events.anybits?(IO::READABLE)
    @writable[io] = Fiber.current if events.anybits?(IO::WRITABLE)
    @waiting[Fiber.current] = clock + duration if duration
    Fiber.yield
  ensure
    @readable.delete(io)
    @writable.delete(io)
    @waiting.delete(Fiber.current)
  end

  def kernel_sleep(duration = nil)
    @waiting[Fiber.current] = clock + duration if duration
    @blocked += 1 if duration.nil?
    Fiber.yield
  ensure
    @blocked -= 1 if duration.nil?
    @waiting.delete(Fiber.current)
  end

  def block(blocker, timeout = nil)
    @on_block&.call(blocker, timeout)
    if timeout
      @waiting[Fiber.current] = clock + timeout
    else
      @blocked += 1
    end
    Fiber.yield
  ensure
    timeout ? @waiting.delete(Fiber.current) : @blocked -= 1
  end

  def unblock(_blocker, fiber)
    @lock.synchronize { @ready << fiber }
    @urgent.last.write_nonblock(".", exception: false)
  end

  private

  def ready?
    @lock.synchronize { @ready.any? }
  end

  def timeout
    return 0 if ready?

    soonest = @waiting.values.min
    soonest && [soonest - clock, 0].max
  end

  def clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
