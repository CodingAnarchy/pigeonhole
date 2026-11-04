# frozen_string_literal: true

# Shared setup: a fresh database path per test, and a database with one shard so tests stay
# light.
require "minitest/autorun"
require "pigeonhole"
require "tmpdir"
require "fileutils"
require "rbconfig"

module PigeonholeTest
  def setup
    super
    @owner_pid = Process.pid
    @dir = Dir.mktmpdir("phdb")
    @path = File.join(@dir, "t.phdb")
  end

  def teardown
    # A forked child unwinding through a test (test_fork.rb) leaves the parent's files alone.
    return super unless Process.pid == @owner_pid

    @db&.close
    FileUtils.remove_entry(@dir) if @dir
    super
  end

  # The test's database, opened on first use and closed by teardown.
  def db
    @db ||= Pigeonhole.open(@path, create: true, shards: 1)
  end

  # How many of this process's threads named with `prefix` are still listed, waiting up to
  # 200 ms for the count to reach 0; nil where the platform doesn't show threads (only
  # Linux's /proc does, which CI runs). The wait is there because a thread just joined stays
  # listed in /proc/self/task for a moment: pthread_join returns when the kernel clears its
  # tid, before do_exit unlists the task (ai1249 on #1451). A thread that really outlived
  # its call is blocked on a channel or in the engine, and stays listed.
  def named_threads_left(prefix)
    return nil unless File.directory?("/proc/self/task")

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.2
    loop do
      n = Dir["/proc/self/task/*/comm"].count do |f|
        File.read(f).start_with?(prefix)
      rescue SystemCallError
        false # the task went while we looked
      end
      return n if n.zero? || Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end

  # Asserts that a call breaking its signature raises one of `classes`. Under `rbs test`'s
  # runtime checking (RBS_TEST_TARGET), RBS raises its own TypeError for such a call after it
  # returns, which counts too: the plain run checks the binding's own error.
  def assert_bad_call(*classes, &)
    classes << RBS::Test::Tester::TypeError if defined?(RBS::Test::Tester::TypeError)
    assert_raises(*classes, &)
  end

  # Runs a Ruby script in a child interpreter with this gem on the load path.
  def ruby_child_cmd(script)
    [RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script]
  end
end
