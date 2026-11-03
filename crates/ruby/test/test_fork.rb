# frozen_string_literal: true

# simp724's R1 on #697: `fork` after open (Puma, Unicorn, Pitchfork with `preload_app`). In a
# forked child every inherited handle raises `Pigeonhole::Unsupported`, and dropping it (GC,
# or the child exiting) neither closes the parent's database nor releases its writer lock.
require_relative "test_helper"

class TestFork < Minitest::Test
  include PigeonholeTest

  def setup
    super
    skip "no fork on this platform" unless Process.respond_to?(:fork)
  end

  # One of every handle kind, opened before the fork.
  def handles
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "before")
    scan = t.scan(chunk: 1)
    scan.next
    {
      db: db,
      table: t,
      builder: db.table("t"),
      snapshot: db.snapshot,
      transaction: db.transaction.put(t, "x", "f", "q", "child?"),
      batch: db.write_batch.put(t, "x", "f", "q", "child?"),
      mutation: t.mutate("x").put("f", "q", "child?"),
      scan: scan,
    }
  end

  # The calls a child tries; each must raise Unsupported. Returns the failures.
  def child_calls(h)
    calls = {
      "db.tables" => -> { h[:db].tables },
      "db.snapshot" => -> { h[:db].snapshot },
      "db.flush" => -> { h[:db].flush },
      "table.get" => -> { h[:table].get("r", "f", "q") },
      "table.put" => -> { h[:table].put("r", "f", "q", "child") },
      "table.scan" => -> { h[:table].scan.first },
      "builder.open" => -> { h[:builder].open },
      "snapshot.seqno" => -> { h[:snapshot].seqno },
      "transaction.get" => -> { h[:transaction].get(h[:table], "r", "f", "q") },
      "transaction.commit" => -> { h[:transaction].commit },
      "batch.commit" => -> { h[:batch].commit },
      "mutation.commit" => -> { h[:mutation].commit },
      "scan.next" => -> { h[:scan].next },
    }
    calls.filter_map do |name, call|
      call.call
      "#{name}: no error"
    rescue Pigeonhole::Unsupported
      nil
    rescue StandardError => e
      "#{name}: #{e.class}: #{e.message}"
    end
  end

  # After the child is gone: the parent commits, another process is still refused the
  # writer lock, and the parent closes cleanly with everything in place.
  def check_parent(h)
    t = h[:table]
    t.put("r", "f", "q", "after")
    h[:transaction].commit
    assert_equal "child?", t.get("x", "f", "q").value
    script = <<~RUBY
      require "pigeonhole"
      begin
        Pigeonhole.open(#{@path.dump}, read_only_fallback: false).close
        puts "opened"
      rescue Pigeonhole::Error => e
        puts e.code
      end
    RUBY
    assert_equal "WriterLocked", IO.popen(ruby_child_cmd(script), &:read).strip
    h[:snapshot].release
    h[:db].close
    Pigeonhole.open(@path, shards: 1) do |d|
      assert_equal "after", d.table("t").open.get("r", "f", "q").value
    end
  end

  # The child checks its calls, drops every handle, GCs, and leaves with `exit!`.
  def test_a_child_that_drops_and_gcs_every_handle
    h = handles
    pid = fork do
      failures = child_calls(h)
      h[:db].close # in a child: forgets, closes nothing
      h.clear
      @db = nil
      GC.start(full_mark: true, immediate_sweep: true)
      GC.start
      $stderr.puts(failures) unless failures.empty?
      exit!(failures.empty? ? 0 : 1)
    end
    _, status = Process.wait2(pid)
    assert_predicate status, :success?, "the child's calls didn't all raise Unsupported"
    check_parent(h)
  end

  # The child exits normally, still holding every handle: Ruby frees them all at exit,
  # which runs each one's drop in the child.
  def test_a_child_that_exits_holding_every_handle
    h = handles
    pid = fork do
      failures = child_calls(h)
      $stderr.puts(failures) unless failures.empty?
      $stdout.reopen(File::NULL) # the test harness unwinds quietly
      exit(failures.empty? ? 0 : 1)
    end
    _, status = Process.wait2(pid)
    assert_predicate status, :success?, "the child's calls didn't all raise Unsupported"
    check_parent(h)
  end
end
