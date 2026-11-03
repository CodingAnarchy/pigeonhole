# frozen_string_literal: true

# Mirrors crates/python/tests/test_readers.py (crates/pigeonhole/tests/readers.rs): reader
# processes see the writer's commits and can't write; without a writer, opening one raises
# NoWriter.
require_relative "test_helper"
require "open3"

class TestReaders < Minitest::Test
  include PigeonholeTest

  # readers.rs: a_reader_with_no_writer_running_gets_no_writer
  def test_a_reader_with_no_writer_running_gets_no_writer
    Pigeonhole.open(@path, create: true, shards: 1).close
    assert_raises(Pigeonhole::NoWriter) { Pigeonhole.open_reader(@path) }
  end

  # A reader in another process (the multi-process case readers exist for).
  def test_a_reader_process_sees_commits_and_cannot_write
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "v1")
    script = <<~RUBY
      require "pigeonhole"
      Pigeonhole.open_reader(#{@path.dump}) do |r|
        t = r.table("t")
        puts t.get("r", "f", "q").value
        $stdout.flush
        $stdin.gets                        # the writer commits again meanwhile
        puts t.get("r", "f", "q").value
        begin
          t.put("r", "f", "q", "x")
        rescue Pigeonhole::ReadOnly => e
          puts e.code
        end
      end
    RUBY
    Open3.popen2(*ruby_child_cmd(script)) do |stdin, stdout, wait|
      assert_equal "v1", stdout.gets&.strip
      t.put("r", "f", "q", "v2")
      stdin.puts "go"
      stdin.close
      assert_equal %w[v2 ReadOnly], stdout.read.split
      assert_predicate wait.value, :success?
    end
  end

  # A reader handle in the writer's own process, as the Rust tests open one.
  def test_a_reader_in_process_reads_and_scans
    t = db.table("t").family("f").create
    3.times { |i| t.put("r#{i}", "f", "q", i) }
    Pigeonhole.open_reader(@path) do |r|
      assert_equal ["t"], r.tables
      rt = r.table("t")
      assert_equal %w[r0 r1 r2], rt.scan.map(&:key)
      r.snapshot do |s|
        t.put("r9", "f", "q", 9)
        assert_equal 3, rt.scan(snapshot: s).count
      end
      assert_raises(Pigeonhole::ReadOnly) { rt.mutate("r") }
      assert_raises(Pigeonhole::ReadOnly) { rt.put("r", "f", "q", "v") }
      assert_raises(Pigeonhole::ReadOnly) { db.write_batch.put(rt, "r", "f", "q", "v") }
      refute_predicate r, :closed?
      r.close
      assert_predicate r, :closed?
      assert_raises(Pigeonhole::Closed) { r.tables }
    end
  end
end
