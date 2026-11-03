# frozen_string_literal: true

# Mirrors crates/python/tests/test_feed.py: the change feed (D310, #1322). Whole commits in
# order, typed changes, resuming from a token across a reopen, expiry, waiting with the GVL
# released, and interrupting a wait.
require_relative "test_helper"
require "timeout"

class TestFeed < Minitest::Test
  include PigeonholeTest

  FEED = 16 << 20

  def db
    @db ||= Pigeonhole.open(@path, create: true, shards: 1, change_feed: FEED)
  end

  def t
    @t ||= db.table("t").family("f").family("n", Pigeonhole::Family.aggregate(:sum)).create
  end

  def test_changes_arrive_typed_and_in_commit_order
    feed = t.feed
    t.put("r", "f", "q", "v")
    t.incr("r", "n", "hits", 3)
    t.mutate("r").delete("f", "q", ts: 5).delete("f", "q").delete("f").commit
    put = feed.next(timeout: 5)
    assert_equal 1, put.size
    c = put.changes.first
    assert_equal ["t", "f", :put, "r", "q", "v"], [c.table, c.family, c.kind, c.row, c.qualifier, c.value]
    assert c.timestamp.positive? && put.timestamp.positive?
    assert put.frozen? && put.changes.frozen? && c.frozen?
    # ai1249's P1 on #1411: the Strings inside are frozen too, so `==` and `hash` hold.
    assert [c.table, c.family, c.row, c.qualifier, c.value].all?(&:frozen?)
    merge = feed.next(timeout: 5)
    assert_operator merge.seqno, :>, put.seqno
    assert_equal [:merge, 3], [merge.changes[0].kind, merge.changes[0].value]
    deletes = feed.next(timeout: 5)
    assert_equal %i[delete_cell delete_column delete_family], deletes.changes.map(&:kind)
    assert(deletes.changes.all? { |ch| ch.value.nil? })
    assert_equal "", deletes.changes[2].qualifier
    assert_nil feed.try_next
    assert_nil feed.next(timeout: 0.05)
  end

  def test_a_batch_across_tables_is_one_commit
    u = db.table("u").family("f").create
    feed = t.feed(tables: [u])
    db.write_batch { |wb| wb.put(t, "a", "f", "q", "1").put(u, "b", "f", "q", "2") }
    c = feed.next(timeout: 5)
    assert_equal [%w[t a], %w[u b]], c.changes.map { |ch| [ch.table, ch.row] }.sort
  end

  def test_a_prefix_and_a_snapshot_start
    t.put("user:1", "f", "q", "seen by the snapshot")
    snap = db.snapshot
    t.put("other", "f", "q", "not under the prefix")
    t.put("user:2", "f", "q", "after the snapshot")
    feed = t.feed(prefix: "user:", from_snapshot: snap)
    assert_equal ["user:2"], feed.next(timeout: 5).changes.map(&:row)
    assert_nil feed.try_next
    assert_raises(ArgumentError) { t.feed(from_snapshot: snap, from_oldest: true) }
    snap.release
  end

  # `from_oldest:` replays the history kept; `visible: true` still delivers each commit once,
  # in order; the block form closes its feed.
  def test_from_oldest_visible_and_the_block_form
    t.put("a", "f", "q", "1")
    t.put("b", "f", "q", "2")
    rows = t.feed(from_oldest: true) do |f|
      Array.new(2) { f.next(timeout: 5).changes[0].row }.tap { assert_nil f.try_next }
    end
    assert_equal %w[a b], rows
    kept = nil
    t.feed(visible: true) do |f|
      kept = f
      t.put("c", "f", "q", "3")
      assert_equal "c", f.next(timeout: 5).changes[0].row
      assert_nil f.try_next
    end
    assert kept.closed?
  end

  def test_a_token_resumes_after_a_reopen_and_a_feed_off_open_expires_it
    d = Pigeonhole.open(@path, create: true, shards: 1, change_feed: FEED)
    tt = d.table("t").family("f").create
    feed = tt.feed
    tt.put("a", "f", "q", "1")
    assert_equal "a", feed.next(timeout: 5).changes[0].row
    token = feed.token
    assert_equal Encoding::BINARY, token.encoding
    tt.put("b", "f", "q", "2")
    feed.close
    assert feed.closed?
    d.close

    later = Pigeonhole.open(@path, shards: 1, change_feed: FEED) do |d2|
      d2.table("t").open.feed(resume: token) do |f|
        rows = []
        f.each(timeout: 0.5) { |c| rows << c.changes[0].row }
        assert_equal ["b"], rows
        f.token
      end
    end

    # An open that doesn't mention the feed inherits it: the token still resumes.
    Pigeonhole.open(@path, shards: 1).close
    Pigeonhole.open(@path, shards: 1, change_feed: FEED) do |d3|
      d3.table("t").open.feed(resume: later) { |f| assert_nil f.try_next }
    end

    # An open that turns the feed off ends its history: the token expired.
    Pigeonhole.open(@path, shards: 1, change_feed_off: true).close
    Pigeonhole.open(@path, shards: 1, change_feed: FEED) do |d4|
      f = d4.table("t").open.feed(resume: later)
      e = assert_raises(Pigeonhole::FeedExpired) { f.next(timeout: 5) }
      assert_equal 31, e.code_num
      assert_raises(Pigeonhole::FeedExpired) { f.try_next }
      f.close
    end
  end

  def test_refusals
    Pigeonhole.open(File.join(@dir, "plain.phdb"), create: true, shards: 1) do |plain_db|
      plain = plain_db.table("p").family("f").create
      assert_raises(Pigeonhole::InvalidArgument) { plain.feed } # this database's feed is off
    end
    assert_raises(ArgumentError) { Pigeonhole.open(File.join(@dir, "age.phdb"), create: true, feed_age: 1) }
    assert_raises(ArgumentError) do
      Pigeonhole.open(File.join(@dir, "both.phdb"), create: true, change_feed: FEED, change_feed_off: true)
    end
    e = assert_raises(ArgumentError) do
      Pigeonhole.open(File.join(@dir, "off.phdb"), create: true, change_feed_off: true, feed_age: 5)
    end
    assert_match(/change_feed_off: can't take change_feed: or feed_age:/, e.message)
    assert_raises(Pigeonhole::InvalidArgument) { t.feed(resume: "not a token") }
    assert_bad_call(TypeError) { t.feed(tables: ["t"]) }
    feed = t.feed
    feed.close
    feed.close
    assert_raises(Pigeonhole::Closed) { feed.try_next }
    assert_raises(Pigeonhole::Closed) { feed.next(timeout: 0) }
    assert_raises(Pigeonhole::Closed) { feed.token }
  end

  # The writer thread runs (and commits) while the consumer waits in `next`.
  def test_waiting_releases_the_gvl
    feed = t.feed
    waiter = Thread.new { feed.next(timeout: 10) }
    sleep 0.2
    t.put("w", "f", "q", "v")
    assert_equal "w", waiter.value.changes[0].row
  end

  # A wait checks for interrupts between its slices: `Thread#raise` and `Timeout` end a
  # consumer blocked with no timeout of its own.
  def test_a_blocked_wait_is_interruptible
    feed = t.feed
    assert_raises(Timeout::Error) { Timeout.timeout(0.3) { feed.next } }
    waiter = Thread.new { feed.next }
    sleep 0.2
    waiter.raise(IOError, "stop")
    assert_raises(IOError) { waiter.join(5) }
    # The feed still works afterwards.
    t.put("after", "f", "q", "v")
    assert_equal "after", feed.next(timeout: 5).changes[0].row
  end

  def elapsed
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
  end

  # perf2's S1 on #1411: a consumer looping in `next` gives way between its 100 ms slices,
  # so a supervisor's `token` and `close` wait about one slice, not many (1.46 s measured
  # before the fix).
  def test_a_supervisor_reaches_a_feed_behind_a_looping_next
    feed = t.feed
    waiter = Thread.new { feed.next }
    sleep 0.15
    waits = Array.new(10) { elapsed { feed.token } + elapsed { feed.closed? } }
    assert_operator waits.max, :<, 0.35, "token + closed? waits: #{waits.map { |w| w.round(3) }}"
    assert_operator elapsed { feed.close }, :<, 0.25
    assert_raises(Pigeonhole::Closed) { waiter.join(5) }
  end

  # perf2 on #1411: closing the database ends a consumer blocked in a live `each`, with
  # `Closed`, the way out besides `break`.
  def test_closing_the_database_ends_a_live_each
    feed = t.feed
    waiter = Thread.new { feed.each { nil } }
    sleep 0.15
    db.close
    @db = nil
    assert_raises(Pigeonhole::Closed) { waiter.join(5) }
  end

  def test_iterating_waits_for_each_commit
    feed = t.feed
    writer = Thread.new do
      3.times do |i|
        sleep 0.05
        t.put("r#{i}", "f", "q", "v")
      end
    end
    rows = feed.first(3).map { |c| c.changes[0].row }
    writer.join
    assert_equal %w[r0 r1 r2], rows
  end

  # D310 PR 4 (#1361): a reader's table subscribes to the running writer's feed.
  def test_a_reader_process_follows_the_writers_feed
    t
    Pigeonhole.open_reader(@path) do |r|
      rt = r.table("t")
      token = rt.feed do |feed|
        t.put("seen by the reader", "f", "q", "v")
        assert_equal "seen by the reader", feed.next(timeout: 5)&.changes&.first&.row
        feed.token
      end
      t.put("after the token", "f", "q", "v")
      rt.feed(resume: token) do |feed|
        assert_equal "after the token", feed.next(timeout: 5).changes[0].row
      end
      # ai1249's N2 on #1411: a feed can't mix a writer's tables with a reader process's.
      assert_raises(Pigeonhole::InvalidArgument) { t.feed(tables: [rt]) }
      assert_raises(Pigeonhole::InvalidArgument) { rt.feed(tables: [t]) }
    end
  end
end
