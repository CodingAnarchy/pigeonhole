# frozen_string_literal: true

# simp724's R2 on #697: non-local exits from the block forms. A block form commits only when
# its block returns normally; `break`, `throw`, an early `return` and exceptions discard the
# batch or roll the transaction back, and nothing stays held (snapshots released, databases
# closed). `next` is a normal return of the block in Ruby, so it commits like falling off
# the end. The block forms are Ruby `ensure`s, so none of this unwinds through Rust.
require_relative "test_helper"

class TestBlocks < Minitest::Test
  include PigeonholeTest

  def t
    @t ||= db.table("t").family("f").create
  end

  # Each way out of a block: the block form is called with a block that leaves that way.
  EXITS = %i[break throw return raise].freeze

  # Calls `form` (which yields one handle to its block), leaving the block by `how` after
  # `work` ran on the handle. Returns the handle the block saw.
  def leave(how, form, &work)
    seen = nil
    case how
    when :break
      form.call { |h| seen = h; work.call(h); break } # rubocop:disable Style/Semicolon
    when :throw
      catch(:out) { form.call { |h| seen = h; work.call(h); throw :out } } # rubocop:disable Style/Semicolon
    when :return
      returner(form, work) { |h| seen = h }
    when :raise
      begin
        form.call { |h| seen = h; work.call(h); raise IOError, "out" } # rubocop:disable Style/Semicolon
      rescue IOError
        nil
      end
    end
    seen
  end

  # Leaves `form`'s block by returning from this method.
  def returner(form, work)
    form.call do |h|
      yield h
      work.call(h)
      return :returned
    end
    :fell_through
  end

  def test_write_batch_discards_on_every_non_local_exit
    EXITS.each do |how|
      b = leave(how, ->(&blk) { db.write_batch(&blk) }) { |w| w.put(t, "r", "f", how.to_s, "v") }
      assert_nil t.get("r", "f", how.to_s), "#{how} committed the batch"
      assert_equal 0, b.size, "#{how} left the batch's writes"
      assert_raises(Pigeonhole::Error, "#{how} left the batch usable") { b.commit }
    end
  end

  def test_write_batch_commits_on_next_and_a_normal_return
    db.write_batch do |w|
      w.put(t, "r", "f", "next", "v")
      next :skipped
    end
    assert_equal "v", t.get("r", "f", "next").value
  end

  def test_transaction_rolls_back_on_every_non_local_exit
    EXITS.each do |how|
      txn = leave(how, ->(&blk) { db.transaction(&blk) }) { |x| x.put(t, "r", "f", how.to_s, "v") }
      assert_nil t.get("r", "f", how.to_s), "#{how} committed the transaction"
      assert_raises(Pigeonhole::Error, "#{how} left the transaction open") { txn.get(t, "r", "f", "q") }
    end
    db.transaction do |x|
      x.put(t, "r", "f", "next", "v")
      next
    end
    assert_equal "v", t.get("r", "f", "next").value
  end

  def test_transact_rolls_back_and_stops_on_every_non_local_exit
    EXITS.each do |how|
      runs = 0
      txn = leave(how, ->(&blk) { db.transact(&blk) }) do |x|
        runs += 1
        x.put(t, "r", "f", how.to_s, "v")
      end
      assert_equal 1, runs, "#{how} retried"
      assert_nil t.get("r", "f", how.to_s), "#{how} committed the attempt"
      assert_raises(Pigeonhole::Error) { txn.commit }
    end
    out = db.transact do |x|
      x.put(t, "r", "f", "next", "v")
      next 7
    end
    assert_equal 7, out
    assert_equal "v", t.get("r", "f", "next").value
  end

  def test_snapshot_is_released_on_every_exit
    (EXITS + [:normal]).each do |how|
      s = if how == :normal
            db.snapshot { |x| x }
          else
            leave(how, ->(&blk) { db.snapshot(&blk) }) { |x| x.seqno }
          end
      assert_raises(Pigeonhole::SnapshotExpired, "#{how} kept the snapshot") { s.seqno }
    end
  end

  def test_open_closes_on_every_exit
    Pigeonhole.open(@path, create: true, shards: 1).close
    (EXITS + [:normal]).each do |how|
      d = if how == :normal
            Pigeonhole.open(@path, shards: 1) { |x| x }
          else
            leave(how, ->(&blk) { Pigeonhole.open(@path, shards: 1, &blk) }) { |x| x.tables }
          end
      assert_predicate d, :closed?, "#{how} left the database open"
    end
    # The writer lock was released each time: it opens again.
    Pigeonhole.open(@path, shards: 1) { |x| assert_equal [], x.tables }
  end

  def test_open_reader_closes_on_every_exit
    db
    EXITS.each do |how|
      r = leave(how, ->(&blk) { Pigeonhole.open_reader(@path, &blk) }) { |x| x.tables }
      assert_predicate r, :closed?, "#{how} left the reader open"
    end
  end

  # Block forms return the block's value.
  def test_block_forms_return_the_blocks_value
    assert_equal 1, db.snapshot { 1 }
    assert_equal 2, db.write_batch { 2 }
    assert_equal 3, db.transaction { 3 }
    assert_equal 4, db.transact { 4 }
  end
end
