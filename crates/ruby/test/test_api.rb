# frozen_string_literal: true

# Mirrors crates/python/tests/test_api.py (itself a mirror of crates/pigeonhole/tests/api.rs):
# tables and families, reads and their selections, scans, snapshots, deletes, typed values
# and aggregate families, batches, durability, conditional commits and closed handles. Each
# test names its Rust counterpart, or says it is the binding's own rule.
require_relative "test_helper"

class TestApi < Minitest::Test
  include PigeonholeTest

  # api.rs: table_builder_create_open_and_families
  def test_table_builder_create_open_and_families
    t = db.table("t").family("a").family("b", Pigeonhole::Family.new(max_versions: 1)).create
    assert_equal "t", t.name
    assert_equal %w[a b], t.families
    assert_equal ["t"], db.tables
    assert_equal %w[a b], db.table("t").open.families
    assert_raises(Pigeonhole::TableExists) { db.table("t").family("a").create }
    assert_equal %w[a b], db.table("t").family("a").create_if_missing.families
    assert_raises(Pigeonhole::TableNotFound) { db.table("nope").open }
    # Symbols name families and tables too.
    assert_equal ["s"], db.table(:u).family(:s).create.families
  end

  # api.rs: drop_table_removes_it_and_its_data
  def test_drop_table_removes_it_and_its_data
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "v")
    db.drop_table("t")
    assert_equal [], db.tables
    t = db.table("t").family("f").create
    assert_nil t.get("r", "f", "q")
  end

  # api.rs: family_settings_are_stored_and_unregistered_operators_refused
  def test_family_settings_are_stored
    t = db.table("t")
          .family("f", Pigeonhole::Family.new(max_versions: 3, ttl: 60, compression: :zstd, zstd_level: 5))
          .family("n", Pigeonhole::Family.counter)
          .create
    info = t.family_info("f")
    assert_equal ["f", 3, nil], [info.name, info.max_versions, info.aggregation]
    assert_in_delta 60.0, info.ttl
    assert_equal [:zstd, 5], [info.compression, info.zstd_level]
    n = t.family_info("n")
    assert_equal [true, :sum], [n.counter?, n.aggregation]
    assert_nil n.ttl
    assert_nil t.family_info("missing")
    # A family's options recreate it elsewhere.
    u = db.table("u").family("f", info.family).create
    assert_equal 3, u.family_info("f").max_versions
    assert_raises(ArgumentError) { Pigeonhole::Family.new(compression: :snappy) }
    assert_raises(ArgumentError) { Pigeonhole::Family.aggregate(:median) }
    assert_raises(ArgumentError) { Pigeonhole::Family.new(ttl: -1) }
    assert_bad_call(TypeError) { Pigeonhole::Family.new(ttl: "60") }
  end

  # api.rs: error_codes_are_stable_numbers
  def test_error_codes_are_stable_numbers
    e = assert_raises(Pigeonhole::TableNotFound) { db.table("nope").open }
    assert_kind_of Pigeonhole::Error, e
    assert_kind_of StandardError, e
    assert_equal ["TableNotFound", 8], [e.code, e.code_num]
    t = db.table("t").family("f").create
    e = assert_raises(Pigeonhole::FamilyNotFound) { t.put("r", "nope", "q", "v") }
    assert_equal 10, e.code_num
    # Every code has its class, the change feed's too.
    assert_operator Pigeonhole::FeedExpired, :<, Pigeonhole::Error
  end

  # api.rs: row_reads_order_families_by_creation_or_request
  def test_row_reads_order_families_by_creation_or_request
    t = db.table("t").family("a").family("b").create
    t.mutate("r").put("b", "q", "2").put("a", "q", "1").commit
    assert_equal %w[a b], t.row("r").map { |f, _, _| f }
    assert_equal %w[b a], t.row("r", families: %w[b a]).map { |f, _, _| f }
    assert_nil t.row("missing")
  end

  # api.rs: qualifier_version_time_and_column_filters
  def test_qualifier_version_and_column_filters
    t = db.table("t").family("f").create
    m = t.mutate("r")
    %w[a1 a2 b1].each { |q| m.put("f", q, "v1", ts: 10).put("f", q, "v2", ts: 20) }
    m.commit
    row = t.row("r", qualifier_prefix: "a")
    assert_equal %w[a1 a2], row.map { |_, q, _| q }
    assert_equal %w[v2 v2], row.map { |_, _, c| c.value }
    every = t.row("r", versions: 0)
    assert_equal [["a1", 20], ["a1", 10]], every.map { |_, q, c| [q, c.ts] }.first(2)
    assert_equal 2, t.row("r", column_limit: 2).size
  end

  # api.rs: scans_bounds_prefixes_limits_and_both_iterator_forms
  def test_scans_bounds_prefixes_and_limits
    t = db.table("t").family("f").create
    db.write_batch do |wb|
      %w[a b c d e].each { |k| wb.put(t, k, "f", "q", k) }
      wb.put(t, "x1", "f", "q", "x")
      wb.put(t, "x2", "f", "q", "x")
    end
    keys = ->(e) { e.map(&:key) }
    assert_equal %w[b c], keys.(t.scan(start: "b", end: "d"))
    assert_equal %w[x1 x2], keys.(t.scan("x"))
    assert_equal %w[a b c], keys.(t.scan(limit: 3))
    assert_equal keys.(t.scan), keys.(t.scan(chunk: 1))
    assert_equal 7, keys.(t.scan).size
    assert_raises(ArgumentError) { t.scan("x", start: "a") }
    assert_raises(ArgumentError) { t.scan(chunk: 0) }
    # An Enumerator: external iteration, laziness, Enumerable.
    e = t.scan(chunk: 2)
    assert_kind_of Enumerator, e
    assert_equal %w[a b], [e.next.key, e.next.key]
    assert_equal %w[c d], t.scan.lazy.map(&:key).select { |k| k < "e" }.drop(2).first(2)
    assert_equal Encoding::ASCII_8BIT, t.scan.first.key.encoding
  end

  # api.rs: row_and_rowref_agree_and_cells_outlive_their_handles
  def test_cells_outlive_their_handles
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "v")
    row = t.row("r")
    cell = row.get("f", "q")
    row = nil # rubocop:disable Lint/UselessAssignment
    GC.start
    db.flush
    assert_equal "v", cell.value
    assert_equal cell, t.row("r").get("f", "q")
    refute_equal cell, "v"
  end

  # api.rs: snapshots_isolate_reads
  def test_snapshots_isolate_reads
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "old")
    snap = nil
    db.snapshot do |s|
      snap = s
      t.put("r", "f", "q", "new")
      assert_equal "old", t.get("r", "f", "q", snapshot: s).value
      assert_equal "old", t.row("r", snapshot: s).get("f", "q").value
      assert_equal ["old"], t.scan(snapshot: s).map { |r| r.get("f", "q").value }
      assert_kind_of Integer, s.seqno
    end
    assert_equal "new", t.get("r", "f", "q").value
    assert_raises(Pigeonhole::SnapshotExpired) { snap.seqno }
    assert_raises(Pigeonhole::SnapshotExpired) { t.get("r", "f", "q", snapshot: snap) }
    snap.release # twice: nothing
  end

  # api.rs: deletes_follow_the_timestamp_rules
  def test_deletes_follow_the_timestamp_rules
    t = db.table("t").family("f").family("g").create
    t.mutate("r").put("f", "a", "1", ts: 10).put("f", "b", "1", ts: 10).put("g", "a", "1").commit
    t.delete("r", "f", "a", ts: 10)
    assert_nil t.get("r", "f", "a")
    t.delete("r", "f")
    assert_nil t.get("r", "f", "b")
    refute_nil t.get("r", "g", "a")
    # A put below a column delete's timestamp stays hidden.
    t.delete("r", "g", "a")
    t.put("r", "g", "a", "old", ts: 1)
    assert_nil t.get("r", "g", "a")
    assert_kind_of Integer, t.delete("r")
    assert_nil t.row("r")
    assert_raises(ArgumentError) { t.delete("r", nil, "a") }
  end

  # api.rs: counters_and_typed_values
  def test_counters_and_typed_values
    t = db.table("t").family("f").family("n", Pigeonhole::Family.counter).create
    t.mutate("r").put("f", "s", "text").put("f", "i", -7).put("f", "x", 0.5)
     .put_u64("f", "u", (2**64) - 1).commit
    t.incr("r", "n", "c", 2)
    t.mutate("r").incr("n", "c", 3).incr("n", "c", 4).commit
    assert_equal "text", t.get("r", "f", "s").typed
    assert_equal Encoding::ASCII_8BIT, t.get("r", "f", "s").typed.encoding
    assert_equal(-7, t.get("r", "f", "i").typed)
    assert_in_delta 0.5, t.get("r", "f", "x").typed
    assert_equal (2**64) - 1, t.get("r", "f", "u").as_u64
    assert_equal 9, t.get("r", "n", "c").as_int
    assert_nil t.get("r", "f", "s").as_int
    assert_bad_call(TypeError) { t.put("r", "f", "b", true) }
    assert_bad_call(TypeError) { t.put("r", "f", "b", false) }
    assert_bad_call(TypeError) { t.put("r", "f", "b", nil) }
    assert_bad_call(TypeError) { t.put("r", "f", "b", :sym) }
    assert_raises(RangeError) { t.put("r", "f", "big", 2**63) }
    assert_raises(RangeError) { t.mutate("r").put_u64("f", "neg", -1) }
  end

  # api.rs: min_and_max_families
  def test_min_and_max_families
    t = db.table("t").family("lo", Pigeonhole::Family.aggregate(:min))
          .family("hi", Pigeonhole::Family.aggregate("max_f64")).create
    m = t.mutate("r")
    [5, 2, 9].each { |v| m.aggregate("lo", "q", v).aggregate("hi", "q", v) }
    m.commit
    assert_equal 2, t.get("r", "lo", "q").as_int
    # An Integer into an f64 family is a float.
    assert_in_delta 9.0, t.get("r", "hi", "q").as_float
    assert_raises(TypeError) { t.mutate("r").aggregate("lo", "q", "text") }
    assert_bad_call(TypeError) { t.mutate("r").aggregate("lo", "q", true) }
  end

  # api.rs: hll_families_estimate_distinct_inputs, bloom_families_and_bytes_elements
  def test_hll_and_bloom_families
    t = db.table("t")
          .family("seen", Pigeonhole::Family.aggregate(:hll))
          .family("bf", Pigeonhole::Family.aggregate(:bloom, bits: 1024, hashes: 3))
          .create
    m = t.mutate("r")
    100.times { |i| m.aggregate("seen", "q", i % 40) }
    m.aggregate("bf", "q", "ada").aggregate("bf", "q", 7).commit
    assert_includes 35..45, t.get("r", "seen", "q").hll_estimate
    bf = t.get("r", "bf", "q")
    assert bf.bloom_contains("ada")
    assert bf.bloom_contains(7)
    assert_bad_call(TypeError) { bf.bloom_contains(1.5) }
  end

  # api.rs: exact_sum_and_mean_var_families, histogram_and_quantile_families
  def test_exact_sum_mean_var_histogram_and_quantiles
    t = db.table("t")
          .family("s", Pigeonhole::Family.aggregate(:sum_f64))
          .family("mv", Pigeonhole::Family.aggregate(:mean_var))
          .family("h", Pigeonhole::Family.aggregate(:histogram, bounds: [1.0, 10.0]))
          .family("qs", Pigeonhole::Family.aggregate(:quantiles))
          .create
    m = t.mutate("r")
    [0.5, 2, 20, 4].each do |x|
      m.aggregate("s", "q", x).aggregate("mv", "q", x).aggregate("h", "q", x).aggregate("qs", "q", x)
    end
    m.commit
    assert_in_delta 26.5, t.get("r", "s", "q").sum_f64
    count, mean, = t.get("r", "mv", "q").mean_var
    assert_equal 4, count
    assert_in_delta 26.5 / 4, mean
    assert_equal 4, t.get("r", "h", "q").histogram_counts.sum
    assert_operator t.get("r", "qs", "q").quantile(1.0), :>=, 19
    assert_raises(ArgumentError) { Pigeonhole::Family.aggregate(:histogram) }
    assert_raises(ArgumentError) { Pigeonhole::Family.aggregate(:bloom, bits: 64) }
  end

  # api.rs: write_batches_span_tables_and_refuse_foreign_tables
  def test_write_batches_span_tables
    a = db.table("a").family("f").create
    b = db.table("b").family("f").create
    wb = db.write_batch.put(a, "r", "f", "q", "1").put(b, "r", "f", "q", "2")
    assert_equal 2, wb.size
    assert_equal 2, wb.length
    assert_kind_of Integer, wb.commit
    assert_equal %w[1 2], [a.get("r", "f", "q").value, b.get("r", "f", "q").value]
    assert_raises(Pigeonhole::SpentError) { wb.commit }
    assert_raises(Pigeonhole::SpentError) { wb.put(a, "r", "f", "q", "3") }
    # An exception in the block discards the batch.
    assert_raises(RuntimeError) do
      db.write_batch do |w|
        w.put(a, "r2", "f", "q", "x")
        raise "boom"
      end
    end
    assert_nil a.get("r2", "f", "q")
    # The block's value is returned.
    assert_equal :done, db.write_batch { |w| w.put(a, "r3", "f", "q", "y") && :done }
  end

  # api.rs: durability_defaults_and_overrides
  def test_durability_defaults_and_overrides
    Pigeonhole.open(@path, create: true, shards: 1, durability: :buffered) do |d|
      t = d.table("t").family("f").create
      t.put("r", "f", "q", "v", durability: :sync)
      t.mutate("r").put("f", "q2", "v").durability(:none).commit
      d.write_batch.put(t, "r", "f", "q3", "v").commit(durability: :group_sync)
    end
    Pigeonhole.open(@path, shards: 1) do |d|
      assert_equal 3, d.table("t").open.row("r").size
    end
  end

  # The binding's own rule: durability is a Symbol.
  def test_durability_is_a_symbol
    t = db.table("t").family("f").create
    t.put("r", "f", "q", "v", durability: :sync)
    assert_bad_call(ArgumentError) { t.put("r", "f", "q", "v", durability: :eventually) }
    assert_bad_call(TypeError) { t.put("r", "f", "q", "v", durability: 3) }
    assert_bad_call(TypeError) { t.put("r", "f", "q", "v", durability: "sync") }
  end

  # api.rs: conditional_commits_sync
  def test_conditional_commits
    t = db.table("t").family("f").create
    absent = Pigeonhole::Condition.absent("f", "lock")
    refute_nil t.mutate("r").put("f", "lock", "me").commit_if(absent)
    assert_nil t.mutate("r").put("f", "lock", "you").commit_if(absent)
    assert_equal "me", t.get("r", "f", "lock").value
    held = Pigeonhole::Condition.equals("f", "lock", "me")
    refute_nil t.mutate("r").delete("f", "lock").commit_if(held)
    assert_nil t.mutate("r").put("f", "x", "1").commit_if(Pigeonhole::Condition.exists("f", "lock"))
  end

  # api.rs: closed_handles_fail_with_closed
  def test_closed_handles_fail_with_closed
    d = Pigeonhole.open(@path, create: true, shards: 1)
    t = d.table("t").family("f").create
    refute_predicate d, :closed?
    d.close
    d.close
    assert_predicate d, :closed?
    assert_raises(Pigeonhole::Closed) { d.tables }
    assert_raises(Pigeonhole::Closed) { t.put("r", "f", "q", "v") }
  end

  # The binding's own rule: a recorded mutation is spent by its commit.
  def test_a_mutation_commits_once
    t = db.table("t").family("f").create
    m = t.mutate("r").put("f", "q", "v")
    m.commit
    e = assert_raises(Pigeonhole::SpentError) { m.commit }
    assert_nil e.code
    assert_raises(Pigeonhole::SpentError) { m.put("f", "q", "w") }
  end

  # The binding's own rule: keys, qualifiers and values are a String's bytes, whatever its
  # encoding; reads return binary Strings.
  def test_strings_are_bytes
    t = db.table("t").family("f").create
    t.put("k1", "f", "q".b, "é")
    v = t.get("k1", "f", "q").value
    assert_equal "é".b, v
    assert_equal Encoding::ASCII_8BIT, v.encoding
    t.put("\xff\x00".b, "f", "q", "\x00\x01".b)
    assert_equal "\x00\x01".b, t.get("\xff\x00".b, "f", "q").value
    # Any encoding's bytes, as they are.
    t.put("k".encode("UTF-16LE"), "f", "q", "w")
    assert_equal "w", t.get("k\x00".b, "f", "q").value
    assert_bad_call(TypeError) { t.get(1, "f", "q") }
    assert_bad_call(TypeError) { t.get(:k1, "f", "q") }
  end

  # The binding's own rule: no instances except from the library.
  def test_classes_have_no_allocator
    assert_raises(NoMethodError, TypeError) { Pigeonhole::Table.new }
    assert_raises(NoMethodError, TypeError) { Pigeonhole::Cell.allocate }
  end
end
