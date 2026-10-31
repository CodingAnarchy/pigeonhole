"""Mirrors crates/pigeonhole/tests/api.rs: tables and families, reads and their selections,
scans, snapshots, deletes, typed values and aggregate families, batches, durability,
conditional commits and closed handles. Each test names its Rust counterpart."""

import pytest

import pigeonhole as ph


def test_table_builder_create_open_and_families(db):
    """api.rs: table_builder_create_open_and_families"""
    t = db.table("t").family("a").family("b", ph.Family(max_versions=1)).create()
    assert t.name == "t"
    assert t.families() == ["a", "b"]
    assert db.tables() == ["t"]
    assert db.table("t").open().families() == ["a", "b"]
    with pytest.raises(ph.TableExists):
        db.table("t").family("a").create()
    assert db.table("t").family("a").create_if_missing().families() == ["a", "b"]
    with pytest.raises(ph.TableNotFound):
        db.table("nope").open()


def test_drop_table_removes_it_and_its_data(db):
    """api.rs: drop_table_removes_it_and_its_data"""
    t = db.table("t").family("f").create()
    t.put(b"r", "f", b"q", b"v")
    db.drop_table("t")
    assert db.tables() == []
    t = db.table("t").family("f").create()
    assert t.get(b"r", "f", b"q") is None


def test_family_settings_are_stored(db):
    """api.rs: family_settings_are_stored_and_unregistered_operators_refused"""
    t = (
        db.table("t")
        .family("f", ph.Family(max_versions=3, ttl=60, compression="zstd", zstd_level=5))
        .family("n", ph.Family.counter())
        .create()
    )
    info = t.family_info("f")
    assert info["max_versions"] == 3
    assert info["ttl_micros"] == 60_000_000
    assert t.family_info("n")["counter"] is True
    assert t.family_info("missing") is None
    with pytest.raises(ValueError):
        ph.Family(compression="snappy")
    with pytest.raises(ValueError):
        ph.Family.aggregate("median")


def test_error_codes_are_stable_numbers(db):
    """api.rs: error_codes_are_stable_numbers"""
    with pytest.raises(ph.TableNotFound) as e:
        db.table("nope").open()
    assert isinstance(e.value, ph.Error)
    assert (e.value.code, e.value.code_num) == ("TableNotFound", 8)
    t = db.table("t").family("f").create()
    with pytest.raises(ph.FamilyNotFound) as e:
        t.put(b"r", "nope", b"q", b"v")
    assert e.value.code_num == 10


def test_row_reads_order_families_by_creation_or_request(db):
    """api.rs: row_reads_order_families_by_creation_or_request"""
    t = db.table("t").family("a").family("b").create()
    t.mutate(b"r").put("b", b"q", b"2").put("a", b"q", b"1").commit()
    assert [f for f, _, _ in t.row(b"r")] == ["a", "b"]
    assert [f for f, _, _ in t.row(b"r", families=["b", "a"])] == ["b", "a"]
    assert t.row(b"missing") is None


def test_qualifier_version_and_column_filters(db):
    """api.rs: qualifier_version_time_and_column_filters"""
    t = db.table("t").family("f").create()
    m = t.mutate(b"r")
    for q in [b"a1", b"a2", b"b1"]:
        m.put("f", q, b"v1", ts=10).put("f", q, b"v2", ts=20)
    m.commit()
    row = t.row(b"r", qualifier_prefix=b"a")
    assert [q for _, q, _ in row] == [b"a1", b"a2"]
    assert [c.value for _, _, c in row] == [b"v2", b"v2"]
    every = t.row(b"r", versions=0)
    assert [(q, c.ts) for _, q, c in every][:2] == [(b"a1", 20), (b"a1", 10)]
    assert len(t.row(b"r", column_limit=2)) == 2


def test_scans_bounds_prefixes_and_limits(db):
    """api.rs: scans_bounds_prefixes_limits_and_both_iterator_forms"""
    t = db.table("t").family("f").create()
    with db.write_batch() as wb:
        for k in ["a", "b", "c", "d", "e"]:
            wb.put(t, k, "f", "q", k)
        wb.put(t, "x1", "f", "q", "x")
        wb.put(t, "x2", "f", "q", "x")
    keys = lambda it: [r.key for r in it]  # noqa: E731
    assert keys(t.scan(start="b", end="d")) == [b"b", b"c"]
    assert keys(t.scan(prefix="x")) == [b"x1", b"x2"]
    assert keys(t.scan(limit=3)) == [b"a", b"b", b"c"]
    assert keys(t.scan(chunk=1)) == keys(t.scan())
    assert len(keys(t.scan())) == 7
    with pytest.raises(ValueError):
        t.scan(prefix="x", start="a")


def test_cells_outlive_their_handles(db):
    """api.rs: row_and_rowref_agree_and_cells_outlive_their_handles"""
    t = db.table("t").family("f").create()
    t.put(b"r", "f", b"q", b"v")
    row = t.row(b"r")
    cell = row.get("f", b"q")
    del row
    db.flush()
    assert cell.value == b"v"
    assert t.row(b"r").get("f", "q") == cell


def test_snapshots_isolate_reads(db):
    """api.rs: snapshots_isolate_reads"""
    t = db.table("t").family("f").create()
    t.put(b"r", "f", b"q", b"old")
    with db.snapshot() as s:
        t.put(b"r", "f", b"q", b"new")
        assert t.get(b"r", "f", b"q", snapshot=s).value == b"old"
        assert t.row(b"r", snapshot=s).get("f", "q").value == b"old"
        assert [r.get("f", "q").value for r in t.scan(snapshot=s)] == [b"old"]
    assert t.get(b"r", "f", b"q").value == b"new"
    with pytest.raises(ph.SnapshotExpired):
        s.seqno


def test_deletes_follow_the_timestamp_rules(db):
    """api.rs: deletes_follow_the_timestamp_rules"""
    t = db.table("t").family("f").family("g").create()
    t.mutate(b"r").put("f", b"a", b"1", ts=10).put("f", b"b", b"1", ts=10).put("g", b"a", b"1").commit()
    t.delete(b"r", "f", b"a", ts=10)
    assert t.get(b"r", "f", b"a") is None
    t.delete(b"r", "f")
    assert t.get(b"r", "f", b"b") is None
    assert t.get(b"r", "g", b"a") is not None
    # A put below a column delete's timestamp stays hidden.
    t.delete(b"r", "g", b"a")
    t.put(b"r", "g", b"a", b"old", ts=1)
    assert t.get(b"r", "g", b"a") is None
    t.delete(b"r")
    assert t.row(b"r") is None
    with pytest.raises(ValueError):
        t.delete(b"r", qualifier=b"a")


def test_counters_and_typed_values(db):
    """api.rs: counters_and_typed_values"""
    t = db.table("t").family("f").family("n", ph.Family.counter()).create()
    t.mutate(b"r").put("f", b"s", "text").put("f", b"i", -7).put("f", b"x", 0.5).put_u64("f", b"u", 2**64 - 1).commit()
    t.incr(b"r", "n", b"c", 2)
    t.mutate(b"r").incr("n", b"c", 3).incr("n", b"c", 4).commit()
    assert t.get(b"r", "f", b"s").typed == b"text"
    assert t.get(b"r", "f", b"i").typed == -7
    assert t.get(b"r", "f", b"x").typed == 0.5
    assert t.get(b"r", "f", b"u").as_u64() == 2**64 - 1
    assert t.get(b"r", "n", b"c").as_int() == 9
    with pytest.raises(TypeError):
        t.put(b"r", "f", b"b", True)
    with pytest.raises(OverflowError):
        t.put(b"r", "f", b"big", 2**63)


def test_min_and_max_families(db):
    """api.rs: min_and_max_families"""
    t = db.table("t").family("lo", ph.Family.aggregate("min")).family("hi", ph.Family.aggregate("max_f64")).create()
    m = t.mutate(b"r")
    for v in [5, 2, 9]:
        m.aggregate("lo", b"q", v).aggregate("hi", b"q", v)
    m.commit()
    assert t.get(b"r", "lo", b"q").as_int() == 2
    # An int into an f64 family is a float.
    assert t.get(b"r", "hi", b"q").as_float() == 9.0
    with pytest.raises(TypeError):
        t.mutate(b"r").aggregate("lo", b"q", "text")


def test_hll_and_bloom_families(db):
    """api.rs: hll_families_estimate_distinct_inputs, bloom_families_and_bytes_elements"""
    t = (
        db.table("t")
        .family("seen", ph.Family.aggregate("hll"))
        .family("bf", ph.Family.aggregate("bloom", bits=1024, hashes=3))
        .create()
    )
    m = t.mutate(b"r")
    for i in range(100):
        m.aggregate("seen", b"q", i % 40)
    m.aggregate("bf", b"q", "ada").aggregate("bf", b"q", 7).commit()
    assert 35 <= t.get(b"r", "seen", b"q").hll_estimate() <= 45
    bf = t.get(b"r", "bf", b"q")
    assert bf.bloom_contains("ada") and bf.bloom_contains(7)


def test_exact_sum_mean_var_histogram_and_quantiles(db):
    """api.rs: exact_sum_and_mean_var_families, histogram_and_quantile_families"""
    t = (
        db.table("t")
        .family("s", ph.Family.aggregate("sum_f64"))
        .family("mv", ph.Family.aggregate("mean_var"))
        .family("h", ph.Family.aggregate("histogram", bounds=[1.0, 10.0]))
        .family("qs", ph.Family.aggregate("quantiles"))
        .create()
    )
    m = t.mutate(b"r")
    for x in [0.5, 2, 20, 4]:
        m.aggregate("s", b"q", x).aggregate("mv", b"q", x).aggregate("h", b"q", x).aggregate("qs", b"q", x)
    m.commit()
    assert t.get(b"r", "s", b"q").sum_f64() == 26.5
    count, mean, _ = t.get(b"r", "mv", b"q").mean_var()
    assert (count, mean) == (4, 26.5 / 4)
    assert sum(t.get(b"r", "h", b"q").histogram_counts()) == 4
    assert t.get(b"r", "qs", b"q").quantile(1.0) >= 19


def test_write_batches_span_tables(db):
    """api.rs: write_batches_span_tables_and_refuse_foreign_tables"""
    a = db.table("a").family("f").create()
    b = db.table("b").family("f").create()
    wb = db.write_batch().put(a, b"r", "f", b"q", b"1").put(b, b"r", "f", b"q", b"2")
    assert len(wb) == 2
    wb.commit()
    assert (a.get(b"r", "f", b"q").value, b.get(b"r", "f", b"q").value) == (b"1", b"2")
    with pytest.raises(ValueError):
        wb.commit()
    # An exception in the block discards the batch.
    with pytest.raises(RuntimeError):
        with db.write_batch() as wb:
            wb.put(a, b"r2", "f", b"q", b"x")
            raise RuntimeError
    assert a.get(b"r2", "f", b"q") is None


def test_durability_defaults_and_overrides(path):
    """api.rs: durability_defaults_and_overrides"""
    with ph.open(path, create=True, shards=1, durability=ph.Durability.BUFFERED) as db:
        t = db.table("t").family("f").create()
        t.put(b"r", "f", b"q", b"v", durability=ph.Durability.SYNC)
        t.mutate(b"r").put("f", b"q2", b"v").durability(ph.Durability.NONE).commit()
        db.write_batch().put(t, b"r", "f", b"q3", b"v").commit(durability=ph.Durability.GROUP_SYNC)
    with ph.open(path, shards=1) as db:
        t = db.table("t").open()
        assert len(t.row(b"r")) == 3


def test_conditional_commits(db):
    """api.rs: conditional_commits_sync"""
    t = db.table("t").family("f").create()
    absent = ph.Condition.absent("f", b"lock")
    assert t.mutate(b"r").put("f", b"lock", b"me").commit_if(absent) is not None
    assert t.mutate(b"r").put("f", b"lock", b"you").commit_if(absent) is None
    assert t.get(b"r", "f", b"lock").value == b"me"
    held = ph.Condition.equals("f", b"lock", b"me")
    assert t.mutate(b"r").delete("f", b"lock").commit_if(held) is not None
    assert t.mutate(b"r").put("f", b"x", b"1").commit_if(ph.Condition.exists("f", b"lock")) is None


def test_closed_handles_fail_with_closed(path):
    """api.rs: closed_handles_fail_with_closed"""
    db = ph.open(path, create=True, shards=1)
    t = db.table("t").family("f").create()
    db.close()
    db.close()
    with pytest.raises(ph.Closed):
        db.tables()
    with pytest.raises(ph.Closed):
        t.put(b"r", "f", b"q", b"v")


def test_a_mutation_commits_once(db):
    """The binding's own rule: a recorded mutation is spent by its commit."""
    t = db.table("t").family("f").create()
    m = t.mutate(b"r").put("f", b"q", b"v")
    m.commit()
    with pytest.raises(ValueError):
        m.commit()
    with pytest.raises(ValueError):
        m.put("f", b"q", b"w")


def test_keys_take_bytes_like_and_str(db):
    """The binding's own rule: bytes, bytearray, memoryview and str (UTF-8) are keys."""
    t = db.table("t").family("f").create()
    t.put(bytearray(b"k1"), "f", memoryview(b"q"), "é")
    assert t.get("k1", "f", b"q").value == "é".encode()
    with pytest.raises(TypeError):
        t.get(1, "f", b"q")
