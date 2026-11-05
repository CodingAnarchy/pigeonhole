"""`Table.scan(...).to_arrow()` (#1480, ICR 0120): a scan's cells as Arrow batches, through the
Arrow PyCapsule protocol, for pyarrow, Polars and DuckDB. Each consumer's test skips if it isn't
installed; CI installs them all, and builds `phdb` for the comparison with `phdb dump`."""

import os
import shutil
import subprocess

import pytest

import pigeonhole as ph

ROWS = 10_000
G_FROM = 9_000


def fill(t, db):
    """arrow-smoke.py's table: an i64 in `f` per row, and from row 9,000 an f64 in `g`, so
    `g` first appears in a later batch than the first and the family dictionary grows."""
    with db.write_batch() as wb:
        for i in range(ROWS):
            row = f"r{i:05d}"
            wb.put(t, row, "f", "n", i)
            if i >= G_FROM:
                wb.put(t, row, "g", "x", i / 2)


@pytest.fixture
def table(db):
    t = db.table("t").family("f").family("g").create()
    fill(t, db)
    return t


def test_the_stream_has_the_long_schema_and_every_cell(table):
    pa = pytest.importorskip("pyarrow")
    s = table.scan().to_arrow()
    assert s.schema_names == ["row", "family", "qualifier", "ts", "value", "value_type"]
    got = s.to_pyarrow().read_all()
    assert got.num_rows == ROWS + (ROWS - G_FROM)
    assert got.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert got.column("family").type == pa.dictionary(pa.int32(), pa.string())


def test_options_map_onto_the_export(table):
    pa = pytest.importorskip("pyarrow")
    got = (
        table.scan(prefix="r0000")
        .to_arrow(utf8_keys=True, typed_values=True, ts="uint64", row_dictionary=True)
        .to_pyarrow()
        .read_all()
    )
    assert got.schema.field("ts").type == pa.uint64()
    assert got.schema.field("row").type == pa.dictionary(pa.int32(), pa.string())
    assert got.schema.field("qualifier").type == pa.string()
    assert got.column("value_i64").to_pylist() == list(range(10))
    batches = list(table.scan().to_arrow(batch_cells=1000).to_pyarrow())
    assert max(b.num_rows for b in batches) == 1000
    with pytest.raises(ValueError, match="ts is"):
        table.scan().to_arrow(ts="seconds")
    with pytest.raises(ValueError, match="at least 1"):
        table.scan().to_arrow(batch_cells=0)
    with pytest.raises(ValueError, match="vector_dim"):
        table.scan().to_arrow(vector_dim=0)


def test_the_scans_selection_carries_over(table, db):
    """The cells are exactly the scan's: its prefix, families and snapshot."""
    pytest.importorskip("pyarrow")
    snap = db.snapshot()
    with db.write_batch() as wb:
        wb.put(table, "r99999", "f", "n", 1)
    scan = dict(prefix="r09", families=["g"], snapshot=snap)
    want = [(r.key, f, q) for r in table.scan(**scan) for (f, q, _) in r]
    got = table.scan(**scan).to_arrow().to_pyarrow().read_all()
    assert list(
        zip(
            got.column("row").to_pylist(),
            got.column("family").to_pylist(),
            got.column("qualifier").to_pylist(),
        )
    ) == want
    assert b"r99999" not in table.scan(snapshot=snap).to_arrow().to_pyarrow().read_all().column(
        "row"
    ).to_pylist()


def test_only_an_unread_open_scan_exports_and_a_stream_is_read_once(table):
    it = table.scan()
    next(it)
    with pytest.raises(ValueError, match="not yet iterated"):
        it.to_arrow()
    closed = table.scan()
    closed.close()
    with pytest.raises(ValueError, match="closed"):
        closed.to_arrow()
    taken = table.scan()
    s = taken.to_arrow()
    assert list(taken) == []  # the stream took the scan
    s.__arrow_c_stream__()
    with pytest.raises(ValueError, match="already read or closed"):
        s.__arrow_c_stream__()
    with table.scan().to_arrow() as s:
        pass
    with pytest.raises(ValueError, match="already read or closed"):
        s.__arrow_c_stream__()


def test_a_stream_past_close_ends_with_closed(path):
    pa = pytest.importorskip("pyarrow")
    db = ph.open(path, create=True, shards=1)
    t = db.table("t").family("f").family("g").create()
    fill(t, db)
    reader = t.scan().to_arrow(batch_cells=100).to_pyarrow()
    reader.read_next_batch()
    db.close()
    with pytest.raises(pa.ArrowException, match="closed"):
        reader.read_all()


def test_polars_and_duckdb_take_the_stream(table):
    pl = pytest.importorskip("polars")
    duckdb = pytest.importorskip("duckdb")
    df = pl.DataFrame(table.scan().to_arrow(typed_values=True))
    assert df.height == ROWS + (ROWS - G_FROM)
    assert df["value_i64"].sum() == sum(range(ROWS))
    stream = table.scan(families=["g"]).to_arrow(typed_values=True)
    [(n, total)] = duckdb.sql("select count(*), sum(value_f64) from stream").fetchall()
    assert (n, total) == (ROWS - G_FROM, sum(i / 2 for i in range(G_FROM, ROWS)))


def phdb():
    return os.environ.get("PHDB") or shutil.which("phdb")


@pytest.mark.skipif(phdb() is None, reason="needs phdb (set PHDB or put it on PATH)")
def test_the_stream_equals_phdb_dump_format_arrow(path):
    """The same cells, schema and batches as `phdb dump --format arrow` (every version,
    typed values), for the arrow-smoke table."""
    pa = pytest.importorskip("pyarrow")
    db = ph.open(path, create=True, shards=1)
    t = db.table("t").family("f").family("g").create()
    fill(t, db)
    db.close()
    dump = subprocess.run(
        [phdb(), "dump", str(path), "--table", "t", "--format", "arrow"],
        check=True,
        capture_output=True,
    ).stdout
    want = pa.ipc.open_stream(dump).read_all()
    db = ph.open(path)
    got = db.table("t").open().scan(versions=0).to_arrow(typed_values=True).to_pyarrow().read_all()
    assert got.schema == want.schema
    assert got.equals(want)
    db.close()
