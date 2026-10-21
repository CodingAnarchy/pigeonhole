#!/usr/bin/env python3
"""CI's check that `phdb dump --format arrow` is a stream other Arrow readers take (#691).

    arrow-smoke.py jsonl
        Prints a `phdb load` input: table `t`, 10,000 rows with an i64 in family `f`, and
        from row 9,000 an f64 in family `g` too. That puts `g` in a later batch than the
        first (8,192 cells), so the family dictionary grows mid-stream.

    arrow-smoke.py check STREAM
        Reads the dump with Polars and checks the cell counts, the typed sums and the
        types. Needs `polars`.
"""

import json
import sys

ROWS = 10_000
G_FROM = 9_000


def jsonl():
    print(json.dumps({"phdb": 1, "command": "dump"}))
    print(json.dumps({"table": "t", "families": [{"name": "f"}, {"name": "g"}]}))
    for i in range(ROWS):
        row = f"r{i:05d}"
        cell = {"table": "t", "row": row, "ts": 1_000 + i}
        print(json.dumps({**cell, "family": "f", "qualifier": "n", "value": {"i64": i}}))
        if i >= G_FROM:
            print(json.dumps({**cell, "family": "g", "qualifier": "x", "value": {"f64": i / 2}}))


def check(path):
    import polars as pl

    df = pl.read_ipc_stream(path)
    want = {
        "row": pl.Binary,
        "family": pl.Categorical,
        "qualifier": pl.Binary,
        "ts": pl.Datetime("us", "UTC"),
        "value": pl.Binary,
        "value_type": pl.Categorical,
        "value_i64": pl.Int64,
        "value_f64": pl.Float64,
    }
    assert dict(df.schema) == want, df.schema
    by = {
        r["family"]: r
        for r in df.group_by("family")
        .agg(pl.len().alias("n"), pl.col("value_i64").sum().alias("i"), pl.col("value_f64").sum().alias("f"))
        .to_dicts()
    }
    assert by["f"]["n"] == ROWS and by["f"]["i"] == sum(range(ROWS)), by
    assert by["g"]["n"] == ROWS - G_FROM and by["g"]["f"] == sum(i / 2 for i in range(G_FROM, ROWS)), by
    first = df.filter(pl.col("family") == "g").row(0, named=True)
    assert first["row"] == b"r09000" and first["value_type"] == "f64", first
    print(f"polars {pl.__version__}: {df.height} cells, families {sorted(by)}: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["jsonl"]:
        jsonl()
    elif len(sys.argv) == 3 and sys.argv[1] == "check":
        check(sys.argv[2])
    else:
        sys.exit(__doc__)
