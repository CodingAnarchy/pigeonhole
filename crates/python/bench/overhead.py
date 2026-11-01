"""The Python bindings' per-op overhead (#696): each operation timed from Python, beside the
same operation from Rust (examples/overhead.rs), at the same options and data. Commits are at
Durability.NONE, so neither side measures a sync. Prints a table of ns per unit.

    python crates/python/bench/overhead.py            # the Python side only
    python crates/python/bench/overhead.py --rust     # and the Rust example, built release

Each side runs `runs` times (default 3) and reports the median.
"""

import argparse
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pigeonhole as ph

ROWS = 1_000
ROUNDS = 20_000


def python_side(dirname: str) -> dict[str, float]:
    out = {}
    db = ph.open(Path(dirname) / "py.phdb", create=True, shards=1, durability=ph.Durability.NONE)
    t = db.table("t").family("f").create_if_missing()
    with db.write_batch() as wb:
        for i in range(ROWS):
            wb.put(t, f"r{i:05}", "f", b"q", b"value-0123456789")

    started = time.perf_counter_ns()
    for i in range(ROUNDS):
        t.get(f"r{i % ROWS:05}", "f", b"q")
    out["get"] = (time.perf_counter_ns() - started) / ROUNDS

    started = time.perf_counter_ns()
    for i in range(ROUNDS):
        t.mutate(f"p{i:06}").put("f", b"q", b"value-0123456789").commit()
    out["put"] = (time.perf_counter_ns() - started) / ROUNDS

    started = time.perf_counter_ns()
    for i in range(ROUNDS):
        t.mutate(f"m{i:06}").put("f", b"a", b"x").put("f", b"b", 7).put("f", b"c", 0.5).commit()
    out["mutate3"] = (time.perf_counter_ns() - started) / ROUNDS

    started = time.perf_counter_ns()
    rows = 0
    for _ in range(20):
        for _ in t.scan(prefix=b"r"):
            rows += 1
    out["scan_row"] = (time.perf_counter_ns() - started) / rows

    keys = [f"r{i * 15:05}" for i in range(64)]
    started = time.perf_counter_ns()
    got = 0
    for _ in range(500):
        got += len(t.multi_get(keys))
    out["multi_get_key"] = (time.perf_counter_ns() - started) / got
    db.close()
    return out


def rust_side(dirname: str) -> dict[str, float]:
    root = Path(__file__).resolve().parents[3]
    run = subprocess.run(
        ["cargo", "run", "--quiet", "--release", "-p", "pigeonhole-python", "--example", "overhead", "--", dirname],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return {op: float(ns) for op, ns in (line.split() for line in run.stdout.splitlines())}


def median_of(runs: int, side) -> dict[str, float]:
    results = []
    for _ in range(runs):
        with tempfile.TemporaryDirectory() as d:
            results.append(side(d))
    return {op: statistics.median(r[op] for r in results) for op in results[0]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", action="store_true", help="also run the Rust example")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    py = median_of(args.runs, python_side)
    rust = median_of(args.runs, rust_side) if args.rust else {}
    print(f"{'op':<15}{'python ns':>12}" + (f"{'rust ns':>12}{'overhead ns':>14}" if rust else ""))
    for op, ns in py.items():
        line = f"{op:<15}{ns:>12.0f}"
        if rust:
            line += f"{rust[op]:>12.0f}{ns - rust[op]:>14.0f}"
        print(line)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
