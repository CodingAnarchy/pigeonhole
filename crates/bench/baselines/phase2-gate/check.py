#!/usr/bin/env python3
"""Compares a sparse-wide gate run with the Phase 2 baseline (D193): Pigeonhole's
throughput, overall p99 and p99.9, and get, put, row-read and scan p99.

    check.py CANDIDATE.json [BASELINE.json ...]   (default: gate2-run5.json gate2-run6.json here)

A measure fails when it is worse than the baselines' worst value by more than its noise
allowance below; each allowance covers the spread of same-code official runs (README).
Exits 1 if any measure fails."""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
# (measure, higher is better, allowance as a factor on the baselines' worst value)
LIMITS = [
    ("throughput ops/s", True, 0.80),
    ("p99 us", False, 1.12),
    ("p99.9 us", False, 1.15),
    ("get p99 us", False, 3.0),
    ("put p99 us", False, 3.0),
    ("row read p99 us", False, 1.12),
    ("scan p99 us", False, 1.12),
]

def measures(path):
    d = json.load(open(path))
    for r in d["results"]:
        if r["store"] == "pigeonhole" and r["workload"] == "sparse-wide":
            ops = {o["op"]: o["stats"]["p99_ns"] / 1000 for o in r["detail"]["by_type"]}
            return {
                "throughput ops/s": r["throughput"],
                "p99 us": r["p99_ns"] / 1000,
                "p99.9 us": r["p999_ns"] / 1000,
                "get p99 us": ops["get"],
                "put p99 us": ops["put"],
                "row read p99 us": ops["row read"],
                "scan p99 us": ops["scan"],
            }, d["environment"]
    sys.exit(f"{path}: no Pigeonhole sparse-wide result")

cand, env = measures(sys.argv[1])
bases = [measures(p)[0] for p in (sys.argv[2:] or [os.path.join(HERE, f) for f in ("gate2-run5.json", "gate2-run6.json")])]
print(f"candidate: {env.get('git_rev')} load {env.get('load_average')}")
print("| measure | baseline (worst) | limit | candidate | |")
print("|---|--:|--:|--:|---|")
failed = False
for name, higher, factor in LIMITS:
    worst = (min if higher else max)(b[name] for b in bases)
    limit = worst * factor
    ok = cand[name] >= limit if higher else cand[name] <= limit
    failed |= not ok
    print(f"| {name} | {worst:,.1f} | {limit:,.1f} | {cand[name]:,.1f} | {'ok' if ok else 'WORSE'} |")
print("FAIL: a measure is worse than the Phase 2 baseline beyond noise (D193)" if failed else "PASS")
sys.exit(1 if failed else 0)
