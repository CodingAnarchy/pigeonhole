#!/usr/bin/env python3
"""The report step of the gate window (#405): summary.md from RESULTS_DIR, with every #406
target (its measured value, a verdict, and the file that shows it), then the data for the
decisions the window settles (the row cache's default, the I/O backend's default, the scan
readahead) and each step's duration.

    report.py RESULTS_DIR > summary.md

A target whose file is missing reads "not run". The verdicts are the script's reading of
the numbers against the spec; the owner's gate decision is still made from the files."""
import json, os, statistics, sys

R = sys.argv[1]
GROUP_COMMIT_CELLS = 4  # crates/bench/src/workload.rs


def path(name):
    return os.path.join(R, name)


def load(name):
    try:
        with open(path(name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def lines(name):
    try:
        with open(path(name)) as f:
            return [json.loads(l) for l in f if l.strip()]
    except (OSError, ValueError):
        return []


def status(name):
    try:
        with open(path(name)) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def results(name, store=None, workload=None):
    d = load(name)
    if not d:
        return []
    return [
        r
        for r in d.get("results", [])
        if (store is None or r["store"] == store) and (workload is None or r["workload"] == workload)
    ]


def op(r, name):
    for t in r.get("detail", {}).get("by_type", []):
        if t["op"] == name:
            return t["stats"]
    return None


def us(ns):
    return ns / 1000


def verdict(ok):
    return "not run" if ok is None else ("**pass**" if ok else "**FAIL**")


rows = []  # (target, measured, verdict, source)

# Point get in memory: p50 < 2 µs, p99 < 10 µs (ycsb-c gets).
got = [op(r, "get") for r in results("latency-ycsb-c.json", "pigeonhole")]
got = [g for g in got if g]
if got:
    g = got[0]
    ok = us(g["p50_ns"]) < 2 and us(g["p99_ns"]) < 10
    rows.append(("Point get in memory: p50 < 2 µs, p99 < 10 µs",
                 f"p50 {us(g['p50_ns']):.2f} µs, p99 {us(g['p99_ns']):.2f} µs", verdict(ok), "latency-ycsb-c.json"))
else:
    rows.append(("Point get in memory: p50 < 2 µs, p99 < 10 µs", "", verdict(None), "latency-ycsb-c.json"))

# Cold get: one I/O (direct I/O, 1 thread, present rows; median over runs).
cg = [l for l in lines("cold-get.jsonl") if l.get("phase") == "present" and l.get("mode") == "direct" and l.get("threads") == 1]
if cg:
    reads = statistics.median(l["device_reads_per_get"] or 0 for l in cg)
    misses = statistics.median(l["cache_misses_per_get"] for l in cg)
    p50 = statistics.median(l["p50_us"] for l in cg)
    p99 = statistics.median(l["p99_us"] for l in cg)
    ok = reads <= 1.1 and misses <= 1.1
    rows.append(("Point get on cold data: one I/O",
                 f"{reads:.3f} device reads, {misses:.3f} cache misses per get; p50 {p50:.0f} µs, p99 {p99:.0f} µs",
                 verdict(ok), "cold-get.jsonl"))
else:
    rows.append(("Point get on cold data: one I/O", "", verdict(None), "cold-get.jsonl"))

# Batched durable writes: > 1M cells/s across cores; p99 commit < 200 µs (group commit).
gc = results("latency-group-commit.json", "pigeonhole")
if gc:
    best = max(r["throughput"] for r in gc) * GROUP_COMMIT_CELLS
    worst_p99 = max(us(r["p99_ns"]) for r in gc)
    detail = ", ".join(f"{r['threads']} thr: p99 {us(r['p99_ns']):.0f} µs" for r in sorted(gc, key=lambda r: r["threads"]))
    rows.append(("Durable group commit: > 1M cells/s; p99 < 200 µs",
                 f"best {best / 1e6:.2f}M cells/s; {detail}", verdict(best > 1e6 and worst_p99 < 200),
                 "latency-group-commit.json"))
else:
    rows.append(("Durable group commit: > 1M cells/s; p99 < 200 µs", "", verdict(None), "latency-group-commit.json"))

# Ordered scan from cache: > 1 GB/s decoded per core.
sc = lines("scan-cache.jsonl")
if sc:
    gbs = sc[-1]["gb_per_sec"]
    rows.append(("Ordered row scan from cache: > 1 GB/s per core",
                 f"{gbs:.2f} GB/s ({sc[-1]['cache_misses']} misses, {sc[-1]['cache_hits']} hits)",
                 verdict(gbs > 1), "scan-cache.jsonl"))
else:
    rows.append(("Ordered row scan from cache: > 1 GB/s per core", "", verdict(None), "scan-cache.jsonl"))

# Open to first read: < 5 ms.
ol = [l for l in lines("open-latency.jsonl") if "open_to_first_read_ms" in l]
if ol:
    worst = max(l["open_to_first_read_ms"] for l in ol)
    detail = ", ".join(f"{l['io']}: {l['open_to_first_read_ms']:.2f} ms (p99 {l['p99_ms']:.2f})" for l in ol)
    rows.append(("Open to first read: < 5 ms", detail, verdict(worst < 5), "open-latency.jsonl"))
else:
    rows.append(("Open to first read: < 5 ms", "", verdict(None), "open-latency.jsonl"))

# Scaling (D204): efficiency >= 0.8, busiest shard <= 2/N, at every N > 1.
scal = []
for n in (4, 8, 16):
    d = load(f"scaling-{n}.json")
    if d and d.get("scaling"):
        s = d["scaling"]
        scal.append((n, s["efficiency"], s["max_share"], s["efficiency"] >= 0.8 and s["max_share"] <= 2 / n))
if scal:
    detail = ", ".join(f"N={n}: {e:.2f} (busiest {m:.0%})" for n, e, m, _ in scal)
    rows.append(("Thread-per-core scaling: ≥ 0.8 × N", detail, verdict(all(ok for *_, ok in scal)), "scaling-N.json"))
else:
    rows.append(("Thread-per-core scaling: ≥ 0.8 × N", "", verdict(None), "scaling-N.json"))

# Within 1.5x of RocksDB on every workload: throughput and p99, both runs.
gap_rows, gap_ok = [], None
for run in ("all-uring-1.json", "all-uring-2.json"):
    ph = {r["workload"]: r for r in results(run, "pigeonhole")}
    rk = {r["workload"]: r for r in results(run, "rocksdb")}
    for w in sorted(ph):
        if w not in rk:
            continue
        tput = rk[w]["throughput"] / ph[w]["throughput"]
        lat = ph[w]["p99_ns"] / max(rk[w]["p99_ns"], 1)
        ok = tput <= 1.5 and lat <= 1.5
        gap_ok = ok if gap_ok is None else gap_ok and ok
        gap_rows.append((run, w, tput, lat, ok))
# #406 names YCSB A-F, sparse-wide, time-series with TTL and scan-heavy adjacency: a run
# missing one of them does not pass.
REQUIRED = {"ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d", "ycsb-e", "ycsb-f", "sparse-wide",
            "time-series-ttl", "adjacency"}
missing = sorted(REQUIRED - {w for _, w, *_ in gap_rows}) if gap_rows else []
if missing:
    gap_ok = False
fails = [f"{w} ({run[:-5]})" for run, w, _, _, ok in gap_rows if not ok]
measured = []
if fails:
    measured.append(f"outside: {', '.join(fails)}")
if missing:
    measured.append(f"missing: {', '.join(missing)}")
rows.append(("Within 1.5× of RocksDB on every workload",
             "; ".join(measured) or ("all within" if gap_ok else ""),
             verdict(gap_ok), "all-uring-1.json, all-uring-2.json"))

rep = status("all-uring-compare.status")
rows.append(("Reproducibility: run 1 and run 2 within tolerance", "", verdict(None if rep is None else rep == 0),
             "all-uring-compare.txt"))
p2 = status("phase2-gate.status")
rows.append(("Phase 2 floors hold (D193 sparse-wide gate)", "", verdict(None if p2 is None else p2 == 0),
             "phase2-gate.json, logs/phase2-gate.log"))
rows.append(("D193 instruction ceilings", "measured by CI's instructions job, not here", "see CI", "-"))

machine = load("machine.json") or {}
try:
    with open(path("timedout.txt")) as f:
        timedout = [l.strip() for l in f if l.strip()]
except OSError:
    timedout = []
print("# Phase 3 gate window: summary\n")
print(f"- **Machine:** {machine.get('cpu', '?')}, {machine.get('cpus', '?')} CPUs, {machine.get('memory', '?')}")
print(f"- **Kernel:** {machine.get('kernel', '?')}; **drive:** {machine.get('drive', '?')}; **mount:** {machine.get('mount', '?')}")
print(f"- **Commit:** {machine.get('commit', '?')}; **scale:** {machine.get('scale', '?')}")
for w in machine.get("warnings", []):
    print(f"- **Warning:** {w}")
for t in timedout:
    print(f"- **Timed out:** step `{t}` (its stacks: `logs/{t}.stacks`); its targets read \"not run\"")
print("\n## Targets (#406)\n")
print("| Target | Measured | Verdict | Source |")
print("|---|---|---|---|")
for t, m, v, s in rows:
    print(f"| {t} | {m} | {v} | `{s}` |")

if gap_rows:
    print("\n## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)\n")
    print("| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |")
    print("|---|---|---:|---:|---|")
    for run, w, t, l, ok in gap_rows:
        print(f"| {run[:-5]} | {w} | {t:.2f} | {l:.2f} | {'yes' if ok else '**no**'} |")

print("\n## Decisions this window settles\n")
# The row cache's default (D201).
rc_rows = []
for w in ("ycsb-c", "ycsb-a"):
    off = results(f"latency-{w}.json", "pigeonhole")
    on = results(f"row-cache-{w}.json", "pigeonhole")
    if off and on:
        rc_rows.append((w, off[0], on[0]))
if rc_rows:
    print("**Row cache (D201):** off (latency step) against on (256 MiB).\n")
    print("| Workload | Throughput off → on | p50 off → on | p99 off → on |")
    print("|---|---|---|---|")
    for w, a, b in rc_rows:
        print(f"| {w} | {a['throughput']:.0f} → {b['throughput']:.0f} | {us(a['p50_ns']):.2f} → {us(b['p50_ns']):.2f} µs | {us(a['p99_ns']):.1f} → {us(b['p99_ns']):.1f} µs |")
# ycsb-a, one client, shard count and shard spin (D198 item 2, #478; ICR 0027's counters).
ya = [(n, results(f"ycsb-a-{n}.json", "pigeonhole")) for n in
      ("default", "shards1", "shards4", "spin100", "spin200", "spin500")]
if any(r for _, r in ya):
    rk = results("ycsb-a-default.json", "rocksdb")
    print("\n**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.\n")
    print("| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |")
    print("|---|--:|--:|--:|---|")
    for n, rs in ya:
        for r in rs:
            # ICR 0027's per-operation counters, whatever #480 names them in the detail.
            parks = ", ".join(f"{k} {v:.2f}" if isinstance(v, float) else f"{k} {v}"
                              for k, v in sorted(r.get("detail", {}).items())
                              if any(w in k for w in ("park", "wake", "idle")))
            print(f"| {n} | {r['throughput']:.0f} | {us(r['p50_ns']):.1f} | {us(r['p99_ns']):.1f} | {parks or '-'} |")
    for r in rk:
        print(f"| RocksDB | {r['throughput']:.0f} | {us(r['p50_ns']):.1f} | {us(r['p99_ns']):.1f} | - |")

# The group sync depth (D207).
depth_rows = {d: results(f"group-sync-depth-{d}.json", "pigeonhole") for d in ("1", "2", "0")}
if any(depth_rows.values()):
    print("\n**Group sync depth (D207):** durable group commits, ops/s and p99 per client count.\n")
    print("| Depth | " + " | ".join(f"{t} clients" for t in (1, 4, 16)) + " |")
    print("|---|---|---|---|")
    for d, label in (("1", "1"), ("2", "2"), ("0", "unlimited")):
        by_threads = {r["threads"]: r for r in depth_rows[d]}
        cells = [
            f"{by_threads[t]['throughput']:.0f} ops/s, p99 {us(by_threads[t]['p99_ns']):.0f} µs" if t in by_threads else "-"
            for t in (1, 4, 16)
        ]
        print(f"| {label} | " + " | ".join(cells) + " |")
# The I/O backend's default (#402 PR 6).
pu = {r["workload"]: r for r in results("all-uring-1.json", "pigeonhole")}
pp = {r["workload"]: r for r in results("all-pread.json", "pigeonhole")}
if pu and pp:
    print("\n**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.\n")
    print("| Workload | uring/pread throughput | uring/pread p99 |")
    print("|---|---:|---:|")
    for w in sorted(set(pu) & set(pp)):
        print(f"| {w} | {pu[w]['throughput'] / pp[w]['throughput']:.2f} | {pu[w]['p99_ns'] / max(pp[w]['p99_ns'], 1):.2f} |")
# The scan readahead (#431).
cs = [l for l in lines("cold-scan.jsonl") if "mib_per_sec" in l]
if cs:
    print("\n**Scan readahead (#431):** cold full scan, median MiB/s over runs.\n")
    print("| io | direct | readahead | MiB/s |")
    print("|---|---|---|---:|")
    groups = {}
    for l in cs:
        groups.setdefault((l["io"], l["direct"], l["readahead"]), []).append(l["mib_per_sec"])
    for (io, d, ra), v in sorted(groups.items()):
        print(f"| {io} | {d} | {ra} | {statistics.median(v):.0f} |")

try:
    with open(path("durations.tsv")) as f:
        steps = [l.rstrip("\n").split("\t") for l in f if l.strip()]
    print("\n## Step durations\n")
    print("| Step | Minutes |")
    print("|---|---:|")
    for name, mins, _ in steps:
        print(f"| {name} | {mins} |")
    print(f"| **total** | **{sum(int(m) for _, m, _ in steps)}** |")
except OSError:
    pass
