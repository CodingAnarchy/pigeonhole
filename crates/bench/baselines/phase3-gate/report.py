#!/usr/bin/env python3
"""The report step of the gate window (#405): summary.md from RESULTS_DIR, with every #406
target (its measured value, a verdict, and the file that shows it), then the data for the
decisions the window settles (the row cache's effect, the I/O backend's default, the scan
readahead) and each step's duration.

    report.py RESULTS_DIR > summary.md

A target whose file is missing reads "not run". The verdicts are the script's reading of
the numbers against the spec; the owner's gate decision is still made from the files."""
import json, os, re, statistics, sys

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


XSHARD = "cross-shard-contention"


def default_config(r):
    """False for the run `all` adds beside the default for D246: Pigeonhole's cross-shard
    contention with tablet changes off. Tables keyed by workload use the default run."""
    return not (r["store"] == "pigeonhole" and r["workload"] == XSHARD
                and "tablets=on" not in r.get("store_config", ""))


def op(r, name):
    for t in r.get("detail", {}).get("by_type", []):
        if t["op"] == name:
            return t["stats"]
    return None


def us(ns):
    return ns / 1000


def rate(x):
    """Operations per second, in K from 10K up."""
    return f"{x / 1e3:.0f}K" if x >= 1e4 else f"{x:.0f}"


def verdict(ok):
    return "not run" if ok is None else ("**pass**" if ok else "**FAIL**")


rows = []  # (target, measured, verdict, source)

# Point get in memory: p50 < 2 µs, p99 < 10 µs (ycsb-c gets). "In memory" (D216): a 2 GiB
# block cache holding the table, warmed, and the defaults (row cache on).
# ycsb-c's gets are recorded as row reads (one family, one cell).
got = [op(r, "get") or op(r, "row read") for r in results("latency-ycsb-c.json", "pigeonhole")]
got = [g for g in got if g]
if got:
    g = got[0]
    ok = us(g["p50_ns"]) < 2 and us(g["p99_ns"]) < 10
    rows.append(("Point get in memory: p50 < 2 µs, p99 < 10 µs",
                 f"p50 {us(g['p50_ns']):.2f} µs, p99 {us(g['p99_ns']):.2f} µs", verdict(ok), "latency-ycsb-c.json"))
else:
    rows.append(("Point get in memory: p50 < 2 µs, p99 < 10 µs", "", verdict(None), "latency-ycsb-c.json"))

# Memtable point lookup: <= 300 ns at 2^18 (262,144) entries, the largest one memtable
# reaches under the defaults with small cells (D230); 1M and 10k reported (#17; criterion
# `seek/`, median of the estimate). Criterion prints the name, then `time: [low mid high]`,
# on one line or two. D239 accepts 337 ns, measured on the AX102 (2026-10-13 pre-gate check,
# main 9ff02d00): up to 354 ns (5% above it) the verdict is "accepted (D239)"; slower is a FAIL.
UNITS = {"ps": 1e-3, "ns": 1.0, "us": 1e3, "µs": 1e3, "ms": 1e6, "s": 1e9}
seek, name = {}, None
try:
    with open(path("memtable-lookup.txt")) as f:
        for l in f:
            if l.lstrip().startswith("seek/"):
                name = l.split("time:")[0].strip()
            m = re.search(r"time:\s*\[\S+ \S+ (\S+) (\S+)", l)
            if m and name:
                seek[name] = float(m.group(1)) * UNITS.get(m.group(2), 1.0)
except OSError:
    pass
def is_hit(k, size):
    # `seek/262k entries/hit (get)`: the size label, then the probe.
    parts = k.split("/")
    return len(parts) >= 3 and parts[1].startswith(size + " ") and parts[2].startswith("hit")

LOOKUP_TARGET = "Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239)"
LOOKUP_ACCEPTED_MAX_NS = 354
judged = next((v for k, v in seek.items() if is_hit(k, "262k")), None)
if judged is not None:
    others = ", ".join(f"{k.split('/', 1)[1]} {v:.0f} ns" for k, v in seek.items() if not is_hit(k, "262k"))
    rows.append((LOOKUP_TARGET, f"262k hit {judged:.0f} ns ({others})",
                 verdict(True) if judged <= 300
                 else "accepted (D239)" if judged <= LOOKUP_ACCEPTED_MAX_NS
                 else verdict(False), "memtable-lookup.txt"))
else:
    rows.append((LOOKUP_TARGET, "", verdict(None), "memtable-lookup.txt"))

# Cold get: one I/O (direct I/O, 1 thread, present rows; median over runs).
cg = [l for l in lines("cold-get.jsonl") if l.get("phase") == "present" and l.get("mode") == "direct" and l.get("threads") == 1]
if cg:
    reads = statistics.median(l["device_reads_per_get"] or 0 for l in cg)
    misses = statistics.median(l["cache_misses_per_get"] for l in cg)
    p50 = statistics.median(l["p50_us"] for l in cg)
    p99 = statistics.median(l["p99_us"] for l in cg)
    ok = reads <= 1.1 and misses <= 1.1
    cache = cg[0].get("cache_mib")
    cache = f"; block cache {cache} MiB" if cache is not None else ""
    rows.append(("Point get on cold data: one I/O",
                 f"{reads:.3f} device reads, {misses:.3f} cache misses per get; p50 {p50:.0f} µs, p99 {p99:.0f} µs{cache}",
                 verdict(ok), "cold-get.jsonl"))
else:
    rows.append(("Point get on cold data: one I/O", "", verdict(None), "cold-get.jsonl"))

# Batched durable writes: > 1M cells/s across cores; p99 commit < 200 µs (group commit). The
# D213 shape: one shard per core, the table split over every shard, each shard's thread
# keeping commits in flight (phdb-bench's `group_commit`). The client-thread sweep (1, 4, 16
# synchronous clients) is reported beside it, not judged. The disk's raw fdatasync, from
# machine.json, is the floor under any durable commit's p99.
fs = (load("machine.json") or {}).get("fdatasync_4k_us") or {}
floor = f"; disk fdatasync p50 {fs['p50']:.0f} µs, p99 {fs['p99']:.0f} µs" if "p99" in fs else ""
gcd = load("latency-group-commit.json") or {}
g = gcd.get("group_commit")
def stages(g):
    # The engine's part of the commit (submission to resolution on the shard), WAL rollover
    # blocks and inline syncs, and tablet changes while measuring (must be 0; D213).
    e = g.get("engine")
    inside = f"; in the engine p50 {us(e['p50_ns']):.0f} µs, p99 {us(e['p99_ns']):.0f} µs, p99.9 {us(e['p999_ns']):.0f} µs" if e else ""
    return (f"{inside}; WAL rollover blocks {g.get('wal_rollover_blocks', '?')}, inline syncs "
            f"{g.get('wal_inline_syncs', '?')}; tablet changes while measuring {g.get('tablet_changes', '?')}")


def run_length(name):
    # The D213 run's length and the flushes and compactions inside it (D219: the judged run
    # is at least 2M commits, so they fall inside the measured phase).
    r = next((r for r in results(name, "pigeonhole") if "presplit" in r.get("store_config", "")), None)
    if not r:
        return ""
    st = r.get("detail", {}).get("stalls") or {}
    return (f"; {r['operations'] / 1e6:.1f}M commits in {r['run_secs']:.1f} s, "
            f"{st.get('flushes', 0)} flushes, {st.get('compactions', 0)} compactions")


if g:
    cells, per_core = g["cells_per_sec"], g["cells_per_sec"] / max(g["cores"], 1)
    fixed = g.get("tablet_changes", 1) == 0
    rows.append(("Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight)",
                 f"{cells / 1e6:.2f}M cells/s at {g['shards']} shards ({per_core / 1e3:.0f}K per core); "
                 f"p50 {us(g['p50_ns']):.0f} µs, p99 {us(g['p99_ns']):.0f} µs; busiest shard "
                 f"{100 * g['max_share']:.0f}%{run_length('latency-group-commit.json')}{stages(g)}{floor}"
                 + ("" if fixed else " — INVALID: the placement changed while measuring"),
                 verdict(fixed and cells > 1e6 and us(g["p99_ns"]) < 200), "latency-group-commit.json"))
else:
    rows.append(("Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213)", floor.lstrip("; "),
                 verdict(None), "latency-group-commit.json"))
# D235: the judged run pins the shard threads; the same run unpinned is reported beside it.
gu = gcd.get("group_commit_unpinned")
if gu:
    vs = f" ({100 * gu['cells_per_sec'] / g['cells_per_sec'] - 100:+.0f}% vs pinned)" if g else ""
    rows.append(("Durable group commit, shard threads unpinned (reported, not judged; D235)",
                 f"{gu['cells_per_sec'] / 1e6:.2f}M cells/s{vs}; p50 {us(gu['p50_ns']):.0f} µs, "
                 f"p99 {us(gu['p99_ns']):.0f} µs", "-", "latency-group-commit.json"))
# The trade-off beside the judged row (D226): the same D213 run at 4 and 8 commits in flight
# per shard instead of 16. Fewer in flight lower the p99 and give up throughput.
def p999_of(name):
    r = next((r for r in results(name, "pigeonhole") if "presplit" in r.get("store_config", "")), None)
    return f", p99.9 {us(r['p999_ns']):.0f} µs" if r and "p999_ns" in r else ""


for f in (4, 8):
    name = f"latency-group-commit-inflight-{f}.json"
    gf = (load(name) or {}).get("group_commit")
    if not gf:
        continue
    vs = f" ({100 * gf['cells_per_sec'] / g['cells_per_sec'] - 100:+.0f}% vs 16)" if g else ""
    e = gf.get("engine")
    inside = f"; in the engine p99 {us(e['p99_ns']):.0f} µs" if e else ""
    rows.append((f"Durable group commit at {f} in flight per shard (reported, not judged; D226)",
                 f"{gf['cells_per_sec'] / 1e6:.2f}M cells/s{vs}; p50 {us(gf['p50_ns']):.0f} µs, "
                 f"p99 {us(gf['p99_ns']):.0f} µs{p999_of(name)}{inside}",
                 "-", name))
gp = (load("latency-group-commit-pread.json") or {}).get("group_commit")
if gp:
    rows.append(("Durable group commit on pread (reported, not judged; D213: decides the io_uring default)",
                 f"{gp['cells_per_sec'] / 1e6:.2f}M cells/s at {gp['shards']} shards "
                 f"({gp['cells_per_sec'] / max(gp['cores'], 1) / 1e3:.0f}K per core); p50 {us(gp['p50_ns']):.0f} µs, "
                 f"p99 {us(gp['p99_ns']):.0f} µs{run_length('latency-group-commit-pread.json')}{stages(gp)}",
                 "-", "latency-group-commit-pread.json"))
gs = (load("latency-group-commit-short.json") or {}).get("group_commit")
if gs:
    rows.append(("Durable group commit, short run (reported, not judged; D219: the first window's length)",
                 f"{gs['cells_per_sec'] / 1e6:.2f}M cells/s at {gs['shards']} shards; p50 {us(gs['p50_ns']):.0f} µs, "
                 f"p99 {us(gs['p99_ns']):.0f} µs{run_length('latency-group-commit-short.json')}{stages(gs)}",
                 "-", "latency-group-commit-short.json"))
clients = [r for r in results("latency-group-commit.json", "pigeonhole") if "inline" not in r.get("store_config", "")]
if clients:
    detail = ", ".join(f"{r['threads']} thr: {r['throughput'] * GROUP_COMMIT_CELLS / 1e3:.0f}K cells/s, "
                       f"p99 {us(r['p99_ns']):.0f} µs" for r in sorted(clients, key=lambda r: r["threads"]))
    rows.append(("Durable group commit, synchronous clients (reported, not judged; D213)", detail, "-",
                 "latency-group-commit.json"))

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

# Engine scaling (D204, D231, D238): efficiency >= 0.8 for every N up to one chiplet's (CCD's)
# physical cores, the table pre-split at the writes' quantiles with the balancer off (its
# busiest share is reported, not judged). Above one CCD, N is reported: on the AX102 (2 CCDs
# x 8 cores) N = 16 crosses the chiplets, and ~0.70 there is accepted for two-chiplet parts.
# machine.json's ccd_cores comes from the L3 topology; without it (a machine.json from
# before D238, or no L3 in sysfs) every N is judged.
machine = load("machine.json") or {}
ccd = machine.get("ccd_cores")
# D238 (Q4): efficiency against a fixed single-shard rate as well as each run's own, reported
# only. 417,922 commits/s: scaling-1.json's single-shard rate (pinned, pre-split, inline, 16
# in flight) on the AX102, main 9ff02d00, the pre-gate check of 2026-10-13
# (.coord results-precheck-20261013).
SINGLE_SHARD_REF = 417_922
scal = []
for n in (1, 4, 8, 16):
    d = load(f"scaling-{n}.json")
    if d and d.get("scaling"):
        s = d["scaling"]
        # The stall breakdown (D219): the N-shard run's length, the share of its shard time
        # spent in write stalls, its flushes and compactions, and its longest operation.
        multi = next((r for r in d.get("results", []) if "inline" in r.get("store_config", "")
                      and r.get("threads") == n), None)
        brk = ""
        if multi and n > 1:
            st = multi.get("detail", {}).get("stalls") or {}
            stalled = st.get("write_stall_nanos", 0) / max(multi["run_secs"] * n * 1e9, 1)
            brk = (f"; {multi['run_secs']:.1f} s, stalled {stalled:.0%} of shard time, "
                   f"{st.get('flushes', 0)} flushes, {st.get('compactions', 0)} compactions, "
                   f"max {multi['max_ns'] / 1e6:.1f} ms")
        fixed = s["multi_shard_throughput"] / (n * SINGLE_SHARD_REF)
        scal.append((n, s["efficiency"], s["max_share"], brk, fixed, ccd is None or n <= ccd))
scal_judged = [x for x in scal if x[5]]
over = [x for x in scal if not x[5]]
where = (f"one CCD is {ccd} physical cores, from the L3 topology" if ccd is not None
         else "the CCD size is unknown, so every N is judged")
SCALING_TARGET = f"Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; {where})"
if scal_judged:
    detail = "; ".join(f"N={n}: {e:.2f}" + (f" (busiest {m:.0%}{b})" if n > 1 else "")
                       for n, e, m, b, _, _ in scal_judged)
    rows.append((SCALING_TARGET, detail, verdict(all(e >= 0.8 for _, e, *_ in scal_judged)), "scaling-N.json"))
else:
    rows.append((SCALING_TARGET, "", verdict(None), "scaling-N.json"))
if over:
    rows.append(("Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts)",
                 "; ".join(f"N={n}: {e:.2f} (busiest {m:.0%}{b})" for n, e, m, b, _, _ in over),
                 "-", "scaling-N.json"))
if scal:
    rows.append((f"Thread-per-core scaling against a fixed single-shard rate, {SINGLE_SHARD_REF:,} commits/s "
                 "(reported, not judged; D238)",
                 "; ".join(f"N={n}: {f:.2f}" for n, _, _, _, f, _ in scal), "-", "scaling-N.json"))
# D235: the judged scaling runs pin each shard's thread; the same runs unpinned are reported.
unp = [(n, (load(f"scaling-{n}.json") or {}).get("scaling_unpinned")) for n in (4, 8, 16)]
unp = [(n, s) for n, s in unp if s]
if unp:
    rows.append(("Thread-per-core scaling, shard threads unpinned (reported, not judged; D235)",
                 "; ".join(f"N={n}: {s['efficiency']:.2f}" for n, s in unp), "-", "scaling-N.json"))

# Balancer placement (D231): open-loop writes at a fixed rate; settled within the bound, the
# busiest shard <= 1.25/N once settled, no tablet change while measuring, the rate sustained.
plc = []
for n in (4, 8, 16):
    d = load(f"placement-{n}.json")
    if d and d.get("placement"):
        p = d["placement"]
        settled = p.get("settle_secs")
        ok = (settled is not None and p["changes_measured"] == 0
              and p["max_share"] <= 1.25 / n and p["achieved_rate"] >= 0.95 * p["offered_rate"])
        plc.append((n, p, ok))
if plc:
    detail = "; ".join(
        f"N={n}: busiest {p['max_share']:.1%} (max {1.25 / n:.1%}), "
        + (f"settled {p['settle_secs']:.0f} s" if p.get("settle_secs") is not None
           else f"not settled in {p['settle_bound_secs']:.0f} s")
        + f", {p['changes_measured']} changes, {p['achieved_rate'] / 1e3:.0f}K of {p['offered_rate'] / 1e3:.0f}K writes/s"
        for n, p, _ in plc)
    rows.append(("Balancer placement: busiest ≤ 1.25/N once settled (D231)", detail,
                 verdict(all(ok for *_, ok in plc)), "placement-N.json"))
else:
    rows.append(("Balancer placement: busiest ≤ 1.25/N once settled (D231)", "", verdict(None), "placement-N.json"))

# Within 1.5x of RocksDB on every workload: throughput and p99, both runs.
gap_rows, gap_ok = [], None
for run in ("all-uring-1.json", "all-uring-2.json"):
    ph = {r["workload"]: r for r in results(run, "pigeonhole") if default_config(r)}
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

# Contended cross-shard commits (D246), on the default config in both `all` runs: 32 clients,
# half the commits across 2-4 of 16 partitions. Judged: within 1.5x of RocksDB (throughput
# and p99, its commits one multi-key WriteBatch); tablet changes on at least 0.8x the same
# build with them off; at most 0.1 wasted 2PC rounds per cross-shard commit; no `Busy`. Until ICR 0043's
# counters land, wasted rounds are inferred as perf2 did: sequence numbers used while
# measuring minus the commits (`seqnos_used`). The cross-shard commit p99 is reported; its
# bound is the owner's, from D245's numbers.
XSHARD_P99_BOUND_US = None  # TODO(D246): the owner sets it from D245's numbers
XSHARD_TARGET = ("Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off; "
                 "≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 bound TODO (D245)")


def xshard(run):
    """D246's figures for one `all` run, or None when the run lacks one of its three records."""
    rs = results(run, workload=XSHARD)
    on = next((r for r in rs if r["store"] == "pigeonhole" and "tablets=on" in r.get("store_config", "")), None)
    off = next((r for r in rs if r["store"] == "pigeonhole" and "tablets=on" not in r.get("store_config", "")), None)
    rk = next((r for r in rs if r["store"] == "rocksdb"), None)
    if not (on and off and rk):
        return None
    cross = op(on, "cross-shard commit") or {}
    commits = cross.get("count", 0) + (op(on, "put") or {}).get("count", 0)
    # Wasted rounds: the engine's refused two-phase rounds (ICR 0043, Metrics::cross_shard,
    # in the stall counters); the seqnos used beyond one per commit (perf2's inference) are
    # kept beside them as a cross-check, and stand in for runs from before the counters.
    st = on.get("detail", {}).get("stalls") or {}
    seqnos = on.get("detail", {}).get("seqnos_used")
    n = cross.get("count") or 0
    inferred = None if seqnos is None or not n else max(seqnos - commits, 0) / n
    counted = st["xshard_refused_rounds"] / n if n and "xshard_refused_rounds" in st else None
    wasted = counted if counted is not None else inferred
    reasons = (f"below floor {st.get('xshard_refused_below_floor', 0)}, move raise {st.get('xshard_refused_move_raise', 0)}, "
               f"moved {st.get('xshard_refused_moved', 0)}, room {st.get('xshard_refused_room', 0)}"
               if counted is not None else "")
    check = f"{inferred:.3f}" if inferred is not None else "-"
    how = (f"refused rounds, ICR 0043; refusals: {reasons}; seqno inference {check}"
           if counted is not None else "inferred from seqnos")
    x = {
        "tput": rk["throughput"] / on["throughput"],
        "p99": on["p99_ns"] / max(rk["p99_ns"], 1),
        "on_off": on["throughput"] / max(off["throughput"], 1e-9),
        "wasted": wasted,
        "how": how,
        "cross_p99_us": us(cross["p99_ns"]) if cross else None,
        "on": on["throughput"],
        "busy": on.get("detail", {}).get("busy_retries", 0),
    }
    x["ok"] = (x["tput"] <= 1.5 and x["p99"] <= 1.5 and x["on_off"] >= 0.8
               and wasted is not None and wasted <= 0.1 and x["busy"] == 0
               and (XSHARD_P99_BOUND_US is None or (x["cross_p99_us"] or 0) < XSHARD_P99_BOUND_US))
    return x


xs = [(run[:-5], xshard(run)) for run in ("all-uring-1.json", "all-uring-2.json")]
if all(x for _, x in xs):
    detail = "; ".join(
        f"{name}: {rate(x['on'])} ops/s, RocksDB {x['tput']:.2f}× throughput, p99 {x['p99']:.2f}×; "
        f"tablets on/off {x['on_off']:.2f}; wasted rounds {x['wasted']:.3f} per cross-shard commit ({x['how']}); "
        f"Busy {x['busy']}; cross-shard p99 {x['cross_p99_us']:.0f} µs"
        for name, x in xs)
    rows.append((XSHARD_TARGET, detail, verdict(all(x["ok"] for _, x in xs)), "all-uring-1.json, all-uring-2.json"))
else:
    rows.append((XSHARD_TARGET, "", verdict(None), "all-uring-1.json, all-uring-2.json"))

# The contended step (D247, perf287's #406 audit): 16 client threads, the default options
# (the default I/O backend), runs of 60 s or more, against RocksDB and SQLite at two
# durabilities (owner decision): `buffered`, D108's parity (Pigeonhole Buffered, RocksDB's
# WAL unsynced, SQLite NORMAL), and `default` (Pigeonhole's GroupSync, the others fsyncing
# every write). Every workload below is judged within 1.5x of each store, throughput and
# p99, at both levels; cross-shard-contention is judged as D246 judges it in `all`, at both.
CONTENDED_LEVELS = [("buffered", "D108 parity, buffered"), ("default", "default durability, GroupSync")]
# ycsb-f's read-modify-write is atomic in every runner (Pigeonhole: commit_if, retried on a
# lost race; RocksDB and fjall: a row lock; SQLite: an immediate transaction).
# counters: Pigeonhole's counter family against RocksDB's merge operator and SQLite's upsert.
CONTENDED_JUDGED = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-e", "ycsb-f", "sparse-wide", "time-series-ttl", XSHARD,
                    "counters"]
CONTENDED_AGAINST = ["rocksdb", "sqlite-eav"]


def contended(level, w):
    """{store: (other/Pigeonhole throughput, Pigeonhole/other p99)} and Pigeonhole's run for
    one workload at one level, or None when Pigeonhole's run is missing."""
    name = f"contended-{level}-{w}.json"
    ph = next((r for r in results(name, "pigeonhole", w) if default_config(r)), None)
    if not ph:
        return None
    vs = {}
    for store in CONTENDED_AGAINST:
        o = next(iter(results(name, store, w)), None)
        if o:
            vs[store] = (o["throughput"] / ph["throughput"], ph["p99_ns"] / max(o["p99_ns"], 1))
    return ph, vs


cont = {}  # (level, workload) -> (pigeonhole run, {store: (tput, p99)}, ok)
for level, _ in CONTENDED_LEVELS:
    for w in CONTENDED_JUDGED:
        c = contended(level, w)
        if c:
            ph, vs = c
            ok = len(vs) == len(CONTENDED_AGAINST) and all(t <= 1.5 and l <= 1.5 for t, l in vs.values())
            cont[(level, w)] = (ph, vs, ok)
for level, label in CONTENDED_LEVELS:
    target = f"Contended, 16 threads on the default options, {label} (D247): within 1.5× of RocksDB and SQLite"
    got = [(w, cont[(level, w)]) for w in CONTENDED_JUDGED if (level, w) in cont]
    if not got:
        rows.append((target, "", verdict(None), f"contended-{level}-*.json"))
        continue
    missing = [w for w in CONTENDED_JUDGED if (level, w) not in cont]
    out = [f"{w} (" + ", ".join(f"{s.split('-')[0]} {t:.2f}×/p99 {l:.2f}×" for s, (t, l) in vs.items()
                                if t > 1.5 or l > 1.5) + ")"
           for w, (ph, vs, ok) in got if not ok]
    detail = ("outside: " + "; ".join(out)) if out else "all within"
    if missing:
        detail += f"; missing: {', '.join(missing)}"
    rows.append((target, detail, verdict(not out and not missing), f"contended-{level}-*.json"))
    xc = xshard(f"contended-{level}-{XSHARD}.json")
    xt = XSHARD_TARGET.replace("(D246)", f"at 16 threads, {label} (D246, D247)")
    if xc:
        rows.append((xt,
                     f"{rate(xc['on'])} ops/s, RocksDB {xc['tput']:.2f}× throughput, p99 {xc['p99']:.2f}×; "
                     f"tablets on/off {xc['on_off']:.2f}; wasted rounds {xc['wasted']:.3f} per cross-shard commit "
                     f"({xc['how']}); Busy {xc['busy']}; cross-shard p99 {xc['cross_p99_us']:.0f} µs",
                     verdict(xc["ok"]), f"contended-{level}-{XSHARD}.json"))
    else:
        rows.append((xt, "", verdict(None), f"contended-{level}-{XSHARD}.json"))

# The contended step's reported rows (D247): not judged, but a row more than 3x worse than its
# nearest judged row gets an issue. "Worse" compares the stores' ratio: the reported row's
# RocksDB/Pigeonhole throughput against its judged neighbour's at the same level.
CONTENDED_REPORTED = [("transfer", XSHARD)]  # (reported workload, nearest judged workload)
reported = []  # (level label, workload, pigeonhole run, {store: (tput, p99)}, flag)
for level, label in CONTENDED_LEVELS:
    for w, near in CONTENDED_REPORTED:
        c = contended(level, w)
        if not c:
            continue
        ph, vs = c
        flag = ""
        judged = cont.get((level, near))
        if judged and "rocksdb" in vs and "rocksdb" in judged[1]:
            mine, theirs = vs["rocksdb"][0], judged[1]["rocksdb"][0]
            if mine > 3 * theirs:
                flag = f"**> 3× worse than {near} against RocksDB ({mine:.2f}× vs {theirs:.2f}×): file an issue**"
        reported.append((label, w, ph, vs, flag))
for label, w, ph, vs, flag in reported:
    d = ph.get("detail", {})
    conflicts = d.get("txn_conflicts")
    per = f"; {conflicts / max(ph['operations'], 1):.3f} conflicts per {w}" if conflicts is not None else ""
    ratios = ", ".join(f"{s.split('-')[0]} {t:.2f}× throughput, p99 {l:.2f}×" for s, (t, l) in vs.items())
    rows.append((f"Contended {w}, 16 threads on the default options, {label} (reported, not judged; D247)",
                 f"{rate(ph['throughput'])} ops/s, p99 {us(ph['p99_ns']):.0f} µs{per}; {ratios}"
                 + (f"; {flag}" if flag else ""), "-", f"contended-*-{w}.json"))

rep = status("all-uring-compare.status")
# D243: runs shorter than 10 s are reported, not judged; D233: pairs whose measured phases did
# different background work are noted.
rep_detail = []
try:
    with open(path("all-uring-compare.txt")) as f:
        text = f.read()
    rep_fails = text.count("**FAIL**")
    short = text.count("**Not judged (D243")
    bg = text.count("**Background work differs:**")
    if rep_fails:
        rep_detail.append(f"{rep_fails} metrics outside tolerance")
    if short:
        rep_detail.append(f"{short} runs shorter than 10 s, not judged (D243)")
    if bg:
        rep_detail.append(f"{bg} with different background work (D233)")
except OSError:
    pass
rows.append(("Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243)",
             "; ".join(rep_detail), verdict(None if rep is None else rep == 0),
             "all-uring-compare.txt"))
p2 = [status(f"{n}.status") for n in ("phase2-gate", "phase2-gate-2")]
rows.append(("Phase 2 floors hold (D193 sparse-wide gate, both runs)", "",
             verdict(None if None in p2 else all(s == 0 for s in p2)),
             "phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log"))


def eav_ratios(name):
    """Pigeonhole's row-read and scan p99 over SQLite EAV's in one sparse-wide gate run
    (#387), or None if the run did not produce both."""
    run = load(f"{name}.json")
    if not run:
        return None
    p99 = {}
    for r in run.get("results", []):
        if r.get("workload") != "sparse-wide":
            continue
        p99[r["store"]] = {t["op"]: t["stats"]["p99_ns"]
                           for t in (r.get("detail") or {}).get("by_type", [])}
    ph, eav = p99.get("pigeonhole"), p99.get("sqlite-eav")
    if not ph or not eav or not all(op in ph and op in eav for op in ("row read", "scan")):
        return None
    return {op: ph[op] / eav[op] for op in ("row read", "scan")}


# A verdict needs both runs: one run varies by about 5%, and #387 sits near the line.
ratios = [eav_ratios(n) for n in ("phase2-gate", "phase2-gate-2")]
if all(ratios):
    shown = "; ".join(f"run {i + 1}: row read {r['row read']:.2f}×, scan {r['scan']:.2f}×"
                      for i, r in enumerate(ratios))
    worst = max(v for r in ratios for v in r.values())
    rows.append(("#387: row-read and scan p99 within 1.5× of SQLite EAV", shown,
                 verdict(worst < 1.5), "phase2-gate.json, phase2-gate-2.json"))
else:
    rows.append(("#387: row-read and scan p99 within 1.5× of SQLite EAV",
                 "not run (needs both gate runs)", verdict(None),
                 "phase2-gate.json, phase2-gate-2.json"))
rows.append(("D193 instruction ceilings", "measured by CI's instructions job, not here", "see CI", "-"))

try:
    with open(path("timedout.txt")) as f:
        timedout = [l.strip() for l in f if l.strip()]
except OSError:
    timedout = []
print("# Phase 3 gate window: summary\n")
print(f"- **Machine:** {machine.get('cpu', '?')}, {machine.get('cpus', '?')} CPUs, {machine.get('memory', '?')}")
print(f"- **Kernel:** {machine.get('kernel', '?')}; **drive:** {machine.get('drive', '?')}; **mount:** {machine.get('mount', '?')}")
print(f"- **Commit:** {machine.get('commit', '?')}; **scale:** {machine.get('scale', '?')}")
l3 = machine.get("l3_cores")
print(f"- **CCD (D238):** " + (f"{ccd} physical cores per L3 (L3s: {', '.join(map(str, l3))})" if ccd is not None
                               else "not recorded (machine.json from before D238, or no L3 in sysfs)"))
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

if cont:
    # The two levels side by side (owner decision): per store, its throughput over
    # Pigeonhole's and Pigeonhole's p99 over its (> 1: the other store ahead).
    print("\n## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)\n")
    print("| Workload | " + " | ".join(f"{lvl}: Pigeonhole ops/s | {lvl}: vs RocksDB (tput, p99) | "
                                     f"{lvl}: vs SQLite (tput, p99) | {lvl}: within 1.5×"
                                     for lvl, _ in CONTENDED_LEVELS) + " |")
    print("|---|" + "--:|---|---|---|" * len(CONTENDED_LEVELS))
    for w in CONTENDED_JUDGED:
        cells = []
        for level, _ in CONTENDED_LEVELS:
            c = cont.get((level, w))
            if not c:
                cells += ["-"] * 4
                continue
            ph, vs, ok = c
            pair = lambda s: f"{vs[s][0]:.2f}, {vs[s][1]:.2f}" if s in vs else "-"
            cells += [f"{ph['throughput']:.0f}", pair("rocksdb"), pair("sqlite-eav"), "yes" if ok else "**no**"]
        print(f"| {w} | " + " | ".join(cells) + " |")

print("\n## Decisions this window settles\n")
# The row cache (D216: on by default, amending D201): off (row-cache step) against the
# default (latency step). Hit rates from the measured phase's counters.
def hit_rate(r):
    st = r.get("detail", {}).get("stalls") or {}
    h, m = st.get("row_cache_hits", 0), st.get("row_cache_misses", 0)
    return f"{100 * h / (h + m):.0f}%" if h + m else "-"
rc_rows = []
for w in ("ycsb-c", "ycsb-a"):
    on = results(f"latency-{w}.json", "pigeonhole")
    off = results(f"row-cache-off-{w}.json", "pigeonhole")
    if off and on:
        rc_rows.append((w, off[0], on[0]))
if rc_rows:
    print("**Row cache (D216):** off (row-cache step) against the default, on (latency step).\n")
    print("| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |")
    print("|---|---|---|---|---|")
    for w, a, b in rc_rows:
        print(f"| {w} | {a['throughput']:.0f} → {b['throughput']:.0f} | {us(a['p50_ns']):.2f} → {us(b['p50_ns']):.2f} µs | {us(a['p99_ns']):.1f} → {us(b['p99_ns']):.1f} µs | {hit_rate(b)} |")
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
            # ICR 0027's counters (#480): run totals in the stall detail, per operation here.
            stalls = r.get("detail", {}).get("stalls", {})
            ops = max(r.get("operations", 0), 1)
            parks = ", ".join(f"{k} {stalls[k] / ops:.2f}"
                              for k in ("shard_parks", "shard_wakes", "commit_parks") if k in stalls)
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
pu = {r["workload"]: r for r in results("all-uring-1.json", "pigeonhole") if default_config(r)}
pp = {r["workload"]: r for r in results("all-pread.json", "pigeonhole") if default_config(r)}
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
        # Since D222 the step runs the default only (16 blocks, unmerged), with no readahead field.
        groups.setdefault((l["io"], l["direct"], l.get("readahead") or "16 (D222)"), []).append(l["mib_per_sec"])
    for (io, d, ra), v in sorted(groups.items()):
        print(f"| {io} | {d} | {ra} | {statistics.median(v):.0f} |")
# The WAL pin bound's default (#527): wal_pin_bytes at 2x or 4x the memtable budget.
wp = lines("wal-pin.jsonl")
wp_w = [l for l in wp if l.get("mode") == "write"]
wp_r = [l for l in wp if l.get("mode") == "reopen"]
if wp_w:
    print("\n**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature "
          "commits (#175's workload), then a reopen after an exit without close. Medians over rounds.\n")
    print("| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | "
          "WAL at the crash | Reopen (replay) | First read |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    med = lambda rows, k: statistics.median(r[k] for r in rows) if rows else float("nan")
    mib = lambda b: f"{b / (1 << 20):.0f} MiB"
    for mult in sorted({l["mult"] for l in wp_w}):
        w = [l for l in wp_w if l["mult"] == mult]
        r = [l for l in wp_r if l["mult"] == mult]
        reopen = f"{med(r, 'open_ms'):.0f} ms" if r else "-"
        first = f"{med(r, 'first_read_us'):.0f} µs" if r else "-"
        print(f"| {mult}× budget | {med(w, 'commits_per_sec'):.0f} | {med(w, 'unpin_passes'):.0f} | "
              f"{med(w, 'small_flushes'):.0f} | {med(w, 'flushes'):.0f} | {mib(med(w, 'max_wal_bytes'))} | "
              f"{mib(med(w, 'wal_bytes_at_crash'))} | {reopen} | {first} |")
    print("\nThe WAL files' size counts whole preallocated segments (the disk the WAL holds), not live "
          "records; the reopen replays what the crash left past each stream's checkpoint.")

try:
    with open(path("durations.tsv")) as f:
        steps = [l.rstrip("\n").split("\t") for l in f if l.strip()]
    # name, minutes, scale, and (since the resume fix) the commit the step ran at.
    commits = sorted({s[3] for s in steps if len(s) > 3} | ({"(not recorded)"} if any(len(s) < 4 for s in steps) else set()))
    if len(commits) > 1:
        print(f"\n**Mixed commits:** the steps ran at {', '.join(commits)} (a resumed window); see the table below.")
    print("\n## Step durations\n")
    print("| Step | Minutes | Commit |")
    print("|---|---:|---|")
    for s in steps:
        print(f"| {s[0]} | {s[1]} | {s[3] if len(s) > 3 else '(not recorded)'} |")
    print(f"| **total** | **{sum(int(s[1]) for s in steps)}** | |")
except OSError:
    pass
