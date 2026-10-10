#!/usr/bin/env bash
# The scaling step of the Phase 3 gate-window run (#405, #154; spec Goals: write throughput
# at N shards ≥ 0.8 × N × one shard). Runs `phdb-bench scaling` (D204: application-owned
# shards, each thread committing the rows its shard owns, against one shard; plus the
# engine-owned synchronous shape at N/2, reported, not gating) at each N, RUNS times, and
# prints the median efficiency, busiest-shard share and visibility waits per operation
# (D19, ICR 0023) for each N.
#
#   crates/bench/baselines/phase3-io/scaling.sh DATA_DIR [OUT_DIR] [RUNS] [SHARDS...]
#
# Linux, on the reference machine (#405, 16 cores) with nothing else running. DATA_DIR must
# be on the NVMe under test. SHARDS defaults to 1 2 4 8 16, RUNS to 3. Each run writes
# OUT_DIR/scaling-N<shards>-run<k>.json (a phdb-bench suite); OUT_DIR/machine.json records
# the machine and OUT_DIR/summary.md the medians. The measured writes grow with N
# (2,000,000 × N, as long again warming up) so every measured phase lasts seconds.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
data="${1:?usage: scaling.sh DATA_DIR [OUT_DIR] [RUNS] [SHARDS...]}"
out="${2:-scaling-$(date +%Y%m%d-%H%M%S)}"
runs="${3:-3}"
shift $(( $# < 3 ? $# : 3 ))
shards=("$@")
[[ ${#shards[@]} -gt 0 ]] || shards=(1 2 4 8 16)
[[ "$(uname)" == Linux ]] || { echo "scaling.sh: the gate runs on Linux (#405)" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --bin phdb-bench --manifest-path "$root/Cargo.toml"
bin="$root/target/release/phdb-bench"
mkdir -p "$data" "$out"

dev="$(df --output=source "$data" 2>/dev/null | tail -1 || true)"
python3 - "$out/machine.json" "$dev" "$data" <<'PY'
import json, os, platform, subprocess, sys
out, dev, data = sys.argv[1:]
def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception as e:
        return f"error: {e}"
machine = {
    "machine": True,
    "kernel": platform.release(),
    "cpu": sh("lscpu | grep 'Model name' | sed 's/.*: *//'"),
    "cpus": os.cpu_count(),
    "threads_per_core": sh("lscpu | grep 'Thread(s) per core' | sed 's/.*: *//'"),
    "memory": sh("grep MemTotal /proc/meminfo"),
    "drive": sh(f"lsblk -ndo MODEL,SIZE $(lsblk -ndo PKNAME {dev} 2>/dev/null || echo {dev})"),
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "io_uring_disabled": sh("cat /proc/sys/kernel/io_uring_disabled 2>/dev/null"),
    "load": sh("cat /proc/loadavg"),
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
PY

for n in "${shards[@]}"; do
    for run in $(seq 1 "$runs"); do
        json="$out/scaling-N$n-run$run.json"
        echo "scaling: $n shards, run $run"
        "$bin" scaling --scale full --shards "$n" --ops $((2000000 * n)) \
            --dir "$data/scaling" --json "$json"
    done
done

python3 - "$out" "$runs" "${shards[@]}" <<'PY' | tee "$out/summary.md"
import json, statistics, sys
out, runs, shards = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
print("| N | Efficiency (median) | Runs | Busiest share | Visibility waits/op | Sync shape (N/2) efficiency |")
print("|--:|--:|--|--:|--:|--:|")
for n in shards:
    gate, sync = [], []
    for k in range(1, runs + 1):
        with open(f"{out}/scaling-N{n}-run{k}.json") as f:
            s = json.load(f)
        gate.append(s["scaling"])
        if s.get("scaling_sync"):
            sync.append(s["scaling_sync"]["efficiency"])
    med = lambda xs: statistics.median(xs) if xs else float("nan")
    effs = [g["efficiency"] for g in gate]
    print(f"| {n} | {med(effs):.2f} | {' '.join(f'{e:.2f}' for e in effs)} "
          f"| {100 * med([g.get('max_share', 0) for g in gate]):.0f}% "
          f"| {med([g.get('visibility_waits_per_op', 0) for g in gate]):.3f} "
          f"| {med(sync):.2f} |")
PY
echo "wrote $out"
