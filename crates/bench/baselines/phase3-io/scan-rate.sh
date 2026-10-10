#!/usr/bin/env bash
# The scan-rate step of the Phase 3 gate-window run (#405, #29; spec Goals: ordered row scan,
# single family, > 1 GB/s decoded per core from cache). One thread, pinned to one core, scans
# whole tables the block cache (or the memtable) holds, for each shape of
# `examples/scanrate.rs` (narrow, wide, small and large cells; compacted and in the memtable),
# RUNS times. Decoded bytes are the row key, qualifier and value of every returned cell.
#
#   crates/bench/baselines/phase3-io/scan-rate.sh [OUT.jsonl] [RUNS] [CORE]
#
# Linux, on the reference machine (#405) with nothing else running. The stores live in the
# system temp directory: the scans read from the cache, not the drive. The first line of
# OUT.jsonl records the machine; then one line per shape and run, as `scanrate rate` prints
# it, plus the run number.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
out="${1:-scan-rate-$(date +%Y%m%d-%H%M%S).jsonl}"
runs="${2:-3}"
core="${3:-2}"
[[ "$(uname)" == Linux ]] || { echo "scan-rate.sh: the gate runs on Linux (#405)" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --example scanrate --manifest-path "$root/Cargo.toml"
bin="$root/target/release/examples/scanrate"

python3 - "$out" "$core" <<'PY'
import json, os, platform, subprocess, sys
out, core = sys.argv[1:]
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
    "core": int(core),
    "governor": sh(f"cat /sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor 2>/dev/null"),
    "memory": sh("grep MemTotal /proc/meminfo"),
    "load": sh("cat /proc/loadavg"),
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
PY

for run in $(seq 1 "$runs"); do
    # Every shape in both places, 5 passes each; the shard thread and the scanning thread
    # share the pinned core, and the shard is idle while the scans run.
    taskset -c "$core" "$bin" rate 5 | while read -r line; do
        echo "$line" | python3 -c '
import json, sys
d = json.loads(sys.stdin.read())
d["run"] = int(sys.argv[1])
print(json.dumps(d))' "$run" | tee -a "$out"
    done
done

python3 - "$out" <<'PY'
import collections, json, statistics, sys
rows = collections.defaultdict(list)
for line in open(sys.argv[1]):
    d = json.loads(line)
    if not d.get("machine"):
        rows[(d["shape"], d["place"])].append(d)
print("| shape | place | value | median GB/s | cells/s |")
print("|---|---|--:|--:|--:|")
for (shape, place), ds in rows.items():
    gb = statistics.median(d["median_gb_s"] for d in ds)
    cs = statistics.median(d["median_cells_s"] for d in ds)
    print(f"| {shape} | {place} | {ds[0]['value_len']} B | {gb:.2f} | {cs / 1e6:.1f}M |")
PY
echo "wrote $out"
