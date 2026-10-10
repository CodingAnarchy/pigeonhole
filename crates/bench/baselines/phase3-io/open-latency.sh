#!/usr/bin/env bash
# The open-latency step of the Phase 3 gate-window run (#405, #158; spec: open to first read
# < 5 ms): opens a small existing database on the NVMe under test, reads one cell and closes,
# RUNS times per backend, and records the median, p99 and where the open's time went (each
# VFS operation, through a timing wrapper; `examples/openlat.rs`).
#
#   crates/bench/baselines/phase3-io/open-latency.sh DATA_DIR [OUT.jsonl] [RUNS]
#
# Linux, on the reference machine (#405) with nothing else running. DATA_DIR must be on the
# NVMe under test. The first line of OUT.jsonl records the machine; then one line per backend.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
data="${1:?usage: open-latency.sh DATA_DIR [OUT.jsonl] [RUNS]}"
out="${2:-open-latency-$(date +%Y%m%d-%H%M%S).jsonl}"
runs="${3:-100}"
[[ "$(uname)" == Linux ]] || { echo "open-latency.sh: the gate runs on Linux (#405)" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --example openlat --manifest-path "$root/Cargo.toml"
bin="$root/target/release/examples/openlat"
mkdir -p "$data"

dev="$(df --output=source "$data" 2>/dev/null | tail -1 || true)"
python3 - "$out" "$dev" "$data" <<'PY'
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
    "drive": sh(f"lsblk -ndo MODEL,SIZE $(lsblk -ndo PKNAME {dev} 2>/dev/null || echo {dev})"),
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "load": sh("cat /proc/loadavg"),
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
PY

for io in pread uring; do
    OPENLAT_JSON=1 "$bin" "$data/open-$io" "$runs" "$io" | tee /dev/stderr | tail -1 >> "$out"
done
echo "wrote $out"
