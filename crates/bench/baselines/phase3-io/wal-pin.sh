#!/usr/bin/env bash
# The wal-pin step of the Phase 3 gate-window run (#527): decides the `wal_pin_bytes`
# default, 2× or 4× the memtable budget. For each, `examples/walpin.rs` writes slots of mixed
# temperature (#175's workload: 4 shards, 32 slots drawn Zipf, one commit in 10 cross-shard,
# 8 clients, 1 KB values) and exits without closing, as a crash leaves the WAL; then reopens
# (replaying it), reads one cell and closes. It records the write path (commits/s, unpin
# passes, the small memtables they flushed, all flushes, the WAL files' size on disk at
# their largest and at the crash) and the open after the crash.
#
#   crates/bench/baselines/phase3-io/wal-pin.sh DATA_DIR [OUT.jsonl] [COMMITS] [ROUNDS]
#
# Linux, on the reference machine (#405) with nothing else running. DATA_DIR must be on the
# NVMe under test. The first line of OUT.jsonl records the machine; then, per round and
# multiple, one `write` and one `reopen` line.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
data="${1:?usage: wal-pin.sh DATA_DIR [OUT.jsonl] [COMMITS] [ROUNDS]}"
out="${2:-wal-pin-$(date +%Y%m%d-%H%M%S).jsonl}"
commits="${3:-4000000}"
rounds="${4:-2}"
[[ "$(uname)" == Linux ]] || { echo "wal-pin.sh: the gate runs on Linux (#405)" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --example walpin --manifest-path "$root/Cargo.toml"
bin="$root/target/release/examples/walpin"
mkdir -p "$data/walpin"

python3 - "$out" "$data" <<'PY'
import json, os, platform, subprocess, sys
out, data = sys.argv[1:]
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
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "load": sh("cat /proc/loadavg"),
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
PY

for round in $(seq 1 "$rounds"); do
    for mult in 2 4; do
        run="$data/walpin/r$round-m$mult"
        [[ ! -e "$run" ]] || rm -r -- "$run"
        "$bin" write "$run" "$mult" "$commits" | tee /dev/stderr | tail -1 >> "$out"
        # Nothing else in the page cache's way: the replay reads what the crash left.
        sync
        "$bin" reopen "$run" "$mult" | tee /dev/stderr | tail -1 >> "$out"
        rm -r -- "$run"
    done
done
echo "wrote $out"
