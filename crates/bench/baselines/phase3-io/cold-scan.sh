#!/usr/bin/env bash
# The cold-scan step of the Phase 3 gate-window run (#405): a full scan of a table the block
# cache and the page cache do not hold, across the I/O backends (#402), the scan readahead
# (#402 PR 5; depth and merging are decided from this run, #431) and direct I/O (#403).
#
#   crates/bench/baselines/phase3-io/cold-scan.sh DATA_DIR [OUT.jsonl] [MIB]
#
# Linux, as root (it empties the page cache before every buffered scan), on the reference
# machine (#405) with nothing else running. DATA_DIR must be on the NVMe under test; the table
# (MIB mebibytes, default 16384: larger than any cache the run uses) is loaded once and kept,
# so a rerun skips the load. Each line of OUT.jsonl is one scan, as `examples/coldscan.rs`
# prints it, plus the run's settings; the first line records the machine.
#
# The direct-I/O runs need `PIGEONHOLE_DIRECT` (#403, PR #427); before it merges they run
# buffered on a warm page cache, so run this after it.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
data="${1:?usage: cold-scan.sh DATA_DIR [OUT.jsonl] [MIB]}"
out="${2:-cold-scan-$(date +%Y%m%d-%H%M%S).jsonl}"
mib="${3:-16384}"
[[ "$(uname)" == Linux ]] || { echo "cold-scan.sh: Linux only (it drops the page cache)" >&2; exit 2; }
[[ "$(id -u)" == 0 ]] || { echo "cold-scan.sh: run as root (it drops the page cache)" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --example coldscan --manifest-path "$root/Cargo.toml"
bin="$root/target/release/examples/coldscan"

drop_caches() {
    sync
    echo 3 > /proc/sys/vm/drop_caches
}

# The machine, kernel, drive and file system, once.
dev="$(df --output=source "$data" 2>/dev/null | tail -1 || true)"
python3 - "$out" "$dev" "$data" "$mib" <<'EOF'
import json, os, platform, subprocess, sys
out, dev, data, mib = sys.argv[1:]
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
    "memory": sh("grep MemTotal /proc/meminfo"),
    "device": dev,
    "drive": sh(f"lsblk -ndo MODEL,SIZE $(lsblk -ndo PKNAME {dev} 2>/dev/null || echo {dev})"),
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "io_uring_disabled": sh("cat /proc/sys/kernel/io_uring_disabled 2>/dev/null"),
    "memlock": sh("ulimit -l"),
    "load": sh("cat /proc/loadavg"),
    "table_mib": int(mib),
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
EOF

if [[ ! -e "$data/cold.phdb" ]]; then
    echo "loading $mib MiB into $data"
    "$bin" load "$data" "$mib"
fi

for io in pread uring; do
    for direct in 0 1; do
        for readahead in 0 4 16 4,merge 16,merge; do
            for run in 1 2 3; do
                [[ "$direct" == 1 ]] || drop_caches
                line=$(PIGEONHOLE_IO=$io PIGEONHOLE_DIRECT=$direct PIGEONHOLE_READAHEAD=$readahead \
                    "$bin" scan "$data")
                echo "$line" | python3 -c '
import json, sys
d = json.loads(sys.stdin.read())
d["run"] = int(sys.argv[1])
print(json.dumps(d))' "$run" | tee -a "$out"
            done
        done
    done
done
echo "wrote $out"
