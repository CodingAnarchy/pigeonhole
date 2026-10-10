#!/usr/bin/env bash
# The cold-get step of the Phase 3 gate-window run (#405, #87): point gets of rows that neither
# the block cache nor the page cache holds (spec: a cold get is one data-block I/O, about 20
# to 80 µs on NVMe), across the I/O backends (#402), with direct I/O (#403) and buffered under
# a memory cap.
#
#   crates/bench/baselines/phase3-io/cold-get.sh DATA_DIR [OUT.jsonl] [MIB]
#
# Linux, as root (it empties the page cache and starts cgroup scopes), with systemd, on the
# reference machine (#405) with nothing else running. DATA_DIR must be on the NVMe under test.
# The table (MIB mebibytes of 1 KiB cells, default 65536) is loaded once and kept, so a rerun
# skips the load. The store need not exceed RAM (the approved plan for #87):
#   - direct: `PIGEONHOLE_DIRECT=1` reads every SST block past the page cache;
#   - capped: buffered reads in a cgroup v2 scope of `COLDGET_CAP` memory (default 8G, swap
#     off), page cache emptied first, so most of the table cannot be cached: the default
#     path's cost.
# Each `get` runs 100,000 warm-up gets, 1,000,000 cold gets of present rows and 1,000,000 of
# absent rows, on 1 thread (the latency target) and 16 (latency under concurrent reads).
# Each line of OUT.jsonl is one phase of one run, as `examples/coldget.rs` prints it, plus
# the run's settings; the first line records the machine. Device reads per get come from
# `/sys/block/DEV/stat` for the device under DATA_DIR (an md array counts its own reads).
#
# Duration, for sizing the rental window (estimates from a 1 GiB load on a laptop, scaled):
#   - load (first run only): about 15-30 minutes for 64 GiB, including the wait for
#     background compaction to go quiet;
#   - matrix: 2 backends x 2 modes x 2 thread counts x 3 runs = 24 runs; a 1-thread run is
#     about 1.5-2 minutes at device latency, a 16-thread run under 30 s: about 25 minutes.
#   - total: about 45 minutes, budget 1 hour.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
data="${1:?usage: cold-get.sh DATA_DIR [OUT.jsonl] [MIB]}"
out="${2:-cold-get-$(date +%Y%m%d-%H%M%S).jsonl}"
mib="${3:-65536}"
cap="${COLDGET_CAP:-8G}"
[[ "$(uname)" == Linux ]] || { echo "cold-get.sh: Linux only (page cache, cgroups)" >&2; exit 2; }
[[ "$(id -u)" == 0 ]] || { echo "cold-get.sh: run as root (page cache, cgroups)" >&2; exit 2; }
command -v systemd-run >/dev/null || { echo "cold-get.sh: needs systemd-run" >&2; exit 2; }

cargo build --release -p pigeonhole-bench --example coldget --manifest-path "$root/Cargo.toml"
bin="$root/target/release/examples/coldget"

drop_caches() {
    sync
    echo 3 > /proc/sys/vm/drop_caches
}

mkdir -p "$data"
# The block device under DATA_DIR, for `/sys/block/DEV/stat`: a partition's parent disk, or
# the device itself (an md array, a device-mapper volume).
dev="$(df --output=source "$data" | tail -1)"
blockdev="$(lsblk -ndo PKNAME "$dev" 2>/dev/null || true)"
[[ -n "$blockdev" ]] || blockdev="$(lsblk -ndo KNAME "$dev")"
[[ -e "/sys/block/$blockdev/stat" ]] || { echo "cold-get.sh: no /sys/block/$blockdev/stat" >&2; exit 2; }

# The machine, kernel, drive and file system, once.
python3 - "$out" "$dev" "$data" "$mib" "$cap" "$blockdev" <<'EOF'
import json, os, platform, subprocess, sys
out, dev, data, mib, cap, blockdev = sys.argv[1:]
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
    "blockdev": blockdev,
    "drive": sh(f"lsblk -ndo MODEL,SIZE /dev/{blockdev}; lsblk -no NAME,MODEL,SIZE /dev/{blockdev}"),
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "io_uring_disabled": sh("cat /proc/sys/kernel/io_uring_disabled 2>/dev/null"),
    "memlock": sh("ulimit -l"),
    "load": sh("cat /proc/loadavg"),
    "table_mib": int(mib),
    "cap": cap,
}
with open(out, "w") as f:
    f.write(json.dumps(machine) + "\n")
EOF

if [[ ! -e "$data/coldget.rows" ]]; then
    echo "loading $mib MiB into $data"
    "$bin" load "$data" "$mib"
fi

start=$(date +%s)
for io in pread uring; do
    for mode in direct capped; do
        for threads in 1 16; do
            for run in 1 2 3; do
                if [[ "$mode" == direct ]]; then
                    lines=$(PIGEONHOLE_IO=$io PIGEONHOLE_DIRECT=1 COLDGET_THREADS=$threads \
                        COLDGET_BLOCKDEV=$blockdev "$bin" get "$data")
                else
                    drop_caches
                    # A transient scope: the cap covers the process and the page cache it
                    # fills; the command inherits this environment.
                    lines=$(PIGEONHOLE_IO=$io PIGEONHOLE_DIRECT=0 COLDGET_THREADS=$threads \
                        COLDGET_BLOCKDEV=$blockdev systemd-run --scope --quiet \
                        -p MemoryMax="$cap" -p MemorySwapMax=0 "$bin" get "$data")
                fi
                echo "$lines" | python3 -c '
import json, sys
mode, cap, run = sys.argv[1], sys.argv[2], int(sys.argv[3])
for line in sys.stdin:
    d = json.loads(line)
    d["mode"], d["run"] = mode, run
    if mode == "capped":
        d["cap"] = cap
    print(json.dumps(d))' "$mode" "$cap" "$run" | tee -a "$out"
            done
        done
    done
done
echo "matrix took $(( ($(date +%s) - start) / 60 )) min; wrote $out"
