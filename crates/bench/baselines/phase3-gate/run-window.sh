#!/usr/bin/env bash
# The Phase 3 gate-window run (#405, #406): every measurement the gate needs, on the reference
# machine (D5: a Hetzner AX102, rented only for this window), in one resumable run.
#
#   crates/bench/baselines/phase3-gate/run-window.sh DATA_DIR RESULTS_DIR [--scale full|small]
#
# Linux 6.x, as root (the cold steps empty the page cache and start cgroup scopes), with
# nothing else running. DATA_DIR is on the NVMe under test (not the OS drive); RESULTS_DIR
# collects every JSON, log and the summary. Each step writes RESULTS_DIR/STEP.done when it
# finishes, so a rerun (after a reboot, or a fix) skips what is done; delete a .done file to
# run that step again. `--scale small` runs everything at small sizes: the dry run, on any
# Linux box or CI runner, that catches script bugs before rented hours do.
#
# Steps (estimated minutes at full scale, from laptop runs scaled; the log records the real
# ones in RESULTS_DIR/durations.tsv):
#   setup          machine, kernel, drive and file-system checks; machine.json        1
#   build          phdb-bench (rocksdb, sqlite) and the examples                      10
#   phase2-gate    D193 sparse-wide gate against SQLite and RocksDB (run-gate.sh)     40
#   all-uring-1    phdb-bench all vs RocksDB, full, PIGEONHOLE_IO=uring               90
#   all-uring-2    the same again (the reproducibility gate: compare 1 and 2)         90
#   all-pread      the same on pread (#402 PR 6: is io_uring the right default?)     90
#   latency        ycsb-c, group-commit (1/4/16 threads) vs RocksDB, ycsb-a, skewed   45
#   row-cache      ycsb-c and ycsb-a with the row cache on (D201's default)           30
#   scaling        scaling gate (D204) at 1, 4, 8, 16 shards                          30
#   open-latency   open to first read (#158; phase3-io/open-latency.sh)               5
#   cold-get       cold point gets, one I/O (#87; phase3-io/cold-get.sh)              45
#   cold-scan      cold scans, readahead depth and merging (#431; cold-scan.sh)       45
#   scan-cache     ordered scan from cache, GB/s decoded per core (#29)               10
#   report         summary.md (every #406 target: value, pass/fail, source file),
#                  and RESULTS_DIR.tar.gz to copy back                                1
# Total: about 9 hours unattended at full scale.
#
# The results leave the machine as a tarball (no GitHub credentials on a rented box): copy
# it back with the `scp` line the last step prints, and commit it from there.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"

usage() { echo "usage: run-window.sh DATA_DIR RESULTS_DIR [--scale full|small]" >&2; exit 2; }
[[ $# -ge 2 ]] || usage
data="$1"
results="$2"
shift 2
scale=full
while [[ $# -gt 0 ]]; do
    case "$1" in
        --scale) scale="${2:?}"; shift 2 ;;
        *) usage ;;
    esac
done
[[ "$scale" == full || "$scale" == small ]] || usage
[[ "$(uname)" == Linux ]] || { echo "run-window.sh: Linux only" >&2; exit 2; }
[[ "$(id -u)" == 0 ]] || { echo "run-window.sh: run as root" >&2; exit 2; }
mkdir -p "$data" "$results/logs"
data="$(cd "$data" && pwd)"
results="$(cd "$results" && pwd)"

steps=(setup build phase2-gate all-uring-1 all-uring-2 all-pread latency row-cache scaling
    open-latency cold-get cold-scan scan-cache report)
declare -A minutes=([setup]=1 [build]=10 [phase2-gate]=40 [all-uring-1]=90 [all-uring-2]=90
    [all-pread]=90 [latency]=45 [row-cache]=30 [scaling]=30 [open-latency]=5 [cold-get]=45
    [cold-scan]=45 [scan-cache]=10 [report]=1)

bench="$root/target/release/phdb-bench"
examples="$root/target/release/examples"
# Sizes: the gate's at full; a quick pass at small.
if [[ "$scale" == full ]]; then
    cold_get_mib=65536 cold_scan_mib=16384 scan_cache_mib=4096
else
    cold_get_mib=256 cold_scan_mib=256 scan_cache_mib=128
fi

remaining() { # minutes left after step $1
    local after=0 m=0
    for s in "${steps[@]}"; do
        [[ $after == 1 && ! -e "$results/$s.done" ]] && m=$((m + minutes[$s]))
        [[ "$s" == "$1" ]] && after=1
    done
    echo "$m"
}

# Runs step NAME (the function step_NAME) unless it is done, logging its output and time.
run() {
    local name="$1" start end
    if [[ -e "$results/$name.done" ]]; then
        echo "== $name: done earlier, skipped"
        return
    fi
    echo "== $name ($(date -u +%H:%MZ); about ${minutes[$name]} min; then about $(remaining "$name") min left)"
    start=$(date +%s)
    if "step_$name" > >(tee "$results/logs/$name.log") 2>&1; then
        end=$(date +%s)
        printf '%s\t%d\t%s\n' "$name" $(((end - start) / 60)) "$scale" >> "$results/durations.tsv"
        touch "$results/$name.done"
    else
        echo "== $name FAILED (log: $results/logs/$name.log); fix and rerun: done steps are skipped" >&2
        exit 1
    fi
}

step_setup() {
    python3 "$here/machine.py" "$data" "$results/machine.json" "$scale"
}

step_build() {
    cargo build --release -p pigeonhole-bench --features sqlite,rocksdb \
        --manifest-path "$root/Cargo.toml"
    cargo build --release -p pigeonhole-bench --examples --manifest-path "$root/Cargo.toml"
}

step_phase2-gate() {
    if [[ "$scale" == small ]]; then
        echo "phase2-gate runs at full scale only (its baselines are full-scale runs); skipped"
        return
    fi
    # A regression is a verdict (check.py exits 1), recorded for the report; only a run that
    # produced no result stops the window.
    local status=0
    TMPDIR="$data" "$root/crates/bench/baselines/phase2-gate/run-gate.sh" \
        "$results/phase2-gate.json" || status=$?
    [[ -s "$results/phase2-gate.json" ]] || return 1
    echo "$status" > "$results/phase2-gate.status"
}

# `phdb-bench all` against RocksDB, stores under DATA_DIR, on backend $1, into $2.json.
all_vs_rocksdb() {
    PIGEONHOLE_IO="$1" "$bench" all --engine pigeonhole,rocksdb --scale "$scale" \
        --dir "$data/bench" --json "$results/$2.json" --markdown "$results/$2.md"
    rm -r "$data/bench"
}

step_all-uring-1() { all_vs_rocksdb uring all-uring-1; }
step_all-uring-2() {
    all_vs_rocksdb uring all-uring-2
    # The reproducibility gate: every result within tolerance across the two runs. A result
    # out of tolerance is a verdict for the report, not a reason to stop.
    local status=0
    "$bench" compare "$results/all-uring-1.json" "$results/all-uring-2.json" \
        > "$results/all-uring-compare.txt" || status=$?
    cat "$results/all-uring-compare.txt"
    echo "$status" > "$results/all-uring-compare.status"
}
step_all-pread() { all_vs_rocksdb pread all-pread; }

step_latency() {
    local w
    for w in ycsb-c ycsb-a skewed-multi-shard; do
        PIGEONHOLE_IO=uring "$bench" "$w" --engine pigeonhole,rocksdb --scale "$scale" \
            --dir "$data/bench" --json "$results/latency-$w.json"
        rm -r "$data/bench"
    done
    # Durable commits at 1, 4 and 16 client threads (GroupSync; RocksDB fsyncs).
    PIGEONHOLE_IO=uring "$bench" group-commit --engine pigeonhole,rocksdb --scale "$scale" \
        --dir "$data/bench" --json "$results/latency-group-commit.json"
    rm -r "$data/bench"
}

step_row-cache() {
    if ! "$bench" --help | grep -q -- --row-cache; then
        echo "phdb-bench has no --row-cache yet (a bench PR); skipped, not marked done" >&2
        return 1
    fi
    local w
    for w in ycsb-c ycsb-a; do
        PIGEONHOLE_IO=uring "$bench" "$w" --engine pigeonhole --scale "$scale" \
            --row-cache 268435456 --dir "$data/bench" --json "$results/row-cache-$w.json"
        rm -r "$data/bench"
    done
}

step_scaling() {
    local n
    for n in 1 4 8 16; do
        PIGEONHOLE_IO=uring "$bench" scaling --shards "$n" --scale "$scale" \
            --dir "$data/bench" --json "$results/scaling-$n.json"
        rm -r "$data/bench"
    done
}

step_open-latency() {
    "$root/crates/bench/baselines/phase3-io/open-latency.sh" "$data/openlat" \
        "$results/open-latency.jsonl"
}

step_cold-get() {
    if [[ "$scale" == small ]]; then
        export COLDGET_GETS=20000 COLDGET_WARMUP=2000 COLDGET_QUIET_SECS=5
    fi
    "$root/crates/bench/baselines/phase3-io/cold-get.sh" "$data/coldget" \
        "$results/cold-get.jsonl" "$cold_get_mib"
}

step_cold-scan() {
    "$root/crates/bench/baselines/phase3-io/cold-scan.sh" "$data/coldscan" \
        "$results/cold-scan.jsonl" "$cold_scan_mib"
}

step_scan-cache() {
    if ! grep -q 'warm' "$root/crates/bench/examples/coldscan.rs"; then
        echo "coldscan has no warm mode yet (a bench PR); skipped, not marked done" >&2
        return 1
    fi
    [[ -e "$data/scancache/cold.phdb" ]] || "$examples/coldscan" load "$data/scancache" "$scan_cache_mib"
    PIGEONHOLE_IO=uring "$examples/coldscan" warm "$data/scancache" | tee "$results/scan-cache.jsonl"
}

step_report() {
    python3 "$here/report.py" "$results" > "$results/summary.md"
    cat "$results/summary.md"
    local tarball="$results.tar.gz"
    tar -C "$(dirname "$results")" -czf "$tarball" "$(basename "$results")"
    echo "copy back with: scp root@$(hostname -f):$tarball ."
}

for s in "${steps[@]}"; do
    run "$s"
done
echo "== all steps done; results in $results (summary.md)"
