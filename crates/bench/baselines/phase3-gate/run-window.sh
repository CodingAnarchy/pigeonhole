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
# A step that runs past 3x its estimate (at small scale, past its estimate or 20 minutes) is
# taken for a hang: every thread's stack goes to logs/STEP.stacks (gdb), it is killed and
# listed in timedout.txt, and the window moves on.
#
# Steps (estimated minutes at full scale, from laptop runs scaled; the log records the real
# ones in RESULTS_DIR/durations.tsv):
#   setup          machine, kernel, drive and file-system checks; machine.json        1
#   build          phdb-bench (rocksdb, sqlite) and the examples                      10
#   memtable-lookup  memtable 1M/10k point lookup (#17; criterion)                    5
#   phase2-gate    D193 sparse-wide gate against SQLite and RocksDB (run-gate.sh)     40
#   all-uring-1    phdb-bench all vs RocksDB, full, PIGEONHOLE_IO=uring               90
#   all-uring-2    the same again (the reproducibility gate: compare 1 and 2)         90
#   all-pread      the same on pread (#402 PR 6: is io_uring the right default?)     90
#   latency        ycsb-c, group-commit (1/4/16 threads) vs RocksDB, ycsb-a, skewed   45
#   ycsb-a-shards  ycsb-a, one client: default shards vs 1 and 4, and shard spins (D198)  15
#   group-sync-depth  group-commit at group sync depth 1, 2 and unlimited (D207)      15
#   row-cache      ycsb-c and ycsb-a with the row cache on (D201's default)           30
#   scaling        scaling gate (D204) at 1, 4, 8, 16 shards                          30
#   open-latency   open to first read (#158; phase3-io/open-latency.sh)               5
#   cold-get       cold point gets, one I/O (#87; phase3-io/cold-get.sh)              45
#   cold-scan      cold scans, readahead depth and merging (#431; cold-scan.sh)       45
#   scan-cache     ordered scan from cache, GB/s decoded per core (#29)               10
#   report         summary.md (every #406 target: value, pass/fail, source file),
#                  and RESULTS_DIR.tar.gz to copy back                                1
# Total: about 9.75 hours unattended at full scale.
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

steps=(setup build memtable-lookup phase2-gate all-uring-1 all-uring-2 all-pread latency ycsb-a-shards
    group-sync-depth row-cache scaling open-latency cold-get cold-scan scan-cache report)
declare -A minutes=([setup]=1 [build]=10 [memtable-lookup]=5 [phase2-gate]=40 [all-uring-1]=90 [all-uring-2]=90
    [all-pread]=90 [latency]=45 [ycsb-a-shards]=15 [group-sync-depth]=15 [row-cache]=30 [scaling]=30 [open-latency]=5 [cold-get]=45
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
# Every thread's stack of each process in process group $1 (the step's), into file $2:
# gdb, else eu-stack, else SIGQUIT (which at least leaves a core where cores are on).
stacks() {
    local pid comm
    for pid in $(pgrep -g "$1"); do
        comm=$(cat "/proc/$pid/comm" 2>/dev/null) || continue
        case "$comm" in bash | tee | sleep | timeout) continue ;; esac
        echo "=== pid $pid ($comm)" >> "$2"
        if command -v gdb > /dev/null; then
            timeout 120 gdb -p "$pid" -batch -ex 'thread apply all bt' >> "$2" 2>&1 || true
        elif command -v eu-stack > /dev/null; then
            timeout 60 eu-stack -p "$pid" >> "$2" 2>&1 || true
        else
            kill -QUIT "$pid" 2> /dev/null || true
        fi
    done
}

# Runs step NAME (the function step_NAME) unless it is done, logging its output and time. A
# step that outlives its limit (3x its estimate at full scale) is a hang: its stacks are
# taken, it is killed, recorded in timedout.txt and the window moves on, so one hang cannot
# eat the rented hours.
run() {
    local name="$1" start end
    if [[ -e "$results/$name.done" ]]; then
        echo "== $name: done earlier, skipped"
        return
    fi
    local limit=$((minutes[$name] * 3))
    ((limit >= 15)) || limit=15
    if [[ "$scale" == small ]]; then
        limit=$((minutes[$name] > 20 ? minutes[$name] : 20))
    fi
    echo "== $name ($(date -u +%H:%MZ); about ${minutes[$name]} min, limit $limit; then about $(remaining "$name") min left)"
    start=$(date +%s)
    local status=0 pid
    # Its own process group (job control), so the whole step can be inspected and killed.
    set -m
    ("step_$name") > >(tee "$results/logs/$name.log") 2>&1 &
    pid=$!
    set +m
    while kill -0 "$pid" 2> /dev/null; do
        if (($(date +%s) - start >= limit * 60)); then
            echo "== $name TIMED OUT after $limit min; stacks: $results/logs/$name.stacks" \
                | tee -a "$results/logs/$name.log" >&2
            stacks "$pid" "$results/logs/$name.stacks"
            kill -TERM -- "-$pid" 2> /dev/null || true
            sleep 10
            kill -KILL -- "-$pid" 2> /dev/null || true
            wait "$pid" 2> /dev/null || true
            echo "$name" >> "$results/timedout.txt"
            # The killed step's store, set aside (not deleted) so the next step starts clean.
            [[ ! -e "$data/bench" ]] || mv "$data/bench" "$data/bench.timedout.$name.$start"
            return
        fi
        sleep 5
    done
    wait "$pid" || status=$?
    if [[ $status == 0 ]]; then
        end=$(date +%s)
        printf '%s\t%d\t%s\n' "$name" $(((end - start) / 60)) "$scale" >> "$results/durations.tsv"
        touch "$results/$name.done"
    elif [[ $status == "$SKIP" ]]; then
        # A step whose feature the built binary lacks: not done, so a rerun after the
        # rebuild runs it; the window goes on.
        echo "== $name skipped (log: $results/logs/$name.log)"
    else
        echo "== $name FAILED (log: $results/logs/$name.log); fix and rerun: done steps are skipped" >&2
        exit 1
    fi
}

# The exit status of a step that skips itself (EX_TEMPFAIL).
SKIP=75

step_setup() {
    python3 "$here/machine.py" "$data" "$results/machine.json" "$scale"
}

step_build() {
    cargo build --release -p pigeonhole-bench --features sqlite,rocksdb \
        --manifest-path "$root/Cargo.toml"
    cargo build --release -p pigeonhole-bench --examples --manifest-path "$root/Cargo.toml"
}

# #17: memtable point lookup at 1M and 10k entries (criterion; target <= 300 ns at 1M),
# pinned to one core so another core's work does not move it (perf287).
step_memtable-lookup() {
    (cd "$root" && taskset -c 2 cargo bench -p pigeonhole-memtable --bench memtable -- 'seek/' \
        2>&1 | grep -E '^seek/|time:') | tee "$results/memtable-lookup.txt"
    cp -r "$root/target/criterion" "$results/memtable-criterion"
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

# ycsb-a's write gap (perf287; ICR 0027; D198 item 2, #478): one client at the default shard
# count, at 1 and 4 shards, and at the default with longer shard spins, beside RocksDB. The
# stall table and JSON carry shard parks, wakes and commit parks per operation.
step_ycsb-a-shards() {
    if ! "$bench" --help | grep -q -- --shard-spin; then
        echo "phdb-bench has no --shard-spin yet (#480); skipped, not marked done" >&2
        return "$SKIP"
    fi
    run_one() { # NAME, phdb-bench arguments after the workload
        local name="$1"
        shift
        PIGEONHOLE_IO=uring "$bench" ycsb-a --scale "$scale" --dir "$data/bench" \
            --json "$results/ycsb-a-$name.json" --markdown "$results/ycsb-a-$name.md" "$@"
        rm -r "$data/bench"
    }
    run_one default --engine pigeonhole,rocksdb
    run_one shards1 --engine pigeonhole --shards 1
    run_one shards4 --engine pigeonhole --shards 4
    local us
    for us in 100 200 500; do
        run_one "spin$us" --engine pigeonhole --shard-spin "$us"
    done
}

# The group sync depth (D207): durable group commits at 1, 4 and 16 clients with at most 1, 2
# or unlimited (0) group syncs in flight per stream. The interim default is 1 on macOS and 2
# elsewhere; this run decides it.
step_group-sync-depth() {
    local d
    for d in 1 2 0; do
        PIGEONHOLE_IO=uring PIGEONHOLE_GROUP_SYNC_DEPTH="$d" "$bench" group-commit \
            --engine pigeonhole --scale "$scale" --dir "$data/bench" \
            --json "$results/group-sync-depth-$d.json"
        rm -r "$data/bench"
    done
}

step_row-cache() {
    if ! "$bench" --help | grep -q -- --row-cache; then
        echo "phdb-bench has no --row-cache yet (a bench PR); skipped, not marked done" >&2
        return "$SKIP"
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
        return "$SKIP"
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
