#!/usr/bin/env bash
# The Phase 3 gate-window run (#405, #406): every measurement the gate needs, on the reference
# machine (D5: a Hetzner AX102, rented only for this window), in one resumable run.
#
#   crates/bench/baselines/phase3-gate/run-window.sh DATA_DIR RESULTS_DIR [--scale full|small]
#
# Linux 6.x, as root (the cold steps empty the page cache and start cgroup scopes), with
# nothing else running. DATA_DIR is on the NVMe under test (not the OS drive); RESULTS_DIR
# collects every JSON, log and the summary. Each step writes RESULTS_DIR/STEP.done when it
# finishes (holding the commit it ran at), so a rerun (after a reboot, or a fix) skips what
# is done; delete a .done file to
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
#   memtable-lookup  memtable 262k (judged, D230)/1M/10k point lookup (#17; criterion) 5
#   phase2-gate    D193 sparse-wide gate against SQLite and RocksDB (run-gate.sh)     40
#   phase2-gate-2  the same again: #387's row-read p99 sits near 1.5x SQLite's, and one
#                  run varies by about 5%                                             40
#   all-uring-1    phdb-bench all vs RocksDB, full, PIGEONHOLE_IO=uring, every run
#                  10 s or more (D243); cross-shard-contention also with tablet
#                  changes off (D246)                                                 90
#   all-uring-2    the same again (the reproducibility gate: compare 1 and 2, judging
#                  runs of 10 s or more, D243)                                        90
#   all-pread      the same on pread (#402 PR 6: is io_uring the right default?)     90
#   latency        ycsb-c (2 GiB cache, warmed: D216), ycsb-a, skewed vs RocksDB;
#                  group-commit: the D213 shape (one shard per core, table split over
#                  all, commits in flight inline) on 2M commits (D219), then 1/4/16
#                  client threads vs RocksDB; the short 200K run, the same on
#                  pread, and 4 and 8 commits in flight (D226), reported              57
#   ycsb-a-shards  ycsb-a, one client: default shards vs 1 and 4, and shard spins (D198)  15
#   row-cache      ycsb-c and ycsb-a with the row cache off (D216: on by default)      30
#   scaling        engine scaling (D204, D231: pre-split, balancer off) at 1, 4, 8, 16
#                  shards, 8 s or more each (D219)                                    45
#   placement      balancer placement (D231): open-loop writes at 4, 8, 16 shards;
#                  settle time, busiest share once settled, churn while measuring     8
#   contended      D247: 16 client threads, default options, 60 s or more per judged run
#                  (45 s per reported one),
#                  against RocksDB and SQLite at two durabilities: D108 parity
#                  (Pigeonhole Buffered, RocksDB unsynced WAL, SQLite NORMAL) and the
#                  default (Pigeonhole GroupSync, the others fsync every write;
#                  every store loads buffered): ycsb-a/b/c/e/f, sparse-wide,
#                  time-series-ttl, cross-shard-contention (D246, also with tablet
#                  changes off), counters; transfer, ycsb-a's riders and 16 KiB
#                  values reported                                                    105
#   open-latency   open to first read (#158; phase3-io/open-latency.sh)               5
#   wal-pin        wal_pin_bytes 2x vs 4x the budget: write path, open after a crash
#                  (#527; phase3-io/wal-pin.sh)                                        10
#   cold-get       cold point gets, one I/O (#87; phase3-io/cold-get.sh)              45
#   cold-scan      cold scans, direct vs buffered at D222's readahead (cold-scan.sh) 15
#   scan-cache     ordered scan from cache, GB/s decoded per core (#29)               10
#   report         summary.md (every #406 target: value, pass/fail, source file),
#                  and RESULTS_DIR.tar.gz to copy back                                1
# Total: about 11.8 hours unattended at full scale (the sum of the estimates above; the
# 2026-10-13b window's steps took 109 minutes before `contended` was added).
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
# setup.sh installs the toolchain with rustup, which puts cargo on PATH only in login shells:
# a fresh tmux pane may not have ~/.cargo/bin.
if ! command -v cargo >/dev/null && [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi
command -v cargo >/dev/null || {
    echo "run-window.sh: cargo not found on PATH or in $HOME/.cargo/env; run setup.sh, or 'source ~/.cargo/env' first" >&2
    exit 2
}
mkdir -p "$data" "$results/logs"
data="$(cd "$data" && pwd)"
results="$(cd "$results" && pwd)"

steps=(setup build memtable-lookup phase2-gate phase2-gate-2 all-uring-1 all-uring-2 all-pread latency ycsb-a-shards
    row-cache scaling placement contended open-latency wal-pin cold-get cold-scan scan-cache report)
declare -A minutes=([setup]=1 [build]=10 [memtable-lookup]=5 [phase2-gate]=40 [phase2-gate-2]=40 [all-uring-1]=90 [all-uring-2]=90
    [all-pread]=90 [latency]=47 [ycsb-a-shards]=15 [row-cache]=30 [scaling]=45 [placement]=8 [contended]=105 [open-latency]=5 [wal-pin]=10 [cold-get]=45
    [cold-scan]=15 [scan-cache]=10 [report]=1)

# The checkout's commit ("-dirty" with local changes), recorded with each finished step.
commit="$(git -C "$root" rev-parse --short=12 HEAD)"
[[ -z "$(git -C "$root" status --porcelain --untracked-files=no)" ]] || commit="$commit-dirty"
# A resumed window must not run a binary built at another commit: build.done records the
# commit it built, and a different checkout refuses until build.done is deleted (rebuild).
if [[ -s "$results/build.done" && "$(cat "$results/build.done")" != "$commit" ]]; then
    echo "run-window.sh: the binaries were built at $(cat "$results/build.done"), the checkout is at $commit;" \
        "delete $results/build.done so the build step rebuilds them" >&2
    exit 2
fi
if [[ -e "$results/build.done" && ! -s "$results/build.done" ]]; then
    echo "warning: build.done does not say which commit it built; delete it to rebuild at $commit" >&2
fi

bench="$root/target/release/phdb-bench"
examples="$root/target/release/examples"
# Sizes: the gate's at full; a quick pass at small.
if [[ "$scale" == full ]]; then
    cold_get_mib=65536 cold_scan_mib=16384 scan_cache_mib=4096
else
    cold_get_mib=256 cold_scan_mib=256 scan_cache_mib=128
fi
# The cold-get block cache: 1/64 of the table, at least 64 MiB (D215: at least 1%, so the
# pinned top indexes and filters and the index partitions stay cached and a get is one I/O).
cold_get_cache_mib=$(( cold_get_mib / 64 > 64 ? cold_get_mib / 64 : 64 ))

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
        # The commit each step measured: a window resumed after a fix runs its later steps on
        # a newer commit, and the report lists the commits per step.
        printf '%s\t%d\t%s\t%s\n' "$name" $(((end - start) / 60)) "$scale" "$commit" >> "$results/durations.tsv"
        echo "$commit" > "$results/$name.done"
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

# The D193 sparse-wide gate into $1.json (run-gate.sh, with check.py's verdict in $1.status).
phase2_gate() {
    if [[ "$scale" == small ]]; then
        echo "phase2-gate runs at full scale only (its baselines are full-scale runs); skipped"
        return
    fi
    # A regression is a verdict (check.py exits 1), recorded for the report; only a run that
    # produced no result stops the window.
    local status=0
    TMPDIR="$data" "$root/crates/bench/baselines/phase2-gate/run-gate.sh" \
        "$results/$1.json" || status=$?
    [[ -s "$results/$1.json" ]] || return 1
    echo "$status" > "$results/$1.status"
}

step_phase2-gate() { phase2_gate phase2-gate; }
step_phase2-gate-2() { phase2_gate phase2-gate-2; }

# `phdb-bench all` against RocksDB, stores under DATA_DIR, on backend $1, into $2.json. Every
# run measures at least $repro_min_secs (D243: the reproducibility check judges only runs of
# about 10 s or more), and the D213 run inside it takes the judged run's commits (D219).
all_vs_rocksdb() {
    PIGEONHOLE_IO="$1" "$bench" all --engine pigeonhole,rocksdb --scale "$scale" \
        --min-secs "$repro_min_secs" --inline-ops "$gc_inline_ops" \
        --dir "$data/bench" --json "$results/$2.json" --markdown "$results/$2.md"
    rm -r "$data/bench"
}

step_all-uring-1() { all_vs_rocksdb uring all-uring-1; }
step_all-uring-2() {
    all_vs_rocksdb uring all-uring-2
    # The reproducibility gate: every result within tolerance across the two runs (judged at
    # 10 s or more, D243). A result out of tolerance is a verdict for the report, not a reason
    # to stop.
    local status=0
    "$bench" compare "$results/all-uring-1.json" "$results/all-uring-2.json" \
        > "$results/all-uring-compare.txt" || status=$?
    cat "$results/all-uring-compare.txt"
    echo "$status" > "$results/all-uring-compare.status"
}
step_all-pread() { all_vs_rocksdb pread all-pread; }

# "In memory" (D216): every engine's block cache holds the whole table (2 GiB) and the
# warmup reads as many rows as the measured phase, so the gets measure cached reads.
ycsb_c_memory=(--cache 2147483648 --warmup 1.0)

# Run lengths (D219). The durable group-commit target is judged on at least 2M commits, long
# enough for every shard's flushes and compactions to fall inside the measured phase; the
# short run (the preset's 200K) is reported beside it. The scaling gate measures at least 8 s
# at every shard count. The small scale (the dry run) keeps both short.
# The contended step (D247) measures each judged run for at least 60 s, and each reported run
# for 45 s (owner), from a count that any store finishes sooner, so the time, not the count,
# sets its length.
if [[ "$scale" == full ]]; then
    gc_inline_ops=2000000
    scaling_min_secs=8
    placement_window=10
    repro_min_secs=10
    contended_secs=60 contended_ops=100000 contended_report_secs=45
    # 16 KiB values (above the default 4 KiB blob threshold): 50K records of 10 fields is 8 GB,
    # far past every cache.
    contended_large_records=50000
else
    gc_inline_ops=50000
    scaling_min_secs=1
    placement_window=3
    repro_min_secs=1
    contended_secs=2 contended_ops=5000 contended_report_secs=2
    contended_large_records=2000
fi

step_latency() {
    local w extra
    for w in ycsb-c ycsb-a skewed-multi-shard; do
        extra=()
        [[ $w == ycsb-c ]] && extra=("${ycsb_c_memory[@]}")
        PIGEONHOLE_IO=uring "$bench" "$w" --engine pigeonhole,rocksdb --scale "$scale" \
            "${extra[@]}" --dir "$data/bench" --json "$results/latency-$w.json"
        rm -r "$data/bench"
    done
    # Durable 4-cell commits (GroupSync; RocksDB fsyncs). Pigeonhole first runs the target's
    # shape (D213): one shard per core, the table split over every shard, each shard's thread
    # keeping commits in flight inline; then, reported beside it, 1, 4 and 16 client threads.
    PIGEONHOLE_IO=uring "$bench" group-commit --engine pigeonhole,rocksdb --scale "$scale" \
        --inline-ops "$gc_inline_ops" --dir "$data/bench" \
        --json "$results/latency-group-commit.json"
    rm -r "$data/bench"
    # The trade-off beside the judged row (D226): the same D213 run at 4 and 8 commits in
    # flight per shard instead of 16, reported, not judged. One short client run each.
    local f
    for f in 4 8; do
        PIGEONHOLE_IO=uring "$bench" group-commit --engine pigeonhole --scale "$scale" \
            --threads 1 --ops 20000 --inline-ops "$gc_inline_ops" --in-flight "$f" \
            --dir "$data/bench" --json "$results/latency-group-commit-inflight-$f.json"
        rm -r "$data/bench"
    done
    # The short D213 run (the preset's commits, D219), reported: the first window's length.
    PIGEONHOLE_IO=uring "$bench" group-commit --engine pigeonhole --scale "$scale" --threads 1 \
        --dir "$data/bench" --json "$results/latency-group-commit-short.json"
    rm -r "$data/bench"
    # The same on the pread backend, reported beside the judged io_uring run (D213): on a
    # Linux runner the D213 shape reached 3x the cells/s on pread, with a p99.9 near 1 ms
    # against 0.6-0.8 s on io_uring. The comparison decides the io_uring default.
    PIGEONHOLE_IO=pread "$bench" group-commit --engine pigeonhole --scale "$scale" \
        --inline-ops "$gc_inline_ops" --dir "$data/bench" \
        --json "$results/latency-group-commit-pread.json"
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

step_row-cache() {
    if ! "$bench" --help | grep -q -- --row-cache; then
        echo "phdb-bench has no --row-cache yet (a bench PR); skipped, not marked done" >&2
        return "$SKIP"
    fi
    # The latency step runs the default (row cache on, D216); this is the same with it off.
    local w extra
    for w in ycsb-c ycsb-a; do
        extra=()
        [[ $w == ycsb-c ]] && extra=("${ycsb_c_memory[@]}")
        PIGEONHOLE_IO=uring "$bench" "$w" --engine pigeonhole --scale "$scale" \
            "${extra[@]}" --row-cache 0 --dir "$data/bench" \
            --json "$results/row-cache-off-$w.json"
        rm -r "$data/bench"
    done
}

# D204, D231: engine scaling on a pre-split table. report.py judges N up to one CCD's physical
# cores (machine.json's ccd_cores, from the setup step) and reports the larger N (D238).
step_scaling() {
    local n
    for n in 1 4 8 16; do
        PIGEONHOLE_IO=uring "$bench" scaling --shards "$n" --scale "$scale" \
            --min-secs "$scaling_min_secs" --dir "$data/bench" --json "$results/scaling-$n.json"
        rm -r "$data/bench"
    done
}

# D231: the balancer's placement, judged on its own under an open-loop load (the scaling
# step's table is pre-split and its balancer off).
step_placement() {
    local n
    for n in 4 8 16; do
        PIGEONHOLE_IO=uring "$bench" placement --shards "$n" --scale "$scale" \
            --min-secs "$placement_window" --dir "$data/bench" --json "$results/placement-$n.json"
        rm -r "$data/bench"
    done
}

# D247 (perf287's #406 audit): every public operation under contention on the defaults. 16
# client threads and the default options (the default I/O backend: no PIGEONHOLE_IO), against
# RocksDB and SQLite, at both durabilities (owner decision): `buffered`, D108's parity
# (Pigeonhole Buffered, RocksDB's WAL unsynced, SQLite NORMAL), and `default`
# (`Options::default()`'s GroupSync against the others fsyncing every write; every store
# loads buffered and switches after the settle step). Judged within
# 1.5x at each; cross-shard-contention also runs with tablet changes off (D246); transfer is
# reported, not judged.
contended_levels=(buffered default)
contended_workloads=(ycsb-a ycsb-b ycsb-c ycsb-e ycsb-f sparse-wide time-series-ttl cross-shard-contention counters)
# Reported, not judged (owner: 45 s runs; the judged ones keep 60 s or more).
contended_reported=(transfer)
step_contended() {
    local level w
    for level in "${contended_levels[@]}"; do
        for w in "${contended_workloads[@]}"; do
            "$bench" "$w" --engine pigeonhole,rocksdb,sqlite --scale "$scale" --threads 16 \
                --durability "$level" --ops "$contended_ops" --min-secs "$contended_secs" \
                --dir "$data/bench" --json "$results/contended-$level-$w.json" \
                --markdown "$results/contended-$level-$w.md"
            rm -r "$data/bench"
        done
        for w in "${contended_reported[@]}"; do
            "$bench" "$w" --engine pigeonhole,rocksdb,sqlite --scale "$scale" --threads 16 \
                --durability "$level" --ops "$contended_ops" --min-secs "$contended_report_secs" \
                --dir "$data/bench" --json "$results/contended-$level-$w.json" \
                --markdown "$results/contended-$level-$w.md"
            rm -r "$data/bench"
        done
        # Reported: large values, ycsb-a with 16 KiB values (blob separation; perf287's H).
        "$bench" ycsb-a --engine pigeonhole,rocksdb,sqlite --scale "$scale" --threads 16 \
            --value-len 16384 --records "$contended_large_records" \
            --durability "$level" --ops "$contended_ops" --min-secs "$contended_report_secs" \
            --dir "$data/bench" --json "$results/contended-$level-ycsb-a-16k.json" \
            --markdown "$results/contended-$level-ycsb-a-16k.md"
        rm -r "$data/bench"
        # Reported riders on ycsb-a (Pigeonhole only): snapshot and durable-snapshot readers,
        # a durable backup and a compaction beside the measured phase.
        "$bench" ycsb-a --engine pigeonhole --scale "$scale" --threads 16 --riders \
            --durability "$level" --ops "$contended_ops" --min-secs "$contended_report_secs" \
            --dir "$data/bench" --json "$results/contended-$level-ycsb-a-riders.json" \
            --markdown "$results/contended-$level-ycsb-a-riders.md"
        rm -r "$data/bench"
    done
}

step_open-latency() {
    "$root/crates/bench/baselines/phase3-io/open-latency.sh" "$data/openlat" \
        "$results/open-latency.jsonl"
}

# The wal_pin_bytes default (#527): 4M mixed-temperature commits per multiple, two rounds
# (about 8 minutes); the small scale (the dry run) keeps it short.
step_wal-pin() {
    local commits=4000000 rounds=2
    if [[ "$scale" == small ]]; then
        commits=50000 rounds=1
    fi
    "$root/crates/bench/baselines/phase3-io/wal-pin.sh" "$data/walpin" "$results/wal-pin.jsonl" \
        "$commits" "$rounds"
}

step_cold-get() {
    if [[ "$scale" == small ]]; then
        export COLDGET_GETS=20000 COLDGET_WARMUP=2000 COLDGET_QUIET_SECS=5
    fi
    COLDGET_CACHE_MIB=$cold_get_cache_mib "$root/crates/bench/baselines/phase3-io/cold-get.sh" \
        "$data/coldget" "$results/cold-get.jsonl" "$cold_get_mib"
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
