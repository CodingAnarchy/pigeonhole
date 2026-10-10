#!/usr/bin/env bash
# Instructions retired per unit, for the read and write paths (#287): the three hot-row states
# of crates/bench/examples/hotrow.rs (as written, flushed, compacted; per cell), then the
# shapes of any shape binaries given. Instructions don't depend on machine load, unlike
# timing.
#
#   scripts/instructions-per-cell.sh HOTROW_BINARY [SHAPE_BINARY:shape,shape,... ...]
#
# for example
#
#   scripts/instructions-per-cell.sh target/release/examples/hotrow \
#       target/release/examples/readshapes:get-mem,get-sst,row
#
# Commit spinning (D198) is off in the shape binaries (`Options::commit_spin(ZERO)`): a waiting
# client's poll count depends on thread timing. crates/bench/examples/commitpath.rs checks the
# default spin on wall time.
#
# A shape binary (crates/bench/examples/readshapes.rs is one):
# - runs as `BINARY SHAPE ITERATIONS DIR`, its work proportional to ITERATIONS;
# - does its measured work only while a `Measured` guard (crates/bench/examples/support/
#   measure.rs) is alive on the thread doing it, and none of its setup, so callgrind counts
#   that work alone (by convention in functions named `shape_*`);
# - prints `units N` on stderr: the units the measured work handled (cells read, gets,
#   commits, entries written, as the binary documents);
# - with `SHAPE_SETUP_ONLY=1` in its environment, does its setup but none of the measured
#   work (hotrow.rs too);
# - is listed with the shape that has the fullest setup last (the setup-only guard runs it).
#
# The guards: on Linux each binary also runs once with `SHAPE_SETUP_ONLY=1` (hotrow's first
# state, a shape binary's last shape: list the shape with the fullest setup last), and the
# script fails unless callgrind counts nothing inside the measured functions then. Setup that
# reaches them (rustc merging a setup function into an identical measured one, as #347
# found, or measured code called from the setup) would otherwise be counted silently. And
# every state and shape must report units and at least `floor` instructions per unit: work
# that escapes the measured functions (#346 counted 19 per compacted entry) fails too.
#
# On Linux with valgrind, callgrind counts exactly the measured work: collection starts off
# (--collect-atstart=no) and each `Measured` guard turns it on for its thread with a client
# request, which does not depend on callgrind resolving function names (a name match,
# --toggle-collect, sometimes counted nothing, #350). Deterministic, so CI can compare
# against a baseline. Elsewhere (macOS),
# `/usr/bin/time -l` counts the whole process at two iteration counts and the difference
# removes the setup.
#
# Output: one line per state or shape, `name instructions_per_unit`.
set -euo pipefail

bin="${1:-target/release/examples/hotrow}"
shift || true
# A fresh scratch directory per run (callgrind output, the examples' stores, which they
# remove themselves). It is left in the system temp directory, not deleted recursively.
work="$(mktemp -d)"

states=("as-written:" "flushed:HOT_FLUSH=1" "compacted:HOT_COMPACT=1")

# Fewest instructions a unit of real work can take: the cheapest state or shape measures well
# over 1,000 (a cell of a compacted row read). Far below this, the measured functions missed
# the work (it ran outside them).
floor=100

# Prints `name instructions_per_unit`, or fails if the run measured no units or an
# implausibly small count per unit.
report() { # name, instructions, units
    local name="$1" ir="${2:-0}" units="${3:-0}"
    if (( units <= 0 )); then
        echo "$name: the measured work reported no units" >&2
        exit 1
    fi
    local per=$(( ir / units ))
    if (( per < floor )); then
        echo "$name: $per instructions per unit, under the floor of $floor;" \
            "the measured functions missed the work" >&2
        exit 1
    fi
    echo "$name $per"
}

# Units handled by the measured work, from a run's stderr (hotrow prints `cells read N`).
units_of() { sed -nE 's/^(cells read|units) ([0-9]+)$/\2/p' "$1"; }

# Each `BINARY:shape,shape` argument as lines `BINARY shape`.
shape_runs() {
    local group shapes
    for group in "$@"; do
        IFS=, read -r -a shapes <<<"${group##*:}"
        for s in "${shapes[@]}"; do
            echo "${group%:*} $s"
        done
    done
}

if [[ "$(uname)" == Linux ]] && command -v valgrind >/dev/null; then
    callgrind() { # pattern, environment, then the command; prints the instructions counted
        local pattern="$1" env_set="$2"
        shift 2
        # The environment goes before valgrind: callgrind does not follow `env`'s exec. A fixed
        # mmap threshold stops glibc moving it whenever any thread frees a large block, which
        # decided by thread timing whether a growing buffer's realloc copied (#354).
        # An unbounded tcache keeps a block freed on another thread (a commit's batch buffer,
        # allocated by the client and freed by the shard after its reply) in the freeing
        # thread's cache. A full cache sends it back to the allocating thread's arena, where
        # thread timing decided the next malloc's bin work: commit-one, commit-at and
        # commit-overwrite varied by 0.26-0.43% on identical code, 0.05% or less with this.
        # The cross-thread free itself is a cost to remove, not to count (#320).
        env GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072:glibc.malloc.tcache_count=65535 \
            $env_set \
            valgrind --tool=callgrind --collect-atstart=no \
            --callgrind-out-file="$work/cg.out" "$@" </dev/null >/dev/null 2>"$work/err"
        sed -n 's/^summary: \([0-9]*\).*/\1/p' "$work/cg.out"
    }
    guard() { # name, pattern, environment, then the command: fails if setup alone is counted
        local name="$1" pattern="$2" env_set="$3"
        shift 3
        local ir
        ir=$(callgrind "$pattern" "SHAPE_SETUP_ONLY=1 $env_set" "$@")
        if [[ "${ir:-0}" != 0 ]]; then
            echo "$name: the setup alone counted $ir instructions ($pattern);" \
                "setup work reaches the measured functions" >&2
            exit 1
        fi
    }
    # One setup-only run per binary (each runs its full setup under valgrind, so one per
    # state or shape would cost about two thirds more time): hotrow's first state, and each
    # shape binary's last shape, which should be the one with the fullest setup.
    guard "${states[0]%%:*}" '*hotrow_iteration*' "${states[0]#*:}" "$bin" 20 "$work"
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        ir=$(callgrind '*hotrow_iteration*' "$env_set" "$bin" 20 "$work")
        report "$name" "$ir" "$(units_of "$work/err")"
    done
    while read -r shape_bin name; do
        ir=$(callgrind '*shape_*' "" "$shape_bin" "$name" 4 "$work")
        report "$name" "$ir" "$(units_of "$work/err")"
    done < <(shape_runs "$@")
    for group in "$@"; do
        last="${group##*,}"; last="${last##*:}"
        guard "$last" '*shape_*' "" "${group%:*}" "$last" 4 "$work"
    done
elif [[ -x /usr/bin/time ]] && [[ "$(uname)" == Darwin ]]; then
    count() { # the command; prints the instructions retired by the whole run
        /usr/bin/time -l "$@" </dev/null 2>"$work/err" >/dev/null
        awk '/instructions retired/{print $1}' "$work/err"
    }
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        a=$(count env $env_set "$bin" 1000 "$work"); a_units=$(units_of "$work/err")
        b=$(count env $env_set "$bin" 3000 "$work"); b_units=$(units_of "$work/err")
        report "$name" "$(( b - a ))" "$(( b_units - a_units ))"
    done
    while read -r shape_bin name; do
        a=$(count "$shape_bin" "$name" 100 "$work"); a_units=$(units_of "$work/err")
        b=$(count "$shape_bin" "$name" 300 "$work"); b_units=$(units_of "$work/err")
        report "$name" "$(( b - a ))" "$(( b_units - a_units ))"
    done < <(shape_runs "$@")
else
    echo "needs valgrind (Linux) or /usr/bin/time -l (macOS)" >&2
    exit 1
fi
