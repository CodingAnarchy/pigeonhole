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
# A shape binary (crates/bench/examples/readshapes.rs is one):
# - runs as `BINARY SHAPE ITERATIONS DIR`, its work proportional to ITERATIONS;
# - does its measured work only inside functions whose names contain `shape_` (and not
#   inside its setup), so callgrind can count them alone;
# - prints `units N` on stderr: the units the measured work handled (cells read, gets,
#   commits, entries written, as the binary documents).
#
# On Linux with valgrind, callgrind counts exactly the measured functions
# (--toggle-collect): deterministic, so CI can compare against a baseline. Elsewhere (macOS),
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
        # The environment goes before valgrind: callgrind does not follow `env`'s exec.
        env $env_set valgrind --tool=callgrind --toggle-collect="$pattern" \
            --callgrind-out-file="$work/cg.out" "$@" </dev/null >/dev/null 2>"$work/err"
        sed -n 's/^summary: \([0-9]*\).*/\1/p' "$work/cg.out"
    }
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        ir=$(callgrind '*hotrow_iteration*' "$env_set" "$bin" 20 "$work")
        echo "$name $(( ir / $(units_of "$work/err") ))"
    done
    while read -r shape_bin name; do
        ir=$(callgrind '*shape_*' "" "$shape_bin" "$name" 4 "$work")
        echo "$name $(( ir / $(units_of "$work/err") ))"
    done < <(shape_runs "$@")
elif [[ -x /usr/bin/time ]] && [[ "$(uname)" == Darwin ]]; then
    count() { # the command; prints the instructions retired by the whole run
        /usr/bin/time -l "$@" </dev/null 2>"$work/err" >/dev/null
        awk '/instructions retired/{print $1}' "$work/err"
    }
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        a=$(count env $env_set "$bin" 1000 "$work"); a_units=$(units_of "$work/err")
        b=$(count env $env_set "$bin" 3000 "$work"); b_units=$(units_of "$work/err")
        echo "$name $(( (b - a) / (b_units - a_units) ))"
    done
    while read -r shape_bin name; do
        a=$(count "$shape_bin" "$name" 100 "$work"); a_units=$(units_of "$work/err")
        b=$(count "$shape_bin" "$name" 300 "$work"); b_units=$(units_of "$work/err")
        echo "$name $(( (b - a) / (b_units - a_units) ))"
    done < <(shape_runs "$@")
else
    echo "needs valgrind (Linux) or /usr/bin/time -l (macOS)" >&2
    exit 1
fi
