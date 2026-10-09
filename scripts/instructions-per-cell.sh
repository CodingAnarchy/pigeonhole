#!/usr/bin/env bash
# Instructions retired per cell read on hotrow (crates/bench/examples/hotrow.rs), for the three
# hot-row states: as written (versions in the memtable and L0), flushed (L0), fully compacted.
# Instructions don't depend on machine load, unlike timing (#287).
#
#   scripts/instructions-per-cell.sh [HOTROW_BINARY]
#
# On Linux with valgrind, callgrind counts exactly the measured iterations
# (--toggle-collect on `hotrow_iteration`): deterministic, so CI can compare against a
# baseline. Elsewhere (macOS), `/usr/bin/time -l` counts the whole process at two iteration
# counts and the difference removes the setup.
#
# Output: one line per state, `state instructions_per_cell`.
set -euo pipefail

bin="${1:-target/release/examples/hotrow}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

states=("as-written:" "flushed:HOT_FLUSH=1" "compacted:HOT_COMPACT=1")

# Cells read in the measured iterations, from hotrow's stderr.
cells_of() { sed -n 's/^cells read \([0-9]*\)$/\1/p' "$1"; }

if [[ "$(uname)" == Linux ]] && command -v valgrind >/dev/null; then
    iters=20
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        out="$work/cg.out"
        env $env_set valgrind --tool=callgrind --toggle-collect='*hotrow_iteration*' \
            --callgrind-out-file="$out" "$bin" "$iters" "$work" >/dev/null 2>"$work/err"
        ir=$(sed -n 's/^summary: \([0-9]*\).*/\1/p' "$out")
        cells=$(cells_of "$work/err")
        echo "$name $(( ir / cells ))"
    done
elif [[ -x /usr/bin/time ]] && [[ "$(uname)" == Darwin ]]; then
    count() { # instructions retired by a whole run of `iters` iterations
        env $2 /usr/bin/time -l "$bin" "$1" "$work" 2>"$work/err" >/dev/null
        awk '/instructions retired/{print $1}' "$work/err"
    }
    for s in "${states[@]}"; do
        name="${s%%:*}"; env_set="${s#*:}"
        a=$(count 1000 "$env_set"); a_cells=$(cells_of "$work/err")
        b=$(count 3000 "$env_set"); b_cells=$(cells_of "$work/err")
        echo "$name $(( (b - a) / (b_cells - a_cells) ))"
    done
else
    echo "needs valgrind (Linux) or /usr/bin/time -l (macOS)" >&2
    exit 1
fi
