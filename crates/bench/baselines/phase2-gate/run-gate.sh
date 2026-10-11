#!/usr/bin/env bash
# Runs the official sparse-wide gate (D193) and compares it with the Phase 2 baseline:
# `phdb-bench compare` against each baseline run (every store; throughput and p50 within
# the tolerance, p99 within twice it), then check.py on Pigeonhole's gated measures.
# Run it on a quiet machine (agents paused).
#
#   crates/bench/baselines/phase2-gate/run-gate.sh [OUT.json]
#
# Builds phdb-bench with the sqlite and rocksdb features (LIBCLANG_PATH as for any rocksdb
# build). Exits nonzero if Pigeonhole regressed beyond noise (check.py).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
out="${1:-gate-$(date +%Y%m%d-%H%M%S).json}"
cargo build --release -p pigeonhole-bench --features sqlite,rocksdb --manifest-path "$root/Cargo.toml"
bench="$root/target/release/phdb-bench"
echo "load average: $(sysctl -n vm.loadavg 2>/dev/null || cat /proc/loadavg)"
"$bench" sparse-wide --engine pigeonhole,sqlite,rocksdb --scale full --json "$out"
# The reference machine's two official runs once committed (D208); until then the Mac's (D193).
bases=("$here"/gate2-run5.json "$here"/gate2-run6.json)
[[ -f "$here/reference-run1.json" && -f "$here/reference-run2.json" ]] &&
    bases=("$here"/reference-run1.json "$here"/reference-run2.json)
for base in "${bases[@]}"; do
  echo "== phdb-bench compare $(basename "$base") $out --tolerance 0.20"
  "$bench" compare "$base" "$out" --tolerance 0.20 || true
done
python3 "$here/check.py" "$out"
