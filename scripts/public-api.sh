#!/usr/bin/env bash
# The public API of the crates under semver (D282) against their committed snapshots
# (crates/*/public-api.txt, #695). CI runs `check`, which fails on any difference, so a PR
# that adds, changes or removes public API shows it in review (and needs an ICR); `bless`
# rewrites the snapshots after such a change.
#
# The listing is `cargo public-api` (rustdoc JSON, so it needs a nightly): every feature on,
# hidden items left out (`#[doc(hidden)]`, and the `__unstable-hooks` items, which are all
# hidden), and blanket, auto-trait and auto-derived impls omitted. The nightly and the tool
# are pinned together, since the rustdoc JSON format changes between nightlies.
set -euo pipefail
NIGHTLY=${PUBLIC_API_NIGHTLY:-nightly-2026-10-05}
mode=${1:-check}
case $mode in check|bless) ;; *) echo "usage: $0 [check|bless]" >&2; exit 2 ;; esac
cd "$(dirname "$0")/.."
status=0
for entry in pigeonhole:crates/pigeonhole pigeonhole-arrow:crates/arrow pigeonhole-cli:crates/cli \
  pigeonhole-vector:crates/vector; do
  pkg=${entry%%:*}
  snapshot=${entry#*:}/public-api.txt
  listing=$(cargo "+$NIGHTLY" public-api -p "$pkg" -sss --all-features)
  if [ "$mode" = bless ]; then
    printf '%s\n' "$listing" > "$snapshot"
    echo "wrote $snapshot"
  elif ! diff -u "$snapshot" <(printf '%s\n' "$listing"); then
    echo "error: $pkg's public API differs from $snapshot (lines above: - snapshot, + now)." >&2
    echo "  If the change is intended, run scripts/public-api.sh bless and name its ICR in the PR." >&2
    status=1
  fi
done
exit $status
