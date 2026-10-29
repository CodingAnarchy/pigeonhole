#!/usr/bin/env bash
# Writes and freezes the corpus of database files written by a release's own build
# (FORMAT.md §13). Run it once per release that has a corpus (1.0.0 first), from the release
# commit, where the workspace version is that release's:
#
#   scripts/release-corpus.sh 1.0.0
#
# It writes crates/pigeonhole/tests/corpus/v<version>/: one directory per entry of
# tests/corpus.rs's `entries()`, and frozen.txt with each file's length and xxh3-64. Then it
# runs the release-corpus check over it and stages it for commit. The generator refuses a
# version other than the build's and a corpus that is already frozen. The directory and its
# RELEASE_CORPORA entry in tests/corpus.rs must exist first (1.0.0's do).
set -euo pipefail
version=${1:?usage: scripts/release-corpus.sh <version>}
cd "$(dirname "$0")/.."
dir=crates/pigeonhole/tests/corpus/v$version
if [ ! -f "$dir/README.md" ]; then
  echo "error: $dir/README.md is missing: add the placeholder and its RELEASE_CORPORA entry first" >&2
  exit 1
fi
PIGEONHOLE_CORPUS_RELEASE=$version cargo test -p pigeonhole --test corpus -- \
  --ignored --exact generate_corpus --nocapture
cargo test -p pigeonhole --test corpus -- --exact release_corpus_entries_open_read_check_and_write
git add "$dir"
echo "v$version corpus written and staged ($dir). Commit it in the release PR."
