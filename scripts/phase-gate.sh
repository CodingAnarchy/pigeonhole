#!/usr/bin/env bash
# Checks the issue part of a phase gate: the phase's GitHub milestone must have no open
# issues. Usage: scripts/phase-gate.sh <phase 1-4>. Exit 0 when the milestone is empty or
# closed, 1 when issues remain (listed), 2 on bad usage. Needs an authenticated `gh`.
set -euo pipefail
phase="${1:-}"
[[ "$phase" =~ ^[1-4]$ ]] || { echo "usage: $0 <phase 1-4>" >&2; exit 2; }
repo="${PIGEONHOLE_REPO:-CodingAnarchy/pigeonhole}"
title=$(gh api "repos/$repo/milestones?state=all" --jq ".[] | select(.title | startswith(\"Phase $phase \")) | .title")
[[ -n "$title" ]] || { echo "no milestone for phase $phase" >&2; exit 2; }
open=$(gh issue list --repo "$repo" --milestone "$title" --state open --limit 200 --json number,title --jq '.[] | "#\(.number) \(.title)"')
if [[ -z "$open" ]]; then
  echo "$title: no open issues"
  exit 0
fi
echo "$title: open issues block the gate:"
echo "$open"
exit 1
