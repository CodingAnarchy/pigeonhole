#!/usr/bin/env python3
"""Checks that crates/format/tests/golden-frozen.txt only gained lines since BASE.

The file lists the golden files frozen at 1.0 (FORMAT.md §13). The format crate's golden
test checks each listed file against its line; this check stops a change from editing or
removing a line alongside the file. Usage: golden-frozen.py BASE (a commit, e.g. the PR's
base). Exits 0 when BASE has no such file yet.
"""

import subprocess
import sys

PATH = "crates/format/tests/golden-frozen.txt"


def entries(text):
    """The non-comment lines, in order."""
    lines = (line.strip() for line in text.splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def check(old, new):
    """The problems with going from `old` to `new` (empty when `new` only appends)."""
    old, new = entries(old), entries(new)
    problems = []
    for i, line in enumerate(old):
        if i >= len(new) or new[i] != line:
            problems.append(f"changed or removed: {line}")
    return problems


def main():
    base = sys.argv[1]
    shown = subprocess.run(
        ["git", "show", f"{base}:{PATH}"], capture_output=True, text=True
    )
    if shown.returncode != 0:
        print(f"{PATH} is not on {base} yet: nothing to compare")
        return 0
    with open(PATH, encoding="utf-8") as f:
        new = f.read()
    problems = check(shown.stdout, new)
    for p in problems:
        print(f"{PATH}: {p}")
    if problems:
        print("Frozen golden files never change: restore the line and the file, and add a")
        print("sibling golden file (with a new line at the end) for the new encoding.")
        return 1
    print(f"{PATH}: append-only since {base}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
