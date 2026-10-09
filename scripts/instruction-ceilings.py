#!/usr/bin/env python3
"""Absolute instruction ceilings per shape (D193): the Phase 2 floors may not regress.

    instruction-ceilings.py check COUNTS [CEILINGS]
        Fails (exit 1) if any shape in COUNTS (the output of scripts/instructions-per-cell.sh:
        lines `shape instructions_per_unit`) is above its ceiling, or has none. Prints a
        markdown table, noting shapes whose ceiling could now be lowered.
    instruction-ceilings.py lower COUNTS [CEILINGS]
        Rewrites CEILINGS, lowering each ceiling to the count times (1 + its headroom) when
        that is lower. Never raises one: a raise needs an owner decision (edit the file by
        hand, citing it).

CEILINGS defaults to crates/bench/baselines/instruction-ceilings.txt: `shape ceiling
headroom_percent` per line, `#` comments kept as they are."""
import math, os, sys

DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "crates", "bench",
                       "baselines", "instruction-ceilings.txt")

def read_counts(path):
    counts = {}
    for line in open(path):
        parts = line.split()
        if len(parts) >= 2 and not line.startswith("#"):
            counts[parts[0]] = int(parts[1])
    return counts

def read_ceilings(path):
    lines, cap = [], {}
    for line in open(path):
        lines.append(line.rstrip("\n"))
        parts = line.split()
        if len(parts) >= 3 and not line.startswith("#"):
            cap[parts[0]] = (int(parts[1]), float(parts[2]))
    return lines, cap

def lowered(count, headroom):
    return math.ceil(count * (1 + headroom / 100))

def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("check", "lower"):
        sys.exit(__doc__)
    cmd, counts = sys.argv[1], read_counts(sys.argv[2])
    path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT
    lines, cap = read_ceilings(path)
    if cmd == "check":
        print("| shape | count | ceiling | |")
        print("|---|--:|--:|---|")
        bad = False
        for shape, count in counts.items():
            if shape not in cap:
                print(f"| {shape} | {count} | (none) | **no ceiling: add one (D193)** |")
                bad = True
                continue
            ceiling, headroom = cap[shape]
            if count > ceiling:
                print(f"| {shape} | {count} | {ceiling} | **above the ceiling** |")
                bad = True
            else:
                low = lowered(count, headroom)
                note = f"ok; can be lowered to {low}" if low < ceiling else "ok"
                print(f"| {shape} | {count} | {ceiling} | {note} |")
        sys.exit(1 if bad else 0)
    out = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 3 and not line.startswith("#") and parts[0] in counts:
            ceiling, headroom = int(parts[1]), float(parts[2])
            low = lowered(counts[parts[0]], headroom)
            if low < ceiling:
                print(f"{parts[0]}: {ceiling} -> {low}")
                line = f"{parts[0]} {low} {parts[2]}"
        out.append(line)
    open(path, "w").write("\n".join(out) + "\n")

main()
