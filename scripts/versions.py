#!/usr/bin/env python3
"""The workspace's two version lines (D295).

    versions.py check   fails unless each line is consistent (CI runs it)
    versions.py list    prints `<crate> <version>` for every published crate, in publish order

- **The internal line:** the 12 internal crates (`INTERNAL`) each set the same `version` in
  their own Cargo.toml. Any incompatible change to any of them takes the line's next major.
- **The public line:** `pigeonhole`, `pigeonhole-arrow`, `pigeonhole-cli` and the unpublished
  `pigeonhole-capi` and `pigeonhole-bench` take the workspace version
  (`[workspace.package] version`), under semver from 1.0.
- **Pins:** the workspace's `[workspace.dependencies]` pins each internal crate exactly at
  the internal line (`=X.Y.Z`, D282) and each public crate at the public line.

Reads the manifests with `tomllib`, so it needs no cargo.
"""

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# In dependency (publish) order.
INTERNAL = [
    "format", "io", "sim", "pager", "wal", "memtable", "cache", "runtime", "shm", "sst",
    "compaction", "engine",
]
PUBLIC = ["pigeonhole", "arrow", "cli"]
UNPUBLISHED = ["capi", "bench"]


def name(directory):
    return "pigeonhole" if directory == "pigeonhole" else f"pigeonhole-{directory}"


def manifest(directory):
    with open(ROOT / "crates" / directory / "Cargo.toml", "rb") as f:
        return tomllib.load(f)


def versions():
    """`(workspace, {directory: version})`, with `None` for a crate on the workspace version."""
    with open(ROOT / "Cargo.toml", "rb") as f:
        root = tomllib.load(f)
    out = {}
    for d in INTERNAL + PUBLIC + UNPUBLISHED:
        v = manifest(d)["package"]["version"]
        out[d] = None if isinstance(v, dict) and v.get("workspace") else v
    return root, out


def check():
    root, crates = versions()
    workspace = root["workspace"]["package"]["version"]
    pins = root["workspace"]["dependencies"]
    errors = []
    internal = {crates[d] for d in INTERNAL}
    if len(internal) != 1 or None in internal:
        errors.append(
            "the internal crates must each set one shared version: "
            + ", ".join(f"{name(d)} {crates[d] or 'workspace'}" for d in INTERNAL)
        )
    line = sorted(v for v in internal if v)[0] if internal - {None} else None
    for d in PUBLIC + UNPUBLISHED:
        if crates[d] is not None:
            errors.append(f"{name(d)} sets version {crates[d]}: the public line is the workspace's")
    for d in INTERNAL:
        want = f"={line}"
        got = pins.get(name(d), {}).get("version")
        if got != want:
            errors.append(f"[workspace.dependencies] {name(d)} is {got!r}, not {want!r}")
    for d in PUBLIC + UNPUBLISHED:
        got = pins.get(name(d), {}).get("version")
        if got != workspace:
            errors.append(f"[workspace.dependencies] {name(d)} is {got!r}, not {workspace!r}")
    if errors:
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"internal line {line} (12 crates), public line {workspace}")
    return 0


def listing():
    root, crates = versions()
    workspace = root["workspace"]["package"]["version"]
    for d in INTERNAL + PUBLIC:
        print(name(d), crates[d] or workspace)
    return 0


def main(argv):
    if argv[1:] == ["check"]:
        return check()
    if argv[1:] == ["list"]:
        return listing()
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
