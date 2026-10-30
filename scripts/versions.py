#!/usr/bin/env python3
"""The workspace's two version lines (D295).

    versions.py check               fails unless each line is consistent (CI runs it)
    versions.py list                prints `<crate> <version>` for every published crate, in
                                    publish order
    versions.py release-check TAG   fails if a line's crates changed since the release TAG while
                                    the line's version did not move (`.coord/publish.sh` runs it)

- **The internal line:** the 12 internal crates (`INTERNAL`) each set the same `version` in
  their own Cargo.toml. Any incompatible change to any of them takes the line's next major.
- **The public line:** `pigeonhole`, `pigeonhole-arrow`, `pigeonhole-cli` and the unpublished
  `pigeonhole-capi` and `pigeonhole-bench` take the workspace version
  (`[workspace.package] version`), under semver from 1.0.
- **Pins:** the workspace's `[workspace.dependencies]` pins each internal crate exactly at
  the internal line (`=X.Y.Z`, D282) and each public crate at the public line.

**Release check:** `publish.sh` skips a crate already on crates.io at its version, so a line
whose crates changed since the last release but whose version did not move would ship the
previous release's code under the new `pigeonhole` (perf287's S2 on #1196). `release-check`
counts what changes a crate's behaviour or dependencies: its `src/`, a `build.rs`, and its
`Cargo.toml` other than the `version` line, plus the workspace `Cargo.toml`'s shared settings
(`[workspace.dependencies]` other than the crates' own pins, and `[workspace.package]` other
than `version`), which every packaged manifest inherits. Tests, benches and docs don't count.

Reads the manifests with `tomllib`, so it needs no cargo (`release-check` needs git).
"""

import subprocess
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


def git(*args):
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout


def toml_at(tag, path):
    return tomllib.loads(git("show", f"{tag}:{path}"))


def without_version(manifest_toml):
    m = dict(manifest_toml)
    m["package"] = {k: v for k, v in m["package"].items() if k != "version"}
    return m


def shared_settings(root):
    """The workspace settings every packaged manifest inherits, without the versions."""
    ws = root["workspace"]
    deps = {k: v for k, v in ws["dependencies"].items() if not k.startswith("pigeonhole")}
    package = {k: v for k, v in ws["package"].items() if k != "version"}
    return deps, package


def changed(tag, d):
    """Whether crate directory `d` changed in a way that ships, since `tag`."""
    files = git("diff", "--name-only", tag, "HEAD", "--", f"crates/{d}").split()
    if any(f.startswith(f"crates/{d}/src/") or f.endswith("/build.rs") for f in files):
        return True
    if f"crates/{d}/Cargo.toml" in files:
        try:
            before = toml_at(tag, f"crates/{d}/Cargo.toml")
        except subprocess.CalledProcessError:
            return True
        return without_version(before) != without_version(manifest(d))
    return False


def release_check(tag):
    root, crates = versions()
    old_root = toml_at(tag, "Cargo.toml")
    shared = shared_settings(root) != shared_settings(old_root)
    old_workspace = old_root["workspace"]["package"]["version"]

    def old_version(d):
        v = toml_at(tag, f"crates/{d}/Cargo.toml")["package"]["version"]
        return old_workspace if isinstance(v, dict) else v

    workspace = root["workspace"]["package"]["version"]
    errors = []
    for line, dirs in (("internal", INTERNAL), ("public", PUBLIC)):
        now = crates[dirs[0]] or workspace
        then = old_version(dirs[0])
        moved = [d for d in dirs if changed(tag, d)]
        if shared:
            moved = list(dirs)
        if moved and now == then:
            errors.append(
                f"the {line} line is still {now}, as at {tag}, but "
                + ("the workspace's shared settings changed" if shared else "these changed: ")
                + ("" if shared else ", ".join(name(d) for d in moved))
                + f": bump the {line} line (and its pins)"
            )
    if errors:
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"release-check against {tag}: ok")
    return 0


def main(argv):
    if argv[1:] == ["check"]:
        return check()
    if argv[1:] == ["list"]:
        return listing()
    if len(argv) == 3 and argv[1] == "release-check":
        return release_check(argv[2])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
