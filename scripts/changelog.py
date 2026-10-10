#!/usr/bin/env python3
"""Changelog fragments (`changelog.d/`): check them, and fold them into CHANGELOG.md.

    changelog.py check
        Checks every fragment in changelog.d/ (README.md aside): the name is
        `<number>-<slug>.md` (the PR or issue number; slug in a-z, 0-9 and `-`), and the
        content is only `### <Section>` headings from Keep a Changelog, each followed by at
        least one `- ` entry. Exits 1 on any error, naming the file and line.

    changelog.py fold VERSION [--date YYYY-MM-DD] [--dry-run]
        At release: merges every fragment and CHANGELOG.md's `[Unreleased]` entries into a new
        `## [VERSION] - DATE` section (sections in Keep a Changelog order; within a section,
        `[Unreleased]`'s entries first, then the fragments' in name order: number, then slug),
        leaves an empty `[Unreleased]` above it, updates the compare links at the bottom, and
        deletes the folded fragments. `--dry-run` prints the new CHANGELOG.md instead. Refuses
        if VERSION already has a section, or if there is nothing to fold.

Python 3 standard library only.
"""
import argparse
import datetime
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
FRAGMENTS = os.path.join(ROOT, "changelog.d")
CHANGELOG = os.path.join(ROOT, "CHANGELOG.md")

# Keep a Changelog's sections, in its order.
SECTIONS = ["Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"]
NAME = re.compile(r"^(\d+)-([a-z0-9]+(?:-[a-z0-9]+)*)\.md$")
HEADING = re.compile(r"^###\s+(.+?)\s*$")


class ChangelogError(Exception):
    pass


def parse_sections(text, where, allowed=None):
    """`[(heading, [entry, ...]), ...]` from markdown made of `### ` headings and `- ` entries.

    An entry is its `- ` line plus the indented and blank lines after it. With `allowed`,
    only those headings may appear. Raises ChangelogError (naming `where` and the line) on a
    heading not allowed, a heading with no entry, or text outside an entry."""
    sections = []
    entry = None
    for no, line in enumerate(text.splitlines(), 1):
        m = HEADING.match(line)
        if m:
            heading = m.group(1)
            if allowed is not None and heading not in allowed:
                raise ChangelogError(
                    f"{where}:{no}: `### {heading}` is not a Keep a Changelog section "
                    f"(one of {', '.join(allowed)})")
            if sections and not sections[-1][1]:
                raise ChangelogError(f"{where}:{no}: `### {sections[-1][0]}` has no entry")
            sections.append((heading, []))
            entry = None
            continue
        if line.startswith("#"):
            raise ChangelogError(f"{where}:{no}: only `### <Section>` headings are allowed")
        if line.startswith("- "):
            if not sections:
                raise ChangelogError(f"{where}:{no}: an entry before any `### <Section>` heading")
            entry = [line]
            sections[-1][1].append(entry)
            continue
        if not line.strip() or line.startswith((" ", "\t")):
            if entry is not None:
                entry.append(line)
            elif line.strip():
                raise ChangelogError(f"{where}:{no}: indented text outside an entry")
            continue
        raise ChangelogError(
            f"{where}:{no}: text outside an entry (entries start with `- `; continue one "
            f"with an indented line)")
    if sections and not sections[-1][1]:
        raise ChangelogError(f"{where}: `### {sections[-1][0]}` has no entry")
    return [(h, ["\n".join(e).rstrip() for e in entries]) for h, entries in sections]


def fragment_names(directory):
    return sorted(f for f in os.listdir(directory) if f.endswith(".md") and f != "README.md")


def fragment_key(name):
    m = NAME.match(name)
    return (int(m.group(1)), m.group(2))


def check(directory=FRAGMENTS):
    """Every error in `directory`'s fragments, as messages."""
    errors = []
    for name in sorted(os.listdir(directory)):
        if name == "README.md":
            continue
        path = os.path.join(directory, name)
        if not NAME.match(name):
            errors.append(
                f"changelog.d/{name}: the name must be `<number>-<slug>.md` (the PR or issue "
                f"number; slug in a-z, 0-9 and `-`), for example `421-commit-spin.md`")
            continue
        with open(path, encoding="utf-8") as f:
            text = f.read()
        try:
            if not parse_sections(text, f"changelog.d/{name}", SECTIONS):
                errors.append(f"changelog.d/{name}: empty (no `### <Section>` heading)")
        except ChangelogError as e:
            errors.append(str(e))
    return errors


def section_order(heading, appearance):
    """Keep a Changelog order; a variant (`Changed (breaking)`) right after its section, and
    any other heading after them all, in order of appearance."""
    base = heading.split(" (")[0]
    if base in SECTIONS:
        return (SECTIONS.index(base), heading != base, appearance)
    return (len(SECTIONS), True, appearance)


def fold_text(changelog, fragments, version, date):
    """The new CHANGELOG.md text. `fragments` is `[(name, text), ...]`."""
    if re.search(rf"^## \[{re.escape(version)}\]", changelog, re.M):
        raise ChangelogError(f"CHANGELOG.md already has a section for {version}")
    m = re.search(r"^## \[Unreleased\][^\n]*\n", changelog, re.M)
    if not m:
        raise ChangelogError("CHANGELOG.md has no `## [Unreleased]` section")
    start = m.end()
    nxt = re.search(r"^## |^\[[^\]]+\]: ", changelog[start:], re.M)
    end = start + nxt.start() if nxt else len(changelog)
    merged = {}
    appearance = []

    def add(sections):
        for heading, entries in sections:
            if heading not in merged:
                merged[heading] = []
                appearance.append(heading)
            merged[heading].extend(entries)

    add(parse_sections(changelog[start:end], "CHANGELOG.md [Unreleased]"))
    for name, text in sorted(fragments, key=lambda f: fragment_key(f[0])):
        add(parse_sections(text, f"changelog.d/{name}", SECTIONS))
    if not merged:
        raise ChangelogError("nothing to fold: no fragments and an empty [Unreleased]")
    order = sorted(appearance, key=lambda h: section_order(h, appearance.index(h)))
    body = [f"## [{version}] - {date}", ""]
    for heading in order:
        body.append(f"### {heading}")
        for entry in merged[heading]:
            body.append(entry)
            # An entry of several paragraphs ends with a blank line, as written.
            if "\n\n" in entry:
                body.append("")
        if body[-1] == "":
            body.pop()
        body.append("")
    new = changelog[:start] + "\n" + "\n".join(body) + "\n" + changelog[end:]
    return update_links(new, version)


def update_links(text, version):
    """Points `[Unreleased]` at the new tag and adds the new version's compare link, in the
    file's existing format. A file without the links is left as it is."""
    m = re.search(r"^\[Unreleased\]: (\S+)/compare/(v[^.\s][^\s]*)\.\.\.HEAD$", text, re.M)
    if not m:
        return text
    base, previous = m.group(1), m.group(2)
    tag = f"v{version}"
    links = (f"[Unreleased]: {base}/compare/{tag}...HEAD\n"
             f"[{version}]: {base}/compare/{previous}...{tag}")
    return text[:m.start()] + links + text[m.end():]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="check every fragment in changelog.d/")
    f = sub.add_parser("fold", help="fold the fragments into CHANGELOG.md at release")
    f.add_argument("version")
    f.add_argument("--date", default=datetime.date.today().isoformat())
    f.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    if a.command == "check":
        errors = check()
        for e in errors:
            print(e, file=sys.stderr)
        if errors:
            return 1
        print(f"changelog.d: {len(fragment_names(FRAGMENTS))} fragments ok")
        return 0
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", a.date):
        print(f"--date must be YYYY-MM-DD, not {a.date!r}", file=sys.stderr)
        return 2
    errors = check()
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        print("fix the fragments before folding", file=sys.stderr)
        return 1
    names = fragment_names(FRAGMENTS)
    fragments = []
    for name in names:
        with open(os.path.join(FRAGMENTS, name), encoding="utf-8") as fh:
            fragments.append((name, fh.read()))
    with open(CHANGELOG, encoding="utf-8") as fh:
        changelog = fh.read()
    try:
        new = fold_text(changelog, fragments, a.version, a.date)
    except ChangelogError as e:
        print(e, file=sys.stderr)
        return 1
    if a.dry_run:
        sys.stdout.write(new)
        return 0
    with open(CHANGELOG, "w", encoding="utf-8") as fh:
        fh.write(new)
    for name in names:
        os.unlink(os.path.join(FRAGMENTS, name))
    print(f"folded {len(names)} fragments into CHANGELOG.md as {a.version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
