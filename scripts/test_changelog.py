#!/usr/bin/env python3
"""Tests for scripts/changelog.py: `python3 scripts/test_changelog.py`."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import changelog  # noqa: E402

CHANGELOG = """# Changelog

Intro.

## [Unreleased]

### Changed (breaking)
- An old breaking change.

### Added
- An old addition.

  With a second paragraph.

## [0.2.0] - 2026-10-09

### Added
- Something released.

[Unreleased]: https://example.com/repo/compare/v0.2.0...HEAD
[0.2.0]: https://example.com/repo/compare/v0.1.0...v0.2.0
"""


def check_dir(files):
    with tempfile.TemporaryDirectory() as d:
        for name, text in files.items():
            with open(os.path.join(d, name), "w", encoding="utf-8") as f:
                f.write(text)
        return changelog.check(d)


class Check(unittest.TestCase):
    def test_a_good_fragment_passes(self):
        self.assertEqual(check_dir({
            "README.md": "# anything\n\nfree text\n",
            "421-commit-spin.md": "### Added\n- One.\n  - nested\n\n### Fixed\n- Two.\n",
        }), [])

    def test_a_bad_heading_is_named(self):
        errors = check_dir({"12-x.md": "### Performance\n- Faster.\n"})
        self.assertEqual(len(errors), 1)
        self.assertIn("changelog.d/12-x.md:1", errors[0])
        self.assertIn("Performance", errors[0])

    def test_a_bad_name_is_named(self):
        for name in ["icr20-get-markers.md", "12-Upper.md", "12.md", "12-a_b.md", "x-12.md"]:
            errors = check_dir({name: "### Added\n- One.\n"})
            self.assertEqual(len(errors), 1, name)
            self.assertIn(f"changelog.d/{name}", errors[0])

    def test_text_outside_a_section_or_entry_fails(self):
        for text in ["Intro.\n### Added\n- One.\n",
                     "- An entry first.\n",
                     "### Added\n- One.\nnot indented\n",
                     "## Added\n- One.\n",
                     "### Added\n",
                     "### Added\n### Fixed\n- One.\n",
                     ""]:
            self.assertEqual(len(check_dir({"1-x.md": text})), 1, repr(text))


class Fold(unittest.TestCase):
    def fold(self, fragments, version="0.3.0"):
        return changelog.fold_text(CHANGELOG, fragments, version, "2026-10-10")

    def test_sections_in_order_and_entries_by_fragment_number(self):
        out = self.fold([
            ("100-b.md", "### Fixed\n- Fix 100.\n\n### Added\n- Add 100.\n"),
            ("9-z.md", "### Added\n- Add 9.\n"),
            ("100-a.md", "### Added\n- Add 100a.\n"),
        ])
        section = out[out.index("## [0.3.0] - 2026-10-10"):out.index("## [0.2.0]")]
        headings = [l for l in section.splitlines() if l.startswith("### ")]
        self.assertEqual(headings, ["### Added", "### Changed (breaking)", "### Fixed"])
        added = section[section.index("### Added"):section.index("### Changed")]
        # [Unreleased]'s entries first, then the fragments by number, then slug.
        self.assertLess(added.index("An old addition"), added.index("Add 9."))
        self.assertLess(added.index("Add 9."), added.index("Add 100a."))
        self.assertLess(added.index("Add 100a."), added.index("Add 100."))
        self.assertIn("  With a second paragraph.", added)

    def test_unreleased_is_left_empty_above_the_new_version(self):
        out = self.fold([("1-x.md", "### Added\n- One.\n")])
        self.assertIn("## [Unreleased]\n\n## [0.3.0] - 2026-10-10\n\n### Added\n", out)
        self.assertEqual(out.count("An old breaking change."), 1)
        self.assertIn("## [0.2.0] - 2026-10-09", out)

    def test_compare_links_follow_the_new_version(self):
        out = self.fold([("1-x.md", "### Added\n- One.\n")])
        self.assertIn(
            "[Unreleased]: https://example.com/repo/compare/v0.3.0...HEAD\n"
            "[0.3.0]: https://example.com/repo/compare/v0.2.0...v0.3.0\n"
            "[0.2.0]: https://example.com/repo/compare/v0.1.0...v0.2.0\n", out)

    def test_an_existing_version_is_refused(self):
        with self.assertRaisesRegex(changelog.ChangelogError, "already has a section"):
            self.fold([("1-x.md", "### Added\n- One.\n")], version="0.2.0")

    def test_nothing_to_fold_is_refused(self):
        empty = CHANGELOG.replace(
            CHANGELOG[CHANGELOG.index("### Changed (breaking)"):CHANGELOG.index("## [0.2.0]")],
            "")
        with self.assertRaisesRegex(changelog.ChangelogError, "nothing to fold"):
            changelog.fold_text(empty, [], "0.3.0", "2026-10-10")

    def test_folding_twice_refuses_the_second(self):
        out = self.fold([("1-x.md", "### Added\n- One.\n")])
        with self.assertRaises(changelog.ChangelogError):
            changelog.fold_text(out, [("2-y.md", "### Added\n- Two.\n")], "0.3.0", "2026-10-11")


class Command(unittest.TestCase):
    def test_fold_writes_the_changelog_and_deletes_the_fragments(self):
        with tempfile.TemporaryDirectory() as d:
            frags = os.path.join(d, "changelog.d")
            os.mkdir(frags)
            for name, text in [("README.md", "# readme\n"), ("5-x.md", "### Added\n- Five.\n")]:
                with open(os.path.join(frags, name), "w", encoding="utf-8") as f:
                    f.write(text)
            path = os.path.join(d, "CHANGELOG.md")
            with open(path, "w", encoding="utf-8") as f:
                f.write(CHANGELOG)
            saved = (changelog.FRAGMENTS, changelog.CHANGELOG)
            changelog.FRAGMENTS, changelog.CHANGELOG = frags, path
            try:
                with open(os.devnull, "w") as null:
                    stdout, sys.stdout = sys.stdout, null
                    try:
                        self.assertEqual(changelog.main(["fold", "0.3.0", "--dry-run"]), 0)
                    finally:
                        sys.stdout = stdout
                self.assertTrue(os.path.exists(os.path.join(frags, "5-x.md")))
                self.assertEqual(changelog.main(["fold", "0.3.0", "--date", "2026-10-10"]), 0)
            finally:
                changelog.FRAGMENTS, changelog.CHANGELOG = saved
            self.assertEqual(sorted(os.listdir(frags)), ["README.md"])
            with open(path, encoding="utf-8") as f:
                self.assertIn("## [0.3.0] - 2026-10-10\n\n### Added\n- An old addition.", f.read())


if __name__ == "__main__":
    unittest.main()
