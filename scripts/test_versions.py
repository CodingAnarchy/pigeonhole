#!/usr/bin/env python3
"""Tests for scripts/versions.py's release check: `python3 scripts/test_versions.py`."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import versions  # noqa: E402


class Semver(unittest.TestCase):
    def test_numbers_not_strings(self):
        self.assertLess(versions.semver("1.9.0"), versions.semver("1.10.0"))
        self.assertLess(versions.semver("1.1.2"), versions.semver("1.2.0"))
        self.assertLess(versions.semver("0.9.0"), versions.semver("0.10.0"))

    def test_a_pre_release_is_below_its_release(self):
        self.assertLess(versions.semver("1.2.0-rc.1"), versions.semver("1.2.0"))
        self.assertLess(versions.semver("1.1.9"), versions.semver("1.2.0-rc.1"))

    def test_pre_release_identifiers_compare_by_semver(self):
        # ai1249's N1 on #1791: numeric identifiers as numbers, not strings.
        self.assertLess(versions.semver("1.2.0-rc.9"), versions.semver("1.2.0-rc.10"))
        # Numeric below alphanumeric; a longer list above its prefix; the spec's own order.
        order = ["1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta",
                 "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0"]
        keys = [versions.semver(v) for v in order]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(set(keys)), len(keys))

    def test_build_metadata_is_ignored(self):
        self.assertEqual(versions.semver("1.2.0+abc"), versions.semver("1.2.0"))


class LineErrors(unittest.TestCase):
    def errors(self, now, then, what=""):
        return versions.line_errors("the public line", "v1.1.2", now, then, what)

    def test_below_the_tag_fails_even_unchanged(self):
        # main at 1.1.0 against v1.1.2 from release/1.1.x: publish.sh would skip 1.1.0.
        [e] = self.errors("1.1.0", "1.1.2")
        self.assertIn("below 1.1.2", e)

    def test_below_the_tag_fails_when_changed(self):
        [e] = self.errors("1.1.0", "1.1.2", "these changed: pigeonhole")
        self.assertIn("never goes back", e)

    def test_unchanged_at_the_same_version_passes(self):
        self.assertEqual(self.errors("1.1.2", "1.1.2"), [])

    def test_changed_at_the_same_version_fails(self):
        [e] = self.errors("1.1.2", "1.1.2", "these changed: pigeonhole")
        self.assertIn("still 1.1.2", e)

    def test_changed_and_raised_passes(self):
        self.assertEqual(self.errors("1.2.0", "1.1.2", "these changed: pigeonhole"), [])
        self.assertEqual(self.errors("1.10.0", "1.9.0", "these changed: pigeonhole"), [])


if __name__ == "__main__":
    unittest.main()
