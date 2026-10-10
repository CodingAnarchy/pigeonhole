# Changelog fragments

Each PR with a user-visible change adds **one new file** here instead of editing `CHANGELOG.md`, so parallel PRs never conflict on the changelog.

- **Name:** `<PR-or-issue-number>-<short-slug>.md`, for example `421-commit-spin.md` (the slug in lowercase `a-z`, `0-9` and `-`). Never edit another PR's fragment.
- **Content:** the section heading(s) from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the entry lines, exactly as they would appear under `[Unreleased]`:

  ```markdown
  ### Added
  - `Options::commit_spin` and `Options::shard_spin`: a bounded spin before parking (D198, ICR 0016).
  ```

- **Content rules:** only the six Keep a Changelog headings (`### Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`), each with at least one `- ` entry. An entry continues on indented lines (nested bullets, a second paragraph); there is no other text.
- **Check:** `python3 scripts/changelog.py check`. The Changelog workflow runs it on every PR.
- **At release:** the coordinator runs `python3 scripts/changelog.py fold <version>` (`--dry-run` first to see it). It folds every fragment into `CHANGELOG.md` under the new version, with sections in Keep a Changelog order and entries in fragment-number order, leaves an empty `[Unreleased]`, updates the compare links, and deletes the fragments, which the release PR then shows.

`CHANGELOG.md`'s `[Unreleased]` section may still hold entries written before this rule; they are folded in the same way.
