# Changelog fragments

Each PR with a user-visible change adds **one new file** here instead of editing `CHANGELOG.md`, so parallel PRs never conflict on the changelog.

- **Name:** `<PR-or-issue-number>-<short-slug>.md`, for example `421-commit-spin.md`. Never edit another PR's fragment.
- **Content:** the section heading(s) from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the entry lines, exactly as they would appear under `[Unreleased]`:

  ```markdown
  ### Added
  - `Options::commit_spin` and `Options::shard_spin`: a bounded spin before parking (D198, ICR 0016).
  ```

- **At release:** the coordinator folds every fragment into `CHANGELOG.md` under the new version (merging sections in Keep a Changelog order), then deletes the fragments in the release PR.

`CHANGELOG.md`'s `[Unreleased]` section may still hold entries written before this rule; they are folded in the same way.
