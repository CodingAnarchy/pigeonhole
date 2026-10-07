# Per-crate open questions

When the spec is silent or contradictory while you work on a crate, do not guess silently and do not edit [`decisions/`](../decisions/README.md) yourself. Record the question in this directory, in `<crate>.md` (for example `pager.md`), one `##` section per question:

```markdown
## Q: <the question in one line>
<context: what the spec says, what is missing, which callers are affected>

**Interim behavior:** <what the code does now, so reviewers and other agents can rely on it>
```

Then build to the interim behavior, and mention the file in your PR.

The coordinator audits these files after component merges. Each question becomes one of:

- a numbered, approved decision in [`decisions/`](../decisions/README.md) (the question confirmed or changed),
- a GitHub issue labeled with the phase (`phase-1`, `phase-3`, …) and the crate, linked from that decision, when work is deferred,
- or a question for the owner when it is a product decision.

The coordinator then deletes the crate's questions file, so this directory normally holds only this README. The pager's questions were folded in as D57–D61 during the 2026-10-06 audit.
