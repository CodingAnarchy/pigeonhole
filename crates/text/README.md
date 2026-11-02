# pigeonhole-text

Full-text search with BM25 ranking for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole), built on its public API only (decision D309, #1247).

- **Layout:** an index is one table of ordinary rows. Each term's posting list is a wide row, with one column per document. Optional positions answer phrase queries.
- **Writes:** the application writes a source column's text through the index, inside its own transaction. The postings and statistics then commit with the text, atomically.
- **Search:** a disjunctive BM25 search at one snapshot, over terms, exact phrases and sloppy phrases (within `N` position moves).

**Status:** the index, with positions, exact and sloppy phrase queries, an exact MaxScore evaluator with `id_prefix`, `ReadIndex` for reader processes, fuzzy terms, and highlighting with snippets (D309 PRs 1–5). It follows the public 1.x version line, with semver from its first release.
