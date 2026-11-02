# Full-text search

> **Status:** this guide describes `main`. The current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. `pigeonhole-text` (D309) is new in 1.1.0. Code samples run as doctests of the `pigeonhole-text` crate (lines starting with `#` are hidden setup).

`pigeonhole-text` ranks documents by BM25 for keyword queries: terms, phrases, and phrases within a slop of position moves. An index is one table of ordinary rows over a **source column**, and you write that column through the index, inside your own transaction. The text, its postings and the statistics then commit together, so a search at a snapshot sees exactly the documents committed by then.

## Creating an index and writing documents
An index is built over one column: a table, a family and a qualifier. Each row of that table is a document, with its row key as the document's id.

```rust
# use pigeonhole::*;
# use pigeonhole_text::{Index, IndexOptions, Source};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let notes = db.table("notes")?.family("body", Family::default()).create_if_missing()?;
let index = Index::create(&db, "notes_text", Source::new(&notes, "body", b"text"), IndexOptions::new())?;

// In your own transaction, beside your other writes.
db.transact(&RetryPolicy::default(), |txn| {
    txn.put(&notes, b"note:1", "body", b"author", b"ada");
    index.put_in(txn, b"note:1", "We chose Postgres in the March meeting")?;
    Ok::<_, pigeonhole_text::Error>(())
})?;
// Or one document per call.
index.put(b"note:2", "The March offsite moved to April")?;
index.delete(b"note:2")?;
# db.close()?;
# Ok(())
# }
```

- **Creating the index backfills it** from the column's existing rows, and `Index::open` reopens it with every option read back.
- **Putting the same text again writes nothing.** Two writes of one document conflict, and writes of different documents never do: every statistic is a blind counter.
- **The contract:** every write to the source column goes **through the index** (`put_in`, `delete_in`, `put`, `delete`).
  - A direct `put` or a row delete around it leaves the index stale.
  - A search skips a hit whose source text is gone, and counts it in `SearchStats::stale`.
  - `verify()` reports the mismatches, and `repair()` rebuilds the index from an emptied table.
- **`rebuild()`** re-indexes every document online, and changes nothing for documents already right.

## Queries
`index.search(query, k).run()` returns the top `k` hits by BM25 score (Lucene's idf, `k1` 1.2, `b` 0.75), with ties broken by id. A query is an OR of clauses.

| Syntax | Builder | Matches |
|---|---|---|
| `postgres meeting` | `Query::new().text("postgres meeting")` | either word, tokenised as documents are |
| `"march meeting"` | `.phrase(["march", "meeting"])` | the words at consecutive positions |
| `"march meeting"~2` | `.phrase_with_slop(["march", "meeting"], 2)` | within 2 position moves |
| `postgress~` | `.fuzzy("postgress", Fuzziness::Auto)` | fuzzy terms (D309 PR 4) |

```rust
# use pigeonhole::*;
# use pigeonhole_text::{Index, IndexOptions, Query, Source};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let notes = db.table("notes")?.family("body", Family::default()).create_if_missing()?;
# let index = Index::create(&db, "notes_text", Source::new(&notes, "body", b"text"), IndexOptions::new())?;
index.put(b"a", "the march meeting chose postgres")?;
index.put(b"b", "march notes from a long meeting")?;
let hits = index.search(r#""march meeting" postgres"#, 10).run()?;
assert_eq!(hits[0].id, b"a");
// Within two moves, "b" matches the phrase too, at a lower score.
let sloppy = index.search(r#""march meeting"~4"#, 10).run()?;
assert_eq!(sloppy.len(), 2);
assert!(sloppy[0].score > sloppy[1].score);
// The same query, built.
let q = Query::new().phrase_with_slop(["march", "meeting"], 4);
assert_eq!(index.search(q, 10).run()?, sloppy);
# db.close()?;
# Ok(())
# }
```

- **Strings are parsed when the search runs.** A bad one fails with `InvalidQuery { at, reason }`, `at` being a byte offset. `Query::parse` checks one up front.
- **Builder terms are matched exactly as given,** after the index's Unicode normalisation: never tokenised, lowercased or capped. Pass them as the tokeniser emits terms, which is lowercase for the built-in one. Use `.text()` for words that should be tokenised.
- **Phrases with slop** use Lucene's sloppy-phrase rule.
  - **What a match is:** one position per word, spanning at most `N` once each word's place in the phrase is taken off. Words out of order cost moves: a reversed pair is 2.
  - **Scoring:** each match adds `1 / (distance + 1)` to the phrase's frequency, so closer matches score higher, and an exact match scores as it does without slop.
- **A namespace:** `.id_prefix(p)` restricts a search to ids starting with `p`. Include the namespace's terminator (`agent:alice\0`), or `agent:alice2` matches too. Scores keep the whole index's statistics.

## Options
Options are fixed when the index is created.

| Option | Default | What it does |
|---|---|---|
| `positions` | on | Stores each term's positions, which phrases need. About 40% more stored bytes on short documents, and a slower write: see [Performance](#performance). Turn it off for a write-heavy index that never searches phrases. |
| `normalization` | `Nfc` | Unicode normalisation of every term: `Nfc` matches composed and decomposed accents, `Nfkc` also folds ligatures and full-width forms, `None` compares code points as written. |
| `stop_words` | off | Drops English stop words (Lucene's 33). A dropped word keeps its position, so `"state of the art"` still needs its gaps. |
| `k1`, `b` | 1.2, 0.75 | BM25's saturation and length normalisation. |
| `max_term_bytes` | 64 | Longer words are dropped. |
| `max_terms` | 10,000 | A document with more distinct terms is refused before anything is written. |

A custom `Tokenizer` (`create_with_tokenizer`) replaces the built-in one, which splits on Unicode word boundaries and lowercases.
- **Its name and fingerprint are stored,** and an open with another tokeniser fails with `Mismatch`.
- **The built-in tokeniser's fingerprint includes its Unicode version,** so a dependency update that changes word boundaries makes `open` refuse until you `repair` into a new index.

## Reader processes
A reader process opens the index with `Index::open_reader(&reader, name)`. That gives a `ReadIndex`: it searches and verifies at the reader's snapshots, and it can't write. It sees what the writer has committed, as every read in a reader process does. During the writer's `repair`, searches fail with `Busy`.

## Performance
<!-- AX102 numbers: filled from .coord/box-text-ai1249 results. -->
MaxScore reads only the postings that can still change the top `k`. The result is the same as scoring every document.

The numbers below are for 1,000,000 documents of about 40 words, from a 50,000-word Zipf vocabulary, on the reference AX102 (`crates/text/examples/bench.rs`). They are p50 and p99 over 1,000 queries per kind, top 10 unless noted.

> **Measured without a full compaction.** A forced full compaction of a text index currently writes about 1,000 times the index's size, a known engine problem (#1313) that's being fixed before 1.1.0. So these numbers are taken after a flush only, with the index in whatever state background compaction had reached. Stored bytes are an over-estimate, since counter operands haven't all been folded yet, and search latency may improve once the index is compacted. They'll be re-taken once #1313 is fixed.

**TBD:** the reference-machine run is queued. Its numbers land in this table before the PR is ready.

- **The hard cases are queries of only the most common terms,** whether as two terms or as a phrase. MaxScore prunes little when every clause's bound is large.
- **Phrases** read every position of their rarest word.

## Errors
| Error | When |
|---|---|
| `InvalidQuery { at, reason }` | A query string doesn't parse: an unclosed quote, a bad slop or fuzzy distance, or a fuzzy word inside a phrase. |
| `InvalidArgument` | A phrase on an index without positions, a document over `max_terms`, a bad option, or a table that isn't an index. |
| `Busy` | A write or search while `repair` runs: retry after it finishes. |
| `Mismatch` | `open` with another tokeniser or tokeniser version than the index was made with. |
| `Db` | The database's own error, passed through. |
