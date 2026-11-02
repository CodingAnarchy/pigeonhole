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
| `postgress~`, `postgress~1` | `.fuzzy("postgress", Fuzziness::Auto)`, `Fuzziness::One` | terms within a few edits |

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
// A misspelling, within two edits of "postgres".
let fuzzy = index.search("Postgress~", 10).run()?;
assert_eq!(fuzzy[0].id, b"a");
# db.close()?;
# Ok(())
# }
```

- **Strings are parsed when the search runs.** A bad one fails with `InvalidQuery { at, reason }`, `at` being a byte offset. `Query::parse` checks one up front.
- **Builder terms are matched exactly as given,** after the index's Unicode normalisation: never tokenised, lowercased or capped. Pass them as the tokeniser emits terms, which is lowercase for the built-in one. Use `.text()` for words that should be tokenised.
- **Phrases with slop** use Lucene's sloppy-phrase rule.
  - **What a match is:** one position per word, spanning at most `N` once each word's place in the phrase is taken off. Words out of order cost moves: a reversed pair is 2.
  - **Scoring:** each match adds `1 / (distance + 1)` to the phrase's frequency, so closer matches score higher, and an exact match scores as it does without slop.
- **Fuzzy terms** match the index's terms within 1 or 2 edits, counting an adjacent transposition as one. `Auto` allows 0 edits up to 2 characters, 1 up to 5, and 2 beyond.
  - **Syntax and builder:** `word~` in a query string is tokenised as documents are, so `Postgress~` works. `.fuzzy(term, ..)` takes the term as given, like `.term`.
  - **Scoring:** each matching term scores as a term, times `1 − edits / length` (the shorter term's length), and a document scores its best match, not their sum.
    - **Every match uses the idf of the most frequent one** (Lucene's blended statistics). So a rare misspelling no longer outranks the common word it misspells on idf alone, though a document can still win on how often the term occurs and how short the document is. Three misspellings in one document don't add up to more than the real word.
  - **Expansions:** at most 50 terms per fuzzy term, the closest first, then the most frequent. `.max_expansions(n)` changes that. `.prefix_length(n)` requires the first `n` characters to match exactly, which makes a fuzzy search on a large dictionary much cheaper. `SearchStats::fuzzy` says how many terms each one matched, and whether the limit cut it.
  - Composed and decomposed accents differ by edits, so keep the default `Nfc` normalisation if you search with fuzzy terms.
- **A namespace:** `.id_prefix(p)` restricts a search to ids starting with `p`. Include the namespace's terminator (`agent:alice\0`), or `agent:alice2` matches too. Scores keep the whole index's statistics.

## Highlighting
`.highlight(true)` fills each hit's `text` (its source text) and `matches`. Each `Match` has three fields:
- **`range`:** a UTF-8 byte range into `text`;
- **`clause`:** the index into `query.clauses()` of the clause it matched;
- **`occurrence`:** which of that clause's occurrences it is.

A term or fuzzy term matches one token. An exact phrase covers its first token to its last. A sloppy phrase match lists one range per phrase word, all sharing one `occurrence`. `pigeonhole_text::snippet(&text, &matches, max_bytes)` picks a window, cut on word boundaries. It prefers the most distinct clauses, then the most matches, then the earliest.

```rust
# use pigeonhole::*;
# use pigeonhole_text::{Index, IndexOptions, Source, snippet};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let notes = db.table("notes")?.family("body", Family::default()).create_if_missing()?;
# let index = Index::create(&db, "notes_text", Source::new(&notes, "body", b"text"), IndexOptions::new())?;
index.put(b"a", "Notes from the March meeting: we chose Postgres over SQLite.")?;
let hits = index.search(r#""march meeting" postgress~"#, 10).highlight(true).run()?;
let text = hits[0].text.as_deref().unwrap();
let found: Vec<&str> = hits[0].matches.iter().map(|m| &text[m.range.clone()]).collect();
assert_eq!(found, ["March meeting", "Postgres"]);
let s = snippet(text, &hits[0].matches, 40);
assert_eq!(&text[s.range], "March meeting: we chose Postgres over ");
# db.close()?;
# Ok(())
# }
```

- **Matches are recomputed** by tokenising the hit's text again, so they always fit the text returned. That costs one pass per hit, and nothing when highlighting is off.
- **An index without positions** still highlights terms and fuzzy terms.
- **Python and MCP callers get code-point offsets,** not bytes.

## Options
Options are fixed when the index is created.

| Option | Default | What it does |
|---|---|---|
| `positions` | on | Stores each term's positions, which phrases need. On the reference machine, about 18% more write time and 58% more stored bytes: see [Performance](#performance). Turn it off for a write-heavy index that never searches phrases. |
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
MaxScore reads only the postings that can still change the top `k`. The result is the same as scoring every document.

The numbers below are for 1,000,000 documents of about 40 words, from a 50,000-word Zipf vocabulary, on the reference AX102 (`crates/text/examples/bench.rs`). They are p50 and p99 over 1,000 queries per kind, top 10 unless noted, after a flush and a full compaction. They're the means of two runs, which agreed within 4%.

**Writing** goes one document per transaction, `Buffered`:

| | per document | stored bytes per document |
|---|---|---|
| positions off | 139 µs | 1,038 |
| positions on (the default) | 157 µs (+13%) | 1,640 (+58%) |

The write rate stays flat from the first document to the millionth. A full compaction of the index takes 4 to 7 seconds.

**Searching**, top 10 unless noted:

| query | p50 | p99 |
|---|---|---|
| two terms, drawn by frequency | 2.3 ms | 95 ms |
| two of the 20 most common terms | 43 ms | 96 ms |
| an exact phrase of two rare words | 0.25 ms | 1 ms |
| an exact phrase, one common word | 77 ms | 216 ms |
| an exact phrase of two common words | 402 ms | 1,084 ms |
| that mixed phrase, slop 2 | 79 ms | 265 ms |
| that mixed phrase, slop 10 | 81 ms | 357 ms |
| two terms, top 1,000 | 28 ms | 188 ms |
| a fuzzy term, 1 edit (41 expansions) | 17 ms | 142 ms |
| a fuzzy term, 2 edits (44 expansions) | 21 ms | 142 ms |
| a fuzzy term, 2 edits, `prefix_length(2)` (37 expansions) | 15 ms | 110 ms |
| a fuzzy term, 1 edit, and a term drawn by frequency | 26 ms | 2,932 ms |

- **The hard cases are queries of only the most common terms,** whether as two terms or as a phrase. MaxScore prunes little when every clause's bound is large.
- **Phrases** read every position of their rarest word. When that word is common too, the read is large: a phrase of two common words takes hundreds of milliseconds at a million documents. Compaction doesn't change that, since it's the positions' size. Positional skipping is tracked in #1334.
- **Fuzzy terms** cost a walk over the dictionary near the term, then the matching terms' postings. Once the top `k` are held, an expansion that can't lift a document into them stops being read (#1333). On this bench's dense vocabulary (`w0` to `w49999`), a typo has dozens of terms within 2 edits, often common ones, which is the hard case. `prefix_length(2)` narrows the walk.
- **A fuzzy term beside a common term** has a long tail. When the common term drives, each of its documents looks the fuzzy term's expansions up. #1368 makes those lookups advance along the postings once that's cheaper, and #1363 tracks the opposite case: a fuzzy term over common expansions, beside a rare term.
- **Positions are cheap to write:** 13% more time per document on the reference machine, much less than a laptop suggests.

## Errors
| Error | When |
|---|---|
| `InvalidQuery { at, reason }` | A query string doesn't parse: an unclosed quote, a bad slop or fuzzy distance, or a fuzzy word inside a phrase. |
| `InvalidArgument` | A phrase on an index without positions, a document over `max_terms`, a bad option, or a table that isn't an index. |
| `Busy` | A write or search while `repair` runs: retry after it finishes. |
| `Mismatch` | `open` with another tokeniser or tokeniser version than the index was made with. |
| `Db` | The database's own error, passed through. |
