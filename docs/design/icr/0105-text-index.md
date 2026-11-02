# 0105: `pigeonhole-text`, full-text search on the public API

**Status:** Approved. The number came from the coordinator. The owner accepted D309 in two steps on 2026-11-02 (both on #1247):
- calls 1–11 at revision 5 (c6565988061);
- calls 12–13, slop and normalisation, at revision 6 (c6566486752).

**Delivery:**
- PR 1 adds the index, with positions, exact phrases and normalisation.
- Later PRs add slop, MaxScore with `id_prefix` and `ReadIndex`, fuzzy terms, and highlighting.

## PR 1: the index
`pigeonhole-text` is on the public 1.x line (D295). Everything here is semver-checked (`crates/text/public-api.txt`).

- **`Index`** (`Clone`, `Send + Sync`):
  - **Creating and opening:**
    - `Index::create(&Pigeonhole, name, Source, IndexOptions) -> Result<Index>`: creates the table `name` with the index's families, then backfills it from the source column. It's `TableExists` if the table is there, and `InvalidArgument` for a missing source family or a source table named `name`.
    - `Index::create_with_tokenizer(.., Box<dyn Tokenizer>)`: the same with a custom tokeniser, which can't be named `standard`.
    - `Index::open(&Pigeonhole, name)` and `Index::open_with_tokenizer(.., Box<dyn Tokenizer>)`: everything else is read from the index. A different tokeniser name or fingerprint is `Mismatch`. A table that isn't an index is `InvalidArgument`.
  - **Writing:**
    - `put_in(&self, &mut Transaction, doc, text: &str) -> Result<()>`: writes the source cell and its postings in the caller's transaction. Putting the same text again writes nothing, provided `F{doc}` is present.
    - `delete_in(&self, &mut Transaction, doc) -> Result<bool>`: `false` if the index didn't hold the document.
    - `put` and `delete`: the same, each in its own `transact` with `RetryPolicy::default()`.
  - **Searching:** `search(&self, impl Into<Query>, k) -> Search<'_>`.
  - **Maintenance:**
    - `rebuild() -> Result<RebuildStats>`: online, one transaction per document, diffed against the stored cells;
    - `repair() -> Result<RebuildStats>`: from an emptied table, behind the repair flag;
    - `verify() -> Result<IndexStats>`.
  - **Accessors:** `options()` and `source()`. There's no handle to the index's own table (cli692): it holds the index's rows only.
- **`Source`** (`Clone + Eq`): `Source::new(&Table, family, qualifier)`, plus the `table()`, `family()` and `qualifier()` getters.
- **`IndexOptions`** (`Copy + PartialEq + Default`). Every option is fixed at create, and each has a `get_*` getter. That's a deliberate exception to the plain getter names, since the setters take those names and `Index::options()` returns this type.
  - `new()`;
  - `k1` (default 1.2) and `b` (default 0.75);
  - `stop_words` (default off; built-in tokeniser only);
  - `max_term_bytes` (1–255, default 64);
  - `max_terms` (default 10,000);
  - `positions` (default on);
  - `normalization` (default `Nfc`).
- **`Normalization`** (`#[non_exhaustive]`, `Default`): `None`, `Nfc` or `Nfkc`.
- **`Search<'a>`** (`#[must_use]`):
  - `snapshot(&Snapshot)`;
  - `run() -> Result<Vec<Hit>>`, ordered by score descending, then id;
  - `run_with_stats() -> Result<(Vec<Hit>, SearchStats)>`.
- **`Hit`** (`#[non_exhaustive]`): `id: Vec<u8>`, `score: f64`.
- **`SearchStats`** (`#[non_exhaustive]`): `stale`.
- **`IndexStats`** (`#[non_exhaustive]`): `documents`, `terms`, `postings`, `counter_drift`, `forward_mismatch`, `source_mismatch`, `invalid_utf8`, and `is_consistent()`.
- **`RebuildStats`** (`#[non_exhaustive]`): `documents`, `changed`, `invalid_utf8`.
- **`Query`** (`Clone + Default + PartialEq`):
  - **Building:** `new()`, `parse(&str) -> Result<Query>`, `term`, `phrase`, `phrase_with_slop(words, u16)`, `text`, `fuzzy(term, Fuzziness)`, and `clauses() -> &[Clause]`.
  - **From strings:** `From<&str>`, `From<String>` and `From<&String>` parse the string, and a parse error comes back when the search runs.
  - **Builder terms** are matched as given after the index's normalisation. They're never tokenised, lowercased or capped.
- **`Clause`** (`#[non_exhaustive]`): `Term(String)`, `Phrase { words, slop: u16 }`, `Text(String)`, `QuotedPhrase { text, slop: u16 }`, `Fuzzy { term, fuzziness }`.
- **`Fuzziness`** (`#[non_exhaustive]`, `Default`): `Auto`, `One` or `Two`.
- **`Tokenizer`** (`Send + Sync`, dyn-compatible): `name()`, `fingerprint()`, and `tokens(&self, &str, &mut dyn FnMut(Token<'_>))`.
- **`Token<'a>`** (`#[non_exhaustive]`): `text`, `position`, `range`, and `Token::new(text, position, range)`.
- **`Error`** (`#[non_exhaustive]`): `Db`, `InvalidArgument`, `InvalidQuery { at, reason }` (the variant is `#[non_exhaustive]` too, so it can gain fields), `Busy` (writes and searches fail with it while a repair runs), `Mismatch` and `Corruption`.
  - It has `code() -> ErrorCode`, `Display`, `std::error::Error` and `From<pigeonhole::Error>`.
  - **`QueryErrorKind`** (`#[non_exhaustive]`): `UnclosedQuote`, `Slop`, `FuzzyInPhrase`, `Distance`.
- **`Result<T>`**.

**Refused until their PRs (PR 1 is unreleased):** a fuzzy clause, and a phrase with slop above 0, parse and build, but a search refuses them with `InvalidArgument`. The message names the PR that adds each: D309 PR 4 for fuzzy, PR 2 for slop.

**Clauses, against rev 5's wording:**
- **Each bare word of the syntax is one `Clause::Text`,** and each quoted phrase one `Clause::QuotedPhrase`. They're tokenised when the search runs, since the tokeniser belongs to the index, not to the `Query`.
- **So `clauses()` maps one-to-one onto the input's words and phrases,** which is what `Match.clause` (PR 5) indexes. A word that tokenises into several terms (`e-mail`) is still one clause.

## PR 2: phrase slop (#1290)
No API is added: `Query::phrase_with_slop(words, u16)`, the `Clause` slop fields and the `"..."~N` syntax shipped in PR 1. What changes is what a search does with them.
- **A phrase with slop above 0** is matched with Lucene's sloppy-phrase semantics (D309 rev 6 §B), not refused.
  - **What a match is:** one position per phrase word, spanning at most `slop` once each word's offset in the phrase is taken off.
  - **The frequency:** the sum of `1 / (distance + 1)` over the matches.
  - **Repeated words:** a word repeated in the phrase never takes one position twice.
- **Slop 0** is the exact phrase, scored as in PR 1.
- **Docs:** `phrase_with_slop` documents the rule, the scoring and the cost.

## PR 3a: MaxScore and `id_prefix`
- **`Search::id_prefix(prefix) -> Search`:** only documents whose id starts with `prefix`.
  - Every posting and position walk is bounded to the prefix.
  - The ranking statistics stay the whole index's, so scores equal an unrestricted search's for the same documents, and fewer than `k` hits may come back.
  - The prefix should carry its terminator (`agent:alice\0`), which the docs say.
- **MaxScore replaces the term-at-a-time evaluator** (no API change). Results and scores are exactly the same as before: a document's contributions are summed in the clauses' fixed order, and pruning tests a strict bound.

## PR 3b: `ReadIndex`, the guide, the measurements
- **`Index::open_reader(&PigeonholeReader, name) -> Result<ReadIndex>`** and **`Index::open_reader_with_tokenizer(.., Box<dyn Tokenizer>)`**: the index opened from a reader process, with the same tokeniser checks as `open` (`Mismatch`).
- **`ReadIndex`** (`Clone`, `Debug`): `search(query, k) -> Search<'_>`, `verify() -> Result<IndexStats>`, `options()` and `source()`. It has no write method.
  - It searches at the reader's snapshots.
  - A search while the writer's `repair` runs fails with `Busy` (cli692's §10 point).
- **`open` and `open_reader` of a table that lacks the index's families** now fail with `InvalidArgument` ("not a text index"), where they used to fail with the database's error for a missing family.
- **The guide page** `docs/guide/text-search.md`, doctested, and the bench example `crates/text/examples/bench.rs`, with the AX102 numbers.

## PR 4: fuzzy terms
Rev 5 §4.4 as decided. The dependency is `levenshtein_automata` 0.2.1 (MIT, no dependencies; `cargo deny` passes).
- **A search runs `Clause::Fuzzy`,** which PR 1 refused.
- **`Clause::FuzzyText { text, fuzziness }`** (new variant) is what the syntax's `word~` parses to. It's tokenised as documents are, and each of its terms is a fuzzy term. So `Postgress~` matches `postgres`, as the syntax's bare words do, while `Query::fuzzy` keeps the builder contract: the term as given, after normalisation. `Auto` applies to each term's own length.
  - PR 1 parsed `word~` to `Clause::Fuzzy`, so `clauses()` changes for that input. The crate is unreleased.
- **`Search::max_expansions(n)`** (default 50; 0 is refused with `InvalidArgument`) and **`Search::prefix_length(n)`** (default 0): search-wide, as rev 5 §6b says.
- **`SearchStats::fuzzy: Vec<FuzzyStats>`.** `FuzzyStats` (`#[non_exhaustive]`) has `clause` (an index into `clauses()`), `term`, `expansions` and `cut`. `SearchStats` loses `Copy` for the `Vec`.
- **The walk:** a scan of the `P` rows from the fixed prefix, reading only the `df` counter.
  - At a key the automaton rejects partway, the scan seeks to the smallest byte string above the key that the automaton hasn't rejected.
  - The UTF-8 automaton rejects a character only at its last byte, so a seek can land on a multi-byte lead byte. That's still a valid lower bound.
  - A term whose `df` is 0 (every document deleted) is not an expansion.
- **One addition to rev 5:** a match whose distance is at least the shorter term's length in characters isn't an expansion. Its boost `1 − d / min(len)` would be 0 or negative: `a~1` would otherwise match every one-letter term, at a boost of 0.
- **Blended statistics** (Lucene's `TopTermsBlendedFreqScoringRewrite`, simp724 on #1327; coordinator, 2026-11-02, as a Lucene-parity correction to rev 5 §4.4):
  - every kept expansion's idf is `idf(max df over the kept expansions)`, after the `max_expansions` cut;
  - the boost and the per-document maximum stay.
  - With rev 5's per-expansion idf, a typo in 1 document outranked the exact term in 1,000 by about 2×. A test pins that case.
- **MaxScore's fuzzy clause** merges its expansions' posting walks. A document's contribution is the largest of theirs, and a lookup is the largest of their lookups. The bound is the largest expansion's `boost · idf · tf_factor(maxtf, minlen)`, with the blended idf. A term clause is the one-walk case with boost 1, so its scores are unchanged.
- **Tests:**
  - against a brute-force optimal-string-alignment scan of every term (transpositions, Greek and accented words, `prefix_length`, the `max_expansions` cut and its order, deleted terms), at full depth and at k = 1 and 3;
  - the syntax's tokenising;
  - the per-clause maximum;
  - mutations of the boost, the maximum, the transposition flag and the order were all caught.
- **The bench** adds `fuzzy-1`, `fuzzy-2` and `fuzzy-2-prefix2`, and prints the mean expansions.

## PR 5: highlighting
Rev 5 §4.5, with `Match` as recorded below (cli692 on #1300).
- **`Search::highlight(bool)`** fills `Hit::text: Option<String>` (the source text) and `Hit::matches: Vec<Match>` (new fields; `Hit` is `#[non_exhaustive]`).
- **`Match`** (`#[non_exhaustive]`, `Clone`, `Eq`, `Hash`) has `range: Range<usize>`, `clause: usize` and `occurrence: u32`.
  - Matches are sorted by range, then clause, then occurrence.
  - A term, text term or fuzzy expansion is one token's range.
  - An exact phrase is its first token to its last.
  - A sloppy match is one range per phrase word, sharing an `occurrence`, at the positions where the matcher found that match's smallest distance.
  - A clause's occurrences count from 0 in the order they're found.
- **`snippet(&str, &[Match], max_bytes) -> Snippet`,** with `Snippet { range, matches }` (`#[non_exhaustive]`).
  - A window starts at the text's start or at a match, and ends on the last UAX #29 word boundary within `max_bytes`.
  - It's chosen by distinct clauses, then matches, then the earliest start.
  - It returns only the matches wholly inside it.
- **How it's computed:**
  - **The tokens:** the hit's source cell, already read for the stale check, is tokenised again (`Index::spans`, the indexing path's normalisation and cap, with ranges).
  - **Fuzzy clauses** match their search's expansions.
  - **A term two clauses yield** is listed under both.
- **The sloppy matcher** is generic over a zero-sized `Record`. Scoring passes `Count`, so its walk is the same code with nothing extra, and highlighting passes `Positions`.
- **Tests:**
  - against a brute-force matcher over the source texts (3 seeds × 60 queries: a term, text, an exact phrase, the same phrase at slop 2, and a fuzzy term, at once);
  - each counted sloppy match is a placement within the slop, and their weights sum to the frequency (3,000 cases);
  - decomposed and capitalised source text;
  - an index without positions;
  - stop words in a phrase;
  - snippet ties and boundaries.
  - Mutations of the occurrence count, a phrase's end, the snippet's tie and the sloppy snapshot were all caught.

## Later PRs: recorded now (cli692 on #1300)
- **PR 5's `Match`** (`#[non_exhaustive]`) has `range`, `clause` and `occurrence: u32`. A sloppy match lists each matched term's range, all sharing one `occurrence`. An exact phrase is one first-to-last range. That's D309 rev 6 §B.6.

