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
  - **Accessors:** `options()`, `source()`, `table()`.
- **`Source`** (`Clone + Eq`): `Source::new(&Table, family, qualifier)`, plus the `table()`, `family()` and `qualifier()` getters.
- **`IndexOptions`** (`Copy + PartialEq + Default`). Every option is fixed at create, and each has a `get_*` getter.
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
- **`Error`** (`#[non_exhaustive]`): `Db`, `InvalidArgument`, `InvalidQuery { at, reason }`, `Busy`, `Mismatch` and `Corruption`.
  - It has `code() -> ErrorCode`, `Display`, `std::error::Error` and `From<pigeonhole::Error>`.
  - **`QueryErrorKind`** (`#[non_exhaustive]`): `UnclosedQuote`, `Slop`, `FuzzyInPhrase`, `Distance`.
- **`Result<T>`**.

**Refused until their PRs (PR 1 is unreleased):** a fuzzy clause, and a phrase with slop above 0, parse and build, but a search refuses them with `InvalidArgument`.

**Clauses, against rev 5's wording:**
- **Each bare word of the syntax is one `Clause::Text`,** and each quoted phrase one `Clause::QuotedPhrase`. They're tokenised when the search runs, since the tokeniser belongs to the index, not to the `Query`.
- **So `clauses()` maps one-to-one onto the input's words and phrases,** which is what `Match.clause` (PR 5) indexes. A word that tokenises into several terms (`e-mail`) is still one clause.
