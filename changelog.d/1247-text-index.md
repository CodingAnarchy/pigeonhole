### Added
- `pigeonhole-text` (D309): full-text search with BM25, built on the public API.
  - **Writes:** `Index::create`/`open` over a source column, and `put_in`/`delete_in` in the caller's transaction, so the text, its postings and the statistics commit together.
  - **Search:** `search(query, k).run()` over terms, text and exact phrases, with a small query syntax (`word`, `"a phrase"`), at one snapshot.
  - **Indexing:** positions are an option, and normalisation is NFC by default, with NFKC as an option.
  - **Maintenance:** `rebuild`, `repair` and `verify` keep the index honest after writes that went around it.
