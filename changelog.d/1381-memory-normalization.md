### Added
- `pigeonhole-memory`: `StoreOptions::normalization` chooses the Unicode normalisation of a store's text index at create (`Nfc` by default). `Nfkc` makes keyword recall match ligatures and full-width forms against plain text. `Store::normalization()` and `ReadStore::normalization()` read it back, and `Normalization` is re-exported (#1381).
