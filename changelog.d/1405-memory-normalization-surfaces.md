### Added
- Python: `pigeonhole.memory.Store.create(..., normalization="nfkc")` chooses a memory store's keyword-index normalisation (`"nfc"`, the default, `"nfkc"` or `"none"`), and `Store.normalization`/`ReadStore.normalization` read it back (#1405).
- `phdb mcp --memory-normalization nfc|nfkc|none` sets a new memory store's normalisation. For an existing store, a value other than the store's stops the server at startup (#1405).
