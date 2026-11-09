### Changed
- `phdb mcp`'s memory tools now run on the same memory core as `phdb serve`'s `Memory` service (D313 ticket 8b), so the two front-ends store, recall and refuse alike. The tools, their arguments and their answers, error codes and exit codes included, are unchanged.
- `phdb serve --memory` with `--embed-url`: when creating a store, a failure of the endpoint's dimension probe (the endpoint down, say) is now an error exit, as `phdb mcp`'s always was, instead of a usage error.

### Fixed
- `phdb mcp`: a `memory_remember` with `supersedes` on a store with an IVF index now settles the index after its write, as every other remember does, instead of leaving the write's index work for the next one.
- `phdb mcp` on a database it opened read-only no longer starts the memory store's background forgetter, which could delete expired memories from a session that was meant to be read-only.
