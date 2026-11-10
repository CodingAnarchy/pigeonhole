### Added
- `phdb serve --memory NAME` serves `phdb mcp`'s memory tools on `/mcp` too (D313 ticket 8c-3b), with the `memory://guide` resource, over the store its `Memory` service serves. A token limited to namespaces reaches only those, and a read token gets `memory_recall` only.

### Fixed
- `phdb serve --memory` now refuses to start with a `--memory-dim` or `--memory-normalization` that differs from the existing store's, as `phdb mcp` always has. It used to start and ignore the flag.
