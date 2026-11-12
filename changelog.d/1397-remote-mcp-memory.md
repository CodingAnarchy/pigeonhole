### Added
- `phdb serve --memory NAME` serves `phdb mcp`'s memory tools on `/mcp` too (D313), with the `memory://guide` resource, over the store its `Memory` service serves. A token limited to namespaces reaches only those, and a read token gets `memory_recall` only.

