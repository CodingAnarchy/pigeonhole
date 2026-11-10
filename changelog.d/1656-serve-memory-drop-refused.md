### Added
- **`Store::table_names`** in `pigeonhole-memory` (ICR 0107): the tables a store keeps its data in, for an application that must leave them alone.

### Fixed
- **`phdb serve --memory NAME` refuses `Schema.DropTable` of the store's own tables** with `FailedPrecondition` (#1651). Before, the drop went through: memory calls then failed, and a restart re-created the store empty.
