### Fixed
- **`phdb serve`: `Schema.DropTable` on a vector or text index's table now evicts that index** (#1588). A call on it answers `TableNotFound`, a background vector index's rebalancer stops, and an index made again under the name, even with `if_missing`, is the one served. Before, the dropped index's cached handle kept answering until the server restarted.
