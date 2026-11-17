### Added

- **`phdb serve --text-pack`** packs the server's text indexes in the background (#1585, ICR 0143): searches of common words read several times less, with no call to `Index::pack`. Off by default, since a packed index can't be opened by a 1.2 build: turn it on only once you won't roll back to 1.2. On a cluster a node packs the partitions it owns. Four counters come with it (`pigeonhole_serve_text_pack_passes_total`, `…_postings_total`, `…_refused_total`, `…_conflicts_total`).
- **`pigeonhole_text::Index::list`** names a database's text indexes.
