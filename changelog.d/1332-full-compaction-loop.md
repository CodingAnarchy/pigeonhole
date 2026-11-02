### Fixed
- A full compaction (`compact()`) could rewrite one SST into itself for ever, writing without end. It happened when a slot's single last-level SST was just over one output piece in length but its data fit within one (#1313). A full compaction now rewrites each slot at most once.
