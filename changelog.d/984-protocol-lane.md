### Changed
- **Cross-shard commits wait less behind other work** (D285, #708). A shard now handles the messages that coordinate a cross-shard commit ahead of other queued work, including between two groups of commits in the same turn, rather than only at the start of its next turn. Results and durability are unchanged.
