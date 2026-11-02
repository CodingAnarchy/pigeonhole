### Fixed
- After a cross-shard commit fails with `OutcomeUnknown` because a shard could not apply its share, or a WAL sync fails, a commit made right after the error is now refused with `Closed` (nothing written), as documented, instead of being admitted and then failing with `OutcomeUnknown` (#1358).
