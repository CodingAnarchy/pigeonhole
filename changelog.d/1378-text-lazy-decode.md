### Changed
- `pigeonhole-text`: once a MaxScore walk advances, it skips the postings below the candidate with a key compare. It copies and decodes only the posting it stops on. Decodes fall by 39% over the fuzzy-plus-term bench queries (by 71% on the slowest). Results are the same (#1378).
