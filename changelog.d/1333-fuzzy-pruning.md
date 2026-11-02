### Changed
- `pigeonhole-text`: fuzzy searches skip the expansions that can't reach the top `k` (MaxScore inside a fuzzy clause, #1333). The results are the same. Fuzzy queries over dense vocabularies are many times faster: at 100,000 documents, a 1-edit fuzzy term's median fell from 25 ms to 2.7 ms.
