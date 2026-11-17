### Changed
- Text search: a search on a common word does less work per posting. It no longer allocates for each document it reads, and it passes over documents that cannot reach the top results without copying their ids. On a 100,000-document index, a top-10 search of the most common word went from about 6.2 ms to 4.7 ms (one laptop). Results and scores are unchanged (#1585).
