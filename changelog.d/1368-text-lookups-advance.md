### Changed
- `pigeonhole-text`: a search's non-essential terms step through their postings instead of looking each candidate up, once that's cheaper (#1368).
  - The results are the same.
  - A fuzzy term beside a common term is many times faster at the tail: 1.4M point reads became 21k plus 236k steps on the slowest query measured.
