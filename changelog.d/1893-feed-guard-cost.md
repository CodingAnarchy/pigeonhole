### Changed
- A feed of every table checks each commit's tables in the pass that builds the commit, against a cache of the tables it has already found in the catalog. This takes back most of the cost the check for a 1.1.x session's drop added: about 4% fewer instructions a commit (#1874).
