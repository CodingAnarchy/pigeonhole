### Changed
- Scans: a scan whose row limit leaves at most 128 rows to return no longer reads blocks ahead (#1029, #712). Short scans finished before their readahead arrived and then waited for it, which made up most of ycsb-e's p99. Scans with no limit, or a larger one, read ahead as before.
