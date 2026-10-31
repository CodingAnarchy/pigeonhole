### Fixed
- A commit waiting for memtable room is never refused with `Busy` before `write_stall_timeout_nanos` (#1235). Its wait was counted in whole milliseconds and could be refused up to 1 ms early (up to 2 ms where the millisecond count wrapped, every 49.7 days of uptime). It is now refused at the timeout or within 2 ms after it.
