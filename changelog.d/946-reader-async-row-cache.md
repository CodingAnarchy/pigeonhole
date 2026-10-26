### Added
- A reader process's async latest reads (`get_async` and the async row read without a snapshot) go through its row cache, as its sync reads do (#946, D281).
