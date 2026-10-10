### Changed
- `pigeonhole-io`'s simulator: under deferred I/O without owner reaping, a wait blocked on the thread that turned deferred I/O on runs the simulated device (one operation, chosen by the seed) instead of waiting for ever, as a real device finishes I/O on its own (ICR 0028). Other threads' waits never run it.
