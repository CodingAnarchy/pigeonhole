### Fixed
- io_uring backend (#976): when the kernel refused for now the system call that hands queued requests to it (`EAGAIN` under memory pressure), the backend failed requests the kernel could still run, and freed or reused their buffers:
  - a read or write whose own submission was refused failed at once, though it stayed queued and the next call handed it over;
  - a refusal of the completion thread's call (or of a thread reaping its own queue) failed every request in flight, and the shared queue's completion thread then stopped, so later requests never completed.

  The kernel could then read into, or write from, memory the request no longer owned. A refused call is now retried, and nothing queued or in flight fails because of it. If a queue breaks for good, its requests fail but their buffers are never freed.
