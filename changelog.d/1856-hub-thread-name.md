### Changed
- **The feed hubs' threads are named `phdb-serve-hub`,** not `phdb-serve-feed-hub` (#1856). Linux keeps 15 bytes of a thread's name, so the old one showed as `phdb-serve-feed`, the delivery threads' name, and a profile or `top -H` couldn't tell the two apart.
