### Fixed
- Cluster read replicas (#2162):
  - A copy's freshness is stamped with when the message could have been made, not when the follower got to it after an apply or a stall. Before, a copy behind by a stall of D looked D fresher than it was to a bounded-staleness read.
  - A copy that a newer map lists again is no longer closed by a pass that read an older map. Before, it stayed closed, with the map listing it, until the next map.
  - A planned `phdb cluster promote` with the owner fenced waits up to 3 s for the copy to hear the owner after the fence, so its `lost_commits` is exact. `lost_commits_counted` now says whether the count is exact or a lower bound (a closed owner, or no word from the owner in time).
