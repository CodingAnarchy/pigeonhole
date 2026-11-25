### Fixed
- Closing a cluster replica copy (a map that drops it, or a promote) no longer waits for its follower's dial to an owner that doesn't answer, which could take the whole connect timeout. The dial now ends on the stop. The follower also no longer starts a re-bootstrap or a retry pause once it's stopped. A promote's wait for the copy to close could run out on the dial (#2162).
