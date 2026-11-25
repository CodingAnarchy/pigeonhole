### Fixed
- Closing a cluster replica copy (a map that drops it, or a promote) no longer waits out its follower's retry pause, or a re-bootstrap from an owner that has lost its lease, when the stop came as the follower's last apply failed. Connecting to an owner that doesn't answer also stops at once. A promote's wait for the copy to close could run out on that (#2162).
