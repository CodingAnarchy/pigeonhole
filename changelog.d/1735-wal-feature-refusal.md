### Fixed
- **An unknown WAL segment feature bit now fails the open** (#1735). A segment header from a newer build that sets a `features` bit this build doesn't know is refused with `UnsupportedFeature`, as `phdb check` already did, so the open no longer skips that segment and its records as a blank slot. No build sets such a bit yet.
