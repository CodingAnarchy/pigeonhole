### Changed
- Publishing a view after a manifest commit rebuilds only the SST slots the commit changed and shares the rest with the previous view. A one-cell flush against a 2,048-SST catalog makes 80% fewer allocations (1,612 to 322) (#499).
