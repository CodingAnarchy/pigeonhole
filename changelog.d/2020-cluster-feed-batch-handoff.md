### Changed
- **A `phdb serve` cluster's feed subscriptions hand their events to each subscriber's connection in batches too** (#2020, #2014). This is #2012's single-node change applied to a cluster hub's members. Events, their order and their tokens are unchanged.
