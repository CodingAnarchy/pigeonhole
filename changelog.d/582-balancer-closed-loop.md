### Changed
- The tablet balancer settles under closed-loop write loads, where each shard writes only its own rows (D224, #582):
  - Write skew that appears only because other shards went idle no longer moves tablets.
  - Placement is corrected on the shards' write loads averaged over about 10 s, one change per 10 s.
  - Shards balancing at once never shed into the same shard.
  - Skew splits cut on write-weighted samples, so a hot row keeps its weight.
  - A tablet must go unwritten for 100 balancer passes (10 s at the default interval) before it is merged or consolidated, instead of 2.
