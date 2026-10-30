### Added
- `PIGEONHOLE_BALANCE_LOG`: set, the engine logs its balancer decisions, tablet changes and write-skew transitions to stderr, each line stamped with wall-clock seconds; `phdb-bench` adds its phase marks. A diagnostic for measurement (a few hundred lines per bench run), separate from `PIGEONHOLE_TRACE` (#1188).
