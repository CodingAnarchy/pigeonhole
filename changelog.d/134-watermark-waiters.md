### Changed
- `Metrics::visibility_wakes.0` counts only the wake passes that reached a registered seqno. A watermark publish below every waiter's seqno now skips the waiter list without taking its lock (#134). Before, every publish with any waiter registered counted a pass, so the count and `visibility_contended` read lower than before for the same workload.
