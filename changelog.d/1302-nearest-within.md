### Added
- `Nearest::run_within(bytes)` and `run_within_async` (D308): a nearest-neighbour search whose result is cut to a byte budget, as a `NearestPage`.
  - The page returns the nearest neighbours that fit, with `bytes`, `cut_by_budget` and `next_bytes`.
  - A neighbour counts its row key, qualifier and 16 bytes.
