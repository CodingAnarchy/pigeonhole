# 0088: One `Pigeonhole::open_application_owned`, returning `AppOwned`

**Status:** Approved (owner via the coordinator, 2026-10-28; #695, API audit 2e, c6480352190). Breaking before the 1.0 freeze. No behaviour change.

## Change
| Before | After |
|---|---|
| `open_application_owned(path, Options) -> Result<(Pigeonhole, Vec<Shard>)>`, which refused `background_workers(n > 0)` with `InvalidArgument` | `open_application_owned(path, Options) -> Result<AppOwned>` |
| `open_application_owned_with(path, Options) -> Result<AppOwned>` | removed: it is now `open_application_owned` |

`AppOwned { db, shards, workers }` (`#[non_exhaustive]`) is unchanged. `workers` is empty unless `Options::background_workers` asks for some, so a caller of the tuple form writes:

```rust
let AppOwned { db, shards, .. } = Pigeonhole::open_application_owned(path, options)?;
```

The engine's own `Engine::open_application_owned` and `open_application_owned_with` are internal and stay as they are. The public open always takes the `_with` path, which hands out zero workers when the count is 0.

## Why
There were two entry points for one mode, differing only in whether workers can be handed out. Freezing both would keep the refusal case, and the second name, for all of 1.x. Merging them is cheap only before 1.0.

## Callers changed
- **Tests:** api, app_owned_uring, app_owned_uring_no_rings, app_owned_workers (the refusal case becomes "zero workers hands none"), backpressure, idle_cpu and nonblocking.
- **Bench:** `runners/inline.rs`, and the examples' `support/{driver, shards}.rs`.
- **Docs:** the open's own rustdoc (the two docs merged), `Options::background_workers`, `AppOwned`, the agent reference, async.md and getting started.
