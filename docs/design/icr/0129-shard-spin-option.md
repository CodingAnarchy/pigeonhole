# 0129: `Options::shard_spin` becomes public

**Status:** Approved. The coordinator assigned the number (2026-11-09), choosing this over a hidden feature for `phdb serve` (#1644).

## The change
- **New public name, additive:** `pigeonhole::Options::shard_spin(self, window: Duration) -> Self`.
  - It was a test and bench hook behind `__unstable-hooks` and `#[doc(hidden)]` (ICR 0085).
  - Its behaviour is unchanged: a window set here is fixed (no adaptive cap), and `Duration::ZERO` sleeps at once.
- **Unchanged and still hooks:** `Options::shard_spin_cap` and `Options::commit_spin`.

## Why
- **D198 decision 3** promised it: "Both windows are documented options that can be set to 0, for battery-powered or CPU-constrained users."
- **`phdb serve` now needs it** in its normal build. D198 amendment 4 makes serve run with no shard poll by default, because the poll was a quarter to half of its CPU per put (perf287's g51 on #1644), and the wakeup it saves is small next to a network round trip.
- **The alternative** was a hidden `__unstable-serve` feature for the CLI. It would have kept a knob D198 says users should see out of the docs.

## Callers changed
`phdb serve` (`crates/cli/src/serve/mod.rs`): its default is now `shard_spin(Duration::ZERO)`, and `--shard-spin <US|adaptive>` changes it.
