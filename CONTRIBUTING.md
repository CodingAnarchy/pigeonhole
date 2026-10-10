# Contributing to Pigeonhole

Pigeonhole is built by a mix of humans and coding agents. The rules below apply to both.

## Read first
- [`docs/design/spec.md`](docs/design/spec.md) — the design. Every crate's behavior is defined there.
- [`docs/design/task-briefs.md`](docs/design/task-briefs.md) — one brief per crate: goal, what to read, what it owns, when it is done.
- [`docs/design/decisions/`](docs/design/decisions/README.md) — numbered decisions that refine the spec; they win over it. Read the index, then the decisions that touch your work.
- [`FORMAT.md`](FORMAT.md) — every on-disk and shared-memory byte (frozen at the interface freeze).

## Workspace rules
- **Dependencies only point down.** No crate depends on a crate in its own layer or above; `pigeonhole-format` depends on no other workspace crate. Layers, bottom to top: `format` → `io` → (`pager`, `wal`, `memtable`, `cache`, `runtime`, `shm`, `sim`) → `sst` → `compaction` → `engine` → `pigeonhole` → (`arrow`, `cli`, `capi`, `bench`).
- **Contracts first.** Public traits and types are written and reviewed before implementation. Every crate below the engine ships an in-memory or mock implementation for the layer above to test against.
- **`unsafe` is fenced.** Only `pigeonhole-io`, `pigeonhole-cache` and `pigeonhole-memtable` may contain `unsafe`, and every block carries a `// SAFETY:` comment (enforced by `clippy::undocumented_unsafe_blocks`). Every other crate has `#![forbid(unsafe_code)]`. The one exception: any crate's test or bench binaries (`tests/`, `benches/`) may define a counting `GlobalAlloc` that forwards to `System`, to check that a path allocates nothing.
- **Every crate runs under the simulator.** All file access goes through the `Vfs` trait so faults, time and scheduling can be controlled.
- **Definition of done:** the crate's acceptance tests (see its brief) pass, the workspace simulation suite passes, and its public API has rustdoc with examples.
- **License: MIT.** Dependencies must be MIT-compatible (`deny.toml`, checked in CI).
- **MSRV** is latest stable minus two (currently 1.96), checked in CI. Linux, macOS and Windows are all supported.

## Agent rules
- One agent owns one crate per task and edits only that crate and its tests. Shared files (`Cargo.toml` workspace deps, `FORMAT.md`, `docs/`) change only when the task says so.
- Frozen interfaces change only through an **interface-change request**: add a short note to `docs/design/icr/NNNN-title.md` naming the change and every caller, and get it approved before changing code.
- No new dependency without a passing `cargo deny check` and a one-line justification in the PR.
- A task is done when its brief's acceptance tests and the workspace simulation suite pass in CI. **Nothing merges on red.**
- When the spec is silent or contradictory, stop and record the question and your interim behavior in `docs/design/questions/<crate>.md` (see its [README](docs/design/questions/README.md)) instead of guessing; the coordinator turns it into a numbered decision.

## Deferred work
Anything left for later is a GitHub issue titled `[crate] summary`, labeled with the crate and `phase-N`, and assigned to the matching **Phase N** milestone. A phase's gate requires its milestone to be empty (D62; `scripts/phase-gate.sh N`). Never leave deferred work only in a doc, a code comment or a PR description.

## Workflow
1. Work on a branch (`crate/<name>-<topic>`), ideally in its own git worktree.
2. Before pushing, run locally:
   ```sh
   cargo fmt --all
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo test --workspace --all-features
   ```
   Crates with `unsafe` also run `cargo +nightly miri test -p <crate>`; crates with concurrency run `RUSTFLAGS="--cfg loom" cargo test --release -p <crate> --lib loom`.
3. Clean up before the PR is ready: remove temporary diagnostics, debug prints, commented-out code and probe tests from the diff; keep a test hook only if a committed test uses it; delete scratch files (logs, traces, copied binaries) you created outside `target/`; stop any background processes you started. Leave deferred work as a milestoned issue, not a TODO.
4. Open a pull request against `main`. CI must be green before merge.

## Releases
A PR with a user-visible change adds a fragment file under [`changelog.d/`](changelog.d/README.md) instead of editing `CHANGELOG.md`, so parallel PRs don't conflict.

Before tagging a release: update [`CHANGELOG.md`](CHANGELOG.md) (fold the `changelog.d/` fragments and any `Unreleased` entries under the new version, then delete the fragments), update the maturity note in `README.md`, `crates/pigeonhole/README.md`, `docs/guide/README.md` and `docs/status.md`, and check the install version in the READMEs and `docs/guide/getting-started.md`.

## Seed sweeps and local resources
Several agents often build and test on one machine, so local runs stay small:
- Run at most one `cargo` command at a time per worktree, and no seed sweep beyond about 20 seeds locally.
- Larger sweeps run on CI runners with the `Sweep` workflow (`.github/workflows/sweep.yml`). Push the branch, then:
  ```sh
  gh workflow run sweep.yml --ref <branch> -f package=pigeonhole -f test=model \
    -f first=1 -f count=300 -f chunks=6 -f env="PIGEONHOLE_TABLET_CHANGES=1"
  gh run list --workflow sweep.yml --branch <branch> --limit 1   # then: gh run watch <id>
  ```
  Each chunk sets `PIGEONHOLE_SEED`/`PIGEONHOLE_SEEDS`; a failing chunk's log names the seed. Reproduce that one seed locally.
- Agent worktrees carry an untracked `.cargo/config.toml` that caps build jobs and test threads; don't override it.

### Test environment variables
These are the only `PIGEONHOLE_*` variables the code reads. All of them are for tests, and only `PIGEONHOLE_TRACE` and `PIGEONHOLE_IO` are read by library code. Add a new one here or not at all.

| Variable | Read by | Effect |
|---|---|---|
| `PIGEONHOLE_SEED`, `PIGEONHOLE_SEEDS` | engine `model_check`, `counters`, `slots`, `tablets`, `deferred_waits`; pigeonhole `model`, `counters` | First seed and number of seeds. The Sweep workflow sets both per chunk. The `counters` targets rerun `model_check` / `model` with counter families (D179). |
| `PIGEONHOLE_SEED` alone | shm `protocol` and `multiprocess`, memtable and pager `tests/common` | The one seed to run. |
| `PIGEONHOLE_TABLET_CHANGES` | engine harness (`tests/common`), pigeonhole `model` | Engine: `1` adds the fast balancer so tablets change during a run, `0` turns tablet changes off. Pigeonhole: `0` turns them off. |
| `PIGEONHOLE_DEFERRED_IO` | engine harness, pigeonhole `model` | `1` runs on `SimVfs` with deferred I/O completion. |
| `PIGEONHOLE_SWEEP_STEP` | engine `tablets` and `model_check` crash sweeps | Crash after every Nth mutating operation instead of the default step. |
| `PIGEONHOLE_SHARDS_64` | engine `model_check` | Runs the 64-shard case, which is skipped otherwise. |
| `PIGEONHOLE_IO` | pigeonhole library (`Options` and `ReaderOptions` with no `io_backend` set) | `uring` runs on io_uring and fails to open where it is unavailable, `auto` uses it where available, anything else `pread`. The Linux CI job sets `uring` so the suites cover the io_uring backend (#402). |
| `PIGEONHOLE_TRACE` | engine library and engine harness | The engine logs its close, checkpoint, manifest, flush and tablet-change steps to stderr (`shard::trace!`), and the harness logs each operation. The engine writes to the stderr handle itself, so a test harness's output capture does not hide it; `pigeonhole_engine::set_tracing` turns it on or off at run time. |
| `PIGEONHOLE_TRACE_LINES` | engine harness | Trace lines a checker failure prints (default 40). |
| `PIGEONHOLE_SEED_TIMEOUT` | pigeonhole `model` | Seconds one seed may run before the watchdog reports it hung (test, seed, configuration and the step it was on), turns on the engine's trace for 5 s to show what the shards do, and aborts (default 120; #244). |
| `PIGEONHOLE_SHM_CHILD`, `PIGEONHOLE_MEMTABLE_CHILD`, `PIGEONHOLE_IO_LOCK_PROBE`, `PIGEONHOLE_TEST_SMALL_SHM` | multi-process tests | Set by a test when it re-runs itself as a child process. Don't set them yourself. |

## Performance discipline
Optimization, simplification and performance are maintained continuously, not bolted on: keep hot paths allocation-free where the spec says so, prefer the simplest structure that meets the brief, and add a criterion benchmark for any path with a latency target. Note measured numbers in the PR.

### Floors: what Phase 2 reached may not regress (D193)
- **Per change.** The `Instructions per cell` workflow counts instructions per unit with callgrind on Linux for every hot-row state, read shape and write shape, at the PR's base and head. It fails a PR that makes any shape worse than its threshold (`THRESHOLDS` in `.github/workflows/instructions.yml`).
- **Absolute ceilings.** The same job fails if any shape's count at the head is above its ceiling in `crates/bench/baselines/instruction-ceilings.txt`. That stops small regressions under the threshold from adding up.
  - When a change improves a shape, lower its ceiling in the same PR: `scripts/instruction-ceilings.py lower COUNTS` with the head's counts (from the job summary or `scripts/instructions-per-cell.sh`). The job summary says which ceilings can go lower.
  - A ceiling is never raised without an owner decision; cite it in the commit.
  - A new shape needs a ceiling in the same PR that adds it.
- **Gate runs.** Every official sparse-wide gate run (the Phase 3 gate, and before each release) is compared with the Phase 2 baseline: `crates/bench/baselines/phase2-gate/run-gate.sh`, on a quiet machine. A loss beyond run-to-run noise in Pigeonhole's throughput, p99, p99.9, or get, put, row-read or scan p99 blocks the gate or release until it's fixed or the owner accepts it (see the README there).
