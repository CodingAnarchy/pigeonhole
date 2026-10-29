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
- The public API of the semver crates (`pigeonhole`, `pigeonhole-arrow`, and `pigeonhole-cli`'s library, which is all hidden) is recorded in `crates/*/public-api.txt`. CI's `scripts/public-api.sh check` fails on any difference. After an approved change, run `scripts/public-api.sh bless`; it needs `cargo install cargo-public-api --version 0.52.0` and `rustup toolchain install nightly-2026-10-05`. Commit the snapshot with the change, so review sees the API diff. Test and bench hooks go behind `pigeonhole`'s `__unstable-hooks` feature and `#[doc(hidden)]`, which keeps them out of the snapshot (ICR 0085).
- **The 1.0 golden files never change; they only gain siblings.** Every on-disk golden file in `crates/format/tests/golden` is listed with its hash in `crates/format/tests/golden-frozen.txt`, which is append-only. A new encoding gets a new file (`superblock_v5.bin`, say) and a new line at the end, and the old file stays with the test that decodes it. The format crate's golden test and a CI step (`scripts/golden-frozen.py`) enforce this. The shared-memory files (`shm_*`) are exempt, since the layout is outside the compatibility promise (FORMAT.md §13).
- **No test-only production paths.** Tests call the code production runs. `#[cfg(test)]` and `test-hooks` items in `src/` are read-only inspection helpers or fault-injection seams only, never an alternate operation; when a production path changes, its tests move to the new path and the old one is deleted. Nothing stays alive only for loom models or doctests (see `AGENTS.md`, Conventions).
- No new dependency without a passing `cargo deny check` and a one-line justification in the PR.
- A task is done when its brief's acceptance tests and the workspace simulation suite pass in CI. **Nothing merges on red.**
- When the spec is silent or contradictory, stop and record the question and your interim behavior in `docs/design/questions/<crate>.md` (see its [README](docs/design/questions/README.md)) instead of guessing; the coordinator turns it into a numbered decision.

## Deferred work
Anything left for later is a GitHub issue titled `[crate] summary`, labeled with the crate and `phase-N`, and assigned to the matching **Phase N** milestone. A phase's gate requires its milestone to be empty (D62; `scripts/phase-gate.sh N`). Never leave deferred work only in a doc, a code comment or a PR description.

## Workflow
1. Work on a branch (`crate/<name>-<topic>`), ideally in its own git worktree.
2. Before pushing, run locally (builds go through `cargo quota`; see [local resources](#seed-sweeps-and-local-resources)):
   ```sh
   cargo fmt --all
   cargo quota clippy --workspace --all-targets --all-features -- -D warnings
   cargo quota test --workspace --all-features
   ```
   Crates with `unsafe` also run `cargo +nightly miri test -p <crate>`; crates with concurrency run `RUSTFLAGS="--cfg loom" cargo test --release -p <crate> --lib loom`.
3. Clean up before the PR is ready: remove temporary diagnostics, debug prints, commented-out code and probe tests from the diff; keep a test hook only if a committed test uses it; delete scratch files (logs, traces, copied binaries) you created outside `target/`; stop any background processes you started. Leave deferred work as a milestoned issue, not a TODO.
4. Open a pull request against `main`. CI must be green before merge.

## Releases
A PR with a user-visible change adds a fragment file under [`changelog.d/`](changelog.d/README.md) instead of editing `CHANGELOG.md`, so parallel PRs don't conflict. `python3 scripts/changelog.py check` checks the fragments; the Changelog workflow runs it on every PR.

Before tagging a release: update [`CHANGELOG.md`](CHANGELOG.md) with `python3 scripts/changelog.py fold <version>` (it folds the `changelog.d/` fragments and any `Unreleased` entries under the new version, updates the compare links and deletes the fragments; `--dry-run` shows the result first), update the maturity note in `README.md`, `crates/pigeonhole/README.md`, `docs/guide/README.md` and `docs/status.md`, and check the install version in the READMEs and `docs/guide/getting-started.md`. Bump every crate's version and every exact (`=`) pin of the internal crates in the workspace `Cargo.toml` together, in the same version-bump PR (D282: `pigeonhole` pins each internal crate exactly; a pin left behind resolves to the previous release's crate).

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
- **The standard sweeps for engine, WAL and I/O changes** (300 seeds, 6 chunks each), all five on the PR's head:
  - engine `model_check` with `PIGEONHOLE_DEFERRED_IO=1`;
  - engine `model_check` plain;
  - engine `counters` plain (`model_check` with counter families: `sum`, `sum_ttl`);
  - `pigeonhole` `model`;
  - engine `tablets` with `PIGEONHOLE_DEFERRED_IO=1`.

  The `tablets` sweep is the only standard sweep that crashes around tablet changes and new streams with the device's completion order shuffled. It found #520 (D211), which the others missed. The `counters` sweep is the only one with counter-kind families, the only families D186/D287 combining and D291 checkpoints touch. Nobody ran it until perf287's long sweeps of 2026-10-28 found a live-read failure there.
- Whoever dispatches a sweep watches it to completion and reports the result.
- Agent worktrees carry an untracked `.cargo/config.toml` that caps build jobs and test threads; don't override it.
- Build through [`cargo-quota`](https://crates.io/crates/cargo-quota) (`cargo install cargo-quota`): `cargo quota build`, `cargo quota test`, and so on, or a shell wrapper that routes them. It prunes the least recently used build artifacts before each build, keeping every worktree's `target/` together under the budget in `[workspace.metadata.quota]` (2% of the disk, shared across worktrees). `cargo quota status` shows where things stand. Plain `cargo` still works; the next `cargo quota` run catches up.

### Test environment variables
These are the only `PIGEONHOLE_*` variables the code reads. All of them are for tests. Library code reads only `PIGEONHOLE_TRACE` in every build, and `PIGEONHOLE_IO` and `PIGEONHOLE_DIRECT` only with `pigeonhole`'s `__unstable-hooks` feature (ICR 0085), which the workspace's tests and the bench turn on and an application never does. D221 and D222 retired `PIGEONHOLE_GROUP_SYNC_DEPTH` and `PIGEONHOLE_READAHEAD`, and ICR 0085 retired `PIGEONHOLE_TAIL_INDEX`. Add a new one here or not at all.

| Variable | Read by | Effect |
|---|---|---|
| `PIGEONHOLE_SEED`, `PIGEONHOLE_SEEDS` | engine `model_check`, `counters`, `slots`, `tablets`, `deferred_waits`; pigeonhole `model`, `counters` | First seed and number of seeds. The Sweep workflow sets both per chunk. The `counters` targets rerun `model_check` / `model` with counter families (D179). |
| `PIGEONHOLE_SEED` alone | shm `protocol` and `multiprocess`, memtable and pager `tests/common` | The one seed to run. |
| `PIGEONHOLE_TABLET_CHANGES` | engine harness (`tests/common`), pigeonhole `model` | Engine: `1` adds the fast balancer so tablets change during a run, `0` turns tablet changes off. Pigeonhole: `0` turns them off. |
| `PIGEONHOLE_DEFERRED_IO` | engine harness, pigeonhole `model` | `1` runs on `SimVfs` with deferred I/O completion. |
| `PIGEONHOLE_SHARD_THREADS` | pigeonhole `model` | `k` runs the shards on `k` shared engine-owned driver threads (`Options::shard_threads`, D289); unset or `0`, a thread per shard. |
| `PIGEONHOLE_DRIVER_CHURN` | pigeonhole `model` | `1` forces the load controller's driver target to a seeded count after one step in ten (D289 PR 2), so shards move between drivers throughout. |
| `PIGEONHOLE_ADAPTIVE_SHARD_THREADS` | pigeonhole `model` | `1` turns the load controller on (`Options::adaptive_shard_threads`, D289) when `PIGEONHOLE_SHARD_THREADS` is unset or `0`. With churn, a forced target goes back to the controller one time in three. |
| `PIGEONHOLE_ROW_CACHE` | engine harness (checker runs), pigeonhole `model` | `1` turns on a 64 KiB writer row cache (D201, D290), small enough to evict. |
| `PIGEONHOLE_CHECKPOINT_STEPS` | engine harness (checker runs), pigeonhole `model` | `n` sets `counter_checkpoint_steps` (D291); `1` checkpoints after every counter read that folds anything. Counter-kind families exist only in the `counters` targets. |
| `PIGEONHOLE_BACKGROUND_WORKERS` | engine harness (checker runs) | `n` application-owned background workers (D284), driven beside the shards (as scheduler tasks in the crash runs). None in runs with deferred I/O: the simulator defers a worker's I/O, which the real backends complete within its slice, and the run then measures that artifact. |
| `PIGEONHOLE_FRONT` | pigeonhole `model` | `async` or `mixed` routes operations through the async front door (all of them, or a seeded choice per operation); anything else is sync. |
| `PIGEONHOLE_REAL_FS` | pigeonhole `model` | `1` runs on real files in a temporary directory instead of `SimVfs`. Fault plans and power-loss crashes are off, and the tests that need them skip. |
| `PIGEONHOLE_HUNG_STACKS` | pigeonhole `model` | `1` makes the hang watchdog print every thread's stack (on by itself when `CI` is set). |
| `PIGEONHOLE_SWEEP_STEP` | engine `tablets` and `model_check` crash sweeps | Crash after every Nth mutating operation instead of the default step. |
| `PIGEONHOLE_SHARDS_64` | engine `model_check` | Runs the 64-shard case, which is skipped otherwise. |
| `PIGEONHOLE_IO` | pigeonhole library with `__unstable-hooks` (`Options` and `ReaderOptions` with no `io_backend` set) | `uring` runs on io_uring and fails to open where it is unavailable, `pread` runs on the thread pool, and anything else uses io_uring where available (`auto`). The Linux CI job sets `uring` so the suites cover the io_uring backend (#402). |
| `PIGEONHOLE_DIRECT` | pigeonhole library with `__unstable-hooks` (`Options` and `ReaderOptions` with no `direct_io` set), engine harness | `1` reads and writes SST and blob extents through a direct-I/O handle (#403); on `SimVfs` every such I/O must be aligned, so the simulated suites check the direct path. |
| `PIGEONHOLE_TRACE` | engine library and engine harness | The engine logs its close, checkpoint, manifest, flush and tablet-change steps to stderr (`shard::trace!`), and the harness logs each operation. The engine writes to the stderr handle itself, so a test harness's output capture does not hide it; `pigeonhole_engine::set_tracing` turns it on or off at run time. |
| `PIGEONHOLE_TRACE_LINES` | engine harness | Trace lines a checker failure prints (default 40). |
| `PIGEONHOLE_SEED_TIMEOUT` | pigeonhole `model` | Seconds one seed may run before the watchdog reports it hung (test, seed, configuration and the step it was on), turns on the engine's trace for 5 s to show what the shards do, and aborts (default 120; #244). |
| `PIGEONHOLE_FAIL_LOG` | pigeonhole `model` | A file each failing or hung seed appends a line to (test, seed, configuration and the checker's message), so a filtered local loop or a sweep cannot lose it (#897). |
| `PIGEONHOLE_SUMMARY_LOG` | pigeonhole `model` | A file a target appends its totals over its seeds to, pass or fail: the recovery arm's forced re-arms (`forced_rearms`, `forced_preempted`) and recoveries (#897). Sweeps keep it as the `summary-<seed>` artifact. |
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
