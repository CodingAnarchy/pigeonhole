# Decisions log

Project-level decisions that refine or deviate from [spec.md](spec.md) and [task-briefs.md](task-briefs.md). Newest last. An entry here wins over those files. Agents: when the spec is silent or contradictory, add a question under **Open questions** instead of guessing; the coordinator turns answers into numbered decisions.

## D1 — `pigeonhole-sim` does not depend on `pigeonhole`
The spec lists `pigeonhole` as a dependency of `sim` ("drives the public API from above"). That creates a cycle the moment `engine` or `pigeonhole` use `sim` in tests. Instead `sim` depends only on `io` and `format`; the full-stack simulation suites live in `crates/pigeonhole/tests/` and `crates/engine/tests/`, which take `pigeonhole-sim` as a dev-dependency. Same coverage, strictly downward graph.

## D2 — the simulated VFS lives in `pigeonhole-io`
Per the io brief, `SimVfs` (fault injection, deterministic from a seed) is an `io` backend at `pigeonhole_io::sim`. `pigeonhole-sim` builds the scheduler, crash points and reference model on top of it.

## D3 — writer lock is a byte-range lock on the lock page
"Multi-process readers" mentions `flock`; "Files and locks" specifies byte-range locks on a reserved lock page (OFD on Linux, `fcntl` with a per-process registry on macOS/BSD, `LockFileEx` on Windows). The more specific section wins.

## D4 — interface-freeze gate
The spec gates the interface freeze on owner review. The owner directed autonomous progress, so the coordinator reviews and approves interfaces, records the approval here, and the owner may revisit at any time through an interface-change request.

## D5 — reference hardware
No enterprise-NVMe Linux box with power-loss protection is attached to this project yet. Benchmarks run on available hardware (developer macOS arm64 and GitHub Linux runners), are reported in every run, and are labeled as non-reference. Performance gates are evaluated against those numbers until reference hardware is available.

## D6 — dependency policy
Allowed licenses: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Zlib, Unicode-3.0, CC0-1.0 (enforced by `deny.toml`). Engine crates keep dependencies minimal; each new dependency gets a one-line justification in the PR description.

## Open questions
_None yet._
