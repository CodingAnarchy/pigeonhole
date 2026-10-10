### Added
- The Phase 3 gate-window runbook (#405): `crates/bench/baselines/phase3-gate/run-window.sh` runs every measurement the gate needs on the reference machine, resumable, with `setup.sh`, the machine checks, a `summary.md` of every #406 target and a tarball to copy back; `.github/workflows/gate-dry-run.yml` runs it at small scale on a Linux runner.
