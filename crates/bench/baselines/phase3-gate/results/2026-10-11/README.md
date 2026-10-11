# Phase 3 gate window, 2026-10-11 (reference machine)

The first Phase 3 gate run on the reference machine (#405): a Hetzner AX102 (Ryzen 9 7950X3D, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP). `run-window.sh` at full scale, 131 minutes in all.

- **Commits:** steps up to `all-pread` ran at f333d48; the window was resumed at 8f675ed after #551 (a bench-only fix to D213's pre-split placement), with an identical engine. See `summary.md`'s step table.
- **Start here:** `summary.md` (every #406 target with its value and verdict, and the decision tables).
- **D208 baseline:** `phase2-gate.json` and `phase2-gate-2.json` are the reference runs future Phase 2 gate runs on this hardware compare against.
- **Not committed:** the raw step logs and criterion's HTML reports. The full tarball is the `phase3-gate-2026-10-11` GitHub release asset.
- **Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection; the measured fdatasync (p50 25 µs, p99 75 µs) agrees.
