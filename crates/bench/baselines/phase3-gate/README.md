# Phase 3 gate window (#405, #406)

The Phase 3 gate is measured on the reference machine (D5): a Hetzner dedicated AX102 (Ryzen 9 7950X3D, 16 cores, 128 GB ECC, 2× 1.92 TB Datacenter Edition NVMe). It is rented for the measurement window only. `run-window.sh` runs every measurement the gate needs, unattended, in about 10.1 hours at full scale. It is resumable, and it leaves one tarball to copy back.

## Before renting

- Every #406 contents item is merged and correctness-tested on Linux CI.
- The dry run passes. It is `.github/workflows/gate-dry-run.yml` (Actions, "Gate window dry run"): the whole script at `--scale small` on a GitHub Linux runner. It finds script bugs before rented hours do. Its numbers mean nothing.
- Pick the commit to measure. The window measures exactly that commit.

## Setting up the machine

1. **Install the OS on one NVMe only.** In Hetzner's `installimage`, choose Ubuntu 24.04 and turn software RAID off (`SWRAID 0`). Install to `nvme0n1` and leave `nvme1n1` out of the drive list. Hetzner's default mirrors both drives (RAID1), which puts every measured write through md. The second NVMe stays raw for the data.
2. **Log in as root and clone the repository at the chosen commit:**
   ```sh
   git clone https://github.com/CodingAnarchy/pigeonhole && cd pigeonhole && git checkout COMMIT
   crates/bench/baselines/phase3-gate/setup.sh
   ```
   `setup.sh` installs the build packages, `nvme-cli`, `xfsprogs` and the Rust toolchain. It turns io_uring on, sets the `performance` governor, and lifts root's memlock limit. Log in again afterwards, so the memlock limit applies.
3. **Make the data file system on the second NVMe** (this erases it; check the device with `nvme list` first):
   ```sh
   mkfs.xfs -f /dev/nvme1n1 && mkdir -p /data && mount -o noatime /dev/nvme1n1 /data
   ```
4. **Check the drive.** `nvme id-ctrl /dev/nvme1n1 | grep -E '^(mn|fr|vwc)'` shows the model, firmware and write cache. A Datacenter Edition drive has power-loss protection. The setup step records this in `machine.json`, and warns if the write cache is volatile.

## Running

```sh
tmux new -s gate
crates/bench/baselines/phase3-gate/run-window.sh /data/gate /root/results-$(date +%Y%m%d)
```

- **Detach** (`Ctrl-b d`) and come back later. Nothing else should run on the machine meanwhile: the setup step refuses to start above a load of 1.0.
- **The steps:**
  1. `setup`: records the machine, and refuses a setup that would not count.
  2. `build`, then `memtable-lookup`: the memtable's 1M- and 10k-entry point lookup (#17; criterion, pinned to one core).
  3. `phase2-gate`, twice (`phase2-gate-2`): the D193 floors. The second run is there because #387's row-read p99 is close to 1.5× SQLite EAV's (1.58× on the Mac), and one run varies by about 5%. The first runs here should become the new D193 baseline, an owner decision, since the current one is from the Mac.
  4. `all` against RocksDB, three times: twice on io_uring (the reproducibility pair) and once on pread (the backend decision).
  5. `latency`: point gets and durable group commit.
  6. `ycsb-a-shards`: ycsb-a with one client, at the default shard count against 1 and 4 shards and longer shard spins, beside RocksDB (D198 item 2, #478).
  7. `group-sync-depth`: durable group commits at a group sync depth of 1, 2 and unlimited (D207's default).
  8. `row-cache`: the row cache's default.
  9. `scaling`: D204, at 1, 4, 8 and 16 shards.
  10. `open-latency`, `cold-get` and `cold-scan`: the `phase3-io` steps.
  11. `scan-cache`: the ordered scan from cache.
  12. `report`.
- **Each step logs** to `RESULTS/logs/STEP.log`, and its real duration goes to `durations.tsv`.
- **A step whose feature the built binary lacks skips itself** (not marked done), and the window goes on.
- **A failed step stops the run.** Fix the cause and rerun the same command: finished steps (`RESULTS/STEP.done`) are skipped. A target that is missed is not a failed step; it is a verdict in the summary.
- **To rerun a step,** delete its `.done` file.

## Results

The last step writes `RESULTS/summary.md`. For every #406 target it gives the measured value, a verdict and the file that shows it. It also gives the data for the decisions the window settles: the row cache's default (D201), the I/O backend's default (#402), and the scan readahead (#431). Then it packs `RESULTS.tar.gz` and prints the `scp` line to copy it back. There are no GitHub credentials on the rented machine (owner decision, 2026-10-10): the results are committed from a development machine. Then cancel the server.

The instruction ceilings (D193) are not measured here: CI's instructions job on the same commit is their record.
