# Phase 3 gate window (#405, #406)

The Phase 3 gate is measured on the reference machine (D5): a Hetzner dedicated AX102 (Ryzen 9 7950X3D, 16 cores, 128 GB ECC, 2× 1.92 TB Datacenter Edition NVMe). It is rented for the measurement window only. `run-window.sh` runs every measurement the gate needs, unattended, in about 10.1 hours at full scale. It is resumable, and it leaves one tarball to copy back.

## Before renting

- Every #406 contents item is merged and correctness-tested on Linux CI.
- The dry run passes. It is `.github/workflows/gate-dry-run.yml` (Actions, "Gate window dry run"): the whole script at `--scale small` on a GitHub Linux runner. It finds script bugs before rented hours do. Its numbers mean nothing.
- Pick the commit to measure. The window measures exactly that commit.

## Setting up the machine

1. **Install the OS on one NVMe only.** In Hetzner's `installimage`, choose Ubuntu 24.04 and turn software RAID off (`SWRAID 0`). Install to one drive and leave the other out of the drive list. Hetzner's default mirrors both drives (RAID1), which puts every measured write through md. The other NVMe stays raw for the data. **Don't rely on kernel names:** `nvme0n1` and `nvme1n1` can swap across reboots and reinstalls (on the AX102 the OS has landed on `nvme1n1`). Step 3 names the data disk by `/dev/disk/by-id`.
2. **Log in as root and clone the repository at the chosen commit:**
   ```sh
   git clone https://github.com/CodingAnarchy/pigeonhole && cd pigeonhole && git checkout COMMIT
   crates/bench/baselines/phase3-gate/setup.sh
   ```
   `setup.sh` installs the build packages, `nvme-cli`, `xfsprogs` and the Rust toolchain. It turns io_uring on, sets the `performance` governor, and lifts root's memlock limit. Log in again afterwards, so the memlock limit applies.
3. **Make the data file system on the empty NVMe, named by id.** `data-disk.sh` never takes a kernel name. Before it formats anything, it prints the disk and a check. It refuses a disk with partitions, anything mounted or used as swap, the root file system, md RAID or device-mapper, or any file-system or RAID signature. Without `--format` it only prints the check.
   ```sh
   crates/bench/baselines/phase3-gate/data-disk.sh                     # list the NVMe disks by id, with their partitions and mounts
   crates/bench/baselines/phase3-gate/data-disk.sh /dev/disk/by-id/nvme-MODEL_SERIAL            # the check only: read it
   crates/bench/baselines/phase3-gate/data-disk.sh /dev/disk/by-id/nvme-MODEL_SERIAL --format   # check again, then mkfs.xfs and mount on /data
   ```
   Use the by-id name the listing shows with no partitions and nothing mounted. **Never type `mkfs` on a `/dev/nvmeXn1` name by hand.** After a reboot, mount it by the same id: `mount -o noatime /dev/disk/by-id/nvme-MODEL_SERIAL /data`.
4. **Check the drive.** `nvme id-ctrl "$(readlink -f /dev/disk/by-id/nvme-MODEL_SERIAL)" | grep -E '^(mn|fr|vwc)'` shows the model, firmware and write cache. A Datacenter Edition drive has power-loss protection. The setup step records this in `machine.json`, and warns if the write cache is volatile. **Expect that warning on the AX102:** its Samsung PM9A3 reports `vwc 0x6` (a volatile write cache present) although it has power-loss protection, so the warning is expected on data-center drives and doesn't invalidate the run.

## Running

```sh
tmux new -s gate
crates/bench/baselines/phase3-gate/run-window.sh /data/gate /root/results-$(date +%Y%m%d)
```

- **cargo on PATH:** a fresh tmux shell isn't a login shell, so it may not have `~/.cargo/bin`, where `setup.sh`'s rustup puts cargo. `run-window.sh` sources `~/.cargo/env` when cargo isn't on PATH, and stops with a clear message if cargo is still missing. To run cargo by hand in the pane, `source ~/.cargo/env` first.
- **Detach** (`Ctrl-b d`) and come back later. Nothing else should run on the machine meanwhile: the setup step refuses to start above a load of 1.0.
- **The steps:**
  1. `setup`: records the machine, and refuses a setup that would not count.
  2. `build`, then `memtable-lookup`: the memtable's 1M- and 10k-entry point lookup (#17; criterion, pinned to one core).
  3. `phase2-gate`, twice (`phase2-gate-2`): the D193 floors. The second run is there because #387's row-read p99 is close to 1.5× SQLite EAV's (1.58× on the Mac), and one run varies by about 5%. Per D208, the first window's two runs will become the Phase 2 gate's reference baseline: copy `phase2-gate.json` and `phase2-gate-2.json` to `crates/bench/baselines/phase2-gate/reference-run1.json` and `reference-run2.json` and commit them, and `check.py` and `run-gate.sh` then use them.
  4. `all` against RocksDB, three times: twice on io_uring (the reproducibility pair) and once on pread (the backend decision).
  5. `latency`: point gets in memory (ycsb-c with every engine's block cache at 2 GiB, holding the table, and a warmup as long as the measured phase: D216), ycsb-a and skewed-multi-shard, and durable group commit in the D213 shape (one shard per physical core: 16 on the AX102, D220; the table split over every shard, commits in flight inline) on 2M commits (D219), plus 1/4/16 client threads; then the D213 shape on the preset's 200K commits (the first window's length) and again on pread (reported, not judged: it decides the io_uring default). The setup step also times the data drive's raw fdatasync, shown beside the group commit p99.
  6. `ycsb-a-shards`: ycsb-a with one client, at the default shard count against 1 and 4 shards and longer shard spins, beside RocksDB (D198 item 2, #478).
  7. `group-sync-depth`: durable group commits at a group sync depth of 1, 2 and unlimited (D207's default).
  8. `row-cache`: ycsb-c and ycsb-a with the row cache off, against the latency step's default (on, D216).
  9. `scaling`: D204, at 1, 4, 8 and 16 shards, each measured for at least 8 s (D219). The summary gives each run's stall breakdown: the share of shard time in write stalls, flushes, compactions and the longest operation.
  10. `open-latency`, `cold-get` and `cold-scan`: the `phase3-io` steps.
  11. `scan-cache`: the ordered scan from cache.
  12. `report`.
- **Each step logs** to `RESULTS/logs/STEP.log`, and its real duration goes to `durations.tsv`.
- **A step whose feature the built binary lacks skips itself** (not marked done), and the window goes on.
- **A failed step stops the run.** Fix the cause and rerun the same command: finished steps (`RESULTS/STEP.done`) are skipped. A target that is missed is not a failed step; it is a verdict in the summary.
- **To rerun a step,** delete its `.done` file.

## Results

The last step writes `RESULTS/summary.md`. For every #406 target it gives the measured value, a verdict and the file that shows it. It also gives the data for the decisions the window settles: the row cache's effect (on by default since D216), the I/O backend's default (#402), and the scan readahead (#431). Then it packs `RESULTS.tar.gz` and prints the `scp` line to copy it back. There are no GitHub credentials on the rented machine (owner decision, 2026-10-10): the results are committed from a development machine. Then cancel the server.

The instruction ceilings (D193) are not measured here: CI's instructions job on the same commit is their record.
