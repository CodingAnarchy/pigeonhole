#!/usr/bin/env python3
"""The setup step of the gate window (#405): records the machine, kernel, drive and file
system in machine.json, and refuses to start a run that would not count.

    machine.py DATA_DIR OUT.json SCALE

Hard failures (exit 1): not Linux 6.1 or newer; io_uring disabled; DATA_DIR on the same file
system as / (the OS drive); less free space under DATA_DIR than the run needs; a busy
machine (1-minute load above 1.0). At `--scale small` (the dry run) only the first two and
the free space are enforced. Warnings (printed, kept in the JSON): CPU frequency governor
not `performance`; DATA_DIR on md RAID or device-mapper; a drive whose write cache is
volatile (no power-loss protection); transparent hugepages `always`."""
import json, os, platform, shutil, subprocess, sys

data, out, scale = sys.argv[1:4]
full = scale == "full"
# Peak space: cold-get's 64 GiB table, cold-scan's 16 GiB, the scan-cache table and a
# full-scale bench store, with headroom for compaction.
NEED_GB = 200 if full else 10


def sh(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return r.stdout.strip()
    except Exception as e:  # recorded, not fatal
        return f"error: {e}"


def read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


failures, warnings = [], []
kernel = platform.release()
major, minor = (int(x) for x in kernel.split("-")[0].split(".")[:2])
if (major, minor) < (6, 1):
    failures.append(f"kernel {kernel}: the gate needs Linux 6.1 or newer (io_uring)")
uring = read("/proc/sys/kernel/io_uring_disabled")
if uring not in (None, "0"):
    failures.append(f"io_uring_disabled = {uring}: set it to 0 (sysctl kernel.io_uring_disabled=0)")

free_gb = shutil.disk_usage(data).free / 1e9
if free_gb < NEED_GB:
    failures.append(f"{free_gb:.0f} GB free under {data}; the run needs {NEED_GB} GB")
same_fs = os.stat(data).st_dev == os.stat("/").st_dev
if same_fs and full:
    failures.append(f"{data} is on the OS file system; use the second NVMe (README: setup)")
load1 = os.getloadavg()[0]
if load1 > 1.0 and full:
    failures.append(f"1-minute load {load1:.2f}: stop everything else first (quiet-machine rule)")

source = sh(f"df --output=source {data} | tail -1")
parent = sh(f"lsblk -ndo PKNAME {source}") or sh(f"lsblk -ndo KNAME {source}")
if source.startswith("/dev/md") or source.startswith("/dev/mapper") or parent.startswith("md"):
    warnings.append(f"{data} is on {source} (RAID or device-mapper); a raw NVMe partition is the reference setup")
nvme = parent if parent.startswith("nvme") else ""
vwc = sh(f"nvme id-ctrl /dev/{nvme} 2>/dev/null | grep -i '^vwc'") if nvme else ""
if nvme and vwc and not vwc.endswith("0") and "0x0" not in vwc:
    warnings.append(f"{nvme} reports a volatile write cache ({vwc}): check it has power-loss protection")
governors = set(sh("cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null").split())
if governors and governors != {"performance"}:
    warnings.append(f"CPU governor {sorted(governors)}: set performance (cpupower frequency-set -g performance)")
thp = read("/sys/kernel/mm/transparent_hugepage/enabled") or ""
if "[always]" in thp:
    warnings.append("transparent hugepages are 'always'; 'madvise' is the usual server setting")

machine = {
    "machine": True,
    "scale": scale,
    "kernel": kernel,
    "os": sh("grep PRETTY_NAME /etc/os-release | cut -d= -f2"),
    "cpu": sh("lscpu | grep 'Model name' | sed 's/.*: *//'"),
    "cpus": os.cpu_count(),
    "smt": read("/sys/devices/system/cpu/smt/control"),
    "governors": sorted(governors),
    "memory": sh("grep MemTotal /proc/meminfo"),
    "thp": thp,
    "data_dir": data,
    "device": source,
    "blockdev": parent,
    "drive": sh(f"nvme id-ctrl /dev/{nvme} 2>/dev/null | grep -E '^(mn|fr|vwc) '") if nvme else sh(f"lsblk -ndo MODEL /dev/{parent}"),
    "nvme_list": sh("nvme list 2>/dev/null"),
    "mount": sh(f"findmnt -no FSTYPE,OPTIONS -T {data}"),
    "free_gb": round(free_gb),
    "io_uring_disabled": uring,
    "memlock": sh("ulimit -l"),
    "load": sh("cat /proc/loadavg"),
    "commit": sh(f"git -C {os.path.dirname(os.path.abspath(__file__))} rev-parse HEAD"),
    "dirty": sh(f"git -C {os.path.dirname(os.path.abspath(__file__))} status --porcelain | head -5"),
    "rustc": sh("rustc --version"),
    "warnings": warnings,
    "failures": failures,
}
with open(out, "w") as f:
    json.dump(machine, f, indent=1)
for w in warnings:
    print(f"warning: {w}")
for e in failures:
    print(f"FAIL: {e}")
print(f"recorded {out}")
sys.exit(1 if failures else 0)
