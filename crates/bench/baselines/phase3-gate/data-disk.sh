#!/usr/bin/env bash
# Checks, then (with --format) formats and mounts, the gate window's data disk (#405). The
# disk is named by its stable /dev/disk/by-id link: NVMe kernel names (nvme0n1, nvme1n1)
# are not stable across reboots, and after a reinstall the OS can land on either.
#
#   data-disk.sh                                   list the NVMe disks by id, with their use
#   data-disk.sh /dev/disk/by-id/nvme-MODEL_SERIAL           check that disk, print the check
#   data-disk.sh /dev/disk/by-id/nvme-MODEL_SERIAL --format  check, then mkfs.xfs and mount /data
#
# The check refuses (exit 1) a disk that has partitions, holds a mounted or swap file
# system, backs the root file system, is part of md RAID or device-mapper, or carries any
# file-system or RAID signature (wipefs). It erases nothing without --format, and never
# formats a disk that fails the check.
set -euo pipefail

if [[ $# == 0 ]]; then
    echo "NVMe disks by id (whole disks, not partitions):"
    for l in /dev/disk/by-id/nvme-*; do
        [[ -e "$l" ]] || { echo "  (no NVMe disks by id)"; break; }
        # Partitions, and the eui./nvme. aliases of a disk already listed by model and serial.
        [[ "$l" == *-part* || "$l" == *_[0-9] || "$l" == */nvme-eui.* || "$l" == */nvme-nvme.* ]] && continue
        dev="$(readlink -f "$l")"
        echo "  $l -> $dev"
        lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS "$dev" | sed 's/^/      /'
    done
    echo "Pick the disk with no partitions and nothing mounted, then: $0 /dev/disk/by-id/... [--format]"
    exit 0
fi

id="$1"
format="${2:-}"
[[ "$id" == /dev/disk/by-id/* ]] || { echo "data-disk.sh: name the disk by /dev/disk/by-id/..., not $id" >&2; exit 2; }
[[ -z "$format" || "$format" == --format ]] || { echo "data-disk.sh: unknown option $format" >&2; exit 2; }
dev="$(readlink -f "$id")"
[[ -b "$dev" ]] || { echo "data-disk.sh: $id does not resolve to a block device" >&2; exit 2; }
name="$(basename "$dev")"

echo "disk:   $id"
echo "device: $dev (today's kernel name; it can change at the next boot)"
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,SERIAL "$dev"

fail=()
[[ "$(lsblk -ndo TYPE "$dev")" == disk ]] || fail+=("$dev is not a whole disk")
parts="$(lsblk -nro NAME "$dev" | tail -n +2)"
[[ -z "$parts" ]] || fail+=("it has partitions or holders: $(echo "$parts" | tr '\n' ' ')")
mounts="$(lsblk -nro MOUNTPOINTS "$dev" | grep -v '^$' || true)"
[[ -z "$mounts" ]] || fail+=("something on it is mounted or used as swap: $(echo "$mounts" | tr '\n' ' ')")
root_src="$(findmnt -nvo SOURCE /)"
root_disk="$(lsblk -ndo PKNAME "$root_src" 2>/dev/null || true)"
[[ "$root_src" != "$dev" && "$root_disk" != "$name" ]] || fail+=("it backs the root file system ($root_src)")
if grep -q "\b$name\b" /proc/mdstat 2>/dev/null; then
    fail+=("it is part of md RAID (/proc/mdstat)")
fi
sigs="$(wipefs -n "$dev" 2>/dev/null | tail -n +2 || true)"
[[ -z "$sigs" ]] || fail+=("it carries signatures (wipefs -n): $(echo "$sigs" | awk '{print $NF}' | tr '\n' ' ')")

if (( ${#fail[@]} )); then
    echo "check: REFUSED"
    for f in "${fail[@]}"; do echo "  - $f"; done
    echo "Not the empty data disk; nothing was changed. Run $0 with no arguments to list the disks."
    exit 1
fi
echo "check: ok (whole disk, no partitions, nothing mounted, not the root disk, no RAID, no signatures)"

if [[ "$format" == --format ]]; then
    mkfs.xfs -f "$dev"
    mkdir -p /data
    mount -o noatime "$id" /data
    findmnt /data
    echo "mounted $id on /data"
else
    echo "Run again with --format to make the XFS file system and mount it on /data."
fi
