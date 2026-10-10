#!/usr/bin/env bash
# Prepares a fresh Ubuntu 24.04 machine for the gate window (#405): packages, the Rust
# toolchain, and the kernel settings run-window.sh checks. Idempotent; run as root.
#
#   crates/bench/baselines/phase3-gate/setup.sh
#
# It does not touch the data drive: formatting is one explicit command in README.md, so a
# mistyped device never erases anything from a script.
set -euo pipefail
[[ "$(id -u)" == 0 ]] || { echo "setup.sh: run as root" >&2; exit 2; }

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
# build-essential, clang and libclang: RocksDB's bindings (bindgen) and the C parts of the
# workspace; nvme-cli and xfsprogs: the drive; linux-tools: cpupower.
apt-get install -y -q build-essential clang libclang-dev pkg-config python3 git curl \
    nvme-cli xfsprogs tmux "linux-tools-$(uname -r)" linux-tools-common || \
    apt-get install -y -q build-essential clang libclang-dev pkg-config python3 git curl \
        nvme-cli xfsprogs tmux

if ! command -v cargo >/dev/null && [[ ! -x "$HOME/.cargo/bin/cargo" ]]; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
rustup toolchain install stable --profile minimal
rustc --version

# io_uring on (some distributions disable it by default).
if [[ -e /proc/sys/kernel/io_uring_disabled ]]; then
    sysctl -q kernel.io_uring_disabled=0
fi
# Fixed CPU frequency policy, so runs compare: the performance governor on every CPU.
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -e "$g" ]] && echo performance > "$g"
done
# Registered buffers (io_uring) count against RLIMIT_MEMLOCK: no limit for root's sessions.
cat > /etc/security/limits.d/90-pigeonhole-gate.conf <<'EOF'
root soft memlock unlimited
root hard memlock unlimited
EOF
echo "setup done. Log in again (the memlock limit applies to new sessions), then follow README.md."
