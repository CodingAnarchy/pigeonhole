#!/usr/bin/env bash
# Installs valgrind for the callgrind jobs (.github/workflows/instructions.yml).
#
#   install-valgrind.sh apt [KEEP]   apt, bounded and retried; with KEEP, the .debs apt
#                                    downloaded are copied to KEEP with a SHA256SUMS manifest,
#                                    for actions/cache to save.
#   install-valgrind.sh cached DIR   the .debs in DIR (restored by actions/cache) through dpkg,
#                                    with no network; apt as above if there are none or they
#                                    don't install.
#
# The cached set is exactly what apt installed on that image (valgrind and its Recommends,
# libc6-dbg among them, which changes valgrind's view of libc's symbols), so the counts don't
# depend on where valgrind came from. In `cached` mode, DEBS_KEY (the exact key asked for) and
# DEBS_MATCHED (the key actions/cache restored, empty on a miss) name the source in a notice.
set -euo pipefail

# Bounded: a runner whose DNS or mirror stalls (#1735's two jobs, "Temporary failure
# resolving") gets three tries, then fails rather than hold the job. `--error-on=any` makes an
# index that can't be fetched fail `update` (it exits 0 otherwise, and `install` runs on the
# image's stale lists into the step's timeout; cli692 on #1736); the loop retries, so apt's own
# retries are 0. Name resolution isn't bounded by apt's timeouts (about 20 s a lookup), so a
# failing try costs a minute or two. When the image's first mirror, azure.archive.ubuntu.com,
# doesn't resolve, apt is pointed at archive.ubuntu.com first. That helps only while the
# runner's resolver still answers for archive.ubuntu.com: on 2026-11-11 it often didn't
# (#1736's runs), which is what the .deb cache is for.
apt_install() {
  local keep=$1 fix=$2
  local apt_opts=(-o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 -o Acquire::Retries=0)
  local install_opts=()
  local archives=/tmp/valgrind-apt-archives
  if [ -n "$keep" ]; then
    # A fresh archive directory, so every package apt fetches lands here, kept after install.
    sudo mkdir -p "$archives/partial"
    install_opts=(-o "Dir::Cache::archives=$archives" -o APT::Keep-Downloaded-Packages=true)
  fi
  # After a failed dpkg of cached .debs, apt first repairs what dpkg left half-configured:
  # `install --fix-broken` with no packages (apt refuses it with a package named).
  local repair=(true)
  [ -n "$fix" ] && repair=(sudo apt-get "${apt_opts[@]}" install -y -q --fix-broken)
  if ! getent hosts azure.archive.ubuntu.com > /dev/null; then
    echo "::warning::azure.archive.ubuntu.com doesn't resolve: apt falls back to archive.ubuntu.com"
    [ -f /etc/apt/apt-mirrors.txt ] && sudo sed -i '/azure\.archive\.ubuntu\.com/d' /etc/apt/apt-mirrors.txt
    for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
      [ -f "$f" ] && sudo sed -i 's#azure\.archive\.ubuntu\.com#archive.ubuntu.com#g' "$f"
    done
  fi
  local ok=
  for try in 1 2 3; do
    if sudo apt-get "${apt_opts[@]}" update -q --error-on=any \
      && "${repair[@]}" \
      && sudo apt-get "${apt_opts[@]}" install -y -q "${install_opts[@]}" valgrind; then
      ok=1
      break
    fi
    echo "::warning::apt install of valgrind failed (try $try of 3)"
    [ "$try" -lt 3 ] && sleep 20
  done
  [ -n "$ok" ] || exit 1
  valgrind --version
  echo "::notice::valgrind from apt"
  if [ -n "$keep" ]; then
    mkdir -p "$keep"
    cp "$archives"/*.deb "$keep"/
    (cd "$keep" && sha256sum -- *.deb > SHA256SUMS)
    echo "::notice::kept $(wc -l < "$keep/SHA256SUMS") .debs ($(du -sh "$keep" | cut -f1)) for the cache"
  fi
}

# The manifest lists the files to install, so a stray file in DIR is never installed, and a
# corrupt or partial entry fails the check and falls back to apt. It is saved with the .debs,
# so it catches corruption, not a hostile writer: only workflows on main write the cache that
# other refs read. dpkg refuses to downgrade a package the image already has newer (an older
# image's entry). It skips that package and still exits 0, so every listed package is then
# checked at its cached version: a refusal, or any other difference, falls back to apt.
installed_as_cached() {
  local dir=$1 deb name want have
  shift
  for deb in "$@"; do
    name=$(dpkg-deb -f "$dir/$deb" Package):$(dpkg-deb -f "$dir/$deb" Architecture)
    want=$(dpkg-deb -f "$dir/$deb" Version)
    have=$(dpkg-query -W -f '${Version} ${Status}' "$name" 2>/dev/null || true)
    if [ "$have" != "$want install ok installed" ]; then
      echo "::warning::$name is ${have:-not installed}, not the cached $want"
      return 1
    fi
  done
}

cached_install() {
  local dir=$1
  if [ ! -s "$dir/SHA256SUMS" ]; then
    echo "::notice::no cached .debs for this image: valgrind from apt"
    apt_install "" ""
    return
  fi
  local source="exact key ${DEBS_KEY:-}"
  [ "${DEBS_MATCHED:-}" != "${DEBS_KEY:-}" ] && source="an older image's entry, ${DEBS_MATCHED:-}"
  local debs=()
  mapfile -t debs < <(awk '{ print $2 }' "$dir/SHA256SUMS")
  if (cd "$dir" && sha256sum --quiet --strict -c SHA256SUMS) \
    && (cd "$dir" && sudo dpkg -i --refuse-downgrade -- "${debs[@]}") \
    && installed_as_cached "$dir" "${debs[@]}" \
    && valgrind --version; then
    echo "::notice::valgrind from the .deb cache ($source, ${#debs[@]} packages)"
    return
  fi
  echo "::warning::the cached .debs ($source) didn't install: valgrind from apt"
  apt_install "" fix
}

case "${1:-}" in
  apt) apt_install "${2:-}" "" ;;
  cached) cached_install "${2:?cached needs the .deb directory}" ;;
  *) echo "usage: $0 apt [KEEP] | cached DIR" >&2; exit 2 ;;
esac
