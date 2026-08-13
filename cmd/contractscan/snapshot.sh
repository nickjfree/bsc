#!/usr/bin/env bash
# Hard-link snapshot of a running BSC node's chaindata.
# Safe to run while the node is live; uses near-zero disk space until
# compactions diverge the snapshot from the live dir.
#
# Usage:
#   ./snapshot.sh <datadir> [snapshot-dir]
#
# Example:
#   ./snapshot.sh /var/lib/bsc /var/lib/bsc/snap-$(date +%s)
#
# The datadir is the one passed to bsc with --datadir; it must contain
# geth/chaindata/. The snapshot must be on the SAME filesystem as the
# source (hard links cannot cross mount points).

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <datadir> [snapshot-dir]" >&2
  exit 2
fi

SRC_ARG="${1%/}"

# Accept either a datadir or the chaindata dir itself.
if [[ -d "$SRC_ARG/geth/chaindata" ]]; then
  SRC_CHAINDATA="$SRC_ARG/geth/chaindata"
elif [[ -d "$SRC_ARG/chaindata" ]]; then
  SRC_CHAINDATA="$SRC_ARG/chaindata"
elif [[ -f "$SRC_ARG/CURRENT" ]]; then
  SRC_CHAINDATA="$SRC_ARG"
else
  echo "error: could not find chaindata under $SRC_ARG" >&2
  echo "  tried: $SRC_ARG/geth/chaindata, $SRC_ARG/chaindata, $SRC_ARG itself" >&2
  exit 1
fi

DST_ROOT="${2:-$(dirname "$SRC_CHAINDATA")/snap-$(date +%Y%m%d-%H%M%S)}"
DST_CHAINDATA="$DST_ROOT/chaindata"

if [[ -e "$DST_ROOT" ]]; then
  echo "error: $DST_ROOT already exists" >&2
  exit 1
fi

src_dev=$(stat -c '%d' "$SRC_CHAINDATA")
mkdir -p "$DST_ROOT"
dst_dev=$(stat -c '%d' "$DST_ROOT")
if [[ "$src_dev" != "$dst_dev" ]]; then
  echo "error: snapshot dir is on a different filesystem from chaindata" >&2
  echo "  src dev=$src_dev  dst dev=$dst_dev" >&2
  echo "  hard links require the same filesystem; pick a path on the same disk" >&2
  rmdir "$DST_ROOT" 2>/dev/null || true
  exit 1
fi

echo "snapshotting $SRC_CHAINDATA -> $DST_CHAINDATA"
cp -al "$SRC_CHAINDATA" "$DST_CHAINDATA"

# Drop the LOCK so a read-only opener can't get confused by the live node's lock.
rm -f "$DST_CHAINDATA/LOCK"

echo "snapshot ready: $DST_CHAINDATA"
echo "remember to: rm -rf '$DST_ROOT'  when done (releases pinned SST inodes)"
