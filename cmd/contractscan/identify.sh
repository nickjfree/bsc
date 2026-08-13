#!/usr/bin/env bash
# Fingerprint a list of contract addresses by eth_getCode + selector grep.
# Usage:  ./identify.sh < addresses.txt
# Or:     echo "0x..." | ./identify.sh
# Reads RPC URL from $RPC (default http://127.0.0.1:8545).

set -euo pipefail
RPC="${RPC:-http://127.0.0.1:8545}"

# Function selector → human label. Order matters: more-specific first.
SELECTORS=(
  # PancakeV2 pair
  "022c0d9f|PancakeV2Pair.swap"
  "6a627842|PancakeV2Pair.mint"
  "89afcb44|PancakeV2Pair.burn"
  # PancakeV3 pool
  "128acb08|PancakeV3Pool.swap"
  "3c8a7d8d|PancakeV3Pool.mint"
  "a34123a7|PancakeV3Pool.burn"
  # Routers
  "38ed1739|RouterV2.swapExactTokensForTokens"
  "18cbafe5|RouterV2.swapExactTokensForETH"
  "5ae401dc|RouterV3.multicall"
  # Multicall aggregators
  "252dba42|Multicall.aggregate"
  "174dea71|Multicall3.aggregate3"
  "82ad56cb|Multicall3.aggregate3Value"
  # WBNB/WETH deposit
  "d0e30db0|WBNB.deposit"
  "2e1a7d4d|WBNB.withdraw"
  # ERC721
  "6352211e|ERC721.ownerOf"
  # ERC20 (least specific, last)
  "a9059cbb|ERC20.transfer"
  "23b872dd|ERC20.transferFrom"
  "095ea7b3|ERC20.approve"
)

identify_one() {
  local addr="$1"
  local code
  code=$(curl -s --max-time 10 "$RPC" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getCode\",\"params\":[\"$addr\",\"latest\"]}" \
    | sed -E 's/.*"result":"0x([0-9a-fA-F]*)".*/\1/')

  if [[ -z "$code" || "$code" == "null" || "$code" == "$addr"* ]]; then
    printf "%s\tEOA-or-empty\t0\n" "$addr"
    return
  fi

  local len=${#code}      # hex chars; bytes = len/2
  local bytes=$((len / 2))

  # Heuristic: very short → proxy / minimal
  local kind=""
  if (( bytes < 200 )); then
    kind="tiny-proxy"
  fi

  # Match selectors
  local hits=()
  for entry in "${SELECTORS[@]}"; do
    sel="${entry%%|*}"
    label="${entry#*|}"
    if [[ "$code" == *"$sel"* ]]; then
      hits+=("$label")
    fi
  done

  # Roll up ERC20 (transfer + transferFrom + approve all present) into one tag
  local erc20=0
  for h in "${hits[@]}"; do
    case "$h" in
      ERC20.transfer|ERC20.transferFrom|ERC20.approve) erc20=$((erc20+1)) ;;
    esac
  done
  local non_erc20=()
  for h in "${hits[@]}"; do
    case "$h" in
      ERC20.*) ;;
      *) non_erc20+=("$h") ;;
    esac
  done

  local tags=()
  [[ -n "$kind" ]] && tags+=("$kind")
  if (( erc20 >= 2 )); then
    tags+=("ERC20-like")
  fi
  for h in "${non_erc20[@]}"; do tags+=("$h"); done
  if [[ ${#tags[@]} -eq 0 ]]; then
    tags+=("unknown")
  fi

  IFS=, ; printf "%s\t%s\t%d\n" "$addr" "${tags[*]}" "$bytes"
  IFS=$' \t\n'
}

while read -r addr; do
  addr=$(echo "$addr" | awk '{print $1}')   # accept "0xADDR\t..." or just "0xADDR"
  [[ -z "$addr" || "$addr" != 0x* ]] && continue
  identify_one "$addr"
done
