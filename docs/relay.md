# P2P Transaction Relay Mode (Design)

Date: 2026-01-25

## Requirements (must-have)
- Relay **only P2P-received** transactions to a target RPC node.
- **Never** relay RPC-submitted transactions.
- Perform **basic sanity checks only** (no state-dependent validation).
- **Do not validate blocks**; disable chain sync/validation paths.
- **Reject all blob transactions**.
- Keep **deduplication + announcement retrieval** via `txFetcher`.

## Non-goals (explicitly out of scope)
- Acting as a full node or miner.
- Maintaining a local txpool for execution.
- Providing reliable transaction inclusion guarantees.
- Supporting blob transactions or blob sidecars.

## Scope
### In-scope
- Inbound P2P `Transactions` and `PooledTransactions` messages.
- Hash-announcement flow (`NewPooledTransactionHashes` → `GetPooledTransactions` → `PooledTransactions`).
- Basic sanity checks (non-nil tx, size limit, signature sanity, intrinsic gas).
- Forwarding to target node via JSON-RPC `eth_sendRawTransaction`.

### Out-of-scope
- RPC transaction submission relay.
- Block processing, chain sync, block validation.
- Local mining, txpool retention.
- Blob transactions.

## Architecture (relay mode)
**Keep:**
- P2P networking (eth protocol) to receive txs.
- `txFetcher` for dedupe + hash-based retrieval.

**Disable/Bypass:**
- Downloader, block fetcher, chain sync, block validation.
- Txpool persistence/processing for execution.

## Data flow (relay mode)
1) Peer announces hashes (`NewPooledTransactionHashes`).
2) `txFetcher.Notify` records announcements.
3) `txFetcher` schedules `GetPooledTransactions` via `peer.RequestTxs`.
4) Peer replies with `PooledTransactions`.
5) `txFetcher.Enqueue` delivers full txs.
6) Relay layer runs sanity checks, dedupes, and forwards via RPC.

## Why keep `txFetcher` in relay mode
The `txFetcher` is still useful even in relay-only mode because it provides:
- **Deduplication**: tracks seen hashes and avoids repeated relays.
- **Announcement-based retrieval**: requests full txs when only hashes are announced.
- **Backpressure/limits**: caps requests (hash count/size) and handles timeouts.

### Dedup cache choice
Use the built-in concurrent LRU cache in [common/lru/lru.go](common/lru/lru.go). Suggested type:
`lru.Cache[common.Hash, struct{}]` with a fixed capacity (e.g., 100k–500k).

## Basic sanity checks (no state)
- tx is non-nil
- size < max (reuse txpool max size)
- signature valid (EIP-155)
- intrinsic gas check
- gas fields sanity (tip <= fee cap)
- **reject blob txs** unconditionally

## Configuration
### New config fields (ethconfig)
- `RelayMode bool`
- `RelayTarget string` (RPC URL)

### CLI flags (geth)
- `--relay.mode` (bool)
- `--relay.target` (RPC URL)

## Implementation plan (LLM-friendly)
### 1) Add config fields
- Update `eth/ethconfig/config.go` and `eth/ethconfig/gen_config.go`.
- Ensure defaults: `RelayMode=false`, `RelayTarget=""`.

### 2) Add relay component
- New package `eth/relay`.
- Provide `RelayRawTx(ctx, tx)` using RPC `eth_sendRawTransaction`.
- Add buffered queue + worker (optional) to avoid blocking P2P handlers.

### 3) Wire relay into handler
- In `eth/handler.go`, when `RelayMode`:
   - Initialize relay client.
   - Initialize dedupe cache (`lru.Cache`).
   - Wire `txFetcher` with:
      - `hasTx`: cache lookup
      - `addTxs`: relay path (not txpool)
      - `fetchTxs`: same as current (`peer.RequestTxs`)
   - Skip downloader/block fetcher/chain sync startup.

### 4) Adjust P2P tx handling
- In `eth/handler_eth.go`:
   - For `TransactionsPacket` / `PooledTransactionsResponse`:
      - reject blob txs
      - forward to relay pipeline (not txpool)

### 5) Disable non-relay features
- In relay mode, do not start:
   - chain syncer
   - block fetcher
   - mined block/vote broadcasts
   - txpool broadcasts

### 6) Logging/metrics
- Log: relay target, relay errors, drops due to sanity checks.
- Optional counters for: received, relayed, deduped, rejected.

## Files to touch
- `eth/handler_eth.go`: route P2P txs to relayer, keep `txFetcher`.
- `eth/fetcher/tx_fetcher.go`: wire `addTxs` to relay path in relay mode.
- `eth/handler.go`: initialize relay + txFetcher in relay mode.
- `eth/ethconfig/config.go` + `eth/ethconfig/gen_config.go`: relay config fields.
- `cmd/geth/*`: CLI flags for relay mode/target.
- New: `eth/relay/relay.go` (RPC client + send raw tx).

## Testing checklist
- P2P txs are relayed to target RPC node.
- RPC `eth_sendRawTransaction` on local node is **not** relayed.
- Blob txs are rejected.
- Duplicate txs are not relayed twice.
- Node does not sync/validate blocks in relay mode.

## Acceptance
- Only P2P-received txs are relayed.
- RPC `eth_sendRawTransaction` is unaffected.
- Blob txs are rejected.
- Node does not attempt to sync/validate blocks in relay mode.
- `txFetcher` remains active to dedupe and request full txs after hash announces.

## Scope
### In-scope
- Inbound P2P `Transactions` and `PooledTransactions` messages.
- Hash-announcement flow (`NewPooledTransactionHashes` → `GetPooledTransactions` → `PooledTransactions`).
- Basic sanity checks (non-nil tx, size limit, signature sanity, intrinsic gas).
- Forwarding to target node via JSON-RPC `eth_sendRawTransaction`.

### Out-of-scope
- RPC transaction submission relay.
- Block processing, chain sync, block validation.
- Local mining, txpool retention.
- Blob transactions.

## Why keep `txFetcher` in relay mode
The `txFetcher` is still useful even in relay-only mode because it provides:
- **Deduplication**: tracks seen hashes and avoids repeated relays.
- **Announcement-based retrieval**: requests full txs when only hashes are announced.
- **Backpressure/limits**: caps requests (hash count/size) and handles timeouts.

### Dedup cache choice
Use the built-in concurrent LRU cache in [common/lru/lru.go](common/lru/lru.go). Suggested type:
`lru.Cache[common.Hash, struct{}]` with a fixed capacity (e.g., 100k–500k).

### Relevant flow (current code)
- `NewPooledTransactionHashes` → `txFetcher.Notify(...)`
- `txFetcher` schedules `GetPooledTransactions` via `peer.RequestTxs(...)`
- `PooledTransactions` arrives → `txFetcher.Enqueue(...)`
- In relay mode, `txFetcher.Enqueue` should call **relay** instead of adding to txpool

## High-level design
1. **Relay configuration**
   - New config fields in `eth/ethconfig`:
     - `RelayMode bool`
     - `RelayTarget string` (RPC URL)
   - CLI flags in `cmd/geth` to set these fields.

2. **Relay component**
   - New package: `eth/relay` (or similar).
   - Holds an RPC client to target node.
   - Provides:
     - `RelayRawTx(ctx, tx *types.Transaction) error`
     - `Close()`
   - Optionally uses a buffered channel + worker to avoid blocking P2P handlers.

3. **P2P tx handling changes**
   - Keep `txFetcher` for announce/retrieve + dedupe.
   - In `eth/handler_eth.go`, for `TransactionsPacket` and `PooledTransactionsResponse`:
     - Reject blob txs.
     - Run basic sanity checks.
     - Relay to target RPC.
     - Do **not** enqueue into txpool.

4. **Disable non-relay functions**
   - Do not start downloader/block fetcher/chain validation in relay mode.
   - Ignore block announce/broadcast handlers.
   - Keep minimal networking to receive P2P txs.

## Basic sanity checks
Use a lightweight validator (no state):
- tx size < configured limit
- EIP-155 signature valid
- intrinsic gas check
- gas fields sanity (tip <= fee cap)
- reject blob txs entirely

## Files to touch
- `eth/handler_eth.go`: route P2P txs to relayer, keep `txFetcher`.
- `eth/fetcher/tx_fetcher.go`: wire `addTxs` to relay path when in relay mode.
- `eth/handler.go`: initialize relay + txFetcher in relay mode.
- `eth/ethconfig/config.go` + `eth/ethconfig/gen_config.go`: relay config fields.
- `cmd/geth/*`: CLI flags for relay mode/target.
- New: `eth/relay/relay.go` (RPC client + send raw tx).

## Acceptance
- Only P2P-received txs are relayed.
- RPC `eth_sendRawTransaction` is unaffected.
- Blob txs are rejected.
- Node does not attempt to sync/validate blocks in relay mode.
- `txFetcher` remains active to dedupe and request full txs after hash announces.
