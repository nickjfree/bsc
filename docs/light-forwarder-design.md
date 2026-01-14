# Lightweight BSC forwarder node

## Goal

Operate a minimal BSC node that only:

1. Joins the p2p network to receive transaction and block broadcasts.
2. Forwards the received data to a central server.
3. Disables all other functionality (execution, consensus, APIs, mining, etc.).

The node should be resource-light (CPU, memory, storage) while preserving the ability to hear the network gossip as fast as a regular peer.

## High-level architecture

```
        BSC p2p network
               │
               │ eth/66 gossip (NewPooledTransactionHashes, NewBlock, NewBlockHashes)
               ▼
      LightForwarder node (p2p-only)
        ├─ P2P stack (handshake, peer mgmt, rate limiting)
        ├─ Gossip interceptors:
        │    • Transactions: decode tx payloads / hashes
        │    • Blocks: decode block headers + bodies
        │    • Optional header-only validation (hash, total difficulty / accumulator sanity)
        ├─ Forwarding pipeline:
        │    • In-memory queue + dedupe (hash set)
        │    • Batching + backpressure
        │    • Resilient sender (HTTP/gRPC/TCP) to central endpoint
        └─ Metrics + minimal logging
               │
               ▼
        Central server (collector/ingestor)
```

## Components and responsibilities

- **Process entry / mode switch**: add a `--light-forwarder` mode (or config file toggle) that constructs a node without ETH execution services. Boot only the p2p stack and the custom forwarder service.
- **P2P stack**: reuse `p2p` and `eth/protocols/eth` for handshake and gossip subscription. Keep discovery and peer management; cap peers to a small number (e.g., 10–20) to remain light.
- **Gossip interceptors**:
  - Hook into the ETH protocol handler (see `eth/handler.go`) to capture `NewPooledTransactionHashes`, `Transactions`, `NewBlock`, and `BlockBodies` messages.
  - Do not insert into txpool or chain; instead, parse and push into the forwarding pipeline.
  - Optionally perform cheap validation (RLP decode, hash check, header parent match against last-known hash) to drop malformed inputs before sending upstream.
- **Forwarding pipeline**:
  - Normalize payloads: transaction hash + raw tx bytes; block hash + header + body.
  - Dedupe by hash (bounded LRU). Batch by count/bytes or flush interval.
  - Send to a configurable central endpoint (e.g., HTTPS/gRPC). Include node ID and peer source metadata for observability.
  - Apply backpressure: if sender is slow, drop oldest batches or temporarily stop reading from peers.
  - Add lightweight retry with exponential backoff; metrics on success/failure.
- **Disabled subsystems**:
  - No RPC/WS/GraphQL, no miner/validator, no txpool repropagation, no state execution, no snapshot/pruning, no database beyond minimal peer store.
  - Disable consensus engines (Parlia/PoSA) and block import. Skip trie/state writes entirely; do not run beacon sync.
- **Persistence**:
  - Keep only peer keys and discovery database. Optional rolling buffer for last N headers to allow basic parent linkage checks; otherwise, operate stateless.
- **Configuration**:
  - Endpoint URL, auth token, batch size, flush interval, dedupe window, max peers, and whether to perform header-only validation are all config options.
- **Observability & safety**:
  - Minimal logs, Prometheus counters (received/forwarded/dropped), and alerting hooks.
  - Rate-limit inbound messages to avoid abuse; enforce size caps per message and per peer.

## Control flow (transactions)

1. Peer sends `NewPooledTransactionHashes` or `Transactions`.
2. Interceptor decodes, drops duplicates, and enqueues tx payloads.
3. Batcher flushes to central server; on success removes from dedupe cache.
4. On backpressure or failures, apply drop/slowdown policy; never inject into local txpool.

## Control flow (blocks)

1. Peer sends `NewBlock`/`NewBlockHashes`; request body if needed (or rely on push).
2. Interceptor decodes header/body; optionally validate hash/parent reference.
3. Enqueue for forwarding; do not insert into chain or trigger sync.
4. Forward batches to central server; drop on sustained failure.

## Failure and security considerations

- **Isolation**: only expose p2p port; disable all public RPC and miner APIs.
- **Resource guards**: limit peers, message sizes, queue depth, and CPU usage (no trie/state).
- **Data integrity**: include hashes and peer IDs in forwarded payloads; allow server-side revalidation.
- **Resilience**: tolerate central endpoint downtime via bounded retries and drop-oldest strategy.
- **Key management**: use a dedicated node key for p2p; rotateable without affecting forwarding logic.

## Minimal implementation touchpoints in this codebase

- `cmd/geth`: add CLI flag/config for `--light-forwarder`.
- `node` package: define a new service wiring only p2p + custom forwarder (no `eth.Ethereum` or `ethdb` state).
- `eth/handler.go` (and related protocol handlers): intercept inbound tx/block gossip before txpool/chain import.
- `p2p` package: reuse existing peer management; cap peers and rate-limit.
- `internal/ethapi`, `rpc`, `miner`, `consensus`, `txpool`: ensure they are **not** started in forwarder mode.

## Rollout plan

1. Add configuration and mode wiring.
2. Implement forwarder service with interceptors and batching sender.
3. Add minimal metrics and logging.
4. Provide sample deployment config and hardening defaults.
5. Integration test with a devnet: verify tx/block receipt and successful forwarding while other subsystems stay off.
