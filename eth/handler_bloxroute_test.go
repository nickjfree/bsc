// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.

package eth

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

// TestIsLocalOnly verifies that only bloXroute-ingested transactions are treated
// as local-only (and therefore skipped by BroadcastTransactions /
// ReannounceTransactions), and that tagging the source never alters the tx hash.
func TestIsLocalOnly(t *testing.T) {
	mk := func() *types.Transaction {
		return types.NewTx(&types.LegacyTx{Nonce: 1, GasPrice: big.NewInt(1), Gas: 21000})
	}

	// Untagged (e.g. our own / RPC-submitted) -> broadcastable.
	if isLocalOnly(mk()) {
		t.Fatal("untagged tx should not be local-only")
	}

	// Tagged with a real p2p peer id -> broadcastable (normal gossip relay).
	p := mk()
	p.SetPeer("enode://deadbeef")
	if isLocalOnly(p) {
		t.Fatal("p2p-peer tx should not be local-only")
	}

	// Tagged as bloXroute-sourced -> local-only (must not leak to peers).
	b := mk()
	b.SetPeer(types.PeerBloXroute)
	if !isLocalOnly(b) {
		t.Fatal("bloXroute-tagged tx must be local-only")
	}

	// Tagging is runtime-only metadata: it must not change the tx hash, or it
	// would break dedup/validation and corrupt propagation.
	h := mk()
	before := h.Hash()
	h.SetPeer(types.PeerBloXroute)
	if h.Hash() != before {
		t.Fatalf("SetPeer changed tx hash: %s -> %s", before, h.Hash())
	}
}
