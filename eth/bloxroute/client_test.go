package bloxroute

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// sampleTx builds a real, signed BSC transaction and its raw (RLP/typed) bytes.
func sampleTx(t *testing.T) (*types.Transaction, []byte) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(56)), &types.LegacyTx{
		Nonce:    7,
		GasPrice: big.NewInt(3_000_000_000),
		Gas:      21000,
		To:       &to,
		Value:    big.NewInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return tx, raw
}

func TestDecodeRaw(t *testing.T) {
	_, raw := sampleTx(t)
	if b, err := decodeRaw(hexutil.Encode(raw)); err != nil || !bytes.Equal(b, raw) {
		t.Fatalf("hex decode failed: %v", err)
	}
	if b, err := decodeRaw(base64.StdEncoding.EncodeToString(raw)); err != nil || !bytes.Equal(b, raw) {
		t.Fatalf("base64 decode failed: %v", err)
	}
}

// TestHandleCloudAPI feeds a Cloud-API notification (rawTx hex) and checks the
// tx is decoded, counted, and enqueued for injection with the right hash.
func TestHandleCloudAPI(t *testing.T) {
	tx, raw := sampleTx(t)
	c := New(Config{}, nil) // inject unused: handle() only enqueues onto txCh
	msg, _ := json.Marshal(map[string]any{
		"params": map[string]any{
			"result": map[string]any{"txHash": tx.Hash().Hex(), "rawTx": hexutil.Encode(raw)},
		},
	})
	c.handle(msg)
	select {
	case got := <-c.txCh:
		if got.Hash() != tx.Hash() {
			t.Fatalf("hash mismatch: got %s want %s", got.Hash(), tx.Hash())
		}
	default:
		t.Fatal("cloud-api: no tx enqueued")
	}
	if c.received.Load() != 1 {
		t.Fatalf("received=%d want 1", c.received.Load())
	}
}

// TestHandleGatewayBase64 covers the Gateway-API raw_tx (base64) shape.
func TestHandleGatewayBase64(t *testing.T) {
	tx, raw := sampleTx(t)
	c := New(Config{}, nil)
	msg, _ := json.Marshal(map[string]any{
		"params": map[string]any{
			"result": map[string]any{"raw_tx": base64.StdEncoding.EncodeToString(raw)},
		},
	})
	c.handle(msg)
	select {
	case got := <-c.txCh:
		if got.Hash() != tx.Hash() {
			t.Fatalf("hash mismatch: got %s want %s", got.Hash(), tx.Hash())
		}
	default:
		t.Fatal("gateway: no tx enqueued")
	}
}

// TestHandleIgnoresAck ensures a subscription-confirmation (no raw tx) is a no-op.
func TestHandleIgnoresAck(t *testing.T) {
	c := New(Config{}, nil)
	c.handle([]byte(`{"id":1,"result":"0f2c-subid"}`))
	select {
	case <-c.txCh:
		t.Fatal("subscription ack should not enqueue a tx")
	default:
	}
	if c.received.Load() != 0 {
		t.Fatalf("received=%d want 0", c.received.Load())
	}
}
