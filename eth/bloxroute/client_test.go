package bloxroute

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gorilla/websocket"
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

// TestGrowBackoff checks the reconnect backoff doubles and caps at maxBackoff.
func TestGrowBackoff(t *testing.T) {
	for _, tc := range []struct{ in, want time.Duration }{
		{time.Second, 2 * time.Second},
		{2 * time.Second, 4 * time.Second},
		{16 * time.Second, maxBackoff}, // 32s -> capped to 30s
		{maxBackoff, maxBackoff},       // already at ceiling
		{40 * time.Second, maxBackoff}, // above ceiling
	} {
		if got := growBackoff(tc.in); got != tc.want {
			t.Errorf("growBackoff(%v)=%v want %v", tc.in, got, tc.want)
		}
	}
}

// wsTestServer upgrades each incoming connection and hands it to handler.
// It returns a ws:// URL and a cleanup func.
func wsTestServer(t *testing.T, handler func(*websocket.Conn)) (string, func()) {
	t.Helper()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}))
	return "ws" + strings.TrimPrefix(srv.URL, "http"), srv.Close
}

// TestReadDeadlineFiresOnSilentServer is the core reliability test: a server that
// accepts the subscribe then holds the socket open while sending nothing (a
// half-open / stalled feed) must cause run() to return promptly via the read
// deadline, rather than blocking forever.
func TestReadDeadlineFiresOnSilentServer(t *testing.T) {
	url, stop := wsTestServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // consume the subscribe
		time.Sleep(3 * time.Second)  // then go silent, holding the connection open
	})
	defer stop()

	c := New(Config{Endpoint: url, Auth: "x", Network: "test"}, func([]*types.Transaction) {})
	c.readTimeout = 150 * time.Millisecond

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- c.run() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected run() to error when the feed is silent")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("run() returned after %v; read deadline did not trip promptly", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() never returned; read deadline did not trip")
	}
}

// TestReadDeadlineResetsWhileStreaming verifies the deadline resets on every
// message, so an actively streaming feed is NOT torn down prematurely; run()
// returns only when the server actually closes.
func TestReadDeadlineResetsWhileStreaming(t *testing.T) {
	url, stop := wsTestServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // consume the subscribe
		for i := 0; i < 10; i++ {    // stream for ~400ms, interval < readTimeout
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{}`)); err != nil {
				return
			}
			time.Sleep(40 * time.Millisecond)
		}
		// then the handler returns -> conn closes -> run() sees a close error
	})
	defer stop()

	c := New(Config{Endpoint: url, Auth: "x", Network: "test"}, func([]*types.Transaction) {})
	c.readTimeout = 150 * time.Millisecond // comfortably above the 40ms send interval

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- c.run() }()
	select {
	case <-done:
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Fatalf("run() returned after %v; deadline tripped despite an active stream", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run() blocked far too long")
	}
}
