// Package bloxroute ingests bloXroute's newTx stream into the local transaction
// pool. Transactions that bloXroute's global network observes before our own p2p
// layer are decoded and handed to txpool.Add, so they surface early in the node
// (e.g. via the pending-tx / newPendingTransactionWithLogs feeds) instead of
// waiting for gossip to reach us.
//
// Docs: https://docs.bloxroute.com/bsc/streams/newtxs-and-pendingtxs
//
// The stream is a Cloud-API websocket: dial wss://<region>.bsc.blxrbdn.com/ws
// with an Authorization header, then subscribe to the "newTxs" feed requesting
// the raw signed tx bytes. newTxs is pre-validation (lowest latency); the txpool
// re-validates on Add, so invalid/dropped entries are harmless.
package bloxroute

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/gorilla/websocket"
)

// Config configures the bloXroute newTx ingester.
type Config struct {
	Enabled  bool   `toml:",omitempty"` // master switch
	Endpoint string `toml:",omitempty"` // Cloud-API websocket, e.g. wss://virginia.bsc.blxrbdn.com/ws
	Auth     string `toml:",omitempty"` // value of the Authorization header (bloXroute account secret)
	Network  string `toml:",omitempty"` // blockchain_network, e.g. BSC-Mainnet
}

// DefaultConfig is disabled with sensible endpoints filled in.
var DefaultConfig = Config{
	Enabled:  false,
	Endpoint: "wss://virginia.bsc.blxrbdn.com/ws",
	Network:  "BSC-Mainnet",
}

// TxInjector hands decoded transactions to the caller (typically txpool.Add).
type TxInjector func([]*types.Transaction)

const (
	txChanSize   = 8192                  // decoded-tx buffer before injection
	maxBatch     = 128                   // txpool.Add batch size
	batchTimeout = 20 * time.Millisecond // flush cadence
	reportEvery  = 30 * time.Second      // throughput log cadence
	maxBackoff   = 30 * time.Second      // reconnect backoff ceiling

	// defaultReadTimeout bounds how long we wait for the next message before
	// treating the socket as dead. bloXroute's newTxs feed is continuous
	// (hundreds/sec on BSC mainnet), so a multi-second silence means a
	// half-open / stalled connection: the read deadline then trips ReadMessage
	// and forces a reconnect instead of blocking forever.
	defaultReadTimeout = 45 * time.Second

	// healthyReset: a session that stayed connected at least this long is
	// considered healthy, so the reconnect backoff resets to its base (instead
	// of staying stuck at the 30s ceiling after some earlier flapping).
	healthyReset = 60 * time.Second
)

// Client streams newTx notifications from bloXroute and injects them locally.
// It implements node.Lifecycle (Start / Stop).
type Client struct {
	cfg    Config
	inject TxInjector

	readTimeout time.Duration // per-message read deadline; 0 disables (set in New)

	txCh chan *types.Transaction
	quit chan struct{}
	wg   sync.WaitGroup

	received atomic.Uint64 // decoded valid txs
	injected atomic.Uint64 // handed to the txpool
	dropped  atomic.Uint64 // dropped because the buffer was full
	errs     atomic.Uint64 // stream errors
}

// New builds an ingester. inject is called with batches of decoded transactions.
func New(cfg Config, inject TxInjector) *Client {
	if cfg.Endpoint == "" {
		cfg.Endpoint = DefaultConfig.Endpoint
	}
	if cfg.Network == "" {
		cfg.Network = DefaultConfig.Network
	}
	return &Client{
		cfg:         cfg,
		inject:      inject,
		readTimeout: defaultReadTimeout,
		txCh:        make(chan *types.Transaction, txChanSize),
		quit:        make(chan struct{}),
	}
}

// Start launches the ingester. Part of node.Lifecycle.
func (c *Client) Start() error {
	if c.cfg.Auth == "" {
		log.Warn("bloXroute ingester enabled but no auth provided; not connecting (set --bloxroute.auth)")
		return nil
	}
	c.wg.Add(3)
	go c.injectLoop()
	go c.streamLoop()
	go c.reportLoop()
	log.Info("bloXroute newTx ingester started", "endpoint", c.cfg.Endpoint, "network", c.cfg.Network)
	return nil
}

// Stop terminates the ingester and waits for its goroutines. Part of node.Lifecycle.
func (c *Client) Stop() error {
	select {
	case <-c.quit:
	default:
		close(c.quit)
	}
	c.wg.Wait()
	return nil
}

// streamLoop keeps a subscription alive, reconnecting with capped backoff.
func (c *Client) streamLoop() {
	defer c.wg.Done()
	backoff := time.Second
	for {
		select {
		case <-c.quit:
			return
		default:
		}
		started := time.Now()
		if err := c.run(); err != nil {
			c.errs.Add(1)
			log.Warn("bloXroute stream error, reconnecting", "err", err, "in", backoff)
		}
		// A session that stayed up a while was healthy: reset the backoff so
		// the next reconnect is prompt instead of stuck at the 30s ceiling.
		if time.Since(started) >= healthyReset {
			backoff = time.Second
		}
		select {
		case <-c.quit:
			return
		case <-time.After(backoff):
		}
		backoff = growBackoff(backoff)
	}
}

// reportLoop periodically logs ingestion throughput.
func (c *Client) reportLoop() {
	defer c.wg.Done()
	t := time.NewTicker(reportEvery)
	defer t.Stop()
	secs := uint64(reportEvery / time.Second)
	var lastR, lastI uint64
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
			r, i := c.received.Load(), c.injected.Load()
			log.Info("bloXroute newTx ingester", "received", r, "injected", i,
				"dropped", c.dropped.Load(), "errors", c.errs.Load(),
				"rx/s", (r-lastR)/secs, "inj/s", (i-lastI)/secs)
			lastR, lastI = r, i
		}
	}
}

// run establishes one websocket session and reads until it errors or we stop.
func (c *Client) run() error {
	hdr := http.Header{}
	hdr.Set("Authorization", c.cfg.Auth)
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second
	dialer.EnableCompression = true

	conn, resp, err := dialer.Dial(c.cfg.Endpoint, hdr)
	if err != nil {
		if resp != nil {
			return &httpError{err: err, status: resp.Status}
		}
		return err
	}
	defer conn.Close()

	// Close the connection promptly on shutdown so ReadMessage unblocks.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-c.quit:
			conn.Close()
		case <-done:
		}
	}()

	sub := map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "subscribe",
		"params": []any{"newTxs", map[string]any{
			"include":            []string{"raw_tx"},
			"blockchain_network": c.cfg.Network,
		}},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return err
	}
	log.Info("bloXroute subscribed to newTxs", "network", c.cfg.Network)

	for {
		// Bound each read: if the feed goes silent (half-open/stalled socket),
		// the deadline trips ReadMessage so streamLoop can reconnect instead of
		// blocking forever. Reset on every message, so a live feed never trips it.
		if c.readTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
				return err
			}
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		c.handle(data)
	}
}

// growBackoff doubles the reconnect backoff, capped at maxBackoff.
func growBackoff(cur time.Duration) time.Duration {
	if cur >= maxBackoff {
		return maxBackoff
	}
	next := cur * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

// notif is the Cloud-API newTx notification envelope.
type notif struct {
	Params struct {
		Result struct {
			TxHash string `json:"txHash"`
			RawTx  string `json:"rawTx"`  // Cloud-API (hex, 0x-prefixed)
			RawTxS string `json:"raw_tx"` // Gateway-API (base64)
		} `json:"result"`
	} `json:"params"`
	Error json.RawMessage `json:"error"`
}

func (c *Client) handle(data []byte) {
	var m notif
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}
	if len(m.Error) > 0 {
		log.Warn("bloXroute error message", "err", string(m.Error))
		return
	}
	raw := m.Params.Result.RawTx
	if raw == "" {
		raw = m.Params.Result.RawTxS
	}
	if raw == "" {
		return // subscription ack, or a result without raw_tx
	}
	b, err := decodeRaw(raw)
	if err != nil {
		return
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(b); err != nil {
		return
	}
	c.received.Add(1)
	select {
	case c.txCh <- tx:
	default:
		c.dropped.Add(1) // never block the reader; the txpool is behind
	}
}

// injectLoop batches decoded txs and hands them to the injector.
func (c *Client) injectLoop() {
	defer c.wg.Done()
	tick := time.NewTicker(batchTimeout)
	defer tick.Stop()
	buf := make([]*types.Transaction, 0, maxBatch)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		c.inject(buf)
		c.injected.Add(uint64(len(buf)))
		buf = make([]*types.Transaction, 0, maxBatch)
	}
	for {
		select {
		case <-c.quit:
			flush()
			return
		case tx := <-c.txCh:
			buf = append(buf, tx)
			if len(buf) >= maxBatch {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func decodeRaw(s string) ([]byte, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return hexutil.Decode(s)
	}
	return base64.StdEncoding.DecodeString(s)
}

type httpError struct {
	err    error
	status string
}

func (e *httpError) Error() string { return e.err.Error() + " (http " + e.status + ")" }
