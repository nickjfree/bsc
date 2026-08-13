// Command createscan enumerates every TOP-LEVEL contract creation in a block
// range, reading a (snapshotted) BSC chaindata directory offline — no state,
// no live node. For each tx with To()==nil it computes the CREATE address
// (keccak(rlp(sender,nonce))). This surfaces contracts that never appear in the
// swap-active set (vaults, presales, bridges, lotteries, staking, …).
//
// CREATE2 / factory-deployed clones are NOT covered (they need tracing/state).
//
//	createscan -chaindata /path/fast/geth/chaindata -from A -to B -out created.csv
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
)

func main() {
	var (
		chaindata = flag.String("chaindata", "", "path to snapshotted geth/chaindata (required)")
		ancient   = flag.String("ancient", "", "ancient store (default <chaindata>/ancient)")
		fromBlock = flag.Uint64("from", 0, "first block (0 = tail)")
		toBlock   = flag.Uint64("to", 0, "last block (0 = last frozen)")
		out       = flag.String("out", "created.csv", "output CSV: address,block,creator,txhash")
		progress  = flag.Uint64("progress", 50000, "log every N blocks")
	)
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelWarn, false)))
	if *chaindata == "" {
		fmt.Fprintln(os.Stderr, "error: -chaindata required")
		os.Exit(2)
	}
	if *ancient == "" {
		*ancient = filepath.Join(*chaindata, "ancient")
	}
	kv, err := pebble.New(*chaindata, 512, 512, "", true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pebble: %v\n", err)
		os.Exit(1)
	}
	db, err := rawdb.Open(kv, rawdb.OpenOptions{Ancient: *ancient, ReadOnly: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	frozen, _ := db.Ancients()
	tail, _ := db.Tail()
	to := *toBlock
	if to == 0 && frozen > 0 {
		to = frozen - 1
	}
	from := *fromBlock
	if from < tail {
		from = tail
	}
	fmt.Fprintf(os.Stderr, "scanning creations in blocks [%d,%d] (tail=%d frozen=%d)\n", from, to, tail, frozen)

	signer := types.LatestSignerForChainID(big.NewInt(56))
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create out: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "address,block,creator,txhash")

	start := time.Now()
	var blocks, creations uint64
	for n := from; n <= to; n++ {
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			continue
		}
		body := rawdb.ReadBody(db, hash, n)
		if body == nil {
			continue
		}
		for _, tx := range body.Transactions {
			if tx.To() != nil {
				continue
			}
			sender, err := types.Sender(signer, tx)
			if err != nil {
				continue
			}
			addr := crypto.CreateAddress(sender, tx.Nonce())
			fmt.Fprintf(w, "%s,%d,%s,%s\n", addr.Hex(), n, sender.Hex(), tx.Hash().Hex())
			creations++
		}
		blocks++
		if *progress > 0 && blocks%*progress == 0 {
			el := time.Since(start)
			fmt.Fprintf(os.Stderr, "  block=%d scanned=%d creations=%d rate=%.0f blk/s\n",
				n, blocks, creations, float64(blocks)/el.Seconds())
		}
	}
	fmt.Fprintf(os.Stderr, "done in %s: blocks=%d top-level-creations=%d -> %s\n",
		time.Since(start).Truncate(time.Millisecond), blocks, creations, *out)
	_ = ethdb.Database(db)
}
