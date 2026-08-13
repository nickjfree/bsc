// contractscan reads a (snapshotted) BSC chaindata directory and ranks
// contract addresses by access frequency over a block range.
//
// "Access" = appears as either:
//   - the `to` field of a top-level transaction, or
//   - the emitter address of any log in a receipt.
//
// Internal CALL/DELEGATECALL targets that emit no log are NOT visible in
// on-disk data and will not be counted.
//
// Run against a HARD-LINK SNAPSHOT, not the live chaindata. See snapshot.sh.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
)

func main() {
	var (
		chaindata   = flag.String("chaindata", "", "path to the snapshotted geth/chaindata directory (required)")
		ancient     = flag.String("ancient", "", "path to the ancient store (default: <chaindata>/ancient)")
		fromBlock   = flag.Uint64("from", 0, "first block number to scan (inclusive)")
		toBlock     = flag.Uint64("to", 0, "last block number to scan (inclusive); 0 = head")
		topN        = flag.Int("top", 100, "number of top addresses to print")
		countLogs   = flag.Bool("logs", true, "count log emitter addresses")
		countTxTo   = flag.Bool("tx-to", true, "count top-level tx.To() addresses")
		progressEvery = flag.Uint64("progress", 10000, "log progress every N blocks (0 = silent)")
	)
	flag.Parse()

	if *chaindata == "" {
		fmt.Fprintln(os.Stderr, "error: --chaindata is required")
		flag.Usage()
		os.Exit(2)
	}
	if !*countLogs && !*countTxTo {
		fmt.Fprintln(os.Stderr, "error: nothing to count (both --logs and --tx-to are false)")
		os.Exit(2)
	}
	if *ancient == "" {
		*ancient = filepath.Join(*chaindata, "ancient")
	}

	db, err := openReadOnly(*chaindata, *ancient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	head, err := resolveHead(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve head: %v\n", err)
		os.Exit(1)
	}
	to := *toBlock
	if to == 0 || to > head {
		to = head
	}
	from := *fromBlock
	if from > to {
		fmt.Fprintf(os.Stderr, "error: --from (%d) > effective --to (%d)\n", from, to)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "scanning blocks [%d, %d]  (head=%d)  logs=%v tx-to=%v\n",
		from, to, head, *countLogs, *countTxTo)

	counts := make(map[common.Address]uint64, 1<<20)
	start := time.Now()
	var (
		blocksScanned uint64
		txCount       uint64
		logCount      uint64
		emptyBlocks   uint64
	)

	for n := from; n <= to; n++ {
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			emptyBlocks++
			continue
		}

		if *countTxTo {
			body := rawdb.ReadBody(db, hash, n)
			if body != nil {
				for _, tx := range body.Transactions {
					if to := tx.To(); to != nil {
						counts[*to]++
						txCount++
					}
				}
			}
		}

		if *countLogs {
			receipts := rawdb.ReadRawReceipts(db, hash, n)
			for _, r := range receipts {
				for _, lg := range r.Logs {
					counts[lg.Address]++
					logCount++
				}
			}
		}

		blocksScanned++
		if *progressEvery > 0 && blocksScanned%*progressEvery == 0 {
			elapsed := time.Since(start)
			rate := float64(blocksScanned) / elapsed.Seconds()
			remaining := time.Duration(float64(to-n) / rate * float64(time.Second))
			fmt.Fprintf(os.Stderr, "  block=%d  scanned=%d  rate=%.0f blk/s  eta=%s  uniq=%d\n",
				n, blocksScanned, rate, remaining.Truncate(time.Second), len(counts))
		}
	}

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "done in %s. blocks=%d empty=%d txs=%d logs=%d unique=%d\n",
		elapsed.Truncate(time.Millisecond), blocksScanned, emptyBlocks, txCount, logCount, len(counts))

	type kv struct {
		addr  common.Address
		count uint64
	}
	ranked := make([]kv, 0, len(counts))
	for a, c := range counts {
		ranked = append(ranked, kv{a, c})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].count > ranked[j].count })

	limit := *topN
	if limit <= 0 || limit > len(ranked) {
		limit = len(ranked)
	}
	fmt.Println("rank\taddress\tcount")
	for i := 0; i < limit; i++ {
		fmt.Printf("%d\t%s\t%d\n", i+1, ranked[i].addr.Hex(), ranked[i].count)
	}
}

func openReadOnly(chaindata, ancient string) (ethdb.Database, error) {
	const (
		cacheMB = 512
		handles = 512
	)
	kv, err := pebble.New(chaindata, cacheMB, handles, "", true /*readonly*/)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	db, err := rawdb.Open(kv, rawdb.OpenOptions{
		Ancient:  ancient,
		ReadOnly: true,
	})
	if err != nil {
		kv.Close()
		return nil, fmt.Errorf("open chain db: %w", err)
	}
	return db, nil
}

func resolveHead(db ethdb.Database) (uint64, error) {
	hash := rawdb.ReadHeadHeaderHash(db)
	if hash == (common.Hash{}) {
		return 0, fmt.Errorf("no head header hash in db")
	}
	num, ok := rawdb.ReadHeaderNumber(db, hash)
	if !ok {
		return 0, fmt.Errorf("head header number missing for %s", hash.Hex())
	}
	return num, nil
}
