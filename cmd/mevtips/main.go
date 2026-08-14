// Command mevtips scans a (snapshotted) BSC path-scheme chaindata directory and
// finds contracts that pay BNB tips to a fixed address via INTERNAL transactions
// — the pattern used by MEV searchers to pay the 48 Club builder/validator
// payment address 0x4848489f0b2BEdd788c696e2D79b6b69D7484848.
//
// Internal value transfers emit no logs and are not present in receipts, so the
// only way to find them offline is to RE-EXECUTE the block's transactions with
// an EVM tracer. That requires the pre-state (parent state root) to be available
// in the trie database. A pruned path-scheme (PBSS) full node only keeps the
// last ~128 blocks of state (128 diff layers + 1 disk layer) unless a state
// history freezer is present, so the traceable window is bounded — use `-mode
// probe` to measure it for a given snapshot.
//
// Run against a HARD-LINK SNAPSHOT, never the live chaindata.
//
// Examples:
//
//	mevtips -mode probe -chaindata /path/fast/geth/chaindata
//	mevtips -mode trace -chaindata /path/fast/geth/chaindata -blocks 20
//	mevtips -mode trace -chaindata /path/fast/geth/chaindata -from 1000 -to 1128 -out hits.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

func main() {
	var (
		chaindata  = flag.String("chaindata", "", "path to the snapshotted geth/chaindata directory (required)")
		ancient    = flag.String("ancient", "", "path to the ancient store (default: <chaindata>/ancient)")
		journaldir = flag.String("journaldir", "", "path to the pathdb journal dir holding merkle.journal (default: <chaindata>/../triedb); empty string disables file journal")
		mode       = flag.String("mode", "trace", "mode: probe | trace")
		targetHex = flag.String("target", "0x4848489f0b2BEdd788c696e2D79b6b69D7484848", "payment address to detect internal transfers to")
		fromBlock = flag.Uint64("from", 0, "first block to trace (inclusive); 0 = derive from -blocks")
		toBlock   = flag.Uint64("to", 0, "last block to trace (inclusive); 0 = head")
		nBlocks   = flag.Uint64("blocks", 20, "when -from is 0, trace this many blocks ending at -to/head")
		minValue  = flag.String("minvalue", "0", "ignore internal transfers below this many wei")
		outFile   = flag.String("out", "", "write full hit list as JSON to this file")
		progress  = flag.Uint64("progress", 1, "log progress every N blocks (0 = silent)")
	)
	flag.Parse()

	if *chaindata == "" {
		fmt.Fprintln(os.Stderr, "error: -chaindata is required")
		flag.Usage()
		os.Exit(2)
	}
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, false)))

	if *ancient == "" {
		*ancient = filepath.Join(*chaindata, "ancient")
	}
	if *journaldir == "default" || (!flagPassed("journaldir")) {
		*journaldir = filepath.Join(filepath.Dir(*chaindata), "triedb")
	}
	if !common.IsHexAddress(*targetHex) {
		fmt.Fprintf(os.Stderr, "error: bad -target address %q\n", *targetHex)
		os.Exit(2)
	}
	target := common.HexToAddress(*targetHex)
	minVal, ok := new(big.Int).SetString(*minValue, 10)
	if !ok {
		fmt.Fprintf(os.Stderr, "error: bad -minvalue %q\n", *minValue)
		os.Exit(2)
	}

	db, err := openReadOnly(*chaindata, *ancient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	pcfg := *pathdb.ReadOnly // copy preset (keeps default cache sizes)
	pcfg.JournalDirectory = *journaldir
	fmt.Fprintf(os.Stderr, "pathdb journal dir: %q\n", *journaldir)
	tdb := triedb.NewDatabase(db, &triedb.Config{PathDB: &pcfg})
	defer tdb.Close()
	stateCache := state.NewDatabase(tdb, nil)

	head, err := resolveHead(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve head: %v\n", err)
		os.Exit(1)
	}

	genesisHash := rawdb.ReadCanonicalHash(db, 0)
	cfg := rawdb.ReadChainConfig(db, genesisHash)
	if cfg == nil {
		fmt.Fprintln(os.Stderr, "warn: no chain config in db; falling back to params.BSCChainConfig")
		cfg = params.BSCChainConfig
	}
	fmt.Fprintf(os.Stderr, "head=%d chainID=%v target=%s\n", head, cfg.ChainID, target.Hex())

	chain := &chainCtx{db: db, config: cfg}

	switch *mode {
	case "probe":
		probe(db, stateCache, head)
	case "trace":
		from, to := *fromBlock, *toBlock
		if to == 0 || to > head {
			to = head
		}
		if from == 0 {
			if *nBlocks == 0 || *nBlocks > to {
				from = 1
			} else {
				from = to - *nBlocks + 1
			}
		}
		runTrace(db, stateCache, chain, cfg, target, minVal, from, to, *progress, *outFile)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown -mode %q (want probe|trace)\n", *mode)
		os.Exit(2)
	}
}

// ---- state availability probe -------------------------------------------------

func flagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func probe(db ethdb.Database, stateCache state.Database, head uint64) {
	// Persistent (disk-layer) state: always present in pebble regardless of the
	// journal. Its state-id is ~ the block height it represents.
	_, diskRoot := rawdb.ReadAccountTrieNodeAndHash(db, nil)
	persistID := rawdb.ReadPersistentStateID(db)
	fmt.Printf("disk-layer root=%s persistentStateID=%d (head=%d, behind head by ~%d blocks)\n",
		diskRoot.Hex(), persistID, head, int64(head)-int64(persistID))
	if _, err := state.New(diskRoot, stateCache); err != nil {
		fmt.Printf("  state.New(diskRoot) FAILED: %v\n", err)
	} else {
		fmt.Printf("  state.New(diskRoot) OK -> the persisted state is usable\n")
	}
	fmt.Println()

	fmt.Println("probing state availability (which parent states can be loaded for re-execution)")
	fmt.Println("offset\tblock\tstate_root\tavailable\tblock_time")
	offsets := []uint64{0, 1, 2, 4, 8, 16, 32, 64, 96, 120, 127, 128, 129, 130, 140, 160, 256, 512, 1024}
	var prevTime uint64
	var prevN uint64
	for _, off := range offsets {
		if off > head {
			break
		}
		n := head - off
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			fmt.Printf("%d\t%d\t<no canonical hash>\n", off, n)
			continue
		}
		hdr := rawdb.ReadHeader(db, hash, n)
		if hdr == nil {
			fmt.Printf("%d\t%d\t<no header>\n", off, n)
			continue
		}
		_, err := state.New(hdr.Root, stateCache)
		avail := "YES"
		if err != nil {
			avail = "no (" + err.Error() + ")"
		}
		fmt.Printf("%d\t%d\t%s\t%s\t%d\n", off, n, hdr.Root.Hex(), avail, hdr.Time)
		if prevTime != 0 && hdr.Time != 0 && prevN > n {
			dt := float64(prevTime-hdr.Time) / float64(prevN-n)
			fmt.Printf("    ~%.2fs/block over [%d,%d]\n", dt, n, prevN)
		}
		prevTime, prevN = hdr.Time, n
	}
	fmt.Println("\nThe deepest 'YES' offset ~= the number of blocks you can trace (parent state must be available).")
}

// ---- trace --------------------------------------------------------------------

// hit is one internal value transfer to the target address.
type hit struct {
	Block    uint64          `json:"block"`
	TxIndex  int             `json:"txIndex"`
	TxHash   common.Hash     `json:"txHash"`
	TxTo     *common.Address `json:"txTo"`  // entry contract called by the tx (nil = contract creation)
	From     common.Address  `json:"from"`  // immediate caller = the contract paying the tip
	Depth    int             `json:"depth"` // 0 = top-level tx, >=1 = internal
	Value    string          `json:"value"` // wei
	CallType string          `json:"callType"`
}

type tipTracer struct {
	target common.Address
	min    *big.Int

	// per-tx context, set before each ApplyMessage
	curBlock   uint64
	curTxIndex int
	curTxHash  common.Hash
	curTxTo    *common.Address

	hits []hit
}

func (t *tipTracer) onEnter(depth int, typ byte, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	if to != t.target || value == nil || value.Sign() == 0 {
		return
	}
	if t.min.Sign() > 0 && value.Cmp(t.min) < 0 {
		return
	}
	var txTo *common.Address
	if t.curTxTo != nil {
		a := *t.curTxTo
		txTo = &a
	}
	t.hits = append(t.hits, hit{
		Block:    t.curBlock,
		TxIndex:  t.curTxIndex,
		TxHash:   t.curTxHash,
		TxTo:     txTo,
		From:     from,
		Depth:    depth,
		Value:    new(big.Int).Set(value).String(),
		CallType: vm.OpCode(typ).String(),
	})
}

func runTrace(db ethdb.Database, stateCache state.Database, chain *chainCtx, cfg *params.ChainConfig,
	target common.Address, minVal *big.Int, from, to uint64, progressEvery uint64, outFile string) {

	tr := &tipTracer{target: target, min: minVal}
	hooks := &tracing.Hooks{OnEnter: tr.onEnter}

	fmt.Fprintf(os.Stderr, "tracing blocks [%d, %d] (%d blocks)\n", from, to, to-from+1)
	start := time.Now()
	var (
		traced       uint64
		skippedState uint64
		firstUnavail uint64
		txExecuted   uint64
		txFailed     uint64
	)

	for n := from; n <= to; n++ {
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			continue
		}
		header := rawdb.ReadHeader(db, hash, n)
		if header == nil {
			continue
		}
		parent := rawdb.ReadHeader(db, header.ParentHash, n-1)
		if parent == nil {
			continue
		}
		statedb, err := state.New(parent.Root, stateCache)
		if err != nil {
			skippedState++
			if firstUnavail == 0 {
				firstUnavail = n
			}
			continue
		}
		body := rawdb.ReadBody(db, hash, n)
		if body == nil {
			continue
		}

		author := header.Coinbase
		blockCtx := core.NewEVMBlockContext(header, chain, &author)
		evm := vm.NewEVM(blockCtx, statedb, cfg, vm.Config{Tracer: hooks})
		signer := types.MakeSigner(cfg, header.Number, header.Time)
		gp := new(core.GasPool).AddGas(header.GasLimit)

		tr.curBlock = n
		for i, tx := range body.Transactions {
			msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
			if err != nil {
				continue
			}
			// BSC system transactions sit at the end of the block, are signed by
			// the validator (== coinbase) and carry a zero gas price. They are
			// applied through a special consensus path; skip them — nothing after
			// them is a normal user tx.
			if msg.From == author && tx.GasPrice().Sign() == 0 {
				break
			}
			tr.curTxIndex = i
			tr.curTxHash = tx.Hash()
			tr.curTxTo = tx.To()
			statedb.SetTxContext(tx.Hash(), i)
			if _, err := core.ApplyMessage(evm, msg, gp); err != nil {
				txFailed++
				// Out of gas in the shared pool would corrupt later txs; refill so
				// the rest of the block still re-executes deterministically.
				gp = new(core.GasPool).AddGas(header.GasLimit)
				continue
			}
			statedb.Finalise(true)
			txExecuted++
		}

		traced++
		if progressEvery > 0 && traced%progressEvery == 0 {
			el := time.Since(start)
			fmt.Fprintf(os.Stderr, "  block=%d traced=%d hits=%d tx=%d rate=%.1f blk/s elapsed=%s\n",
				n, traced, len(tr.hits), txExecuted, float64(traced)/el.Seconds(), el.Truncate(time.Second))
		}
	}

	el := time.Since(start)
	fmt.Fprintf(os.Stderr, "done in %s: traced=%d state-unavailable=%d (first at %d) txExecuted=%d txFailed=%d hits=%d\n",
		el.Truncate(time.Millisecond), traced, skippedState, firstUnavail, txExecuted, txFailed, len(tr.hits))

	report(tr.hits, outFile)
}

// ---- reporting ----------------------------------------------------------------

type agg struct {
	addr   common.Address
	hits   int
	txs    map[common.Hash]struct{}
	total  *big.Int
	sample hit
}

func report(hits []hit, outFile string) {
	if outFile != "" {
		f, err := os.Create(outFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: cannot write %s: %v\n", outFile, err)
		} else {
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			_ = enc.Encode(hits)
			f.Close()
			fmt.Fprintf(os.Stderr, "wrote %d hits to %s\n", len(hits), outFile)
		}
	}

	if len(hits) == 0 {
		fmt.Println("\nNo internal transfers to the target address found in the traced range.")
		return
	}

	// Aggregate by the paying contract: the immediate caller (from). For the
	// rare top-level (depth 0) transfer there is no contract payer, so attribute
	// to the tx sender's target via TxTo instead; we still surface depth.
	byPayer := map[common.Address]*agg{}
	byEntry := map[common.Address]*agg{}
	add := func(m map[common.Address]*agg, a common.Address, h hit) {
		e := m[a]
		if e == nil {
			e = &agg{addr: a, txs: map[common.Hash]struct{}{}, total: new(big.Int), sample: h}
			m[a] = e
		}
		e.hits++
		e.txs[h.TxHash] = struct{}{}
		v, _ := new(big.Int).SetString(h.Value, 10)
		e.total.Add(e.total, v)
	}
	for _, h := range hits {
		add(byPayer, h.From, h)
		if h.TxTo != nil {
			add(byEntry, *h.TxTo, h)
		}
	}

	printRanked := func(title string, m map[common.Address]*agg) {
		list := make([]*agg, 0, len(m))
		for _, e := range m {
			list = append(list, e)
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].total.Cmp(list[j].total) != 0 {
				return list[i].total.Cmp(list[j].total) > 0
			}
			return list[i].hits > list[j].hits
		})
		fmt.Printf("\n=== %s (%d unique) ===\n", title, len(list))
		fmt.Printf("%-44s %8s %8s %22s  %s\n", "address", "transfers", "txs", "total_BNB", "sample_tx")
		for _, e := range list {
			fmt.Printf("%-44s %8d %8d %22s  %s\n",
				e.addr.Hex(), e.hits, len(e.txs), formatBNB(e.total), e.sample.TxHash.Hex())
		}
	}

	printRanked("paying contracts (immediate caller of transfer to 48 Club)", byPayer)
	printRanked("entry contracts (tx.to of those txs)", byEntry)
}

func formatBNB(wei *big.Int) string {
	// 18 decimals; print with 6 fractional digits.
	q := new(big.Int)
	r := new(big.Int)
	q.QuoRem(wei, big.NewInt(1e18), r)
	frac := new(big.Int).Mul(r, big.NewInt(1e6))
	frac.Div(frac, big.NewInt(1e18))
	return fmt.Sprintf("%s.%06d", q.String(), frac.Int64())
}

// ---- plumbing -----------------------------------------------------------------

func openReadOnly(chaindata, ancient string) (ethdb.Database, error) {
	kv, err := pebble.New(chaindata, 512, 512, "", true /*readonly*/)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	db, err := rawdb.Open(kv, rawdb.OpenOptions{Ancient: ancient, ReadOnly: true})
	if err != nil {
		kv.Close()
		return nil, fmt.Errorf("open chain db: %w", err)
	}
	return db, nil
}

func resolveHead(db ethdb.Database) (uint64, error) {
	hash := rawdb.ReadHeadBlockHash(db)
	if hash == (common.Hash{}) {
		hash = rawdb.ReadHeadHeaderHash(db)
	}
	if hash == (common.Hash{}) {
		return 0, fmt.Errorf("no head block/header hash in db")
	}
	num, ok := rawdb.ReadHeaderNumber(db, hash)
	if !ok {
		return 0, fmt.Errorf("head header number missing for %s", hash.Hex())
	}
	return num, nil
}

// chainCtx is a minimal core.ChainContext backed directly by rawdb. Engine() is
// never called because we always pass an explicit author to NewEVMBlockContext.
type chainCtx struct {
	db     ethdb.Database
	config *params.ChainConfig
}

func (c *chainCtx) Config() *params.ChainConfig { return c.config }
func (c *chainCtx) Engine() consensus.Engine    { return nil }
func (c *chainCtx) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(c.db, hash, number)
}
func (c *chainCtx) GetHeaderByNumber(number uint64) *types.Header {
	hash := rawdb.ReadCanonicalHash(c.db, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return rawdb.ReadHeader(c.db, hash, number)
}
func (c *chainCtx) GetHeaderByHash(hash common.Hash) *types.Header {
	number, ok := rawdb.ReadHeaderNumber(c.db, hash)
	if !ok {
		return nil
	}
	return rawdb.ReadHeader(c.db, hash, number)
}
func (c *chainCtx) CurrentHeader() *types.Header {
	hash := rawdb.ReadHeadHeaderHash(c.db)
	number, ok := rawdb.ReadHeaderNumber(c.db, hash)
	if !ok {
		return nil
	}
	return rawdb.ReadHeader(c.db, hash, number)
}
