// Command mevscan finds likely MEV-bot contracts on BSC from on-disk data only
// (block bodies + receipts), without needing historical state.
//
// Rationale: the 48 Club builder is paid by searchers via an INTERNAL bnb
// transfer to 0x4848489f0b2BEdd788c696e2D79b6b69D7484848. Internal transfers
// emit no logs, so they cannot be read from receipts and (for a pruned
// path-scheme snapshot) cannot be re-executed either. But the paying txs are
// arbitrage/sandwich txs, which DO emit DEX swap events, and — because the
// builder is paid out-of-band via that internal transfer rather than via gas —
// they are typically included with a sub-mempool-minimum (≈0) priority fee.
//
// This tool scans a block range, and for every tx that contains >=1 DEX swap
// event groups it by tx.To (the contract that was called = the bot). For each
// such contract it records arb/MEV signals computable from disk:
//   - swap-tx count, multi-swap-tx count, total swap events
//   - low-gas (sub-mempool-min) and zero-tip tx counts  <- builder-bid signal
//   - top-of-block (index 0) tx count                   <- bid-for-top signal
//   - distinct blocks, min gas price, sample tx hashes
//
// Famous routers/aggregators are tagged via a built-in denylist so they can be
// excluded; the remaining high-signal unknown contracts are the MEV candidates.
// Verify a candidate by checking whether its sample txs contain an internal tx
// to the 48 Club address (e.g. BscScan txlistinternal&txhash=...).
//
// Run against a snapshot's chaindata READ-ONLY. Only the frozen (ancient) range
// is guaranteed consistent on a hard-link snapshot of a live node.
//
//	mevscan -chaindata /path/fast/geth/chaindata -blocks 5000
//	mevscan -chaindata /path/fast/geth/chaindata -from 40000000 -to 40050000 -out groups.json
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
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
)

// DEX swap event topic0 -> family label.
var swapTopics = map[common.Hash]string{
	common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"): "v2",       // UniV2/PancakeV2 Swap
	common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"): "univ3",    // UniswapV3 Swap
	common.HexToHash("0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"): "cakev3",   // PancakeV3 Swap
	common.HexToHash("0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"): "univ4",    // UniswapV4 PoolManager Swap
	common.HexToHash("0x04206ad2b7c0f463bff3dd4f33c5735b0f2957a351e4f79763a4fa9e775dd237"): "cakeinf",  // Pancake Infinity CL (tentative)
}

// Famous routers / aggregators on BSC. Swap txs to these are ordinary user
// trades, not MEV bots; tagged so they can be filtered out.
var famous = map[common.Address]string{
	common.HexToAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"): "PancakeV2Router",
	common.HexToAddress("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4"): "PancakeSmartRouterV3",
	common.HexToAddress("0x1b81D678ffb9C0263b24A97847620C99d213eB14"): "PancakeSmartRouter",
	common.HexToAddress("0x1A0A18AC4BECDDbd6389559687d1A73d8927E416"): "PancakeUniversalRouter",
	common.HexToAddress("0xEfF92A263d31888d860bD50809A8D171709b7b1c"): "PancakeUniversalRouter2",
	common.HexToAddress("0x4Dae2f939ACf50408e13d58534Ff8c2776d45265"): "UniswapUniversalRouter",
	common.HexToAddress("0x111111125421cA6dc452d289314280a0f8842A65"): "1inchV6Router",
	common.HexToAddress("0x1111111254EEB25477B68fb85Ed929f73A960582"): "1inchV5Router",
	common.HexToAddress("0xDef1C0ded9bec7F1a1670819833240f027b25EfF"): "0xExchangeProxy",
	common.HexToAddress("0x6131B5fae19EA4f9D964eAc0408E4408b66337b5"): "KyberMetaAggregatorV2",
	common.HexToAddress("0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57"): "ParaswapV5",
	common.HexToAddress("0x6352a56caadC4F1E25CD6c75970Fa768A3304e64"): "OpenOceanRouter",
	common.HexToAddress("0x89b8AA89FDd0507a99d334CBe3C808fAFC7d850E"): "OdosRouterV2",
	common.HexToAddress("0x111111125434b319222CdBf8C261674aDB56F3ae"): "1inchAggregationRouter",
}

func main() {
	var (
		chaindata = flag.String("chaindata", "", "path to the snapshotted geth/chaindata directory (required)")
		ancient   = flag.String("ancient", "", "path to the ancient store (default: <chaindata>/ancient)")
		fromBlock = flag.Uint64("from", 0, "first block to scan (inclusive); 0 = derive from -blocks")
		toBlock   = flag.Uint64("to", 0, "last block to scan (inclusive); 0 = last frozen block")
		nBlocks   = flag.Uint64("blocks", 5000, "when -from is 0, scan this many blocks ending at -to")
		lowGasGwei = flag.Float64("lowgas", 0.05, "gas price (gwei) at/below which a tx is flagged as a builder-bid (sub-mempool-min)")
		topN      = flag.Int("top", 80, "rows to print")
		minSwapTx = flag.Int("minswaptx", 2, "only print contracts with at least this many swap txs")
		out       = flag.String("out", "", "write all groups as JSON to this file")
		progress  = flag.Uint64("progress", 2000, "log progress every N blocks (0 = silent)")
	)
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelWarn, false)))

	if *chaindata == "" {
		fmt.Fprintln(os.Stderr, "error: -chaindata is required")
		flag.Usage()
		os.Exit(2)
	}
	if *ancient == "" {
		*ancient = filepath.Join(*chaindata, "ancient")
	}
	lowGasWei := new(big.Int).SetUint64(uint64(*lowGasGwei * 1e9))

	db, err := openReadOnly(*chaindata, *ancient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	head := headBlock(db)
	frozen, _ := db.Ancients()
	tail, _ := db.Tail()
	fmt.Fprintf(os.Stderr, "head=%d frozen(ancients)=%d tail=%d\n", head, frozen, tail)

	// Default to scanning inside the frozen range (consistent on a live snapshot).
	to := *toBlock
	if to == 0 {
		if frozen > 0 {
			to = frozen - 1
		} else {
			to = head
		}
	}
	from := *fromBlock
	if from == 0 {
		if *nBlocks == 0 || *nBlocks > to {
			from = 0
		} else {
			from = to - *nBlocks + 1
		}
	}
	if from < tail {
		from = tail
	}
	fmt.Fprintf(os.Stderr, "scanning blocks [%d, %d] (%d blocks); lowgas<=%.4f gwei\n", from, to, to-from+1, *lowGasGwei)

	groups := map[common.Address]*group{}
	start := time.Now()
	var (
		blocksScanned uint64
		swapTxs       uint64
		totalTxs      uint64
		firstScanTime uint64
		lastScanTime  uint64
	)

	for n := from; n <= to; n++ {
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			continue
		}
		header := rawdb.ReadHeader(db, hash, n)
		body := rawdb.ReadBody(db, hash, n)
		receipts := rawdb.ReadRawReceipts(db, hash, n)
		if header == nil || body == nil || receipts == nil {
			continue
		}
		baseFee := header.BaseFee // may be nil
		if firstScanTime == 0 {
			firstScanTime = header.Time
		}
		lastScanTime = header.Time

		for i, tx := range body.Transactions {
			totalTxs++
			if i >= len(receipts) || receipts[i] == nil {
				continue
			}
			to := tx.To()
			if to == nil { // contract creation, not a bot call
				continue
			}
			// Count swap events of this tx.
			var nSwap int
			var fam uint8
			for _, lg := range receipts[i].Logs {
				if len(lg.Topics) == 0 {
					continue
				}
				if f, ok := swapTopics[lg.Topics[0]]; ok {
					nSwap++
					fam |= famBit(f)
				}
			}
			if nSwap == 0 {
				continue
			}
			swapTxs++

			gp := effectiveGasPrice(tx, baseFee)
			lowGas := gp.Cmp(lowGasWei) <= 0
			zeroTip := isZeroTip(tx, baseFee)

			g := groups[*to]
			if g == nil {
				g = &group{addr: *to, minGas: new(big.Int).Set(gp), lastBlock: ^uint64(0),
					firstBlock: n, firstTime: header.Time}
				groups[*to] = g
			}
			g.nSwapTx++
			g.swapEvents += nSwap
			g.fam |= fam
			if nSwap >= 2 {
				g.nMultiSwapTx++
			}
			if lowGas {
				g.nLowGasTx++
			}
			if zeroTip {
				g.nZeroTipTx++
			}
			if i == 0 {
				g.nTopOfBlock++
			}
			if gp.Cmp(g.minGas) < 0 {
				g.minGas.Set(gp)
			}
			if n != g.lastBlock {
				g.distinctBlocks++
				g.lastBlock = n
				g.lastTime = header.Time
			}
			// Keep sample tx hashes, preferring low-gas ones (most likely bids).
			g.addSample(tx.Hash(), lowGas)
		}

		blocksScanned++
		if *progress > 0 && blocksScanned%*progress == 0 {
			el := time.Since(start)
			rate := float64(blocksScanned) / el.Seconds()
			eta := time.Duration(float64(to-n) / rate * float64(time.Second))
			fmt.Fprintf(os.Stderr, "  block=%d scanned=%d swapTx=%d groups=%d rate=%.0f blk/s eta=%s\n",
				n, blocksScanned, swapTxs, len(groups), rate, eta.Truncate(time.Second))
		}
	}

	el := time.Since(start)
	spanDays := float64(lastScanTime-firstScanTime) / 86400.0
	fmt.Fprintf(os.Stderr, "done in %s: blocks=%d totalTx=%d swapTx=%d uniqueContracts=%d\n",
		el.Truncate(time.Millisecond), blocksScanned, totalTxs, swapTxs, len(groups))
	fmt.Fprintf(os.Stderr, "data span: startTime=%d endTime=%d (%.2f days)\n",
		firstScanTime, lastScanTime, spanDays)

	report(groups, *topN, *minSwapTx, *out)
}

// ---- per-contract aggregate ---------------------------------------------------

type group struct {
	addr           common.Address
	nSwapTx        int
	nMultiSwapTx   int
	nLowGasTx      int
	nZeroTipTx     int
	nTopOfBlock    int
	swapEvents     int
	distinctBlocks int
	fam            uint8
	minGas         *big.Int
	lastBlock      uint64
	firstBlock     uint64
	firstTime      uint64
	lastTime       uint64
	samples        []common.Hash
	lowSamples     int
}

const maxSamples = 6

func (g *group) addSample(h common.Hash, lowGas bool) {
	if len(g.samples) < maxSamples {
		if lowGas {
			// put low-gas samples first
			g.samples = append([]common.Hash{h}, g.samples...)
			g.lowSamples++
		} else {
			g.samples = append(g.samples, h)
		}
		return
	}
	// replace a non-low-gas sample with a low-gas one if we don't have enough.
	if lowGas && g.lowSamples < maxSamples {
		g.samples[len(g.samples)-1] = h
		g.lowSamples++
	}
}

func famBit(f string) uint8 {
	switch f {
	case "v2":
		return 1
	case "univ3":
		return 2
	case "cakev3":
		return 4
	case "univ4":
		return 8
	case "cakeinf":
		return 16
	}
	return 0
}

func famString(b uint8) string {
	parts := []string{}
	if b&1 != 0 {
		parts = append(parts, "v2")
	}
	if b&2 != 0 {
		parts = append(parts, "univ3")
	}
	if b&4 != 0 {
		parts = append(parts, "cakev3")
	}
	if b&8 != 0 {
		parts = append(parts, "univ4")
	}
	if b&16 != 0 {
		parts = append(parts, "cakeinf")
	}
	if len(parts) == 0 {
		return "-"
	}
	s := parts[0]
	for _, p := range parts[1:] {
		s += "," + p
	}
	return s
}

// ---- gas helpers --------------------------------------------------------------

// effectiveGasPrice returns the gas price actually paid: for legacy txs it's
// GasPrice; for dynamic-fee txs it's baseFee + min(tipCap, feeCap-baseFee).
func effectiveGasPrice(tx *types.Transaction, baseFee *big.Int) *big.Int {
	if baseFee == nil || baseFee.Sign() == 0 {
		// No base fee: tip cap == effective price for both tx kinds on BSC.
		if tx.Type() == types.LegacyTxType || tx.Type() == types.AccessListTxType {
			return new(big.Int).Set(tx.GasPrice())
		}
		return new(big.Int).Set(tx.GasTipCap())
	}
	tip := new(big.Int).Sub(tx.GasFeeCap(), baseFee)
	if tip.Sign() < 0 {
		tip.SetInt64(0)
	}
	if tip.Cmp(tx.GasTipCap()) > 0 {
		tip.Set(tx.GasTipCap())
	}
	return tip.Add(tip, baseFee)
}

// isZeroTip reports whether the tx pays ~zero priority fee to the validator.
func isZeroTip(tx *types.Transaction, baseFee *big.Int) bool {
	if baseFee == nil {
		baseFee = common.Big0
	}
	tip := new(big.Int).Sub(effectiveGasPrice(tx, baseFee), baseFee)
	return tip.Sign() <= 0
}

// ---- reporting ----------------------------------------------------------------

type row struct {
	Address      string   `json:"address"`
	Famous       string   `json:"famous,omitempty"`
	SwapTx       int      `json:"swapTx"`
	MultiSwapTx  int      `json:"multiSwapTx"`
	LowGasTx     int      `json:"lowGasTx"`
	ZeroTipTx    int      `json:"zeroTipTx"`
	TopOfBlock   int      `json:"topOfBlock"`
	SwapEvents   int      `json:"swapEvents"`
	Blocks       int      `json:"distinctBlocks"`
	FirstBlock   uint64   `json:"firstBlock"`
	LastBlock    uint64   `json:"lastBlock"`
	FirstTime    uint64   `json:"firstTime"`
	LastTime     uint64   `json:"lastTime"`
	MinGasGwei   float64  `json:"minGasGwei"`
	Families     string   `json:"families"`
	Samples      []string `json:"sampleTxs"`
}

func report(groups map[common.Address]*group, topN, minSwapTx int, outFile string) {
	rows := make([]row, 0, len(groups))
	for _, g := range groups {
		name := famous[g.addr]
		samples := make([]string, len(g.samples))
		for i, h := range g.samples {
			samples[i] = h.Hex()
		}
		rows = append(rows, row{
			Address:     g.addr.Hex(),
			Famous:      name,
			SwapTx:      g.nSwapTx,
			MultiSwapTx: g.nMultiSwapTx,
			LowGasTx:    g.nLowGasTx,
			ZeroTipTx:   g.nZeroTipTx,
			TopOfBlock:  g.nTopOfBlock,
			SwapEvents:  g.swapEvents,
			Blocks:      g.distinctBlocks,
			FirstBlock:  g.firstBlock,
			LastBlock:   g.lastBlock,
			FirstTime:   g.firstTime,
			LastTime:    g.lastTime,
			MinGasGwei:  gweiFloat(g.minGas),
			Families:    famString(g.fam),
			Samples:     samples,
		})
	}

	// Rank by builder-bid signal: low-gas swap txs, then multi-swap, then swap txs.
	rank := func(a, b row) bool {
		if a.LowGasTx != b.LowGasTx {
			return a.LowGasTx > b.LowGasTx
		}
		if a.MultiSwapTx != b.MultiSwapTx {
			return a.MultiSwapTx > b.MultiSwapTx
		}
		return a.SwapTx > b.SwapTx
	}

	if outFile != "" {
		sort.Slice(rows, func(i, j int) bool { return rank(rows[i], rows[j]) })
		f, err := os.Create(outFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: cannot write %s: %v\n", outFile, err)
		} else {
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			_ = enc.Encode(rows)
			f.Close()
			fmt.Fprintf(os.Stderr, "wrote %d groups to %s\n", len(rows), outFile)
		}
	}

	// Candidate shortlist: unknown (not famous) contracts, ranked by bid signal.
	cands := make([]row, 0, len(rows))
	for _, r := range rows {
		if r.Famous != "" || r.SwapTx < minSwapTx {
			continue
		}
		cands = append(cands, r)
	}
	sort.Slice(cands, func(i, j int) bool { return rank(cands[i], cands[j]) })

	fmt.Printf("\n=== MEV-candidate contracts (unknown tx.to, ranked by builder-bid signal) ===\n")
	fmt.Printf("%-44s %7s %7s %7s %7s %7s %7s %9s %-10s\n",
		"address", "swapTx", "multi", "lowGas", "zeroTip", "topBlk", "blocks", "minGwei", "dex")
	n := topN
	if n > len(cands) {
		n = len(cands)
	}
	for i := 0; i < n; i++ {
		r := cands[i]
		fmt.Printf("%-44s %7d %7d %7d %7d %7d %7d %9.4f %-10s\n",
			r.Address, r.SwapTx, r.MultiSwapTx, r.LowGasTx, r.ZeroTipTx, r.TopOfBlock, r.Blocks, r.MinGasGwei, r.Families)
	}
	fmt.Printf("\n(%d unknown candidates total; %d famous routers/aggregators filtered out)\n",
		len(cands), len(rows)-len(cands))
	fmt.Println("Verify a candidate: check whether its sample txs contain an internal tx to")
	fmt.Println("0x4848489f0b2BEdd788c696e2D79b6b69D7484848 (BscScan txlistinternal&txhash=...).")
}

func gweiFloat(wei *big.Int) float64 {
	if wei == nil {
		return 0
	}
	f := new(big.Float).SetInt(wei)
	f.Quo(f, big.NewFloat(1e9))
	v, _ := f.Float64()
	return v
}

// ---- plumbing -----------------------------------------------------------------

func openReadOnly(chaindata, ancient string) (ethdb.Database, error) {
	kv, err := pebble.New(chaindata, 512, 512, "", true)
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

func headBlock(db ethdb.Database) uint64 {
	hash := rawdb.ReadHeadBlockHash(db)
	if hash == (common.Hash{}) {
		hash = rawdb.ReadHeadHeaderHash(db)
	}
	if hash == (common.Hash{}) {
		return 0
	}
	if n, ok := rawdb.ReadHeaderNumber(db, hash); ok {
		return n
	}
	return 0
}
