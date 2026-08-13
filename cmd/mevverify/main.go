// Command mevverify confirms which MEV-candidate contracts (from mevscan) pay
// the 48 Club builder by checking, via the Etherscan/BscScan API, whether a
// candidate's sample transactions contain an INTERNAL transfer to the 48 Club
// payment address 0x4848489f0b2BEdd788c696e2D79b6b69D7484848.
//
// Internal transfers are produced by Etherscan's own tracing, so this is the
// piece we cannot get from the snapshot. It runs only against a small, already
// filtered candidate set, so the call volume stays low.
//
// Input is the JSON written by `mevscan -out`. Provide an API key via -apikey
// or the ETHERSCAN_API_KEY / BSCSCAN_API_KEY env var.
//
//	mevverify -in groups.json -apikey $KEY -max 300 -out confirmed.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const target = "0x4848489f0b2bedd788c696e2d79b6b69d7484848" // lowercased

type candidate struct {
	Address     string   `json:"address"`
	Famous      string   `json:"famous,omitempty"`
	SwapTx      int      `json:"swapTx"`
	MultiSwapTx int      `json:"multiSwapTx"`
	LowGasTx    int      `json:"lowGasTx"`
	ZeroTipTx   int      `json:"zeroTipTx"`
	TopOfBlock  int      `json:"topOfBlock"`
	Blocks      int      `json:"distinctBlocks"`
	Families    string   `json:"families"`
	Samples     []string `json:"sampleTxs"`
}

type result struct {
	candidate
	Checked     int    `json:"checkedTxs"`
	Pays48      bool   `json:"pays48club"`
	ConfirmTx   string `json:"confirmTx,omitempty"`
	ValueWei    string `json:"valueWei,omitempty"`
	OtherBuild  string `json:"otherBuilderTo,omitempty"` // a non-48 builder-ish recipient seen, if any
	Err         string `json:"error,omitempty"`
}

type apiResp struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  []struct {
		Hash  string `json:"hash"`
		From  string `json:"from"`
		To    string `json:"to"`
		Value string `json:"value"`
		Type  string `json:"type"`
	} `json:"result"`
}

func main() {
	var (
		in       = flag.String("in", "", "mevscan JSON file (required)")
		apikey   = flag.String("apikey", "", "Etherscan API key (or env ETHERSCAN_API_KEY/BSCSCAN_API_KEY)")
		baseURL  = flag.String("api", "https://api.etherscan.io/v2/api?chainid=56", "API base (Etherscan V2 multichain, chainid=56=BSC)")
		mode     = flag.String("mode", "addr", "addr = check each contract's full internal-tx history (1 call, reliable); txhash = check only its sample txs")
		fromBlk  = flag.Uint64("from", 94367952, "addr mode: startblock for internal-tx lookup")
		toBlk    = flag.Uint64("to", 94919952, "addr mode: endblock for internal-tx lookup")
		maxCand  = flag.Int("max", 1000, "max candidates to verify (ranked by multiSwap then swapTx)")
		maxSamp  = flag.Int("samples", 4, "txhash mode: max sample txs to check per candidate")
		minSwap  = flag.Int("minswaptx", 3, "skip candidates with fewer swap txs")
		ratems   = flag.Int("ratems", 220, "delay between API calls in ms (free tier ~5/s)")
		outFile  = flag.String("out", "", "write results JSON here")
		stopHit  = flag.Bool("stop-on-hit", true, "txhash mode: stop a candidate's samples once 48 Club payment is found")
	)
	flag.Parse()

	if *apikey == "" {
		*apikey = firstNonEmpty(os.Getenv("ETHERSCAN_API_KEY"), os.Getenv("BSCSCAN_API_KEY"))
	}
	if *in == "" {
		fmt.Fprintln(os.Stderr, "error: -in is required")
		os.Exit(2)
	}
	if *apikey == "" {
		fmt.Fprintln(os.Stderr, "error: no API key (-apikey or ETHERSCAN_API_KEY)")
		os.Exit(2)
	}

	raw, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", *in, err)
		os.Exit(1)
	}
	var cands []candidate
	if err := json.Unmarshal(raw, &cands); err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", *in, err)
		os.Exit(1)
	}

	// Keep unknown (non-famous) candidates with enough swap txs.
	filtered := cands[:0]
	for _, c := range cands {
		if c.Famous != "" || c.SwapTx < *minSwap || len(c.Samples) == 0 {
			continue
		}
		filtered = append(filtered, c)
	}
	// Rank by arb shape so the -max cut keeps the strongest candidates.
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].MultiSwapTx != filtered[j].MultiSwapTx {
			return filtered[i].MultiSwapTx > filtered[j].MultiSwapTx
		}
		return filtered[i].SwapTx > filtered[j].SwapTx
	})
	if len(filtered) > *maxCand {
		filtered = filtered[:*maxCand]
	}
	fmt.Fprintf(os.Stderr, "verifying %d candidates (<=%d samples each) against 48 Club %s\n",
		len(filtered), *maxSamp, target)

	client := &http.Client{Timeout: 25 * time.Second}
	delay := time.Duration(*ratems) * time.Millisecond
	var results []result
	confirmed := 0

	for idx, c := range filtered {
		r := result{candidate: c}
		if *mode == "addr" {
			r.Checked = 1
			hit, val, tx, other, err := internalToByAddr(client, *baseURL, *apikey, c.Address, *fromBlk, *toBlk, delay)
			if err != nil {
				r.Err = err.Error()
			} else if hit {
				r.Pays48 = true
				r.ConfirmTx = tx
				r.ValueWei = val
			} else if other != "" {
				r.OtherBuild = other
			}
		} else {
			ns := c.Samples
			if len(ns) > *maxSamp {
				ns = ns[:*maxSamp]
			}
			for _, h := range ns {
				r.Checked++
				to, val, other, err := internalTo(client, *baseURL, *apikey, h, delay)
				if err != nil {
					r.Err = err.Error()
					continue
				}
				if to {
					r.Pays48 = true
					r.ConfirmTx = h
					r.ValueWei = val
					break
				}
				if other != "" && r.OtherBuild == "" {
					r.OtherBuild = other
				}
				if *stopHit && r.Pays48 {
					break
				}
			}
		}
		if r.Pays48 {
			confirmed++
			fmt.Printf("CONFIRM %s  zeroTip=%d lowGas=%d swapTx=%d  value=%s  tx=%s\n",
				c.Address, c.ZeroTipTx, c.LowGasTx, c.SwapTx, r.ValueWei, r.ConfirmTx)
		} else {
			fmt.Printf("  -     %s  zeroTip=%d lowGas=%d swapTx=%d  (no 48club in %d samples%s)\n",
				c.Address, c.ZeroTipTx, c.LowGasTx, c.SwapTx, r.Checked, otherNote(r.OtherBuild))
		}
		results = append(results, r)
		if (idx+1)%25 == 0 {
			fmt.Fprintf(os.Stderr, "  ...%d/%d checked, %d confirmed\n", idx+1, len(filtered), confirmed)
		}
	}

	fmt.Printf("\n=== %d / %d candidates confirmed paying 48 Club ===\n", confirmed, len(filtered))
	sort.Slice(results, func(i, j int) bool {
		if results[i].Pays48 != results[j].Pays48 {
			return results[i].Pays48
		}
		return results[i].ZeroTipTx > results[j].ZeroTipTx
	})
	for _, r := range results {
		if r.Pays48 {
			fmt.Printf("%s\tswapTx=%d\tmulti=%d\tzeroTip=%d\tblocks=%d\tdex=%s\n",
				r.Address, r.SwapTx, r.MultiSwapTx, r.ZeroTipTx, r.Blocks, r.Families)
		}
	}

	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err == nil {
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			_ = enc.Encode(results)
			f.Close()
			fmt.Fprintf(os.Stderr, "wrote %d results to %s\n", len(results), *outFile)
		}
	}
}

// internalTo reports whether tx `hash` has an internal transfer to the 48 Club
// address; returns (found, valueWei, otherRecipientHint, err).
func internalTo(client *http.Client, base, key, hash string, delay time.Duration) (bool, string, string, error) {
	time.Sleep(delay)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	u := fmt.Sprintf("%s%smodule=account&action=txlistinternal&txhash=%s&apikey=%s",
		base, sep, url.QueryEscape(hash), url.QueryEscape(key))
	resp, err := client.Get(u)
	if err != nil {
		return false, "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var ar apiResp
	if err := json.Unmarshal(body, &ar); err != nil {
		return false, "", "", fmt.Errorf("bad json: %s", truncate(string(body), 120))
	}
	if ar.Status != "1" && len(ar.Result) == 0 {
		// "No transactions found" is normal (a tx with no internal calls).
		if strings.Contains(strings.ToLower(ar.Message), "rate limit") {
			return false, "", "", fmt.Errorf("rate limited")
		}
		return false, "", "", nil
	}
	var other string
	for _, it := range ar.Result {
		if strings.EqualFold(it.To, target) {
			return true, it.Value, "", nil
		}
		// Heuristic: an internal transfer of value to an EOA-looking sink may be
		// a different builder; surface the first such recipient as a hint.
		if it.Value != "" && it.Value != "0" && other == "" {
			other = it.To
		}
	}
	return false, "", other, nil
}

// internalToByAddr checks a contract's full internal-tx history over [from,to]
// for any transfer to the 48 Club address. Returns (found, valueWei, parentTx,
// otherSinkHint, err). One API page (10k rows) is enough to confirm a bot that
// tips on most txs; we sort ascending so an early-adopter bot is caught.
func internalToByAddr(client *http.Client, base, key, addr string, from, to uint64, delay time.Duration) (bool, string, string, string, error) {
	time.Sleep(delay)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	u := fmt.Sprintf("%s%smodule=account&action=txlistinternal&address=%s&startblock=%d&endblock=%d&page=1&offset=10000&sort=asc&apikey=%s",
		base, sep, url.QueryEscape(addr), from, to, url.QueryEscape(key))
	resp, err := client.Get(u)
	if err != nil {
		return false, "", "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var ar apiResp
	if err := json.Unmarshal(body, &ar); err != nil {
		return false, "", "", "", fmt.Errorf("bad json: %s", truncate(string(body), 160))
	}
	if ar.Status != "1" && len(ar.Result) == 0 {
		if strings.Contains(strings.ToLower(ar.Message), "rate limit") {
			return false, "", "", "", fmt.Errorf("rate limited")
		}
		if strings.Contains(strings.ToLower(fmt.Sprint(ar.Message)), "no transactions") {
			return false, "", "", "", nil
		}
		// surface unexpected API errors (e.g. plan/chain not supported)
		if ar.Message != "OK" && ar.Message != "" {
			return false, "", "", "", fmt.Errorf("api: %s", truncate(fmt.Sprint(ar.Message), 80))
		}
		return false, "", "", "", nil
	}
	var other string
	for _, it := range ar.Result {
		if strings.EqualFold(it.To, target) {
			return true, it.Value, it.Hash, "", nil
		}
		if it.Value != "" && it.Value != "0" && other == "" {
			other = it.To
		}
	}
	return false, "", "", other, nil
}

func otherNote(s string) string {
	if s == "" {
		return ""
	}
	return ", other sink " + s
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
