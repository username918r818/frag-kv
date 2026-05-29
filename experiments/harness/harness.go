// Package harness runs reproducible experiments against a live fragkv cluster
// and writes results as CSV files for further analysis.
package harness

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes a single experiment run.
type Config struct {
	MetaAddr       string
	PrometheusAddr string // optional, e.g. "http://prometheus:9090"
	OutputDir      string
	ValueSizes     []int64 // bytes
	NumValues      int
	Workers        int // parallel PUT/GET goroutines
	WriteQuorum    int
}

const defaultReplicationFactor = 3

func DefaultConfig() Config {
	return Config{
		MetaAddr:    "http://localhost:9000",
		OutputDir:   "experiments/results",
		ValueSizes:  []int64{1 << 20, 10 << 20, 100 << 20},
		NumValues:   10,
		Workers:     4,
		WriteQuorum: 2,
	}
}

// ---- Experiment 1: Throughput & Latency ------------------------------------

// RunThroughput measures PUT/GET throughput and latency for various value sizes.
func RunThroughput(cfg Config) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(cfg.OutputDir, "throughput.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"op", "value_size_bytes", "duration_ms", "throughput_mbps", "status"}) //nolint:errcheck

	for _, size := range cfg.ValueSizes {
		fmt.Printf("[throughput] value_size=%d MB\n", size>>20)
		for i := 0; i < cfg.NumValues; i++ {
			key := fmt.Sprintf("throughput-%d-%d", size, i)
			data := randomBytes(size)

			// PUT
			start := time.Now()
			status := "ok"
			if err := PutValue(cfg, key, data); err != nil {
				status = "error: " + err.Error()
			}
			dur := time.Since(start)
			mbps := float64(size) / dur.Seconds() / (1 << 20)
			w.Write([]string{"put", fmt.Sprint(size), fmt.Sprint(dur.Milliseconds()), fmt.Sprintf("%.2f", mbps), status}) //nolint:errcheck

			if status != "ok" {
				continue
			}

			// GET
			start = time.Now()
			status = "ok"
			if _, err := GetValue(cfg, key); err != nil {
				status = "error: " + err.Error()
			}
			dur = time.Since(start)
			mbps = float64(size) / dur.Seconds() / (1 << 20)
			w.Write([]string{"get", fmt.Sprint(size), fmt.Sprint(dur.Milliseconds()), fmt.Sprintf("%.2f", mbps), status}) //nolint:errcheck
		}
	}
	fmt.Println("[throughput] done →", filepath.Join(cfg.OutputDir, "throughput.csv"))
	return nil
}

// ---- Experiment 2: Availability under node failures -----------------------

// RunAvailability writes N values, then kills nodes one by one and measures
// what fraction of GET operations succeed.
// killNodeFn must stop the k-th node (1-indexed) and return when it's down.
func RunAvailability(cfg Config, size int64, numKeys int, killNodeFn func(k int)) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	keys := make([]string, numKeys)
	data := randomBytes(size)
	fmt.Printf("[availability] writing %d keys (size=%d MB)…\n", numKeys, size>>20)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("avail-%d", i)
		if err := PutValue(cfg, keys[i], data); err != nil {
			return fmt.Errorf("put key %d: %w", i, err)
		}
	}

	f, err := os.Create(filepath.Join(cfg.OutputDir, "availability.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"failed_nodes", "success_count", "error_count", "availability_pct"}) //nolint:errcheck

	measure := func(failedNodes int) {
		var success, errCount int64
		var wg sync.WaitGroup
		sem := make(chan struct{}, cfg.Workers)
		for _, k := range keys {
			wg.Add(1)
			sem <- struct{}{}
			go func(key string) {
				defer wg.Done()
				defer func() { <-sem }()
				if _, err := GetValue(cfg, key); err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					atomic.AddInt64(&success, 1)
				}
			}(k)
		}
		wg.Wait()
		total := success + errCount
		pct := 100.0 * float64(success) / float64(total)
		w.Write([]string{fmt.Sprint(failedNodes), fmt.Sprint(success), fmt.Sprint(errCount), fmt.Sprintf("%.1f", pct)}) //nolint:errcheck
		fmt.Printf("[availability] failed_nodes=%d  success=%.1f%%\n", failedNodes, pct)
	}

	measure(0)
	for k := 1; k <= 2; k++ {
		fmt.Printf("[availability] killing node %d…\n", k)
		killNodeFn(k)
		time.Sleep(3 * time.Second) // wait for failure detection
		measure(k)
	}

	fmt.Println("[availability] done →", filepath.Join(cfg.OutputDir, "availability.csv"))
	return nil
}

// ---- Experiment 3: Load balance -------------------------------------------

// RunLoadBalance writes many values and measures disk-usage std/mean across nodes.
func RunLoadBalance(cfg Config, size int64, numKeys int) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	fmt.Printf("[loadbalance] writing %d keys (size=%d MB)…\n", numKeys, size>>20)
	data := randomBytes(size)
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.Workers)
	var errCount int64
	errCh := make(chan error, numKeys)
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := PutValue(cfg, fmt.Sprintf("lb-%d", i), data); err != nil {
				atomic.AddInt64(&errCount, 1)
				select {
				case errCh <- err:
				default:
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	if errCount > 0 {
		var firstErr error
		for err := range errCh {
			firstErr = err
			break
		}
		return fmt.Errorf("loadbalance: %d/%d PUT operations failed; first error: %w", errCount, numKeys, firstErr)
	}

	// Wait for Prometheus to scrape fresh data after writes. Waiting only for
	// non-zero gauges is not enough: different nodes may still expose stale
	// heartbeat values, which makes the measured CV look much worse than it is.
	if cfg.PrometheusAddr != "" {
		fmt.Printf("[loadbalance] waiting for Prometheus scrape (up to 60s)...\n")
		expectedTotal := float64(size * int64(numKeys) * defaultReplicationFactor)
		if total, err := WaitForPrometheusData(cfg.PrometheusAddr, `sum(kv_node_used_bytes{node_id!="storage-6"})`, expectedTotal, 60*time.Second); err != nil {
			fmt.Printf("[loadbalance] Prometheus scrape timeout, falling back to REST API\n")
		} else {
			fmt.Printf(" total=%d MB\n", int64(total)>>20)
		}
	}

	// Collect disk usage — prefer Prometheus (accurate, no heartbeat lag),
	// fall back to metad REST API.
	var usages []float64
	var nodeLabels []string

	if cfg.PrometheusAddr != "" {
		// Query per-node disk usage from Prometheus.
		type promVectorResult struct {
			Status string `json:"status"`
			Data   struct {
				Result []struct {
					Metric map[string]string  `json:"metric"`
					Value  [2]json.RawMessage `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		queryURL := cfg.PrometheusAddr + "/api/v1/query?query=" + url.QueryEscape(`kv_node_used_bytes{node_id!="storage-6"}`)
		if resp, err := http.Get(queryURL); err == nil { //nolint:noctx
			var r promVectorResult
			if json.NewDecoder(resp.Body).Decode(&r) == nil && r.Status == "success" {
				for _, res := range r.Data.Result {
					var v float64
					var s string
					json.Unmarshal(res.Value[1], &s) //nolint:errcheck
					fmt.Sscanf(s, "%f", &v)
					usages = append(usages, v)
					nodeLabels = append(nodeLabels, res.Metric["node_id"])
				}
			}
			resp.Body.Close()
		}
	}

	// Fallback: REST API.
	if len(usages) == 0 {
		nodes, err := listNodes(cfg.MetaAddr)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			usages = append(usages, float64(n.UsedBytes))
			nodeLabels = append(nodeLabels, n.ID)
		}
	}

	_ = nodeLabels

	mean, stddev := stats(usages)
	cv := 0.0
	if mean > 0 {
		cv = stddev / mean
	}

	f, err := os.Create(filepath.Join(cfg.OutputDir, "loadbalance.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"node_id", "used_bytes", "mean_bytes", "stddev_bytes", "cv"}) //nolint:errcheck
	for i, u := range usages {
		label := fmt.Sprintf("node-%d", i)
		if i < len(nodeLabels) {
			label = nodeLabels[i]
		}
		w.Write([]string{label, fmt.Sprintf("%.0f", u), fmt.Sprintf("%.0f", mean), fmt.Sprintf("%.0f", stddev), fmt.Sprintf("%.4f", cv)}) //nolint:errcheck
	}
	fmt.Printf("[loadbalance] mean=%.0f MB  stddev=%.0f MB  CV=%.3f\n", mean/(1<<20), stddev/(1<<20), cv)
	fmt.Println("[loadbalance] done →", filepath.Join(cfg.OutputDir, "loadbalance.csv"))
	return nil
}

// ---- Prometheus helpers ----------------------------------------------------

// QueryPrometheus executes an instant PromQL query and returns the scalar result.
// Retries up to 10 times with 3-second delays to handle slow Prometheus startup and scrape lag.
func QueryPrometheus(prometheusAddr, query string) (float64, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Second)
		}
		val, err := queryPrometheusOnce(prometheusAddr, query)
		if err == nil {
			return val, nil
		}
		lastErr = err
	}
	return -1, lastErr
}

// WaitForPrometheusData polls until the given query returns a value >= minVal.
// Useful after writing data — Prometheus needs at least one scrape cycle (5s) to reflect it.
func WaitForPrometheusData(prometheusAddr, query string, minVal float64, timeout time.Duration) (float64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		val, err := queryPrometheusOnce(prometheusAddr, query)
		if err == nil && val >= minVal {
			return val, nil
		}
		time.Sleep(5 * time.Second)
	}
	return -1, fmt.Errorf("timeout waiting for %s >= %.0f", query, minVal)
}

func queryPrometheusOnce(prometheusAddr, query string) (float64, error) {
	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", prometheusAddr, url.QueryEscape(query))
	resp, err := http.Get(queryURL) //nolint:noctx
	if err != nil {
		return -1, fmt.Errorf("prometheus unavailable: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return -1, fmt.Errorf("decode prometheus response: %w", err)
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return -1, fmt.Errorf("no data for query %q", query)
	}
	var valStr string
	if err := json.Unmarshal(result.Data.Result[0].Value[1], &valStr); err != nil {
		return -1, err
	}
	var val float64
	fmt.Sscanf(valStr, "%f", &val)
	return val, nil
}

// ---- Experiment 4: Scale-out (rebalance) -----------------------------------

// MeasureRebalance fills a cluster, then registers a new node and measures
// how many bytes were moved.
// If cfg.PrometheusAddr is set, bytes are read from kv_rebalance_bytes_total counter.
// Otherwise falls back to disk-usage delta on the new node.
func MeasureRebalance(cfg Config, size int64, numKeys int, newNodeInfo NodeInfo) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	fmt.Printf("[rebalance] filling %d keys (size=%d MB)…\n", numKeys, size>>20)
	data := randomBytes(size)
	for i := 0; i < numKeys; i++ {
		PutValue(cfg, fmt.Sprintf("rb-%d", i), data) //nolint:errcheck
	}

	// Get real total cluster bytes from Prometheus before adding new node.
	var totalBytes int64
	var promBefore float64
	usePrometheus := cfg.PrometheusAddr != ""
	if usePrometheus {
		// Wait until Prometheus has scraped actual data (not 0).
		fmt.Printf("[rebalance] waiting for Prometheus to reflect cluster data (up to 60s)...\n")
		if total, err := WaitForPrometheusData(cfg.PrometheusAddr, `sum(kv_node_used_bytes)`, 1, 60*time.Second); err != nil {
			fmt.Printf("[rebalance] prometheus not available (%v), falling back to disk delta\n", err)
			usePrometheus = false
		} else {
			totalBytes = int64(total)
			fmt.Printf("[rebalance] cluster total bytes (from Prometheus): %d MB\n", totalBytes>>20)
		}
		if usePrometheus {
			if v, err := QueryPrometheus(cfg.PrometheusAddr, `sum(kv_rebalance_bytes_total)`); err == nil {
				promBefore = v
			}
		}
	}

	// Disk-usage fallback for totalBytes.
	before, _ := nodeUsages(cfg.MetaAddr)
	if !usePrometheus {
		for _, v := range before {
			totalBytes += v
		}
	}

	// Register new node — this triggers rebalance.
	t0 := time.Now()
	if err := registerNode(cfg.MetaAddr, newNodeInfo); err != nil {
		return fmt.Errorf("register new node: %w", err)
	}
	// Wait for rebalance to settle.
	time.Sleep(15 * time.Second)
	rebalanceDur := time.Since(t0)

	after, _ := nodeUsages(cfg.MetaAddr)

	// Measure moved bytes — prefer Prometheus counter, fall back to disk delta.
	var movedBytes int64
	measureMethod := "disk_delta"
	if usePrometheus {
		promAfter, err := QueryPrometheus(cfg.PrometheusAddr, `sum(kv_rebalance_bytes_total)`)
		if err == nil && promAfter >= 0 {
			movedBytes = int64(promAfter - promBefore)
			measureMethod = "prometheus_counter"
			fmt.Printf("[rebalance] prometheus counter: before=%.0f after=%.0f moved=%d bytes\n",
				promBefore, promAfter, movedBytes)
		}
	}
	if measureMethod == "disk_delta" {
		movedBytes = after[newNodeInfo.ID] - before[newNodeInfo.ID]
		if movedBytes < 0 {
			movedBytes = 0
		}
	}

	fraction := 0.0
	if totalBytes > 0 {
		fraction = float64(movedBytes) / float64(totalBytes)
	}

	f, err := os.Create(filepath.Join(cfg.OutputDir, "rebalance.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"total_bytes", "moved_bytes", "fraction_moved", "duration_ms", "expected_fraction", "measure_method"}) //nolint:errcheck

	nodes, _ := listNodes(cfg.MetaAddr)
	expectedFraction := 1.0 / float64(len(nodes))
	w.Write([]string{
		fmt.Sprint(totalBytes),
		fmt.Sprint(movedBytes),
		fmt.Sprintf("%.4f", fraction),
		fmt.Sprint(rebalanceDur.Milliseconds()),
		fmt.Sprintf("%.4f", expectedFraction),
		measureMethod,
	}) //nolint:errcheck

	fmt.Printf("[rebalance] moved=%.1f MB / total=%.1f GB  fraction=%.2f%%  expected=%.2f%%  duration=%s\n",
		float64(movedBytes)/(1<<20), float64(totalBytes)/(1<<30),
		fraction*100, expectedFraction*100, rebalanceDur.Round(time.Millisecond))
	fmt.Println("[rebalance] done →", filepath.Join(cfg.OutputDir, "rebalance.csv"))
	return nil
}

// ---- shared HTTP helpers ---------------------------------------------------

type nodeInfo struct {
	ID         string `json:"id"`
	Addr       string `json:"addr"`
	UsedBytes  int64  `json:"used_bytes"`
	TotalBytes int64  `json:"total_bytes"`
	Status     string `json:"status"`
}

// NodeInfo is exported for experiment callers.
type NodeInfo = nodeInfo

func listNodes(metaAddr string) ([]nodeInfo, error) {
	resp, err := http.Get(metaAddr + "/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var nodes []nodeInfo
	return nodes, json.NewDecoder(resp.Body).Decode(&nodes)
}

func registerNode(metaAddr string, n NodeInfo) error {
	body, _ := json.Marshal(n)
	resp, err := http.Post(metaAddr+"/nodes", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register node: %d %s", resp.StatusCode, b)
	}
	return nil
}

func nodeUsages(metaAddr string) (map[string]int64, error) {
	nodes, err := listNodes(metaAddr)
	if err != nil {
		return nil, err
	}
	m := make(map[string]int64, len(nodes))
	for _, n := range nodes {
		m[n.ID] = n.UsedBytes
	}
	return m, nil
}

type keyEntry struct {
	Key       string `json:"key"`
	TotalSize int64  `json:"total_size"`
	ChunkSize int64  `json:"chunk_size"`
	Fragments []struct {
		ID        string   `json:"id"`
		Index     int      `json:"index"`
		Size      int64    `json:"size"`
		NodeAddrs []string `json:"node_addrs"`
	} `json:"fragments"`
}

func PutValue(cfg Config, key string, data []byte) error {
	// Prepare plan.
	body, _ := json.Marshal(map[string]int64{"total_size": int64(len(data))})
	resp, err := http.Post(cfg.MetaAddr+"/keys/"+key+"/prepare", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("prepare %d: %s", resp.StatusCode, b)
	}
	var entry keyEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		return err
	}

	// Upload fragments.
	chunkSize := entry.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 8 << 20
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(entry.Fragments))
	for _, frag := range entry.Fragments {
		start := int64(frag.Index) * chunkSize
		end := start + frag.Size
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[start:end]
		if len(frag.NodeAddrs) == 0 {
			continue
		}
		wg.Add(1)
		go func(frag struct {
			ID        string   `json:"id"`
			Index     int      `json:"index"`
			Size      int64    `json:"size"`
			NodeAddrs []string `json:"node_addrs"`
		}, chunk []byte) {
			defer wg.Done()
			primary := frag.NodeAddrs[0]
			req, _ := http.NewRequest(http.MethodPut, primary+"/fragments/"+frag.ID, bytes.NewReader(chunk))
			if len(frag.NodeAddrs) > 1 {
				req.Header.Set("X-Replica-Addrs", strings.Join(frag.NodeAddrs[1:], ","))
			}
			req.Header.Set("X-Write-Quorum", fmt.Sprint(cfg.WriteQuorum))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errCh <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errCh <- fmt.Errorf("fragment PUT %d", resp.StatusCode)
			}
		}(frag, chunk)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}

	// Commit.
	resp2, err := http.Post(cfg.MetaAddr+"/keys/"+key+"/commit", "application/json", nil)
	if err != nil {
		return err
	}
	resp2.Body.Close()
	return nil
}

func GetValue(cfg Config, key string) ([]byte, error) {
	resp, err := http.Get(cfg.MetaAddr + "/keys/" + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key %s not found", key)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get key entry %d: %s", resp.StatusCode, b)
	}
	var entry keyEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		return nil, err
	}

	result := make([]byte, entry.TotalSize)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make([]error, len(entry.Fragments))

	for _, frag := range entry.Fragments {
		wg.Add(1)
		go func(frag struct {
			ID        string   `json:"id"`
			Index     int      `json:"index"`
			Size      int64    `json:"size"`
			NodeAddrs []string `json:"node_addrs"`
		}) {
			defer wg.Done()
			var data []byte
			var lastErr error
			for _, addr := range frag.NodeAddrs {
				r, err := http.Get(addr + "/fragments/" + frag.ID)
				if err != nil {
					lastErr = err
					continue
				}
				b, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || r.StatusCode != http.StatusOK {
					lastErr = fmt.Errorf("status %d", r.StatusCode)
					continue
				}
				data = b
				break
			}
			if data == nil {
				errs[frag.Index] = lastErr
				return
			}
			chunkSize := entry.ChunkSize
			if chunkSize <= 0 {
				chunkSize = 8 << 20
			}
			offset := int64(frag.Index) * chunkSize
			mu.Lock()
			copy(result[offset:], data)
			mu.Unlock()
		}(frag)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return result, nil
}

// ---- math helpers ----------------------------------------------------------

func stats(vals []float64) (mean, stddev float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	for _, v := range vals {
		mean += v
	}
	mean /= float64(len(vals))
	for _, v := range vals {
		d := v - mean
		stddev += d * d
	}
	stddev = math.Sqrt(stddev / float64(len(vals)))
	return
}

// Percentile returns the p-th percentile (0-100) of sorted values.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p / 100 * float64(len(sorted)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// SortedCopy returns a sorted copy of vals.
func SortedCopy(vals []float64) []float64 {
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	return cp
}

func randomBytes(n int64) []byte {
	b := make([]byte, n)
	rand.Read(b) //nolint:gosec
	return b
}
