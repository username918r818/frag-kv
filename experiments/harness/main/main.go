// Runs experiments against a live fragkv cluster and writes CSV results.
//
// Usage:
//
//	bench --meta http://meta-1:9000 --scenario throughput --keys 10
//	bench --meta http://meta-1:9000 --scenario loadbalance --keys 50 --size 10
//	bench --meta http://meta-1:9000 --scenario availability --keys 20 --size 10
//	bench --meta http://meta-1:9000 --scenario rebalance --keys 20 --size 10 --new-node-id storage-6 --new-node-addr storage-6:8006
//	bench --meta http://meta-1:9000 --scenario all
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/username918r818/fragkv/experiments/harness"
)

func main() {
	meta := flag.String("meta", "http://localhost:9000", "metadata service address")
	prom := flag.String("prometheus", "http://prometheus:9090", "Prometheus address")
	out := flag.String("out", "experiments/results", "output directory for CSV files")
	scenario := flag.String("scenario", "all", "throughput|loadbalance|availability|rebalance|all")
	numKeys := flag.Int("keys", 10, "number of keys per scenario")
	sizeMB := flag.Int("size", 10, "value size in MB")
	workers := flag.Int("workers", 4, "parallel goroutines")
	writeQuorum := flag.Int("quorum", 2, "write quorum W")
	newNodeID := flag.String("new-node-id", "storage-6", "new node ID for rebalance experiment")
	newNodeAddr := flag.String("new-node-addr", "storage-6:8006", "new node addr (host:port) for rebalance experiment")
	newNodeBytes := flag.Int64("new-node-bytes", 10*1024*1024*1024, "new node total bytes")
	seed := flag.Int64("seed", 0, "random seed for chaos monkey (0 = use current time)")
	flag.Parse()

	cfg := harness.Config{
		MetaAddr:       *meta,
		PrometheusAddr: *prom,
		OutputDir:      *out,
		ValueSizes:     []int64{1 << 20, 10 << 20, 100 << 20, 500 << 20},
		NumValues:      *numKeys,
		Workers:        *workers,
		WriteQuorum:    *writeQuorum,
	}
	size := int64(*sizeMB) << 20

	switch *scenario {
	case "throughput":
		log.Println("=== Experiment 1: Throughput & Latency ===")
		must(harness.RunThroughput(cfg))

	case "loadbalance":
		log.Println("=== Experiment 3: Load Balance ===")
		must(harness.RunLoadBalance(cfg, size, *numKeys))

	case "availability":
		log.Println("=== Experiment 2a: Availability (последовательные отказы) ===")
		must(runAvailabilityToFile(cfg, size, *numKeys, "availability.csv"))

	case "availability-sim":
		log.Println("=== Experiment 2b: Availability (2 одновременных отказа) ===")
		must(runAvailabilityToFile(cfg, size, *numKeys, "availability_sim.csv"))

	case "rebalance":
		log.Println("=== Experiment 4: Rebalance ===")
		newNode := harness.NodeInfo{
			ID:         *newNodeID,
			Addr:       *newNodeAddr,
			TotalBytes: *newNodeBytes,
			Status:     "up",
		}
		// Fill data first, then register node and measure.
		log.Printf("[rebalance] filling %d keys (%d MB each)...", *numKeys, *sizeMB)
		must(harness.RunLoadBalance(cfg, size, *numKeys))
		must(harness.MeasureRebalance(cfg, size, *numKeys, newNode))

	case "rolling":
		log.Println("=== Experiment 5: Rolling Failure (каждый узел хоть раз упал) ===")
		must(runRolling(cfg, size, *numKeys))

	case "chaos":
		log.Println("=== Experiment 6: Chaos Monkey ===")
		chaosSeed := *seed
		if chaosSeed == 0 {
			chaosSeed = time.Now().UnixNano()
		}
		containers := []string{
			"deploy-storage-1-1",
			"deploy-storage-2-1",
			"deploy-storage-3-1",
			"deploy-storage-4-1",
			"deploy-storage-5-1",
		}
		must(runChaos(cfg, size, *numKeys, chaosSeed, containers))

	case "all":
		log.Println("=== Experiment 1: Throughput & Latency ===")
		must(harness.RunThroughput(cfg))
		log.Println("=== Experiment 3: Load Balance ===")
		must(harness.RunLoadBalance(cfg, size, *numKeys))
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// runRolling writes keys, then measures reads for 5 × 50s = 250 seconds.
// The bash script kills each storage node on schedule (one per 50s window).
// Each node is guaranteed to be down at least once during the test.
func runRolling(cfg harness.Config, size int64, numKeys int) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	keys := make([]string, numKeys)
	data := make([]byte, size)
	fmt.Printf("[rolling] writing %d keys (size=%d MB)…\n", numKeys, size>>20)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("rolling-%d-%d", size, i)
		if err := harness.PutValue(cfg, keys[i], data); err != nil {
			return fmt.Errorf("put %s: %w", keys[i], err)
		}
	}
	fmt.Println("[rolling] data written. Bash script will now kill nodes one by one.")

	f, err := os.Create(cfg.OutputDir + "/rolling.csv")
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"elapsed_sec", "success", "fail", "availability_pct"}) //nolint:errcheck

	// 5 nodes × 50 seconds each = 250 seconds total measurement window.
	const totalDuration = 250 * time.Second
	start := time.Now()
	for time.Since(start) < totalDuration {
		var success, fail int64
		for _, k := range keys {
			if _, err := harness.GetValue(cfg, k); err != nil {
				atomic.AddInt64(&fail, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
		}
		total := success + fail
		pct := 100.0 * float64(success) / float64(total)
		elapsed := int(time.Since(start).Seconds())
		w.Write([]string{fmt.Sprint(elapsed), fmt.Sprint(success), fmt.Sprint(fail), fmt.Sprintf("%.1f", pct)}) //nolint:errcheck
		w.Flush()
		fmt.Printf("[rolling] t=%3ds  success=%.1f%%  (%d/%d)\n", elapsed, pct, success, total)
		time.Sleep(5 * time.Second)
	}
	fmt.Printf("[rolling] done → %s/rolling.csv\n", cfg.OutputDir)
	return nil
}

// runAvailabilityToFile writes keys, then measures reads for 90s, saving to outFile.
// Kill storage nodes externally (bash script) while this runs.
func runAvailabilityToFile(cfg harness.Config, size int64, numKeys int, outFile string) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	keys := make([]string, numKeys)
	data := make([]byte, size)
	label := outFile[:len(outFile)-4] // strip .csv for log prefix
	fmt.Printf("[%s] writing %d keys (size=%d MB)…\n", label, numKeys, size>>20)
	actualKeys := keys[:0]
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("%s-%d-%d", label, size, i)
		if err := harness.PutValue(cfg, k, data); err == nil {
			actualKeys = append(actualKeys, k)
		}
	}
	keys = actualKeys
	fmt.Printf("[%s] %d/%d keys written. Kill storage nodes now.\n", label, len(keys), numKeys)

	f, err := os.Create(cfg.OutputDir + "/" + outFile)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"elapsed_sec", "success", "fail", "availability_pct"}) //nolint:errcheck

	start := time.Now()
	for time.Since(start) < 90*time.Second {
		var success, fail int64
		for _, k := range keys {
			if _, err := harness.GetValue(cfg, k); err != nil {
				atomic.AddInt64(&fail, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
		}
		total := success + fail
		pct := 100.0 * float64(success) / float64(total)
		elapsed := int(time.Since(start).Seconds())
		w.Write([]string{fmt.Sprint(elapsed), fmt.Sprint(success), fmt.Sprint(fail), fmt.Sprintf("%.1f", pct)}) //nolint:errcheck
		w.Flush()
		fmt.Printf("[%s] t=%3ds  success=%.1f%%  (%d/%d)\n", label, elapsed, pct, success, total)
		time.Sleep(5 * time.Second)
	}
	fmt.Printf("[%s] done → %s/%s\n", label, cfg.OutputDir, outFile)
	return nil
}
