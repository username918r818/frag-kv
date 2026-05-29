package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/username918r818/fragkv/experiments/harness"
)

type chaosEvent struct {
	ElapsedSec int    `json:"elapsed_sec"`
	Node       string `json:"node"`
	Action     string `json:"action"`
}

func runChaos(cfg harness.Config, size int64, numKeys int, seed int64, containers []string) error {
	const duration = 300 * time.Second

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(seed))
	start := time.Now()
	elapsed := func() int { return int(time.Since(start).Seconds()) }

	// Pre-load
	var committedMu sync.Mutex
	committed := make([]string, 0, numKeys*4)
	data := make([]byte, size)

	fmt.Printf("[chaos] pre-loading %d keys (size=%d MB, seed=%d)…\n", numKeys, size>>20, seed)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chaos-pre-%d", i)
		if err := harness.PutValue(cfg, key, data); err == nil {
			committedMu.Lock()
			committed = append(committed, key)
			committedMu.Unlock()
		}
	}
	fmt.Printf("[chaos] pre-loaded %d keys. Starting chaos…\n", len(committed))
	time.Sleep(2 * time.Second)

	var events []chaosEvent
	var eventsMu sync.Mutex
	nodeStatus := make(map[string]bool)
	for _, c := range containers {
		nodeStatus[c] = true
	}

	stop := make(chan struct{})

	// Chaos goroutine
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			time.Sleep(time.Duration(5+rng.Intn(18)) * time.Second)
			select {
			case <-stop:
				return
			default:
			}
			downCount := 0
			for _, up := range nodeStatus {
				if !up {
					downCount++
				}
			}
			if downCount == 0 {
				node := containers[rng.Intn(len(containers))]
				if exec.Command("docker", "stop", node).Run() == nil {
					nodeStatus[node] = false
					t := elapsed()
					fmt.Printf("[chaos] t=%3ds  KILL %s\n", t, node)
					eventsMu.Lock()
					events = append(events, chaosEvent{t, node, "kill"})
					eventsMu.Unlock()
				}
			} else {
				for node, up := range nodeStatus {
					if !up {
						if exec.Command("docker", "start", node).Run() == nil {
							nodeStatus[node] = true
							t := elapsed()
							fmt.Printf("[chaos] t=%3ds  RESTORE %s\n", t, node)
							eventsMu.Lock()
							events = append(events, chaosEvent{t, node, "restore"})
							eventsMu.Unlock()
						}
						break
					}
				}
			}
		}
	}()

	// Writer goroutine
	var writeOK, writeFail int64
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("chaos-live-%d-%d", seed, i)
			if err := harness.PutValue(cfg, key, data); err == nil {
				atomic.AddInt64(&writeOK, 1)
				committedMu.Lock()
				committed = append(committed, key)
				committedMu.Unlock()
			} else {
				atomic.AddInt64(&writeFail, 1)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// Measurement loop — per-interval stats (delta), not cumulative
	csvFile, err := os.Create(filepath.Join(cfg.OutputDir, "chaos.csv"))
	if err != nil {
		return err
	}
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	defer w.Flush()
	w.Write([]string{"elapsed_sec", "read_ok", "read_fail", "read_avail_pct", "write_ok", "write_fail", "write_ok_pct"}) //nolint:errcheck

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(duration) // FIX: один таймер на весь тест, не пересоздаётся
	defer deadline.Stop()

	var prevWriteOK, prevWriteFail int64

	for {
		select {
		case <-deadline.C:
			goto done
		case <-ticker.C:
			committedMu.Lock()
			snap := make([]string, len(committed))
			copy(snap, committed)
			committedMu.Unlock()

			if len(snap) > 20 {
				r2 := rand.New(rand.NewSource(time.Now().UnixNano()))
				r2.Shuffle(len(snap), func(i, j int) { snap[i], snap[j] = snap[j], snap[i] })
				snap = snap[:20]
			}

			var readOK, readFail int64
			for _, k := range snap {
				if _, err := harness.GetValue(cfg, k); err == nil {
					readOK++
				} else {
					readFail++
				}
			}

			curOK := atomic.LoadInt64(&writeOK)
			curFail := atomic.LoadInt64(&writeFail)
			dOK := curOK - prevWriteOK
			dFail := curFail - prevWriteFail
			prevWriteOK, prevWriteFail = curOK, curFail

			total := readOK + readFail
			readPct, writePct := 0.0, 0.0
			if total > 0 {
				readPct = 100 * float64(readOK) / float64(total)
			}
			if dOK+dFail > 0 {
				writePct = 100 * float64(dOK) / float64(dOK+dFail)
			}

			t := elapsed()
			fmt.Printf("[chaos] t=%3ds  reads=%.0f%%(%d/%d)  writes=%.0f%%(%d/%d)\n",
				t, readPct, readOK, total, writePct, dOK, dOK+dFail)
			w.Write([]string{fmt.Sprint(t),
				fmt.Sprint(readOK), fmt.Sprint(readFail), fmt.Sprintf("%.1f", readPct),
				fmt.Sprint(dOK), fmt.Sprint(dFail), fmt.Sprintf("%.1f", writePct),
			}) //nolint:errcheck
			w.Flush()
		}
	}

done:
	close(stop)
	for node, up := range nodeStatus {
		if !up {
			exec.Command("docker", "start", node).Run() //nolint:errcheck
		}
	}
	time.Sleep(5 * time.Second)

	// Integrity check
	committedMu.Lock()
	allKeys := make([]string, len(committed))
	copy(allKeys, committed)
	committedMu.Unlock()

	fmt.Printf("[chaos] integrity check: %d committed keys…\n", len(allKeys))
	intOK := 0
	for _, k := range allKeys {
		if _, err := harness.GetValue(cfg, k); err == nil {
			intOK++
		}
	}
	intPct := 100.0 * float64(intOK) / float64(len(allKeys))
	fmt.Printf("[chaos] integrity: %.1f%% (%d/%d)\n", intPct, intOK, len(allKeys))

	eventsMu.Lock()
	type summary struct {
		Events       []chaosEvent `json:"events"`
		IntegrityPct float64      `json:"integrity_pct"`
		IntegrityOK  int          `json:"integrity_ok"`
		IntegrityAll int          `json:"integrity_all"`
	}
	j, _ := json.MarshalIndent(summary{events, intPct, intOK, len(allKeys)}, "", "  ")
	eventsMu.Unlock()
	os.WriteFile(filepath.Join(cfg.OutputDir, "chaos_events.json"), j, 0o644) //nolint:errcheck

	fmt.Println("[chaos] done → chaos.csv + chaos_events.json")
	return nil
}
