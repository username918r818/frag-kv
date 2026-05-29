package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/username918r818/fragkv/internal/fragment"
	"github.com/username918r818/fragkv/internal/metrics"
)

var (
	metaAddr    string
	writeQuorum int
)

func main() {
	root := &cobra.Command{
		Use:   "kvctl",
		Short: "fragkv CLI client",
	}
	root.PersistentFlags().StringVar(&metaAddr, "meta", "http://localhost:9000", "metadata service address")
	root.PersistentFlags().IntVar(&writeQuorum, "quorum", 2, "write quorum (W)")

	root.AddCommand(cmdPut(), cmdGet(), cmdDelete(), cmdList(), cmdStatus())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ---- put -------------------------------------------------------------------

func cmdPut() *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <file>",
		Short: "Upload a file",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, path := args[0], args[1]

			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			info, err := f.Stat()
			if err != nil {
				return err
			}

			start := time.Now()

			plan, err := preparePlan(key, info.Size())
			if err != nil {
				return fmt.Errorf("prepare: %w", err)
			}

			fmt.Printf("Uploading %q (%d bytes, %d fragments)\n", key, info.Size(), len(plan.Fragments))

			// Buffer all chunks first so we can fan them out in parallel.
			chunks := [][]byte{}
			if _, err := fragment.Split(key, f, plan.ChunkSize, func(_ int, data []byte) error {
				chunks = append(chunks, data)
				return nil
			}); err != nil {
				return fmt.Errorf("splitting: %w", err)
			}

			type result struct {
				index int
				err   error
			}
			results := make(chan result, len(plan.Fragments))

			for i, frag := range plan.Fragments {
				go func(i int, frag fragment.Info, data []byte) {
					results <- result{i, uploadFragment(frag, data, writeQuorum)}
				}(i, frag, chunks[i])
			}

			for range plan.Fragments {
				r := <-results
				if r.err != nil {
					return fmt.Errorf("fragment %d: %w", r.index, r.err)
				}
			}

			if err := commitKey(key); err != nil {
				return fmt.Errorf("commit: %w", err)
			}

			elapsed := time.Since(start)
			throughput := float64(info.Size()) / elapsed.Seconds() / (1 << 20)
			fmt.Printf("OK: %q stored (%d bytes) in %.2fs — %.1f MB/s\n", key, info.Size(), elapsed.Seconds(), throughput)

			metrics.PutDuration.WithLabelValues(metrics.SizeBucket(info.Size())).Observe(elapsed.Seconds())
			return nil
		},
	}
}

// ---- get -------------------------------------------------------------------

func cmdGet() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key> <outfile>",
		Short: "Download a value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, path := args[0], args[1]

			entry, err := getKeyEntry(key)
			if err != nil {
				return err
			}

			start := time.Now()

			out, err := os.Create(path)
			if err != nil {
				return err
			}
			defer out.Close()

			type fetched struct {
				index int
				data  []byte
				err   error
			}
			ch := make(chan fetched, len(entry.Fragments))
			for _, frag := range entry.Fragments {
				go func(frag fragment.Info) {
					data, err := downloadFragment(frag)
					ch <- fetched{frag.Index, data, err}
				}(frag)
			}

			fetches := make(map[int][]byte, len(entry.Fragments))
			for range entry.Fragments {
				f := <-ch
				if f.err != nil {
					return fmt.Errorf("fragment %d: %w", f.index, f.err)
				}
				fetches[f.index] = f.data
			}

			for i := 0; i < len(entry.Fragments); i++ {
				if _, err := out.Write(fetches[i]); err != nil {
					return err
				}
			}

			elapsed := time.Since(start)
			throughput := float64(entry.TotalSize) / elapsed.Seconds() / (1 << 20)
			fmt.Printf("OK: %q → %s (%d bytes) in %.2fs — %.1f MB/s\n", key, path, entry.TotalSize, elapsed.Seconds(), throughput)

			metrics.GetDuration.WithLabelValues(metrics.SizeBucket(entry.TotalSize)).Observe(elapsed.Seconds())
			return nil
		},
	}
}

// ---- delete ----------------------------------------------------------------

func cmdDelete() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <key>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodDelete, metaAddr+"/keys/"+args[0], nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("delete failed (%d): %s", resp.StatusCode, body)
			}
			fmt.Printf("deleted %q\n", args[0])
			return nil
		},
	}
}

// ---- list ------------------------------------------------------------------

func cmdList() *cobra.Command {
	return &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(metaAddr + "/keys/")
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var keys []string
			json.NewDecoder(resp.Body).Decode(&keys) //nolint:errcheck
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Println(k)
			}
			return nil
		},
	}
}

// ---- status ----------------------------------------------------------------

func cmdStatus() *cobra.Command {
	return &cobra.Command{
		Use: "status",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(metaAddr + "/nodes")
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var nodes []struct {
				ID         string    `json:"id"`
				Addr       string    `json:"addr"`
				UsedBytes  int64     `json:"used_bytes"`
				TotalBytes int64     `json:"total_bytes"`
				Status     string    `json:"status"`
				LastSeen   time.Time `json:"last_seen"`
			}
			json.NewDecoder(resp.Body).Decode(&nodes) //nolint:errcheck
			fmt.Println("Nodes:")
			for _, n := range nodes {
				pct := 0.0
				if n.TotalBytes > 0 {
					pct = float64(n.UsedBytes) / float64(n.TotalBytes) * 100
				}
				fmt.Printf("  %-12s  %-8s  used=%.1f%%  last_seen=%s\n",
					n.ID, n.Status, pct, n.LastSeen.Format(time.RFC3339))
			}
			return nil
		},
	}
}

// ---- HTTP helpers ----------------------------------------------------------

type keyEntry struct {
	Key       string          `json:"key"`
	TotalSize int64           `json:"total_size"`
	ChunkSize int64           `json:"chunk_size"`
	Fragments []fragment.Info `json:"fragments"`
}

func preparePlan(key string, size int64) (*keyEntry, error) {
	body, _ := json.Marshal(map[string]int64{"total_size": size})
	resp, err := http.Post(metaAddr+"/keys/"+key+"/prepare", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("metad %d: %s", resp.StatusCode, b)
	}
	var entry keyEntry
	return &entry, json.NewDecoder(resp.Body).Decode(&entry)
}

func commitKey(key string) error {
	resp, err := http.Post(metaAddr+"/keys/"+key+"/commit", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit %d: %s", resp.StatusCode, b)
	}
	return nil
}

func getKeyEntry(key string) (*keyEntry, error) {
	resp, err := http.Get(metaAddr + "/keys/" + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("key not found")
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("metad %d: %s", resp.StatusCode, b)
	}
	var entry keyEntry
	return &entry, json.NewDecoder(resp.Body).Decode(&entry)
}

// uploadFragment sends fragment to primary node; primary fans out to replicas via headers.
func uploadFragment(frag fragment.Info, data []byte, wQuorum int) error {
	if len(frag.NodeAddrs) == 0 {
		return errors.New("no target node addresses for fragment")
	}
	primaryAddr := frag.NodeAddrs[0]
	var replicaAddrs []string
	if len(frag.NodeAddrs) > 1 {
		replicaAddrs = frag.NodeAddrs[1:]
	}

	req, err := http.NewRequest(http.MethodPut, primaryAddr+"/fragments/"+frag.ID, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if len(replicaAddrs) > 0 {
		req.Header.Set("X-Replica-Addrs", strings.Join(replicaAddrs, ","))
	}
	req.Header.Set("X-Write-Quorum", fmt.Sprintf("%d", wQuorum))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("primary %s: %d %s", primaryAddr, resp.StatusCode, b)
	}
	return nil
}

// downloadFragment fetches a fragment, trying each replica addr.
func downloadFragment(frag fragment.Info) ([]byte, error) {
	var mu sync.Mutex
	var result []byte
	var lastErr error

	for _, addr := range frag.NodeAddrs {
		if addr == "" {
			continue
		}
		resp, err := http.Get(addr + "/fragments/" + frag.ID)
		if err != nil {
			mu.Lock()
			lastErr = err
			mu.Unlock()
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("node %s: status %d", addr, resp.StatusCode)
			continue
		}
		result = data
		break
	}

	if result == nil {
		return nil, fmt.Errorf("all replicas failed: %w", lastErr)
	}
	return result, nil
}
