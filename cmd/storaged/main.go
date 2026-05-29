package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/username918r818/fragkv/internal/metrics"
	"github.com/username918r818/fragkv/internal/storage"
	"github.com/username918r818/fragkv/internal/transport"
	"gopkg.in/yaml.v3"
)

type config struct {
	NodeID            string        `yaml:"node_id"`
	Addr              string        `yaml:"addr"`        // listen address, e.g. ":8001"
	PublicAddr        string        `yaml:"public_addr"` // address others use to reach this node, e.g. "storage-1:8001"
	DataDir           string        `yaml:"data_dir"`
	MetaAddr          string        `yaml:"meta_addr"`
	TotalBytes        int64         `yaml:"total_bytes"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

func main() {
	cfgPath := "storaged.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg := config{
		NodeID:            "storage-1",
		Addr:              ":8001",
		PublicAddr:        "localhost:8001",
		DataDir:           "/data",
		MetaAddr:          "http://localhost:9000",
		TotalBytes:        10 * 1024 * 1024 * 1024,
		HeartbeatInterval: 5 * time.Second,
	}
	if data, err := os.ReadFile(cfgPath); err == nil {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("parse config: %v", err)
		}
	}
	// Fall back: if public_addr not set, derive from addr (strip leading colon).
	if cfg.PublicAddr == "" {
		cfg.PublicAddr = "localhost" + cfg.Addr
	}

	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	go registerNode(cfg)
	go heartbeatLoop(cfg, store)

	srv := transport.NewStoragedServer(cfg.NodeID, store)
	log.Printf("storaged %s listening on %s (public %s)", cfg.NodeID, cfg.Addr, cfg.PublicAddr)
	if err := http.ListenAndServe(cfg.Addr, srv); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func registerNode(cfg config) {
	type nodeInfo struct {
		ID         string `json:"id"`
		Addr       string `json:"addr"` // public addr used by other nodes + kvctl
		TotalBytes int64  `json:"total_bytes"`
		Status     string `json:"status"`
	}
	body, _ := json.Marshal(nodeInfo{
		ID:         cfg.NodeID,
		Addr:       cfg.PublicAddr,
		TotalBytes: cfg.TotalBytes,
		Status:     "up",
	})
	for {
		resp, err := http.Post(cfg.MetaAddr+"/nodes", "application/json", bytes.NewReader(body))
		if err == nil && resp.StatusCode == http.StatusCreated {
			resp.Body.Close()
			log.Printf("registered with metadata service at %s", cfg.MetaAddr)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Printf("waiting for metadata service at %s: %v", cfg.MetaAddr, err)
		time.Sleep(2 * time.Second)
	}
}

func heartbeatLoop(cfg config, store *storage.Store) {
	ticker := time.NewTicker(cfg.HeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		used, err := store.DiskUsage()
		if err != nil {
			continue
		}
		metrics.NodeUsedBytes.WithLabelValues(cfg.NodeID).Set(float64(used))
		body, _ := json.Marshal(map[string]int64{
			"used_bytes":  used,
			"total_bytes": cfg.TotalBytes,
		})
		url := fmt.Sprintf("%s/nodes/%s/heartbeat", cfg.MetaAddr, cfg.NodeID)
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("heartbeat failed: %v", err)
			continue
		}
		resp.Body.Close()
	}
}
