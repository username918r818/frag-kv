package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/username918r818/fragkv/internal/metadata"
	"gopkg.in/yaml.v3"
)

type peerCfg struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
}

type config struct {
	NodeID            string        `yaml:"node_id"`
	Addr              string        `yaml:"addr"`
	DataDir           string        `yaml:"data_dir"`
	RaftAddr          string        `yaml:"raft_addr"`
	RaftAdvertiseAddr string        `yaml:"raft_advertise_addr"`
	Bootstrap         bool          `yaml:"bootstrap"`
	Peers             []peerCfg     `yaml:"peers"`
	ReplicationFactor int           `yaml:"replication_factor"`
	ChunkSize         int64         `yaml:"chunk_size_bytes"`
	PlacementStrategy string        `yaml:"placement_strategy"`
	DeadThreshold     time.Duration `yaml:"dead_threshold"`
}

func main() {
	cfgPath := "metad.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg := config{
		NodeID:            "meta-1",
		Addr:              ":9000",
		DataDir:           "/meta-data",
		RaftAddr:          ":9001",
		Bootstrap:         true,
		ReplicationFactor: 3,
		ChunkSize:         8 * 1024 * 1024,
		PlacementStrategy: "weighted",
		DeadThreshold:     15 * time.Second,
	}
	if data, err := os.ReadFile(cfgPath); err == nil {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("parse config: %v", err)
		}
	}

	peers := make([]metadata.Peer, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = metadata.Peer{ID: p.ID, RaftAddr: p.RaftAddr}
	}

	store, err := metadata.Open(metadata.Config{
		NodeID:            cfg.NodeID,
		RaftAddr:          cfg.RaftAddr,
		RaftAdvertiseAddr: cfg.RaftAdvertiseAddr,
		DataDir:           cfg.DataDir,
		Peers:             peers,
		Bootstrap:         cfg.Bootstrap,
		ReplicationFactor: cfg.ReplicationFactor,
		ChunkSize:         cfg.ChunkSize,
		PlacementStrategy: cfg.PlacementStrategy,
		DeadThreshold:     cfg.DeadThreshold,
	})
	if err != nil {
		log.Fatalf("open metadata store: %v", err)
	}
	defer store.Close()
	store.StartWorkers()

	srv := metadata.NewServer(store)
	log.Printf("metad %s listening on %s (raft: %s)", cfg.NodeID, cfg.Addr, cfg.RaftAddr)
	if err := http.ListenAndServe(cfg.Addr, srv); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
