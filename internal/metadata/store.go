// Package metadata implements the Raft-replicated metadata service:
// key catalog, node membership, placement coordination.
package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/username918r818/fragkv/internal/fragment"
	"github.com/username918r818/fragkv/internal/placement"
)

var (
	ErrNotLeader = errors.New("not the Raft leader")
	ErrNotFound  = errors.New("key not found")
)

const raftTimeout = 5 * time.Second

// Config holds configuration for a metadata store node.
type Config struct {
	NodeID            string
	RaftAddr          string // TCP listen address, e.g. ":9001"
	RaftAdvertiseAddr string // address advertised to peers, e.g. "meta-1:9001"; defaults to RaftAddr
	DataDir           string
	Peers             []Peer // other nodes in the Raft cluster
	Bootstrap         bool   // true for the very first node in a new cluster

	ReplicationFactor int
	ChunkSize         int64
	PlacementStrategy string // "consistent_hash" | "rendezvous" | "weighted"
	DeadThreshold     time.Duration
}

// Peer is another Raft node.
type Peer struct {
	ID       string
	RaftAddr string
}

// Store is the metadata service backed by Raft.
type Store struct {
	cfg    Config
	raft   *raft.Raft
	fsm    *fsm
	placer placement.Placer
}

// Open initialises the Raft node and returns a ready Store.
func Open(cfg Config) (*Store, error) {
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 8 * 1024 * 1024
	}
	if cfg.DeadThreshold == 0 {
		cfg.DeadThreshold = 15 * time.Second
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	placer := newPlacer(cfg.PlacementStrategy)

	machine := newFSM()

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("raft log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("raft stable store: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft snapshot store: %w", err)
	}

	advertise := cfg.RaftAdvertiseAddr
	if advertise == "" {
		advertise = cfg.RaftAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("resolve raft advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}

	r, err := raft.NewRaft(rc, machine, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		servers := []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(advertise),
		}}
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.RaftAddr),
			})
		}
		cf := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := cf.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	return &Store{cfg: cfg, raft: r, fsm: machine, placer: placer}, nil
}

// Close shuts down the Raft node.
func (s *Store) Close() error {
	return s.raft.Shutdown().Error()
}

// IsLeader reports whether this node is the current Raft leader.
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// LeaderAddr returns the current leader's Raft address.
func (s *Store) LeaderAddr() string {
	addr, _ := s.raft.LeaderWithID()
	return string(addr)
}

// ---- Key catalog operations ------------------------------------------------

// PrepareKey computes fragment placement for a new key and stores a pending entry.
// Returns the planned KeyEntry so the client knows where to send each fragment.
func (s *Store) PrepareKey(key string, totalSize int64) (*KeyEntry, error) {
	nodes := s.liveNodes()
	if len(nodes) == 0 {
		return nil, errors.New("no live storage nodes")
	}

	chunkSize := s.cfg.ChunkSize
	numFragments := int((totalSize + chunkSize - 1) / chunkSize)
	if numFragments == 0 {
		numFragments = 1
	}

	n := s.cfg.ReplicationFactor
	frags := make([]fragment.Info, numFragments)
	for i := 0; i < numFragments; i++ {
		fid := fragment.ID(key, i)
		size := chunkSize
		if i == numFragments-1 {
			rem := totalSize % chunkSize
			if rem > 0 {
				size = rem
			}
		}
		placed := s.placer.Place(fid, nodes, n)
		nodeIDs := make([]string, len(placed))
		nodeAddrs := make([]string, len(placed))
		for j, nd := range placed {
			nodeIDs[j] = nd.ID
			// Resolve HTTP addr from membership.
			if info, ok := s.fsm.getNode(nd.ID); ok {
				nodeAddrs[j] = "http://" + info.Addr
			}
		}
		frags[i] = fragment.Info{
			ID:        fid,
			Key:       key,
			Index:     i,
			Size:      size,
			NodeIDs:   nodeIDs,
			NodeAddrs: nodeAddrs,
		}
	}

	entry := &KeyEntry{
		Key:       key,
		TotalSize: totalSize,
		ChunkSize: chunkSize,
		Fragments: frags,
		CreatedAt: time.Now(),
		Committed: false,
	}

	if err := s.apply(command{Type: cmdPutKey, Entry: entry}); err != nil {
		return nil, err
	}
	return entry, nil
}

// CommitKey marks a key as fully written.
func (s *Store) CommitKey(key string) error {
	return s.apply(command{Type: cmdCommitKey, Key: key})
}

// GetKey returns the committed KeyEntry for key, or ErrNotFound.
func (s *Store) GetKey(key string) (*KeyEntry, error) {
	e, ok := s.fsm.getKey(key)
	if !ok {
		return nil, ErrNotFound
	}
	return e, nil
}

// DeleteKey removes a key from the catalog.
func (s *Store) DeleteKey(key string) error {
	return s.apply(command{Type: cmdDeleteKey, Key: key})
}

// ListKeys returns all committed key names.
func (s *Store) ListKeys() []string {
	return s.fsm.listKeys()
}

// ---- Node membership -------------------------------------------------------

// RegisterNode adds or updates a storage node registration.
func (s *Store) RegisterNode(info NodeInfo) error {
	if info.Status == "" {
		info.Status = NodeUp
	}
	if info.LastSeen.IsZero() {
		info.LastSeen = time.Now()
	}
	return s.apply(command{Type: cmdRegisterNode, Node: &info})
}

// Heartbeat updates a node's last-seen time and disk stats.
func (s *Store) Heartbeat(id string, usedBytes, totalBytes int64) error {
	n := &NodeInfo{
		ID:         id,
		LastSeen:   time.Now(),
		UsedBytes:  usedBytes,
		TotalBytes: totalBytes,
	}
	return s.apply(command{Type: cmdHeartbeat, Node: n})
}

// GetNode returns node info or ErrNotFound.
func (s *Store) GetNode(id string) (*NodeInfo, error) {
	n, ok := s.fsm.getNode(id)
	if !ok {
		return nil, ErrNotFound
	}
	return n, nil
}

// ListNodes returns all registered nodes.
func (s *Store) ListNodes() []*NodeInfo {
	return s.fsm.listNodes()
}

// MarkNodeDown flags a node as down.
func (s *Store) MarkNodeDown(id string) error {
	n := &NodeInfo{ID: id}
	return s.apply(command{Type: cmdMarkNodeDown, Node: n})
}

// UpdateFragmentNodes updates the replica node list for a fragment (after recovery/rebalance).
func (s *Store) UpdateFragmentNodes(fragmentID string, nodeIDs []string) error {
	return s.apply(command{Type: cmdUpdateFragment, FragmentID: fragmentID, NodeIDs: nodeIDs})
}

// ---- internal --------------------------------------------------------------

func (s *Store) apply(cmd command) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// liveNodes returns placement.Node list for all UP storage nodes.
func (s *Store) liveNodes() []placement.Node {
	nodes := s.fsm.listNodes()
	out := make([]placement.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Status == NodeUp {
			out = append(out, n.ToPlacementNode())
		}
	}
	return out
}

func newPlacer(strategy string) placement.Placer {
	switch strategy {
	case "rendezvous":
		return &placement.Rendezvous{}
	case "weighted":
		return &placement.Weighted{}
	default: // "consistent_hash" or empty
		return &placement.ConsistentHash{}
	}
}
