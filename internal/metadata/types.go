package metadata

import (
	"time"

	"github.com/username918r818/fragkv/internal/fragment"
	"github.com/username918r818/fragkv/internal/placement"
)

// NodeStatus describes liveness of a storage node.
type NodeStatus string

const (
	NodeUp       NodeStatus = "up"
	NodeDown     NodeStatus = "down"
	NodeDraining NodeStatus = "draining"
)

// NodeInfo describes a registered storage node.
type NodeInfo struct {
	ID         string     `json:"id"`
	Addr       string     `json:"addr"` // "host:port" for HTTP API
	TotalBytes int64      `json:"total_bytes"`
	UsedBytes  int64      `json:"used_bytes"`
	LastSeen   time.Time  `json:"last_seen"`
	Status     NodeStatus `json:"status"`
}

func (n NodeInfo) FreeBytes() int64 {
	free := n.TotalBytes - n.UsedBytes
	if free < 0 {
		return 0
	}
	return free
}

func (n NodeInfo) ToPlacementNode() placement.Node {
	return placement.Node{
		ID:         n.ID,
		FreeBytes:  n.FreeBytes(),
		TotalBytes: n.TotalBytes,
	}
}

// KeyEntry holds the fragment catalog for one key.
type KeyEntry struct {
	Key       string          `json:"key"`
	TotalSize int64           `json:"total_size"`
	ChunkSize int64           `json:"chunk_size"`
	Fragments []fragment.Info `json:"fragments"`
	CreatedAt time.Time       `json:"created_at"`
	Committed bool            `json:"committed"`
}

// state is the in-memory FSM state, serialized for snapshots.
type state struct {
	Keys  map[string]*KeyEntry `json:"keys"`
	Nodes map[string]*NodeInfo `json:"nodes"`
}

func newState() *state {
	return &state{
		Keys:  make(map[string]*KeyEntry),
		Nodes: make(map[string]*NodeInfo),
	}
}

// ---- Raft commands ---------------------------------------------------------

type cmdType string

const (
	cmdPutKey         cmdType = "put_key"
	cmdCommitKey      cmdType = "commit_key"
	cmdDeleteKey      cmdType = "delete_key"
	cmdRegisterNode   cmdType = "register_node"
	cmdHeartbeat      cmdType = "heartbeat"
	cmdMarkNodeDown   cmdType = "mark_node_down"
	cmdUpdateFragment cmdType = "update_fragment" // update NodeIDs after rebalance/recovery
)

type command struct {
	Type cmdType `json:"type"`

	// cmdPutKey
	Entry *KeyEntry `json:"entry,omitempty"`

	// cmdCommitKey / cmdDeleteKey
	Key string `json:"key,omitempty"`

	// cmdRegisterNode / cmdHeartbeat / cmdMarkNodeDown
	Node *NodeInfo `json:"node,omitempty"`

	// cmdUpdateFragment
	FragmentID string   `json:"fragment_id,omitempty"`
	NodeIDs    []string `json:"node_ids,omitempty"`
}
