package metadata

import (
	"testing"
	"time"

	"github.com/username918r818/fragkv/internal/fragment"
	"github.com/username918r818/fragkv/internal/placement"
)

// TestPrepareKeyPlacement verifies that PrepareKey distributes fragments
// across nodes according to the configured placement strategy.
// Uses the FSM directly (no Raft), simulating the Store internals.
func TestPrepareKeyPlacement(t *testing.T) {
	f := newFSM()

	// Register 5 nodes.
	for i := 1; i <= 5; i++ {
		n := &NodeInfo{
			ID:         nodeID(i),
			Addr:       nodeAddr(i),
			TotalBytes: 100 << 30,
			Status:     NodeUp,
			LastSeen:   time.Now(),
		}
		applyCmd(f, command{Type: cmdRegisterNode, Node: n})
	}

	// Build a store with the FSM but without Raft (use placer directly).
	placer := newPlacer("rendezvous")
	cfg := Config{ReplicationFactor: 3, ChunkSize: 8 << 20}

	// Simulate PrepareKey logic.
	const valueSize = 25 << 20 // 25 МБ → 4 fragments at 8 МБ
	numFragments := int((valueSize + cfg.ChunkSize - 1) / cfg.ChunkSize)
	if numFragments == 0 {
		numFragments = 1
	}

	nodes := []placement.Node{}
	for _, n := range f.listNodes() {
		if n.Status == NodeUp {
			nodes = append(nodes, n.ToPlacementNode())
		}
	}

	frags := make([]fragment.Info, numFragments)
	nodeUsage := map[string]int{}
	for i := 0; i < numFragments; i++ {
		fid := fragment.ID("testkey", i)
		placed := placer.Place(fid, nodes, cfg.ReplicationFactor)
		ids := make([]string, len(placed))
		for j, nd := range placed {
			ids[j] = nd.ID
			nodeUsage[nd.ID]++
		}
		frags[i] = fragment.Info{ID: fid, Key: "testkey", Index: i, NodeIDs: ids}
	}

	t.Logf("Fragment→node assignment: %v", frags)
	t.Logf("Node replica counts: %v", nodeUsage)

	// Each fragment must have exactly ReplicationFactor unique nodes.
	for _, fr := range frags {
		if len(fr.NodeIDs) != cfg.ReplicationFactor {
			t.Errorf("fragment %s: got %d replicas, want %d", fr.ID, len(fr.NodeIDs), cfg.ReplicationFactor)
		}
		seen := map[string]bool{}
		for _, id := range fr.NodeIDs {
			if seen[id] {
				t.Errorf("fragment %s: duplicate node %s", fr.ID, id)
			}
			seen[id] = true
		}
	}

	// Each node should host at least 1 replica (with 5 nodes, 4 frags × 3 replicas = 12 placements).
	for i := 1; i <= 5; i++ {
		if nodeUsage[nodeID(i)] == 0 {
			t.Logf("WARN: node %s got 0 replicas (possible with small fragment count)", nodeID(i))
		}
	}
}

func TestNodeToPlacementNode(t *testing.T) {
	n := NodeInfo{
		ID:         "n1",
		Addr:       "n1:8001",
		TotalBytes: 100 << 30,
		UsedBytes:  30 << 30,
		Status:     NodeUp,
	}
	pn := n.ToPlacementNode()
	if pn.ID != "n1" {
		t.Errorf("ID mismatch")
	}
	if pn.FreeBytes != 70<<30 {
		t.Errorf("FreeBytes = %d, want %d", pn.FreeBytes, 70<<30)
	}
}

func TestNodeFreeBytes_NeverNegative(t *testing.T) {
	n := NodeInfo{TotalBytes: 10, UsedBytes: 50}
	if n.FreeBytes() != 0 {
		t.Errorf("FreeBytes should clamp to 0, got %d", n.FreeBytes())
	}
}

func nodeID(i int) string   { return "storage-" + string(rune('0'+i)) }
func nodeAddr(i int) string { return "storage-" + string(rune('0'+i)) + ":800" + string(rune('0'+i)) }
