// Package placement provides strategies for distributing fragments across storage nodes.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Node represents a storage node with capacity info.
type Node struct {
	ID         string
	FreeBytes  int64
	TotalBytes int64
	Weight     float64 // optional manual weight override; 0 means use FreeBytes ratio
}

// Placer selects n nodes for a given fragment.
type Placer interface {
	Place(fragmentID string, nodes []Node, n int) []Node
}

// ---- ConsistentHash --------------------------------------------------------

// ConsistentHash places fragments using a virtual-node consistent hash ring.
type ConsistentHash struct {
	VNodes int // virtual nodes per physical node; default 150
}

type ringEntry struct {
	hash   uint32
	nodeID string
}

func (c *ConsistentHash) Place(fragmentID string, nodes []Node, n int) []Node {
	vn := c.VNodes
	if vn <= 0 {
		vn = 150
	}
	if n > len(nodes) {
		n = len(nodes)
	}

	ring := make([]ringEntry, 0, len(nodes)*vn)
	for _, nd := range nodes {
		for i := 0; i < vn; i++ {
			key := fmt.Sprintf("%s#%d", nd.ID, i)
			ring = append(ring, ringEntry{hash: hash32(key), nodeID: nd.ID})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })

	fh := hash32(fragmentID)
	start := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= fh })

	seen := map[string]bool{}
	result := make([]Node, 0, n)
	nodeMap := nodeByID(nodes)

	for i := 0; len(result) < n; i++ {
		idx := (start + i) % len(ring)
		nid := ring[idx].nodeID
		if !seen[nid] {
			seen[nid] = true
			result = append(result, nodeMap[nid])
		}
		if i >= len(ring) {
			break
		}
	}
	return result
}

// ---- Rendezvous ------------------------------------------------------------

// Rendezvous uses highest random weight (HRW) hashing.
// When a node is added, only ~1/N fragments move — optimal redistribution.
type Rendezvous struct{}

func (r *Rendezvous) Place(fragmentID string, nodes []Node, n int) []Node {
	if n > len(nodes) {
		n = len(nodes)
	}
	type scored struct {
		score float64
		node  Node
	}
	scores := make([]scored, len(nodes))
	for i, nd := range nodes {
		h := hash64(nd.ID + "|" + fragmentID)
		// convert to [0,1) float
		score := float64(h) / float64(math.MaxUint64)
		scores[i] = scored{score, nd}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	result := make([]Node, n)
	for i := 0; i < n; i++ {
		result[i] = scores[i].node
	}
	return result
}

// ---- Weighted --------------------------------------------------------------

// Weighted prefers nodes with more free space while still using hashing for
// tie-breaking. Useful when nodes have different capacities or fill rates.
type Weighted struct{}

func (w *Weighted) Place(fragmentID string, nodes []Node, n int) []Node {
	if n > len(nodes) {
		n = len(nodes)
	}
	type scored struct {
		score float64
		node  Node
	}

	var totalFree int64
	for _, nd := range nodes {
		totalFree += nd.FreeBytes
	}

	scores := make([]scored, len(nodes))
	for i, nd := range nodes {
		var weightFactor float64
		if nd.Weight > 0 {
			weightFactor = nd.Weight
		} else if totalFree > 0 {
			weightFactor = float64(nd.FreeBytes) / float64(totalFree)
		} else {
			weightFactor = 1.0 / float64(len(nodes))
		}
		// combine capacity weight with a random component (rendezvous-style)
		h := hash64(nd.ID + "|" + fragmentID)
		randComponent := float64(h) / float64(math.MaxUint64)
		// geometric combination: higher weight nodes get meaningfully better scores
		scores[i] = scored{score: weightFactor * (0.5 + 0.5*randComponent), node: nd}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	result := make([]Node, n)
	for i := 0; i < n; i++ {
		result[i] = scores[i].node
	}
	return result
}

// ---- helpers ---------------------------------------------------------------

func hash32(s string) uint32 {
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint32(h[:4])
}

func hash64(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

func nodeByID(nodes []Node) map[string]Node {
	m := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		m[n.ID] = n
	}
	return m
}
