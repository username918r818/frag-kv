package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// StartWorkers launches background goroutines for failure detection,
// recovery, and rebalance. Call once after Open().
func (s *Store) StartWorkers() {
	go s.failureDetectorLoop()
	go s.recoveryLoop()
}

// ---- failure detector ------------------------------------------------------

// failureDetectorLoop marks nodes as DOWN when heartbeats stop arriving.
func (s *Store) failureDetectorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !s.IsLeader() {
			continue
		}
		for _, n := range s.fsm.listNodes() {
			if n.Status != NodeUp {
				continue
			}
			if time.Since(n.LastSeen) > s.cfg.DeadThreshold {
				log.Printf("[recovery] marking node %s as DOWN (last seen %s ago)",
					n.ID, time.Since(n.LastSeen).Round(time.Second))
				if err := s.MarkNodeDown(n.ID); err != nil {
					log.Printf("[recovery] mark down %s: %v", n.ID, err)
				}
			}
		}
	}
}

// ---- recovery --------------------------------------------------------------

// recoveryLoop finds under-replicated fragments and triggers re-replication.
func (s *Store) recoveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !s.IsLeader() {
			continue
		}
		s.recoverUnderReplicated()
	}
}

func (s *Store) recoverUnderReplicated() {
	liveMap := map[string]*NodeInfo{}
	for _, n := range s.fsm.listNodes() {
		if n.Status == NodeUp {
			liveMap[n.ID] = n
		}
	}

	for _, entry := range s.fsm.allEntries() {
		for _, frag := range entry.Fragments {
			liveReplicas := filterLive(frag.NodeIDs, liveMap)
			if len(liveReplicas) >= s.cfg.ReplicationFactor {
				continue
			}
			// Need to add more replicas.
			needed := s.cfg.ReplicationFactor - len(liveReplicas)
			// Find candidates: live nodes not already holding a replica.
			candidates := []string{}
			for id := range liveMap {
				if !contains(liveReplicas, id) {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) == 0 || len(liveReplicas) == 0 {
				log.Printf("[recovery] fragment %s: %d live replicas, no recovery possible", frag.ID, len(liveReplicas))
				continue
			}

			sourceAddr := liveMap[liveReplicas[0]].Addr
			for i := 0; i < needed && i < len(candidates); i++ {
				target := liveMap[candidates[i]]
				log.Printf("[recovery] fragment %s: copy from %s to %s", frag.ID, sourceAddr, target.ID)
				if err := triggerReplicate(target.Addr, frag.ID, sourceAddr); err != nil {
					log.Printf("[recovery] replicate fragment %s to %s: %v", frag.ID, target.ID, err)
					continue
				}
				liveReplicas = append(liveReplicas, target.ID)
			}

			// Update the fragment's node list in the catalog.
			if err := s.UpdateFragmentNodes(frag.ID, liveReplicas); err != nil {
				log.Printf("[recovery] update fragment %s nodes: %v", frag.ID, err)
			}
		}
	}
}

// ---- rebalance -------------------------------------------------------------

// TriggerRebalance moves fragments to a newly added node.
// Call this after a new storage node has been registered.
func (s *Store) TriggerRebalance() {
	if !s.IsLeader() {
		return
	}
	go s.rebalance()
}

func (s *Store) rebalance() {
	log.Println("[rebalance] starting")
	liveNodes := s.liveNodes()
	if len(liveNodes) == 0 {
		return
	}

	moved := 0
	for _, entry := range s.fsm.allEntries() {
		for _, frag := range entry.Fragments {
			desired := s.placer.Place(frag.ID, liveNodes, s.cfg.ReplicationFactor)
			desiredIDs := make([]string, len(desired))
			for i, n := range desired {
				desiredIDs[i] = n.ID
			}

			// Find nodes in desired that don't yet have the fragment.
			liveMap := map[string]*NodeInfo{}
			for _, n := range s.fsm.listNodes() {
				if n.Status == NodeUp {
					liveMap[n.ID] = n
				}
			}
			currentSet := map[string]bool{}
			for _, id := range frag.NodeIDs {
				currentSet[id] = true
			}

			for _, id := range desiredIDs {
				if currentSet[id] {
					continue
				}
				// This fragment should now be on 'id' but isn't — copy it there.
				source := ""
				for _, cur := range frag.NodeIDs {
					if liveMap[cur] != nil {
						source = liveMap[cur].Addr
						break
					}
				}
				if source == "" {
					continue
				}
				target := liveMap[id]
				if target == nil {
					continue
				}
				if err := triggerReplicate(target.Addr, frag.ID, source); err != nil {
					log.Printf("[rebalance] copy fragment %s to %s: %v", frag.ID, id, err)
					continue
				}
				moved++
				currentSet[id] = true
			}

			// Update canonical node list.
			updated := make([]string, 0, len(desiredIDs))
			for _, id := range desiredIDs {
				if currentSet[id] {
					updated = append(updated, id)
				}
			}
			if err := s.UpdateFragmentNodes(frag.ID, updated); err != nil {
				log.Printf("[rebalance] update fragment nodes: %v", err)
			}
		}
	}
	log.Printf("[rebalance] done: moved %d fragments", moved)
}

// ---- helpers ---------------------------------------------------------------

func (f *fsm) allEntries() []*KeyEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*KeyEntry, 0, len(f.state.Keys))
	for _, e := range f.state.Keys {
		cp := *e
		out = append(out, &cp)
	}
	return out
}

func filterLive(nodeIDs []string, liveMap map[string]*NodeInfo) []string {
	var out []string
	for _, id := range nodeIDs {
		if liveMap[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// triggerReplicate asks targetAddr to pull a fragment from sourceAddr.
func triggerReplicate(targetAddr, fragmentID, sourceURL string) error {
	body, _ := json.Marshal(map[string]string{"source_url": "http://" + sourceURL})
	url := fmt.Sprintf("http://%s/fragments/%s/replicate", targetAddr, fragmentID)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
