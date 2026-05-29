// Package transport provides HTTP handlers for storage nodes.
package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/username918r818/fragkv/internal/metrics"
	"github.com/username918r818/fragkv/internal/storage"
)

// StoragedServer serves fragment read/write API for a single storage node.
type StoragedServer struct {
	store  *storage.Store
	nodeID string
	mux    *http.ServeMux
}

func NewStoragedServer(nodeID string, store *storage.Store) *StoragedServer {
	s := &StoragedServer{store: store, nodeID: nodeID, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *StoragedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *StoragedServer) routes() {
	s.mux.HandleFunc("/fragments/", s.handleFragment)
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.Handle("/metrics", metrics.Handler())
}

// PUT    /fragments/{id}            — write fragment; body may include X-Replica-Addrs header
// GET    /fragments/{id}            — read fragment (streaming)
// DELETE /fragments/{id}            — delete fragment
// POST   /fragments/{id}/replicate  — pull from another node
func (s *StoragedServer) handleFragment(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/fragments/")
	var id, action string
	if strings.HasSuffix(path, "/replicate") {
		id = strings.TrimSuffix(path, "/replicate")
		action = "replicate"
	} else {
		id = path
	}
	id = strings.TrimSuffix(id, "/")

	switch {
	case action == "replicate" && r.Method == http.MethodPost:
		s.replicateFrom(w, r, id)
	case r.Method == http.MethodPut:
		s.putFragment(w, r, id)
	case r.Method == http.MethodGet:
		s.getFragment(w, r, id)
	case r.Method == http.MethodDelete:
		s.deleteFragment(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// putFragment stores the fragment locally, then fans out to replica addresses
// listed in the X-Replica-Addrs header (comma-separated HTTP base URLs).
// Write is acknowledged after W successful replicas (including self).
func (s *StoragedServer) putFragment(w http.ResponseWriter, r *http.Request, id string) {
	start := time.Now()

	// Buffer body so we can replicate it.
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.store.Put(id, bytes.NewReader(data)); err != nil {
		metrics.FragmentOpsTotal.WithLabelValues("put", s.nodeID, "error").Inc()
		http.Error(w, "storing fragment: "+err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.FragmentOpsTotal.WithLabelValues("put", s.nodeID, "ok").Inc()
	metrics.FragmentPutDuration.WithLabelValues(s.nodeID).Observe(time.Since(start).Seconds())

	// Fan-out replication to replica nodes.
	replicaHeader := r.Header.Get("X-Replica-Addrs")
	wQuorum := parseIntHeader(r.Header.Get("X-Write-Quorum"), 2)
	if replicaHeader != "" {
		replicas := strings.Split(replicaHeader, ",")
		successes := 1 // self already written
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, addr := range replicas {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				if err := sendFragment(addr, id, data); err == nil {
					mu.Lock()
					successes++
					mu.Unlock()
				}
			}(addr)
		}
		wg.Wait()
		if successes < wQuorum {
			http.Error(w, fmt.Sprintf("quorum not reached: %d/%d", successes, wQuorum), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *StoragedServer) getFragment(w http.ResponseWriter, r *http.Request, id string) {
	start := time.Now()
	rc, size, err := s.store.GetStream(id)
	if err != nil {
		if err == storage.ErrNotFound {
			metrics.FragmentOpsTotal.WithLabelValues("get", s.nodeID, "notfound").Inc()
			http.NotFound(w, r)
			return
		}
		metrics.FragmentOpsTotal.WithLabelValues("get", s.nodeID, "error").Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	metrics.FragmentOpsTotal.WithLabelValues("get", s.nodeID, "ok").Inc()
	metrics.FragmentGetDuration.WithLabelValues(s.nodeID).Observe(time.Since(start).Seconds())

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	io.Copy(w, rc) //nolint:errcheck
}

func (s *StoragedServer) deleteFragment(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.store.Delete(id); err != nil {
		metrics.FragmentOpsTotal.WithLabelValues("delete", s.nodeID, "error").Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.FragmentOpsTotal.WithLabelValues("delete", s.nodeID, "ok").Inc()
	w.WriteHeader(http.StatusNoContent)
}

// replicateFrom pulls a fragment from another node.
// Body JSON: {"source_url": "http://storage-2:8002"}
func (s *StoragedServer) replicateFrom(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		SourceURL string `json:"source_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.SourceURL == "" {
		http.Error(w, "source_url required", http.StatusBadRequest)
		return
	}

	resp, err := http.Get(req.SourceURL + "/fragments/" + id) //nolint:noctx
	if err != nil {
		http.Error(w, "fetching from source: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("source returned %d", resp.StatusCode), http.StatusBadGateway)
		return
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "reading source: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := s.store.Put(id, bytes.NewReader(data)); err != nil {
		http.Error(w, "storing: "+err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.FragmentOpsTotal.WithLabelValues("replicate", s.nodeID, "ok").Inc()
	metrics.RebalanceBytesTotal.WithLabelValues("received").Add(float64(len(data)))
	w.WriteHeader(http.StatusCreated)
}

// GET /health
func (s *StoragedServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	used, err := s.store.DiskUsage()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.NodeUsedBytes.WithLabelValues(s.nodeID).Set(float64(used))
	type healthResp struct {
		NodeID    string `json:"node_id"`
		UsedBytes int64  `json:"used_bytes"`
		Status    string `json:"status"`
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(healthResp{NodeID: s.nodeID, UsedBytes: used, Status: "ok"}) //nolint:errcheck
}

// sendFragment PUTs raw data to a replica node.
func sendFragment(addr, id string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, addr+"/fragments/"+id, bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("replica %s: status %d", addr, resp.StatusCode)
	}
	return nil
}

func parseIntHeader(s string, def int) int {
	if s == "" {
		return def
	}
	var n int
	fmt.Sscanf(s, "%d", &n)
	if n <= 0 {
		return def
	}
	return n
}
