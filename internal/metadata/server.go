package metadata

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/username918r818/fragkv/internal/metrics"
)

// Server exposes the metadata store over HTTP.
type Server struct {
	store *Store
	mux   *http.ServeMux
}

// NewServer creates the HTTP server wiring for a metadata store.
func NewServer(store *Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/keys/", s.handleKeys)
	s.mux.HandleFunc("/nodes", s.handleNodes)
	s.mux.HandleFunc("/nodes/", s.handleNodeByID)
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.Handle("/metrics", metrics.Handler())
}

// ---- /keys/{key}[/prepare|/commit] ----------------------------------------

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	// path: /keys/<key>[/prepare|/commit]
	path := strings.TrimPrefix(r.URL.Path, "/keys/")
	parts := strings.SplitN(path, "/", 2)
	key := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	if key == "" {
		// GET /keys → list
		if r.Method == http.MethodGet {
			jsonResp(w, s.store.ListKeys())
			return
		}
		http.NotFound(w, r)
		return
	}

	switch action {
	case "prepare":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			TotalSize int64 `json:"total_size"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		entry, err := s.store.PrepareKey(key, req.TotalSize)
		if err != nil {
			if errors.Is(err, ErrNotLeader) {
				w.Header().Set("X-Raft-Leader", s.store.LeaderAddr())
				http.Error(w, "not leader", http.StatusMisdirectedRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResp(w, entry)

	case "commit":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := s.store.CommitKey(key); err != nil {
			if errors.Is(err, ErrNotLeader) {
				w.Header().Set("X-Raft-Leader", s.store.LeaderAddr())
				http.Error(w, "not leader", http.StatusMisdirectedRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case "":
		switch r.Method {
		case http.MethodGet:
			e, err := s.store.GetKey(key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					http.NotFound(w, r)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			jsonResp(w, e)

		case http.MethodDelete:
			if err := s.store.DeleteKey(key); err != nil {
				if errors.Is(err, ErrNotLeader) {
					w.Header().Set("X-Raft-Leader", s.store.LeaderAddr())
					http.Error(w, "not leader", http.StatusMisdirectedRequest)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}

	default:
		http.NotFound(w, r)
	}
}

// ---- /nodes ----------------------------------------------------------------

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jsonResp(w, s.store.ListNodes())
	case http.MethodPost:
		var info NodeInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.store.RegisterNode(info); err != nil {
			if errors.Is(err, ErrNotLeader) {
				w.Header().Set("X-Raft-Leader", s.store.LeaderAddr())
				http.Error(w, "not leader", http.StatusMisdirectedRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Trigger rebalance so existing fragments migrate to the new node.
		s.store.TriggerRebalance()
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ---- /nodes/{id}[/heartbeat] -----------------------------------------------

func (s *Server) handleNodeByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/nodes/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	if action == "heartbeat" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UsedBytes  int64 `json:"used_bytes"`
			TotalBytes int64 `json:"total_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.store.Heartbeat(id, req.UsedBytes, req.TotalBytes); err != nil {
			if errors.Is(err, ErrNotLeader) {
				w.Header().Set("X-Raft-Leader", s.store.LeaderAddr())
				http.Error(w, "not leader", http.StatusMisdirectedRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// GET /nodes/{id}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n, err := s.store.GetNode(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResp(w, n)
}

// ---- /status ---------------------------------------------------------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	type statusResp struct {
		IsLeader   bool   `json:"is_leader"`
		LeaderAddr string `json:"leader_addr"`
		State      string `json:"state"`
		ServerTime string `json:"server_time"`
	}
	jsonResp(w, statusResp{
		IsLeader:   s.store.IsLeader(),
		LeaderAddr: s.store.LeaderAddr(),
		State:      s.store.raft.State().String(),
		ServerTime: time.Now().UTC().Format(time.RFC3339),
	})
}

// ---- helpers ---------------------------------------------------------------

func jsonResp(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
