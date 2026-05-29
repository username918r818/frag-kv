package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// fsm implements raft.FSM. It holds all mutable catalog state.
type fsm struct {
	mu    sync.RWMutex
	state *state
}

func newFSM() *fsm {
	return &fsm{state: newState()}
}

// Apply is called by Raft to apply a committed log entry to the FSM.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case cmdPutKey:
		if cmd.Entry != nil {
			f.state.Keys[cmd.Entry.Key] = cmd.Entry
		}
	case cmdCommitKey:
		if e, ok := f.state.Keys[cmd.Key]; ok {
			e.Committed = true
		}
	case cmdDeleteKey:
		delete(f.state.Keys, cmd.Key)
	case cmdRegisterNode:
		if cmd.Node != nil {
			f.state.Nodes[cmd.Node.ID] = cmd.Node
		}
	case cmdHeartbeat:
		if cmd.Node != nil {
			if existing, ok := f.state.Nodes[cmd.Node.ID]; ok {
				existing.LastSeen = cmd.Node.LastSeen
				existing.UsedBytes = cmd.Node.UsedBytes
				existing.TotalBytes = cmd.Node.TotalBytes
				existing.Status = NodeUp
			}
		}
	case cmdMarkNodeDown:
		if cmd.Node != nil {
			if existing, ok := f.state.Nodes[cmd.Node.ID]; ok {
				existing.Status = NodeDown
			}
		}
	case cmdUpdateFragment:
		for _, e := range f.state.Keys {
			for i, fr := range e.Fragments {
				if fr.ID == cmd.FragmentID {
					e.Fragments[i].NodeIDs = cmd.NodeIDs
				}
			}
		}
	}
	return nil
}

// Snapshot creates a point-in-time snapshot of the FSM state.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s state
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = &s
	return nil
}

// ---- read helpers (called outside Raft, safe with mu.RLock) ---------------

func (f *fsm) getKey(key string) (*KeyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.state.Keys[key]
	if !ok {
		return nil, false
	}
	// return a copy
	cp := *e
	return &cp, true
}

func (f *fsm) listKeys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	keys := make([]string, 0, len(f.state.Keys))
	for k := range f.state.Keys {
		keys = append(keys, k)
	}
	return keys
}

func (f *fsm) getNode(id string) (*NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, ok := f.state.Nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

func (f *fsm) listNodes() []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*NodeInfo, 0, len(f.state.Nodes))
	for _, n := range f.state.Nodes {
		cp := *n
		out = append(out, &cp)
	}
	return out
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
