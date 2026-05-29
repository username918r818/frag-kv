package metadata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/username918r818/fragkv/internal/fragment"
)

// applyCmd is a test helper that applies a command directly to the FSM.
func applyCmd(f *fsm, cmd command) {
	data, _ := json.Marshal(cmd)
	f.Apply(&raft.Log{Data: data})
}

func TestFSM_PutCommitGetKey(t *testing.T) {
	f := newFSM()

	entry := &KeyEntry{
		Key:       "hello",
		TotalSize: 100,
		ChunkSize: 8 << 20,
		Fragments: []fragment.Info{{ID: "hello/0", Key: "hello", Index: 0, Size: 100}},
		CreatedAt: time.Now(),
		Committed: false,
	}

	applyCmd(f, command{Type: cmdPutKey, Entry: entry})

	got, ok := f.getKey("hello")
	if !ok {
		t.Fatal("key not found after put")
	}
	if got.Committed {
		t.Error("expected not committed yet")
	}

	applyCmd(f, command{Type: cmdCommitKey, Key: "hello"})

	got, _ = f.getKey("hello")
	if !got.Committed {
		t.Error("expected committed after commit cmd")
	}
}

func TestFSM_DeleteKey(t *testing.T) {
	f := newFSM()
	entry := &KeyEntry{Key: "todelete", TotalSize: 10, Committed: true}
	applyCmd(f, command{Type: cmdPutKey, Entry: entry})
	applyCmd(f, command{Type: cmdDeleteKey, Key: "todelete"})

	if _, ok := f.getKey("todelete"); ok {
		t.Error("key should be gone after delete")
	}
}

func TestFSM_RegisterNodeAndHeartbeat(t *testing.T) {
	f := newFSM()

	n := &NodeInfo{
		ID:         "storage-1",
		Addr:       "storage-1:8001",
		TotalBytes: 10 << 30,
		Status:     NodeUp,
		LastSeen:   time.Now().Add(-30 * time.Second),
	}
	applyCmd(f, command{Type: cmdRegisterNode, Node: n})

	got, ok := f.getNode("storage-1")
	if !ok {
		t.Fatal("node not found after register")
	}
	if got.Status != NodeUp {
		t.Errorf("status = %s, want up", got.Status)
	}

	// Heartbeat updates last_seen and usage.
	applyCmd(f, command{Type: cmdHeartbeat, Node: &NodeInfo{
		ID:         "storage-1",
		LastSeen:   time.Now(),
		UsedBytes:  1 << 30,
		TotalBytes: 10 << 30,
	}})

	got, _ = f.getNode("storage-1")
	if got.UsedBytes != 1<<30 {
		t.Errorf("used_bytes = %d, want %d", got.UsedBytes, 1<<30)
	}
	if got.Status != NodeUp {
		t.Errorf("heartbeat should keep status UP, got %s", got.Status)
	}
}

func TestFSM_MarkNodeDown(t *testing.T) {
	f := newFSM()
	n := &NodeInfo{ID: "dying", Status: NodeUp, LastSeen: time.Now()}
	applyCmd(f, command{Type: cmdRegisterNode, Node: n})
	applyCmd(f, command{Type: cmdMarkNodeDown, Node: &NodeInfo{ID: "dying"}})

	got, _ := f.getNode("dying")
	if got.Status != NodeDown {
		t.Errorf("expected DOWN, got %s", got.Status)
	}
}

func TestFSM_UpdateFragmentNodes(t *testing.T) {
	f := newFSM()
	entry := &KeyEntry{
		Key: "fragkey",
		Fragments: []fragment.Info{
			{ID: "fragkey/0", NodeIDs: []string{"n1", "n2"}},
		},
	}
	applyCmd(f, command{Type: cmdPutKey, Entry: entry})
	applyCmd(f, command{Type: cmdUpdateFragment, FragmentID: "fragkey/0", NodeIDs: []string{"n1", "n3", "n4"}})

	got, _ := f.getKey("fragkey")
	if len(got.Fragments[0].NodeIDs) != 3 {
		t.Errorf("expected 3 node IDs, got %v", got.Fragments[0].NodeIDs)
	}
	if got.Fragments[0].NodeIDs[1] != "n3" {
		t.Errorf("expected n3 at index 1, got %s", got.Fragments[0].NodeIDs[1])
	}
}

func TestFSM_ListKeys(t *testing.T) {
	f := newFSM()
	for _, k := range []string{"a", "b", "c"} {
		applyCmd(f, command{Type: cmdPutKey, Entry: &KeyEntry{Key: k}})
	}
	keys := f.listKeys()
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	f := newFSM()
	applyCmd(f, command{Type: cmdPutKey, Entry: &KeyEntry{Key: "snap1", TotalSize: 42, Committed: true}})
	applyCmd(f, command{Type: cmdRegisterNode, Node: &NodeInfo{ID: "snap-node", Status: NodeUp}})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Write snapshot to a buffer.
	var buf bytes.Buffer
	sink := &testSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore into a fresh FSM.
	f2 := newFSM()
	if err := f2.Restore(io_nopCloser(&buf)); err != nil {
		t.Fatal(err)
	}

	got, ok := f2.getKey("snap1")
	if !ok {
		t.Fatal("key snap1 not found after restore")
	}
	if got.TotalSize != 42 {
		t.Errorf("TotalSize = %d, want 42", got.TotalSize)
	}
	if _, ok := f2.getNode("snap-node"); !ok {
		t.Error("snap-node not found after restore")
	}
}

// ---- test helpers ----------------------------------------------------------

type testSink struct {
	buf *bytes.Buffer
}

func (s *testSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSink) Close() error                { return nil }
func (s *testSink) ID() string                  { return "test-snap" }
func (s *testSink) Cancel() error               { return nil }

func io_nopCloser(r *bytes.Buffer) nopCloser { return nopCloser{r} }

type nopCloser struct{ *bytes.Buffer }

func (nopCloser) Close() error { return nil }

// TestFSM_UnknownCommandIgnored verifies that unknown commands don't crash.
func TestFSM_UnknownCommandIgnored(t *testing.T) {
	f := newFSM()
	data, _ := json.Marshal(command{Type: "unknown_future_cmd"})
	result := f.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("expected nil result for unknown cmd, got %v", result)
	}
}

// TestFSM_ConcurrentReads verifies that concurrent reads don't race.
func TestFSM_ConcurrentReads(t *testing.T) {
	f := newFSM()
	for i := 0; i < 10; i++ {
		applyCmd(f, command{Type: cmdPutKey, Entry: &KeyEntry{
			Key: strings.Repeat("k", i+1),
		}})
	}
	done := make(chan struct{}, 20)
	for i := 0; i < 20; i++ {
		go func() {
			f.listKeys()
			f.listNodes()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}
