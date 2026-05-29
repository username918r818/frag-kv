package smoke_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/username918r818/fragkv/internal/metadata"
	"github.com/username918r818/fragkv/internal/storage"
	"github.com/username918r818/fragkv/internal/transport"
)

type keyEntry struct {
	Key       string `json:"key"`
	TotalSize int64  `json:"total_size"`
	Committed bool   `json:"committed"`
	Fragments []struct {
		ID        string   `json:"id"`
		Index     int      `json:"index"`
		Size      int64    `json:"size"`
		NodeAddrs []string `json:"node_addrs"`
	} `json:"fragments"`
}

type testNode struct {
	id    string
	url   string
	store *storage.Store
}

func TestClusterSmoke(t *testing.T) {
	metaStore := openMetadataStore(t)
	defer metaStore.Close()

	waitForLeader(t, metaStore)

	metaServer := httptest.NewServer(metadata.NewServer(metaStore))
	defer metaServer.Close()

	nodes := []*testNode{
		newStorageNode(t, "storage-1"),
		newStorageNode(t, "storage-2"),
		newStorageNode(t, "storage-3"),
	}

	for _, node := range nodes {
		registerNode(t, metaServer.URL, node)
	}

	if got := fetchNodeCount(t, metaServer.URL); got != len(nodes) {
		t.Fatalf("registered nodes = %d, want %d", got, len(nodes))
	}

	input := bytes.Repeat([]byte("fragkv-smoke-"), 700000)
	key := "smoke-key"

	plan := prepareKey(t, metaServer.URL, key, int64(len(input)))
	if len(plan.Fragments) == 0 {
		t.Fatal("prepare returned no fragments")
	}

	for i, frag := range plan.Fragments {
		if len(frag.NodeAddrs) == 0 {
			t.Fatalf("fragment %d has no node addresses", i)
		}
		uploadFragment(t, frag.NodeAddrs, frag.ID, inputChunk(input, plan, i))
	}

	commitKey(t, metaServer.URL, key)

	entry := getKey(t, metaServer.URL, key)
	if !entry.Committed {
		t.Fatal("committed key is still marked pending")
	}
	if entry.TotalSize != int64(len(input)) {
		t.Fatalf("stored size = %d, want %d", entry.TotalSize, len(input))
	}

	output := downloadValue(t, entry)
	if !bytes.Equal(output, input) {
		t.Fatal("downloaded value differs from uploaded value")
	}

	keys := listKeys(t, metaServer.URL)
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("list keys = %v, want [%s]", keys, key)
	}

	status := fetchStatus(t, metaServer.URL)
	if !status.IsLeader || !strings.EqualFold(status.State, "leader") {
		t.Fatalf("unexpected metadata status: %+v", status)
	}

	deleteKey(t, metaServer.URL, key)
	if got := listKeys(t, metaServer.URL); len(got) != 0 {
		t.Fatalf("keys after delete = %v, want []", got)
	}
}

func openMetadataStore(t *testing.T) *metadata.Store {
	t.Helper()

	raftAddr := reserveTCPAddr(t)
	store, err := metadata.Open(metadata.Config{
		NodeID:            "meta-1",
		RaftAddr:          raftAddr,
		RaftAdvertiseAddr: raftAddr,
		DataDir:           t.TempDir(),
		Bootstrap:         true,
		ReplicationFactor: 3,
		ChunkSize:         8 * 1024 * 1024,
		PlacementStrategy: "rendezvous",
		DeadThreshold:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("open metadata store: %v", err)
	}
	return store
}

func newStorageNode(t *testing.T, id string) *testNode {
	t.Helper()

	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open storage %s: %v", id, err)
	}
	t.Cleanup(func() { store.Close() })

	srv := httptest.NewServer(transport.NewStoragedServer(id, store))
	t.Cleanup(srv.Close)

	return &testNode{id: id, url: srv.URL, store: store}
}

func waitForLeader(t *testing.T, store *metadata.Store) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("metadata store did not become leader in time")
}

func reserveTCPAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP addr: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func registerNode(t *testing.T, metaURL string, node *testNode) {
	t.Helper()

	body, err := json.Marshal(metadata.NodeInfo{
		ID:         node.id,
		Addr:       strings.TrimPrefix(node.url, "http://"),
		TotalBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("marshal node %s: %v", node.id, err)
	}

	resp, err := http.Post(metaURL+"/nodes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register node %s: %v", node.id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		failResponse(t, "register node", resp)
	}
}

func prepareKey(t *testing.T, metaURL, key string, totalSize int64) keyEntry {
	t.Helper()

	body, err := json.Marshal(map[string]int64{"total_size": totalSize})
	if err != nil {
		t.Fatalf("marshal prepare request: %v", err)
	}

	resp, err := http.Post(metaURL+"/keys/"+key+"/prepare", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("prepare key: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failResponse(t, "prepare key", resp)
	}

	var entry keyEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		t.Fatalf("decode prepare response: %v", err)
	}
	return entry
}

func commitKey(t *testing.T, metaURL, key string) {
	t.Helper()

	resp, err := http.Post(metaURL+"/keys/"+key+"/commit", "application/json", nil)
	if err != nil {
		t.Fatalf("commit key: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		failResponse(t, "commit key", resp)
	}
}

func getKey(t *testing.T, metaURL, key string) keyEntry {
	t.Helper()

	resp, err := http.Get(metaURL + "/keys/" + key)
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failResponse(t, "get key", resp)
	}

	var entry keyEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		t.Fatalf("decode key response: %v", err)
	}
	return entry
}

func deleteKey(t *testing.T, metaURL, key string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, metaURL+"/keys/"+key, nil)
	if err != nil {
		t.Fatalf("build delete request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete key: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		failResponse(t, "delete key", resp)
	}
}

func listKeys(t *testing.T, metaURL string) []string {
	t.Helper()

	resp, err := http.Get(metaURL + "/keys/")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failResponse(t, "list keys", resp)
	}

	var keys []string
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		t.Fatalf("decode keys list: %v", err)
	}
	return keys
}

func fetchNodeCount(t *testing.T, metaURL string) int {
	t.Helper()

	resp, err := http.Get(metaURL + "/nodes")
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failResponse(t, "list nodes", resp)
	}

	var nodes []metadata.NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode nodes list: %v", err)
	}
	return len(nodes)
}

func uploadFragment(t *testing.T, nodeAddrs []string, id string, data []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, nodeAddrs[0]+"/fragments/"+id, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build upload request: %v", err)
	}
	if len(nodeAddrs) > 1 {
		req.Header.Set("X-Replica-Addrs", strings.Join(nodeAddrs[1:], ","))
	}
	req.Header.Set("X-Write-Quorum", fmt.Sprintf("%d", len(nodeAddrs)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload fragment %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		failResponse(t, "upload fragment", resp)
	}
}

func inputChunk(input []byte, plan keyEntry, index int) []byte {
	start := 0
	for i := 0; i < index; i++ {
		start += chunkLength(plan, i)
	}
	end := start + chunkLength(plan, index)
	return input[start:end]
}

func chunkLength(plan keyEntry, index int) int {
	return int(plan.Fragments[index].Size)
}

func downloadValue(t *testing.T, entry keyEntry) []byte {
	t.Helper()

	var out bytes.Buffer
	for _, frag := range entry.Fragments {
		var data []byte
		var err error
		for _, addr := range frag.NodeAddrs {
			resp, getErr := http.Get(addr + "/fragments/" + frag.ID)
			if getErr != nil {
				err = getErr
				continue
			}
			if resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("GET %s: status %d", frag.ID, resp.StatusCode)
				resp.Body.Close()
				continue
			}
			data, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("download fragment %s: %v", frag.ID, err)
		}
		out.Write(data)
	}
	return out.Bytes()
}

type statusResp struct {
	IsLeader bool   `json:"is_leader"`
	State    string `json:"state"`
}

func fetchStatus(t *testing.T, metaURL string) statusResp {
	t.Helper()

	resp, err := http.Get(metaURL + "/status")
	if err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failResponse(t, "fetch status", resp)
	}

	var status statusResp
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	return status
}

func failResponse(t *testing.T, op string, resp *http.Response) {
	t.Helper()

	body, _ := io.ReadAll(resp.Body)
	t.Fatalf("%s: status %d: %s", op, resp.StatusCode, bytes.TrimSpace(body))
}
