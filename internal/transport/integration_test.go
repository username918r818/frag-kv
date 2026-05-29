package transport_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/username918r818/fragkv/internal/storage"
	"github.com/username918r818/fragkv/internal/transport"
)

// newTestNode creates an in-memory storage + HTTP test server.
func newTestNode(t *testing.T, id string) (*httptest.Server, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	srv := httptest.NewServer(transport.NewStoragedServer(id, store))
	t.Cleanup(srv.Close)
	return srv, store
}

// TestPutGet exercises single-fragment PUT and GET via HTTP.
func TestPutGet(t *testing.T) {
	srv, _ := newTestNode(t, "node-1")

	data := make([]byte, 1024*512) // 512 KB
	rand.Read(data)                //nolint:errcheck

	// PUT
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/fragments/key1/0", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: got %d, want 201", resp.StatusCode)
	}

	// GET
	resp, err = http.Get(srv.URL + "/fragments/key1/0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: got %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Error("GET returned different data than PUT")
	}
}

// TestPutWithQuorumReplication exercises fan-out to replica nodes via X-Replica-Addrs.
func TestPutWithQuorumReplication(t *testing.T) {
	primary, primaryStore := newTestNode(t, "primary")
	replica1, replica1Store := newTestNode(t, "replica-1")
	replica2, replica2Store := newTestNode(t, "replica-2")

	data := make([]byte, 64*1024) // 64 KB
	rand.Read(data)               //nolint:errcheck

	// PUT to primary with replica addresses in header.
	req, _ := http.NewRequest(http.MethodPut, primary.URL+"/fragments/testkey/0", bytes.NewReader(data))
	req.Header.Set("X-Replica-Addrs", replica1.URL+","+replica2.URL)
	req.Header.Set("X-Write-Quorum", "3") // require all 3
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: got %d", resp.StatusCode)
	}

	// All three stores should have the fragment.
	for name, s := range map[string]*storage.Store{
		"primary":   primaryStore,
		"replica-1": replica1Store,
		"replica-2": replica2Store,
	} {
		got, err := s.Get("testkey/0")
		if err != nil {
			t.Errorf("%s: Get error: %v", name, err)
			continue
		}
		if !bytes.Equal(got, data) {
			t.Errorf("%s: data mismatch", name)
		}
	}
}

// TestDelete exercises DELETE.
func TestDelete(t *testing.T) {
	srv, _ := newTestNode(t, "n1")

	// Store something first.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/fragments/delkey/0", bytes.NewReader([]byte("hello")))
	http.DefaultClient.Do(req) //nolint:errcheck

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/fragments/delkey/0", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: got %d", resp.StatusCode)
	}

	// GET should 404 now.
	resp, _ = http.Get(srv.URL + "/fragments/delkey/0")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("after DELETE, GET returned %d", resp.StatusCode)
	}
}

// TestHealth checks the /health endpoint.
func TestHealth(t *testing.T) {
	srv, _ := newTestNode(t, "health-node")
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body) //nolint:errcheck
	if body["status"] != "ok" {
		t.Errorf("health status = %v", body["status"])
	}
}

// TestReplicateFrom checks the /replicate endpoint.
func TestReplicateFrom(t *testing.T) {
	source, sourceStore := newTestNode(t, "source")
	target, targetStore := newTestNode(t, "target")

	data := []byte("replicate me please")
	sourceStore.Put("mykey/0", bytes.NewReader(data)) //nolint:errcheck

	body, _ := json.Marshal(map[string]string{"source_url": source.URL})
	resp, err := http.Post(target.URL+"/fragments/mykey/0/replicate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("replicate: got %d", resp.StatusCode)
	}

	got, err := targetStore.Get("mykey/0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("replicated data mismatch")
	}
}

// TestGetNotFound checks that missing fragment returns 404.
func TestGetNotFound(t *testing.T) {
	srv, _ := newTestNode(t, "n-empty")
	resp, _ := http.Get(srv.URL + "/fragments/missing/99")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

// TestLargeFragment verifies 50 MB streaming PUT/GET round-trip.
func TestLargeFragment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large fragment test in short mode")
	}
	srv, _ := newTestNode(t, "large-node")

	data := make([]byte, 50*1024*1024) // 50 МБ
	rand.Read(data)                    //nolint:errcheck

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/fragments/bigkey/0", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT large: %d", resp.StatusCode)
	}

	resp, _ = http.Get(fmt.Sprintf("%s/fragments/bigkey/0", srv.URL))
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Error("large fragment round-trip mismatch")
	}
}
