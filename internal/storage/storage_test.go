package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestStoreBasicOps(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := "mykey/0"
	data := []byte("hello fragment world")

	if err := s.Put(id, bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get returned %q, want %q", got, data)
	}

	meta, err := s.Meta(id)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("Meta.Size = %d, want %d", meta.Size, len(data))
	}

	if err := s.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = s.Get(id)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: expected ErrNotFound, got %v", err)
	}
}

func TestStoreGetStream(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := "key/1"
	data := make([]byte, 1024*1024) // 1 МБ
	for i := range data {
		data[i] = byte(i)
	}

	if err := s.Put(id, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	rc, size, err := s.GetStream(id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}

	var buf bytes.Buffer
	buf.ReadFrom(rc)
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("streamed bytes differ from original")
	}
}

func TestStoreDiskUsage(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 3; i++ {
		data := make([]byte, 100)
		s.Put("k/"+string(rune('0'+i)), bytes.NewReader(data))
	}

	usage, err := s.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage != 300 {
		t.Errorf("DiskUsage = %d, want 300", usage)
	}
}

func TestStoreNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Get("nonexistent/0")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStorePersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_persist_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	data := []byte("persistent fragment")

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("persist/0", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Переоткрываем — данные должны сохраниться
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.Get("persist/0")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data lost after reopen")
	}
}
