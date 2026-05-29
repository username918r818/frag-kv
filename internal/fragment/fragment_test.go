package fragment

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestSplitAssemble(t *testing.T) {
	const total = 25 * 1024 * 1024 // 25 МБ → 4 фрагмента при chunkSize=8МБ
	original := make([]byte, total)
	if _, err := rand.Read(original); err != nil {
		t.Fatal(err)
	}

	chunks := map[string][]byte{}
	plan, err := Split("testkey", bytes.NewReader(original), DefaultChunkSize, func(index int, data []byte) error {
		id := ID("testkey", index)
		chunks[id] = data
		return nil
	})
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	if plan.TotalSize != int64(total) {
		t.Errorf("TotalSize = %d, want %d", plan.TotalSize, total)
	}

	expectedFragments := (total + DefaultChunkSize - 1) / DefaultChunkSize
	if len(plan.Fragments) != expectedFragments {
		t.Errorf("len(Fragments) = %d, want %d", len(plan.Fragments), expectedFragments)
	}

	var out bytes.Buffer
	err = Assemble(&out, plan.Fragments, func(id string) ([]byte, error) {
		return chunks[id], nil
	})
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}

	if !bytes.Equal(out.Bytes(), original) {
		t.Error("assembled bytes differ from original")
	}
}

func TestSplitEmpty(t *testing.T) {
	plan, err := Split("empty", bytes.NewReader(nil), DefaultChunkSize, func(_ int, _ []byte) error {
		return nil
	})
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Fragments) != 0 {
		t.Errorf("expected 0 fragments for empty input, got %d", len(plan.Fragments))
	}
}

func TestSplitExactChunk(t *testing.T) {
	data := make([]byte, DefaultChunkSize)
	plan, err := Split("exact", bytes.NewReader(data), DefaultChunkSize, func(_ int, _ []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Fragments) != 1 {
		t.Errorf("expected 1 fragment, got %d", len(plan.Fragments))
	}
}
