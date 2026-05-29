package fragment

import (
	"fmt"
	"io"
)

const DefaultChunkSize = 8 * 1024 * 1024 // 8 МБ

// ID уникально идентифицирует фрагмент: "<key>/<index>"
func ID(key string, index int) string {
	return fmt.Sprintf("%s/%d", key, index)
}

// Info описывает один фрагмент значения.
type Info struct {
	ID        string   `json:"id"`
	Key       string   `json:"key"`
	Index     int      `json:"index"`
	Size      int64    `json:"size"`
	NodeIDs   []string `json:"node_ids"`   // логические ID узлов
	NodeAddrs []string `json:"node_addrs"` // HTTP-адреса ("http://host:port"), параллельно NodeIDs
}

// Plan — список фрагментов для одного ключа.
type Plan struct {
	Key       string
	TotalSize int64
	ChunkSize int64
	Fragments []Info
}

// Split читает r и нарезает данные на фрагменты размером chunkSize.
// Для каждого фрагмента вызывает fn(index, data).
func Split(key string, r io.Reader, chunkSize int64, fn func(index int, data []byte) error) (Plan, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	buf := make([]byte, chunkSize)
	plan := Plan{Key: key, ChunkSize: chunkSize}
	index := 0

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			info := Info{
				ID:    ID(key, index),
				Key:   key,
				Index: index,
				Size:  int64(n),
			}
			plan.Fragments = append(plan.Fragments, info)
			plan.TotalSize += int64(n)
			if ferr := fn(index, chunk); ferr != nil {
				return plan, ferr
			}
			index++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return plan, fmt.Errorf("reading input: %w", err)
		}
	}
	return plan, nil
}

// Assemble записывает фрагменты в порядке их индексов в w.
// fragments должны быть отсортированы по Index.
func Assemble(w io.Writer, fragments []Info, fetch func(id string) ([]byte, error)) error {
	for _, f := range fragments {
		data, err := fetch(f.ID)
		if err != nil {
			return fmt.Errorf("fetching fragment %s: %w", f.ID, err)
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("writing fragment %s: %w", f.ID, err)
		}
	}
	return nil
}
