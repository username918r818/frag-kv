// Package storage provides a local fragment store backed by files + a bbolt index.
//
// Layout on disk:
//
//	<dataDir>/chunks/<fragmentID-escaped>.chunk  — raw fragment bytes
//	<dataDir>/index.db                           — bbolt: fragmentID → FragmentMeta (JSON)
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrNotFound = errors.New("fragment not found")

var bucketFragments = []byte("fragments")

// FragmentMeta хранится в индексе.
type FragmentMeta struct {
	ID        string    `json:"id"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

// Store — локальное хранилище фрагментов на одном узле.
type Store struct {
	dataDir   string
	chunksDir string
	db        *bolt.DB
}

// Open открывает (или создаёт) хранилище в dataDir.
func Open(dataDir string) (*Store, error) {
	chunksDir := filepath.Join(dataDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating chunks dir: %w", err)
	}

	db, err := bolt.Open(filepath.Join(dataDir, "index.db"), 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening bbolt: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketFragments)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating bucket: %w", err)
	}

	return &Store{dataDir: dataDir, chunksDir: chunksDir, db: db}, nil
}

// Close закрывает хранилище.
func (s *Store) Close() error {
	return s.db.Close()
}

// Put сохраняет фрагмент. Читает данные из r.
func (s *Store) Put(id string, r io.Reader) error {
	path := s.chunkPath(id)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating chunk file: %w", err)
	}

	size, err := io.Copy(f, r)
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return fmt.Errorf("writing chunk: %w", err)
	}

	meta := FragmentMeta{ID: id, Size: size, CreatedAt: time.Now()}
	metaBytes, _ := json.Marshal(meta)

	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFragments).Put([]byte(id), metaBytes)
	})
}

// Get возвращает содержимое фрагмента или ErrNotFound.
func (s *Store) Get(id string) ([]byte, error) {
	if _, err := s.Meta(id); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading chunk file: %w", err)
	}
	return data, nil
}

// GetStream открывает фрагмент для потокового чтения. Вызывающий должен закрыть возвращённый ReadCloser.
func (s *Store) GetStream(id string) (io.ReadCloser, int64, error) {
	meta, err := s.Meta(id)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(s.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("opening chunk file: %w", err)
	}
	return f, meta.Size, nil
}

// Delete удаляет фрагмент.
func (s *Store) Delete(id string) error {
	if err := os.Remove(s.chunkPath(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing chunk file: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFragments).Delete([]byte(id))
	})
}

// Meta возвращает метаданные фрагмента или ErrNotFound.
func (s *Store) Meta(id string) (FragmentMeta, error) {
	var meta FragmentMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketFragments).Get([]byte(id))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &meta)
	})
	return meta, err
}

// List возвращает список всех ID фрагментов.
func (s *Store) List() ([]string, error) {
	var ids []string
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFragments).ForEach(func(k, _ []byte) error {
			ids = append(ids, string(k))
			return nil
		})
	})
	return ids, err
}

// DiskUsage возвращает суммарный размер всех фрагментов в байтах.
func (s *Store) DiskUsage() (int64, error) {
	var total int64
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFragments).ForEach(func(_, v []byte) error {
			var meta FragmentMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}
			total += meta.Size
			return nil
		})
	})
	return total, err
}

// chunkPath возвращает путь к файлу фрагмента.
// "/" в ID заменяется на "_" чтобы избежать создания вложенных директорий.
func (s *Store) chunkPath(id string) string {
	safe := strings.ReplaceAll(id, "/", "_")
	return filepath.Join(s.chunksDir, safe+".chunk")
}
