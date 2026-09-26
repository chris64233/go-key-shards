package keyshards

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore 将协调状态与 outbox 以 JSON 快照形式持久化到单个文件。
// 写入采用“临时文件 + rename”，保证崩溃后要么看到旧快照、要么看到新快照。
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 创建落盘存储；文件不存在时视为空状态。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (f *FileStore) Load() (*State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return newState(), nil
		}
		return nil, fmt.Errorf("keyshards: load state: %w", err)
	}
	if len(data) == 0 {
		return newState(), nil
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("keyshards: decode state: %w", err)
	}
	if s.Ceremonies == nil {
		s.Ceremonies = make(map[string]*Ceremony)
	}
	return &s, nil
}

func (f *FileStore) Save(s *State) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("keyshards: encode state: %w", err)
	}
	if dir := filepath.Dir(f.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("keyshards: create state dir: %w", err)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), ".keyshards-*.tmp")
	if err != nil {
		return fmt.Errorf("keyshards: create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("keyshards: write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("keyshards: sync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("keyshards: close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("keyshards: replace state file: %w", err)
	}
	return nil
}
