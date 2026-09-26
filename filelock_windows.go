//go:build windows

package keyshards

import (
	"fmt"
	"os"
)

// windowsFileLocker 是 Windows 占位实现：进程内互斥仍由 FileRepository
// 的 sync.Mutex 保证；跨进程互斥未实现（生产部署请使用 unix 或补全锁实现）。
type windowsFileLocker struct {
	f *os.File
}

func newFileLocker(path string) (fileLocker, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("keyshards: open lock file: %w", err)
	}
	return &windowsFileLocker{f: f}, nil
}

func (l *windowsFileLocker) Lock() error   { return nil }
func (l *windowsFileLocker) Unlock() error { return nil }
func (l *windowsFileLocker) Close() error  { return l.f.Close() }
