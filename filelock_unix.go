//go:build !windows

package keyshards

import (
	"fmt"
	"os"
	"syscall"
)

// unixFileLocker 基于 flock(2) 提供跨进程互斥；
// 同一进程内 FileRepository 另有 sync.Mutex 保证可重入安全
// （flock 是关联到打开文件描述的劝告锁，同进程加锁两次会死锁，因此不能省掉进程锁）。
type unixFileLocker struct {
	f *os.File
}

func newFileLocker(path string) (fileLocker, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("keyshards: open lock file: %w", err)
	}
	return &unixFileLocker{f: f}, nil
}

func (l *unixFileLocker) Lock() error {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("keyshards: acquire lock: %w", err)
	}
	return nil
}

func (l *unixFileLocker) Unlock() error {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("keyshards: release lock: %w", err)
	}
	return nil
}

func (l *unixFileLocker) Close() error {
	_ = l.Unlock()
	return l.f.Close()
}
