package keyshards

// fileLocker 抽象跨进程文件锁。
type fileLocker interface {
	Lock() error
	Unlock() error
	Close() error
}
