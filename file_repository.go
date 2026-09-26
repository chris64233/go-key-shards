package keyshards

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// FileRepository 把仪式状态、审计流持久化到本地目录，把密钥激活 outbox
// 以“每仪式一个文件、只写一次”的方式落盘。
//
// 目录布局：
//
//	dir/state.json        全部仪式 + 审计事件（临时文件 + rename 原子替换）
//	dir/lock              跨进程互斥文件锁
//	dir/outbox/<id>.json  完成时写出的唯一激活消息（先于 state.json 落盘）
//
// 崩溃恢复：outbox 先写后改状态；若在两者之间崩溃，状态仍是 active，
// 后续完成会以相同文件名覆盖重写，不会产生第二条激活消息。
type FileRepository struct {
	dir string

	procMu sync.Mutex // 进程内串行；flock 提供跨进程串行
	locker fileLocker // 平台相关的文件锁

	// 内存镜像，每次 Update 后与磁盘一致。
	state *fileState
}

type fileState struct {
	Ceremonies map[string]*Ceremony    `json:"ceremonies"`
	Events     map[string][]AuditEvent `json:"events,omitempty"`
	NextSeq    map[string]int64        `json:"next_seq,omitempty"`
}

// NewFileRepository 打开（不存在则初始化）目录下的持久化仓储。
func NewFileRepository(dir string) (*FileRepository, error) {
	if dir == "" {
		return nil, errInvalid("directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "outbox"), 0o755); err != nil {
		return nil, fmt.Errorf("keyshards: create data dir: %w", err)
	}
	locker, err := newFileLocker(filepath.Join(dir, "lock"))
	if err != nil {
		return nil, err
	}
	r := &FileRepository{dir: dir, locker: locker}
	if err := r.load(); err != nil {
		_ = locker.Close()
		return nil, err
	}
	return r, nil
}

func statePath(dir string) string { return filepath.Join(dir, "state.json") }

func (r *FileRepository) load() error {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return err
	}
	defer r.locker.Unlock()

	data, err := os.ReadFile(statePath(r.dir))
	switch {
	case errors.Is(err, os.ErrNotExist):
		r.state = newFileState()
		return nil
	case err != nil:
		return fmt.Errorf("keyshards: read state: %w", err)
	}
	var st fileState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("keyshards: corrupt state.json: %w", err)
	}
	if st.Ceremonies == nil {
		st.Ceremonies = map[string]*Ceremony{}
	}
	if st.Events == nil {
		st.Events = map[string][]AuditEvent{}
	}
	if st.NextSeq == nil {
		st.NextSeq = map[string]int64{}
	}
	r.state = &st
	return nil
}

func newFileState() *fileState {
	return &fileState{
		Ceremonies: map[string]*Ceremony{},
		Events:     map[string][]AuditEvent{},
		NextSeq:    map[string]int64{},
	}
}

// persist 必须在已持有文件锁时调用：先写 outbox（若有新增），
// 再原子替换 state.json。
func (r *FileRepository) persist(prev, next *fileState) error {
	// 新增 outbox：状态切换到 completed 的仪式。
	for id, c := range next.Ceremonies {
		old := prev.Ceremonies[id]
		if c.Outbox != nil && (old == nil || old.Outbox == nil) {
			if err := writeJSONAtomic(filepath.Join(r.dir, "outbox", id+".json"), c.Outbox, 0o644); err != nil {
				return err
			}
		}
	}
	return writeJSONAtomic(statePath(r.dir), next, 0o644)
}

// Create 写入新仪式并持久化；ID 冲突返回 ErrExists。
func (r *FileRepository) Create(ctx context.Context, c *Ceremony, events []AuditEvent) error {
	if c == nil || c.ID == "" {
		return ErrInvalidArgument
	}
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return err
	}
	defer r.locker.Unlock()

	if _, exists := r.state.Ceremonies[c.ID]; exists {
		return ErrExists
	}
	next := r.snapshotState()
	next.Ceremonies[c.ID] = cloneCeremony(c)
	assignSeqs(next, c.ID, events)
	if err := r.persist(r.state, next); err != nil {
		return err
	}
	r.state = next
	return nil
}

// Update 在文件锁保护下执行原子读-改-写并持久化。
func (r *FileRepository) Update(ctx context.Context, id string, fn func(c *Ceremony) (*Ceremony, []AuditEvent, error)) error {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return err
	}
	defer r.locker.Unlock()

	current, ok := r.state.Ceremonies[id]
	if !ok {
		return ErrNotFound
	}

	out, events, err := fn(cloneCeremony(current))
	next := r.snapshotState()
	if out != nil {
		next.Ceremonies[id] = cloneCeremony(out)
	}
	assignSeqs(next, id, events)
	if perr := r.persist(r.state, next); perr != nil {
		return perr
	}
	r.state = next
	return err
}

func (r *FileRepository) snapshotState() *fileState {
	st := newFileState()
	for id, c := range r.state.Ceremonies {
		st.Ceremonies[id] = cloneCeremony(c)
	}
	for id, evs := range r.state.Events {
		cp := make([]AuditEvent, len(evs))
		for i, e := range evs {
			cp[i] = cloneEvent(e)
		}
		st.Events[id] = cp
	}
	for id, seq := range r.state.NextSeq {
		st.NextSeq[id] = seq
	}
	return st
}

// assignSeqs 给事件分配仪式内单调递增序号并追加到状态。
func assignSeqs(st *fileState, id string, events []AuditEvent) {
	if len(events) == 0 {
		return
	}
	seq := st.NextSeq[id]
	stored := st.Events[id]
	for i := range events {
		seq++
		e := cloneEvent(events[i])
		e.Seq = seq
		e.CeremonyID = id
		stored = append(stored, e)
	}
	st.Events[id] = stored
	st.NextSeq[id] = seq
}

// Get 返回仪式深拷贝快照。
func (r *FileRepository) Get(ctx context.Context, id string) (*Ceremony, error) {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return nil, err
	}
	defer r.locker.Unlock()
	c, ok := r.state.Ceremonies[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneCeremony(c), nil
}

// List 按 ID 排序返回全部仪式深拷贝。
func (r *FileRepository) List(ctx context.Context) ([]*Ceremony, error) {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return nil, err
	}
	defer r.locker.Unlock()
	ids := make([]string, 0, len(r.state.Ceremonies))
	for id := range r.state.Ceremonies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Ceremony, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneCeremony(r.state.Ceremonies[id]))
	}
	return out, nil
}

// Audit 查询审计事件，语义同接口注释。
func (r *FileRepository) Audit(ctx context.Context, ceremonyID string, fromSeq int64, limit int) ([]AuditEvent, error) {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if err := r.locker.Lock(); err != nil {
		return nil, err
	}
	defer r.locker.Unlock()

	var ids []string
	if ceremonyID != "" {
		if _, ok := r.state.Ceremonies[ceremonyID]; !ok {
			return nil, ErrNotFound
		}
		ids = []string{ceremonyID}
	} else {
		for id := range r.state.Events {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}

	var out []AuditEvent
	for _, id := range ids {
		for _, e := range r.state.Events[id] {
			if e.Seq <= fromSeq {
				continue
			}
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
			out = append(out, cloneEvent(e))
		}
	}
	return out, nil
}

// ReadOutboxFile 读取已落盘的密钥激活消息文件（供外部投递组件消费）。
func (r *FileRepository) ReadOutboxFile(id string) (*Outbox, error) {
	data, err := os.ReadFile(filepath.Join(r.dir, "outbox", id+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("keyshards: read outbox: %w", err)
	}
	var o Outbox
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, fmt.Errorf("keyshards: corrupt outbox: %w", err)
	}
	return &o, nil
}

// Close 释放文件锁。
func (r *FileRepository) Close() error { return r.locker.Close() }

// writeJSONAtomic 经临时文件 + fsync + rename 原子落盘。
func writeJSONAtomic(path string, v any, mode os.FileMode) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("keyshards: encode state: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("keyshards: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("keyshards: write state: %w", err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("keyshards: chmod state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("keyshards: fsync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("keyshards: close state: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("keyshards: rename state: %w", err)
	}
	// 尽力 fsync 目录以持久化 rename 条目。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}
