package keyshards

import (
	"context"
	"sort"
	"sync"
)

// MemRepository 是进程内的 Repository 实现。
//
// 并发模型：每个仪式一把互斥量，Update 在该仪式范围内完全串行；
// 另外一把全局锁保护仪式表与审计流。fn 操作的是深拷贝快照，
// 只有 fn 返回非 nil 仪式时结果才会生效。
type MemRepository struct {
	mu sync.Mutex

	ceremonies map[string]*Ceremony
	locks      map[string]*sync.Mutex
	events     map[string][]AuditEvent // ceremonyID -> 事件（seq 在仪式内单调递增）
	nextSeq    map[string]int64
}

// NewMemRepository 创建空的内存仓储。
func NewMemRepository() *MemRepository {
	return &MemRepository{
		ceremonies: map[string]*Ceremony{},
		locks:      map[string]*sync.Mutex{},
		events:     map[string][]AuditEvent{},
		nextSeq:    map[string]int64{},
	}
}

func (r *MemRepository) lockFor(id string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.locks[id]
	if !ok {
		l = &sync.Mutex{}
		r.locks[id] = l
	}
	return l
}

// Create 以初始状态写入新仪式并追加事件；ID 冲突返回 ErrExists。
func (r *MemRepository) Create(ctx context.Context, c *Ceremony, events []AuditEvent) error {
	if c == nil || c.ID == "" {
		return ErrInvalidArgument
	}
	l := r.lockFor(c.ID)
	l.Lock()
	defer l.Unlock()

	r.mu.Lock()
	if _, exists := r.ceremonies[c.ID]; exists {
		r.mu.Unlock()
		return ErrExists
	}
	r.ceremonies[c.ID] = cloneCeremony(c)
	r.mu.Unlock()

	r.appendEvents(c.ID, events)
	return nil
}

// Update 在仪式互斥量保护下执行原子读-改-写。
func (r *MemRepository) Update(ctx context.Context, id string, fn func(c *Ceremony) (*Ceremony, []AuditEvent, error)) error {
	l := r.lockFor(id)
	l.Lock()
	defer l.Unlock()

	r.mu.Lock()
	current, ok := r.ceremonies[id]
	r.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	out, events, err := fn(cloneCeremony(current))
	if out != nil {
		r.mu.Lock()
		r.ceremonies[id] = cloneCeremony(out)
		r.mu.Unlock()
	}
	r.appendEvents(id, events)
	return err
}

func (r *MemRepository) appendEvents(id string, events []AuditEvent) {
	if len(events) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := r.nextSeq[id]
	stored := r.events[id]
	for i := range events {
		seq++
		e := cloneEvent(events[i])
		e.Seq = seq
		e.CeremonyID = id
		stored = append(stored, e)
	}
	r.events[id] = stored
	r.nextSeq[id] = seq
}

// Get 返回仪式深拷贝快照。
func (r *MemRepository) Get(ctx context.Context, id string) (*Ceremony, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.ceremonies[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneCeremony(c), nil
}

// List 按 ID 排序返回全部仪式深拷贝。
func (r *MemRepository) List(ctx context.Context) ([]*Ceremony, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.ceremonies))
	for id := range r.ceremonies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Ceremony, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneCeremony(r.ceremonies[id]))
	}
	return out, nil
}

// Audit 查询审计事件。
func (r *MemRepository) Audit(ctx context.Context, ceremonyID string, fromSeq int64, limit int) ([]AuditEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var ids []string
	if ceremonyID != "" {
		if _, ok := r.ceremonies[ceremonyID]; !ok {
			return nil, ErrNotFound
		}
		ids = []string{ceremonyID}
	} else {
		for id := range r.events {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}

	var out []AuditEvent
	for _, id := range ids {
		for _, e := range r.events[id] {
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

// Close 释放资源（内存实现无底层资源）。
func (r *MemRepository) Close() error { return nil }
