package keyshards

import (
	"sort"
	"strings"
)

// State 是协调层需要持久化的全部状态：仪式、outbox 与审计流水。
type State struct {
	Ceremonies map[string]*Ceremony
	Outbox     []OutboxEvent
	Audit      []AuditEvent
	Seq        int64 // 审计序号发号器
}

func newState() *State {
	return &State{Ceremonies: make(map[string]*Ceremony)}
}

// Store 持久化协调状态与 outbox。Save 与 Load 必须整体一致：
// 实现方应保证一次 Save 落盘的内容在崩溃后要么全部可见、要么全部不可见。
type Store interface {
	Load() (*State, error)
	Save(s *State) error
}

// MemoryStore 进程内实现，主要用于测试与不需落盘的场景。
type MemoryStore struct {
	state *State
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: newState()}
}

func (m *MemoryStore) Load() (*State, error) {
	if m.state == nil {
		return newState(), nil
	}
	return m.state, nil
}

func (m *MemoryStore) Save(s *State) error {
	m.state = s
	return nil
}

// sortStrings 返回排序后的副本。
func sortStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

// sortedContributions 按参与者 ID 排序返回贡献集合的副本，保证冻结结果确定。
func sortedContributions(m map[string]Contribution) []Contribution {
	out := make([]Contribution, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ParticipantID != out[j].ParticipantID {
			return out[i].ParticipantID < out[j].ParticipantID
		}
		return strings.Compare(out[i].RequestID, out[j].RequestID) < 0
	})
	return out
}
