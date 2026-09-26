package keyshards

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Service 是门限密钥分片仪式的状态协调层。
//
// 职责边界：本层只协调状态——参与者名册、轮次、门限计数、幂等与 outbox；
// 承诺与分片摘要由外部密码学组件产生，本层不设计加密算法，也不接触秘密分片明文。
//
// 所有变更在单把互斥锁内完成“校验 → 变更 → 落盘”，因此替换、贡献、
// 取消、超时与完成并发交错时，只会产生合法的轮次演进与唯一终态。
type Service struct {
	mu    sync.Mutex
	store Store
	state *State
	now   func() time.Time
}

// Option 自定义 Service 行为。
type Option func(*Service)

// WithClock 注入时钟，便于测试截止时间与超时。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// NewService 从存储恢复状态并创建协调服务。
func NewService(store Store, opts ...Option) (*Service, error) {
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	if state.Ceremonies == nil {
		state.Ceremonies = make(map[string]*Ceremony)
	}
	s := &Service{store: store, state: state, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// CreateParams 创建仪式的参数；创建后参与者、门限、截止时间即被冻结。
type CreateParams struct {
	CeremonyID   string // 为空时自动生成
	Participants []string
	Threshold    int
	Deadline     time.Time
}

// ReplaceParams 替换参与者的参数；会显式开启新轮次。
type ReplaceParams struct {
	Participants []string
	Threshold    int
	Deadline     time.Time
	Actor        string // 操作者，记入审计
	Reason       string
}

// CreateCeremony 创建仪式并开启第 1 轮，冻结参与者、门限与截止时间。
func (s *Service) CreateCeremony(p CreateParams) (CeremonyView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	roster, err := validateRoster(p.Participants, p.Threshold, p.Deadline, now)
	if err != nil {
		return CeremonyView{}, err
	}
	id := p.CeremonyID
	if id == "" {
		id = newCeremonyID()
	}
	if _, exists := s.state.Ceremonies[id]; exists {
		return CeremonyView{}, fmt.Errorf("%w: ceremony %q already exists", ErrInvalidParams, id)
	}
	c := &Ceremony{
		ID:        id,
		Status:    StatusActive,
		CreatedAt: now,
		Idem:      make(map[string]IdemRecord),
		Rounds: []Round{{
			Number:        1,
			Participants:  roster,
			Threshold:     p.Threshold,
			Deadline:      p.Deadline,
			Open:          true,
			Contributions: make(map[string]Contribution),
		}},
	}
	s.state.Ceremonies[id] = c
	s.auditLocked(AuditCeremonyCreated, c, 1, "", fmt.Sprintf("participants=%d threshold=%d deadline=%s", len(roster), p.Threshold, p.Deadline.Format(time.RFC3339)))
	if err := s.persistLocked(); err != nil {
		return CeremonyView{}, err
	}
	return s.viewLocked(c), nil
}

// SubmitContribution 提交一次贡献。
//
// 幂等语义：同一 RequestID 且内容（承诺+摘要）相同的重试返回首次结果；
// 同一 RequestID 内容不同则返回 ErrIdempotencyConflict。
// 非成员、旧轮次、超过截止时间的贡献都会被拒绝且不计入门限。
func (s *Service) SubmitContribution(ceremonyID string, c Contribution) (SubmitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c.RequestID == "" || c.ParticipantID == "" {
		return SubmitResult{}, fmt.Errorf("%w: request ID and participant ID are required", ErrInvalidParams)
	}
	if len(c.Commitment) == 0 || len(c.ShareDigest) == 0 {
		return SubmitResult{}, fmt.Errorf("%w: commitment and share digest are required", ErrInvalidParams)
	}
	cer, err := s.ceremonyLocked(ceremonyID)
	if err != nil {
		return SubmitResult{}, err
	}

	// 幂等检查先于一切状态校验：即使仪式此后已完成或取消，
	// 重试也必须拿到与首次一致的结果。
	hash := contentHash(c.Commitment, c.ShareDigest)
	if rec, ok := cer.Idem[c.RequestID]; ok {
		if rec.ContentHash != hash {
			return SubmitResult{}, fmt.Errorf("%w: request %q", ErrIdempotencyConflict, c.RequestID)
		}
		replayed := rec.Result
		replayed.Replayed = true
		s.auditLocked(AuditContributionReplayed, cer, rec.Result.Round, c.ParticipantID, "request="+c.RequestID)
		if err := s.persistLocked(); err != nil {
			return SubmitResult{}, err
		}
		return replayed, nil
	}

	if err := s.requireActiveForContributionLocked(cer); err != nil {
		s.auditLocked(AuditContributionRejected, cer, c.Round, c.ParticipantID, "request="+c.RequestID+" reason="+errReason(err))
		if perr := s.persistLocked(); perr != nil {
			return SubmitResult{}, perr
		}
		return SubmitResult{}, err
	}
	round := cer.CurrentRound()
	now := s.now()
	reject := func(err error, detail string) (SubmitResult, error) {
		s.auditLocked(AuditContributionRejected, cer, c.Round, c.ParticipantID, detail)
		if perr := s.persistLocked(); perr != nil {
			return SubmitResult{}, perr
		}
		return SubmitResult{}, err
	}
	if c.Round != round.Number {
		return reject(ErrStaleRound, fmt.Sprintf("request=%s round=%d current=%d", c.RequestID, c.Round, round.Number))
	}
	if !round.Has(c.ParticipantID) {
		return reject(ErrNotMember, "request="+c.RequestID)
	}
	if _, dup := round.Contributions[c.ParticipantID]; dup {
		return reject(ErrDuplicateContributor, "request="+c.RequestID)
	}

	c.ReceivedAt = now
	round.Contributions[c.ParticipantID] = c
	count := len(round.Contributions)
	res := SubmitResult{
		Accepted:      true,
		Round:         round.Number,
		ParticipantID: c.ParticipantID,
		RequestID:     c.RequestID,
		CurrentCount:  count,
		Threshold:     round.Threshold,
		ThresholdMet:  count >= round.Threshold,
	}
	cer.Idem[c.RequestID] = IdemRecord{ContentHash: hash, Result: res}
	s.auditLocked(AuditContributionAccepted, cer, round.Number, c.ParticipantID, fmt.Sprintf("request=%s count=%d/%d", c.RequestID, count, round.Threshold))
	if err := s.persistLocked(); err != nil {
		return SubmitResult{}, err
	}
	return res, nil
}

// ReplaceParticipants 在完成前替换参与者：显式开启新轮次，
// 旧轮次随之关闭，其未决贡献全部失效、不再计入门限。
func (s *Service) ReplaceParticipants(ceremonyID string, p ReplaceParams) (CeremonyView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cer, err := s.activeCeremonyLocked(ceremonyID)
	if err != nil {
		return CeremonyView{}, err
	}
	now := s.now()
	roster, err := validateRoster(p.Participants, p.Threshold, p.Deadline, now)
	if err != nil {
		return CeremonyView{}, err
	}
	old := cer.CurrentRound()
	old.Open = false
	cer.Rounds = append(cer.Rounds, Round{
		Number:        old.Number + 1,
		Participants:  roster,
		Threshold:     p.Threshold,
		Deadline:      p.Deadline,
		Open:          true,
		Contributions: make(map[string]Contribution),
	})
	s.auditLocked(AuditRoundOpened, cer, old.Number+1, p.Actor,
		fmt.Sprintf("replaces round=%d dropped=%d reason=%s", old.Number, len(old.Contributions), p.Reason))
	if err := s.persistLocked(); err != nil {
		return CeremonyView{}, err
	}
	return s.viewLocked(cer), nil
}

// Complete 在门限达成后完成仪式。
//
// 互斥锁保证并发下只有一个请求执行完成动作：冻结实际采用的贡献集合，
// 并写出唯一的密钥激活 outbox。其后的完成调用与额外贡献都不会改变结果；
// 重复完成幂等返回首次结果。
func (s *Service) Complete(ceremonyID string) (CompleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cer, err := s.ceremonyLocked(ceremonyID)
	if err != nil {
		return CompleteResult{}, err
	}
	if cer.Status == StatusCompleted {
		return s.completeResultLocked(cer, true)
	}
	if err := s.requireActiveLocked(cer); err != nil {
		return CompleteResult{}, err
	}
	round := cer.CurrentRound()
	if len(round.Contributions) < round.Threshold {
		return CompleteResult{}, fmt.Errorf("%w: have %d of %d", ErrThresholdNotMet, len(round.Contributions), round.Threshold)
	}

	now := s.now()
	adopted := sortedContributions(round.Contributions)
	round.Open = false
	cer.Adopted = adopted
	cer.Status = StatusCompleted
	cer.CompletedAt = now
	evt := OutboxEvent{
		ID:           cer.ID + "/key-activation",
		Type:         OutboxEventTypeKeyActivation,
		CeremonyID:   cer.ID,
		Round:        round.Number,
		Adopted:      adopted,
		Threshold:    round.Threshold,
		Participants: append([]string(nil), round.Participants...),
		CreatedAt:    now,
	}
	s.state.Outbox = append(s.state.Outbox, evt)
	s.auditLocked(AuditCompleted, cer, round.Number, "", fmt.Sprintf("adopted=%d", len(adopted)))
	s.auditLocked(AuditOutboxWritten, cer, round.Number, "", "outbox="+evt.ID)
	if err := s.persistLocked(); err != nil {
		return CompleteResult{}, err
	}
	return CompleteResult{
		CeremonyID: cer.ID,
		Round:      round.Number,
		Adopted:    adopted,
		Outbox:     evt,
	}, nil
}

// Cancel 取消进行中的仪式；重复取消幂等返回当前视图。
func (s *Service) Cancel(ceremonyID, actor, reason string) (CeremonyView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cer, err := s.ceremonyLocked(ceremonyID)
	if err != nil {
		return CeremonyView{}, err
	}
	if cer.Status == StatusCancelled {
		return s.viewLocked(cer), nil
	}
	if err := s.requireActiveLocked(cer); err != nil {
		return CeremonyView{}, err
	}
	now := s.now()
	cer.CurrentRound().Open = false
	cer.Status = StatusCancelled
	cer.CancelledAt = now
	cer.CancelReason = reason
	s.auditLocked(AuditCancelled, cer, cer.CurrentRound().Number, actor, "reason="+reason)
	if err := s.persistLocked(); err != nil {
		return CeremonyView{}, err
	}
	return s.viewLocked(cer), nil
}

// GetCeremony 查询仪式当前视图；超时状态在读取时惰性结算。
func (s *Service) GetCeremony(ceremonyID string) (CeremonyView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cer, err := s.ceremonyLocked(ceremonyID)
	if err != nil {
		return CeremonyView{}, err
	}
	// 读取时惰性结算超时。
	if cer.Status == StatusActive && s.now().After(cer.CurrentRound().Deadline) {
		_ = s.requireActiveLocked(cer) // 状态已被改为 TIMED_OUT
		if err := s.persistLocked(); err != nil {
			return CeremonyView{}, err
		}
	}
	return s.viewLocked(cer), nil
}

// AuditTrail 返回指定仪式按序排列的审计记录。
func (s *Service) AuditTrail(ceremonyID string) ([]AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.ceremonyLocked(ceremonyID); err != nil {
		return nil, err
	}
	var out []AuditEvent
	for _, e := range s.state.Audit {
		if e.CeremonyID == ceremonyID {
			out = append(out, e)
		}
	}
	return out, nil
}

// Outbox 返回全部 outbox 事件；每个仪式至多一条密钥激活事件。
func (s *Service) Outbox() []OutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]OutboxEvent, len(s.state.Outbox))
	copy(out, s.state.Outbox)
	return out
}

// --- 内部辅助（调用方必须已持有 s.mu） ---

func (s *Service) ceremonyLocked(id string) (*Ceremony, error) {
	cer, ok := s.state.Ceremonies[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrCeremonyNotFound, id)
	}
	return cer, nil
}

// activeCeremonyLocked 找到仪式并结算惰性超时，要求仪式处于进行中。
func (s *Service) activeCeremonyLocked(id string) (*Ceremony, error) {
	cer, err := s.ceremonyLocked(id)
	if err != nil {
		return nil, err
	}
	if err := s.requireActiveLocked(cer); err != nil {
		return nil, err
	}
	return cer, nil
}

// requireActiveLocked 结算惰性超时，并在仪式非进行中时返回 ErrCeremonyClosed。
func (s *Service) requireActiveLocked(cer *Ceremony) error {
	if cer.Status != StatusActive {
		return fmt.Errorf("%w: status=%s", ErrCeremonyClosed, cer.Status)
	}
	if s.expireIfPastDeadlineLocked(cer) {
		return fmt.Errorf("%w: status=%s", ErrCeremonyClosed, cer.Status)
	}
	return nil
}

// requireActiveForContributionLocked 与 requireActiveLocked 类似，
// 但对“仪式仍进行中、仅本次贡献晚于截止时间”的情况返回 ErrDeadlineExceeded，
// 以便调用方区分轮次超时与仪式终态。
func (s *Service) requireActiveForContributionLocked(cer *Ceremony) error {
	if cer.Status != StatusActive {
		return fmt.Errorf("%w: status=%s", ErrCeremonyClosed, cer.Status)
	}
	if s.expireIfPastDeadlineLocked(cer) {
		return ErrDeadlineExceeded
	}
	return nil
}

// expireIfPastDeadlineLocked 在超过截止时间时把仪式结算为 TIMED_OUT，返回是否发生了结算。
func (s *Service) expireIfPastDeadlineLocked(cer *Ceremony) bool {
	round := cer.CurrentRound()
	if !s.now().After(round.Deadline) {
		return false
	}
	round.Open = false
	cer.Status = StatusTimedOut
	cer.TimedOutAt = s.now()
	s.auditLocked(AuditTimedOut, cer, round.Number, "",
		fmt.Sprintf("deadline=%s count=%d/%d", round.Deadline.Format(time.RFC3339), len(round.Contributions), round.Threshold))
	return true
}

// errReason 提取哨兵错误的可读原因，用于审计详情。
func errReason(err error) string {
	switch {
	case errors.Is(err, ErrCeremonyClosed):
		return "ceremony_closed"
	case errors.Is(err, ErrDeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "rejected"
	}
}

func (s *Service) completeResultLocked(cer *Ceremony, already bool) (CompleteResult, error) {
	var evt OutboxEvent
	found := false
	for _, e := range s.state.Outbox {
		if e.CeremonyID == cer.ID {
			evt = e
			found = true
			break
		}
	}
	if !found {
		return CompleteResult{}, fmt.Errorf("keyshards: ceremony %q completed without outbox event", cer.ID)
	}
	adopted := make([]Contribution, len(cer.Adopted))
	copy(adopted, cer.Adopted)
	return CompleteResult{
		CeremonyID:  cer.ID,
		Round:       evt.Round,
		Adopted:     adopted,
		Outbox:      evt,
		AlreadyDone: already,
	}, nil
}

func (s *Service) viewLocked(cer *Ceremony) CeremonyView {
	round := cer.CurrentRound()
	return CeremonyView{
		ID:                cer.ID,
		Status:            cer.Status,
		CurrentRound:      round.Number,
		Participants:      append([]string(nil), round.Participants...),
		Threshold:         round.Threshold,
		Deadline:          round.Deadline,
		ContributionCount: len(round.Contributions),
		AdoptedCount:      len(cer.Adopted),
		CreatedAt:         cer.CreatedAt,
		CompletedAt:       cer.CompletedAt,
		CancelledAt:       cer.CancelledAt,
		CancelReason:      cer.CancelReason,
		TimedOutAt:        cer.TimedOutAt,
	}
}

func (s *Service) auditLocked(typ string, cer *Ceremony, round int, actor, detail string) {
	s.state.Seq++
	s.state.Audit = append(s.state.Audit, AuditEvent{
		Seq:        s.state.Seq,
		Type:       typ,
		CeremonyID: cer.ID,
		Round:      round,
		Actor:      actor,
		Detail:     detail,
		At:         s.now(),
	})
}

func (s *Service) persistLocked() error {
	return s.store.Save(s.state)
}

// contentHash 计算幂等内容指纹：只覆盖承诺与摘要，不含请求号与时间。
func contentHash(commitment, shareDigest []byte) string {
	h := sha256.New()
	h.Write(commitment)
	h.Write(shareDigest)
	return hex.EncodeToString(h.Sum(nil))
}

func newCeremonyID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("keyshards: generate ceremony ID: %v", err))
	}
	return "cer-" + hex.EncodeToString(b[:])
}
