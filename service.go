package keyshards

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Service 是门限密钥分片仪式的状态协调层。
//
// 它不实现任何密码学计算：承诺（Commitment）与分片摘要（ShardDigest）
// 均由外部密码学组件产出，协调层只做轮次/成员/门限/状态的编排与持久化，
// 并且任何接口都不接受或记录秘密分片明文。
type Service struct {
	repo Repository
	now  func() time.Time
}

// NewService 创建协调服务。now 为 nil 时使用系统时钟。
func NewService(repo Repository, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{repo: repo, now: now}
}

// CreateInput 创建仪式的输入：参与者、门限、截止时间在此冻结。
type CreateInput struct {
	ID        string
	Members   []string  // 成员 ID 列表；内部去重并按首次出现顺序冻结
	Threshold int       // 所需有效贡献数；创建后不可变
	Deadline  time.Time // 截止时间；超过后任何贡献/替换/完成都不被接受
}

// CreateCeremony 冻结参与者、门限、截止时间并开启第 1 轮，轮次号从 1 递增。
func (s *Service) CreateCeremony(ctx context.Context, in CreateInput) (*Ceremony, error) {
	if in.ID == "" {
		return nil, errInvalid("ceremony id is required")
	}
	members, err := normalizeMembers(in.Members)
	if err != nil {
		return nil, err
	}
	if in.Threshold < 1 || in.Threshold > len(members) {
		return nil, errInvalid("threshold must be in [1, member count]")
	}
	if in.Deadline.IsZero() {
		return nil, errInvalid("deadline is required")
	}

	now := s.now()
	c := &Ceremony{
		ID:          in.ID,
		Threshold:   in.Threshold,
		Deadline:    in.Deadline.UTC(),
		CreatedAt:   now,
		Status:      StatusActive,
		Rounds:      []*Round{newRound(1, members, in.Threshold, now)},
		Idempotency: map[string]*IdemRecord{},
	}
	ev := AuditEvent{
		At:          now,
		CeremonyID:  in.ID,
		RoundNumber: 1,
		Kind:        AuditCreated,
		Detail: map[string]any{
			"members":   members,
			"threshold": in.Threshold,
			"deadline":  in.Deadline.UTC().Format(time.RFC3339Nano),
		},
	}
	if err := s.repo.Create(ctx, c, []AuditEvent{ev}); err != nil {
		return nil, err
	}
	return cloneCeremony(c), nil
}

// ContributeInput 提交一次贡献。
type ContributeInput struct {
	CeremonyID string
	// RequestID 幂等请求号：同号同内容重试返回原结果，同号异内容返回 ErrConflict。
	RequestID string
	// RoundNumber 客户端针对的轮次；非当前轮次（通常是替换参与者后的旧轮次）一律拒绝。
	RoundNumber  int
	Contribution Contribution
}

// ContributeResult 是提交结果。
type ContributeResult struct {
	RoundNumber       int
	ContributionCount int // 当前轮有效贡献数
	Threshold         int
	// Replayed 为 true 表示这是同请求号同内容的重试，返回首次提交的结果，未重复计入门限。
	Replayed bool
}

// SubmitContribution 接收外部密码学组件给出的承诺与分片摘要。
//
// 拒绝规则（均不计入门限）：非成员、旧轮次、超过截止时间、仪式终态、
// 同一参与者当轮重复贡献；同请求号不同内容判为幂等冲突。
func (s *Service) SubmitContribution(ctx context.Context, in ContributeInput) (*ContributeResult, error) {
	if in.CeremonyID == "" || in.RequestID == "" {
		return nil, errInvalid("ceremony id and request id are required")
	}
	if in.RoundNumber < 1 {
		return nil, errInvalid("round number is required")
	}
	if err := validateContribution(in.Contribution); err != nil {
		return nil, err
	}
	fp := fingerprint(in.Contribution)
	idempKey := idemContributionPrefix + in.RequestID

	var result *ContributeResult
	err := s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		r := c.CurrentRound()

		// 幂等判定先于一切状态检查：同请求号是同一次提交的重试，
		// 即使其间发生了轮次替换、过期或完成，也必须返回首次结果而不是报错。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != fp || rec.RoundNumber != in.RoundNumber {
				evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "idempotency_conflict",
					ErrConflict, map[string]any{"request_id": in.RequestID})
				return nil, evs, errReject
			}
			result = &ContributeResult{
				RoundNumber:       rec.RoundNumber,
				ContributionCount: rec.ContributionCount,
				Threshold:         c.Threshold,
				Replayed:          true,
			}
			return c, nil, nil
		}

		ev, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		var events []AuditEvent
		if ev != nil {
			events = append(events, *ev)
			// 已惰性过期，拒绝本次贡献。
			return c, events, ErrDeadlineExceeded
		}
		if in.RoundNumber != r.Number {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "stale_round",
				ErrStaleRound, map[string]any{"requested_round": in.RoundNumber, "current_round": r.Number})
			return nil, evs, errReject
		}
		if !isMember(r.Members, in.Contribution.ParticipantID) {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "not_member", ErrNotMember, nil)
			return nil, evs, errReject
		}
		if _, dup := r.Contributions[in.Contribution.ParticipantID]; dup {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "duplicate_contribution",
				ErrAlreadyContributed, nil)
			return nil, evs, errReject
		}

		cv := cloneContribution(&in.Contribution)
		r.Contributions[cv.ParticipantID] = cv
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint:       fp,
			RoundNumber:       r.Number,
			ContributionCount: len(r.Contributions),
		}

		events = []AuditEvent{{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditContributed,
			Actor:       cv.ParticipantID,
			Detail: map[string]any{
				"request_id":         in.RequestID,
				"commitment":         hex.EncodeToString(cv.Commitment),
				"shard_digest":       hex.EncodeToString(cv.ShardDigest),
				"contribution_count": len(r.Contributions),
				"threshold":          c.Threshold,
			},
		}}
		result = &ContributeResult{
			RoundNumber:       r.Number,
			ContributionCount: len(r.Contributions),
			Threshold:         c.Threshold,
		}
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReplaceParticipantsInput 在完成前替换参与者集合：必须显式开启新一轮。
type ReplaceParticipantsInput struct {
	CeremonyID string
	// RequestID 幂等请求号；同号同成员集合重试返回同一新轮次。
	RequestID  string
	NewMembers []string
	// Reason 记录到审计日志的替换原因。
	Reason string
}

// ReplaceParticipants 冻结新成员集合并开启下一轮；旧轮次的全部贡献即刻失效，
// 不再计入任何门限。门限沿用创建时冻结的值。
func (s *Service) ReplaceParticipants(ctx context.Context, in ReplaceParticipantsInput) (roundNumber int, err error) {
	if in.CeremonyID == "" || in.RequestID == "" {
		return 0, errInvalid("ceremony id and request id are required")
	}
	members, err := normalizeMembers(in.NewMembers)
	if err != nil {
		return 0, err
	}
	idempKey := idemReplacePrefix + in.RequestID
	newFp := membersFingerprint(members)

	err = s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		// 同请求号重试先于状态检查：替换已生效则原样返回当时开启的轮次号；
		// 即使仪式其后完成/取消/过期，重试也不应报终态错误。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != newFp {
				return nil, nil, ErrConflict
			}
			roundNumber = rec.RoundNumber
			return c, nil, nil
		}
		ev, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		var events []AuditEvent
		if ev != nil {
			events = append(events, *ev)
			return c, events, ErrDeadlineExceeded
		}
		if c.Threshold > len(members) {
			return nil, nil, errInvalid("member count after replacement is below frozen threshold")
		}

		old := c.CurrentRound()
		next := newRound(old.Number+1, members, c.Threshold, now)
		c.Rounds = append(c.Rounds, next)
		c.Idempotency[idempKey] = &IdemRecord{Fingerprint: newFp, RoundNumber: next.Number}

		events = []AuditEvent{{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: next.Number,
			Kind:        AuditReplaced,
			Actor:       in.RequestID,
			Detail: map[string]any{
				"previous_round":            old.Number,
				"invalidated_contributions": len(old.Contributions),
				"members":                   members,
				"threshold":                 c.Threshold,
				"reason":                    in.Reason,
			},
		}}
		roundNumber = next.Number
		return c, events, nil
	})
	return roundNumber, err
}

// CompleteInput 完成仪式。
type CompleteInput struct {
	CeremonyID string
	// RequestID 幂等请求号；完成后用同一请求号重试会返回已冻结的 outbox。
	RequestID string
	// KeyID 待激活密钥标识；Payload 为外部密码学组件给出的聚合激活载荷。
	KeyID   string
	Payload []byte
}

// Complete 在门限达成后完成仪式：冻结实际采用的贡献集合并写出唯一的
// 密钥激活 outbox。并发完成请求在仓储层串行化，只有一个能成功；
// 其后到达的任何贡献都不能改变结果（仪式进入终态）。
func (s *Service) Complete(ctx context.Context, in CompleteInput) (*Outbox, error) {
	if in.CeremonyID == "" || in.RequestID == "" || in.KeyID == "" {
		return nil, errInvalid("ceremony id, request id and key id are required")
	}
	idempKey := idemCompletePrefix + in.RequestID
	completeFp := completeFingerprint(in.KeyID, in.Payload)

	var outbox *Outbox
	err := s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		// 已完成：同请求号同内容重试原样返回 outbox；其余完成请求一律拒绝。
		// 此判定必须先于惰性过期检查，否则重放会被误判为终态冲突。
		if c.Status == StatusCompleted {
			if rec, ok := c.Idempotency[idempKey]; ok && rec.Fingerprint == completeFp {
				outbox = cloneOutbox(c.Outbox)
				return c, nil, nil
			}
			return nil, nil, ErrCeremonyTerminal
		}
		ev, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		var events []AuditEvent
		if ev != nil {
			events = append(events, *ev)
			return c, events, ErrDeadlineExceeded
		}
		if c.Status != StatusActive {
			return nil, nil, ErrCeremonyTerminal
		}

		r := c.CurrentRound()
		if len(r.Contributions) < c.Threshold {
			return nil, nil, ErrThresholdNotReached
		}

		adopted := contributionsInMemberOrder(r)
		ob := &Outbox{
			CeremonyID:    c.ID,
			RoundNumber:   r.Number,
			KeyID:         in.KeyID,
			Payload:       append([]byte(nil), in.Payload...),
			Contributions: adopted,
			ActivatedAt:   now,
		}
		c.Status = StatusCompleted
		c.Outbox = ob
		c.Idempotency[idempKey] = &IdemRecord{Fingerprint: completeFp, RoundNumber: r.Number}

		events = []AuditEvent{{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditCompleted,
			Actor:       in.RequestID,
			Detail: map[string]any{
				"key_id":          in.KeyID,
				"adopted_count":   len(adopted),
				"adopted_members": participantIDs(adopted),
			},
		}}
		outbox = cloneOutbox(ob)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return outbox, nil
}

// Cancel 取消仪式。取消后进入终态，不会写出 outbox。
func (s *Service) Cancel(ctx context.Context, ceremonyID, reason string) error {
	if ceremonyID == "" {
		return errInvalid("ceremony id is required")
	}
	return s.repo.Update(ctx, ceremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		ev, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		var events []AuditEvent
		if ev != nil {
			events = append(events, *ev)
			return c, events, ErrDeadlineExceeded
		}
		c.Status = StatusCanceled
		c.CancelReason = reason
		events = []AuditEvent{{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: c.CurrentRound().Number,
			Kind:        AuditCanceled,
			Detail:      map[string]any{"reason": reason},
		}}
		return c, events, nil
	})
}

// GetCeremony 返回仪式快照（必要时把超过截止时间的活跃仪式惰性标记为过期）。
func (s *Service) GetCeremony(ctx context.Context, id string) (*Ceremony, error) {
	if id == "" {
		return nil, errInvalid("ceremony id is required")
	}
	_ = s.repo.Update(ctx, id, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		ev, err := expireIfDue(c, s.now())
		if err != nil || ev == nil {
			return c, nil, err
		}
		return c, []AuditEvent{*ev}, nil
	})
	c, err := s.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ListCeremonies 返回全部仪式快照。
func (s *Service) ListCeremonies(ctx context.Context) ([]*Ceremony, error) {
	return s.repo.List(ctx)
}

// Audit 查询审计事件，语义同 Repository.Audit。
func (s *Service) Audit(ctx context.Context, ceremonyID string, fromSeq int64, limit int) ([]AuditEvent, error) {
	return s.repo.Audit(ctx, ceremonyID, fromSeq, limit)
}

// ---- 内部辅助 ----

const (
	idemContributionPrefix = "contrib:"
	idemReplacePrefix      = "replace:"
	idemCompletePrefix     = "complete:"
)

// expireIfDue 若活跃仪式已过截止时间则迁移到 expired，并返回应记录的审计事件。
// 返回 nil 事件表示无需迁移。终态非活跃仪式返回 ErrCeremonyTerminal。
func expireIfDue(c *Ceremony, now time.Time) (*AuditEvent, error) {
	if c.Status != StatusActive {
		if c.Status.IsTerminal() {
			return nil, ErrCeremonyTerminal
		}
		return nil, nil
	}
	if !now.After(c.Deadline) {
		return nil, nil
	}
	c.Status = StatusExpired
	return &AuditEvent{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: c.CurrentRound().Number,
		Kind:        AuditExpired,
		Detail:      map[string]any{"deadline": c.Deadline.Format(time.RFC3339Nano)},
	}, nil
}

// reject 构造一条 rejected 审计事件连同业务错误；状态不变、事件留痕。
// 调用方应返回 (nil, []AuditEvent{ev}, cause)。
func (s *Service) reject(c *Ceremony, r *Round, now time.Time, actor, reason string, cause error, extra map[string]any) ([]AuditEvent, error) {
	detail := map[string]any{"reason": reason}
	for k, v := range extra {
		detail[k] = v
	}
	return []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: r.Number,
		Kind:        AuditRejected,
		Actor:       actor,
		Detail:      detail,
	}}, cause
}

func validateContribution(cv Contribution) error {
	if cv.ParticipantID == "" {
		return errInvalid("participant id is required")
	}
	if len(cv.Commitment) == 0 {
		return errInvalid("commitment is required")
	}
	if len(cv.ShardDigest) == 0 {
		return errInvalid("shard digest is required")
	}
	return nil
}

func fingerprint(cv Contribution) string {
	h := sha256.New()
	h.Write([]byte(cv.ParticipantID))
	h.Write([]byte{0})
	h.Write(cv.Commitment)
	h.Write([]byte{0})
	h.Write(cv.ShardDigest)
	return hex.EncodeToString(h.Sum(nil))
}

func membersFingerprint(members []string) string {
	h := sha256.New()
	for _, m := range members {
		h.Write([]byte(m))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func completeFingerprint(keyID string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(keyID))
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func isMember(members []string, id string) bool {
	for _, m := range members {
		if m == id {
			return true
		}
	}
	return false
}

func normalizeMembers(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, errInvalid("at least one member is required")
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, m := range in {
		if m == "" {
			return nil, errInvalid("member id must not be empty")
		}
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	return out, nil
}

func newRound(number int, members []string, threshold int, at time.Time) *Round {
	return &Round{
		Number:        number,
		Members:       append([]string(nil), members...),
		Threshold:     threshold,
		Contributions: map[string]*Contribution{},
		StartedAt:     at,
	}
}

func contributionsInMemberOrder(r *Round) []Contribution {
	out := make([]Contribution, 0, len(r.Contributions))
	for _, m := range r.Members {
		if cv, ok := r.Contributions[m]; ok {
			out = append(out, *cloneContribution(cv))
		}
	}
	return out
}

func participantIDs(cs []Contribution) []string {
	out := make([]string, len(cs))
	for i, cv := range cs {
		out[i] = cv.ParticipantID
	}
	return out
}

// invalidError 包装 ErrInvalidArgument 并附说明。
type invalidError struct{ msg string }

func (e *invalidError) Error() string        { return "keyshards: invalid argument: " + e.msg }
func (e *invalidError) Is(target error) bool { return target == ErrInvalidArgument }

func errInvalid(msg string) error { return &invalidError{msg: msg} }
