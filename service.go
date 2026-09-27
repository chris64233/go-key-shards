package keyshards

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
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
	Threshold int       // 所需有效贡献数，也是替换参与者所需的同意人数；创建后不可变
	Deadline  time.Time // 第 1 轮截止时间；替换生效后每轮重新冻结
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
	deadline := in.Deadline.UTC()
	c := &Ceremony{
		ID:           in.ID,
		Threshold:    in.Threshold,
		Deadline:     deadline,
		CreatedAt:    now,
		Status:       StatusActive,
		Rounds:       []*Round{newRound(1, members, in.Threshold, now, deadline)},
		Replacements: map[string]*Replacement{},
		Idempotency:  map[string]*IdemRecord{},
	}
	ev := AuditEvent{
		At:          now,
		CeremonyID:  in.ID,
		RoundNumber: 1,
		Kind:        AuditCreated,
		Detail: map[string]any{
			"members":   members,
			"threshold": in.Threshold,
			"deadline":  deadline.Format(time.RFC3339Nano),
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
// 拒绝规则（均不计入门限）：非成员、旧轮次、超过当前轮截止时间、仪式终态、
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
	fp := contributionFingerprint(in.Contribution)
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

		expired, expireEvents, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
			// 已惰性过期：提交过期迁移（含同事务通知），拒绝本次贡献。
			return c, expireEvents, ErrDeadlineExceeded
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

		events := []AuditEvent{{
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

// InitiateReplacementInput 发起一次参与者替换。
type InitiateReplacementInput struct {
	CeremonyID string
	// RequestID 替换请求号；仪式内唯一，既是幂等键也是 Replacement.ID，
	// 后续批准/查询/撤销都引用它。
	RequestID string
	// RequestedBy 发起人，必须是发起时当前轮的冻结成员；其同意自动计入。
	RequestedBy string
	// NewMembers 生效时为新轮冻结的完整成员集合（不是增量补丁）。
	NewMembers []string
	// NewDeadline 为新轮重新冻结的截止时间，必须晚于发起时刻。
	NewDeadline time.Time
	// Reason 记录到审计与通知的替换原因。
	Reason string
}

// InitiateReplacement 发起参与者替换并进入 pending：记录新成员集合与新截止时间，
// 发起人的同意自动计入。当同意人数达到门限（threshold 为 1 时即发起瞬间）时，
// 在同一事务内开启新轮并作废旧轮全部贡献。
//
// 同请求号同内容重试（即使跨越生效、终态）原样返回首次的 Replacement 快照；
// 同请求号不同内容返回 ErrConflict。
func (s *Service) InitiateReplacement(ctx context.Context, in InitiateReplacementInput) (*Replacement, error) {
	if in.CeremonyID == "" || in.RequestID == "" {
		return nil, errInvalid("ceremony id and request id are required")
	}
	if in.RequestedBy == "" {
		return nil, errInvalid("requested by is required")
	}
	members, err := normalizeMembers(in.NewMembers)
	if err != nil {
		return nil, err
	}
	if in.NewDeadline.IsZero() {
		return nil, errInvalid("new deadline is required")
	}
	newDeadline := in.NewDeadline.UTC()
	idempKey := idemReplacePrefix + in.RequestID
	fp := replacementFingerprint(in.RequestedBy, members, newDeadline, in.Reason)

	var rep *Replacement
	err = s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()

		// 同请求号重试先于状态检查：已生效则原样返回请求记录，
		// 即使仪式其后完成/取消/过期，重试也不应报终态错误。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != fp {
				return nil, nil, ErrConflict
			}
			existing := c.Replacements[in.RequestID]
			if existing == nil {
				return nil, nil, ErrConflict
			}
			rep = cloneReplacement(existing)
			return c, nil, nil
		}

		expired, events, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
			return c, events, ErrDeadlineExceeded
		}
		r := c.CurrentRound()
		if !isMember(r.Members, in.RequestedBy) {
			evs, errReject := s.reject(c, r, now, in.RequestedBy, "initiator_not_member", ErrNotMember,
				map[string]any{"request_id": in.RequestID})
			return nil, evs, errReject
		}
		if c.Threshold > len(members) {
			return nil, events, errInvalid("member count after replacement is below frozen threshold")
		}
		if equalStringSets(r.Members, members) {
			return nil, events, errInvalid("new members are identical to the current round members")
		}
		if !newDeadline.After(now) {
			return nil, events, errInvalid("new deadline must be in the future")
		}
		// 只有继续参与者（新旧成员集合交集）拥有批准权：被替换出去的人
		// 不参与计数。交集人数必须不少于门限，否则替换不可能获批。
		// 发起人若继续参与则其同意自动计入；离场者本人可以发起但不计票。
		continuing := intersectMembers(r.Members, members)
		if len(continuing) < c.Threshold {
			return nil, events, errInvalid("fewer than threshold participants continue after replacement")
		}

		approvers := []string{}
		if isMember(continuing, in.RequestedBy) {
			approvers = []string{in.RequestedBy}
		}
		req := &Replacement{
			ID:          in.RequestID,
			Status:      ReplacementPending,
			RequestedBy: in.RequestedBy,
			Reason:      in.Reason,
			NewMembers:  members,
			NewDeadline: newDeadline,
			TargetRound: r.Number,
			Approvers:   approvers,
			CreatedAt:   now,
		}
		c.Replacements[in.RequestID] = req
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint:   fp,
			ReplacementID: in.RequestID,
		}

		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditReplacementInitiated,
			Actor:       in.RequestedBy,
			Detail: map[string]any{
				"request_id":   in.RequestID,
				"new_members":  members,
				"new_deadline": newDeadline.Format(time.RFC3339Nano),
				"reason":       in.Reason,
			},
		})
		addNotification(c, Notification{
			At: now, CeremonyID: c.ID, Type: NtfReplacementInitiated,
			RoundNumber: r.Number, ReplacementID: in.RequestID,
			Detail: map[string]any{
				"requested_by": in.RequestedBy,
				"new_members":  members,
				"new_deadline": newDeadline.Format(time.RFC3339Nano),
				"reason":       in.Reason,
			},
		})

		// 门限为 1：发起人本人同意即生效，同一事务内开启新轮。
		if len(req.Approvers) >= c.Threshold {
			events = append(events, applyReplacementApproval(c, req, now)...)
		}
		rep = cloneReplacement(req)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// ApprovalInput 是对某个待决替换请求的同意。
type ApprovalInput struct {
	CeremonyID string
	// RequestID 被批准的替换请求号（InitiateReplacementInput.RequestID）。
	RequestID string
	// VoteRequestID 本次同意动作自身的幂等请求号。
	VoteRequestID string
	// Voter 同意人，必须是当前轮的冻结成员，且同一请求每人只能同意一次。
	Voter string
}

// ApproveReplacement 记录一名成员对替换请求的同意。同意人数（含发起人）达到
// 门限的瞬间，在同一事务内：开启新轮、重新冻结成员与截止时间、快照作废旧轮贡献、
// 关闭其他待决替换请求，并写出 replacement_approved 通知。
//
// 返回的 Replacement 始终是批准动作执行后的最新快照。
func (s *Service) ApproveReplacement(ctx context.Context, in ApprovalInput) (*Replacement, error) {
	if in.CeremonyID == "" || in.RequestID == "" || in.VoteRequestID == "" || in.Voter == "" {
		return nil, errInvalid("ceremony id, request id, vote request id and voter are required")
	}
	idempKey := idemApprovalPrefix + in.VoteRequestID
	fp := approvalFingerprint(in.RequestID, in.Voter)

	var rep *Replacement
	err := s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()

		// 同意动作的同请求号重试先于状态检查，返回该替换请求的当前快照。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != fp {
				return nil, nil, ErrConflict
			}
			existing := c.Replacements[rec.ReplacementID]
			if existing == nil || existing.ID != in.RequestID {
				return nil, nil, ErrConflict
			}
			rep = cloneReplacement(existing)
			return c, nil, nil
		}

		expired, events, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
			return c, events, ErrDeadlineExceeded
		}
		req, ok := c.Replacements[in.RequestID]
		if !ok {
			return nil, events, ErrReplacementNotFound
		}
		r := c.CurrentRound()
		if req.Status != ReplacementPending || req.TargetRound != r.Number {
			// 已终态或已被其他生效请求取代：关闭并拒绝（防御性路径，
			// 正常情况下生效时已关闭其余 pending 请求）。
			if req.Status == ReplacementPending {
				events = append(events, closeReplacement(c, req, now, "superseded")...)
				return c, events, ErrReplacementFinal
			}
			return nil, events, ErrReplacementFinal
		}
		if !isMember(r.Members, in.Voter) {
			evs, errReject := s.reject(c, r, now, in.Voter, "approver_not_member", ErrNotMember,
				map[string]any{"request_id": in.RequestID})
			return nil, evs, errReject
		}
		// 批准权属于继续参与者（新旧成员集合交集）：被替换出去的成员的投票拒绝。
		if !isMember(req.NewMembers, in.Voter) {
			evs, errReject := s.reject(c, r, now, in.Voter, "approver_not_continuing",
				ErrApproverNotContinuing, map[string]any{"request_id": in.RequestID})
			return nil, evs, errReject
		}
		if isMember(req.Approvers, in.Voter) {
			evs, errReject := s.reject(c, r, now, in.Voter, "duplicate_approval", ErrAlreadyApproved,
				map[string]any{"request_id": in.RequestID})
			return nil, evs, errReject
		}

		req.Approvers = append(req.Approvers, in.Voter)
		sort.Strings(req.Approvers)
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint:   fp,
			ReplacementID: req.ID,
		}
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditReplacementApproval,
			Actor:       in.Voter,
			Detail: map[string]any{
				"request_id":     req.ID,
				"approver_count": len(req.Approvers),
				"threshold":      c.Threshold,
			},
		})

		if len(req.Approvers) >= c.Threshold {
			// 生效瞬间新轮截止时间已过期（pending 跨越过久）：该请求不可能再生效，
			// 在同一事务内关闭它，避免后续批准反复撞入死局。
			if !req.NewDeadline.After(now) {
				events = append(events, closeReplacement(c, req, now, "new_deadline_elapsed")...)
				return c, events, ErrDeadlineExceeded
			}
			events = append(events, applyReplacementApproval(c, req, now)...)
		}
		rep = cloneReplacement(req)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// WithdrawReplacement 由发起人撤销一个尚未生效的替换请求（pending -> rejected）。
// 对已 rejected 的请求重复撤销幂等成功；approved/closed 请求返回 ErrReplacementFinal。
func (s *Service) WithdrawReplacement(ctx context.Context, ceremonyID, requestID, operator string) error {
	if ceremonyID == "" || requestID == "" || operator == "" {
		return errInvalid("ceremony id, request id and operator are required")
	}
	return s.repo.Update(ctx, ceremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		expired, events, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
			return c, events, ErrDeadlineExceeded
		}
		req, ok := c.Replacements[requestID]
		if !ok {
			return nil, events, ErrReplacementNotFound
		}
		switch req.Status {
		case ReplacementRejected:
			return c, events, nil
		case ReplacementPending:
		default:
			return nil, events, ErrReplacementFinal
		}
		if req.RequestedBy != operator {
			return nil, events, errInvalid("only the initiator may withdraw the replacement")
		}
		req.Status = ReplacementRejected
		req.DecidedAt = now
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: req.TargetRound,
			Kind:        AuditReplacementWithdrawn,
			Actor:       operator,
			Detail:      map[string]any{"request_id": req.ID},
		})
		addNotification(c, Notification{
			At: now, CeremonyID: c.ID, Type: NtfReplacementRejected,
			RoundNumber: req.TargetRound, ReplacementID: req.ID,
			Detail: map[string]any{"requested_by": operator, "reason": "withdrawn"},
		})
		return c, events, nil
	})
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

// Complete 在当前轮门限达成后完成仪式：冻结实际采用的贡献集合并写出唯一的
// 密钥激活 outbox。并发完成请求在仓储层串行化，只有一个能成功；
// 其后到达的任何贡献都不能改变结果（仪式进入终态）。
//
// 完成的同一事务内关闭全部待决替换请求并写出通知，因此“替换生效”与“完成”
// 在并发下只会产生一个终态。
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
		expired, events, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
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

		// 与终态同一事务：关闭待决替换、追加审计与通知。
		events = append(events, closePendingReplacements(c, now, "ceremony_completed", "")...)
		events = append(events, AuditEvent{
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
		})
		addNotification(c, Notification{
			At: now, CeremonyID: c.ID, Type: NtfCeremonyCompleted, RoundNumber: r.Number,
			Detail: map[string]any{"key_id": in.KeyID, "round_number": r.Number},
		})
		outbox = cloneOutbox(ob)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return outbox, nil
}

// Cancel 取消仪式。取消后进入终态，不会写出 outbox；
// 待决替换请求在同一事务内关闭。
func (s *Service) Cancel(ctx context.Context, ceremonyID, reason string) error {
	if ceremonyID == "" {
		return errInvalid("ceremony id is required")
	}
	return s.repo.Update(ctx, ceremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		expired, events, err := expireIfDue(c, now)
		if err != nil {
			return nil, nil, err
		}
		if expired {
			return c, events, ErrDeadlineExceeded
		}
		c.Status = StatusCanceled
		c.CancelReason = reason
		events = append(events, closePendingReplacements(c, now, "ceremony_canceled", "")...)
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: c.CurrentRound().Number,
			Kind:        AuditCanceled,
			Detail:      map[string]any{"reason": reason},
		})
		return c, events, nil
	})
}

// GetCeremony 返回仪式快照（必要时把当前轮超过截止时间的活跃仪式惰性标记为过期）。
func (s *Service) GetCeremony(ctx context.Context, id string) (*Ceremony, error) {
	if id == "" {
		return nil, errInvalid("ceremony id is required")
	}
	_ = s.repo.Update(ctx, id, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		expired, events, err := expireIfDue(c, s.now())
		if err != nil || !expired {
			return c, nil, err
		}
		return c, events, nil
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

// GetReplacement 返回一次替换请求的完整记录（新旧轮次、批准人、失效贡献快照）。
func (s *Service) GetReplacement(ctx context.Context, ceremonyID, requestID string) (*Replacement, error) {
	if ceremonyID == "" || requestID == "" {
		return nil, errInvalid("ceremony id and request id are required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	req, ok := c.Replacements[requestID]
	if !ok {
		return nil, ErrReplacementNotFound
	}
	return cloneReplacement(req), nil
}

// ListReplacements 按发起顺序列出仪式的全部替换请求（含 pending/approved/rejected/closed）。
func (s *Service) ListReplacements(ctx context.Context, ceremonyID string) ([]*Replacement, error) {
	if ceremonyID == "" {
		return nil, errInvalid("ceremony id is required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	reps := make([]*Replacement, 0, len(c.Replacements))
	for _, req := range c.Replacements {
		reps = append(reps, req)
	}
	sort.Slice(reps, func(i, j int) bool {
		if !reps[i].CreatedAt.Equal(reps[j].CreatedAt) {
			return reps[i].CreatedAt.Before(reps[j].CreatedAt)
		}
		return reps[i].ID < reps[j].ID
	})
	out := make([]*Replacement, len(reps))
	for i, req := range reps {
		out[i] = cloneReplacement(req)
	}
	return out, nil
}

// Notifications 返回与状态迁移同事务保存的通知（seq 从 fromSeq 之后开始，
// 0 表示从头；limit <= 0 表示不限），供外部投递组件按序消费。
func (s *Service) Notifications(ctx context.Context, ceremonyID string, fromSeq int64, limit int) ([]Notification, error) {
	if ceremonyID == "" {
		return nil, errInvalid("ceremony id is required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	var out []Notification
	for _, n := range c.Notifications {
		if n.Seq <= fromSeq {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, cloneNotification(n))
	}
	return out, nil
}

// ---- 内部辅助 ----

const (
	idemContributionPrefix = "contrib:"
	idemReplacePrefix      = "replace:"
	idemApprovalPrefix     = "approve:"
	idemCompletePrefix     = "complete:"
)

// expireIfDue 若活跃仪式的当前轮已过截止时间则迁移到 expired，
// 并在同一状态变更内关闭全部待决替换请求、写出通知与审计事件。
// expired 为 true 表示本次调用发生了迁移：调用方必须原样提交状态
// （返回非 nil ceremony）并以 ErrDeadlineExceeded 结束当次操作。
// 终态非活跃仪式返回 ErrCeremonyTerminal。
func expireIfDue(c *Ceremony, now time.Time) (expired bool, events []AuditEvent, err error) {
	if c.Status != StatusActive {
		if c.Status.IsTerminal() {
			return false, nil, ErrCeremonyTerminal
		}
		return false, nil, nil
	}
	r := c.CurrentRound()
	if !now.After(r.Deadline) {
		return false, nil, nil
	}
	c.Status = StatusExpired
	events = []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: r.Number,
		Kind:        AuditExpired,
		Detail:      map[string]any{"deadline": r.Deadline.Format(time.RFC3339Nano)},
	}}
	events = append(events, closePendingReplacements(c, now, "ceremony_expired", "")...)
	addNotification(c, Notification{
		At: now, CeremonyID: c.ID, Type: NtfCeremonyExpired, RoundNumber: r.Number,
		Detail: map[string]any{"deadline": r.Deadline.Format(time.RFC3339Nano)},
	})
	return true, events, nil
}

// applyReplacementApproval 在同意数达到门限时生效一次替换：
// 开启新轮、冻结新成员与新截止时间、快照旧轮全部贡献为失效，
// 关闭其余待决请求，幂等记录回填新轮次号，并追加审计与通知。
// 全部修改都在调用方的仓储事务内完成。
func applyReplacementApproval(c *Ceremony, req *Replacement, now time.Time) []AuditEvent {
	old := c.CurrentRound()
	invalidated := contributionsInMemberOrder(old)
	next := newRound(old.Number+1, req.NewMembers, c.Threshold, now, req.NewDeadline)
	c.Rounds = append(c.Rounds, next)

	req.Status = ReplacementApproved
	req.PreviousRound = old.Number
	req.NewRoundNumber = next.Number
	req.InvalidatedContributions = invalidated
	req.DecidedAt = now
	if rec, ok := c.Idempotency[idemReplacePrefix+req.ID]; ok {
		rec.RoundNumber = next.Number
	}

	events := []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: next.Number,
		Kind:        AuditReplaced,
		Actor:       req.RequestedBy,
		Detail: map[string]any{
			"request_id":                req.ID,
			"previous_round":            old.Number,
			"new_round":                 next.Number,
			"approvers":                 append([]string(nil), req.Approvers...),
			"invalidated_contributions": participantIDs(invalidated),
			"members":                   req.NewMembers,
			"deadline":                  req.NewDeadline.Format(time.RFC3339Nano),
			"reason":                    req.Reason,
		},
	}}
	events = append(events, closePendingReplacements(c, now, "superseded:"+req.ID, req.ID)...)
	addNotification(c, Notification{
		At: now, CeremonyID: c.ID, Type: NtfReplacementApproved,
		RoundNumber: next.Number, ReplacementID: req.ID,
		Detail: map[string]any{
			"previous_round":            old.Number,
			"new_round":                 next.Number,
			"approvers":                 append([]string(nil), req.Approvers...),
			"invalidated_contributions": participantIDs(invalidated),
			"members":                   req.NewMembers,
			"deadline":                  req.NewDeadline.Format(time.RFC3339Nano),
		},
	})
	return events
}

// closePendingReplacements 把全部 pending 替换请求置为 closed（except 非空时跳过它），
// 返回对应的审计事件并追加同事务通知。关闭确定性按请求号排序。
func closePendingReplacements(c *Ceremony, now time.Time, reason, except string) []AuditEvent {
	ids := make([]string, 0)
	for id, req := range c.Replacements {
		if req != nil && req.Status == ReplacementPending && id != except {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	events := make([]AuditEvent, 0, len(ids))
	for _, id := range ids {
		events = append(events, closeReplacement(c, c.Replacements[id], now, reason)...)
	}
	return events
}

// closeReplacement 把单个请求置为 closed 并追加审计事件与通知。
func closeReplacement(c *Ceremony, req *Replacement, now time.Time, reason string) []AuditEvent {
	req.Status = ReplacementClosed
	req.DecidedAt = now
	addNotification(c, Notification{
		At: now, CeremonyID: c.ID, Type: NtfReplacementClosed,
		RoundNumber: req.TargetRound, ReplacementID: req.ID,
		Detail: map[string]any{"reason": reason},
	})
	return []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: req.TargetRound,
		Kind:        AuditReplacementClosed,
		Actor:       req.RequestedBy,
		Detail:      map[string]any{"request_id": req.ID, "reason": reason},
	}}
}

// addNotification 在当前事务内追加通知并分配仪式内单调递增序号。
func addNotification(c *Ceremony, n Notification) {
	n.Seq = int64(len(c.Notifications) + 1)
	c.Notifications = append(c.Notifications, n)
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

func contributionFingerprint(cv Contribution) string {
	h := sha256.New()
	h.Write([]byte(cv.ParticipantID))
	h.Write([]byte{0})
	h.Write(cv.Commitment)
	h.Write([]byte{0})
	h.Write(cv.ShardDigest)
	return hex.EncodeToString(h.Sum(nil))
}

func replacementFingerprint(requester string, members []string, deadline time.Time, reason string) string {
	h := sha256.New()
	h.Write([]byte(requester))
	h.Write([]byte{0})
	for _, m := range members {
		h.Write([]byte(m))
		h.Write([]byte{0})
	}
	h.Write([]byte(deadline.UTC().Format(time.RFC3339Nano)))
	h.Write([]byte{0})
	h.Write([]byte(reason))
	return hex.EncodeToString(h.Sum(nil))
}

func approvalFingerprint(requestID, voter string) string {
	h := sha256.New()
	h.Write([]byte(requestID))
	h.Write([]byte{0})
	h.Write([]byte(voter))
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

// equalStringSets 报告两个有序成员切片是否包含同一集合。
func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// intersectMembers 返回同时存在于新旧成员集合中的继续参与者，
// 结果按旧轮成员顺序排列（二者均已去重有序）。
func intersectMembers(old, newMembers []string) []string {
	set := make(map[string]struct{}, len(newMembers))
	for _, m := range newMembers {
		set[m] = struct{}{}
	}
	out := make([]string, 0)
	for _, m := range old {
		if _, ok := set[m]; ok {
			out = append(out, m)
		}
	}
	return out
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

func newRound(number int, members []string, threshold int, at time.Time, deadline time.Time) *Round {
	return &Round{
		Number:        number,
		Members:       append([]string(nil), members...),
		Threshold:     threshold,
		Contributions: map[string]*Contribution{},
		StartedAt:     at,
		Deadline:      deadline,
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
