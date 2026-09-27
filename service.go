package keyshards

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
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

// CreateInput 创建仪式的输入：参与者、门限、首轮截止时间在此冻结。
type CreateInput struct {
	ID        string
	Members   []string  // 成员 ID 列表；内部去重并按首次出现顺序冻结
	Threshold int       // 所需有效贡献数；创建后不可变，后续轮次沿用
	Deadline  time.Time // 第 1 轮截止时间；替换参与者后每轮重新冻结
}

// CreateCeremony 冻结参与者、门限、首轮截止时间并开启第 1 轮，轮次号从 1 递增。
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
		ID:          in.ID,
		Threshold:   in.Threshold,
		Deadline:    deadline,
		CreatedAt:   now,
		Status:      StatusActive,
		Rounds:      []*Round{newRound(1, members, in.Threshold, deadline, now)},
		Proposals:   map[string]*ReplacementProposal{},
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
	// RoundNumber 客户端针对的轮次；非当前轮次（通常是替换生效后的旧轮次）一律拒绝。
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
// 若请求恰好触发当前轮惰性过期，过期状态与通知在同一事务内保存。
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

		events, notes, expErr := s.lazyExpire(c, now)
		if expErr == ErrDeadlineExceeded {
			// 状态迁移（含挂起提案废弃）与通知必须同事务提交：返回非 nil 仪式。
			events = append(events, notificationEvents(notes)...)
			events = append(events, rejectEvent(c, r, now, in.Contribution.ParticipantID,
				"deadline_exceeded", map[string]any{"request_id": in.RequestID})...)
			appendNotifications(c, notes)
			return c, events, ErrDeadlineExceeded
		}
		if expErr != nil {
			return nil, events, expErr
		}
		if in.RoundNumber != r.Number {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "stale_round",
				ErrStaleRound, map[string]any{"requested_round": in.RoundNumber, "current_round": r.Number})
			return nil, append(events, evs...), errReject
		}
		if !isMember(r.Members, in.Contribution.ParticipantID) {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "not_member", ErrNotMember, nil)
			return nil, append(events, evs...), errReject
		}
		if _, dup := r.Contributions[in.Contribution.ParticipantID]; dup {
			evs, errReject := s.reject(c, r, now, in.Contribution.ParticipantID, "duplicate_contribution",
				ErrAlreadyContributed, nil)
			return nil, append(events, evs...), errReject
		}

		cv := cloneContribution(&in.Contribution)
		r.Contributions[cv.ParticipantID] = cv
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint:       fp,
			RoundNumber:       r.Number,
			ContributionCount: len(r.Contributions),
		}

		events = append(events, AuditEvent{
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
		})
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

// ProposeReplacementInput 在完成前提出参与者替换：生效必须显式开启新一轮。
type ProposeReplacementInput struct {
	CeremonyID string
	// RequestID 提案幂等请求号，同时作为提案 ID；同号同内容重试返回原提案。
	RequestID string
	// ProposedBy 提案人，必须是提案时当前轮次的冻结成员；提案人不计入批准人。
	ProposedBy string
	// Remove 要移出的成员（须为当前轮成员）；Add 要加入的新成员（不得与保留成员重名）。
	// 两者按名单去重；生效后成员 = 当前成员 - Remove + Add（保持当前顺序后追加新成员）。
	Remove []string
	Add    []string
	// NewDeadline 新轮次重新冻结的截止时间；必须晚于提出时间。
	NewDeadline time.Time
	// Reason 记录到审计日志的替换原因。
	Reason string
}

// ProposalResult 是提案/重放提案的结果。
type ProposalResult struct {
	ProposalID        string
	RoundNumber       int // 提案针对的旧轮
	Status            ReplacementStatus
	NewRoundNumber    int // 已生效时的新轮号，否则 0
	RequiredApprovals int
	ApprovalCount     int
	// Replayed 为 true 表示同请求号同内容重试，未重复创建提案。
	Replayed bool
}

// ProposeReplacement 冻结替换名单与新轮截止时间，创建 pending 提案，
// 等待当前轮门限数量的“其他参与者”批准。提案本身不改变轮次与贡献集合。
func (s *Service) ProposeReplacement(ctx context.Context, in ProposeReplacementInput) (*ProposalResult, error) {
	if in.CeremonyID == "" || in.RequestID == "" {
		return nil, errInvalid("ceremony id and request id are required")
	}
	if in.ProposedBy == "" {
		return nil, errInvalid("proposer is required")
	}
	remove, err := normalizeNameList(in.Remove, "remove")
	if err != nil {
		return nil, err
	}
	add, err := normalizeNameList(in.Add, "add")
	if err != nil {
		return nil, err
	}
	if len(remove) == 0 && len(add) == 0 {
		return nil, errInvalid("replacement must remove or add at least one member")
	}
	if in.NewDeadline.IsZero() {
		return nil, errInvalid("new round deadline is required")
	}
	newDeadline := in.NewDeadline.UTC()
	fp := proposalFingerprint(in.ProposedBy, remove, add, newDeadline)
	idempKey := idemProposalPrefix + in.RequestID

	var result *ProposalResult
	err = s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		// 同请求号重试先于状态检查：提案一旦创建，重试即返回其当前状态，
		// 即使仪式其后完成/取消/过期。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != fp {
				return nil, nil, ErrConflict
			}
			p := c.Proposals[rec.ProposalID]
			if p == nil {
				// 理论上不会发生：幂等记录与提案同生共死。
				return nil, nil, ErrConflict
			}
			result = proposalResult(p, len(p.Approvals), true)
			return c, nil, nil
		}

		events, notes, expErr := s.lazyExpire(c, now)
		if expErr == ErrDeadlineExceeded {
			events = append(events, notificationEvents(notes)...)
			appendNotifications(c, notes)
			return c, events, ErrDeadlineExceeded
		}
		if expErr != nil {
			return nil, events, expErr
		}
		if !newDeadline.After(now) {
			return nil, events, errInvalid("new round deadline must be in the future")
		}

		r := c.CurrentRound()
		if !isMember(r.Members, in.ProposedBy) {
			return nil, events, ErrNotMember
		}
		// 提案人本人不能投票：若其他参与者数量少于门限，提案在结构上不可能生效，
		// 直接拒绝而不是留下永远 pending 的提案。
		if len(r.Members)-1 < c.Threshold {
			return nil, events, errInvalid("not enough other participants to reach approval threshold")
		}
		newMembers, errCheck := checkMemberListChange(r.Members, remove, add)
		if errCheck != nil {
			return nil, events, errCheck
		}
		if c.Threshold > len(newMembers) {
			return nil, events, errInvalid("member count after replacement is below frozen threshold")
		}

		p := &ReplacementProposal{
			ID:                in.RequestID,
			RequestID:         in.RequestID,
			Status:            ProposalPending,
			RoundNumber:       r.Number,
			ProposedBy:        in.ProposedBy,
			Reason:            in.Reason,
			Remove:            remove,
			Add:               add,
			NewMembers:        newMembers,
			NewDeadline:       newDeadline,
			RequiredApprovals: c.Threshold,
			Approvals:         map[string]Approval{},
			CreatedAt:         now,
		}
		c.Proposals[p.ID] = p
		c.ProposalOrder = append(c.ProposalOrder, p.ID)
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint: fp, RoundNumber: r.Number, ProposalID: p.ID,
		}

		// 通知其他成员审批；提案人不接收自己提案的审批请求。
		// 没有其他成员时（例如单成员仪式）不产生空收件人通知。
		recipients := otherMembers(r.Members, in.ProposedBy)
		notes = nil // 复用上文 lazyExpire 的变量；非过期路径其值本为 nil。
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditProposalCreated,
			Actor:       in.ProposedBy,
			Detail: map[string]any{
				"proposal_id":        p.ID,
				"request_id":         in.RequestID,
				"remove":             remove,
				"add":                add,
				"new_members":        newMembers,
				"new_deadline":       newDeadline.Format(time.RFC3339Nano),
				"required_approvals": p.RequiredApprovals,
				"reason":             in.Reason,
			},
		})
		if len(recipients) > 0 {
			note := Notification{
				At:          now,
				CeremonyID:  c.ID,
				Kind:        NotifyReplacementProposed,
				RoundNumber: r.Number,
				ProposalID:  p.ID,
				Recipients:  recipients,
				Detail: map[string]any{
					"remove":             remove,
					"add":                add,
					"required_approvals": p.RequiredApprovals,
					"proposed_by":        in.ProposedBy,
				},
			}
			notes = []Notification{note}
			events = append(events, notificationEvent(note))
		}
		appendNotifications(c, notes)
		result = proposalResult(p, 0, false)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ApproveReplacementInput 批准一个 pending 的替换提案。
type ApproveReplacementInput struct {
	CeremonyID string
	// RequestID 批准动作的幂等请求号（与提案请求号相互独立）。
	RequestID string
	// ProposalID 待批准提案（即提出时的请求号）。
	ProposalID string
	// Approver 批准人：必须是提案轮次成员且不是提案人本人。
	// 被移除成员在替换生效前仍是旧轮参与者，其同意有效（批准的是旧轮提案）。
	Approver string
}

// ApproveResult 是批准结果；Applied 为 true 时本次批准使提案达到门限并已开启新轮。
type ApproveResult struct {
	ProposalID        string
	Status            ReplacementStatus
	ApprovalCount     int
	RequiredApprovals int
	Applied           bool
	NewRoundNumber    int
	// Replayed 为 true 表示同请求号同内容重试，未重复计入批准。
	Replayed bool
}

// ApproveReplacement 记录一名其他参与者的批准。批准数达到提案轮次门限的瞬间，
// 在同一事务内：开启新轮次并重新冻结成员与截止时间、旧轮全部贡献快照失效、
// 其余 pending 提案标记 superseded，并把状态迁移与全部通知一起保存。
func (s *Service) ApproveReplacement(ctx context.Context, in ApproveReplacementInput) (*ApproveResult, error) {
	if in.CeremonyID == "" || in.RequestID == "" || in.ProposalID == "" || in.Approver == "" {
		return nil, errInvalid("ceremony id, request id, proposal id and approver are required")
	}
	fp := approvalFingerprint(in.ProposalID, in.Approver)
	idempKey := idemApprovePrefix + in.RequestID

	var result *ApproveResult
	err := s.repo.Update(ctx, in.CeremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		// 批准重放先于状态检查：同请求号返回首次批准时的计数/生效结果。
		if rec, ok := c.Idempotency[idempKey]; ok {
			if rec.Fingerprint != fp || rec.ProposalID != in.ProposalID {
				return nil, nil, ErrConflict
			}
			p := c.Proposals[in.ProposalID]
			if p == nil {
				return nil, nil, ErrConflict
			}
			result = &ApproveResult{
				ProposalID:        p.ID,
				Status:            p.Status,
				ApprovalCount:     rec.ApprovalCount,
				RequiredApprovals: p.RequiredApprovals,
				Applied:           p.Status == ProposalApplied,
				NewRoundNumber:    p.NewRoundNumber,
				Replayed:          true,
			}
			return c, nil, nil
		}

		events, notes, expErr := s.lazyExpire(c, now)
		if expErr == ErrDeadlineExceeded {
			events = append(events, notificationEvents(notes)...)
			appendNotifications(c, notes)
			return c, events, ErrDeadlineExceeded
		}
		if expErr != nil {
			return nil, events, expErr
		}

		p, ok := c.Proposals[in.ProposalID]
		if !ok {
			return nil, events, ErrProposalNotFound
		}
		if p.Status != ProposalPending {
			return nil, events, ErrProposalNotPending
		}
		r := c.Rounds[p.RoundNumber-1]
		if !isMember(r.Members, in.Approver) {
			return nil, events, ErrNotMember
		}
		// 只有提案人本人不能批准自己的提案。被移除成员在替换生效前仍是旧轮
		// “其他参与者”，其同意有效（否则踢人人数过多时永远凑不齐门限）。
		if in.Approver == p.ProposedBy {
			return nil, events, ErrCannotApprove
		}
		if _, dup := p.Approvals[in.Approver]; dup {
			return nil, events, ErrAlreadyApproved
		}

		p.Approvals[in.Approver] = Approval{Approver: in.Approver, At: now}
		count := len(p.Approvals)
		c.Idempotency[idempKey] = &IdemRecord{
			Fingerprint: fp, RoundNumber: r.Number, ProposalID: p.ID, ApprovalCount: count,
		}

		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: r.Number,
			Kind:        AuditProposalApproved,
			Actor:       in.Approver,
			Detail: map[string]any{
				"proposal_id":    p.ID,
				"approval_count": count,
				"required":       p.RequiredApprovals,
			},
		})
		notes = append(notes, Notification{
			At:          now,
			CeremonyID:  c.ID,
			Kind:        NotifyReplacementApproved,
			RoundNumber: r.Number,
			ProposalID:  p.ID,
			Recipients:  []string{p.ProposedBy},
			Detail:      map[string]any{"approval_count": count, "required": p.RequiredApprovals, "by": in.Approver},
		})

		result = &ApproveResult{
			ProposalID:        p.ID,
			Status:            ProposalPending,
			ApprovalCount:     count,
			RequiredApprovals: p.RequiredApprovals,
		}

		if count == p.RequiredApprovals {
			// 门限达到：在同一事务内生效替换（开启新轮、旧贡献失效、其余提案退场）。
			applyEvents, applyNotes := s.applyProposal(c, p, now)
			events = append(events, applyEvents...)
			notes = append(notes, applyNotes...)
			result.Status = ProposalApplied
			result.Applied = true
			result.NewRoundNumber = p.NewRoundNumber
		}

		events = append(events, notificationEvents(notes)...)
		appendNotifications(c, notes)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// applyProposal 在已持有事务时让提案生效：
//  1. 快照旧轮全部贡献为 InvalidatedContributions；
//  2. 开启下一轮（成员、门限、截止时间重新冻结，贡献计数清零）；
//  3. 本提案 applied，其余 pending 提案 superseded（同一时刻至多一个生效终态）；
//  4. 产出 applied / superseded / contributions_invalidated 通知。
//
// 只返回审计“事实”事件与通知；通知落库与通知投影事件由调用方统一处理，
// 保证状态与通知在同一事务内保存。
func (s *Service) applyProposal(c *Ceremony, p *ReplacementProposal, now time.Time) ([]AuditEvent, []Notification) {
	old := c.Rounds[p.RoundNumber-1]
	nextNo := old.Number + 1

	invalidated := make([]InvalidatedContribution, 0, len(old.Contributions))
	for _, m := range old.Members {
		cv, ok := old.Contributions[m]
		if !ok {
			continue
		}
		invalidated = append(invalidated, InvalidatedContribution{
			ParticipantID: cv.ParticipantID,
			Commitment:    cloneBytes(cv.Commitment),
			ShardDigest:   cloneBytes(cv.ShardDigest),
			InvalidatedAt: now,
		})
	}

	p.Status = ProposalApplied
	p.DecidedAt = now
	p.NewRoundNumber = nextNo
	p.InvalidatedContributions = invalidated
	next := newRound(nextNo, p.NewMembers, c.Threshold, p.NewDeadline, now)
	c.Rounds = append(c.Rounds, next)

	approvers := make([]string, 0, len(p.Approvals))
	for _, m := range old.Members {
		if _, ok := p.Approvals[m]; ok {
			approvers = append(approvers, m)
		}
	}
	events := []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: nextNo,
		Kind:        AuditProposalApplied,
		Actor:       p.ProposedBy,
		Detail: map[string]any{
			"proposal_id":               p.ID,
			"previous_round":            old.Number,
			"new_round":                 nextNo,
			"members":                   p.NewMembers,
			"deadline":                  p.NewDeadline.Format(time.RFC3339Nano),
			"approvers":                 approvers,
			"invalidated_contributions": len(invalidated),
			"invalidated_participants":  invalidatedParticipants(invalidated),
		},
	}}
	notes := []Notification{{
		At:          now,
		CeremonyID:  c.ID,
		Kind:        NotifyReplacementApplied,
		RoundNumber: nextNo,
		ProposalID:  p.ID,
		Recipients:  append([]string{p.ProposedBy}, approvers...),
		Detail: map[string]any{
			"previous_round": old.Number,
			"new_round":      nextNo,
			"members":        p.NewMembers,
			"deadline":       p.NewDeadline.Format(time.RFC3339Nano),
		},
	}}
	if len(invalidated) > 0 {
		notes = append(notes, Notification{
			At:          now,
			CeremonyID:  c.ID,
			Kind:        NotifyContributionsInvalidated,
			RoundNumber: old.Number,
			ProposalID:  p.ID,
			Recipients:  invalidatedParticipants(invalidated),
			Detail: map[string]any{
				"previous_round": old.Number,
				"new_round":      nextNo,
				"participants":   invalidatedParticipants(invalidated),
			},
		})
	}

	// 其余 pending 提案不可能再针对新轮生效：统一 superseded。
	for _, otherID := range c.ProposalOrder {
		other := c.Proposals[otherID]
		if other == p || other.Status != ProposalPending {
			continue
		}
		other.Status = ProposalSuperseded
		other.DecidedAt = now
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: nextNo,
			Kind:        AuditProposalSuperseded,
			Detail:      map[string]any{"proposal_id": other.ID, "by_proposal": p.ID, "new_round": nextNo},
		})
		notes = append(notes, Notification{
			At:          now,
			CeremonyID:  c.ID,
			Kind:        NotifyReplacementSuperseded,
			RoundNumber: nextNo,
			ProposalID:  other.ID,
			Recipients:  []string{other.ProposedBy},
			Detail:      map[string]any{"by_proposal": p.ID, "new_round": nextNo},
		})
	}
	return events, notes
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
// 完成的同一事务内，所有 pending 替换提案标记 abandoned 并发出通知。
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
		events, notes, expErr := s.lazyExpire(c, now)
		if expErr == ErrDeadlineExceeded {
			events = append(events, notificationEvents(notes)...)
			appendNotifications(c, notes)
			return c, events, ErrDeadlineExceeded
		}
		if expErr != nil {
			return nil, events, expErr
		}
		if c.Status != StatusActive {
			return nil, events, ErrCeremonyTerminal
		}

		r := c.CurrentRound()
		if len(r.Contributions) < c.Threshold {
			return nil, events, ErrThresholdNotReached
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

		// 挂起提案随仪式完成一并废弃：终态唯一，替换不可能再生效。
		abEvents, abNotes := s.abandonPendingProposals(c, now, "ceremony_completed")
		notes = append(notes, abNotes...)
		events = append(events, abEvents...)
		events = append(events, notificationEvents(notes)...)
		appendNotifications(c, notes)
		outbox = cloneOutbox(ob)
		return c, events, nil
	})
	if err != nil {
		return nil, err
	}
	return outbox, nil
}

// Cancel 取消仪式。取消后进入终态，不会写出 outbox；
// 挂起的替换提案在同一事务内废弃并通知。
func (s *Service) Cancel(ctx context.Context, ceremonyID, reason string) error {
	if ceremonyID == "" {
		return errInvalid("ceremony id is required")
	}
	return s.repo.Update(ctx, ceremonyID, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		now := s.now()
		events, notes, expErr := s.lazyExpire(c, now)
		if expErr == ErrDeadlineExceeded {
			events = append(events, notificationEvents(notes)...)
			appendNotifications(c, notes)
			return c, events, ErrDeadlineExceeded
		}
		if expErr != nil {
			return nil, events, expErr
		}
		c.Status = StatusCanceled
		c.CancelReason = reason
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: c.CurrentRound().Number,
			Kind:        AuditCanceled,
			Detail:      map[string]any{"reason": reason},
		})
		abEvents, abNotes := s.abandonPendingProposals(c, now, "ceremony_canceled")
		notes = append(notes, abNotes...)
		events = append(events, abEvents...)
		events = append(events, notificationEvents(notes)...)
		appendNotifications(c, notes)
		return c, events, nil
	})
}

// GetCeremony 返回仪式快照（必要时把超过当前轮截止时间的活跃仪式惰性标记为过期）。
func (s *Service) GetCeremony(ctx context.Context, id string) (*Ceremony, error) {
	if id == "" {
		return nil, errInvalid("ceremony id is required")
	}
	_ = s.repo.Update(ctx, id, func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		events, notes, expErr := s.lazyExpire(c, s.now())
		if expErr != ErrDeadlineExceeded {
			return c, events, expErr
		}
		events = append(events, notificationEvents(notes)...)
		appendNotifications(c, notes)
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

// ---- 查询：新旧轮次 / 批准人 / 失效贡献的关联 ----

// GetProposal 返回指定替换提案的快照；不存在返回 ErrProposalNotFound。
func (s *Service) GetProposal(ctx context.Context, ceremonyID, proposalID string) (*ReplacementProposal, error) {
	if ceremonyID == "" || proposalID == "" {
		return nil, errInvalid("ceremony id and proposal id are required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	p, ok := c.Proposals[proposalID]
	if !ok {
		return nil, ErrProposalNotFound
	}
	return cloneProposal(p), nil
}

// ListProposals 按提出顺序返回提案快照；status 为空表示全部状态。
// 每个提案都可关联到旧轮（RoundNumber）、新轮（NewRoundNumber）、
// 批准人（Approvals）与失效贡献（InvalidatedContributions）。
func (s *Service) ListProposals(ctx context.Context, ceremonyID string, status ReplacementStatus) ([]*ReplacementProposal, error) {
	if ceremonyID == "" {
		return nil, errInvalid("ceremony id is required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	out := make([]*ReplacementProposal, 0, len(c.ProposalOrder))
	for _, id := range c.ProposalOrder {
		p := c.Proposals[id]
		if status != "" && p.Status != status {
			continue
		}
		out = append(out, cloneProposal(p))
	}
	return out, nil
}

// ListNotifications 返回仪式的通知 outbox（按 seq 有序）；外部投递组件据此消费。
func (s *Service) ListNotifications(ctx context.Context, ceremonyID string) ([]Notification, error) {
	if ceremonyID == "" {
		return nil, errInvalid("ceremony id is required")
	}
	c, err := s.repo.Get(ctx, ceremonyID)
	if err != nil {
		return nil, err
	}
	out := make([]Notification, len(c.Notifications))
	for i, n := range c.Notifications {
		out[i] = cloneNotification(n)
	}
	return out, nil
}

// ---- 内部辅助 ----

const (
	idemContributionPrefix = "contrib:"
	idemProposalPrefix     = "replace-proposal:"
	idemApprovePrefix      = "replace-approve:"
	idemCompletePrefix     = "complete:"
)

// lazyExpire 若活跃仪式已过“当前轮”截止时间则迁移到 expired，
// 并在同一变更内废弃全部 pending 提案。返回值：
//   - ErrDeadlineExceeded：已迁移；events 为过期/废弃审计事实，notes 为应同事务保存的通知。
//     调用方必须返回非 nil 仪式（连同 ErrDeadlineExceeded），状态与通知才会一起提交；
//   - ErrCeremonyTerminal：仪式已是其他终态，调用方直接拒绝，不落任何变更；
//   - nil：未过期，调用方继续正常处理。
func (s *Service) lazyExpire(c *Ceremony, now time.Time) (events []AuditEvent, notes []Notification, err error) {
	if c.Status != StatusActive {
		return nil, nil, ErrCeremonyTerminal
	}
	r := c.CurrentRound()
	if !now.After(r.Deadline) {
		return nil, nil, nil
	}
	c.Status = StatusExpired
	events = []AuditEvent{{
		At:          now,
		CeremonyID:  c.ID,
		RoundNumber: r.Number,
		Kind:        AuditExpired,
		Detail:      map[string]any{"deadline": r.Deadline.Format(time.RFC3339Nano), "round": r.Number},
	}}
	notes = []Notification{{
		At:          now,
		CeremonyID:  c.ID,
		Kind:        AuditExpired,
		RoundNumber: r.Number,
		Recipients:  append([]string(nil), r.Members...),
		Detail:      map[string]any{"round": r.Number, "deadline": r.Deadline.Format(time.RFC3339Nano)},
	}}
	abEvents, abNotes := s.abandonPendingProposals(c, now, "round_deadline_exceeded")
	return append(events, abEvents...), append(notes, abNotes...), ErrDeadlineExceeded
}

// abandonPendingProposals 在已持有事务时把全部 pending 提案标记 abandoned，
// 返回应同事务保存的审计事实与通知（原因：完成/取消/当前轮超时）。
// 不处理通知落库与通知投影，由调用方统一完成。
func (s *Service) abandonPendingProposals(c *Ceremony, now time.Time, reason string) ([]AuditEvent, []Notification) {
	var events []AuditEvent
	var notes []Notification
	for _, id := range c.ProposalOrder {
		p := c.Proposals[id]
		if p.Status != ProposalPending {
			continue
		}
		p.Status = ProposalAbandoned
		p.DecidedAt = now
		events = append(events, AuditEvent{
			At:          now,
			CeremonyID:  c.ID,
			RoundNumber: p.RoundNumber,
			Kind:        AuditProposalAbandoned,
			Detail:      map[string]any{"proposal_id": p.ID, "reason": reason},
		})
		notes = append(notes, Notification{
			At:          now,
			CeremonyID:  c.ID,
			Kind:        NotifyReplacementAbandoned,
			RoundNumber: p.RoundNumber,
			ProposalID:  p.ID,
			Recipients:  []string{p.ProposedBy},
			Detail:      map[string]any{"reason": reason},
		})
	}
	return events, notes
}

// appendNotifications 给通知分配仪式内单调序号并写入聚合（与状态同一事务）。
func appendNotifications(c *Ceremony, notes []Notification) {
	for i := range notes {
		c.NotificationSeq++
		notes[i].Seq = c.NotificationSeq
		notes[i].ID = notificationID(c.NotificationSeq)
		notes[i].CeremonyID = c.ID
		c.Notifications = append(c.Notifications, cloneNotification(notes[i]))
	}
}

func notificationID(seq int64) string { return "n-" + strconv.FormatInt(seq, 16) }

// notificationEvent 把通知投影为一条审计事实，保证“通知”在审计流中同样可查。
func notificationEvent(n Notification) AuditEvent {
	detail := map[string]any{
		"notification_id": n.ID,
		"recipients":      append([]string(nil), n.Recipients...),
	}
	for k, v := range n.Detail {
		detail[k] = v
	}
	return AuditEvent{
		At:          n.At,
		CeremonyID:  n.CeremonyID,
		RoundNumber: n.RoundNumber,
		Kind:        "notification:" + n.Kind,
		Detail:      detail,
	}
}

// notificationEvents 批量投影。
func notificationEvents(notes []Notification) []AuditEvent {
	events := make([]AuditEvent, 0, len(notes))
	for _, n := range notes {
		events = append(events, notificationEvent(n))
	}
	return events
}

// rejectEvent 构造一条 rejected 审计事件（不落任何状态变更）。
func rejectEvent(c *Ceremony, r *Round, now time.Time, actor, reason string, extra map[string]any) []AuditEvent {
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
	}}
}

// reject 同 rejectEvent，返回 (事件, 业务错误) 的便捷形式。
func (s *Service) reject(c *Ceremony, r *Round, now time.Time, actor, reason string, cause error, extra map[string]any) ([]AuditEvent, error) {
	return rejectEvent(c, r, now, actor, reason, extra), cause
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

func proposalFingerprint(proposer string, remove, add []string, deadline time.Time) string {
	h := sha256.New()
	h.Write([]byte(proposer))
	h.Write([]byte{0})
	for _, m := range remove {
		h.Write([]byte(m))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	for _, m := range add {
		h.Write([]byte(m))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	h.Write([]byte(deadline.UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(h.Sum(nil))
}

func approvalFingerprint(proposalID, approver string) string {
	h := sha256.New()
	h.Write([]byte(proposalID))
	h.Write([]byte{0})
	h.Write([]byte(approver))
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
	return normalizeNameList(in, "member")
}

// normalizeNameList 去重并保序；空名非法；nil/空切片合法（表示空名单）。
func normalizeNameList(in []string, field string) ([]string, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, m := range in {
		if m == "" {
			return nil, errInvalid(field + " id must not be empty")
		}
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	return out, nil
}

// checkMemberListChange 校验并计算替换后的成员快照：
// remove 必须全部是当前成员；add 不得与（移除后的）当前成员重名。
func checkMemberListChange(current, remove, add []string) ([]string, error) {
	removing := make(map[string]struct{}, len(remove))
	for _, m := range remove {
		if !isMember(current, m) {
			return nil, errInvalid("removed participant is not a current member: " + m)
		}
		removing[m] = struct{}{}
	}
	out := make([]string, 0, len(current)+len(add))
	for _, m := range current {
		if _, ok := removing[m]; ok {
			continue
		}
		out = append(out, m)
	}
	existing := make(map[string]struct{}, len(out))
	for _, m := range out {
		existing[m] = struct{}{}
	}
	for _, m := range add {
		if _, ok := existing[m]; ok {
			return nil, errInvalid("added participant is already a retained member: " + m)
		}
		existing[m] = struct{}{}
		out = append(out, m)
	}
	return out, nil
}

func otherMembers(members []string, self string) []string {
	out := make([]string, 0, len(members))
	for _, m := range members {
		if m != self {
			out = append(out, m)
		}
	}
	return out
}

func newRound(number int, members []string, threshold int, deadline time.Time, at time.Time) *Round {
	return &Round{
		Number:        number,
		Members:       append([]string(nil), members...),
		Threshold:     threshold,
		Deadline:      deadline,
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

func invalidatedParticipants(xs []InvalidatedContribution) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = x.ParticipantID
	}
	return out
}

func proposalResult(p *ReplacementProposal, approvals int, replayed bool) *ProposalResult {
	return &ProposalResult{
		ProposalID:        p.ID,
		RoundNumber:       p.RoundNumber,
		Status:            p.Status,
		NewRoundNumber:    p.NewRoundNumber,
		RequiredApprovals: p.RequiredApprovals,
		ApprovalCount:     approvals,
		Replayed:          replayed,
	}
}

// invalidError 包装 ErrInvalidArgument 并附说明。
type invalidError struct{ msg string }

func (e *invalidError) Error() string        { return "keyshards: invalid argument: " + e.msg }
func (e *invalidError) Is(target error) bool { return target == ErrInvalidArgument }

func errInvalid(msg string) error { return &invalidError{msg: msg} }
