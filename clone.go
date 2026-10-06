package keyshards

// 深拷贝辅助：仓储与服务对外返回的快照都必须与内部状态隔离，
// 避免调用方修改污染聚合。

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneContribution(cv *Contribution) *Contribution {
	if cv == nil {
		return nil
	}
	return &Contribution{
		ParticipantID: cv.ParticipantID,
		Commitment:    cloneBytes(cv.Commitment),
		ShardDigest:   cloneBytes(cv.ShardDigest),
		Status:        cv.Status,
		WithdrawalID:  cv.WithdrawalID,
	}
}

func cloneRound(r *Round) *Round {
	if r == nil {
		return nil
	}
	cs := make(map[string]*Contribution, len(r.Contributions))
	for k, v := range r.Contributions {
		cs[k] = cloneContribution(v)
	}
	return &Round{
		Number:        r.Number,
		Members:       append([]string(nil), r.Members...),
		Threshold:     r.Threshold,
		Contributions: cs,
		StartedAt:     r.StartedAt,
		Deadline:      r.Deadline,
	}
}

func cloneReplacement(req *Replacement) *Replacement {
	if req == nil {
		return nil
	}
	invalidated := make([]Contribution, len(req.InvalidatedContributions))
	for i, cv := range req.InvalidatedContributions {
		invalidated[i] = *cloneContribution(&cv)
	}
	return &Replacement{
		ID:                       req.ID,
		Status:                   req.Status,
		RequestedBy:              req.RequestedBy,
		Reason:                   req.Reason,
		NewMembers:               append([]string(nil), req.NewMembers...),
		NewDeadline:              req.NewDeadline,
		TargetRound:              req.TargetRound,
		Approvers:                append([]string(nil), req.Approvers...),
		Rejecters:                append([]string(nil), req.Rejecters...),
		PreviousRound:            req.PreviousRound,
		NewRoundNumber:           req.NewRoundNumber,
		InvalidatedContributions: invalidated,
		CreatedAt:                req.CreatedAt,
		DecidedAt:                req.DecidedAt,
	}
}

func cloneWithdrawal(w *ContributionWithdrawal) *ContributionWithdrawal {
	if w == nil {
		return nil
	}
	return &ContributionWithdrawal{
		ID:                 w.ID,
		Status:             w.Status,
		RoundNumber:        w.RoundNumber,
		ParticipantID:      w.ParticipantID,
		ContributionDigest: cloneBytes(w.ContributionDigest),
		Contribution:       *cloneContribution(&w.Contribution),
		Reason:             w.Reason,
		Reviewer:           w.Reviewer,
		RequestedAt:        w.RequestedAt,
		DecidedAt:          w.DecidedAt,
	}
}

func cloneNotification(n Notification) Notification {
	detail := make(map[string]any, len(n.Detail))
	for k, v := range n.Detail {
		detail[k] = v
	}
	return Notification{
		Seq:           n.Seq,
		CeremonyID:    n.CeremonyID,
		At:            n.At,
		Type:          n.Type,
		ReplacementID: n.ReplacementID,
		WithdrawalID:  n.WithdrawalID,
		RoundNumber:   n.RoundNumber,
		Detail:        detail,
	}
}

func cloneOutbox(o *Outbox) *Outbox {
	if o == nil {
		return nil
	}
	cs := make([]Contribution, len(o.Contributions))
	for i, cv := range o.Contributions {
		cs[i] = *cloneContribution(&cv)
	}
	return &Outbox{
		CeremonyID:    o.CeremonyID,
		RoundNumber:   o.RoundNumber,
		KeyID:         o.KeyID,
		Payload:       cloneBytes(o.Payload),
		Contributions: cs,
		ActivatedAt:   o.ActivatedAt,
	}
}

func cloneCeremony(c *Ceremony) *Ceremony {
	if c == nil {
		return nil
	}
	rounds := make([]*Round, len(c.Rounds))
	for i, r := range c.Rounds {
		rounds[i] = cloneRound(r)
	}
	reps := make(map[string]*Replacement, len(c.Replacements))
	for k, v := range c.Replacements {
		reps[k] = cloneReplacement(v)
	}
	withdrawals := make(map[string]*ContributionWithdrawal, len(c.ContributionWithdrawals))
	for k, v := range c.ContributionWithdrawals {
		withdrawals[k] = cloneWithdrawal(v)
	}
	ntfs := make([]Notification, len(c.Notifications))
	for i, n := range c.Notifications {
		ntfs[i] = cloneNotification(n)
	}
	idem := make(map[string]*IdemRecord, len(c.Idempotency))
	for k, v := range c.Idempotency {
		if v == nil {
			idem[k] = nil
			continue
		}
		cp := *v
		idem[k] = &cp
	}
	return &Ceremony{
		ID:                      c.ID,
		Threshold:               c.Threshold,
		Deadline:                c.Deadline,
		CreatedAt:               c.CreatedAt,
		CancelReason:            c.CancelReason,
		Rounds:                  rounds,
		Replacements:            reps,
		ContributionWithdrawals: withdrawals,
		Notifications:           ntfs,
		Status:                  c.Status,
		Outbox:                  cloneOutbox(c.Outbox),
		Idempotency:             idem,
	}
}

func cloneEvent(e AuditEvent) AuditEvent {
	detail := make(map[string]any, len(e.Detail))
	for k, v := range e.Detail {
		detail[k] = v
	}
	return AuditEvent{
		Seq:         e.Seq,
		At:          e.At,
		CeremonyID:  e.CeremonyID,
		RoundNumber: e.RoundNumber,
		Kind:        e.Kind,
		Actor:       e.Actor,
		Detail:      detail,
	}
}
