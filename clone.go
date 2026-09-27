package keyshards

import "time"

// 深拷贝辅助：仓储与服务对外返回的快照都必须与内部状态隔离，
// 避免调用方修改污染聚合。

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneTimePtr(t time.Time) time.Time { return t }

func cloneContribution(cv *Contribution) *Contribution {
	if cv == nil {
		return nil
	}
	return &Contribution{
		ParticipantID: cv.ParticipantID,
		Commitment:    cloneBytes(cv.Commitment),
		ShardDigest:   cloneBytes(cv.ShardDigest),
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
		Deadline:      r.Deadline,
		Contributions: cs,
		StartedAt:     r.StartedAt,
	}
}

func cloneInvalidatedContributions(in []InvalidatedContribution) []InvalidatedContribution {
	if in == nil {
		return nil
	}
	out := make([]InvalidatedContribution, len(in))
	for i, x := range in {
		out[i] = InvalidatedContribution{
			ParticipantID: x.ParticipantID,
			Commitment:    cloneBytes(x.Commitment),
			ShardDigest:   cloneBytes(x.ShardDigest),
			InvalidatedAt: x.InvalidatedAt,
		}
	}
	return out
}

func cloneProposal(p *ReplacementProposal) *ReplacementProposal {
	if p == nil {
		return nil
	}
	approvals := make(map[string]Approval, len(p.Approvals))
	for k, v := range p.Approvals {
		approvals[k] = v
	}
	return &ReplacementProposal{
		ID:                       p.ID,
		RequestID:                p.RequestID,
		Status:                   p.Status,
		RoundNumber:              p.RoundNumber,
		ProposedBy:               p.ProposedBy,
		Reason:                   p.Reason,
		Remove:                   append([]string(nil), p.Remove...),
		Add:                      append([]string(nil), p.Add...),
		NewMembers:               append([]string(nil), p.NewMembers...),
		NewDeadline:              p.NewDeadline,
		RequiredApprovals:        p.RequiredApprovals,
		Approvals:                approvals,
		CreatedAt:                p.CreatedAt,
		DecidedAt:                p.DecidedAt,
		NewRoundNumber:           p.NewRoundNumber,
		InvalidatedContributions: cloneInvalidatedContributions(p.InvalidatedContributions),
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

func cloneNotification(n Notification) Notification {
	detail := make(map[string]any, len(n.Detail))
	for k, v := range n.Detail {
		detail[k] = v
	}
	return Notification{
		Seq:         n.Seq,
		ID:          n.ID,
		At:          n.At,
		CeremonyID:  n.CeremonyID,
		Kind:        n.Kind,
		RoundNumber: n.RoundNumber,
		ProposalID:  n.ProposalID,
		Recipients:  append([]string(nil), n.Recipients...),
		Detail:      detail,
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
	idem := make(map[string]*IdemRecord, len(c.Idempotency))
	for k, v := range c.Idempotency {
		if v == nil {
			idem[k] = nil
			continue
		}
		cp := *v
		idem[k] = &cp
	}
	proposals := make(map[string]*ReplacementProposal, len(c.Proposals))
	for k, v := range c.Proposals {
		proposals[k] = cloneProposal(v)
	}
	notes := make([]Notification, len(c.Notifications))
	for i, n := range c.Notifications {
		notes[i] = cloneNotification(n)
	}
	return &Ceremony{
		ID:              c.ID,
		Threshold:       c.Threshold,
		Deadline:        c.Deadline,
		CreatedAt:       c.CreatedAt,
		CancelReason:    c.CancelReason,
		Rounds:          rounds,
		Status:          c.Status,
		Outbox:          cloneOutbox(c.Outbox),
		Proposals:       proposals,
		ProposalOrder:   append([]string(nil), c.ProposalOrder...),
		Notifications:   notes,
		NotificationSeq: c.NotificationSeq,
		Idempotency:     idem,
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
