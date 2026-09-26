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
		ID:           c.ID,
		Threshold:    c.Threshold,
		Deadline:     c.Deadline,
		CreatedAt:    c.CreatedAt,
		CancelReason: c.CancelReason,
		Rounds:       rounds,
		Status:       c.Status,
		Outbox:       cloneOutbox(c.Outbox),
		Idempotency:  idem,
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
