package keyshards

import "time"

// CeremonyStatus 是仪式的生命周期状态。
type CeremonyStatus string

const (
	// StatusActive 仪式进行中，可以贡献、替换参与者（开启新轮次）。
	StatusActive CeremonyStatus = "active"
	// StatusCompleted 门限达成且已完成；贡献集合与密钥激活 outbox 已冻结。
	StatusCompleted CeremonyStatus = "completed"
	// StatusCanceled 仪式被显式取消。
	StatusCanceled CeremonyStatus = "canceled"
	// StatusExpired 超过截止时间且未完成（由贡献/完成/查询时惰性迁移得到）。
	StatusExpired CeremonyStatus = "expired"
)

// IsTerminal 报告状态是否为不可再变更的终态。
func (s CeremonyStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusCanceled || s == StatusExpired
}

// Contribution 是外部密码学组件产出的贡献凭据。
//
// 协调层只持有承诺与分片“摘要”（均为不透明字节，例如哈希/承诺点），
// 绝不持有秘密分片明文，也不对这些字节做任何密码学解释。
type Contribution struct {
	// ParticipantID 贡献者（必须是当前轮次冻结成员）。
	ParticipantID string
	// Commitment 外部密码学组件给出的承诺，不透明字节。
	Commitment []byte
	// ShardDigest 外部密码学组件给出的分片摘要，不透明字节；绝不记录分片明文。
	ShardDigest []byte
}

// Round 记录一轮参与者集合与其上的有效贡献。
type Round struct {
	// Number 从 1 开始单调递增的轮次号。
	Number int `json:"number"`
	// Members 创建时/替换参与者时冻结的成员集合（去重后的有序快照）。
	Members []string `json:"members"`
	// Threshold 该轮冻结的门限。
	Threshold int `json:"threshold"`
	// Contributions 按参与者索引的有效贡献（同一参与者每轮至多一条）。
	Contributions map[string]*Contribution `json:"contributions,omitempty"`
	// StartedAt 该轮开启时间。
	StartedAt time.Time `json:"started_at"`
}

// Outbox 是完成仪式时写出的唯一密钥激活消息。
type Outbox struct {
	// CeremonyID 所属仪式。
	CeremonyID string `json:"ceremony_id"`
	// RoundNumber 完成时采用的轮次。
	RoundNumber int `json:"round_number"`
	// KeyID 外部指定的待激活密钥标识。
	KeyID string `json:"key_id"`
	// Payload 由外部密码学组件提供的激活载荷（聚合结果等），协调层不解释。
	Payload []byte `json:"payload,omitempty"`
	// Contributions 完成时冻结的实际采用的贡献集合（参与者有序）。
	Contributions []Contribution `json:"contributions"`
	// ActivatedAt 写出时间。
	ActivatedAt time.Time `json:"activated_at"`
}

// Ceremony 是状态协调层的聚合根。
type Ceremony struct {
	ID string `json:"id"`

	// 以下字段在创建时冻结。
	Threshold    int       `json:"threshold"`
	Deadline     time.Time `json:"deadline"`
	CreatedAt    time.Time `json:"created_at"`
	CancelReason string    `json:"cancel_reason,omitempty"`

	// Rounds 轮次按 Number 递增追加；最后一个为当前轮次。
	// 旧轮次及其未决贡献在新轮开启时即告失效（不再计入当前门限）。
	Rounds []*Round `json:"rounds"`

	Status CeremonyStatus `json:"status"`
	// Outbox 完成时写出且仅写出一次；完成后不可变。
	Outbox *Outbox `json:"outbox,omitempty"`

	// 幂等表：请求号（带操作前缀）-> 首次提交记录，用于区分重试与冲突。
	Idempotency map[string]*IdemRecord `json:"idempotency,omitempty"`
}

// IdemRecord 记录一个请求号首次提交时的内容指纹与上下文，
// 使同号重试即使跨越轮次/终态也能原样返回首次结果。
type IdemRecord struct {
	// Fingerprint 首次提交内容（参与者+承诺+摘要 / 成员集合 / key+载荷）的指纹。
	Fingerprint string `json:"fingerprint"`
	// RoundNumber 首次提交时的当前轮次；贡献重放据此返回首轮结果。
	RoundNumber int `json:"round_number"`
	// ContributionCount 首次贡献被接受后的当前轮有效贡献数。
	ContributionCount int `json:"contribution_count,omitempty"`
}

// CurrentRound 返回仪式当前（最新）轮次。
func (c *Ceremony) CurrentRound() *Round {
	if len(c.Rounds) == 0 {
		return nil
	}
	return c.Rounds[len(c.Rounds)-1]
}

// AuditEvent 是审计流中的一条不可变事件。
type AuditEvent struct {
	Seq         int64          `json:"seq"`
	At          time.Time      `json:"at"`
	CeremonyID  string         `json:"ceremony_id"`
	RoundNumber int            `json:"round_number"`
	Kind        string         `json:"kind"`
	Actor       string         `json:"actor,omitempty"`
	Detail      map[string]any `json:"detail,omitempty"`
}

// 审计事件类型。
const (
	AuditCreated     = "created"
	AuditContributed = "contributed"
	AuditRejected    = "rejected"
	AuditReplaced    = "participants_replaced"
	AuditCompleted   = "completed"
	AuditCanceled    = "canceled"
	AuditExpired     = "expired"
)
