package keyshards

import "time"

// CeremonyStatus 是仪式的生命周期状态。
type CeremonyStatus string

const (
	// StatusActive 仪式进行中，可以贡献、提出/批准替换（开启新轮次）。
	StatusActive CeremonyStatus = "active"
	// StatusCompleted 门限达成且已完成；贡献集合与密钥激活 outbox 已冻结。
	StatusCompleted CeremonyStatus = "completed"
	// StatusCanceled 仪式被显式取消。
	StatusCanceled CeremonyStatus = "canceled"
	// StatusExpired 超过当前轮次截止时间且未完成（由贡献/批准/完成/查询时惰性迁移得到）。
	StatusExpired CeremonyStatus = "expired"
)

// IsTerminal 报告状态是否为不可再变更的终态。
func (s CeremonyStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusCanceled || s == StatusExpired
}

// ReplacementStatus 是参与者替换提案的生命周期状态。
type ReplacementStatus string

const (
	// ProposalPending 提案已提出，正在收集其他参与者批准。
	ProposalPending ReplacementStatus = "pending"
	// ProposalApplied 批准达到门限，已开启新轮次（唯一成功终态）。
	ProposalApplied ReplacementStatus = "applied"
	// ProposalSuperseded 其他提案先达到门限，本提案随之失效。
	ProposalSuperseded ReplacementStatus = "superseded"
	// ProposalAbandoned 仪式先完成/取消/过期，提案不再可能生效。
	ProposalAbandoned ReplacementStatus = "abandoned"
)

// IsTerminal 报告提案是否已离开 pending。
func (s ReplacementStatus) IsTerminal() bool {
	return s == ProposalApplied || s == ProposalSuperseded || s == ProposalAbandoned
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

// Round 记录一轮参与者集合、轮次截止时间与其上的有效贡献。
type Round struct {
	// Number 从 1 开始单调递增的轮次号。
	Number int `json:"number"`
	// Members 创建时/替换生效时冻结的成员集合（去重后的有序快照）。
	Members []string `json:"members"`
	// Threshold 该轮冻结的门限（仪式级门限不可变，每轮沿用）。
	Threshold int `json:"threshold"`
	// Deadline 该轮冻结的截止时间：第 1 轮取创建时的值，后续轮次取
	// 替换提案中携带的新截止时间，在替换生效瞬间重新冻结。
	Deadline time.Time `json:"deadline"`
	// Contributions 按参与者索引的有效贡献（同一参与者每轮至多一条）。
	// 旧轮次被替换后，其贡献保留在此仅供关联查询，不再计入任何门限。
	Contributions map[string]*Contribution `json:"contributions,omitempty"`
	// StartedAt 该轮开启时间。
	StartedAt time.Time `json:"started_at"`
}

// Approval 记录一名参与者对替换提案的批准。
type Approval struct {
	// Approver 批准人（提案轮次的成员，且不是提案人、不在被移除名单内）。
	Approver string `json:"approver"`
	// At 批准时间。
	At time.Time `json:"at"`
}

// InvalidatedContribution 记录新轮次开启时被宣告失效的旧轮次贡献，
// 只保留承诺与分片摘要（与 Contribution 同一纪律：无分片明文），
// 用于“失效贡献 ↔ 旧轮次 ↔ 替换提案 ↔ 新轮次”的关联查询。
type InvalidatedContribution struct {
	ParticipantID string    `json:"participant_id"`
	Commitment    []byte    `json:"commitment,omitempty"`
	ShardDigest   []byte    `json:"shard_digest,omitempty"`
	InvalidatedAt time.Time `json:"invalidated_at"`
}

// ReplacementProposal 是一次参与者替换提案。
//
// 提案在某一轮（RoundNumber，即“旧轮”）提出；只有获得该轮门限数量的
// 其他参与者批准后才生效：生效时开启 NewRoundNumber 轮并重新冻结成员与
// 截止时间，旧轮全部贡献记入 InvalidatedContributions。
type ReplacementProposal struct {
	// ID 提案标识，等于提出时使用的幂等请求号，仪式内唯一。
	ID string `json:"id"`
	// RequestID 提案幂等请求号（同 ID）。
	RequestID string `json:"request_id"`
	// Status 提案状态：pending/applied/superseded/abandoned。
	Status ReplacementStatus `json:"status"`

	// RoundNumber 提案提出时的当前轮次（旧轮）。
	RoundNumber int `json:"round_number"`
	// ProposedBy 提案人（旧轮成员）。
	ProposedBy string `json:"proposed_by"`
	// Reason 记录到审计日志的替换原因。
	Reason string `json:"reason,omitempty"`

	// Remove / Add 提案请求的有序去重名单。
	Remove []string `json:"remove,omitempty"`
	Add    []string `json:"add,omitempty"`
	// NewMembers 提案时冻结的生效后成员快照；批准只决定是否生效，不再改名单。
	NewMembers []string `json:"new_members"`
	// NewDeadline 新轮次将冻结的截止时间。
	NewDeadline time.Time `json:"new_deadline"`
	// RequiredApprovals 生效所需批准数（= 提案轮次冻结的门限）。
	RequiredApprovals int `json:"required_approvals"`

	// Approvals 已收集批准，按批准人索引（每人对同一提案至多一次）。
	Approvals map[string]Approval `json:"approvals,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	// DecidedAt 离开 pending 的时间（applied/superseded/abandoned）。
	DecidedAt time.Time `json:"decided_at,omitempty"`
	// NewRoundNumber 生效时开启的新轮次号；未生效为 0。
	NewRoundNumber int `json:"new_round_number,omitempty"`
	// InvalidatedContributions 生效时快照的旧轮全部贡献。
	InvalidatedContributions []InvalidatedContribution `json:"invalidated_contributions,omitempty"`
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

	// 以下字段在创建时冻结。Threshold 仪式级不可变，替换后的每一轮都沿用；
	// Deadline 为第 1 轮截止时间，后续轮次的截止时间见 Round.Deadline。
	Threshold    int       `json:"threshold"`
	Deadline     time.Time `json:"deadline"`
	CreatedAt    time.Time `json:"created_at"`
	CancelReason string    `json:"cancel_reason,omitempty"`

	// Rounds 轮次按 Number 递增追加；最后一个为当前轮次。
	// 旧轮次及其贡献在新轮开启时即告失效（不再计入当前门限），但保留可查。
	Rounds []*Round `json:"rounds"`

	Status CeremonyStatus `json:"status"`
	// Outbox 完成时写出且仅写出一次；完成后不可变。
	Outbox *Outbox `json:"outbox,omitempty"`

	// Proposals 替换提案（ID -> 提案）；ProposalOrder 给出稳定的时间顺序。
	Proposals     map[string]*ReplacementProposal `json:"proposals,omitempty"`
	ProposalOrder []string                        `json:"proposal_order,omitempty"`

	// Notifications 与状态在同一事务内追加的通知 outbox（仪式内单调编号）。
	// 协调层只负责生产，投递与确认由外部组件按 ID 去重消费。
	Notifications   []Notification `json:"notifications,omitempty"`
	NotificationSeq int64          `json:"notification_seq,omitempty"`

	// 幂等表：请求号（带操作前缀）-> 首次提交记录，用于区分重试与冲突。
	Idempotency map[string]*IdemRecord `json:"idempotency,omitempty"`
}

// IdemRecord 记录一个请求号首次提交时的内容指纹与上下文，
// 使同号重试即使跨越轮次/终态也能原样返回首次结果。
type IdemRecord struct {
	// Fingerprint 首次提交内容（参与者+承诺+摘要 / 替换名单 / 批准 / key+载荷）的指纹。
	Fingerprint string `json:"fingerprint"`
	// RoundNumber 首次提交时的当前轮次；贡献重放据此返回首轮结果。
	RoundNumber int `json:"round_number"`
	// ContributionCount 首次贡献被接受后的当前轮有效贡献数。
	ContributionCount int `json:"contribution_count,omitempty"`
	// ProposalID 替换提案/批准请求关联的提案 ID。
	ProposalID string `json:"proposal_id,omitempty"`
	// ApprovalCount 批准首次写入后该提案的批准总数。
	ApprovalCount int `json:"approval_count,omitempty"`
}

// CurrentRound 返回仪式当前（最新）轮次。
func (c *Ceremony) CurrentRound() *Round {
	if len(c.Rounds) == 0 {
		return nil
	}
	return c.Rounds[len(c.Rounds)-1]
}

// Notification 是协调层在状态迁移同一事务内写出的通知（outbox 模式）。
type Notification struct {
	// Seq / ID 仪式内单调递增的序号与稳定标识（n-<seq>），供外部投递去重。
	Seq int64  `json:"seq"`
	ID  string `json:"id"`

	At          time.Time      `json:"at"`
	CeremonyID  string         `json:"ceremony_id"`
	Kind        string         `json:"kind"`
	RoundNumber int            `json:"round_number"`
	ProposalID  string         `json:"proposal_id,omitempty"`
	Recipients  []string       `json:"recipients"`
	Detail      map[string]any `json:"detail,omitempty"`
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
	AuditCreated = "created"

	AuditContributed = "contributed"
	AuditRejected    = "rejected"

	AuditProposalCreated    = "replacement_proposed"
	AuditProposalApproved   = "replacement_approved"
	AuditProposalApplied    = "replacement_applied"
	AuditProposalSuperseded = "replacement_superseded"
	AuditProposalAbandoned  = "replacement_abandoned"

	AuditCompleted = "completed"
	AuditCanceled  = "canceled"
	AuditExpired   = "expired"
)

// 通知类型（通知是状态的投递投影；审计事件是事实流，二者同事务落库）。
const (
	NotifyReplacementProposed      = AuditProposalCreated
	NotifyReplacementApproved      = AuditProposalApproved
	NotifyReplacementApplied       = AuditProposalApplied
	NotifyReplacementSuperseded    = AuditProposalSuperseded
	NotifyReplacementAbandoned     = AuditProposalAbandoned
	NotifyContributionsInvalidated = "contributions_invalidated"
)
