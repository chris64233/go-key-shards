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

// ContributionStatus 描述一份贡献记录在审核流程中的状态。
type ContributionStatus string

const (
	// ContributionValid 贡献有效，可计入当前轮门限并可被完成采用。
	ContributionValid ContributionStatus = "valid"
	// ContributionReviewPending 贡献的撤回申请待审核，不计入门限。
	ContributionReviewPending ContributionStatus = "review_pending"
	// ContributionWithdrawn 撤回审核已通过，贡献永久撤回且不可恢复。
	ContributionWithdrawn ContributionStatus = "withdrawn"
	// ContributionClosed 待审核期间仪式进入终态，贡献未被采用且审核关闭。
	ContributionClosed ContributionStatus = "closed"
)

// ContributionRecord 保留一份贡献及其审核状态。同一参与者撤回后补交时，
// 新贡献会追加为新记录；Round.Contributions 仍只指向当前有效贡献。
type ContributionRecord struct {
	Contribution
	Status       ContributionStatus `json:"status"`
	WithdrawalID string             `json:"withdrawal_id,omitempty"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

// Round 记录一轮参与者集合与其上的有效贡献。
type Round struct {
	// Number 从 1 开始单调递增的轮次号。
	Number int `json:"number"`
	// Members 创建时/替换参与者时冻结的成员集合（去重后的有序快照）。
	Members []string `json:"members"`
	// Threshold 该轮冻结的门限（仪式级，全轮一致）。
	Threshold int `json:"threshold"`
	// Contributions 按参与者索引的有效贡献（同一参与者每轮至多一条）。
	Contributions map[string]*Contribution `json:"contributions,omitempty"`
	// ContributionRecords 保存该轮全部贡献记录，包含待审核与已撤回记录。
	ContributionRecords []*ContributionRecord `json:"contribution_records,omitempty"`
	// StartedAt 该轮开启时间。
	StartedAt time.Time `json:"started_at"`
	// Deadline 该轮冻结的截止时间：第 1 轮取创建仪式时的参数，
	// 后续轮取替换请求中携带的新截止时间。惰性过期一律以当前轮的该值为准。
	Deadline time.Time `json:"deadline"`
}

// WithdrawalStatus 是一次贡献撤回申请的生命周期状态。
type WithdrawalStatus string

const (
	// WithdrawalPending 撤回申请已提交，等待固定审核人决定。
	WithdrawalPending WithdrawalStatus = "pending"
	// WithdrawalApproved 审核通过：贡献已不可变撤回。
	WithdrawalApproved WithdrawalStatus = "approved"
	// WithdrawalRejected 审核拒绝：原贡献恢复为有效。
	WithdrawalRejected WithdrawalStatus = "rejected"
	// WithdrawalClosed 等待审核期间轮次或仪式终态先行生效，申请关闭且不再影响状态。
	WithdrawalClosed WithdrawalStatus = "closed"
)

// IsFinal 报告撤回申请是否已不可再审核。
func (s WithdrawalStatus) IsFinal() bool {
	return s == WithdrawalApproved || s == WithdrawalRejected || s == WithdrawalClosed
}

// Withdrawal 是一次贡献撤回申请及审核时间线。
type Withdrawal struct {
	// ID 撤回请求号，仪式内唯一且幂等。
	ID            string           `json:"id"`
	Status        WithdrawalStatus `json:"status"`
	RoundNumber   int              `json:"round_number"`
	ParticipantID string           `json:"participant_id"`
	// ShardDigest 申请时锁定的贡献摘要。
	ShardDigest []byte `json:"shard_digest"`
	Reason      string `json:"reason"`
	// Reviewer 申请时固定的审核人；只有该审核人可以作出决定。
	Reviewer    string    `json:"reviewer"`
	RequestedAt time.Time `json:"requested_at"`
	DecidedAt   time.Time `json:"decided_at,omitempty"`
	// MissingContributors 审核通过导致有效贡献不足时，列出当前仍缺的成员。
	MissingContributors    []string `json:"missing_contributors,omitempty"`
	ValidContributionCount int      `json:"valid_contribution_count,omitempty"`
	Threshold              int      `json:"threshold,omitempty"`
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

// ReplacementStatus 是一次替换请求的生命周期状态。
type ReplacementStatus string

const (
	// ReplacementPending 等待收集门限数量的同意。
	ReplacementPending ReplacementStatus = "pending"
	// ReplacementApproved 同意数达到门限：新轮已在同一事务内开启，旧轮贡献已失效。
	ReplacementApproved ReplacementStatus = "approved"
	// ReplacementRejected 发起人撤销或人数不再可能达到门限。
	ReplacementRejected ReplacementStatus = "rejected"
	// ReplacementClosed 等待期间仪式进入终态（完成/取消/过期），请求作废且未开新轮。
	ReplacementClosed ReplacementStatus = "closed"
)

// IsFinal 报告替换状态是否不可再变更。
func (s ReplacementStatus) IsFinal() bool {
	return s == ReplacementApproved || s == ReplacementRejected || s == ReplacementClosed
}

// Replacement 是一次“替换参与者”请求的完整记录，
// 保留新旧轮次、批准人与失效贡献的关联，供审计与关联查询。
type Replacement struct {
	// ID 请求号（InitiateReplacementInput.RequestID），仪式内唯一且幂等。
	ID string `json:"id"`
	// Status 请求状态。
	Status ReplacementStatus `json:"status"`
	// RequestedBy 发起人（必须是当前轮冻结成员），其同意在发起时自动计入。
	RequestedBy string `json:"requested_by"`
	// Reason 替换原因。
	Reason string `json:"reason,omitempty"`
	// NewMembers 请求要冻结的新成员集合（去重有序快照）。
	NewMembers []string `json:"new_members"`
	// NewDeadline 生效时为新轮冻结的截止时间；必须晚于发起时刻。
	NewDeadline time.Time `json:"new_deadline"`
	// TargetRound 发起时针对的轮次；只有该轮仍是当前轮时批准才能生效。
	TargetRound int `json:"target_round"`
	// Approvers 已同意的成员（去重有序），含发起人。
	Approvers []string `json:"approvers"`
	// Rejecters 明确拒绝的成员（仅留痕，不改变计数语义）。
	Rejecters []string `json:"rejecters,omitempty"`
	// PreviousRound 生效时被关闭的旧轮次号；未生效为 0。
	PreviousRound int `json:"previous_round,omitempty"`
	// NewRoundNumber 生效时开启的新轮次号；未生效为 0。
	NewRoundNumber int `json:"new_round_number,omitempty"`
	// InvalidatedContributions 生效瞬间旧轮被作废的贡献（按旧轮成员顺序快照），
	// 此后旧轮的贡献映射仍保留供审计，但门限只认新轮。
	InvalidatedContributions []Contribution `json:"invalidated_contributions,omitempty"`
	// CreatedAt 发起时间。
	CreatedAt time.Time `json:"created_at"`
	// DecidedAt 生效/拒绝/关闭时间。
	DecidedAt time.Time `json:"decided_at,omitempty"`
}

// Notification 是与状态迁移在同一事务内保存的通知（事务性 outbox）。
//
// 协调层不直接投递：外部投递组件按顺序读取并负责至少一次投递，
// 投递成功后可删除/标记。通知与它所描述的状态变更要么同时可见，要么都不可见。
type Notification struct {
	// Seq 仪式内单调递增的通知序号，与审计序号独立。
	Seq int64 `json:"seq"`
	// CeremonyID 所属仪式。
	CeremonyID string `json:"ceremony_id"`
	// At 生成时间（等于对应状态迁移的事务时间）。
	At time.Time `json:"at"`
	// Type 通知类型，见 Ntf* 常量。
	Type string `json:"type"`
	// ReplacementID 关联的替换请求（替换类通知）。
	ReplacementID string `json:"replacement_id,omitempty"`
	// WithdrawalID 关联的贡献撤回申请。
	WithdrawalID string `json:"withdrawal_id,omitempty"`
	// RoundNumber 关联轮次。
	RoundNumber int `json:"round_number,omitempty"`
	// Detail 通知负载（批准人、失效贡献者、新截止时间等）。
	Detail map[string]any `json:"detail,omitempty"`
}

// 通知类型。
const (
	// NtfReplacementInitiated 替换请求已发起，等待门限批准。
	NtfReplacementInitiated = "replacement_initiated"
	// NtfReplacementApproved 门限批准达成：新轮已开启，旧轮贡献已失效。
	NtfReplacementApproved = "replacement_approved"
	// NtfReplacementRejected 替换请求被拒绝/撤销。
	NtfReplacementRejected = "replacement_rejected"
	// NtfReplacementClosed 仪式进入终态，待决替换被关闭。
	NtfReplacementClosed = "replacement_closed"
	// NtfCeremonyCompleted 仪式完成，密钥激活 outbox 已冻结。
	NtfCeremonyCompleted = "ceremony_completed"
	// NtfCeremonyExpired 当前轮超过截止时间，仪式过期。
	NtfCeremonyExpired = "ceremony_expired"
	// NtfContributionWithdrawalRequested 贡献进入撤回待审核。
	NtfContributionWithdrawalRequested = "contribution_withdrawal_requested"
	// NtfContributionWithdrawalApproved 撤回通过，贡献不再有效。
	NtfContributionWithdrawalApproved = "contribution_withdrawal_approved"
	// NtfContributionWithdrawalRejected 撤回拒绝，贡献恢复有效。
	NtfContributionWithdrawalRejected = "contribution_withdrawal_rejected"
	// NtfContributionWithdrawalClosed 轮次或仪式终态先行，待审核申请关闭。
	NtfContributionWithdrawalClosed = "contribution_withdrawal_closed"
)

// Ceremony 是状态协调层的聚合根。
type Ceremony struct {
	ID string `json:"id"`

	// 以下字段在创建时冻结。
	Threshold    int       `json:"threshold"`
	CreatedAt    time.Time `json:"created_at"`
	CancelReason string    `json:"cancel_reason,omitempty"`

	// Deadline 第 1 轮的截止时间（创建时冻结），保留用于展示与向后兼容；
	// 过期判断一律以当前轮的 Round.Deadline 为准（替换生效后新轮重新冻结）。
	Deadline time.Time `json:"deadline"`

	// Rounds 轮次按 Number 递增追加；最后一个为当前轮次。
	// 旧轮次及其未决贡献在新轮开启时即告失效（不再计入当前门限）。
	Rounds []*Round `json:"rounds"`

	// Replacements 替换请求按发起顺序保存（幂等 ID 为键）；
	// 每个 approved 请求恰好关联一对（旧轮, 新轮）与一份失效贡献快照。
	Replacements map[string]*Replacement `json:"replacements,omitempty"`

	// Withdrawals 贡献撤回申请按请求号保存，供幂等重放与审核查询。
	Withdrawals map[string]*Withdrawal `json:"withdrawals,omitempty"`

	// Notifications 事务性通知，按 Seq 有序追加。
	Notifications []Notification `json:"notifications,omitempty"`

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
	// RoundNumber 首次提交时的当前轮次；贡献重放据此返回首轮结果，
	// 替换生效后回填为开启的新轮次号。
	RoundNumber int `json:"round_number,omitempty"`
	// ContributionCount 首次贡献被接受后的当前轮有效贡献数。
	ContributionCount int `json:"contribution_count,omitempty"`
	// ReplacementID 替换/批准请求关联的替换记录 ID。
	ReplacementID string `json:"replacement_id,omitempty"`
	// WithdrawalID 撤回申请关联的撤回记录 ID。
	WithdrawalID string `json:"withdrawal_id,omitempty"`
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
	AuditCreated              = "created"
	AuditContributed          = "contributed"
	AuditRejected             = "rejected"
	AuditReplacementInitiated = "replacement_initiated"
	AuditReplacementApproval  = "replacement_approved_vote"
	AuditReplaced             = "participants_replaced"
	AuditReplacementWithdrawn = "replacement_withdrawn"
	AuditReplacementClosed    = "replacement_closed"
	AuditCompleted            = "completed"
	AuditCanceled             = "canceled"
	AuditExpired              = "expired"
	AuditWithdrawalRequested  = "contribution_withdrawal_requested"
	AuditWithdrawalApproved   = "contribution_withdrawal_approved"
	AuditWithdrawalRejected   = "contribution_withdrawal_rejected"
	AuditWithdrawalClosed     = "contribution_withdrawal_closed"
)
