package keyshards

import (
	"errors"
	"fmt"
	"time"
)

// Status 仪式的终态机状态。
type Status string

const (
	StatusActive    Status = "ACTIVE"    // 进行中，接受当前轮次的贡献
	StatusCompleted Status = "COMPLETED" // 门限达成且已完成，贡献集合已冻结
	StatusCancelled Status = "CANCELLED" // 被取消
	StatusTimedOut  Status = "TIMED_OUT" // 超过截止时间仍未完成
)

// IsTerminal 报告状态是否为终态。
func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusCancelled || s == StatusTimedOut
}

// 错误分类：调用方可用 errors.Is 区分轮次、成员、门限、状态与幂等冲突。
var (
	ErrCeremonyNotFound     = errors.New("keyshards: ceremony not found")
	ErrCeremonyClosed       = errors.New("keyshards: ceremony is not active")
	ErrNotMember            = errors.New("keyshards: participant is not a member of the current round")
	ErrStaleRound           = errors.New("keyshards: contribution targets a stale round")
	ErrDeadlineExceeded     = errors.New("keyshards: contribution is past the round deadline")
	ErrDuplicateContributor = errors.New("keyshards: participant already contributed in this round")
	ErrIdempotencyConflict  = errors.New("keyshards: request ID was already used with different content")
	ErrThresholdNotMet      = errors.New("keyshards: threshold not met in the current round")
	ErrInvalidParams        = errors.New("keyshards: invalid parameters")
)

// Contribution 是密码学组件提交的一次贡献。
//
// 协调层只接收承诺（Commitment）与分片摘要（ShareDigest）这两个不透明字段，
// 绝不接触、也不持久化秘密分片的明文。
type Contribution struct {
	RequestID     string    // 幂等请求号，由调用方生成
	ParticipantID string    // 参与者标识
	Round         int       // 目标轮次，必须等于当前轮次
	Commitment    []byte    // 密码学组件输出的承诺（不透明）
	ShareDigest   []byte    // 秘密分片的摘要/哈希（不透明）
	ReceivedAt    time.Time // 协调层接收时间
}

// Round 一轮仪式：参与者、门限与截止时间在开启时被冻结。
type Round struct {
	Number        int
	Participants  []string // 已排序、去重、冻结
	Threshold     int
	Deadline      time.Time
	Open          bool
	Contributions map[string]Contribution // key: ParticipantID
}

// Has 报告参与者是否属于本轮。
func (r *Round) Has(participantID string) bool {
	for _, p := range r.Participants {
		if p == participantID {
			return true
		}
	}
	return false
}

// IdemRecord 记录已接受贡献的幂等凭证，用于重试去重与冲突检测。
type IdemRecord struct {
	ContentHash string // sha256(commitment || shareDigest) 的十六进制
	Result      SubmitResult
}

// Ceremony 一次门限密钥分片仪式的全部协调状态。
type Ceremony struct {
	ID           string
	Status       Status
	CreatedAt    time.Time
	Rounds       []Round        // Rounds[len-1] 为当前轮次；轮次号单调递增
	Adopted      []Contribution // 完成时冻结的实际采用贡献集合
	CompletedAt  time.Time
	CancelledAt  time.Time
	CancelReason string
	TimedOutAt   time.Time

	Idem map[string]IdemRecord // key: RequestID（仪式范围内唯一）
}

// CurrentRound 返回当前轮次的指针。
func (c *Ceremony) CurrentRound() *Round {
	return &c.Rounds[len(c.Rounds)-1]
}

// OutboxEvent 是完成时写出的唯一密钥激活事件。
// 每个仪式至多产生一条，ID 形如 "<ceremonyID>/key-activation"。
type OutboxEvent struct {
	ID           string
	Type         string // 固定为 "KEY_ACTIVATION"
	CeremonyID   string
	Round        int
	Adopted      []Contribution
	Threshold    int
	Participants []string
	CreatedAt    time.Time
}

// OutboxEventTypeKeyActivation 是唯一支持的 outbox 事件类型。
const OutboxEventTypeKeyActivation = "KEY_ACTIVATION"

// 审计事件类型。
const (
	AuditCeremonyCreated      = "CEREMONY_CREATED"
	AuditContributionAccepted = "CONTRIBUTION_ACCEPTED"
	AuditContributionRejected = "CONTRIBUTION_REJECTED"
	AuditContributionReplayed = "CONTRIBUTION_REPLAYED"
	AuditRoundOpened          = "ROUND_OPENED"
	AuditCompleted            = "COMPLETED"
	AuditOutboxWritten        = "OUTBOX_WRITTEN"
	AuditCancelled            = "CANCELLED"
	AuditTimedOut             = "TIMED_OUT"
)

// AuditEvent 一条审计记录，Seq 在服务实例内单调递增。
type AuditEvent struct {
	Seq        int64
	Type       string
	CeremonyID string
	Round      int
	Actor      string
	Detail     string
	At         time.Time
}

// SubmitResult 贡献提交的结果；幂等重试时原样返回首次结果并置 Replayed。
type SubmitResult struct {
	Accepted      bool
	Round         int
	ParticipantID string
	RequestID     string
	CurrentCount  int // 接受后当前轮次的有效贡献数
	Threshold     int
	ThresholdMet  bool // 接受后是否已达门限
	Replayed      bool // 是否为幂等重放的首次结果
}

// CompleteResult 完成仪式的结果；并发与重试场景下所有调用方得到同一份。
type CompleteResult struct {
	CeremonyID  string
	Round       int
	Adopted     []Contribution
	Outbox      OutboxEvent
	AlreadyDone bool // 仪式此前已完成，本次为幂等返回
}

// CeremonyView 仪式状态的只读视图，供查询接口返回。
type CeremonyView struct {
	ID                string
	Status            Status
	CurrentRound      int
	Participants      []string
	Threshold         int
	Deadline          time.Time
	ContributionCount int
	AdoptedCount      int
	CreatedAt         time.Time
	CompletedAt       time.Time
	CancelledAt       time.Time
	CancelReason      string
	TimedOutAt        time.Time
}

// validateRoster 校验参与者名单与门限，返回排序去重后的名单。
func validateRoster(participants []string, threshold int, deadline, now time.Time) ([]string, error) {
	if len(participants) == 0 {
		return nil, fmt.Errorf("%w: participants must not be empty", ErrInvalidParams)
	}
	seen := make(map[string]struct{}, len(participants))
	roster := make([]string, 0, len(participants))
	for _, p := range participants {
		if p == "" {
			return nil, fmt.Errorf("%w: participant ID must not be empty", ErrInvalidParams)
		}
		if _, dup := seen[p]; dup {
			return nil, fmt.Errorf("%w: duplicate participant %q", ErrInvalidParams, p)
		}
		seen[p] = struct{}{}
		roster = append(roster, p)
	}
	if threshold < 1 || threshold > len(roster) {
		return nil, fmt.Errorf("%w: threshold %d out of range [1, %d]", ErrInvalidParams, threshold, len(roster))
	}
	if !deadline.After(now) {
		return nil, fmt.Errorf("%w: deadline must be in the future", ErrInvalidParams)
	}
	return sortStrings(roster), nil
}
