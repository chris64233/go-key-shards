package keyshards

import "errors"

// 协调层只做状态判断，所有错误均为确定性的状态/参数错误，
// 不包含任何密码学失败语义（密码学计算由外部组件负责）。
var (
	// ErrInvalidArgument 参数不合法（空 ID、成员重复、门限越界等）。
	ErrInvalidArgument = errors.New("keyshards: invalid argument")
	// ErrNotFound 仪式或相关资源不存在。
	ErrNotFound = errors.New("keyshards: ceremony not found")
	// ErrExists 创建时仪式 ID 已存在。
	ErrExists = errors.New("keyshards: ceremony already exists")

	// ErrNotMember 贡献者/批准人不是当前轮次冻结的成员。
	ErrNotMember = errors.New("keyshards: participant is not a member of the current round")
	// ErrStaleRound 请求针对的轮次不是当前轮次（旧轮次贡献一律失效）。
	ErrStaleRound = errors.New("keyshards: stale round")
	// ErrDeadlineExceeded 已超过当前轮次冻结的截止时间。
	ErrDeadlineExceeded = errors.New("keyshards: ceremony deadline exceeded")
	// ErrAlreadyContributed 同一参与者在同一轮次只能贡献一次。
	ErrAlreadyContributed = errors.New("keyshards: participant already contributed in this round")
	// ErrConflict 同一请求号携带了与首次提交不同的内容。
	ErrConflict = errors.New("keyshards: idempotency conflict")
	// ErrThresholdNotReached 有效贡献数量尚未达到门限，不能完成仪式。
	ErrThresholdNotReached = errors.New("keyshards: threshold not reached")
	// ErrCeremonyTerminal 仪式已处于终态（completed/canceled/expired），不能再变更。
	ErrCeremonyTerminal = errors.New("keyshards: ceremony already in terminal state")

	// ErrProposalNotFound 替换提案不存在。
	ErrProposalNotFound = errors.New("keyshards: replacement proposal not found")
	// ErrProposalNotPending 替换提案已离开 pending（applied/superseded/abandoned），不能再批准。
	ErrProposalNotPending = errors.New("keyshards: replacement proposal is not pending")
	// ErrAlreadyApproved 该参与者已经批准过此提案（同请求号重放除外，重放走幂等路径）。
	ErrAlreadyApproved = errors.New("keyshards: participant already approved this proposal")
	// ErrCannotApprove 批准人不具备批准资格（提案人本人、被移除成员或不是旧轮成员）。
	ErrCannotApprove = errors.New("keyshards: participant cannot approve this proposal")
)
