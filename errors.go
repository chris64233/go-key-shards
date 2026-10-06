package keyshards

import "errors"

// 协调层只做状态判断，所有错误均为确定性的状态/参数错误，
// 不包含任何密码学失败语义（密码学计算由外部组件负责）。
var (
	// ErrInvalidArgument 参数不合法（空 ID、成员重复、门限越界等）。
	ErrInvalidArgument = errors.New("keyshards: invalid argument")
	// ErrNotFound 仪式不存在。
	ErrNotFound = errors.New("keyshards: ceremony not found")
	// ErrExists 创建时仪式 ID 已存在。
	ErrExists = errors.New("keyshards: ceremony already exists")

	// ErrNotMember 贡献者不是当前轮次冻结的成员。
	ErrNotMember = errors.New("keyshards: participant is not a member of the current round")
	// ErrStaleRound 请求针对的轮次不是当前轮次（旧轮次贡献一律失效）。
	ErrStaleRound = errors.New("keyshards: stale round")
	// ErrDeadlineExceeded 已超过仪式冻结的截止时间。
	ErrDeadlineExceeded = errors.New("keyshards: ceremony deadline exceeded")
	// ErrAlreadyContributed 同一参与者在同一轮次只能贡献一次。
	ErrAlreadyContributed = errors.New("keyshards: participant already contributed in this round")
	// ErrConflict 同一请求号携带了不同的内容（参与者、承诺或摘要不一致）。
	ErrConflict = errors.New("keyshards: idempotency conflict")
	// ErrThresholdNotReached 有效贡献数量尚未达到门限，不能完成仪式。
	ErrThresholdNotReached = errors.New("keyshards: threshold not reached")
	// ErrCeremonyTerminal 仪式已处于终态（completed/canceled/expired），不能再变更。
	ErrCeremonyTerminal = errors.New("keyshards: ceremony already in terminal state")

	// ErrReplacementNotFound 替换请求号不存在。
	ErrReplacementNotFound = errors.New("keyshards: replacement request not found")
	// ErrReplacementFinal 替换请求已处于终态（approved/rejected/closed），不能再批准或撤销。
	ErrReplacementFinal = errors.New("keyshards: replacement request already finalized")
	// ErrAlreadyApproved 同一成员已经同意过该替换请求。
	ErrAlreadyApproved = errors.New("keyshards: participant already approved this replacement")
	// ErrApproverNotContinuing 批准人不在替换后的成员集合中：只有继续参与的成员
	// （新旧成员集合的交集）才有同意权，被替换出去的人不能批准自己的替换。
	ErrApproverNotContinuing = errors.New("keyshards: approver is not a continuing participant")

	// ErrContributionReviewPending 贡献正处于撤回待审核，暂时不能再次提交或被完成采用。
	ErrContributionReviewPending = errors.New("keyshards: contribution withdrawal is pending review")
	// ErrWithdrawalNotFound 撤回申请不存在。
	ErrWithdrawalNotFound = errors.New("keyshards: contribution withdrawal not found")
	// ErrWithdrawalFinal 撤回申请已有最终审核结果或已关闭。
	ErrWithdrawalFinal = errors.New("keyshards: contribution withdrawal already finalized")
	// ErrReviewerMismatch 审核人与撤回申请固定的审核人不一致。
	ErrReviewerMismatch = errors.New("keyshards: withdrawal reviewer mismatch")
)
