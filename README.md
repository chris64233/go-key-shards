# go-key-shards

门限密钥分片仪式的**状态协调层**。本服务只负责协调状态——参与者名册、仪式轮次、
门限计数、幂等与 outbox；承诺（commitment）与分片摘要（share digest）由外部密码学
组件产生并传入，本层**不设计加密算法，也不接触、不持久化秘密分片的明文**。

开发环境：Go 1.23.0。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| Ceremony（仪式） | 一次门限密钥分片过程，创建时冻结参与者、门限、截止时间 |
| Round（轮次） | 仪式的第 N 轮，轮次号单调递增；替换参与者会开启新轮次 |
| Contribution（贡献） | 一次提交，只含承诺与分片摘要两个不透明字段 |
| Outbox | 完成时写出的唯一密钥激活事件，每个仪式至多一条 |
| Audit（审计） | 全部状态变迁的有序流水，含被拒绝的贡献 |

## 状态机与不变量

```
ACTIVE ──门限达成 + Complete──▶ COMPLETED   （冻结采用集合，写唯一 outbox）
ACTIVE ──Cancel───────────────▶ CANCELLED
ACTIVE ──超过截止时间（惰性结算）─▶ TIMED_OUT
```

- 创建时冻结参与者、门限、截止时间；轮次号从 1 开始单调递增。
- 一个参与者在同一轮次只能贡献一次；非成员、旧轮次、超时的贡献一律拒绝且不计入门限。
- 幂等：同一 `RequestID` + 相同内容重试返回首次结果；内容不同返回 `ErrIdempotencyConflict`。
  幂等检查先于状态校验——即使仪式已终态或轮次已推进，重试仍返回首次结果。
- 完成互斥：所有变更在单把互斥锁内完成“校验 → 变更 → 落盘”，并发下只有一个请求
  执行完成动作；完成冻结实际采用的贡献集合（按参与者排序，结果确定），并写出唯一
  outbox 事件 `<ceremonyID>/key-activation`。其后到达的贡献与完成调用都不能改变结果。
- 替换参与者必须显式开启新轮次：旧轮次关闭，其未决贡献全部失效。
- 超时为惰性结算：任何操作或查询触及已过截止时间的进行中仪式时，将其转为 `TIMED_OUT`。

## API 概览

```go
svc, _ := keyshards.NewService(keyshards.NewFileStore("state.json"))

view, _ := svc.CreateCeremony(keyshards.CreateParams{
    Participants: []string{"alice", "bob", "carol"},
    Threshold:    2,
    Deadline:     time.Now().Add(time.Hour),
})

res, err := svc.SubmitContribution(view.ID, keyshards.Contribution{
    RequestID:     "req-001",      // 幂等请求号
    ParticipantID: "alice",
    Round:         1,              // 必须等于当前轮次
    Commitment:    commitment,     // 密码学组件输出，不透明
    ShareDigest:   digest,         // 分片摘要，非明文
})

view, _ = svc.ReplaceParticipants(view.ID, keyshards.ReplaceParams{...}) // 开启新轮次
done, _ := svc.Complete(view.ID)     // 冻结采用集合 + 写唯一 outbox
view, _ = svc.Cancel(view.ID, "ops", "reason")
view, _ = svc.GetCeremony(view.ID)   // 查询（惰性结算超时）
events, _ := svc.AuditTrail(view.ID) // 审计流水
outbox := svc.Outbox()               // outbox 事件
```

## 错误分类

用 `errors.Is` 区分：

| 错误 | 含义 |
| --- | --- |
| `ErrCeremonyNotFound` | 仪式不存在 |
| `ErrCeremonyClosed` | 仪式已终态（完成/取消/超时） |
| `ErrNotMember` | 非当前轮次成员 |
| `ErrStaleRound` | 目标轮次不是当前轮次 |
| `ErrDeadlineExceeded` | 贡献晚于轮次截止时间 |
| `ErrDuplicateContributor` | 同一参与者同轮重复贡献 |
| `ErrIdempotencyConflict` | 同一请求号携带不同内容 |
| `ErrThresholdNotMet` | 当前轮次未达门限 |
| `ErrInvalidParams` | 参数非法（门限越界、名单重复、截止时间已过等） |

## 持久化

`Store` 接口抽象整体状态的 `Load`/`Save`：

- `MemoryStore`：进程内实现，用于测试。
- `FileStore`：JSON 快照落盘，写入采用“临时文件 + rename”，崩溃后要么看到旧快照、
  要么看到新快照；状态、outbox 与审计流水一并持久化，重启后可恢复。

## 测试

```sh
go test ./... -race
```

覆盖：名册/门限/截止时间冻结与参数校验，成员/轮次/截止时间/重复贡献规则，
幂等重试与冲突（含终态后重试、跨轮次重试），替换开启新轮次并使旧贡献失效，
完成的采用集合冻结与唯一 outbox，并发完成单胜者、并发贡献去重、
替换/贡献/完成/取消并发下终态合法性，审计流水有序性，明文不落盘，
以及 FileStore 重启恢复。
