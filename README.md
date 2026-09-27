# go-key-shards

门限密钥分片仪式（threshold key ceremony）的**状态协调层**。

本包只负责仪式的编排与状态机：创建仪式、接收贡献、**门限批准式参与者替换**、
完成仪式、取消与审计查询，并把完成时产生的密钥激活消息与流程通知写入事务性 outbox。
**所有密码学计算都由外部组件完成**——协调层只接收外部产出的承诺
（commitment）与分片摘要（shard digest），任何接口都不接受、不记录秘密分片明文，
也不内置任何加密算法。

开发环境：Go 1.23.0。

## 领域模型与不变量

- **仪式冻结项**：创建时冻结参与者集合与门限 `threshold`（此后不可修改）；
  截止时间按轮冻结（见下）。
- **轮次（round）**：轮次号从 1 开始单调递增；每轮冻结当时的成员集合、
  门限与该轮自己的截止时间 `Round.Deadline`，独立累计贡献。
- **贡献（contribution）**：同一参与者在同一轮次至多贡献一次；只接受当前轮次成员的贡献。
- **门限**：当前轮有效贡献数达到门限才允许完成。
- **终态**：`completed` / `canceled` / `expired`，进入后不可变更。
- **outbox 唯一**：完成时冻结“实际采用的贡献集合”，并写出唯一的密钥激活消息；
  之后到达的额外贡献不能改变结果。

### 贡献的接收与拒绝（均不计入门限）

| 情形 | 结果 |
| --- | --- |
| 非当前轮次成员 | `ErrNotMember` |
| 针对旧轮次（替换参与者后） | `ErrStaleRound` |
| 已过当前轮截止时间 | `ErrDeadlineExceeded`，仪式惰性迁移为 `expired` |
| 仪式已处于终态 | `ErrCeremonyTerminal` |
| 同一参与者当轮重复贡献 | `ErrAlreadyContributed` |
| 同一请求号、内容不同 | `ErrConflict` |
| 同一请求号、内容相同的重试 | 返回首次结果（幂等重放），不重复计数 |

幂等按请求号（`RequestID`）判定，并记录首次提交时的轮次号：
即使重试发生在轮次替换、截止过期或仪式完成之后，同号同内容的重试仍返回首次结果，
而复用请求号提交不同内容（或改投其他轮次）一律判为冲突。

## 完成前替换参与者：门限批准 + 显式新一轮

替换无法继续参与的人是一个两步流程，而不是直接改一个人员字段：

1. **发起** `InitiateReplacement`：给出替换后的**完整**新成员集合、
   **新轮的截止时间**与原因。请求进入 `pending`，状态为待批准。
2. **批准** `ApproveReplacement`：继续参与者逐一同意。当同意人数达到门限的瞬间，
   在**同一个仓储事务**内开启下一轮、重新冻结成员与截止时间、
   把旧轮全部贡献快照为失效、关闭其他待决替换请求并写出通知。

### 谁能批准

- 批准权属于**继续参与者**——新旧成员集合的交集。被替换出去的人对自己的替换
  没有同意权（投票返回 `ErrApproverNotContinuing`）；替换后才加入的新人在生效前
  尚不是成员，也不能投票。
- 继续参与者的同意人数达到冻结的门限 `threshold` 即生效。发起人若本身是
  继续参与者，其同意在发起时自动计入；离场者本人可以发起替换，但不计票。
- 若交集人数本身少于门限，则该替换在数学上不可能获批，发起阶段即被拒绝。

### 生效瞬间发生了什么（同一事务）

- 新轮 `Round.Number+1` 开启，冻结 `NewMembers` 与 `NewDeadline`；
  旧轮的截止时间不再约束新轮。
- 旧轮的贡献**全部失效**：不折算、不携带、不计入新轮门限。
  新轮必须重新收集到门限数量的有效贡献才能 `Complete`。
- 替换记录 `Replacement` 被置为 `approved`，关联保存：
  `PreviousRound` / `NewRoundNumber`、批准人 `Approvers`、
  以及 `InvalidatedContributions`（旧轮贡献在生效瞬间的有序快照）。
- 其他仍处于 `pending` 的替换请求在同一事务内被 `closed`
  （它们针对的轮次已不是当前轮）。
- 审计事件 `participants_replaced` 与通知 `replacement_approved` 同事务写出。

### 替换后哪些数据仍有效、哪些失效

| 数据 | 替换生效后的效力 |
| --- | --- |
| 新轮成员集合、门限、新截止时间 | **有效**，是唯一的当前事实 |
| 新轮内收集的贡献 | **有效**，达到门限后可完成 |
| 旧轮的贡献 | **失效**：不计门限、不可折算、不可补投；但记录仍保留在旧 `Round` 与 `Replacement.InvalidatedContributions` 中，仅供审计与关联查询 |
| 针对旧轮的迟到贡献 | **拒绝**（`ErrStaleRound`），绝不带入新轮 |
| 完成 outbox | 只能由新轮贡献生成；`Outbox.RoundNumber` 标明实际采用的轮次，旧轮不可能产生 outbox |
| 仪式 ID、门限、历史审计流 | **有效**，审计事件只追加、不修改 |
| `pending` 的其他替换请求 | **关闭**（`closed`），不再可能生效 |
| 幂等记录 | **有效**：同请求号重试仍返回首次结果（贡献重放返回旧轮结果但不重新计数） |

### 终态互斥与事务性通知

替换生效、完成、取消、当前轮超时四者竞争时，每个仪式的所有状态迁移都串行化在
仓储的一次原子读-改-写事务内，因此**只会产生一个终态**：

- 仪式 `completed` / `canceled` / `expired` 的同一事务内，全部 `pending`
  替换请求被 `closed`，不会在终态之后再开新轮。
- 反之，替换先生效则旧轮不可能再完成或过期——完成只认当前（新）轮，
  过期只判断当前轮的截止时间。

通知（`Notification`，类型如 `replacement_initiated` / `replacement_approved` /
`replacement_closed` / `ceremony_completed` / `ceremony_expired`）与对应的状态变更
**在同一事务保存**：外部投递组件通过 `Notifications` 按序消费，
要么同时看到“新状态 + 通知”，要么都看不到。协调层不直接投递。

### 撤销

发起人可通过 `WithdrawReplacement` 在生效前撤销自己的请求
（`pending -> rejected`，重复撤销幂等）。一旦 `approved` 或 `closed` 即不可撤销。

## API 概览

```go
svc := keyshards.NewService(repo, nil) // nil 使用系统时钟；测试可注入可控时钟

// 创建：冻结成员/门限/第 1 轮截止时间，开启第 1 轮
c, _ := svc.CreateCeremony(ctx, keyshards.CreateInput{
    ID: "cer-1", Members: []string{"alice", "bob", "carol"},
    Threshold: 2, Deadline: time.Now().Add(time.Hour),
})

// 提交贡献：承诺与分片摘要来自外部密码学组件，均为不透明字节
res, _ := svc.SubmitContribution(ctx, keyshards.ContributeInput{
    CeremonyID: "cer-1", RequestID: "client-req-1", RoundNumber: c.CurrentRound().Number,
    Contribution: keyshards.Contribution{
        ParticipantID: "alice",
        Commitment:  cryptoOut.Commitment,   // 外部组件产出
        ShardDigest: cryptoOut.ShardDigest,  // 只有摘要，没有分片明文
    },
})

// 完成前替换参与者（bob 无法继续）：alice 发起，指定完整新集合与新轮截止时间
rep, _ := svc.InitiateReplacement(ctx, keyshards.InitiateReplacementInput{
    CeremonyID: "cer-1", RequestID: "rotate-bob-1", RequestedBy: "alice",
    NewMembers: []string{"alice", "carol", "dave"},
    NewDeadline: time.Now().Add(2 * time.Hour),
    Reason:      "bob lost device",
})
// 继续参与者 carol 批准；同意数达到门限 2（alice 自动计入）即同事务开启第 2 轮
rep, _ = svc.ApproveReplacement(ctx, keyshards.ApprovalInput{
    CeremonyID: "cer-1", RequestID: "rotate-bob-1",
    VoteRequestID: "vote-carol-1", Voter: "carol",
})

// 关联查询：新旧轮次、批准人、失效贡献快照
rep, _ = svc.GetReplacement(ctx, "cer-1", "rotate-bob-1")
all, _ := svc.ListReplacements(ctx, "cer-1")
notifications, _ := svc.Notifications(ctx, "cer-1", 0 /*fromSeq*/, 0 /*limit*/)

// 新轮重新收集到门限后完成：冻结采用集合（属于新轮），写出唯一 outbox
ob, _ := svc.Complete(ctx, keyshards.CompleteInput{
    CeremonyID: "cer-1", RequestID: "finish-1",
    KeyID: "kek-2026", Payload: cryptoOut.AggregatedActivation,
})

// 取消 / 审计
_ = svc.Cancel(ctx, "cer-1", "operator abort")
events, _ := svc.Audit(ctx, "cer-1", 0 /*fromSeq*/, 0 /*limit*/)
```

## 持久化

`Repository` 接口有两个实现，均满足“单仪式更新原子串行”的契约：

- `MemRepository`：进程内实现，每仪式一把互斥量，适合测试与单进程部署。
- `FileRepository`：JSON 文件持久化，目录布局：
  - `state.json`：全部仪式状态（含轮次、替换记录、通知）与审计事件
    （临时文件 + fsync + rename 原子替换）；
  - `lock`：`flock(2)` 文件锁，提供跨进程互斥（Windows 为进程内锁占位）；
  - `outbox/<ceremony-id>.json`：完成时写出且**只写一次**的密钥激活消息，
    供外部投递组件消费（`ReadOutboxFile`）。

写盘顺序是“先 outbox、后状态”：若两步之间崩溃，状态仍为 `active`，
重试完成会以相同文件名覆盖重写，不会产生第二条激活消息。
替换记录、失效贡献快照与通知都在 `state.json` 内，与仪式状态原子同存。

### 审计

所有状态迁移（created / contributed / replacement_initiated /
replacement_approved_vote / participants_replaced / replacement_withdrawn /
replacement_closed / completed / canceled / expired）以及被拒绝的操作
（rejected，含原因）都写入按仪式单调递增的事件流；贡献事件只记录承诺与
分片摘要的十六进制编码，没有任何明文字段。

## 运行测试

```sh
go test ./...            # 行为测试（内存与文件两套仓储共用用例）
go test -race ./...      # 含并发完成唯一胜者、替换批准/贡献/完成/取消/超时交织不变量
```
