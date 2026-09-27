# go-key-shards

门限密钥分片仪式（threshold key ceremony）的**状态协调层**。

本包只负责仪式的编排与状态机：创建仪式、接收贡献、**提出/批准参与者替换**、
完成仪式、取消与审计查询，并把完成时产生的密钥激活消息与替换流程中的通知写入 outbox。
**所有密码学计算都由外部组件完成**——协调层只接收外部产出的承诺
（commitment）与分片摘要（shard digest），任何接口都不接受、不记录秘密分片明文，
也不内置任何加密算法。

开发环境：Go 1.23.0。

## 领域模型与不变量

- **仪式冻结项**：创建时冻结参与者集合、门限 `threshold` 与**第 1 轮**截止时间 `deadline`。
  门限仪式级不可变，后续每一轮沿用；截止时间则是**每轮独立冻结**的。
- **轮次（round）**：轮次号从 1 开始单调递增；每轮冻结当时的成员集合、门限与该轮截止时间，
  独立累计贡献。只有替换提案获得门限批准后才会开启新一轮。
- **贡献（contribution）**：同一参与者在同一轮次至多贡献一次；只接受当前轮次成员的贡献。
- **门限**：当前轮有效贡献数达到门限才允许完成。
- **终态**：`completed` / `canceled` / `expired`，进入后不可变更。
- **outbox 唯一**：完成时冻结“实际采用的贡献集合”，并写出唯一的密钥激活消息；
  之后到达的额外贡献不能改变结果。

## 参与者替换：提案 → 门限批准 → 新轮次

替换**不能直接改人员字段**，而是一个显式的两阶段流程：

1. **提案 `ProposeReplacement`**：当前轮成员作为提案人，提交 `Remove`/`Add` 名单与
   **新轮截止时间**。协调层在提案瞬间冻结“生效后成员快照”（`NewMembers`），
   创建 `pending` 提案，但轮次与贡献集合此时都不变。
   - 提案人不计入批准人；若“其他参与者”数量本身少于门限，提案在结构上不可能凑齐，
     创建时即以 `ErrInvalidArgument` 拒绝；替换后成员数也不得低于门限。
2. **批准 `ApproveReplacement`**：只有提案轮次的其他成员可以批准（提案人本人不可；
   非成员/尚未加入的新成员不可；每人每提案一票）。被移除成员在替换生效前仍是旧轮参与者，
   其批准有效。
3. **生效**：批准数达到**当前轮冻结的门限**的瞬间，在**同一事务**内：
   1. 旧轮全部贡献快照为 `InvalidatedContributions`（保留参与者、承诺、摘要与失效时间，
      **无分片明文**）；
   2. 开启下一轮：重新冻结成员、门限（沿用）与截止时间，贡献计数清零；
   3. 本提案 `applied`，其余 `pending` 提案一律 `superseded`；
   4. 状态变更、失效贡献快照与全部通知（提案/批准/生效/失效/退场）一起落库。

### 替换之后，哪些数据仍有效？

| 数据 | 是否仍有效 | 说明 |
| --- | --- | --- |
| 新轮次的成员/门限/截止时间 | ✅ 有效 | 生效瞬间重新冻结；新轮截止时间取提案中的 `NewDeadline` |
| 新轮次中重新收集的贡献 | ✅ 有效 | 必须重新收集到门限数量，才能完成仪式 |
| 旧轮次记录（成员、贡献、截止时间） | ✅ 可查但失效 | 保留在 `Ceremony.Rounds` 与审计流中仅供追溯，**不计入任何门限** |
| 旧轮次的贡献 | ❌ 不计门限、不可复用 | 全部进入生效提案的 `InvalidatedContributions`；对旧轮的迟到贡献返回 `ErrStaleRound` |
| 旧幂等记录（请求号 → 首次结果） | ✅ 有效 | 同请求号同内容的重试永远返回首次结果，即使已跨轮/终态 |
| 未生效的其他替换提案 | ❌ 失效 | `superseded`（被先生效的提案取代）或 `abandoned`（仪式完成/取消/超时），记录可查 |
| 完成 outbox | 仅完成轮有效 | `Outbox.RoundNumber` 标明最终采用的是哪一轮；替换前不会有 outbox |

**最终结果采用哪一轮数据**：只可能是完成时的当前（最新）轮次。
新轮必须独立凑齐门限，旧轮贡献绝不折算复用；`Outbox.RoundNumber` 与
生效提案的 `NewRoundNumber` 给出完整的新旧轮关联。

### 提案状态机

```
pending ──批准达到门限──▶ applied（唯一成功终态，开启新轮）
   │
   ├──其他提案先生效──▶ superseded
   └──仪式完成/取消/当前轮超时──▶ abandoned
```

终态互斥由仓储的单仪式原子事务保证：**替换生效、完成、取消、超时并发时，
有且仅有一个终态**；仪式进入终态时所有挂起提案在同一事务内 `abandoned` 并通知。

### 贡献的接收与拒绝（均不计入门限）

| 情形 | 结果 |
| --- | --- |
| 非当前轮次成员 | `ErrNotMember` |
| 针对旧轮次（替换生效后） | `ErrStaleRound` |
| 已过当前轮截止时间 | `ErrDeadlineExceeded`，仪式惰性迁移为 `expired` |
| 仪式已处于终态 | `ErrCeremonyTerminal` |
| 同一参与者当轮重复贡献 | `ErrAlreadyContributed` |
| 同一请求号、内容不同 | `ErrConflict` |
| 同一请求号、内容相同的重试 | 返回首次结果（幂等重放），不重复计数 |

幂等按请求号（`RequestID`）判定（提案请求号与批准请求号各自独立），
并记录首次提交时的轮次/提案：即使重试发生在替换生效、截止过期或仪式完成之后，
同号同内容的重试仍返回首次结果，而复用请求号提交不同内容一律判为冲突。

### 通知 outbox

状态迁移与通知在**同一事务**内保存（内存仓储为同一把互斥量下的一次更新；
文件仓储为同一次 `state.json` 原子替换）。通知有仪式内单调递增的 `Seq`/`ID`，
供外部投递组件按 ID 去重消费（`ListNotifications`）：

- `replacement_proposed`：请其他成员审批；
- `replacement_approved`：新批准到达（通知提案人）；
- `replacement_applied`：新轮开启（通知提案人与批准人）；
- `contributions_invalidated`：旧轮贡献失效（通知被失效贡献的参与者）；
- `replacement_superseded` / `replacement_abandoned`：提案退场；
- `expired`：当前轮超时。

通知同时以 `notification:<kind>` 事件投影进审计流。

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

// 1) 提出替换：冻结新名单与新轮截止时间，等待门限数量的其他成员批准
prop, _ := svc.ProposeReplacement(ctx, keyshards.ProposeReplacementInput{
    CeremonyID: "cer-1", RequestID: "rotate-1", ProposedBy: "alice",
    Remove: []string{"bob"}, Add: []string{"dave"},
    NewDeadline: time.Now().Add(2 * time.Hour), Reason: "bob unavailable",
})

// 2) 其他成员逐一批准；最后一票达到门限时在同一事务开启新一轮
vote, _ := svc.ApproveReplacement(ctx, keyshards.ApproveReplacementInput{
    CeremonyID: "cer-1", RequestID: "approve-carol-1",
    ProposalID: prop.ProposalID, Approver: "carol",
})
_ = vote.Applied // true 表示本轮已开启（vote.NewRoundNumber 为新轮号）

// 关联查询：一条提案串起旧轮、新轮、批准人与失效贡献
applied, _ := svc.GetProposal(ctx, "cer-1", prop.ProposalID)
// applied.RoundNumber=旧轮  applied.NewRoundNumber=新轮
// applied.Approvals=批准人  applied.InvalidatedContributions=旧轮失效贡献快照
all, _ := svc.ListProposals(ctx, "cer-1", "") // 可按状态过滤
notes, _ := svc.ListNotifications(ctx, "cer-1")

// 新轮重新凑齐门限后完成：冻结采用集合，写出唯一 outbox（RoundNumber 标明采用轮）
ob, err := svc.Complete(ctx, keyshards.CompleteInput{
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
  - `state.json`：全部仪式状态（轮次、提案、批准、失效贡献快照、通知、幂等表）
    与审计事件，临时文件 + fsync + rename 原子替换——**状态与通知在同一次写盘事务内**；
  - `lock`：`flock(2)` 文件锁，提供跨进程互斥（Windows 为进程内锁占位）；
  - `outbox/<ceremony-id>.json`：完成时写出且**只写一次**的密钥激活消息，
    供外部投递组件消费（`ReadOutboxFile`）。

写盘顺序是“先 outbox、后状态”：若两步之间崩溃，状态仍为 `active`，
重试完成会以相同文件名覆盖重写，不会产生第二条激活消息。

### 审计

所有状态迁移（created / contributed / replacement_proposed / replacement_approved /
replacement_applied / replacement_superseded / replacement_abandoned /
completed / canceled / expired）、通知投影（`notification:*`）以及被拒绝的操作
（rejected，含原因）都写入按仪式单调递增的事件流；
贡献与失效贡献事件只记录承诺与分片摘要的十六进制编码，没有任何明文字段。

## 运行测试

```sh
go test ./...            # 行为测试（内存与文件两套仓储共用用例）
go test -race ./...      # 含：门限批准生效、新轮重新计数、终态互斥并发交织、
                         #     提案/批准幂等重放、通知同事务、持久化恢复
```
