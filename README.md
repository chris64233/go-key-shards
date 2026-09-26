# go-key-shards

门限密钥分片仪式（threshold key ceremony）的**状态协调层**。

本包只负责仪式的编排与状态机：创建仪式、接收贡献、替换参与者、完成仪式、
取消与审计查询，并把完成时产生的密钥激活消息写入 outbox。
**所有密码学计算都由外部组件完成**——协调层只接收外部产出的承诺
（commitment）与分片摘要（shard digest），任何接口都不接受、不记录秘密分片明文，
也不内置任何加密算法。

开发环境：Go 1.23.0。

## 领域模型与不变量

- **仪式冻结项**：创建时冻结参与者集合、门限 `threshold` 与截止时间 `deadline`，此后不可修改。
- **轮次（round）**：轮次号从 1 开始单调递增；每轮冻结当时的成员集合，独立累计贡献。
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
| 已过截止时间 | `ErrDeadlineExceeded`，仪式惰性迁移为 `expired` |
| 仪式已处于终态 | `ErrCeremonyTerminal` |
| 同一参与者当轮重复贡献 | `ErrAlreadyContributed` |
| 同一请求号、内容不同 | `ErrConflict` |
| 同一请求号、内容相同的重试 | 返回首次结果（幂等重放），不重复计数 |

幂等按请求号（`RequestID`）判定，并记录首次提交时的轮次号：
即使重试发生在轮次替换、截止过期或仪式完成之后，同号同内容的重试仍返回首次结果，
而复用请求号提交不同内容（或改投其他轮次）一律判为冲突。

### 替换参与者 = 显式开启新一轮

完成前替换成员必须调用 `ReplaceParticipants`：冻结新成员集合并开启下一轮，
**旧轮次的全部未决贡献即刻失效**，不再计入任何门限；门限沿用创建时冻结的值
（替换后成员数不得低于门限）。

替换、贡献、完成、取消与超时并发时，协调层把每个仪式的所有状态迁移串行化在
仓储的一次原子读-改-写事务内，因此只会得到合法的轮次演进与终态：
门限达成后并发的完成请求**恰好只有一个**成功。

## API 概览

```go
svc := keyshards.NewService(repo, nil) // nil 使用系统时钟；测试可注入可控时钟

// 创建：冻结成员/门限/截止时间，开启第 1 轮
c, _ := svc.CreateCeremony(ctx, keyshards.CreateInput{
    ID: "cer-1", Members: []string{"alice", "bob", "carol"},
    Threshold: 2, Deadline: time.Now().Add(time.Hour),
})

// 提交贡献：承诺与分片摘要来自外部密码学组件，均为不透明字节
res, err := svc.SubmitContribution(ctx, keyshards.ContributeInput{
    CeremonyID: "cer-1", RequestID: "client-req-1", RoundNumber: c.CurrentRound().Number,
    Contribution: keyshards.Contribution{
        ParticipantID: "alice",
        Commitment:  cryptoOut.Commitment,   // 外部组件产出
        ShardDigest: cryptoOut.ShardDigest,  // 只有摘要，没有分片明文
    },
})

// 完成前替换参与者：显式开启新一轮，旧轮贡献全部失效
round, _ := svc.ReplaceParticipants(ctx, keyshards.ReplaceParticipantsInput{
    CeremonyID: "cer-1", RequestID: "rotate-1",
    NewMembers: []string{"alice", "carol", "dave"}, Reason: "rotate bob",
})

// 门限达成后完成：冻结采用集合，写出唯一 outbox
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
  - `state.json`：全部仪式状态与审计事件（临时文件 + fsync + rename 原子替换）；
  - `lock`：`flock(2)` 文件锁，提供跨进程互斥（Windows 为进程内锁占位）；
  - `outbox/<ceremony-id>.json`：完成时写出且**只写一次**的密钥激活消息，
    供外部投递组件消费（`ReadOutboxFile`）。

写盘顺序是“先 outbox、后状态”：若两步之间崩溃，状态仍为 `active`，
重试完成会以相同文件名覆盖重写，不会产生第二条激活消息。

### 审计

所有状态迁移（created / contributed / participants_replaced / completed /
canceled / expired）以及被拒绝的操作（rejected，含原因）都写入按仪式单调递增的
事件流；贡献事件只记录承诺与分片摘要的十六进制编码，没有任何明文字段。

## 运行测试

```sh
go test ./...            # 行为测试（内存与文件两套仓储共用用例）
go test -race ./...      # 含并发完成唯一胜者、替换/贡献/完成/取消/超时交织不变量
```
