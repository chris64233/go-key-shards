// Package keyshards 实现门限密钥分片仪式的状态协调层。
//
// 本包只做状态编排（仪式、轮次、成员、门限、替换批准、幂等、终态、
// 事务性通知与 outbox、审计），不实现任何密码学计算：承诺（Commitment）
// 与分片摘要（ShardDigest）均由外部密码学组件产出，协调层不接受也不记录
// 秘密分片明文。
//
// 完成前替换参与者是“发起 + 门限批准”两步流程：InitiateReplacement 发起后
// 请求进入 pending；继续参与者（新旧成员集合交集）通过 ApproveReplacement
// 逐一同意，同意数达到门限即在同一事务内开启新轮、重新冻结成员与截止时间、
// 作废旧轮全部贡献、关闭其他待决请求并写出通知。替换记录（Replacement）
// 保留新旧轮次、批准人与失效贡献快照的关联，可通过 GetReplacement /
// ListReplacements 查询；通知通过 Notifications 按序消费。
//
// 主要类型：Service 提供创建/贡献/替换发起/替换批准/撤销/完成/取消/查询操作；
// Repository 抽象原子的单仪式读-改-写持久化，实现见 MemRepository 与
// FileRepository。
package keyshards
