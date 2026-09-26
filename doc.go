// Package keyshards 实现门限密钥分片仪式的状态协调层。
//
// 本包只做状态编排（仪式、轮次、成员、门限、幂等、终态、outbox 与审计），
// 不实现任何密码学计算：承诺（Commitment）与分片摘要（ShardDigest）均由外部
// 密码学组件产出，协调层不接受也不记录秘密分片明文。
//
// 主要类型：Service 提供创建/贡献/替换/完成/取消/审计操作；Repository 抽象
// 原子的单仪式读-改-写持久化，实现见 MemRepository 与 FileRepository。
package keyshards
