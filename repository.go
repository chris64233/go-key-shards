package keyshards

import (
	"context"
)

// Repository 持久化仪式状态（轮次、替换提案、批准、通知、幂等表）、
// 密钥激活 outbox 与审计流。
//
// 实现必须保证：单个仪式的一次 Update 在其可见范围内串行执行
// （内存实现用每仪式互斥量；文件实现用文件锁 + 读改写 + state.json 原子替换），
// 协调层据此保证“门限达成后只有一个并发请求能完成仪式”，以及
// “替换生效/完成/取消/超时并发时只有一个终态”。
//
// 通知（Ceremony.Notifications）内嵌在仪式状态中，因此与状态迁移天然在
// 同一次 Update/同一次 state.json 写盘事务内提交，不存在状态已变而通知丢失的中间态。
type Repository interface {
	// Create 以初始状态写入一个新仪式；ID 已存在时返回 ErrExists。
	Create(ctx context.Context, c *Ceremony, events []AuditEvent) error

	// Update 在一次原子的读-改-写事务内对单个仪式执行 fn：
	// fn 基于当前快照（仓储提供的深拷贝）返回修改后的仪式与需要追加的审计事件。
	//
	// 语义：
	//   - 返回的仪式非 nil：提交该状态并追加事件（无论 err 是否为 nil），
	//     Update 原样把 err 返回给调用方；
	//   - 返回的仪式为 nil 且 err 非 nil：丢弃状态修改，但返回的事件（若有）
	//     仍会被追加，用于对“被拒绝的贡献/操作”留审计痕；
	//   - 仪式不存在时返回 ErrNotFound。
	Update(ctx context.Context, id string, fn func(c *Ceremony) (*Ceremony, []AuditEvent, error)) error

	// Get 读取仪式快照；不存在返回 ErrNotFound。
	Get(ctx context.Context, id string) (*Ceremony, error)

	// List 列出全部仪式（按 ID 排序，供审计/管理查询）。
	List(ctx context.Context) ([]*Ceremony, error)

	// Audit 查询指定仪式的审计事件（seq 从 fromSeq 开始，0 表示从头）；
	// ceremonyID 为空时查询全部仪式，按 (仪式ID, seq) 有序返回，最多 limit 条（<=0 表示不限）。
	Audit(ctx context.Context, ceremonyID string, fromSeq int64, limit int) ([]AuditEvent, error)

	// Close 释放底层资源。
	Close() error
}
