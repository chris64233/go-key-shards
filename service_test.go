package keyshards

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---- 测试夹具 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type repoFixture struct {
	name string
	new  func(t *testing.T) Repository
}

func fixtures(t *testing.T) []repoFixture {
	t.Helper()
	return []repoFixture{
		{name: "mem", new: func(t *testing.T) Repository { return NewMemRepository() }},
		{name: "file", new: func(t *testing.T) Repository {
			dir := t.TempDir()
			r, err := NewFileRepository(dir)
			if err != nil {
				t.Fatalf("NewFileRepository: %v", err)
			}
			t.Cleanup(func() { _ = r.Close() })
			return r
		}},
	}
}

func cv(participant string, n byte) Contribution {
	return Contribution{
		ParticipantID: participant,
		Commitment:    []byte{0xC0, n},
		ShardDigest:   []byte{0xD0, n},
	}
}

func mustCreate(t *testing.T, s *Service, id string, members []string, threshold int, deadline time.Time) *Ceremony {
	t.Helper()
	c, err := s.CreateCeremony(context.Background(), CreateInput{
		ID: id, Members: members, Threshold: threshold, Deadline: deadline,
	})
	if err != nil {
		t.Fatalf("CreateCeremony: %v", err)
	}
	return c
}

func contrib(t *testing.T, s *Service, id, reqID string, round int, c Contribution) *ContributeResult {
	t.Helper()
	res, err := s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: id, RequestID: reqID, RoundNumber: round, Contribution: c,
	})
	if err != nil {
		t.Fatalf("SubmitContribution(%s): %v", reqID, err)
	}
	return res
}

func errIs(err, target error) bool { return errors.Is(err, target) }

// ---- 创建 ----

func TestCreateCeremonyFreezesConfig(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			deadline := clk.now().Add(time.Hour)

			c := mustCreate(t, s, "cer-1", []string{"a", "b", "b", "c"}, 2, deadline)

			if c.Status != StatusActive {
				t.Fatalf("status = %s", c.Status)
			}
			if c.Threshold != 2 {
				t.Fatalf("threshold = %d", c.Threshold)
			}
			if !c.Deadline.Equal(deadline) {
				t.Fatalf("deadline = %v", c.Deadline)
			}
			r := c.CurrentRound()
			if r.Number != 1 {
				t.Fatalf("first round = %d, want 1", r.Number)
			}
			if got, want := r.Members, []string{"a", "b", "c"}; !equalStrings(got, want) {
				t.Fatalf("frozen members = %v, want %v", got, want)
			}
			if r.Threshold != 2 {
				t.Fatalf("round threshold = %d", r.Threshold)
			}

			// 重复 ID 拒绝。
			_, err := s.CreateCeremony(context.Background(), CreateInput{
				ID: "cer-1", Members: []string{"a"}, Threshold: 1, Deadline: deadline,
			})
			if !errIs(err, ErrExists) {
				t.Fatalf("duplicate create err = %v, want ErrExists", err)
			}
		})
	}
}

func TestCreateValidation(t *testing.T) {
	s := NewService(NewMemRepository(), newFakeClock().now)
	deadline := time.Now().Add(time.Hour)
	cases := []struct {
		name string
		in   CreateInput
	}{
		{"empty id", CreateInput{Members: []string{"a"}, Threshold: 1, Deadline: deadline}},
		{"no members", CreateInput{ID: "x", Threshold: 1, Deadline: deadline}},
		{"empty member", CreateInput{ID: "x", Members: []string{"", "a"}, Threshold: 1, Deadline: deadline}},
		{"threshold zero", CreateInput{ID: "x", Members: []string{"a"}, Threshold: 0, Deadline: deadline}},
		{"threshold too large", CreateInput{ID: "x", Members: []string{"a"}, Threshold: 2, Deadline: deadline}},
		{"zero deadline", CreateInput{ID: "x", Members: []string{"a"}, Threshold: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.CreateCeremony(context.Background(), tc.in); !errIs(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

// ---- 贡献：成员/轮次/截止/幂等/冲突 ----

func TestSubmitContributionAcceptanceAndReplay(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))

			res := contrib(t, s, "c", "req-a", 1, cv("a", 1))
			if res.ContributionCount != 1 || res.Replayed {
				t.Fatalf("unexpected first result: %+v", res)
			}

			// 同请求号同内容重试：原样返回，不重复计入门限。
			replay := contrib(t, s, "c", "req-a", 1, cv("a", 1))
			if !replay.Replayed || replay.ContributionCount != 1 {
				t.Fatalf("replay = %+v", replay)
			}

			// 同一参与者换请求号再来：拒绝。
			_, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "req-a2", RoundNumber: 1, Contribution: cv("a", 9),
			})
			if !errIs(err, ErrAlreadyContributed) {
				t.Fatalf("duplicate participant err = %v", err)
			}
		})
	}
}

func TestSubmitContributionIdempotencyConflict(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 2, clk.now().Add(time.Hour))

	first := cv("a", 1)
	contrib(t, s, "c", "req-x", 1, first)

	// 同请求号、不同承诺/摘要 => 冲突。
	diff := first
	diff.ShardDigest = []byte{0xEE}
	_, err := s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "req-x", RoundNumber: 1, Contribution: diff,
	})
	if !errIs(err, ErrConflict) {
		t.Fatalf("conflict err = %v, want ErrConflict", err)
	}

	// 同请求号、不同参与者 => 同样冲突（指纹纳入参与者）。
	_, err = s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "req-x", RoundNumber: 1, Contribution: cv("b", 1),
	})
	if !errIs(err, ErrConflict) {
		t.Fatalf("participant swap err = %v, want ErrConflict", err)
	}

	got, err := s.GetCeremony(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.CurrentRound().Contributions) != 1 {
		t.Fatalf("conflicting retries must not be counted, got %d", len(got.CurrentRound().Contributions))
	}
}

func TestSubmitContributionRejections(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b"}, 2, clk.now().Add(time.Hour))

			// 非成员。
			_, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "r1", RoundNumber: 1, Contribution: cv("outsider", 1),
			})
			if !errIs(err, ErrNotMember) {
				t.Fatalf("not member err = %v", err)
			}

			// 超过截止时间：贡献拒绝且仪式惰性过期。
			clk.advance(2 * time.Hour)
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "r2", RoundNumber: 1, Contribution: cv("a", 1),
			})
			if !errIs(err, ErrDeadlineExceeded) {
				t.Fatalf("after deadline err = %v", err)
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.Status != StatusExpired {
				t.Fatalf("status after deadline = %s", got.Status)
			}

			// 终态后任何贡献都拒绝。
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "r3", RoundNumber: 1, Contribution: cv("b", 2),
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("terminal err = %v", err)
			}
		})
	}
}

// ---- 轮次替换 ----

func TestReplaceParticipantsOpensNewRound(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))

			contrib(t, s, "c", "req-a", 1, cv("a", 1))
			contrib(t, s, "c", "req-b", 1, cv("b", 1))

			// 显式开启新一轮替换成员：旧轮 2 个贡献失效，门限重新计数。
			rn, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
				CeremonyID: "c", RequestID: "rep-1", NewMembers: []string{"a", "d", "e"}, Reason: "b rotated",
			})
			if err != nil {
				t.Fatalf("ReplaceParticipants: %v", err)
			}
			if rn != 2 {
				t.Fatalf("new round = %d, want 2", rn)
			}

			got, _ := s.GetCeremony(context.Background(), "c")
			if len(got.Rounds) != 2 {
				t.Fatalf("rounds = %d", len(got.Rounds))
			}
			cur := got.CurrentRound()
			if !equalStrings(cur.Members, []string{"a", "d", "e"}) {
				t.Fatalf("new members = %v", cur.Members)
			}
			if len(cur.Contributions) != 0 {
				t.Fatalf("new round must start with zero contributions")
			}

			// 旧轮次贡献一律拒绝。
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "req-c-old", RoundNumber: 1, Contribution: cv("c", 1),
			})
			if !errIs(err, ErrStaleRound) {
				t.Fatalf("stale round err = %v", err)
			}

			// 旧轮次里的成员 a 的历史贡献不计入新轮：a 可以在新轮再贡献一次。
			res := contrib(t, s, "c", "req-a-r2", 2, cv("a", 2))
			if res.ContributionCount != 1 {
				t.Fatalf("count on new round = %d", res.ContributionCount)
			}

			// 同请求号同成员集合重试：返回同一轮次，不重复开轮。
			rn2, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
				CeremonyID: "c", RequestID: "rep-1", NewMembers: []string{"a", "d", "e"},
			})
			if err != nil || rn2 != 2 {
				t.Fatalf("replace replay = (%d, %v)", rn2, err)
			}
			got, _ = s.GetCeremony(context.Background(), "c")
			if len(got.Rounds) != 2 {
				t.Fatalf("replay must not add a round, got %d", len(got.Rounds))
			}

			// 同请求号不同成员集合：冲突。
			_, err = s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
				CeremonyID: "c", RequestID: "rep-1", NewMembers: []string{"a", "d"},
			})
			if !errIs(err, ErrConflict) {
				t.Fatalf("replace conflict err = %v", err)
			}
		})
	}
}

func TestReplaceParticipantsValidationAndTerminal(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 3, clk.now().Add(time.Hour))

	// 替换后人数低于冻结门限：拒绝。
	_, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
		CeremonyID: "c", RequestID: "rep", NewMembers: []string{"a", "b"},
	})
	if !errIs(err, ErrInvalidArgument) {
		t.Fatalf("below threshold err = %v", err)
	}

	if err := s.Cancel(context.Background(), "c", "n/a"); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
		CeremonyID: "c", RequestID: "rep2", NewMembers: []string{"a", "b", "c"},
	})
	if !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("replace after cancel err = %v", err)
	}
}

// ---- 完成：门限、并发唯一、outbox 冻结 ----

func TestCompleteThresholdAndFrozenOutbox(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
			contrib(t, s, "c", "req-a", 1, cv("a", 1))

			// 门限未达不能完成。
			_, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp", KeyID: "key-1", Payload: []byte("agg"),
			})
			if !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("early complete err = %v", err)
			}

			contrib(t, s, "c", "req-b", 1, cv("b", 2))
			outbox, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp", KeyID: "key-1", Payload: []byte("agg"),
			})
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if outbox.KeyID != "key-1" || outbox.RoundNumber != 1 {
				t.Fatalf("outbox = %+v", outbox)
			}
			// 实际采用的贡献集合被冻结，按成员顺序排列。
			if got := participantIDs(outbox.Contributions); !equalStrings(got, []string{"a", "b"}) {
				t.Fatalf("adopted = %v", got)
			}

			// 完成后到达的额外贡献不能改变结果。
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "req-c-late", RoundNumber: 1, Contribution: cv("c", 3),
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("late contribution err = %v", err)
			}

			// 第二次完成（不同请求号）拒绝；同请求号同内容重放返回同一 outbox。
			_, err = s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp-other", KeyID: "key-1",
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("second complete err = %v", err)
			}
			replayed, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp", KeyID: "key-1", Payload: []byte("agg"),
			})
			if err != nil {
				t.Fatalf("complete replay: %v", err)
			}
			if replayed.ActivatedAt != outbox.ActivatedAt || !bytes.Equal(replayed.Payload, outbox.Payload) {
				t.Fatalf("replayed outbox differs: %+v vs %+v", replayed, outbox)
			}

			// 取消与替换在完成后同样被拒。
			if err := s.Cancel(context.Background(), "c", "x"); !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("cancel after complete err = %v", err)
			}
			_, err = s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
				CeremonyID: "c", RequestID: "rep", NewMembers: []string{"a", "b"},
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("replace after complete err = %v", err)
			}
		})
	}
}

func TestConcurrentCompleteSingleWinner(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
			contrib(t, s, "c", "req-a", 1, cv("a", 1))
			contrib(t, s, "c", "req-b", 1, cv("b", 2))

			const n = 16
			var wg sync.WaitGroup
			winners := make(chan string, n)
			errorsCh := make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					reqID := fmt.Sprintf("comp-%d", i)
					ob, err := s.Complete(context.Background(), CompleteInput{
						CeremonyID: "c", RequestID: reqID, KeyID: "key-1",
					})
					if err == nil {
						winners <- reqID
						_ = ob
					} else {
						errorsCh <- err
					}
				}(i)
			}
			wg.Wait()
			close(winners)
			close(errorsCh)

			var ws []string
			for w := range winners {
				ws = append(ws, w)
			}
			if len(ws) != 1 {
				t.Fatalf("winners = %v, want exactly 1", ws)
			}
			for err := range errorsCh {
				if !errIs(err, ErrCeremonyTerminal) {
					t.Fatalf("unexpected concurrent complete error: %v", err)
				}
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.Status != StatusCompleted || got.Outbox == nil {
				t.Fatalf("final state = %s outbox=%v", got.Status, got.Outbox)
			}
			if len(got.Outbox.Contributions) < got.Threshold {
				t.Fatalf("adopted %d < threshold %d", len(got.Outbox.Contributions), got.Threshold)
			}
		})
	}
}

// ---- 取消 / 过期 ----

func TestCancel(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
			contrib(t, s, "c", "req-a", 1, cv("a", 1))

			if err := s.Cancel(context.Background(), "c", "abort"); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.Status != StatusCanceled || got.CancelReason != "abort" || got.Outbox != nil {
				t.Fatalf("after cancel: %+v", got)
			}

			// 门限虽已满足，取消后不能完成，也不能重复取消。
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "x", KeyID: "k",
			}); !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("complete after cancel err = %v", err)
			}
			if err := s.Cancel(context.Background(), "c", "again"); !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("double cancel err = %v", err)
			}
		})
	}
}

func TestExpiryLazyAndDeadlineForNewOps(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	// 创建时截止时间已过：允许创建（配置在创建瞬间冻结），首个操作触发过期。
	c := mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(-time.Minute))
	if c.Status != StatusActive {
		t.Fatalf("new ceremony past deadline status = %s", c.Status)
	}
	got, err := s.GetCeremony(context.Background(), "c")
	if err != nil || got.Status != StatusExpired {
		t.Fatalf("lazy expiry via get: status=%s err=%v", got.Status, err)
	}
}

// ---- 审计 ----

func TestAuditTrail(t *testing.T) {
	clk := newFakeClock()
	repo := NewMemRepository()
	s := NewService(repo, clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	contrib(t, s, "c", "req-a", 1, cv("a", 1))
	_, _ = s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "req-x", RoundNumber: 1, Contribution: cv("zzz", 1),
	}) // 非成员，被拒但应留痕
	if _, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k",
	}); err != nil {
		t.Fatal(err)
	}

	events, err := s.Audit(context.Background(), "c", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, len(events))
	for i, e := range events {
		kinds[i] = e.Kind
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap: event %d has seq %d", i, e.Seq)
		}
	}
	want := []string{AuditCreated, AuditContributed, AuditRejected, AuditCompleted}
	if !equalStrings(kinds, want) {
		t.Fatalf("audit kinds = %v, want %v", kinds, want)
	}

	// fromSeq 分页。
	page, err := s.Audit(context.Background(), "c", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Kind != AuditRejected || page[0].Seq != 3 {
		t.Fatalf("page = %+v", page)
	}

	// 拒绝事件含原因与动作人；贡献事件只记录摘要/承诺，没有明文字段。
	var rejected, contributed *AuditEvent
	for i := range events {
		switch events[i].Kind {
		case AuditRejected:
			rejected = &events[i]
		case AuditContributed:
			contributed = &events[i]
		}
	}
	if rejected.Actor != "zzz" || rejected.Detail["reason"] != "not_member" {
		t.Fatalf("rejected event = %+v", rejected)
	}
	if contributed.Detail["shard_digest"] == "" || contributed.Detail["commitment"] == "" {
		t.Fatalf("contribution audit missing digest/commitment: %+v", contributed.Detail)
	}
	if _, hasPlaintext := contributed.Detail["shard"]; hasPlaintext {
		t.Fatal("audit must never record plaintext shard")
	}
}

// ---- 幂等重放跨越状态边界：重试必须返回首次结果 ----

func TestContributionReplaySurvivesRoundReplacementAndTerminal(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))

			first := contrib(t, s, "c", "req-a", 1, cv("a", 1))
			if first.ContributionCount != 1 {
				t.Fatalf("first = %+v", first)
			}

			// 替换参与者开启新一轮。
			if _, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
				CeremonyID: "c", RequestID: "rep", NewMembers: []string{"a", "b", "c"},
			}); err != nil {
				t.Fatal(err)
			}

			// 旧轮次已 stale，但同一请求号重试返回首次结果，而不是 ErrStaleRound。
			replayAfterReplace := contrib(t, s, "c", "req-a", 1, cv("a", 1))
			if !replayAfterReplace.Replayed || replayAfterReplace.RoundNumber != 1 ||
				replayAfterReplace.ContributionCount != 1 {
				t.Fatalf("replay after replace = %+v", replayAfterReplace)
			}
			// 重放不得在新一轮重新计数。
			got, _ := s.GetCeremony(context.Background(), "c")
			if len(got.CurrentRound().Contributions) != 0 {
				t.Fatalf("replay leaked into new round: %d", len(got.CurrentRound().Contributions))
			}

			// 完成仪式后重放仍返回首次结果（客户端网络重试场景）。
			contrib(t, s, "c", "req-a2", 2, cv("a", 2))
			contrib(t, s, "c", "req-b2", 2, cv("b", 2))
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "done", KeyID: "k",
			}); err != nil {
				t.Fatal(err)
			}
			replayAfterComplete := contrib(t, s, "c", "req-a", 1, cv("a", 1))
			if !replayAfterComplete.Replayed {
				t.Fatalf("replay after complete = %+v", replayAfterComplete)
			}

			// 同请求号、轮次号也改成新轮：这不是重试而是复用请求号 => 冲突。
			_, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "req-a", RoundNumber: 2, Contribution: cv("a", 2),
			})
			if !errIs(err, ErrConflict) {
				t.Fatalf("request id reuse err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestReplaceAndCompleteReplayAfterTerminal(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(24*time.Hour))
	contrib(t, s, "c", "req-a", 1, cv("a", 1))

	rn, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
		CeremonyID: "c", RequestID: "rep", NewMembers: []string{"a", "b"},
	})
	if err != nil || rn != 2 {
		t.Fatalf("replace = (%d, %v)", rn, err)
	}
	contrib(t, s, "c", "req-b2", 2, cv("b", 2))
	ob, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k", Payload: []byte("p"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// 完成后替换请求重放仍返回其开启的轮次号。
	rn2, err := s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
		CeremonyID: "c", RequestID: "rep", NewMembers: []string{"a", "b"},
	})
	if err != nil || rn2 != 2 {
		t.Fatalf("replace replay after complete = (%d, %v)", rn2, err)
	}
	// 完成请求同号重放返回同一 outbox。
	ob2, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k", Payload: []byte("p"),
	})
	if err != nil || ob2.ActivatedAt != ob.ActivatedAt {
		t.Fatalf("complete replay = (%+v, %v)", ob2, err)
	}
	// 完成请求同号但载荷不同 => 终态冲突。
	_, err = s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k", Payload: []byte("different"),
	})
	if !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("complete payload swap err = %v", err)
	}
}

// ---- 文件仓储：持久化恢复与 outbox 唯一落盘 ----

func TestFileRepositoryPersistenceAndOutboxFile(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()

	func() {
		repo, err := NewFileRepository(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer repo.Close()
		s := NewService(repo, clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
		contrib(t, s, "c", "req-a", 1, cv("a", 1))
		contrib(t, s, "c", "req-b", 1, cv("b", 2))
		if _, err := s.Complete(context.Background(), CompleteInput{
			CeremonyID: "c", RequestID: "done", KeyID: "key-1", Payload: []byte("agg"),
		}); err != nil {
			t.Fatal(err)
		}
	}()

	entries, err := os.ReadDir(filepath.Join(dir, "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "c.json" {
		t.Fatalf("outbox files = %v, want exactly c.json", entries)
	}

	// 重新打开：状态、outbox、审计全部恢复。
	repo2, err := NewFileRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repo2.Close()
	got, err := repo2.Get(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusCompleted || got.Outbox == nil {
		t.Fatalf("recovered state = %s", got.Status)
	}
	if got.Outbox.KeyID != "key-1" || string(got.Outbox.Payload) != "agg" {
		t.Fatalf("recovered outbox = %+v", got.Outbox)
	}
	ob, err := repo2.ReadOutboxFile("c")
	if err != nil {
		t.Fatal(err)
	}
	if ob.RoundNumber != 1 || len(ob.Contributions) != 2 {
		t.Fatalf("outbox file = %+v", ob)
	}
	events, err := repo2.Audit(context.Background(), "c", 0, 0)
	if err != nil || len(events) < 4 {
		t.Fatalf("recovered audit = %d events, err=%v", len(events), err)
	}

	// 终态恢复后操作仍被拒，且不会产生第二个 outbox 文件。
	s2 := NewService(repo2, clk.now)
	if err := s2.Cancel(context.Background(), "c", "x"); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("post-recovery cancel err = %v", err)
	}
	entries, _ = os.ReadDir(filepath.Join(dir, "outbox"))
	if len(entries) != 1 {
		t.Fatalf("outbox file count changed: %d", len(entries))
	}
}

func TestFileRepositoryNotFound(t *testing.T) {
	dir := t.TempDir()
	repo, err := NewFileRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	if _, err := repo.Get(context.Background(), "missing"); !errIs(err, ErrNotFound) {
		t.Fatalf("get err = %v", err)
	}
	if err := repo.Update(context.Background(), "missing", func(c *Ceremony) (*Ceremony, []AuditEvent, error) {
		return c, nil, nil
	}); !errIs(err, ErrNotFound) {
		t.Fatalf("update err = %v", err)
	}
}

// ---- 替换/贡献/完成/取消/超时并发交织：只能得到合法演进与终态 ----

func TestConcurrentInterleavingInvariants(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			const iterations = 40
			for iter := 0; iter < iterations; iter++ {
				clk := newFakeClock()
				repo := f.new(t)
				s := NewService(repo, clk.now)
				id := fmt.Sprintf("c-%d", iter)
				mustCreate(t, s, id, []string{"a", "b", "c", "d"}, 2, clk.now().Add(24*time.Hour))

				var wg sync.WaitGroup
				barrier := make(chan struct{})

				// 4 个成员反复尝试贡献（每轮一次，失败即停）。
				for _, m := range []string{"a", "b", "c", "d"} {
					wg.Add(1)
					go func(m string, iter int) {
						defer wg.Done()
						<-barrier
						// 可能成功（首轮或替换后恰为新成员），也可能因各种合法原因失败。
						_, _ = s.SubmitContribution(context.Background(), ContributeInput{
							CeremonyID:   id,
							RequestID:    fmt.Sprintf("r1-%s-%d", m, iter),
							RoundNumber:  1,
							Contribution: cv(m, byte(iter)),
						})
					}(m, iter)
				}

				// 替换者：与贡献/完成/取消竞争，最多开启一轮。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.ReplaceParticipants(context.Background(), ReplaceParticipantsInput{
						CeremonyID: id,
						RequestID:  fmt.Sprintf("rep-%d", iter),
						NewMembers: []string{"a", "c", "e", "f"},
						Reason:     "race",
					})
				}()

				// 完成者。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.Complete(context.Background(), CompleteInput{
						CeremonyID: id,
						RequestID:  fmt.Sprintf("done-%d", iter),
						KeyID:      "key-1",
					})
				}()

				// 取消者。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_ = s.Cancel(context.Background(), id, "race")
				}()

				close(barrier)
				wg.Wait()

				assertCeremonyInvariants(t, s, id)
			}
		})
	}
}

func assertCeremonyInvariants(t *testing.T, s *Service, id string) {
	t.Helper()
	c, err := s.GetCeremony(context.Background(), id)
	if err != nil {
		t.Fatalf("get after race: %v", err)
	}

	// 1. 轮次号从 1 严格递增，不跳号。
	for i, r := range c.Rounds {
		if r.Number != i+1 {
			t.Fatalf("round numbers not contiguous: %+v", c.Rounds)
		}
		if r.Threshold != c.Threshold {
			t.Fatalf("round threshold drifted: %d vs %d", r.Threshold, c.Threshold)
		}
		// 2. 每个成员每轮至多一条贡献，且贡献者必须是该轮成员。
		if len(r.Contributions) > len(r.Members) {
			t.Fatalf("round %d has %d contributions for %d members", r.Number, len(r.Contributions), len(r.Members))
		}
		for p := range r.Contributions {
			if !isMember(r.Members, p) {
				t.Fatalf("round %d contribution by non-member %s", r.Number, p)
			}
		}
	}

	switch c.Status {
	case StatusActive:
		if c.Outbox != nil {
			t.Fatal("active ceremony must not have outbox")
		}
	case StatusCompleted:
		// 3. 唯一 outbox：冻结的贡献集合来自完成轮且达到门限。
		if c.Outbox == nil {
			t.Fatal("completed without outbox")
		}
		cur := c.CurrentRound()
		if c.Outbox.RoundNumber != cur.Number {
			t.Fatalf("outbox round %d != final round %d", c.Outbox.RoundNumber, cur.Number)
		}
		if len(c.Outbox.Contributions) < c.Threshold {
			t.Fatalf("adopted %d < threshold %d", len(c.Outbox.Contributions), c.Threshold)
		}
		for _, adopted := range c.Outbox.Contributions {
			if !isMember(cur.Members, adopted.ParticipantID) {
				t.Fatalf("adopted non-member %s", adopted.ParticipantID)
			}
		}
		if hasDup(participantIDs(c.Outbox.Contributions)) {
			t.Fatal("duplicate participant in adopted set")
		}
	case StatusCanceled, StatusExpired:
		if c.Outbox != nil {
			t.Fatalf("%s ceremony must not have outbox", c.Status)
		}
	default:
		t.Fatalf("illegal final status %q", c.Status)
	}
}

// ---- 辅助 ----

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasDup(xs []string) bool {
	seen := map[string]struct{}{}
	for _, x := range xs {
		if _, ok := seen[x]; ok {
			return true
		}
		seen[x] = struct{}{}
	}
	return false
}
