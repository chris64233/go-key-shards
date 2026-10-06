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

func initiate(t *testing.T, s *Service, id, reqID, by string, members []string, newDeadline time.Time, reason string) *Replacement {
	t.Helper()
	rep, err := s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID:  id,
		RequestID:   reqID,
		RequestedBy: by,
		NewMembers:  members,
		NewDeadline: newDeadline,
		Reason:      reason,
	})
	if err != nil {
		t.Fatalf("InitiateReplacement(%s): %v", reqID, err)
	}
	return rep
}

func approve(t *testing.T, s *Service, id, reqID, voteID, voter string) *Replacement {
	t.Helper()
	rep, err := s.ApproveReplacement(context.Background(), ApprovalInput{
		CeremonyID: id, RequestID: reqID, VoteRequestID: voteID, Voter: voter,
	})
	if err != nil {
		t.Fatalf("ApproveReplacement(%s by %s): %v", reqID, voter, err)
	}
	return rep
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
			if !r.Deadline.Equal(deadline) {
				t.Fatalf("round deadline = %v", r.Deadline)
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

// ---- 参与者替换：门限批准、新轮冻结、旧贡献失效 ----

// 达到门限数量的继续参与者同意后才生效；生效开启新轮并重新冻结成员与截止时间，
// 旧轮贡献全部快照失效，新轮必须重新收集门限数量的贡献。
func TestReplacementThresholdApprovalsAndFreshRound(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c", "d"}, 3, clk.now().Add(24*time.Hour))

			// 旧轮已有 a、b 两个贡献（未达门限 3）。
			contrib(t, s, "c", "req-a", 1, cv("a", 1))
			contrib(t, s, "c", "req-b", 1, cv("b", 2))

			newDeadline := clk.now().Add(48 * time.Hour)
			// a 发起替换 b：新集合 [a c d e]，继续参与者为 a、c、d。
			rep := initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d", "e"}, newDeadline, "rotate b")
			if rep.Status != ReplacementPending || !equalStrings(rep.Approvers, []string{"a"}) {
				t.Fatalf("after initiate: %+v", rep)
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.CurrentRound().Number != 1 {
				t.Fatalf("round must not change while pending")
			}

			// 被替换出去的 b 没有批准权。
			_, err := s.ApproveReplacement(context.Background(), ApprovalInput{
				CeremonyID: "c", RequestID: "rep-1", VoteRequestID: "vote-b", Voter: "b",
			})
			if !errIs(err, ErrApproverNotContinuing) {
				t.Fatalf("removed member vote err = %v", err)
			}

			// c 同意：2/3，仍不生效。
			rep = approve(t, s, "c", "rep-1", "vote-c", "c")
			if rep.Status != ReplacementPending || !equalStrings(rep.Approvers, []string{"a", "c"}) {
				t.Fatalf("after c vote: %+v", rep)
			}
			got, _ = s.GetCeremony(context.Background(), "c")
			if got.CurrentRound().Number != 1 {
				t.Fatalf("round must not change before threshold")
			}

			// c 重复同意拒绝。
			_, err = s.ApproveReplacement(context.Background(), ApprovalInput{
				CeremonyID: "c", RequestID: "rep-1", VoteRequestID: "vote-c-again", Voter: "c",
			})
			if !errIs(err, ErrAlreadyApproved) {
				t.Fatalf("duplicate vote err = %v", err)
			}

			// d 同意：3/3，同一事务生效。
			rep = approve(t, s, "c", "rep-1", "vote-d", "d")
			if rep.Status != ReplacementApproved || rep.NewRoundNumber != 2 || rep.PreviousRound != 1 {
				t.Fatalf("after d vote: %+v", rep)
			}
			if !equalStrings(rep.Approvers, []string{"a", "c", "d"}) {
				t.Fatalf("approvers = %v", rep.Approvers)
			}
			// 失效贡献快照：旧轮 a、b 的贡献，按旧轮成员顺序。
			if ids := participantIDs(rep.InvalidatedContributions); !equalStrings(ids, []string{"a", "b"}) {
				t.Fatalf("invalidated = %v, want [a b]", ids)
			}

			got, _ = s.GetCeremony(context.Background(), "c")
			if len(got.Rounds) != 2 {
				t.Fatalf("rounds = %d, want 2", len(got.Rounds))
			}
			old, cur := got.Rounds[0], got.CurrentRound()
			if cur.Number != 2 {
				t.Fatalf("current round = %d", cur.Number)
			}
			if !equalStrings(cur.Members, []string{"a", "c", "d", "e"}) {
				t.Fatalf("new members = %v", cur.Members)
			}
			if !cur.Deadline.Equal(newDeadline) {
				t.Fatalf("new round deadline = %v, want %v", cur.Deadline, newDeadline)
			}
			if len(cur.Contributions) != 0 {
				t.Fatalf("new round must start with zero contributions")
			}
			// 旧轮数据仍保留可供审计关联，但已不代表有效集合。
			if len(old.Contributions) != 2 {
				t.Fatalf("old round contributions should be retained for audit, got %d", len(old.Contributions))
			}

			// 迟到的旧轮贡献不得带入新轮。
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "req-c-old", RoundNumber: 1, Contribution: cv("c", 3),
			})
			if !errIs(err, ErrStaleRound) {
				t.Fatalf("late old-round contribution err = %v", err)
			}

			// 新轮必须重新收集：1 个不够门限，不能完成。
			contrib(t, s, "c", "req-a2", 2, cv("a", 4))
			_, err = s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp-early", KeyID: "k",
			})
			if !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("complete below threshold err = %v", err)
			}
			// 旧轮贡献绝不折算：再收集 c、d 后才达到门限 3。
			contrib(t, s, "c", "req-c2", 2, cv("c", 5))
			contrib(t, s, "c", "req-d2", 2, cv("d", 6))
			ob, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "comp", KeyID: "k", Payload: []byte("agg"),
			})
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if ob.RoundNumber != 2 {
				t.Fatalf("outbox round = %d, want 2 (old round must not be reused)", ob.RoundNumber)
			}
			if ids := participantIDs(ob.Contributions); !equalStrings(ids, []string{"a", "c", "d"}) {
				t.Fatalf("adopted = %v", ids)
			}
		})
	}
}

// 门限为 1 时，继续参与者发起即生效（本人同意自动计入）。
func TestReplacementThresholdOneAutoApproves(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(24*time.Hour))

	rep := initiate(t, s, "c", "rep", "a", []string{"a", "c"}, clk.now().Add(48*time.Hour), "x")
	if rep.Status != ReplacementApproved || rep.NewRoundNumber != 2 {
		t.Fatalf("threshold-one replacement = %+v", rep)
	}
	got, _ := s.GetCeremony(context.Background(), "c")
	if got.CurrentRound().Number != 2 {
		t.Fatalf("current round = %d", got.CurrentRound().Number)
	}
}

// 离场者本人可以发起替换但不计票：门限数量的同意全部来自继续参与者。
func TestReplacementInitiatedByLeavingMember(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	// 旧集合 a b c d，新集合 c d e f，继续参与者仅 c、d。
	mustCreate(t, s, "c", []string{"a", "b", "c", "d"}, 2, clk.now().Add(24*time.Hour))

	rep := initiate(t, s, "c", "rep", "a", []string{"c", "d", "e", "f"}, clk.now().Add(48*time.Hour), "a leaves")
	if rep.Status != ReplacementPending || len(rep.Approvers) != 0 {
		t.Fatalf("leaving initiator must not count as approver: %+v", rep)
	}
	rep = approve(t, s, "c", "rep", "vote-c", "c")
	if rep.Status != ReplacementPending || len(rep.Approvers) != 1 {
		t.Fatalf("after c: %+v", rep)
	}
	rep = approve(t, s, "c", "rep", "vote-d", "d")
	if rep.Status != ReplacementApproved || rep.NewRoundNumber != 2 {
		t.Fatalf("after d: %+v", rep)
	}
}

// 继续参与者人数少于门限时，替换在发起阶段即被拒绝（数学上不可能获批）。
func TestReplacementImpossibleQuorumRejected(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 3, clk.now().Add(24*time.Hour))

	_, err := s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID: "c", RequestID: "rep", RequestedBy: "a",
		NewMembers: []string{"a", "x", "y"}, NewDeadline: clk.now().Add(time.Hour),
	})
	if !errIs(err, ErrInvalidArgument) {
		t.Fatalf("impossible quorum err = %v", err)
	}
}

func TestReplacementValidation(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))

	// 新人数低于门限。
	_, err := s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID: "c", RequestID: "r1", RequestedBy: "a",
		NewMembers: []string{"a"}, NewDeadline: clk.now().Add(time.Hour),
	})
	if !errIs(err, ErrInvalidArgument) {
		t.Fatalf("below threshold err = %v", err)
	}
	// 成员集合未变化。
	_, err = s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID: "c", RequestID: "r2", RequestedBy: "a",
		NewMembers: []string{"a", "b", "c"}, NewDeadline: clk.now().Add(time.Hour),
	})
	if !errIs(err, ErrInvalidArgument) {
		t.Fatalf("identical members err = %v", err)
	}
	// 新截止时间不在未来。
	_, err = s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID: "c", RequestID: "r3", RequestedBy: "a",
		NewMembers: []string{"a", "c"}, NewDeadline: clk.now().Add(-time.Minute),
	})
	if !errIs(err, ErrInvalidArgument) {
		t.Fatalf("past deadline err = %v", err)
	}
	// 发起人不是当前轮成员。
	_, err = s.InitiateReplacement(context.Background(), InitiateReplacementInput{
		CeremonyID: "c", RequestID: "r4", RequestedBy: "zzz",
		NewMembers: []string{"a", "c"}, NewDeadline: clk.now().Add(time.Hour),
	})
	if !errIs(err, ErrNotMember) {
		t.Fatalf("outsider initiate err = %v", err)
	}
	// 批准不存在的请求。
	_, err = s.ApproveReplacement(context.Background(), ApprovalInput{
		CeremonyID: "c", RequestID: "nope", VoteRequestID: "v", Voter: "a",
	})
	if !errIs(err, ErrReplacementNotFound) {
		t.Fatalf("vote missing err = %v", err)
	}
}

// ---- 替换请求幂等：同号重放原样返回，异内容冲突，跨越生效/终态仍可重放 ----

func TestReplacementIdempotency(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
			nd := clk.now().Add(48 * time.Hour)

			first := initiate(t, s, "c", "rep", "a", []string{"a", "c", "d"}, nd, "x")
			if first.Status != ReplacementPending {
				t.Fatalf("first = %+v", first)
			}

			// 同号同内容重试：返回同一 pending 快照，不重复创建。
			replay := initiate(t, s, "c", "rep", "a", []string{"a", "c", "d"}, nd, "x")
			if replay.Status != ReplacementPending || replay.CreatedAt != first.CreatedAt {
				t.Fatalf("replay = %+v", replay)
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if len(got.Replacements) != 1 {
				t.Fatalf("replacements = %d, want 1", len(got.Replacements))
			}

			// 同号不同新成员集合：冲突。
			_, err := s.InitiateReplacement(context.Background(), InitiateReplacementInput{
				CeremonyID: "c", RequestID: "rep", RequestedBy: "a",
				NewMembers: []string{"a", "c"}, NewDeadline: nd,
			})
			if !errIs(err, ErrConflict) {
				t.Fatalf("conflict err = %v", err)
			}

			// 投票同号重试：返回请求当前快照，不重复计票。
			approve(t, s, "c", "rep", "vote-c", "c")
			again := approve(t, s, "c", "rep", "vote-c", "c")
			if again.Status != ReplacementApproved || again.NewRoundNumber != 2 {
				t.Fatalf("vote replay = %+v", again)
			}
			got, _ = s.GetCeremony(context.Background(), "c")
			if len(got.Rounds) != 2 {
				t.Fatalf("vote replay must not open another round, got %d", len(got.Rounds))
			}

			// 生效后发起请求同号重放：原样返回 approved 快照（含新轮次号）。
			after := initiate(t, s, "c", "rep", "a", []string{"a", "c", "d"}, nd, "x")
			if after.Status != ReplacementApproved || after.NewRoundNumber != 2 {
				t.Fatalf("post-effect replay = %+v", after)
			}

			// 投票同号但换了批准对象：冲突。
			_, err = s.ApproveReplacement(context.Background(), ApprovalInput{
				CeremonyID: "c", RequestID: "rep", VoteRequestID: "vote-c", Voter: "d",
			})
			if !errIs(err, ErrConflict) {
				t.Fatalf("vote reuse err = %v", err)
			}

			// 仪式完成后，替换请求同号重放仍返回其首次结果，而不是终态错误。
			contrib(t, s, "c", "ra2", 2, cv("a", 1))
			contrib(t, s, "c", "rc2", 2, cv("c", 2))
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "done", KeyID: "k",
			}); err != nil {
				t.Fatal(err)
			}
			postTerminal := initiate(t, s, "c", "rep", "a", []string{"a", "c", "d"}, nd, "x")
			if postTerminal.Status != ReplacementApproved {
				t.Fatalf("replay after terminal = %+v", postTerminal)
			}
		})
	}
}

// ---- 终态与待决替换互斥：仪式进入终态的同一事务关闭全部 pending 请求 ----

func TestPendingReplacementsClosedOnTerminal(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		clk := newFakeClock()
		s := NewService(NewMemRepository(), clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
		// rep-1 待决；第 1 轮门限恰好达成，完成与替换竞争。
		initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "")
		contrib(t, s, "c", "ra", 1, cv("a", 1))
		contrib(t, s, "c", "rb", 1, cv("b", 2))
		if _, err := s.Complete(context.Background(), CompleteInput{
			CeremonyID: "c", RequestID: "done", KeyID: "k",
		}); err != nil {
			t.Fatal(err)
		}
		rep, err := s.GetReplacement(context.Background(), "c", "rep-1")
		if err != nil {
			t.Fatal(err)
		}
		if rep.Status != ReplacementClosed {
			t.Fatalf("replacement after complete = %s", rep.Status)
		}
		// 关闭后再批准被拒（仪式已终态，返回终态错误），且不会偷偷开新轮。
		if _, err := s.ApproveReplacement(context.Background(), ApprovalInput{
			CeremonyID: "c", RequestID: "rep-1", VoteRequestID: "v", Voter: "c",
		}); !errIs(err, ErrCeremonyTerminal) {
			t.Fatalf("vote after close err = %v", err)
		}
		got, _ := s.GetCeremony(context.Background(), "c")
		if len(got.Rounds) != 1 {
			t.Fatalf("closed replacement must not open a round, got %d", len(got.Rounds))
		}
	})

	t.Run("canceled", func(t *testing.T) {
		clk := newFakeClock()
		s := NewService(NewMemRepository(), clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
		initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "")
		if err := s.Cancel(context.Background(), "c", "abort"); err != nil {
			t.Fatal(err)
		}
		rep, _ := s.GetReplacement(context.Background(), "c", "rep-1")
		if rep.Status != ReplacementClosed {
			t.Fatalf("replacement after cancel = %s", rep.Status)
		}
	})

	t.Run("expired", func(t *testing.T) {
		clk := newFakeClock()
		s := NewService(NewMemRepository(), clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
		initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "")

		// 越过当前轮（第 1 轮）截止时间；下一次操作惰性过期并同事务关闭请求。
		clk.advance(2 * time.Hour)
		_, err := s.ApproveReplacement(context.Background(), ApprovalInput{
			CeremonyID: "c", RequestID: "rep-1", VoteRequestID: "v-c", Voter: "c",
		})
		if !errIs(err, ErrDeadlineExceeded) {
			t.Fatalf("vote after deadline err = %v", err)
		}
		got, _ := s.GetCeremony(context.Background(), "c")
		if got.Status != StatusExpired {
			t.Fatalf("status = %s", got.Status)
		}
		rep := got.Replacements["rep-1"]
		if rep.Status != ReplacementClosed {
			t.Fatalf("replacement after expiry = %s", rep.Status)
		}
	})
}

// 一个请求生效开启新轮时，其他待决请求在同一事务内被关闭，不能再就旧轮生效。
func TestApprovedReplacementClosesOtherPending(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
	initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "")
	initiate(t, s, "c", "rep-2", "a", []string{"a", "b", "d"}, clk.now().Add(48*time.Hour), "")

	approve(t, s, "c", "rep-1", "v-c", "c") // rep-1 生效开启第 2 轮

	rep2, err := s.GetReplacement(context.Background(), "c", "rep-2")
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Status != ReplacementClosed {
		t.Fatalf("competing request = %s, want closed", rep2.Status)
	}
	if _, err := s.ApproveReplacement(context.Background(), ApprovalInput{
		CeremonyID: "c", RequestID: "rep-2", VoteRequestID: "v-b", Voter: "b",
	}); !errIs(err, ErrReplacementFinal) {
		t.Fatalf("vote on closed err = %v", err)
	}
}

// ---- 撤销 ----

func TestWithdrawReplacement(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
	initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "")

	// 非发起人不能撤销。
	if err := s.WithdrawReplacement(context.Background(), "c", "rep-1", "b"); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("withdraw by other err = %v", err)
	}
	if err := s.WithdrawReplacement(context.Background(), "c", "rep-1", "a"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	rep, _ := s.GetReplacement(context.Background(), "c", "rep-1")
	if rep.Status != ReplacementRejected {
		t.Fatalf("after withdraw = %s", rep.Status)
	}
	// 撤销后投票被拒，重复撤销幂等。
	if _, err := s.ApproveReplacement(context.Background(), ApprovalInput{
		CeremonyID: "c", RequestID: "rep-1", VoteRequestID: "v", Voter: "c",
	}); !errIs(err, ErrReplacementFinal) {
		t.Fatalf("vote after withdraw err = %v", err)
	}
	if err := s.WithdrawReplacement(context.Background(), "c", "rep-1", "a"); err != nil {
		t.Fatalf("idempotent withdraw err = %v", err)
	}
}

// ---- 通知：与状态迁移在同一事务保存 ----

func TestReplacementNotificationsTransactional(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
			contrib(t, s, "c", "ra", 1, cv("a", 1))

			nd := clk.now().Add(48 * time.Hour)
			initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, nd, "rotate b")
			ntfs, err := s.Notifications(context.Background(), "c", 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(ntfs) != 1 || ntfs[0].Type != NtfReplacementInitiated ||
				ntfs[0].ReplacementID != "rep-1" {
				t.Fatalf("notifications after initiate = %+v", ntfs)
			}

			// 生效事务同时写出 approved 通知，负载关联新旧轮次、批准人与失效贡献。
			approve(t, s, "c", "rep-1", "v-c", "c")
			ntfs, _ = s.Notifications(context.Background(), "c", 0, 0)
			if len(ntfs) != 2 {
				t.Fatalf("notifications = %d, want 2", len(ntfs))
			}
			approved := ntfs[1]
			if approved.Type != NtfReplacementApproved || approved.RoundNumber != 2 ||
				approved.Detail["previous_round"] != 1 {
				t.Fatalf("approved notification = %+v", approved)
			}
			invalidated, _ := approved.Detail["invalidated_contributions"].([]string)
			if !equalStrings(invalidated, []string{"a"}) {
				t.Fatalf("invalidated in notification = %v", invalidated)
			}
			approvers, _ := approved.Detail["approvers"].([]string)
			if !equalStrings(approvers, []string{"a", "c"}) {
				t.Fatalf("approvers in notification = %v", approvers)
			}
			// 通知状态与仪式状态在同一快照中一致：当前轮已是第 2 轮。
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.CurrentRound().Number != 2 {
				t.Fatalf("state/notif divergence: round %d", got.CurrentRound().Number)
			}
			for i := range ntfs {
				if ntfs[i].Seq != int64(i+1) {
					t.Fatalf("notification seq gap at %d: %d", i, ntfs[i].Seq)
				}
			}

			// 完成事务追加完成通知。
			contrib(t, s, "c", "ra2", 2, cv("a", 2))
			contrib(t, s, "c", "rc2", 2, cv("c", 3))
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "done", KeyID: "k",
			}); err != nil {
				t.Fatal(err)
			}
			ntfs, _ = s.Notifications(context.Background(), "c", 0, 0)
			if len(ntfs) != 3 || ntfs[2].Type != NtfCeremonyCompleted {
				t.Fatalf("notifications after complete = %+v", ntfs)
			}

			// fromSeq 分页。
			page, _ := s.Notifications(context.Background(), "c", 1, 1)
			if len(page) != 1 || page[0].Type != NtfReplacementApproved || page[0].Seq != 2 {
				t.Fatalf("page = %+v", page)
			}
		})
	}
}

// ---- 关联查询：新旧轮次、批准人、失效贡献 ----

func TestReplacementLookup(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
	contrib(t, s, "c", "ra", 1, cv("a", 1))
	contrib(t, s, "c", "rb", 1, cv("b", 2))
	initiate(t, s, "c", "rep-1", "a", []string{"a", "c", "d"}, clk.now().Add(48*time.Hour), "r1")
	initiate(t, s, "c", "rep-2", "b", []string{"b", "c", "d"}, clk.now().Add(72*time.Hour), "r2")
	approve(t, s, "c", "rep-1", "vc", "c")

	rep, err := s.GetReplacement(context.Background(), "c", "rep-1")
	if err != nil {
		t.Fatal(err)
	}
	if rep.PreviousRound != 1 || rep.NewRoundNumber != 2 ||
		!equalStrings(rep.Approvers, []string{"a", "c"}) {
		t.Fatalf("rep-1 = %+v", rep)
	}
	if len(rep.InvalidatedContributions) != 2 {
		t.Fatalf("invalidated = %d", len(rep.InvalidatedContributions))
	}
	if _, err := s.GetReplacement(context.Background(), "c", "missing"); !errIs(err, ErrReplacementNotFound) {
		t.Fatalf("missing lookup err = %v", err)
	}

	all, err := s.ListReplacements(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].ID != "rep-1" || all[1].ID != "rep-2" {
		t.Fatalf("ordered replacements = %+v", all)
	}
	if all[1].Status != ReplacementClosed {
		t.Fatalf("rep-2 should be closed, got %s", all[1].Status)
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
			_, err = s.InitiateReplacement(context.Background(), InitiateReplacementInput{
				CeremonyID: "c", RequestID: "rep", RequestedBy: "a",
				NewMembers: []string{"a", "c"}, NewDeadline: clk.now().Add(time.Hour),
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

// 新轮冻结的是新截止时间：旧截止时间已过不影响新轮，新轮过期只认新值。
func TestNewRoundUsesRefrozenDeadline(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	// 旧轮即将过期前替换：threshold 1，发起即生效。
	clk.advance(59 * time.Minute)
	initiate(t, s, "c", "rep", "a", []string{"a", "c"}, clk.now().Add(2*time.Hour), "extend")
	// 越过旧轮截止时间，但新轮截止更晚：贡献仍被接受。
	clk.advance(30 * time.Minute)
	res := contrib(t, s, "c", "ra2", 2, cv("a", 1))
	if res.RoundNumber != 2 {
		t.Fatalf("contribution round = %d", res.RoundNumber)
	}
	// 越过新轮截止时间：惰性过期。
	clk.advance(3 * time.Hour)
	_, err := s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "rc-late", RoundNumber: 2, Contribution: cv("c", 2),
	})
	if !errIs(err, ErrDeadlineExceeded) {
		t.Fatalf("after new deadline err = %v", err)
	}
	got, _ := s.GetCeremony(context.Background(), "c")
	if got.Status != StatusExpired {
		t.Fatalf("status = %s", got.Status)
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

			// 替换参与者开启新一轮（a 发起 + b 批准）。
			initiate(t, s, "c", "rep", "a", []string{"a", "b", "c", "d"}, clk.now().Add(48*time.Hour), "")
			approve(t, s, "c", "rep", "vote-b", "b")

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

	rep := initiate(t, s, "c", "rep", "a", []string{"a", "b", "c"}, clk.now().Add(48*time.Hour), "")
	if rep.NewRoundNumber != 2 {
		t.Fatalf("replace = %+v", rep)
	}
	contrib(t, s, "c", "req-b2", 2, cv("b", 2))
	ob, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k", Payload: []byte("p"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// 完成后替换请求重放仍返回其 approved 记录（新轮次号 2）。
	rep2 := initiate(t, s, "c", "rep", "a", []string{"a", "b", "c"}, clk.now().Add(48*time.Hour), "")
	if rep2.Status != ReplacementApproved || rep2.NewRoundNumber != 2 {
		t.Fatalf("replace replay after complete = %+v", rep2)
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
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
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

// 替换流程的关联记录与通知同样随文件仓储持久化恢复。
func TestFileRepositoryReplacementRecovery(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()

	func() {
		repo, err := NewFileRepository(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer repo.Close()
		s := NewService(repo, clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))
		contrib(t, s, "c", "ra", 1, cv("a", 1))
		nd := clk.now().Add(48 * time.Hour)
		initiate(t, s, "c", "rep", "a", []string{"a", "c", "d"}, nd, "rotate b")
		approve(t, s, "c", "rep", "vc", "c")
	}()

	repo2, err := NewFileRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repo2.Close()
	got, err := repo2.Get(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rounds) != 2 || !got.Rounds[1].Deadline.Equal(clk.now().Add(48*time.Hour)) {
		t.Fatalf("rounds/deadline not recovered: %+v", got.Rounds)
	}
	rep := got.Replacements["rep"]
	if rep == nil || rep.Status != ReplacementApproved ||
		rep.PreviousRound != 1 || rep.NewRoundNumber != 2 {
		t.Fatalf("replacement not recovered: %+v", rep)
	}
	if len(rep.InvalidatedContributions) != 1 ||
		rep.InvalidatedContributions[0].ParticipantID != "a" {
		t.Fatalf("invalidated snapshot not recovered: %+v", rep.InvalidatedContributions)
	}
	if len(got.Notifications) != 2 ||
		got.Notifications[0].Type != NtfReplacementInitiated ||
		got.Notifications[1].Type != NtfReplacementApproved {
		t.Fatalf("notifications not recovered: %+v", got.Notifications)
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

// ---- 替换/贡献/完成/取消并发交织：只能得到合法演进与终态 ----

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
						_, _ = s.SubmitContribution(context.Background(), ContributeInput{
							CeremonyID:   id,
							RequestID:    fmt.Sprintf("r1-%s-%d", m, iter),
							RoundNumber:  1,
							Contribution: cv(m, byte(iter)),
						})
					}(m, iter)
				}

				// 替换者：threshold 2，a 发起后 b 批准，与贡献/完成/取消竞争。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.InitiateReplacement(context.Background(), InitiateReplacementInput{
						CeremonyID:  id,
						RequestID:   fmt.Sprintf("rep-%d", iter),
						RequestedBy: "a",
						NewMembers:  []string{"a", "c", "e", "f"},
						NewDeadline: clk.now().Add(48 * time.Hour),
						Reason:      "race",
					})
					_, _ = s.ApproveReplacement(context.Background(), ApprovalInput{
						CeremonyID: id, RequestID: fmt.Sprintf("rep-%d", iter),
						VoteRequestID: fmt.Sprintf("vote-c-%d", iter), Voter: "c",
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

// 替换生效与当前轮超时并发：旧轮过期与新轮开启只能有一个结局。
func TestConcurrentReplacementVsExpiry(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			const iterations = 60
			for iter := 0; iter < iterations; iter++ {
				clk := newFakeClock()
				repo := f.new(t)
				s := NewService(repo, clk.now)
				id := fmt.Sprintf("c-%d", iter)
				mustCreate(t, s, id, []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))

				nd := clk.now().Add(48 * time.Hour)
				initiate(t, s, id, "rep", "a", []string{"a", "b", "c", "d"}, nd, "x")

				var wg sync.WaitGroup
				barrier := make(chan struct{})

				// 最后一票使同意达到门限。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.ApproveReplacement(context.Background(), ApprovalInput{
						CeremonyID: id, RequestID: "rep", VoteRequestID: "vote-b", Voter: "b",
					})
				}()
				// 越过旧轮截止时间并触发一次惰性过期。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					clk.advance(2 * time.Hour)
					_, _ = s.GetCeremony(context.Background(), id)
				}()

				close(barrier)
				wg.Wait()

				got, _ := s.GetCeremony(context.Background(), id)
				rep := got.Replacements["rep"]
				switch got.Status {
				case StatusActive:
					// 替换先生效：新轮已开启，截止时间是更晚的新值，请求 approved。
					if got.CurrentRound().Number != 2 {
						t.Fatalf("active but round = %d", got.CurrentRound().Number)
					}
					if rep.Status != ReplacementApproved {
						t.Fatalf("active with round 2 but replacement = %s", rep.Status)
					}
				case StatusExpired:
					// 过期先生效：仪式终止，待决替换在同一事务关闭，未开新轮。
					if len(got.Rounds) != 1 {
						t.Fatalf("expired but rounds = %d", len(got.Rounds))
					}
					if rep.Status != ReplacementClosed {
						t.Fatalf("expired but replacement = %s", rep.Status)
					}
				default:
					t.Fatalf("illegal status %q", got.Status)
				}
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

	// 1. 轮次号从 1 严格递增，不跳号；截止时间均已冻结。
	for i, r := range c.Rounds {
		if r.Number != i+1 {
			t.Fatalf("round numbers not contiguous: %+v", c.Rounds)
		}
		if r.Threshold != c.Threshold {
			t.Fatalf("round threshold drifted: %d vs %d", r.Threshold, c.Threshold)
		}
		if r.Deadline.IsZero() {
			t.Fatalf("round %d has no frozen deadline", r.Number)
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

	// 3. 替换关联完整：approved 请求的新旧轮次与失效贡献快照一致。
	for rid, rep := range c.Replacements {
		switch rep.Status {
		case ReplacementApproved:
			if rep.NewRoundNumber < 1 || rep.PreviousRound != rep.NewRoundNumber-1 {
				t.Fatalf("replacement %s round linkage broken: %+v", rid, rep)
			}
			if rep.NewRoundNumber > len(c.Rounds) {
				t.Fatalf("replacement %s points beyond rounds", rid)
			}
			newRound := c.Rounds[rep.NewRoundNumber-1]
			if !equalStrings(newRound.Members, rep.NewMembers) {
				t.Fatalf("replacement %s members mismatch: %v vs %v", rid, newRound.Members, rep.NewMembers)
			}
			oldRound := c.Rounds[rep.PreviousRound-1]
			if len(rep.InvalidatedContributions) != len(oldRound.Contributions) {
				t.Fatalf("replacement %s invalidated snapshot %d != old round contributions %d",
					rid, len(rep.InvalidatedContributions), len(oldRound.Contributions))
			}
			if len(rep.Approvers) < c.Threshold {
				t.Fatalf("replacement %s approved with %d < threshold %d", rid, len(rep.Approvers), c.Threshold)
			}
		case ReplacementPending:
			if c.Status != StatusActive {
				t.Fatalf("pending replacement %s in %s ceremony", rid, c.Status)
			}
		}
	}

	// 4. 通知序号连续且与状态同生共死：completed/expired 之后必有对应终态通知。
	var sawCompleted, sawExpired bool
	for i, n := range c.Notifications {
		if n.Seq != int64(i+1) {
			t.Fatalf("notification seq gap: %d != %d", n.Seq, i+1)
		}
		switch n.Type {
		case NtfCeremonyCompleted:
			sawCompleted = true
		case NtfCeremonyExpired:
			sawExpired = true
		}
	}
	if (c.Status == StatusCompleted) != sawCompleted {
		t.Fatalf("completed state=%s but notification present=%v", c.Status, sawCompleted)
	}
	if (c.Status == StatusExpired) != sawExpired {
		t.Fatalf("expired state=%s but notification present=%v", c.Status, sawExpired)
	}

	switch c.Status {
	case StatusActive:
		if c.Outbox != nil {
			t.Fatal("active ceremony must not have outbox")
		}
	case StatusCompleted:
		// 5. 唯一 outbox：冻结的贡献集合来自完成轮且达到门限。
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

// ---- 贡献撤回审核 ----

func requestWithdrawal(t *testing.T, s *Service, id, reqID string, round int, participant string, digest []byte, reason, reviewer string) *ContributionWithdrawal {
	t.Helper()
	w, err := s.RequestContributionWithdrawal(context.Background(), WithdrawContributionInput{
		CeremonyID:         id,
		RequestID:          reqID,
		RoundNumber:        round,
		ParticipantID:      participant,
		ContributionDigest: digest,
		Reason:             reason,
		Reviewer:           reviewer,
	})
	if err != nil {
		t.Fatalf("RequestContributionWithdrawal(%s): %v", reqID, err)
	}
	return w
}

func reviewWithdrawal(t *testing.T, s *Service, id, withdrawalID, decisionID, reviewer string, approve bool) *ContributionWithdrawal {
	t.Helper()
	w, err := s.ReviewContributionWithdrawal(context.Background(), ReviewContributionWithdrawalInput{
		CeremonyID:   id,
		WithdrawalID: withdrawalID,
		RequestID:    decisionID,
		Reviewer:     reviewer,
		Approve:      approve,
	})
	if err != nil {
		t.Fatalf("ReviewContributionWithdrawal(%s): %v", withdrawalID, err)
	}
	return w
}

func TestContributionWithdrawalReviewLifecycleAndThresholdRecovery(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
			first := cv("a", 1)
			second := cv("b", 2)
			contrib(t, s, "c", "req-a", 1, first)
			contrib(t, s, "c", "req-b", 1, second)

			// 第二个仪式覆盖申请、拒绝、再申请、通过、补交、完成的完整时间线。
			mustCreate(t, s, "c2", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
			contrib(t, s, "c2", "req-a2", 1, first)
			contrib(t, s, "c2", "req-b2", 1, second)

			w := requestWithdrawal(t, s, "c2", "wd-a", 1, "a", first.ShardDigest, "crypto invalid", "auditor")
			if w.Status != WithdrawalPending {
				t.Fatalf("withdrawal status = %s", w.Status)
			}
			status, reviews, err := s.GetRoundContributionStatus(context.Background(), "c2", 1)
			if err != nil {
				t.Fatal(err)
			}
			if status.ValidCount != 1 || status.PendingReviewCount != 1 || !equalStrings(status.MissingParticipants, []string{"a", "c"}) {
				t.Fatalf("pending status = %+v", status)
			}
			if got := reviews[0].Status; got != ContributionReviewPending {
				t.Fatalf("review status = %s", got)
			}
			if _, err := s.Complete(context.Background(), CompleteInput{CeremonyID: "c2", RequestID: "blocked", KeyID: "k"}); !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("complete during review err = %v", err)
			}

			// 审核拒绝：恢复有效；重放审核请求返回原结果。
			reviewWithdrawal(t, s, "c2", "wd-a", "dec-reject", "auditor", false)
			replayedReject := reviewWithdrawal(t, s, "c2", "wd-a", "dec-reject", "auditor", false)
			if replayedReject.Status != WithdrawalRejected {
				t.Fatalf("replayed rejection = %s", replayedReject.Status)
			}
			status, _, _ = s.GetRoundContributionStatus(context.Background(), "c2", 1)
			if status.ValidCount != 2 || !equalStrings(status.MissingParticipants, []string{"c"}) {
				t.Fatalf("after reject status = %+v", status)
			}

			// 再次申请并通过：原贡献不可采用，同一参与者补交后恢复门限。
			w = requestWithdrawal(t, s, "c2", "wd-a-2", 1, "a", first.ShardDigest, "still invalid", "auditor")
			reviewWithdrawal(t, s, "c2", w.ID, "dec-approve", "auditor", true)
			if _, err := s.Complete(context.Background(), CompleteInput{CeremonyID: "c2", RequestID: "blocked-2", KeyID: "k"}); !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("complete after withdrawal err = %v", err)
			}
			status, reviews, _ = s.GetRoundContributionStatus(context.Background(), "c2", 1)
			if status.ValidCount != 1 || status.WithdrawnCount != 1 || !equalStrings(status.MissingParticipants, []string{"a", "c"}) {
				t.Fatalf("after approval status = %+v", status)
			}
			if reviews[0].Withdrawal == nil || reviews[0].Withdrawal.Status != WithdrawalApproved {
				t.Fatalf("withdrawal timeline missing: %+v", reviews[0])
			}

			replacement := cv("a", 3)
			contrib(t, s, "c2", "req-a-fixed", 1, replacement)
			ob, err := s.Complete(context.Background(), CompleteInput{CeremonyID: "c2", RequestID: "done", KeyID: "k", Payload: []byte("payload")})
			if err != nil {
				t.Fatalf("complete after supplemental contribution: %v", err)
			}
			if ids := participantIDs(ob.Contributions); !equalStrings(ids, []string{"a", "b"}) {
				t.Fatalf("adopted = %v", ids)
			}
			if !bytes.Equal(ob.Contributions[0].ShardDigest, replacement.ShardDigest) {
				t.Fatal("completed outbox adopted withdrawn contribution")
			}

			// 已完成仪式中的贡献不能再撤回。
			_, err = s.RequestContributionWithdrawal(context.Background(), WithdrawContributionInput{
				CeremonyID: "c2", RequestID: "late", RoundNumber: 1, ParticipantID: "b",
				ContributionDigest: second.ShardDigest, Reason: "late", Reviewer: "auditor",
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("withdraw completed contribution err = %v", err)
			}
		})
	}
}

func TestContributionWithdrawalIdempotencyConflictAndStaleRound(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 1, clk.now().Add(24*time.Hour))
	first := cv("a", 1)
	contrib(t, s, "c", "req-a", 1, first)
	contrib(t, s, "c", "req-b", 1, cv("b", 2))

	requestWithdrawal(t, s, "c", "wd", 1, "a", first.ShardDigest, "reason", "auditor")
	replayed := requestWithdrawal(t, s, "c", "wd", 1, "a", first.ShardDigest, "reason", "auditor")
	if replayed.Status != WithdrawalPending {
		t.Fatalf("replay status = %s", replayed.Status)
	}
	cases := []WithdrawContributionInput{
		{CeremonyID: "c", RequestID: "wd", RoundNumber: 2, ParticipantID: "a", ContributionDigest: first.ShardDigest, Reason: "reason", Reviewer: "auditor"},
		{CeremonyID: "c", RequestID: "wd", RoundNumber: 1, ParticipantID: "b", ContributionDigest: first.ShardDigest, Reason: "reason", Reviewer: "auditor"},
		{CeremonyID: "c", RequestID: "wd", RoundNumber: 1, ParticipantID: "a", ContributionDigest: []byte{0x99}, Reason: "reason", Reviewer: "auditor"},
		{CeremonyID: "c", RequestID: "wd", RoundNumber: 1, ParticipantID: "a", ContributionDigest: first.ShardDigest, Reason: "other", Reviewer: "auditor"},
		{CeremonyID: "c", RequestID: "wd", RoundNumber: 1, ParticipantID: "a", ContributionDigest: first.ShardDigest, Reason: "reason", Reviewer: "other"},
	}
	for i, in := range cases {
		if _, err := s.RequestContributionWithdrawal(context.Background(), in); !errIs(err, ErrConflict) {
			t.Fatalf("case %d err = %v, want conflict", i, err)
		}
	}

	initiate(t, s, "c", "rep", "b", []string{"a", "b", "c", "d"}, clk.now().Add(48*time.Hour), "add d")
	got, _ := s.GetCeremony(context.Background(), "c")
	if got.CurrentRound().Number != 2 {
		t.Fatalf("current round = %d", got.CurrentRound().Number)
	}
	if got.ContributionWithdrawals["wd"].Status != WithdrawalClosed {
		t.Fatalf("old pending withdrawal = %s", got.ContributionWithdrawals["wd"].Status)
	}
	_, err := s.RequestContributionWithdrawal(context.Background(), WithdrawContributionInput{
		CeremonyID: "c", RequestID: "old-round", RoundNumber: 1, ParticipantID: "a",
		ContributionDigest: first.ShardDigest, Reason: "late", Reviewer: "auditor",
	})
	if !errIs(err, ErrStaleRound) {
		t.Fatalf("old round withdrawal err = %v", err)
	}
}

func TestConcurrentWithdrawalDecisionSupplementAndComplete(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			for iter := 0; iter < 60; iter++ {
				clk := newFakeClock()
				s := NewService(f.new(t), clk.now)
				id := fmt.Sprintf("c-%d", iter)
				mustCreate(t, s, id, []string{"a", "b"}, 2, clk.now().Add(time.Hour))
				first := cv("a", 1)
				contrib(t, s, id, "req-a", 1, first)
				contrib(t, s, id, "req-b", 1, cv("b", 2))
				requestWithdrawal(t, s, id, "wd-a", 1, "a", first.ShardDigest, "bad", "auditor")

				var wg sync.WaitGroup
				barrier := make(chan struct{})
				wg.Add(3)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.ReviewContributionWithdrawal(context.Background(), ReviewContributionWithdrawalInput{
						CeremonyID: id, WithdrawalID: "wd-a", RequestID: "decide", Reviewer: "auditor", Approve: true,
					})
				}()
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.SubmitContribution(context.Background(), ContributeInput{
						CeremonyID: id, RequestID: "req-a-fixed", RoundNumber: 1, Contribution: cv("a", 9),
					})
				}()
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.Complete(context.Background(), CompleteInput{CeremonyID: id, RequestID: "finish", KeyID: "k"})
				}()
				close(barrier)
				wg.Wait()

				got, _ := s.GetCeremony(context.Background(), id)
				if got.Status == StatusCompleted {
					if got.Outbox == nil || len(got.Outbox.Contributions) != 2 {
						t.Fatalf("completed outbox = %+v", got.Outbox)
					}
					for _, adopted := range got.Outbox.Contributions {
						if adopted.ParticipantID == "a" && bytes.Equal(adopted.ShardDigest, first.ShardDigest) {
							t.Fatal("complete adopted reviewed/withdrawn contribution")
						}
					}
				} else {
					status, _, err := s.GetRoundContributionStatus(context.Background(), id, 1)
					if err != nil {
						t.Fatal(err)
					}
					if status.ValidCount < 2 && got.Status != StatusActive {
						t.Fatalf("noncompleted status = %s, count = %d", got.Status, status.ValidCount)
					}
				}
			}
		})
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
