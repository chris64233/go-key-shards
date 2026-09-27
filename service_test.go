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

func propose(t *testing.T, s *Service, in ProposeReplacementInput) *ProposalResult {
	t.Helper()
	res, err := s.ProposeReplacement(context.Background(), in)
	if err != nil {
		t.Fatalf("ProposeReplacement(%s): %v", in.RequestID, err)
	}
	return res
}

func approve(t *testing.T, s *Service, ceremonyID, reqID, proposalID, approver string) *ApproveResult {
	t.Helper()
	res, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: ceremonyID, RequestID: reqID, ProposalID: proposalID, Approver: approver,
	})
	if err != nil {
		t.Fatalf("ApproveReplacement(%s by %s): %v", proposalID, approver, err)
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
			if !r.Deadline.Equal(deadline) {
				t.Fatalf("round deadline = %v, want %v", r.Deadline, deadline)
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

			// 超过截止时间：贡献拒绝且仪式惰性过期，通知同事务落库。
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

// ---- 替换提案：校验与幂等 ----

func TestProposeReplacementValidation(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c", "x"}, 3, clk.now().Add(time.Hour))
	future := clk.now().Add(2 * time.Hour)

	base := func() ProposeReplacementInput {
		return ProposeReplacementInput{
			CeremonyID: "c", RequestID: "req", ProposedBy: "a",
			Remove: []string{"b"}, Add: []string{"d"}, NewDeadline: future,
		}
	}

	// 提案人不是成员。
	in := base()
	in.ProposedBy = "zzz"
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrNotMember) {
		t.Fatalf("outsider proposer err = %v", err)
	}
	// 新截止时间不晚于现在。
	in = base()
	in.NewDeadline = clk.now().Add(-time.Minute)
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("past deadline err = %v", err)
	}
	// Remove/Add 皆空。
	in = base()
	in.Remove, in.Add = nil, nil
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("empty change err = %v", err)
	}
	// 移除非成员。
	in = base()
	in.Remove = []string{"zzz"}
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("remove outsider err = %v", err)
	}
	// 加入与保留成员同名的人。
	in = base()
	in.Remove, in.Add = nil, []string{"b"}
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("add retained member err = %v", err)
	}
	// 名单含空 ID。
	in = base()
	in.Add = []string{""}
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("empty add id err = %v", err)
	}
	// 生效后人数低于门限。
	in = base()
	in.Remove, in.Add = []string{"b", "c"}, nil
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("below threshold err = %v", err)
	}

	// 其他参与者数量少于门限（提案人不能投票）：提案结构上不可能生效。
	sTight := NewService(NewMemRepository(), clk.now)
	mustCreate(t, sTight, "tight", []string{"a", "b"}, 2, clk.now().Add(time.Hour))
	if _, err := sTight.ProposeReplacement(context.Background(), ProposeReplacementInput{
		CeremonyID: "tight", RequestID: "rp", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: future,
	}); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("unreachable quorum err = %v", err)
	}

	// 合法提案：pending，所需批准数 = 门限。
	res := propose(t, s, base())
	if res.Status != ProposalPending || res.RoundNumber != 1 ||
		res.RequiredApprovals != 3 || res.ApprovalCount != 0 || res.Replayed {
		t.Fatalf("proposal = %+v", res)
	}

	// 终态后不能提案。
	if err := s.Cancel(context.Background(), "c", "n/a"); err != nil {
		t.Fatal(err)
	}
	in = base()
	in.RequestID = "req2"
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("propose after cancel err = %v", err)
	}
}

func TestProposeReplacementIdempotency(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))

	in := ProposeReplacementInput{
		CeremonyID: "c", RequestID: "prop-1", ProposedBy: "a",
		Remove: []string{"b"}, Add: []string{"d"}, NewDeadline: clk.now().Add(3 * time.Hour),
	}
	first := propose(t, s, in)

	// 同号同内容：重放，不重复建提案。
	replay := propose(t, s, in)
	if !replay.Replayed || replay.ProposalID != first.ProposalID {
		t.Fatalf("replay = %+v", replay)
	}
	got, _ := s.GetCeremony(context.Background(), "c")
	if len(got.ProposalOrder) != 1 {
		t.Fatalf("proposals = %d, want 1", len(got.ProposalOrder))
	}

	// 同号不同内容 => 冲突。
	in.Add = []string{"e"}
	if _, err := s.ProposeReplacement(context.Background(), in); !errIs(err, ErrConflict) {
		t.Fatalf("proposal conflict err = %v", err)
	}
}

// ---- 批准资格 ----

func TestApproveReplacementEligibility(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	// 5 成员、门限 3：a 提案移除 b；合格批准人只有 c,d,e（恰好 3 人）。
	mustCreate(t, s, "c", []string{"a", "b", "c", "d", "e"}, 3, clk.now().Add(2*time.Hour))
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
		Remove: []string{"b"}, Add: []string{"f"}, NewDeadline: clk.now().Add(4 * time.Hour),
	})

	mustFail := func(approver, reqID string, want error) {
		t.Helper()
		_, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
			CeremonyID: "c", RequestID: reqID, ProposalID: "p1", Approver: approver,
		})
		if !errIs(err, want) {
			t.Fatalf("approve by %s err = %v, want %v", approver, err, want)
		}
	}
	mustFail("a", "r-a", ErrCannotApprove) // 提案人本人
	mustFail("f", "r-f", ErrNotMember)     // 尚未加入的新成员
	mustFail("zzz", "r-z", ErrNotMember)   // 非成员

	// 被移除成员 b 在替换生效前仍是旧轮“其他参与者”，其批准有效。
	res := approve(t, s, "c", "r-b", "p1", "b")
	if res.ApprovalCount != 1 || res.Applied {
		t.Fatalf("removed member approval = %+v", res)
	}

	// 提案不存在。
	_, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: "c", RequestID: "r-x", ProposalID: "missing", Approver: "c",
	})
	if !errIs(err, ErrProposalNotFound) {
		t.Fatalf("missing proposal err = %v", err)
	}
	// 参数缺失。
	if _, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: "c", ProposalID: "p1", Approver: "c",
	}); !errIs(err, ErrInvalidArgument) {
		t.Fatalf("missing request id err = %v", err)
	}

	// c 批准成功（第 2 票）；c 再批准一次（换请求号）=> 已批准。
	res = approve(t, s, "c", "r-c", "p1", "c")
	if res.ApprovalCount != 2 || res.Applied {
		t.Fatalf("second approval = %+v", res)
	}
	_, err = s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: "c", RequestID: "r-c2", ProposalID: "p1", Approver: "c",
	})
	if !errIs(err, ErrAlreadyApproved) {
		t.Fatalf("duplicate approval err = %v", err)
	}

	// 批准幂等：同请求号重放不重复计数（返回首次写入时的计数 2）。
	replay := approve(t, s, "c", "r-c", "p1", "c")
	if !replay.Replayed || replay.ApprovalCount != 2 {
		t.Fatalf("approval replay = %+v", replay)
	}
	// 同请求号换批准人 => 冲突。
	if _, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: "c", RequestID: "r-c", ProposalID: "p1", Approver: "d",
	}); !errIs(err, ErrConflict) {
		t.Fatalf("approver swap err = %v", err)
	}
}

// ---- 核心：门限批准后开启新轮、冻结成员与截止时间、旧贡献失效 ----

func TestApprovalQuorumOpensNewRound(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c", "d", "e"}, 3, clk.now().Add(2*time.Hour))

			// 旧轮有 2 个贡献。
			contrib(t, s, "c", "cv-a", 1, cv("a", 1))
			contrib(t, s, "c", "cv-b", 1, cv("b", 2))

			newDeadline := clk.now().Add(5 * time.Hour)
			p := propose(t, s, ProposeReplacementInput{
				CeremonyID: "c", RequestID: "prop-1", ProposedBy: "a",
				Remove: []string{"b"}, Add: []string{"f"},
				NewDeadline: newDeadline, Reason: "b unavailable",
			})
			if p.ProposalID != "prop-1" || p.RequiredApprovals != 3 {
				t.Fatalf("proposal = %+v", p)
			}

			// 前两票不生效。
			if r := approve(t, s, "c", "ap-c", "prop-1", "c"); r.Applied || r.ApprovalCount != 1 {
				t.Fatalf("vote 1 = %+v", r)
			}
			if r := approve(t, s, "c", "ap-d", "prop-1", "d"); r.Applied || r.ApprovalCount != 2 {
				t.Fatalf("vote 2 = %+v", r)
			}

			// 第三票达到门限：同一调用返回生效结果。
			r3, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
				CeremonyID: "c", RequestID: "ap-e", ProposalID: "prop-1", Approver: "e",
			})
			if err != nil {
				t.Fatalf("vote 3: %v", err)
			}
			if !r3.Applied || r3.NewRoundNumber != 2 || r3.Status != ProposalApplied {
				t.Fatalf("vote 3 = %+v", r3)
			}

			got, _ := s.GetCeremony(context.Background(), "c")
			if len(got.Rounds) != 2 || got.Status != StatusActive {
				t.Fatalf("after apply: rounds=%d status=%s", len(got.Rounds), got.Status)
			}
			cur := got.CurrentRound()
			if !equalStrings(cur.Members, []string{"a", "c", "d", "e", "f"}) {
				t.Fatalf("new round members = %v", cur.Members)
			}
			if !cur.Deadline.Equal(newDeadline) {
				t.Fatalf("new round deadline = %v, want %v", cur.Deadline, newDeadline)
			}
			if cur.Threshold != 3 || len(cur.Contributions) != 0 {
				t.Fatalf("new round must start empty with frozen threshold: %+v", cur)
			}

			// 旧轮保留可查（2 个贡献）；提案快照了失效贡献，按旧轮成员有序。
			old := got.Rounds[0]
			if len(old.Contributions) != 2 {
				t.Fatalf("old round contributions = %d (kept for audit)", len(old.Contributions))
			}
			pp := got.Proposals["prop-1"]
			if pp.Status != ProposalApplied || pp.NewRoundNumber != 2 || pp.RoundNumber != 1 {
				t.Fatalf("proposal linkage = %+v", pp)
			}
			if ids := invalidatedParticipants(pp.InvalidatedContributions); !equalStrings(ids, []string{"a", "b"}) {
				t.Fatalf("invalidated participants = %v, want [a b]", ids)
			}
			for _, ic := range pp.InvalidatedContributions {
				if len(ic.ShardDigest) == 0 || len(ic.Commitment) == 0 || ic.InvalidatedAt.IsZero() {
					t.Fatalf("invalidated snapshot incomplete: %+v", ic)
				}
			}
			var approvers []string
			for _, m := range []string{"a", "b", "c", "d", "e"} {
				if _, ok := pp.Approvals[m]; ok {
					approvers = append(approvers, m)
				}
			}
			if !equalStrings(approvers, []string{"c", "d", "e"}) {
				t.Fatalf("approvers = %v, want [c d e]", approvers)
			}

			// 生效后不能再批准该提案。
			if _, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
				CeremonyID: "c", RequestID: "ap-late", ProposalID: "prop-1", Approver: "f",
			}); !errIs(err, ErrProposalNotPending) {
				t.Fatalf("approve applied proposal err = %v", err)
			}

			// 批准请求重放：返回生效结果，不重复计数。
			rep := approve(t, s, "c", "ap-e", "prop-1", "e")
			if !rep.Replayed || !rep.Applied || rep.NewRoundNumber != 2 || rep.ApprovalCount != 3 {
				t.Fatalf("approve replay = %+v", rep)
			}
		})
	}
}

// ---- 新轮必须重新收集门限贡献；旧轮贡献不折算、迟到贡献不带入 ----

func TestNewRoundRequiresFreshContributions(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))

			// 旧轮已达门限。
			contrib(t, s, "c", "cv-a", 1, cv("a", 1))
			contrib(t, s, "c", "cv-b", 1, cv("b", 2))
			propose(t, s, ProposeReplacementInput{
				CeremonyID: "c", RequestID: "p", ProposedBy: "a",
				Remove: []string{"b"}, Add: []string{"d"},
				NewDeadline: clk.now().Add(3 * time.Hour),
			})
			// a 是提案人不能批准；被移除的 b 与 c 作为旧轮其他参与者各投一票，达到门限 2。
			r1 := approve(t, s, "c", "ap-b", "p", "b")
			r2 := approve(t, s, "c", "ap-c", "p", "c")
			if !r2.Applied || r1.ApprovalCount != 1 {
				t.Fatalf("replacement votes = %+v / %+v", r1, r2)
			}

			// 新轮 0 贡献：即使旧轮曾达到门限也不能完成，旧轮贡献不折算复用。
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "done-early", KeyID: "k",
			}); !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("complete new round without contributions err = %v", err)
			}

			// 旧轮迟到贡献不得带入新轮。
			if _, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "cv-c-late", RoundNumber: 1, Contribution: cv("c", 3),
			}); !errIs(err, ErrStaleRound) {
				t.Fatalf("late contribution to old round err = %v", err)
			}
			// 被移除的 b 在新轮既不是成员，也不能用“旧轮已贡献”为由参与新轮。
			if _, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "cv-b-r2", RoundNumber: 2, Contribution: cv("b", 3),
			}); !errIs(err, ErrNotMember) {
				t.Fatalf("removed member contributes to new round err = %v", err)
			}

			// 新轮重新收集 2 个有效贡献后才能完成；outbox 采用第 2 轮数据。
			contrib(t, s, "c", "cv-a2", 2, cv("a", 4))
			contrib(t, s, "c", "cv-d2", 2, cv("d", 5))
			ob, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "done", KeyID: "key-1", Payload: []byte("agg"),
			})
			if err != nil {
				t.Fatalf("complete round 2: %v", err)
			}
			if ob.RoundNumber != 2 {
				t.Fatalf("outbox round = %d, want 2 (old round contributions never carry over)", ob.RoundNumber)
			}
			if got := participantIDs(ob.Contributions); !equalStrings(got, []string{"a", "d"}) {
				t.Fatalf("adopted = %v, want [a d]", got)
			}

			// 提案保留了新旧轮次与失效贡献的关联。
			p, err := s.GetProposal(context.Background(), "c", "p")
			if err != nil {
				t.Fatal(err)
			}
			if p.RoundNumber != 1 || p.NewRoundNumber != 2 ||
				len(p.InvalidatedContributions) != 2 || len(p.Approvals) != 2 {
				t.Fatalf("proposal linkage = %+v", p)
			}
		})
	}
}

// ---- 轮次级截止时间：每轮独立冻结 ----

func TestPerRoundDeadline(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))

			contrib(t, s, "c", "cv-a", 1, cv("a", 1))
			// 在旧轮 deadline 内提案：纯增员，合格批准人为 b,c，门限 2，两票生效。
			newDeadline := clk.now().Add(3 * time.Hour)
			propose(t, s, ProposeReplacementInput{
				CeremonyID: "c", RequestID: "p", ProposedBy: "a",
				Add: []string{"d"}, NewDeadline: newDeadline,
			})
			approve(t, s, "c", "ap-b", "p", "b")
			applied := approve(t, s, "c", "ap-c", "p", "c")
			if !applied.Applied {
				t.Fatalf("replacement not applied: %+v", applied)
			}

			// 时间越过旧轮 deadline，但仍在新轮 deadline 内：
			// 新轮贡献必须照常接收，证明旧轮截止时间不再约束新轮。
			clk.advance(2 * time.Hour)
			res := contrib(t, s, "c", "cv-b2", 2, cv("b", 3))
			if res.RoundNumber != 2 || res.ContributionCount != 1 {
				t.Fatalf("new round contribution after old deadline = %+v", res)
			}

			// 越过新轮 deadline：新轮惰性过期。
			clk.advance(2 * time.Hour)
			_, err := s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "cv-c2", RoundNumber: 2, Contribution: cv("c", 4),
			})
			if !errIs(err, ErrDeadlineExceeded) {
				t.Fatalf("after new deadline err = %v", err)
			}
			got, _ := s.GetCeremony(context.Background(), "c")
			if got.Status != StatusExpired || got.CurrentRound().Number != 2 {
				t.Fatalf("state = %s round %d", got.Status, got.CurrentRound().Number)
			}
		})
	}
}

// ---- 终态互斥：替换生效 / 完成 / 超时 / 取消 并发时只有一个终态 ----

func TestReplaceCompleteTimeoutSingleTerminal(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			const iterations = 30
			for iter := 0; iter < iterations; iter++ {
				clk := newFakeClock()
				repo := f.new(t)
				s := NewService(repo, clk.now)
				id := fmt.Sprintf("c-%d", iter)
				// 5 成员、门限 3。
				mustCreate(t, s, id, []string{"a", "b", "c", "d", "e"}, 3, clk.now().Add(24*time.Hour))

				var wg sync.WaitGroup
				barrier := make(chan struct{})

				// 5 个成员向旧轮贡献。
				for _, m := range []string{"a", "b", "c", "d", "e"} {
					wg.Add(1)
					go func(m string) {
						defer wg.Done()
						<-barrier
						_, _ = s.SubmitContribution(context.Background(), ContributeInput{
							CeremonyID: id, RequestID: fmt.Sprintf("cv-%s-%d", m, iter),
							RoundNumber: 1, Contribution: cv(m, byte(iter)),
						})
					}(m)
				}

				// 提案 p1（a 发起，移除 b），c/d/e 各尝试批准，凑齐 3 票即生效。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.ProposeReplacement(context.Background(), ProposeReplacementInput{
						CeremonyID: id, RequestID: fmt.Sprintf("p1-%d", iter), ProposedBy: "a",
						Remove: []string{"b"}, Add: []string{"f"},
						NewDeadline: clk.now().Add(48 * time.Hour),
					})
					for _, m := range []string{"c", "d", "e"} {
						wg.Add(1)
						go func(m string) {
							defer wg.Done()
							<-barrier
							_, _ = s.ApproveReplacement(context.Background(), ApproveReplacementInput{
								CeremonyID: id, RequestID: fmt.Sprintf("p1-%s-%d", m, iter),
								ProposalID: fmt.Sprintf("p1-%d", iter), Approver: m,
							})
						}(m)
					}
				}()

				// 完成者：若旧轮先凑齐 3 贡献则可能成功；若替换先生效则新轮为空必然失败。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_, _ = s.Complete(context.Background(), CompleteInput{
						CeremonyID: id, RequestID: fmt.Sprintf("done-%d", iter), KeyID: "key-1",
					})
				}()
				// 取消者。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					_ = s.Cancel(context.Background(), id, "race")
				}()
				// 超时者：把时钟推过当前轮 deadline（新旧轮 deadline 都在 24h 后以上，
				// 推进 100h 对任何当前轮都构成超时）。
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-barrier
					clk.advance(100 * time.Hour)
					_, _ = s.GetCeremony(context.Background(), id) // 触发惰性过期
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

	// 1. 轮次号从 1 严格递增，不跳号；每轮门限/成员/贡献自洽。
	for i, r := range c.Rounds {
		if r.Number != i+1 {
			t.Fatalf("round numbers not contiguous: %+v", c.Rounds)
		}
		if r.Threshold != c.Threshold {
			t.Fatalf("round threshold drifted: %d vs %d", r.Threshold, c.Threshold)
		}
		if r.Deadline.IsZero() {
			t.Fatalf("round %d deadline not frozen", r.Number)
		}
		if len(r.Contributions) > len(r.Members) {
			t.Fatalf("round %d has %d contributions for %d members", r.Number, len(r.Contributions), len(r.Members))
		}
		for p := range r.Contributions {
			if !isMember(r.Members, p) {
				t.Fatalf("round %d contribution by non-member %s", r.Number, p)
			}
		}
	}

	// 2. 提案状态机：至多一个 applied；终态仪式不得残留 pending；
	//    applied 提案的新轮号必须真实存在，失效贡献与旧轮贡献集合一致。
	var applied []*ReplacementProposal
	for _, pid := range c.ProposalOrder {
		p := c.Proposals[pid]
		switch p.Status {
		case ProposalPending:
			if c.Status.IsTerminal() {
				t.Fatalf("terminal ceremony still has pending proposal %s", pid)
			}
		case ProposalApplied:
			applied = append(applied, p)
			if p.NewRoundNumber > len(c.Rounds) {
				t.Fatalf("applied proposal %s points to missing round %d", pid, p.NewRoundNumber)
			}
			old := c.Rounds[p.RoundNumber-1]
			if len(p.InvalidatedContributions) != len(old.Contributions) {
				t.Fatalf("proposal %s invalidated %d, old round has %d contributions",
					pid, len(p.InvalidatedContributions), len(old.Contributions))
			}
			if len(p.Approvals) < p.RequiredApprovals {
				t.Fatalf("applied proposal %s with only %d/%d approvals",
					pid, len(p.Approvals), p.RequiredApprovals)
			}
		case ProposalSuperseded, ProposalAbandoned:
			if p.NewRoundNumber != 0 || len(p.InvalidatedContributions) != 0 {
				t.Fatalf("non-applied proposal %s carries round/invalidated linkage: %+v", pid, p)
			}
		default:
			t.Fatalf("illegal proposal status %q", p.Status)
		}
	}
	if len(applied) > 1 {
		t.Fatalf("%d proposals applied, want at most 1", len(applied))
	}

	// 3. 通知：seq 从 1 连续、ID 唯一；通知与状态同事务（计数一致）。
	seenIDs := map[string]struct{}{}
	for i, n := range c.Notifications {
		if n.Seq != int64(i+1) {
			t.Fatalf("notification seq gap at %d: %d", i, n.Seq)
		}
		if n.ID != notificationID(n.Seq) {
			t.Fatalf("notification id %s != %s", n.ID, notificationID(n.Seq))
		}
		if _, dup := seenIDs[n.ID]; dup {
			t.Fatalf("duplicate notification id %s", n.ID)
		}
		seenIDs[n.ID] = struct{}{}
		if len(n.Recipients) == 0 {
			t.Fatalf("notification %s has no recipients", n.ID)
		}
	}
	if c.NotificationSeq != int64(len(c.Notifications)) {
		t.Fatalf("notification seq %d != stored %d", c.NotificationSeq, len(c.Notifications))
	}

	switch c.Status {
	case StatusActive:
		if c.Outbox != nil {
			t.Fatal("active ceremony must not have outbox")
		}
	case StatusCompleted:
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

// ---- 完成 / 取消 / 超时：挂起提案一律 abandoned，终态唯一 ----

func TestPendingProposalsAbandonedOnTerminal(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		clk := newFakeClock()
		s := NewService(NewMemRepository(), clk.now)
		mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))
		contrib(t, s, "c", "cv-a", 1, cv("a", 1))
		propose(t, s, ProposeReplacementInput{
			CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
			Add: []string{"d"}, NewDeadline: clk.now().Add(3 * time.Hour),
		})
		contrib(t, s, "c", "cv-b", 1, cv("b", 2))
		if _, err := s.Complete(context.Background(), CompleteInput{
			CeremonyID: "c", RequestID: "done", KeyID: "k",
		}); err != nil {
			t.Fatal(err)
		}
		p, err := s.GetProposal(context.Background(), "c", "p1")
		if err != nil {
			t.Fatal(err)
		}
		if p.Status != ProposalAbandoned {
			t.Fatalf("proposal status = %s, want abandoned", p.Status)
		}
		// 终态后批准被拒。
		if _, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
			CeremonyID: "c", RequestID: "ap-late", ProposalID: "p1", Approver: "c",
		}); !errIs(err, ErrCeremonyTerminal) {
			t.Fatalf("approve after complete err = %v", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		for _, f := range fixtures(t) {
			t.Run(f.name, func(t *testing.T) {
				clk := newFakeClock()
				s := NewService(f.new(t), clk.now)
				mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
				propose(t, s, ProposeReplacementInput{
					CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
					Add: []string{"d"}, NewDeadline: clk.now().Add(3 * time.Hour),
				})
				clk.advance(2 * time.Hour)
				got, _ := s.GetCeremony(context.Background(), "c")
				if got.Status != StatusExpired {
					t.Fatalf("status = %s", got.Status)
				}
				p := got.Proposals["p1"]
				if p.Status != ProposalAbandoned {
					t.Fatalf("proposal = %s, want abandoned", p.Status)
				}
				// 过期通知 + 提案废弃通知与状态同一事务落库。
				notes, err := s.ListNotifications(context.Background(), "c")
				if err != nil {
					t.Fatal(err)
				}
				var kinds []string
				for _, n := range notes {
					kinds = append(kinds, n.Kind)
				}
				if !contains(kinds, AuditExpired) || !contains(kinds, NotifyReplacementAbandoned) {
					t.Fatalf("notifications = %v, want expired + abandoned", kinds)
				}
			})
		}
	})
}

// ---- 多提案竞争：先生效者让其余 pending 提案 superseded ----

func TestCompetingProposalsSuperseded(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))

	// 两个纯增员提案（提案人 a），合格批准人都是 b,c；门限 2。
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: clk.now().Add(3 * time.Hour),
	})
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p2", ProposedBy: "a",
		Add: []string{"e"}, NewDeadline: clk.now().Add(4 * time.Hour),
	})

	// p1 先凑齐 b,c 两票生效。
	approve(t, s, "c", "p1-b", "p1", "b")
	applied := approve(t, s, "c", "p1-c", "p1", "c")
	if !applied.Applied || applied.NewRoundNumber != 2 {
		t.Fatalf("p1 = %+v", applied)
	}

	// p2 自动 superseded；对它的后续批准一律拒绝。
	p2, err := s.GetProposal(context.Background(), "c", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if p2.Status != ProposalSuperseded {
		t.Fatalf("p2 = %s, want superseded", p2.Status)
	}
	if _, err := s.ApproveReplacement(context.Background(), ApproveReplacementInput{
		CeremonyID: "c", RequestID: "p2-b", ProposalID: "p2", Approver: "b",
	}); !errIs(err, ErrProposalNotPending) {
		t.Fatalf("approve superseded err = %v", err)
	}

	// 新一轮开启后仍可提出新提案（针对第 2 轮），旧提案保持 superseded 可查。
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p3", ProposedBy: "a",
		Add: []string{"f"}, NewDeadline: clk.now().Add(5 * time.Hour),
	})
	all, err := s.ListProposals(context.Background(), "c", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].ID != "p1" || all[1].ID != "p2" || all[2].ID != "p3" {
		t.Fatalf("proposal order = %v", proposalIDs(all))
	}
	pending, err := s.ListProposals(context.Background(), "c", ProposalPending)
	if err != nil || len(pending) != 1 || pending[0].ID != "p3" {
		t.Fatalf("pending = %v err=%v", proposalIDs(pending), err)
	}
}

// ---- 旧轮贡献幂等重放跨越替换与终态 ----

func TestContributionReplaySurvivesReplacementAndTerminal(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(24*time.Hour))

	first := contrib(t, s, "c", "req-a", 1, cv("a", 1))
	if first.ContributionCount != 1 {
		t.Fatalf("first = %+v", first)
	}

	// 审批通过开启新一轮（纯增员 d；b,c 批准）。
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "rep", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: clk.now().Add(48 * time.Hour),
	})
	approve(t, s, "c", "rep-b", "rep", "b")
	approve(t, s, "c", "rep-c", "rep", "c")

	// 旧轮已 stale，但同请求号重试返回首次结果，而不是 ErrStaleRound。
	replayAfterReplace := contrib(t, s, "c", "req-a", 1, cv("a", 1))
	if !replayAfterReplace.Replayed || replayAfterReplace.RoundNumber != 1 ||
		replayAfterReplace.ContributionCount != 1 {
		t.Fatalf("replay after replace = %+v", replayAfterReplace)
	}
	got, _ := s.GetCeremony(context.Background(), "c")
	if len(got.CurrentRound().Contributions) != 0 {
		t.Fatalf("replay leaked into new round: %d", len(got.CurrentRound().Contributions))
	}

	// 新轮凑齐门限并完成后，旧请求号重放仍返回首次结果。
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
}

// ---- 完成：门限、并发唯一、outbox 冻结 ----

func TestCompleteThresholdAndFrozenOutbox(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
	contrib(t, s, "c", "req-a", 1, cv("a", 1))

	// 门限未达不能完成。
	if _, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "comp", KeyID: "key-1", Payload: []byte("agg"),
	}); !errIs(err, ErrThresholdNotReached) {
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
	if got := participantIDs(outbox.Contributions); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("adopted = %v", got)
	}

	// 完成后到达的额外贡献不能改变结果。
	if _, err := s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "req-c-late", RoundNumber: 1, Contribution: cv("c", 3),
	}); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("late contribution err = %v", err)
	}
	// 第二次完成（不同请求号）拒绝；同请求号同内容重放返回同一 outbox。
	if _, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "comp-other", KeyID: "key-1",
	}); !errIs(err, ErrCeremonyTerminal) {
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
	// 取消与提案在完成后同样被拒。
	if err := s.Cancel(context.Background(), "c", "x"); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("cancel after complete err = %v", err)
	}
	if _, err := s.ProposeReplacement(context.Background(), ProposeReplacementInput{
		CeremonyID: "c", RequestID: "rep", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: clk.now().Add(time.Hour),
	}); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("propose after complete err = %v", err)
	}
}

func TestConcurrentCompleteSingleWinner(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
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
			if _, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: reqID, KeyID: "key-1",
			}); err == nil {
				winners <- reqID
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
}

// ---- 取消 / 过期 ----

func TestCancel(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	contrib(t, s, "c", "req-a", 1, cv("a", 1))

	if err := s.Cancel(context.Background(), "c", "abort"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got, _ := s.GetCeremony(context.Background(), "c")
	if got.Status != StatusCanceled || got.CancelReason != "abort" || got.Outbox != nil {
		t.Fatalf("after cancel: %+v", got)
	}
	if _, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "x", KeyID: "k",
	}); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("complete after cancel err = %v", err)
	}
	if err := s.Cancel(context.Background(), "c", "again"); !errIs(err, ErrCeremonyTerminal) {
		t.Fatalf("double cancel err = %v", err)
	}
}

func TestExpiryLazy(t *testing.T) {
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

// ---- 通知：与状态同事务、内容正确 ----

func TestReplacementNotifications(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))
	contrib(t, s, "c", "cv-a", 1, cv("a", 1))

	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
		Remove: []string{"b"}, Add: []string{"d"},
		NewDeadline: clk.now().Add(3 * time.Hour), Reason: "b out",
	})
	approve(t, s, "c", "ap-c", "p1", "c")

	// 生效前：proposed（收件人为其他成员 b,c）+ approved（收件人为提案人 a）。
	notes, _ := s.ListNotifications(context.Background(), "c")
	var kinds []string
	for _, n := range notes {
		kinds = append(kinds, n.Kind)
	}
	if !equalStrings(kinds, []string{
		NotifyReplacementProposed, NotifyReplacementApproved,
	}) {
		t.Fatalf("notifications before apply = %v", kinds)
	}
	if !equalStrings(notes[0].Recipients, []string{"b", "c"}) {
		t.Fatalf("proposed recipients = %v", notes[0].Recipients)
	}
	if notes[0].ProposalID != "p1" {
		t.Fatalf("proposed note proposal = %s", notes[0].ProposalID)
	}

	// p1 仅收到 c 一票未生效（门限 2）；生效批通知在下面的独立仪式验证。
	s2 := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s2, "c2", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))
	mustContrib := func(id, who string, n byte) {
		if _, err := s2.SubmitContribution(context.Background(), ContributeInput{
			CeremonyID: "c2", RequestID: id, RoundNumber: 1, Contribution: cv(who, n),
		}); err != nil {
			t.Fatal(err)
		}
	}
	mustContrib("cv-a2", "a", 1)
	mustContrib("cv-b2", "b", 2)
	propose(t, s2, ProposeReplacementInput{
		CeremonyID: "c2", RequestID: "p2", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: clk.now().Add(3 * time.Hour),
	})
	approve(t, s2, "c2", "ap-b2", "p2", "b")
	approve(t, s2, "c2", "ap-c2", "p2", "c")

	notes2, _ := s2.ListNotifications(context.Background(), "c2")
	var kinds2 []string
	var applied, invalidated *Notification
	for i := range notes2 {
		kinds2 = append(kinds2, notes2[i].Kind)
		switch notes2[i].Kind {
		case NotifyReplacementApplied:
			applied = &notes2[i]
		case NotifyContributionsInvalidated:
			invalidated = &notes2[i]
		}
	}
	wantKinds := []string{
		NotifyReplacementProposed,
		NotifyReplacementApproved,
		NotifyReplacementApproved,
		NotifyReplacementApplied,
		NotifyContributionsInvalidated,
	}
	if !equalStrings(kinds2, wantKinds) {
		t.Fatalf("apply notifications = %v, want %v", kinds2, wantKinds)
	}
	if applied == nil || applied.RoundNumber != 2 {
		t.Fatalf("applied note = %+v", applied)
	}
	if invalidated == nil || !equalStrings(invalidated.Recipients, []string{"a", "b"}) {
		t.Fatalf("invalidated note = %+v", invalidated)
	}
}

// ---- 审计 ----

func TestAuditTrail(t *testing.T) {
	clk := newFakeClock()
	repo := NewMemRepository()
	s := NewService(repo, clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(3*time.Hour))
	contrib(t, s, "c", "req-a", 1, cv("a", 1))
	_, _ = s.SubmitContribution(context.Background(), ContributeInput{
		CeremonyID: "c", RequestID: "req-x", RoundNumber: 1, Contribution: cv("zzz", 1),
	}) // 非成员，被拒但应留痕

	// 一个完整的审批生效周期。
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
		Add: []string{"d"}, NewDeadline: clk.now().Add(2 * time.Hour),
	})
	approve(t, s, "c", "ap-b", "p1", "b")
	approve(t, s, "c", "ap-c", "p1", "c")
	contrib(t, s, "c", "req-b2", 2, cv("b", 9))
	contrib(t, s, "c", "req-c2", 2, cv("c", 9))
	if _, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "done", KeyID: "k",
	}); err != nil {
		t.Fatal(err)
	}

	events, err := s.Audit(context.Background(), "c", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap: event %d has seq %d", i, e.Seq)
		}
	}
	kinds := make([]string, len(events))
	for i, e := range events {
		kinds[i] = e.Kind
	}
	wantContains := []string{
		AuditCreated, AuditContributed, AuditRejected,
		AuditProposalCreated, AuditProposalApproved, AuditProposalApplied,
		AuditCompleted,
	}
	for _, want := range wantContains {
		if !contains(kinds, want) {
			t.Fatalf("audit kinds %v missing %q", kinds, want)
		}
	}

	// applied 事件关联新旧轮次、批准人与失效贡献。
	var applied *AuditEvent
	for i := range events {
		if events[i].Kind == AuditProposalApplied {
			applied = &events[i]
		}
	}
	if applied.Detail["previous_round"] != 1 || applied.Detail["new_round"] != 2 {
		t.Fatalf("applied linkage = %+v", applied.Detail)
	}
	if applied.Detail["invalidated_contributions"] != 1 {
		t.Fatalf("applied invalidated count = %v", applied.Detail["invalidated_contributions"])
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

// ---- 文件仓储：持久化恢复、outbox 唯一落盘、通知随状态恢复 ----

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
		// 走审批开启第 2 轮，再在第 2 轮完成。
		propose(t, s, ProposeReplacementInput{
			CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
			Add: []string{"d"}, NewDeadline: clk.now().Add(2 * time.Hour),
		})
		approve(t, s, "c", "ap-b", "p1", "b")
		approve(t, s, "c", "ap-c", "p1", "c")
		contrib(t, s, "c", "req-a2", 2, cv("a", 2))
		contrib(t, s, "c", "req-b2", 2, cv("b", 2))
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

	// 重新打开：轮次、提案关联、通知、outbox、审计全部恢复。
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
	if got.Outbox.KeyID != "key-1" || got.Outbox.RoundNumber != 2 || string(got.Outbox.Payload) != "agg" {
		t.Fatalf("recovered outbox = %+v", got.Outbox)
	}
	if len(got.Rounds) != 2 || got.Rounds[1].Deadline.IsZero() {
		t.Fatalf("recovered rounds = %d", len(got.Rounds))
	}
	p := got.Proposals["p1"]
	if p == nil || p.Status != ProposalApplied || p.NewRoundNumber != 2 ||
		len(p.InvalidatedContributions) != 1 || len(p.Approvals) != 2 {
		t.Fatalf("recovered proposal = %+v", p)
	}
	if len(got.Notifications) == 0 || got.NotificationSeq != int64(len(got.Notifications)) {
		t.Fatalf("recovered notifications = %d seq=%d", len(got.Notifications), got.NotificationSeq)
	}
	ob, err := repo2.ReadOutboxFile("c")
	if err != nil {
		t.Fatal(err)
	}
	if ob.RoundNumber != 2 || len(ob.Contributions) != 2 {
		t.Fatalf("outbox file = %+v", ob)
	}
	events, err := repo2.Audit(context.Background(), "c", 0, 0)
	if err != nil || len(events) < 6 {
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
	if _, err := NewService(repo, nil).GetProposal(context.Background(), "missing", "p1"); !errIs(err, ErrNotFound) {
		t.Fatalf("get proposal err = %v", err)
	}
}

// ---- 关联查询：新旧轮次、批准人、失效贡献 ----

func TestProposalLinkageQuery(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(2*time.Hour))
	contrib(t, s, "c", "cv-a", 1, cv("a", 1))
	contrib(t, s, "c", "cv-b", 1, cv("b", 2))
	propose(t, s, ProposeReplacementInput{
		CeremonyID: "c", RequestID: "p1", ProposedBy: "a",
		Remove: []string{"c"}, Add: []string{"d"},
		NewDeadline: clk.now().Add(3 * time.Hour),
	})

	// 提案快照：旧轮 1、新轮未定、名单与所需批准数。
	got, err := s.GetProposal(context.Background(), "c", "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.RoundNumber != 1 || got.NewRoundNumber != 0 || !equalStrings(got.NewMembers, []string{"a", "b", "d"}) {
		t.Fatalf("pending proposal = %+v", got)
	}
	if _, err := s.GetProposal(context.Background(), "c", "nope"); !errIs(err, ErrProposalNotFound) {
		t.Fatalf("missing proposal err = %v", err)
	}

	approve(t, s, "c", "ap-b", "p1", "b")
	approve(t, s, "c", "ap-c", "p1", "c") // c 虽被移除，但属于旧轮“其他参与者”，对旧轮提案有批准资格

	applied, err := s.GetProposal(context.Background(), "c", "p1")
	if err != nil {
		t.Fatal(err)
	}
	// 生效后一条记录串起全部关联：
	if applied.Status != ProposalApplied ||
		applied.RoundNumber != 1 || applied.NewRoundNumber != 2 ||
		len(applied.Approvals) != 2 || len(applied.InvalidatedContributions) != 2 {
		t.Fatalf("applied linkage = %+v", applied)
	}
	if ids := invalidatedParticipants(applied.InvalidatedContributions); !equalStrings(ids, []string{"a", "b"}) {
		t.Fatalf("invalidated = %v", ids)
	}
}

// ---- 辅助 ----

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func proposalIDs(ps []*ReplacementProposal) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.ID
	}
	return out
}

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
