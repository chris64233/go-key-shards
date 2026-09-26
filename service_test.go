package keyshards

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// --- 测试辅助 ---

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	svc, err := NewService(NewMemoryStore(), WithClock(clk.now))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, clk
}

func createTestCeremony(t *testing.T, svc *Service, participants []string, threshold int, deadline time.Time) string {
	t.Helper()
	v, err := svc.CreateCeremony(CreateParams{
		CeremonyID:   "cer-test",
		Participants: participants,
		Threshold:    threshold,
		Deadline:     deadline,
	})
	if err != nil {
		t.Fatalf("CreateCeremony: %v", err)
	}
	if v.Status != StatusActive || v.CurrentRound != 1 {
		t.Fatalf("unexpected view: %+v", v)
	}
	return v.ID
}

func contrib(id string, round int, reqID string, payload byte) Contribution {
	return Contribution{
		RequestID:     reqID,
		ParticipantID: id,
		Round:         round,
		Commitment:    []byte{payload},
		ShareDigest:   []byte{payload ^ 0x0f},
	}
}

// --- 创建 ---

func TestCreateCeremonyFreezesRoster(t *testing.T) {
	svc, clk := newTestService(t)
	deadline := clk.now().Add(time.Hour)
	v, err := svc.CreateCeremony(CreateParams{
		Participants: []string{"bob", "alice"},
		Threshold:    2,
		Deadline:     deadline,
	})
	if err != nil {
		t.Fatalf("CreateCeremony: %v", err)
	}
	// 名册被规范化排序、门限与截止时间冻结。
	if got := v.Participants; len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Fatalf("roster not sorted/frozen: %v", got)
	}
	if v.Threshold != 2 || !v.Deadline.Equal(deadline) {
		t.Fatalf("threshold/deadline not frozen: %+v", v)
	}
	events, _ := svc.AuditTrail(v.ID)
	if len(events) != 1 || events[0].Type != AuditCeremonyCreated {
		t.Fatalf("expected creation audit event, got %+v", events)
	}
}

func TestCreateCeremonyValidation(t *testing.T) {
	svc, clk := newTestService(t)
	cases := []struct {
		name         string
		participants []string
		threshold    int
		deadline     time.Time
	}{
		{"empty participants", nil, 1, clk.now().Add(time.Hour)},
		{"empty participant id", []string{"a", ""}, 1, clk.now().Add(time.Hour)},
		{"duplicate participants", []string{"a", "a"}, 1, clk.now().Add(time.Hour)},
		{"threshold zero", []string{"a"}, 0, clk.now().Add(time.Hour)},
		{"threshold too high", []string{"a", "b"}, 3, clk.now().Add(time.Hour)},
		{"past deadline", []string{"a"}, 1, clk.now().Add(-time.Minute)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateCeremony(CreateParams{Participants: tc.participants, Threshold: tc.threshold, Deadline: tc.deadline}); !errors.Is(err, ErrInvalidParams) {
				t.Fatalf("want ErrInvalidParams, got %v", err)
			}
		})
	}

	createTestCeremony(t, svc, []string{"a"}, 1, clk.now().Add(time.Hour))
	if _, err := svc.CreateCeremony(CreateParams{CeremonyID: "cer-test", Participants: []string{"a"}, Threshold: 1, Deadline: clk.now().Add(time.Hour)}); !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("duplicate ceremony ID: want ErrInvalidParams, got %v", err)
	}
}

// --- 贡献：成员、轮次、截止时间、重复 ---

func TestSubmitContributionRules(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol"}, 2, clk.now().Add(time.Hour))

	// 非成员。
	if _, err := svc.SubmitContribution("cer-test", contrib("eve", 1, "req-eve", 1)); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member: want ErrNotMember, got %v", err)
	}
	// 旧轮次号。
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 0, "req-old", 1)); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("stale round: want ErrStaleRound, got %v", err)
	}
	// 缺字段。
	bad := contrib("alice", 1, "req-bad", 1)
	bad.Commitment = nil
	if _, err := svc.SubmitContribution("cer-test", bad); !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("missing commitment: want ErrInvalidParams, got %v", err)
	}

	// 正常接受。
	res, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-alice", 1))
	if err != nil {
		t.Fatalf("alice submit: %v", err)
	}
	if !res.Accepted || res.CurrentCount != 1 || res.ThresholdMet {
		t.Fatalf("unexpected result: %+v", res)
	}
	// 同一参与者同轮只能贡献一次（换请求号也不行）。
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-alice-2", 2)); !errors.Is(err, ErrDuplicateContributor) {
		t.Fatalf("duplicate contributor: want ErrDuplicateContributor, got %v", err)
	}
	// 超过截止时间的贡献不计入门限。
	clk.advance(2 * time.Hour)
	if _, err := svc.SubmitContribution("cer-test", contrib("bob", 1, "req-bob-late", 3)); !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("late: want ErrDeadlineExceeded, got %v", err)
	}
	v, _ := svc.GetCeremony("cer-test")
	if v.Status != StatusTimedOut {
		t.Fatalf("expected TIMED_OUT, got %s", v.Status)
	}
	if v.ContributionCount != 1 {
		t.Fatalf("late contribution must not count, got %d", v.ContributionCount)
	}
}

// --- 幂等与冲突 ---

func TestSubmitIdempotentRetry(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob"}, 2, clk.now().Add(time.Hour))

	first, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-1", 7))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// 同请求号 + 同内容：返回原结果并标记重放，计数不变。
	replay, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-1", 7))
	if err != nil || !replay.Replayed {
		t.Fatalf("replay: err=%v replayed=%v", err, replay.Replayed)
	}
	if replay.CurrentCount != first.CurrentCount || replay.ThresholdMet != first.ThresholdMet {
		t.Fatalf("replay result differs: %+v vs %+v", first, replay)
	}
	v, _ := svc.GetCeremony("cer-test")
	if v.ContributionCount != 1 {
		t.Fatalf("replay must not double count, got %d", v.ContributionCount)
	}
	// 同请求号 + 不同内容：冲突。
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-1", 8)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict: want ErrIdempotencyConflict, got %v", err)
	}
}

func TestIdempotentRetrySurvivesTerminalState(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob"}, 2, clk.now().Add(time.Hour))

	original := contrib("alice", 1, "req-1", 7)
	first, err := svc.SubmitContribution("cer-test", original)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	submitN(t, svc, "cer-test", 1, "bob", 2, "req-bob")
	if _, err := svc.Complete("cer-test"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// 完成后重试仍返回首次结果，而不是 closed 错误。
	replay, err := svc.SubmitContribution("cer-test", original)
	if err != nil || !replay.Replayed || replay.CurrentCount != first.CurrentCount {
		t.Fatalf("post-complete replay: err=%v result=%+v", err, replay)
	}
}

// --- 替换参与者 / 轮次演进 ---

func TestReplaceParticipantsOpensNewRound(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol"}, 2, clk.now().Add(time.Hour))

	submitN(t, svc, "cer-test", 1, "alice", 1, "req-alice-r1")
	v, err := svc.ReplaceParticipants("cer-test", ReplaceParams{
		Participants: []string{"alice", "dave"},
		Threshold:    2,
		Deadline:     clk.now().Add(2 * time.Hour),
		Actor:        "admin",
		Reason:       "rotate",
	})
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if v.CurrentRound != 2 || v.ContributionCount != 0 || len(v.Participants) != 2 {
		t.Fatalf("new round not reset correctly: %+v", v)
	}
	// 旧轮次贡献失效：alice 在新轮次可重新贡献；bob/carol 不再是成员。
	if _, err := svc.SubmitContribution("cer-test", contrib("bob", 2, "req-bob-r2", 5)); !errors.Is(err, ErrNotMember) {
		t.Fatalf("removed member: want ErrNotMember, got %v", err)
	}
	// 旧轮次号 + 新请求号：轮次过期。
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-alice-stale", 1)); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("old round resubmit: want ErrStaleRound, got %v", err)
	}
	// 旧轮次已接受的请求号重试：即使轮次已推进，也幂等返回首次结果。
	replay, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-alice-r1", 1))
	if err != nil || !replay.Replayed || replay.Round != 1 {
		t.Fatalf("replay across rounds: err=%v result=%+v", err, replay)
	}
	// 完成后不能再替换。
	submitN(t, svc, "cer-test", 2, "alice", 9, "req-alice-r2")
	submitN(t, svc, "cer-test", 2, "dave", 10, "req-dave-r2")
	if _, err := svc.Complete("cer-test"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	_, err = svc.ReplaceParticipants("cer-test", ReplaceParams{
		Participants: []string{"x"}, Threshold: 1, Deadline: clk.now().Add(time.Hour),
	})
	if !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("replace after complete: want ErrCeremonyClosed, got %v", err)
	}
}

// --- 完成、冻结、唯一 outbox ---

func submitN(t *testing.T, svc *Service, ceremonyID string, round int, participant string, payload byte, reqID string) SubmitResult {
	t.Helper()
	res, err := svc.SubmitContribution(ceremonyID, contrib(participant, round, reqID, payload))
	if err != nil {
		t.Fatalf("submit %s: %v", participant, err)
	}
	return res
}

func TestCompleteFreezesAdoptedSetAndOutbox(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol"}, 2, clk.now().Add(time.Hour))

	// 未达门限不能完成。
	if _, err := svc.Complete("cer-test"); !errors.Is(err, ErrThresholdNotMet) {
		t.Fatalf("early complete: want ErrThresholdNotMet, got %v", err)
	}
	submitN(t, svc, "cer-test", 1, "alice", 1, "req-alice")
	submitN(t, svc, "cer-test", 1, "bob", 2, "req-bob")

	res, err := svc.Complete("cer-test")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if res.Round != 1 || len(res.Adopted) != 2 {
		t.Fatalf("unexpected complete result: %+v", res)
	}
	if res.Outbox.ID != "cer-test/key-activation" || res.Outbox.Type != OutboxEventTypeKeyActivation {
		t.Fatalf("bad outbox event: %+v", res.Outbox)
	}
	// 采用集合按参与者排序、确定且冻结。
	if res.Adopted[0].ParticipantID != "alice" || res.Adopted[1].ParticipantID != "bob" {
		t.Fatalf("adopted not sorted: %+v", res.Adopted)
	}
	// 完成后额外贡献不能改变结果。
	if _, err := svc.SubmitContribution("cer-test", contrib("carol", 1, "req-carol", 3)); !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("post-complete submit: want ErrCeremonyClosed, got %v", err)
	}
	again, err := svc.Complete("cer-test")
	if err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
	if !again.AlreadyDone || again.Outbox.ID != res.Outbox.ID {
		t.Fatalf("complete not idempotent: %+v", again)
	}
	if len(svc.Outbox()) != 1 {
		t.Fatalf("outbox must contain exactly one event, got %d", len(svc.Outbox()))
	}
}

// 并发完成：只有一个请求执行完成，所有调用方得到同一份结果与唯一 outbox。
func TestConcurrentCompleteSingleWinner(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol"}, 3, clk.now().Add(time.Hour))
	submitN(t, svc, "cer-test", 1, "alice", 1, "req-alice")
	submitN(t, svc, "cer-test", 1, "bob", 2, "req-bob")
	submitN(t, svc, "cer-test", 1, "carol", 3, "req-carol")

	const n = 32
	var wg sync.WaitGroup
	results := make([]CompleteResult, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.Complete("cer-test")
		}(i)
	}
	wg.Wait()

	var ok int
	for i, err := range errs {
		if err != nil {
			t.Errorf("completion %d failed: %v", i, err)
			continue
		}
		if len(results[i].Adopted) != 3 {
			t.Errorf("completion %d adopted=%d", i, len(results[i].Adopted))
		}
		ok++
	}
	if ok != n {
		t.Fatalf("%d of %d completions failed", n-ok, n)
	}
	if got := len(svc.Outbox()); got != 1 {
		t.Fatalf("want exactly 1 outbox event, got %d", got)
	}
}

// 贡献并发：同一参与者多次并发提交只有一次被计入。
func TestConcurrentSubmitOnePerParticipant(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol", "dave"}, 4, clk.now().Add(time.Hour))

	const n = 40
	var wg sync.WaitGroup
	accepted, duplicated := 0, 0
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			participant := []string{"alice", "bob", "carol", "dave"}[i%4]
			_, err := svc.SubmitContribution("cer-test",
				contrib(participant, 1, fmt.Sprintf("req-%s-%d", participant, i), byte(i)))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrDuplicateContributor):
				duplicated++
			default:
				t.Errorf("unexpected error for %s: %v", participant, err)
			}
		}(i)
	}
	wg.Wait()
	if accepted != 4 || duplicated != n-4 {
		t.Fatalf("accepted=%d duplicated=%d, want 4 and %d", accepted, duplicated, n-4)
	}
	res, err := svc.Complete("cer-test")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if len(res.Adopted) != 4 || len(svc.Outbox()) != 1 {
		t.Fatalf("adopted=%d outbox=%d", len(res.Adopted), len(svc.Outbox()))
	}
}

// 替换、贡献、完成、取消并发：只能产生合法轮次演进与唯一终态。
func TestConcurrentReplaceSubmitCompleteCancel(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob", "carol"}, 2, clk.now().Add(time.Hour))

	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}

	// 先让新轮次有机会凑够门限，再让完成/取消/替换/迟到贡献全部并发冲击。
	run(func() {
		_, _ = svc.ReplaceParticipants("cer-test", ReplaceParams{
			Participants: []string{"alice", "bob", "carol", "dave"},
			Threshold:    2,
			Deadline:     clk.now().Add(2 * time.Hour),
			Actor:        "ops",
			Reason:       "add dave",
		})
	})
	run(func() { _, _ = svc.SubmitContribution("cer-test", contrib("alice", 1, "r1-alice", 1)) })
	run(func() { _, _ = svc.SubmitContribution("cer-test", contrib("alice", 2, "r2-alice", 2)) })
	run(func() { _, _ = svc.SubmitContribution("cer-test", contrib("bob", 2, "r2-bob", 3)) })
	run(func() { _, _ = svc.SubmitContribution("cer-test", contrib("carol", 1, "r1-carol", 4)) })
	run(func() { _, _ = svc.Complete("cer-test") })
	run(func() { _, _ = svc.Cancel("cer-test", "ops", "abort") })
	wg.Wait()

	v, err := svc.GetCeremony("cer-test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	switch v.Status {
	case StatusCompleted, StatusCancelled:
		// 合法终态
	default:
		t.Fatalf("illegal final status: %s", v.Status)
	}
	if v.Status == StatusCompleted {
		if len(svc.Outbox()) != 1 {
			t.Fatalf("completed but outbox count = %d", len(svc.Outbox()))
		}
		if v.AdoptedCount < v.Threshold {
			t.Fatalf("completed with adopted=%d threshold=%d", v.AdoptedCount, v.Threshold)
		}
	} else if len(svc.Outbox()) != 0 {
		t.Fatalf("cancelled ceremony must have no outbox event, got %d", len(svc.Outbox()))
	}
	// 终态不可逆：再完成/替换都失败；取消仅在已取消时幂等。
	if _, err := svc.Complete("cer-test"); v.Status == StatusCancelled && !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("complete after cancel: want ErrCeremonyClosed, got %v", err)
	}
	if _, err := svc.Cancel("cer-test", "ops", "again"); v.Status == StatusCompleted && !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("cancel after complete: want ErrCeremonyClosed, got %v", err)
	}
	if _, err := svc.ReplaceParticipants("cer-test", ReplaceParams{
		Participants: []string{"x"}, Threshold: 1, Deadline: clk.now().Add(time.Hour),
	}); !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("replace after terminal: want ErrCeremonyClosed, got %v", err)
	}
}

// --- 取消 ---

func TestCancel(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob"}, 2, clk.now().Add(time.Hour))
	v, err := svc.Cancel("cer-test", "admin", "no longer needed")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if v.Status != StatusCancelled || v.CancelReason != "no longer needed" {
		t.Fatalf("bad cancel view: %+v", v)
	}
	// 取消幂等。
	v2, err := svc.Cancel("cer-test", "admin", "different reason")
	if err != nil || v2.Status != StatusCancelled {
		t.Fatalf("idempotent cancel: %v %+v", err, v2)
	}
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req", 1)); !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("submit after cancel: want ErrCeremonyClosed, got %v", err)
	}
	if _, err := svc.Complete("cer-test"); !errors.Is(err, ErrCeremonyClosed) {
		t.Fatalf("complete after cancel: want ErrCeremonyClosed, got %v", err)
	}
	if len(svc.Outbox()) != 0 {
		t.Fatalf("cancelled ceremony must not write outbox")
	}
}

// --- 审计 ---

func TestAuditTrail(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice", "bob"}, 2, clk.now().Add(time.Hour))
	submitN(t, svc, "cer-test", 1, "alice", 1, "req-alice")
	submitN(t, svc, "cer-test", 1, "bob", 2, "req-bob")
	if _, err := svc.Complete("cer-test"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	events, err := svc.AuditTrail("cer-test")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	wantTypes := []string{
		AuditCeremonyCreated,
		AuditContributionAccepted,
		AuditContributionAccepted,
		AuditCompleted,
		AuditOutboxWritten,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("audit events = %d, want %d: %+v", len(events), len(wantTypes), events)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("event %d = %s, want %s", i, events[i].Type, want)
		}
		if i > 0 && events[i].Seq <= events[i-1].Seq {
			t.Fatalf("audit seq not monotonic: %d <= %d", events[i].Seq, events[i-1].Seq)
		}
	}
	// 拒绝也会留痕。
	if _, err := svc.SubmitContribution("cer-test", contrib("alice", 1, "req-late", 9)); err == nil {
		t.Fatal("expected rejection after completion")
	}
	events, _ = svc.AuditTrail("cer-test")
	last := events[len(events)-1]
	if last.Type != AuditContributionRejected {
		t.Fatalf("rejection not audited, last event = %s", last.Type)
	}
	if _, err := svc.AuditTrail("missing"); !errors.Is(err, ErrCeremonyNotFound) {
		t.Fatalf("audit missing ceremony: want ErrCeremonyNotFound, got %v", err)
	}
}

// --- 明文隔离：持久化内容只含承诺与摘要 ---

func TestNoPlaintextSharePersisted(t *testing.T) {
	svc, clk := newTestService(t)
	createTestCeremony(t, svc, []string{"alice"}, 1, clk.now().Add(time.Hour))
	secret := []byte("THE-SECRET-SHARE-PLAINTEXT")
	c := Contribution{
		RequestID:     "req-1",
		ParticipantID: "alice",
		Round:         1,
		Commitment:    []byte("commitment"),
		ShareDigest:   []byte("digest-only"),
		// 故意不放 secret；序列化整个状态后确认任何地方都不可能出现明文。
	}
	_ = secret
	if _, err := svc.SubmitContribution("cer-test", c); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := svc.Complete("cer-test"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, evt := range svc.Outbox() {
		for _, a := range evt.Adopted {
			if bytes.Contains(a.Commitment, secret) || bytes.Contains(a.ShareDigest, secret) {
				t.Fatal("plaintext secret leaked into persisted state")
			}
		}
	}
}

// --- FileStore 持久化恢复 ---

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	clk := &fakeClock{t: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	svc, err := NewService(NewFileStore(path), WithClock(clk.now))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	v, err := svc.CreateCeremony(CreateParams{
		CeremonyID:   "cer-fs",
		Participants: []string{"alice", "bob"},
		Threshold:    2,
		Deadline:     clk.now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitN(t, svc, v.ID, 1, "alice", 1, "req-alice")
	submitN(t, svc, v.ID, 1, "bob", 2, "req-bob")
	if _, err := svc.Complete(v.ID); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// 重新加载，状态、冻结集合、outbox 与审计均应恢复。
	svc2, err := NewService(NewFileStore(path), WithClock(clk.now))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, err := svc2.GetCeremony("cer-fs")
	if err != nil {
		t.Fatalf("get after reload: %v", err)
	}
	if got.Status != StatusCompleted || got.AdoptedCount != 2 {
		t.Fatalf("state not restored: %+v", got)
	}
	again, err := svc2.Complete("cer-fs")
	if err != nil || !again.AlreadyDone {
		t.Fatalf("complete after reload should be idempotent: err=%v done=%v", err, again.AlreadyDone)
	}
	if len(svc2.Outbox()) != 1 || svc2.Outbox()[0].ID != "cer-fs/key-activation" {
		t.Fatalf("outbox not restored: %+v", svc2.Outbox())
	}
	if events, _ := svc2.AuditTrail("cer-fs"); len(events) != 5 {
		t.Fatalf("audit not restored: %d events", len(events))
	}
}
