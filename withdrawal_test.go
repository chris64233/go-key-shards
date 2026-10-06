package keyshards

import (
	"context"
	"sync"
	"testing"
	"time"
)

func requestWithdrawal(t *testing.T, s *Service, id, reqID string, round int, participant string, digest []byte, reason, reviewer string) *Withdrawal {
	t.Helper()
	w, err := s.RequestContributionWithdrawal(context.Background(), RequestContributionWithdrawalInput{
		CeremonyID: id, RequestID: reqID, RoundNumber: round,
		ParticipantID: participant, ShardDigest: digest, Reason: reason, Reviewer: reviewer,
	})
	if err != nil {
		t.Fatalf("RequestContributionWithdrawal(%s): %v", reqID, err)
	}
	return w
}

func reviewWithdrawal(t *testing.T, s *Service, id, reqID, reviewer string, approve bool) *Withdrawal {
	t.Helper()
	w, err := s.ReviewContributionWithdrawal(context.Background(), ReviewContributionWithdrawalInput{
		CeremonyID: id, RequestID: reqID, Reviewer: reviewer, Approve: approve,
	})
	if err != nil {
		t.Fatalf("ReviewContributionWithdrawal(%s): %v", reqID, err)
	}
	return w
}

func TestContributionWithdrawalApprovalAndResubmitRecovery(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			clk := newFakeClock()
			s := NewService(f.new(t), clk.now)
			mustCreate(t, s, "c", []string{"a", "b", "c"}, 2, clk.now().Add(time.Hour))
			first := cv("a", 1)
			contrib(t, s, "c", "contrib-a-1", 1, first)
			contrib(t, s, "c", "contrib-b", 1, cv("b", 2))

			w := requestWithdrawal(t, s, "c", "wd-a", 1, "a", first.ShardDigest, "crypto mismatch", "auditor")
			if w.Status != WithdrawalPending || w.ValidContributionCount != 1 {
				t.Fatalf("withdrawal = %+v", w)
			}
			_, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "blocked", KeyID: "k",
			})
			if !errIs(err, ErrThresholdNotReached) {
				t.Fatalf("complete during review = %v", err)
			}
			_, err = s.SubmitContribution(context.Background(), ContributeInput{
				CeremonyID: "c", RequestID: "resubmit-blocked", RoundNumber: 1, Contribution: cv("a", 3),
			})
			if !errIs(err, ErrContributionReviewPending) {
				t.Fatalf("resubmit during review = %v", err)
			}

			w = reviewWithdrawal(t, s, "c", "wd-a", "auditor", true)
			if w.Status != WithdrawalApproved || !equalStrings(w.MissingContributors, []string{"a", "c"}) {
				t.Fatalf("approval = %+v", w)
			}
			state, err := s.GetCurrentRoundState(context.Background(), "c")
			if err != nil {
				t.Fatal(err)
			}
			if state.ValidContributionCount != 1 || !equalStrings(state.MissingContributors, []string{"a", "c"}) {
				t.Fatalf("recovery state = %+v", state)
			}
			statuses := map[string]ContributionStatus{}
			for _, item := range state.Contributions {
				statuses[item.ParticipantID] = item.Status
			}
			if statuses["a"] != ContributionWithdrawn || statuses["b"] != ContributionValid {
				t.Fatalf("contribution statuses = %+v", statuses)
			}

			contrib(t, s, "c", "contrib-a-2", 1, cv("a", 4))
			ob, err := s.Complete(context.Background(), CompleteInput{
				CeremonyID: "c", RequestID: "complete", KeyID: "k",
			})
			if err != nil {
				t.Fatalf("complete after resubmit: %v", err)
			}
			if ids := participantIDs(ob.Contributions); !equalStrings(ids, []string{"a", "b"}) {
				t.Fatalf("adopted = %v", ids)
			}
			if ob.Contributions[0].ShardDigest[1] != 4 {
				t.Fatalf("withdrawn contribution was adopted: %+v", ob.Contributions[0])
			}

			_, err = s.RequestContributionWithdrawal(context.Background(), RequestContributionWithdrawalInput{
				CeremonyID: "c", RequestID: "late", RoundNumber: 1,
				ParticipantID: "b", ShardDigest: cv("b", 2).ShardDigest, Reason: "x", Reviewer: "auditor",
			})
			if !errIs(err, ErrCeremonyTerminal) {
				t.Fatalf("withdrawal after complete = %v", err)
			}
		})
	}
}

func TestContributionWithdrawalRejectedRestoresContribution(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	first := cv("a", 1)
	contrib(t, s, "c", "contrib-a", 1, first)

	requestWithdrawal(t, s, "c", "wd-a", 1, "a", first.ShardDigest, "suspect", "auditor")
	w := reviewWithdrawal(t, s, "c", "wd-a", "auditor", false)
	if w.Status != WithdrawalRejected || w.ValidContributionCount != 1 {
		t.Fatalf("review = %+v", w)
	}
	ob, err := s.Complete(context.Background(), CompleteInput{
		CeremonyID: "c", RequestID: "complete", KeyID: "k",
	})
	if err != nil {
		t.Fatalf("complete after rejection: %v", err)
	}
	if ids := participantIDs(ob.Contributions); !equalStrings(ids, []string{"a"}) {
		t.Fatalf("adopted = %v", ids)
	}
}

func TestContributionWithdrawalReplayConflictAndTimeline(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	first := cv("a", 1)
	contrib(t, s, "c", "contrib-a", 1, first)
	in := RequestContributionWithdrawalInput{
		CeremonyID: "c", RequestID: "wd", RoundNumber: 1,
		ParticipantID: "a", ShardDigest: first.ShardDigest, Reason: "reason", Reviewer: "auditor",
	}
	requestWithdrawal(t, s, in.CeremonyID, in.RequestID, in.RoundNumber, in.ParticipantID, in.ShardDigest, in.Reason, in.Reviewer)
	replay := requestWithdrawal(t, s, in.CeremonyID, in.RequestID, in.RoundNumber, in.ParticipantID, in.ShardDigest, in.Reason, in.Reviewer)
	if replay.Status != WithdrawalPending {
		t.Fatalf("replay changed result: %+v", replay)
	}

	for _, mutate := range []func(*RequestContributionWithdrawalInput){
		func(x *RequestContributionWithdrawalInput) { x.RoundNumber = 2 },
		func(x *RequestContributionWithdrawalInput) { x.ParticipantID = "b" },
		func(x *RequestContributionWithdrawalInput) { x.ShardDigest = []byte("other") },
	} {
		conflict := in
		mutate(&conflict)
		_, err := s.RequestContributionWithdrawal(context.Background(), conflict)
		if !errIs(err, ErrConflict) {
			t.Fatalf("conflict for %+v = %v", conflict, err)
		}
	}
	_, err := s.ReviewContributionWithdrawal(context.Background(), ReviewContributionWithdrawalInput{
		CeremonyID: "c", RequestID: "wd", Reviewer: "other", Approve: true,
	})
	if !errIs(err, ErrReviewerMismatch) {
		t.Fatalf("wrong reviewer = %v", err)
	}
	approved := reviewWithdrawal(t, s, "c", "wd", "auditor", true)
	again := reviewWithdrawal(t, s, "c", "wd", "auditor", false)
	if again.Status != WithdrawalApproved || again.DecidedAt != approved.DecidedAt {
		t.Fatalf("final decision changed: %+v", again)
	}
	events, err := s.Audit(context.Background(), "c", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, ev := range events {
		if ev.Kind == AuditWithdrawalRequested || ev.Kind == AuditWithdrawalApproved {
			kinds = append(kinds, ev.Kind)
		}
	}
	if !equalStrings(kinds, []string{AuditWithdrawalRequested, AuditWithdrawalApproved}) {
		t.Fatalf("withdrawal timeline = %v", kinds)
	}
}

func TestWithdrawalRacesWithCompleteAndReplacement(t *testing.T) {
	clk := newFakeClock()
	s := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s, "c", []string{"a", "b"}, 1, clk.now().Add(time.Hour))
	contrib(t, s, "c", "a", 1, cv("a", 1))

	var wg sync.WaitGroup
	var completeErr, withdrawErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, completeErr = s.Complete(context.Background(), CompleteInput{
			CeremonyID: "c", RequestID: "finish", KeyID: "k",
		})
	}()
	go func() {
		defer wg.Done()
		_, withdrawErr = s.RequestContributionWithdrawal(context.Background(), RequestContributionWithdrawalInput{
			CeremonyID: "c", RequestID: "wd", RoundNumber: 1,
			ParticipantID: "a", ShardDigest: cv("a", 1).ShardDigest, Reason: "x", Reviewer: "r",
		})
	}()
	wg.Wait()
	got, _ := s.GetCeremony(context.Background(), "c")
	if got.Status == StatusCompleted {
		if completeErr != nil || !errIs(withdrawErr, ErrCeremonyTerminal) {
			t.Fatalf("complete winner: complete=%v withdrawal=%v", completeErr, withdrawErr)
		}
	} else if !errIs(completeErr, ErrThresholdNotReached) || withdrawErr != nil {
		t.Fatalf("withdrawal winner: complete=%v withdrawal=%v", completeErr, withdrawErr)
	}

	s2 := NewService(NewMemRepository(), clk.now)
	mustCreate(t, s2, "c2", []string{"a", "b", "c"}, 1, clk.now().Add(time.Hour))
	first := cv("a", 1)
	contrib(t, s2, "c2", "a", 1, first)
	requestWithdrawal(t, s2, "c2", "wd", 1, "a", first.ShardDigest, "x", "auditor")
	initiate(t, s2, "c2", "rep", "b", []string{"b", "c", "d"}, clk.now().Add(2*time.Hour), "rotate")
	got2, _ := s2.GetCeremony(context.Background(), "c2")
	if got2.CurrentRound().Number != 2 || got2.Withdrawals["wd"].Status != WithdrawalClosed {
		t.Fatalf("state = round %d withdrawal %+v", got2.CurrentRound().Number, got2.Withdrawals["wd"])
	}
	_, err := s2.ReviewContributionWithdrawal(context.Background(), ReviewContributionWithdrawalInput{
		CeremonyID: "c2", RequestID: "wd", Reviewer: "auditor", Approve: true,
	})
	if !errIs(err, ErrWithdrawalFinal) {
		t.Fatalf("stale review = %v", err)
	}
	_, err = s2.Complete(context.Background(), CompleteInput{
		CeremonyID: "c2", RequestID: "finish", KeyID: "k",
	})
	if !errIs(err, ErrThresholdNotReached) {
		t.Fatalf("new round threshold = %v", err)
	}
}
