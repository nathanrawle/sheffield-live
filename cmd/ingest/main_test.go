package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

func TestCreateReviewGroupsFromReportStagesDuplicateGroups(t *testing.T) {
	st := &fakeReviewStageStore{ids: []int64{101}}
	report := successfulManualReportForReviewStage()

	stage, err := createReviewGroupsFromReport(context.Background(), st, report)
	if err != nil {
		t.Fatalf("stage review groups: %v", err)
	}

	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("stage groups created = %d, want %d", got, want)
	}
	if got, want := stage.CandidateCount, 2; got != want {
		t.Fatalf("stage candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 1; got != want {
		t.Fatalf("stage groups = %d, want %d", got, want)
	}
	if got, want := stage.Groups[0].ID, int64(101); got != want {
		t.Fatalf("stage group ID = %d, want %d", got, want)
	}
}

func TestReviewStageForReportSkipsFailedManualRun(t *testing.T) {
	st := &fakeReviewStageStore{ids: []int64{101}}

	stage, err := reviewStageForReport(context.Background(), st, successfulManualReportForReviewStage(), errors.New("manual ingest failed"))
	if err != nil {
		t.Fatalf("review stage for failed run: %v", err)
	}
	if got, want := len(st.inputs), 0; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if !stage.Enabled {
		t.Fatal("stage enabled = false, want true")
	}
	if stage.GroupsCreated != 0 || stage.CandidateCount != 0 {
		t.Fatalf("stage counts = groups %d candidates %d, want zero", stage.GroupsCreated, stage.CandidateCount)
	}
}

func TestCreateReviewGroupsFromReportReportsCreateError(t *testing.T) {
	st := &fakeReviewStageStore{err: errors.New("insert failed")}

	stage, err := createReviewGroupsFromReport(context.Background(), st, successfulManualReportForReviewStage())
	if err == nil {
		t.Fatal("expected staging error")
	}
	if got, want := stage.GroupsCreated, 0; got != want {
		t.Fatalf("stage groups created = %d, want %d", got, want)
	}
	if got, want := len(stage.Errors), 1; got != want {
		t.Fatalf("stage errors = %d, want %d", got, want)
	}
	if !strings.Contains(stage.Errors[0], "insert failed") {
		t.Fatalf("stage error = %q, want insert failure", stage.Errors[0])
	}
}

type fakeReviewStageStore struct {
	ids    []int64
	err    error
	inputs []review.GroupInput
}

func (s *fakeReviewStageStore) CreateReviewGroup(_ context.Context, input review.GroupInput) (int64, error) {
	s.inputs = append(s.inputs, input)
	if s.err != nil {
		return 0, s.err
	}
	if len(s.ids) == 0 {
		return int64(len(s.inputs)), nil
	}
	id := s.ids[0]
	s.ids = s.ids[1:]
	return id, nil
}

func successfulManualReportForReviewStage() ingest.Report {
	return ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				URL: "https://calendar.example.test/one.ics",
				Candidates: []ingest.EventCandidate{
					{
						UID:      "duplicate",
						Summary:  "Duplicate one",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:00:00Z",
					},
					{
						UID:      "duplicate",
						Summary:  "Duplicate two",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-02T19:00:00Z",
					},
					{
						UID:      "singleton",
						Summary:  "Singleton",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-03T19:00:00Z",
					},
				},
			},
		},
	}
}
