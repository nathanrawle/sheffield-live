package ingest

import (
	"testing"
	"time"
)

func TestSnapshotRetentionForReportFindsLatestStart(t *testing.T) {
	report := Report{
		ImportRunID: 42,
		Calendars: []CalendarReport{
			{
				Candidates: []EventCandidate{
					{Summary: "blank"},
					{Summary: "early", StartAt: "2026-05-10T18:30:00+01:00"},
					{Summary: "invalid", StartAt: "not-a-time"},
				},
			},
			{
				Candidates: []EventCandidate{
					{Summary: "latest", StartAt: "2026-05-11T19:00:00Z"},
					{Summary: "older", StartAt: "2026-05-09T19:00:00Z"},
				},
			},
		},
	}

	retention := SnapshotRetentionForReport(report)
	if retention.ImportRunID != 42 {
		t.Fatalf("import run ID = %d, want 42", retention.ImportRunID)
	}
	if got, want := retention.CandidateCount, 5; got != want {
		t.Fatalf("candidate count = %d, want %d", got, want)
	}
	if got, want := retention.ParseableStartCount, 3; got != want {
		t.Fatalf("parseable start count = %d, want %d", got, want)
	}
	want := time.Date(2026, time.May, 11, 19, 0, 0, 0, time.UTC)
	if retention.LatestStartAt == nil || !retention.LatestStartAt.Equal(want) {
		t.Fatalf("latest start = %v, want %v", retention.LatestStartAt, want)
	}
}

func TestSnapshotRetentionForReportWithoutParseableStarts(t *testing.T) {
	report := Report{
		ImportRunID: 99,
		Calendars: []CalendarReport{{
			Candidates: []EventCandidate{
				{Summary: "blank"},
				{Summary: "invalid", StartAt: "not-a-time"},
			},
		}},
	}

	retention := SnapshotRetentionForReport(report)
	if got, want := retention.CandidateCount, 2; got != want {
		t.Fatalf("candidate count = %d, want %d", got, want)
	}
	if retention.ParseableStartCount != 0 {
		t.Fatalf("parseable start count = %d, want 0", retention.ParseableStartCount)
	}
	if retention.LatestStartAt != nil {
		t.Fatalf("latest start = %v, want nil", retention.LatestStartAt)
	}
}
