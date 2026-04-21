package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReplayImportRunRebuildsReportFromSnapshotEnvelopes(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         77,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=3 skips=4 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         101,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         102,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live.ics",
						FinalURL:    "https://legacy.example.test/live.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: duplicate",
							"SUMMARY: Duplicate one",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260501T190000Z",
							"DTEND:20260501T210000Z",
							"END:VEVENT",
							"BEGIN:VEVENT",
							"UID: duplicate",
							"SUMMARY: Duplicate two",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260502T190000Z",
							"END:VEVENT",
							"BEGIN:VEVENT",
							"UID: singleton",
							"SUMMARY: Singleton",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260503T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 77, ReplayOptions{Limit: 1})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if report.Status != importStatusSucceeded {
		t.Fatalf("status = %q, want %q", report.Status, importStatusSucceeded)
	}
	if report.ImportRunID != 77 {
		t.Fatalf("import run id = %d, want 77", report.ImportRunID)
	}
	if report.Source != DefaultSource {
		t.Fatalf("source = %q, want %q", report.Source, DefaultSource)
	}
	if report.SourceURL != "https://www.sidneyandmatilda.com/" {
		t.Fatalf("source url = %q, want homepage", report.SourceURL)
	}
	if report.Limit != 1 {
		t.Fatalf("limit = %d, want 1", report.Limit)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if report.Page == nil || report.Page.ID != 101 {
		t.Fatalf("page snapshot = %#v, want ID 101", report.Page)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Skips), 0; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Totals.Snapshots, 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 0; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func TestReplayImportRunWrapsLoadError(t *testing.T) {
	_, err := ReplayImportRun(context.Background(), fakeReplayStore{err: errors.New("load failed")}, 91, ReplayOptions{Limit: 1})
	if err == nil {
		t.Fatal("expected load error")
	}
	if !strings.Contains(err.Error(), "load import run 91") || !strings.Contains(err.Error(), "load failed") {
		t.Fatalf("error = %v, want wrapped load failure", err)
	}
}

func TestReplayImportRunRejectsMalformedSnapshotJSON(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         92,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         1201,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload:    "{not-json",
				},
			},
		},
	}

	_, err := ReplayImportRun(context.Background(), store, 92, ReplayOptions{Limit: 1})
	if err == nil || !strings.Contains(err.Error(), "decode snapshot 1201 payload") {
		t.Fatalf("error = %v, want malformed JSON rejection", err)
	}
}

func TestReplayImportRunRejectsInvalidBase64Body(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         93,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         1202,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload:    `{"version":1,"body_base64":"not-base64","sha256":"abc","truncated":false,"metadata":{"url":"https://www.sidneyandmatilda.com/","final_url":"https://www.sidneyandmatilda.com/","body_bytes":1,"captured_at":"2026-04-20T12:01:00Z"}}`,
				},
			},
		},
	}

	_, err := ReplayImportRun(context.Background(), store, 93, ReplayOptions{Limit: 1})
	if err == nil || !strings.Contains(err.Error(), "decode snapshot 1202 body") {
		t.Fatalf("error = %v, want invalid base64 rejection", err)
	}
}

func TestReplayImportRunRejectsZeroLimit(t *testing.T) {
	store := fakeReplayStore{
		run: ReplayRun{
			ID:        76,
			StartedAt: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			Status:    "succeeded",
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 76, ReplayOptions{}); err == nil || !strings.Contains(err.Error(), "limit must be between 1 and") {
		t.Fatalf("error = %v, want zero-limit rejection", err)
	}
}

func TestReplayImportRunMatchesSnapshotByFinalURL(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         78,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=1 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         201,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/redirected.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         202,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://redirect.example.test/original.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://redirect.example.test/original.ics",
						FinalURL:    "https://legacy.example.test/redirected.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: one",
							"SUMMARY: One",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260501T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 78, ReplayOptions{Limit: 1})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}
	if got, want := report.Calendars[0].URL, "https://legacy.example.test/redirected.ics"; got != want {
		t.Fatalf("calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Snapshot.FinalURL, "https://legacy.example.test/redirected.ics"; got != want {
		t.Fatalf("calendar final url = %q, want %q", got, want)
	}
}

func TestReplayImportRunRejectsDuplicateICSSnapshotLookupKey(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         78,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         210,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         211,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/other.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/other.ics",
						FinalURL:    "https://legacy.example.test/live.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         212,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live.ics",
						FinalURL:    "https://legacy.example.test/live.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 78, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "duplicate ICS snapshot lookup key") {
		t.Fatalf("error = %v, want duplicate lookup key rejection", err)
	}
}

func TestReplayImportRunRejectsHashMismatch(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         79,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         301,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, func(envelope *SnapshotEnvelope) {
						envelope.SHA256 = strings.Repeat("0", len(envelope.SHA256))
					}),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 79, ReplayOptions{Limit: 1}); err == nil {
		t.Fatal("expected hash mismatch error")
	}
}

func TestReplayImportRunRejectsMissingICSSnapshot(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         80,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         401,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 80, ReplayOptions{Limit: 1}); err == nil {
		t.Fatal("expected missing ICS snapshot error")
	}
}

func TestReplayImportRunSelectsSourceSnapshotByMetadata(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         81,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=1 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         601,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live.ics",
						FinalURL:    "https://legacy.example.test/live.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: one",
							"SUMMARY: One",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260501T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         602,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 81, ReplayOptions{Limit: 1})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}
	if report.Limit != 1 {
		t.Fatalf("limit = %d, want 1", report.Limit)
	}
	if report.Page == nil || report.Page.ID != 602 {
		t.Fatalf("page snapshot = %#v, want ID 602", report.Page)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
}

func TestReplayImportRunUsesExplicitLimitNotNotes(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         82,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=2 candidates=4 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         701,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live-one.ics">Google Calendar ICS</a><a href="https://legacy.example.test/live-two.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         702,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live-one.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live-one.ics",
						FinalURL:    "https://legacy.example.test/live-one.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: one",
							"SUMMARY: One",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260501T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         703,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live-two.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live-two.ics",
						FinalURL:    "https://legacy.example.test/live-two.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: two",
							"SUMMARY: Two",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260502T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 82, ReplayOptions{Limit: 1})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}
	if report.Limit != 1 {
		t.Fatalf("limit = %d, want 1", report.Limit)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
}

func TestReplayImportRunRejectsUnfinishedRun(t *testing.T) {
	store := fakeReplayStore{
		run: ReplayRun{
			ID:        83,
			StartedAt: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			Status:    "succeeded",
			Notes:     "links=0 candidates=0 skips=0 errors=0",
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 83, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "unfinished") {
		t.Fatalf("error = %v, want unfinished run rejection", err)
	}
}

func TestReplayImportRunRejectsNonSucceededRun(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         84,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "failed",
			Notes:      "links=0 candidates=0 skips=0 errors=1",
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 84, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "want \"succeeded\"") {
		t.Fatalf("error = %v, want status rejection", err)
	}
}

func TestReplayImportRunRejectsVersionMismatch(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         85,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         801,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, func(envelope *SnapshotEnvelope) {
						envelope.Version = 2
					}),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 85, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("error = %v, want version rejection", err)
	}
}

func TestReplayImportRunRejectsMultiplePageSnapshots(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         86,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         901,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         902,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.sidneyandmatilda.com/",
						FinalURL:    "https://www.sidneyandmatilda.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
						CapturedAt:  time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 86, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "multiple source page snapshots") {
		t.Fatalf("error = %v, want multiple page rejection", err)
	}
}

func TestReplayImportRunRejectsNoPageSnapshot(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         87,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=0 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         1001,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://legacy.example.test/live.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://legacy.example.test/live.ics",
						FinalURL:    "https://legacy.example.test/live.ics",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID: one",
							"SUMMARY: One",
							"LOCATION: Sidney & Matilda",
							"DTSTART:20260501T190000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	if _, err := ReplayImportRun(context.Background(), store, 87, ReplayOptions{Limit: 1}); err == nil || !strings.Contains(err.Error(), "no source page snapshot") {
		t.Fatalf("error = %v, want missing page rejection", err)
	}
}

func TestReplayImportRunRejectsTruncatedOrFailedCalendarWithoutParsing(t *testing.T) {
	cases := []struct {
		name        string
		runID       int64
		truncated   bool
		statusCode  int
		wantErrText string
	}{
		{name: "truncated", runID: 88, truncated: true, statusCode: 200, wantErrText: "ICS response was truncated"},
		{name: "non-2xx", runID: 89, truncated: false, statusCode: 500, wantErrText: "ICS returned HTTP 500"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
			store := fakeReplayStore{
				run: ReplayRun{
					ID:         tc.runID,
					StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
					FinishedAt: &finishedAt,
					Status:     "succeeded",
					Notes:      "links=1 candidates=1 skips=0 errors=0",
					Snapshots: []ReplaySnapshot{
						{
							ID:         1101,
							SourceName: "Sidney & Matilda listings",
							SourceURL:  "https://www.sidneyandmatilda.com/",
							CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
							Payload: mustReplaySnapshotPayload(t, FetchResult{
								URL:         "https://www.sidneyandmatilda.com/",
								FinalURL:    "https://www.sidneyandmatilda.com/events/",
								Status:      "200 OK",
								StatusCode:  200,
								ContentType: "text/html",
								Body:        []byte(`<a href="https://legacy.example.test/live.ics">Google Calendar ICS</a>`),
								CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
							}, nil),
						},
						{
							ID:         1102,
							SourceName: "Sidney & Matilda Google Calendar ICS",
							SourceURL:  "https://legacy.example.test/live.ics",
							CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
							Payload: mustReplaySnapshotPayload(t, FetchResult{
								URL:         "https://legacy.example.test/live.ics",
								FinalURL:    "https://legacy.example.test/live.ics",
								Status:      "200 OK",
								StatusCode:  200,
								ContentType: "text/calendar",
								Body: []byte(strings.Join([]string{
									"BEGIN:VCALENDAR",
									"BEGIN:VEVENT",
									"UID: one",
									"SUMMARY: One",
									"LOCATION: Sidney & Matilda",
									"DTSTART:20260501T190000Z",
									"END:VEVENT",
									"END:VCALENDAR",
									"",
								}, "\n")),
								CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
							}, func(envelope *SnapshotEnvelope) {
								envelope.Truncated = tc.truncated
								envelope.Metadata.StatusCode = tc.statusCode
							}),
						},
					},
				},
			}

			report, err := ReplayImportRun(context.Background(), store, tc.runID, ReplayOptions{Limit: 1})
			if !errors.Is(err, ErrRunFailed) {
				t.Fatalf("error = %v, want ErrRunFailed", err)
			}
			if report.Status != importStatusFailed {
				t.Fatalf("status = %q, want failed", report.Status)
			}
			if got, want := len(report.Calendars), 1; got != want {
				t.Fatalf("calendars = %d, want %d", got, want)
			}
			if got, want := len(report.Calendars[0].Candidates), 0; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			if got, want := len(report.Calendars[0].Errors), 1; got != want {
				t.Fatalf("calendar errors = %d, want %d", got, want)
			}
			if !strings.Contains(report.Calendars[0].Errors[0], tc.wantErrText) {
				t.Fatalf("calendar error = %q, want %q", report.Calendars[0].Errors[0], tc.wantErrText)
			}
		})
	}
}

type fakeReplayStore struct {
	run ReplayRun
	err error
}

func (s fakeReplayStore) LoadImportRun(_ context.Context, id int64) (ReplayRun, error) {
	if s.err != nil {
		return ReplayRun{}, s.err
	}
	run := s.run
	run.ID = id
	return run, nil
}

func mustReplaySnapshotPayload(t *testing.T, result FetchResult, mutate func(*SnapshotEnvelope)) string {
	t.Helper()

	envelope := NewSnapshotEnvelope(result)
	if mutate != nil {
		mutate(&envelope)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal snapshot envelope: %v", err)
	}
	return string(raw)
}
