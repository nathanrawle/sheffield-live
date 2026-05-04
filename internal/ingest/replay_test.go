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

func TestReplayImportRunRebuildsYellowArchReportFromSourcePageSnapshot(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         177,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=2 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         301,
					SourceName: "Yellow Arch listings",
					SourceURL:  "https://www.yellowarch.com/events/",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.yellowarch.com/events/",
						FinalURL:    "https://www.yellowarch.com/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "yellow_arch.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 177, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, yellowArchSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, "https://www.yellowarch.com/events/"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Yellow Arch Studios"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsCafeNo9ReportFromSourcePageSnapshot(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         277,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=2 skips=2 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         351,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9",
						FinalURL:    "https://www.wegottickets.com/Cafe9",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "cafe9_page.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 277, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, CafeNo9Source; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, "https://www.wegottickets.com/Cafe9"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Cafe No9"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReplayImportRunEnrichesCafeNo9DescriptionsFromDetailSnapshots(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         287,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=1 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         451,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:        "https://www.wegottickets.com/Cafe9",
						FinalURL:   "https://www.wegottickets.com/Cafe9",
						Status:     "200 OK",
						StatusCode: 200,
						Body: []byte(`
							<h2><a href="/event/700004">An evening with Gideon Conn at Cafe No. 9</a></h2>
							<p>0 SHEFFIELD: Cafe No. 9</p>
							<p>P Thursday 5th November, 2026</p>
							<p>N Door time: 7:00pm, Start time: 7:30pm</p>
							<p>C Music - General</p>
						`),
						CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         452,
					SourceName: "Cafe No. 9 listings event details",
					SourceURL:  "https://www.wegottickets.com/event/700004",
					CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:        "https://www.wegottickets.com/event/700004",
						FinalURL:   "https://www.wegottickets.com/event/700004",
						Status:     "200 OK",
						StatusCode: 200,
						Body:       readFixture(t, "cafe9_detail.html"),
						CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 287, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Calendars[0].Candidates[0].Description, "The Leisure Society were founded by Nick Hemming.\n\nExpect oustanding songwriting and production craft."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := report.Totals.Snapshots, 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
}

func TestReplayImportRunEnrichesSidneyDescriptionsFromDetailSnapshots(t *testing.T) {
	finishedAt := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         88,
			StartedAt:  time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=1 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         461,
					SourceName: "Sidney & Matilda listings",
					SourceURL:  "https://www.sidneyandmatilda.com/",
					CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:        "https://www.sidneyandmatilda.com/",
						FinalURL:   "https://www.sidneyandmatilda.com/events/",
						Status:     "200 OK",
						StatusCode: 200,
						Body: []byte(`
							<a href="https://calendar.example.test/live.ics">Google Calendar ICS</a>
							<a href="/events/leo-middea-brazil">Leo Middea (Brazil)</a>
						`),
						CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         462,
					SourceName: "Sidney & Matilda Google Calendar ICS",
					SourceURL:  "https://calendar.example.test/live.ics",
					CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:        "https://calendar.example.test/live.ics",
						FinalURL:   "https://calendar.example.test/live.ics",
						Status:     "200 OK",
						StatusCode: 200,
						Body: []byte(strings.Join([]string{
							"BEGIN:VCALENDAR",
							"BEGIN:VEVENT",
							"UID:leo",
							"SUMMARY:Leo Middea (Brazil)",
							"LOCATION:Sidney & Matilda",
							"URL:https://www.sidneyandmatilda.com/events/leo-middea-brazil",
							"DTSTART:20260504T183000Z",
							"END:VEVENT",
							"END:VCALENDAR",
							"",
						}, "\n")),
						CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         463,
					SourceName: "Sidney & Matilda listings event details",
					SourceURL:  "https://www.sidneyandmatilda.com/events/leo-middea-brazil",
					CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:        "https://www.sidneyandmatilda.com/events/leo-middea-brazil",
						FinalURL:   "https://www.sidneyandmatilda.com/events/leo-middea-brazil",
						Status:     "200 OK",
						StatusCode: 200,
						Body:       readFixture(t, "sidney_detail.html"),
						CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 88, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Calendars[0].Candidates[0].Description, "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := report.Totals.Snapshots, 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
}

func TestReplayImportRunRebuildsJazzAtTheLescarReportFromSourcePageSnapshot(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         276,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=0 candidates=3 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         341,
					SourceName: "Jazz at The Lescar listings",
					SourceURL:  "http://www.jazzatthelescar.com/index.html",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "http://www.jazzatthelescar.com/index.html",
						FinalURL:    "http://www.jazzatthelescar.com/index.html",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "jazz_at_the_lescar.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 276, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, JazzAtTheLescarSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, "http://www.jazzatthelescar.com/index.html"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "The Lescar"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsTheGreystonesReportFromStoredMonthPages(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         275,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=2 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         331,
					SourceName: "The Greystones listings",
					SourceURL:  "https://www.mygreystones.co.uk/events/",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.mygreystones.co.uk/events/",
						FinalURL:    "https://www.mygreystones.co.uk/events/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "greystones_events.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         332,
					SourceName: "The Greystones month page",
					SourceURL:  "https://www.mygreystones.co.uk/april/",
					CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.mygreystones.co.uk/april/",
						FinalURL:    "https://www.mygreystones.co.uk/april/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "greystones_april.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 275, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, TheGreystonesSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 2; got != want {
		t.Fatalf("first month candidates = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Skips), 1; got != want {
		t.Fatalf("first month skips = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "The Greystones"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsCafeNo9PaginatedReportFromSourcePageSnapshots(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         278,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=2 candidates=2 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         361,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9",
						FinalURL:    "https://www.wegottickets.com/Cafe9",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "cafe9_paged_1.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         362,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9/page/2",
					CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9/page/2",
						FinalURL:    "https://www.wegottickets.com/Cafe9/page/2",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "cafe9_paged_2.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         363,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9/page/3",
					CapturedAt: time.Date(2026, 4, 23, 19, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9/page/3",
						FinalURL:    "https://www.wegottickets.com/Cafe9/page/3",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<html><body><h2><a href="/event/700201">An evening with Page Three at Cafe No9</a></h2><p>0 SHEFFIELD: Cafe No9</p><p>P Friday 15th May, 2026</p><p>N Door time: 7:00pm, Start time: 7:30pm</p><p><a href="/event/700201">Event info</a></p></body></html>`),
						CapturedAt:  time.Date(2026, 4, 23, 19, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 278, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := len(report.Links), 2; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 3; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Calendars[1].Candidates[0].UID, "https://www.wegottickets.com/event/700101"; got != want {
		t.Fatalf("page 2 uid = %q, want %q", got, want)
	}
	if got, want := report.Calendars[2].Candidates[0].UID, "https://www.wegottickets.com/event/700201"; got != want {
		t.Fatalf("page 3 uid = %q, want %q", got, want)
	}
}

func TestReplayImportRunCafeNo9PaginationRespectsGlobalLinkedPageLimit(t *testing.T) {
	finishedAt := time.Date(2026, 4, 23, 19, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         279,
			StartedAt:  time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=2 skips=2 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         371,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9",
					CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9",
						FinalURL:    "https://www.wegottickets.com/Cafe9",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "cafe9_paged_1.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         372,
					SourceName: "Cafe No. 9 listings",
					SourceURL:  "https://www.wegottickets.com/Cafe9/page/2",
					CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.wegottickets.com/Cafe9/page/2",
						FinalURL:    "https://www.wegottickets.com/Cafe9/page/2",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "cafe9_paged_2.html"),
						CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 279, ReplayOptions{Limit: 1})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
}

func TestReplayImportRunRebuildsLeadmillReportFromStoredLinkedICS(t *testing.T) {
	finishedAt := time.Date(2026, 4, 24, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         278,
			StartedAt:  time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=1 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         401,
					SourceName: "The Leadmill listings",
					SourceURL:  "https://leadmill.co.uk/live/",
					CapturedAt: time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://leadmill.co.uk/live/",
						FinalURL:    "https://leadmill.co.uk/live/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        []byte(`<link rel="alternate" type="text/calendar" href="https://leadmill.co.uk/listings/?ical=1">`),
						CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         402,
					SourceName: "The Leadmill iCal feed",
					SourceURL:  "https://leadmill.co.uk/listings/?ical=1",
					CapturedAt: time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://leadmill.co.uk/listings/?ical=1",
						FinalURL:    "https://leadmill.co.uk/listings/?ical=1",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/calendar",
						Body: []byte("BEGIN:VCALENDAR\n" +
							"BEGIN:VEVENT\n" +
							"UID:live-sheffield\n" +
							"SUMMARY:Maybe Gold - Yellow Arch\n" +
							"LOCATION:Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX\n" +
							"CATEGORIES:Live\n" +
							"DTSTART:20260501T190000Z\n" +
							"END:VEVENT\n" +
							"BEGIN:VEVENT\n" +
							"UID:not-live\n" +
							"SUMMARY:Club Night\n" +
							"LOCATION:The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE\n" +
							"CATEGORIES:Club\n" +
							"DTSTART:20260502T190000Z\n" +
							"END:VEVENT\n" +
							"END:VCALENDAR\n"),
						CapturedAt: time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 278, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, LeadmillSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, "https://leadmill.co.uk/live/"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Yellow Arch"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsCorporationReportFromStoredDetailPages(t *testing.T) {
	finishedAt := time.Date(2026, 4, 24, 12, 30, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         379,
			StartedAt:  time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=2 candidates=2 skips=0 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         501,
					SourceName: "Corporation Sheffield live listings",
					SourceURL:  "https://www.corporation.org.uk/live/",
					CapturedAt: time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.corporation.org.uk/live/",
						FinalURL:    "https://www.corporation.org.uk/live/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "corporation_live.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         502,
					SourceName: "Corporation Sheffield event detail page",
					SourceURL:  "https://www.corporation.org.uk/event/tyketto/",
					CapturedAt: time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.corporation.org.uk/event/tyketto/",
						FinalURL:    "https://www.corporation.org.uk/event/tyketto/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "corporation_tyketto.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         503,
					SourceName: "Corporation Sheffield event detail page",
					SourceURL:  "https://www.corporation.org.uk/event/frog-lord/",
					CapturedAt: time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.corporation.org.uk/event/frog-lord/",
						FinalURL:    "https://www.corporation.org.uk/event/frog-lord/",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "corporation_frog_lord.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 379, ReplayOptions{Limit: 20})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, CorporationSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 2; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Corporation"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
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
