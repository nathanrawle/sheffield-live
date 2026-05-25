package ingest

import (
	"context"
	"strings"
	"testing"
	"time"
)

const theWashingtonExpectedAPIURLLimit10 = "https://www.googleapis.com/calendar/v3/calendars/c_u2bs6ittml6rm5k0l5qjt3pn1o%40group.calendar.google.com/events?key=AIzaSyAVR1AfCKKfQrAZj_ErEY5UEy0QLG0AHAc&singleEvents=true&orderBy=startTime&maxResults=10&timeMin=2026-05-24T23%3A00%3A00Z"

func freezeTheWashingtonClock(t *testing.T) {
	t.Helper()
	old := theWashingtonNow
	theWashingtonNow = func() time.Time {
		return time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	}
	t.Cleanup(func() {
		theWashingtonNow = old
	})
}

func TestTheWashingtonDiscoveryFixturesPointToTheCalendarPage(t *testing.T) {
	freezeTheWashingtonClock(t)

	home := string(readFixture(t, "the_washington_home.html"))
	if !strings.Contains(home, `href="/events"`) {
		t.Fatalf("home fixture does not link to /events")
	}

	events := string(readFixture(t, "the_washington_events.html"))
	if !strings.Contains(events, `src="/cal.html"`) {
		t.Fatalf("events fixture does not embed /cal.html")
	}

	// The official events page embeds /cal.html, and that page exposes the
	// public Google Calendar config used to derive the delegated API URL.
	links, err := the_washington_api_links("https://thewashington.pub/cal.html", readFixture(t, "the_washington_cal.html"), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	if got, wantLen := len(links), 1; got != wantLen {
		t.Fatalf("links = %d, want %d", got, wantLen)
	}
	if got := links[0]; got != theWashingtonExpectedAPIURLLimit10 {
		t.Fatalf("api url = %q, want %q", got, theWashingtonExpectedAPIURLLimit10)
	}
}

func TestParseTheWashingtonAPIDetailPageParsesMusicEventAndSkipsUnsupportedItems(t *testing.T) {
	result := ParseTheWashingtonAPIDetailPage(theWashingtonExpectedAPIURLLimit10, readFixture(t, "the_washington_api.json"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 4; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.Summary, "[DJ] FIESTA!"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, theWashingtonVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.LocationRaw, "The Washington, 79 Fitzwilliam St, Sheffield City Centre, Sheffield S1 4JP, UK"; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
	if got, want := candidate.URL, "https://www.google.com/calendar/event?eid=Xzhrc2o2aDlrNnNxM2diYTY2NHBqZWI5azg1MTQyYmExOGNxMzJiOW42NHBrMmg5bzhoMGs0Z3E0OGtfMjAyNjA1MjZUMjAwMDAwWiBjX3UyYnM2aXR0bWw2cm01azBsNXFqdDNwbjFvQGc"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := candidate.StartAt, "2026-05-26T20:00:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-05-27T02:00:00Z"; got != want {
		t.Fatalf("end at = %q, want %q", got, want)
	}
	if got, want := result.Skips[0].Reason, "all-day event"; got != want {
		t.Fatalf("first skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "non-music event"; got != want {
		t.Fatalf("second skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[2].Reason, "missing venue evidence"; got != want {
		t.Fatalf("third skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[3].Reason, "unsupported venue"; got != want {
		t.Fatalf("fourth skip reason = %q, want %q", got, want)
	}
}

func TestParseTheWashingtonAPIDetailPageHonorsRequestedWindowAndMaxResults(t *testing.T) {
	pageURL := strings.Replace(theWashingtonExpectedAPIURLLimit10, "maxResults=10", "maxResults=1", 1)
	result := ParseTheWashingtonAPIDetailPage(pageURL, []byte(`{
  "items": [
    {
      "id": "stale-dj",
      "status": "confirmed",
      "htmlLink": "https://www.google.com/calendar/event?eid=stale",
      "summary": "[DJ] Stale Night",
      "description": "DJ set before the requested window.",
      "location": "The Washington, 79 Fitzwilliam St, Sheffield",
      "start": {"dateTime": "2026-05-24T20:00:00+01:00"},
      "end": {"dateTime": "2026-05-24T22:00:00+01:00"},
      "iCalUID": "stale-dj@google.com"
    },
    {
      "id": "current-dj-1",
      "status": "confirmed",
      "htmlLink": "https://www.google.com/calendar/event?eid=current1",
      "summary": "[DJ] Current One",
      "description": "DJ set inside the requested window.",
      "location": "The Washington, 79 Fitzwilliam St, Sheffield",
      "start": {"dateTime": "2026-05-25T20:00:00+01:00"},
      "end": {"dateTime": "2026-05-25T22:00:00+01:00"},
      "iCalUID": "current-dj-1@google.com"
    },
    {
      "id": "current-dj-2",
      "status": "confirmed",
      "htmlLink": "https://www.google.com/calendar/event?eid=current2",
      "summary": "[DJ] Current Two",
      "description": "Second DJ set inside the requested window.",
      "location": "The Washington, 79 Fitzwilliam St, Sheffield",
      "start": {"dateTime": "2026-05-25T21:00:00+01:00"},
      "end": {"dateTime": "2026-05-25T23:00:00+01:00"},
      "iCalUID": "current-dj-2@google.com"
    }
  ]
}`))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].Summary, "[DJ] Current One"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "event before requested time window"; got != want {
		t.Fatalf("first skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "candidate limit reached"; got != want {
		t.Fatalf("second skip reason = %q, want %q", got, want)
	}
}

func TestRunManualTheWashingtonDiscoversAndParsesCalendarAPI(t *testing.T) {
	freezeTheWashingtonClock(t)

	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)}
	calURL := "https://thewashington.pub/cal.html"
	apiURL := theWashingtonExpectedAPIURLLimit10
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			calURL: {
				URL:         calURL,
				FinalURL:    calURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "the_washington_cal.html"),
				CapturedAt:  time.Date(2026, 5, 25, 12, 1, 0, 0, time.UTC),
			},
			apiURL: {
				URL:         apiURL,
				FinalURL:    apiURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "application/json",
				Body:        readFixture(t, "the_washington_api.json"),
				CapturedAt:  time.Date(2026, 5, 25, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: TheWashingtonSource, Limit: 10})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 4; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].URL, apiURL; got != want {
		t.Fatalf("calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, theWashingtonVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].URL, "https://www.google.com/calendar/event?eid=Xzhrc2o2aDlrNnNxM2diYTY2NHBqZWI5azg1MTQyYmExOGNxMzJiOW42NHBrMmg5bzhoMGs0Z3E0OGtfMjAyNjA1MjZUMjAwMDAwWiBjX3UyYnM2aXR0bWw2cm01azBsNXFqdDNwbjFvQGc"; got != want {
		t.Fatalf("candidate url = %q, want %q", got, want)
	}
}

func TestReviewStageTheWashingtonIsAuthoritativeOwnedVenue(t *testing.T) {
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	report := Report{
		Source:      TheWashingtonSource,
		SourceURL:   "https://thewashington.pub/cal.html",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: theWashingtonExpectedAPIURLLimit10,
				Candidates: []EventCandidate{
					{
						UID:      "E93E4748-F137-4ABA-AC41-713AE8DABCDE",
						Summary:  "[DJ] FIESTA!",
						Location: theWashingtonVenueName,
						URL:      "https://www.google.com/calendar/event?eid=Xzhrc2o2aDlrNnNxM2diYTY2NHBqZWI5azg1MTQyYmExOGNxMzJiOW42NHBrMmg5bzhoMGs0Z3E0OGtfMjAyNjA1MjZUMjAwMDAwWiBjX3UyYnM2aXR0bWw2cm01azBsNXFqdDNwbjFvQGc",
						StartAt:  "2026-05-26T20:00:00Z",
						EndAt:    "2026-05-27T02:00:00Z",
						Status:   "CONFIRMED",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReportWithCatalog(catalog, report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}

	cluster := clusters[0]
	if got, want := cluster.Candidates[0].VenueSlug, TheWashingtonSource; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceName, "The Washington manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceURL, "https://www.google.com/calendar/event?eid=Xzhrc2o2aDlrNnNxM2diYTY2NHBqZWI5azg1MTQyYmExOGNxMzJiOW42NHBrMmg5bzhoMGs0Z3E0OGtfMjAyNjA1MjZUMjAwMDAwWiBjX3UyYnM2aXR0bWw2cm01azBsNXFqdDNwbjFvQGc"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceEventKey, "uid:E93E4748-F137-4ABA-AC41-713AE8DABCDE"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsTheWashingtonReportFromCalendarAPI(t *testing.T) {
	freezeTheWashingtonClock(t)

	finishedAt := time.Date(2026, 5, 25, 12, 30, 0, 0, time.UTC)
	calURL := "https://thewashington.pub/cal.html"
	apiURL := theWashingtonExpectedAPIURLLimit10
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         501,
			StartedAt:  time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=1 candidates=1 skips=4 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         701,
					SourceName: "The Washington listings",
					SourceURL:  calURL,
					CapturedAt: time.Date(2026, 5, 25, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         calURL,
						FinalURL:    calURL,
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "the_washington_cal.html"),
						CapturedAt:  time.Date(2026, 5, 25, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         702,
					SourceName: "The Washington Google Calendar API",
					SourceURL:  apiURL,
					CapturedAt: time.Date(2026, 5, 25, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         apiURL,
						FinalURL:    apiURL,
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "application/json",
						Body:        readFixture(t, "the_washington_api.json"),
						CapturedAt:  time.Date(2026, 5, 25, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 501, ReplayOptions{Limit: 10})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, TheWashingtonSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, calURL; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].URL, apiURL; got != want {
		t.Fatalf("calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, theWashingtonVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Totals.Snapshots, 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 4; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}
