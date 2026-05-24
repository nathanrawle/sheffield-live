package ingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestExtractCrookesClubSecondaryPages(t *testing.T) {
	got, err := ExtractCrookesClubSecondaryPages("https://crookesclub.co.uk/", readFixture(t, "crookes_club_home.html"), 10)
	if err != nil {
		t.Fatalf("extract secondary pages: %v", err)
	}

	want := []string{"https://crookesclub.co.uk/lounge-live-music"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractCrookesClubSecondaryPagesIncludesKnownLoungePage(t *testing.T) {
	got, err := ExtractCrookesClubSecondaryPages("https://crookesclub.co.uk/", []byte(`<html><body>No lounge nav today</body></html>`), 10)
	if err != nil {
		t.Fatalf("extract secondary pages: %v", err)
	}

	want := []string{"https://crookesclub.co.uk/lounge-live-music"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseCrookesClubSourcePageParsesHomepageMusicEventAndSkipsNonMusic(t *testing.T) {
	result := ParseCrookesClubSourcePage("https://crookesclub.co.uk/", readFixture(t, "crookes_club_home.html"), 20)

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.Summary, "Kat Eaton"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, crookesClubVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.RoomText, crookesClubConcertRoomName; got != want {
		t.Fatalf("room text = %q, want %q", got, want)
	}
	if got, want := candidate.Rooms, []RoomCandidate{{Slug: "concert-room", Name: crookesClubConcertRoomName}}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rooms = %#v, want %#v", got, want)
	}
	if got, want := candidate.StartAt, "2026-10-03T18:00:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := candidate.UID, "crookes-club|concert-room|kat eaton|2026-10-03T18:00:00Z"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.URL, "https://crookesclub.co.uk/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := result.Skips[0].Reason, "comedy"; got != want {
		t.Fatalf("first skip = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "non-music social event"; got != want {
		t.Fatalf("second skip = %q, want %q", got, want)
	}
}

func TestParseCrookesClubSourcePageParsesLoungeArtistesWithDefaultTime(t *testing.T) {
	result := ParseCrookesClubSourcePage("https://crookesclub.co.uk/lounge-live-music", readFixture(t, "crookes_club_lounge.html"), 20)

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "missing deterministic year"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.Summary, "Thomas Marx"; got != want {
		t.Fatalf("first summary = %q, want %q", got, want)
	}
	if got, want := first.RoomText, crookesClubLoungeRoomName; got != want {
		t.Fatalf("first room text = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2026-01-03T20:45:00Z"; got != want {
		t.Fatalf("first start at = %q, want %q", got, want)
	}
	if got, want := first.UID, "crookes-club|lounge|thomas marx|2026-01-03T20:45:00Z"; got != want {
		t.Fatalf("first uid = %q, want %q", got, want)
	}

	second := result.Candidates[1]
	if got, want := second.Summary, "John C Morgan"; got != want {
		t.Fatalf("second summary = %q, want %q", got, want)
	}
	if got, want := second.StartAt, "2026-01-10T20:45:00Z"; got != want {
		t.Fatalf("second start at = %q, want %q", got, want)
	}
}

func TestRunManualCrookesClubParsesHomepageAndLoungePages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)}
	homepageURL := "https://crookesclub.co.uk/"
	loungeURL := "https://crookesclub.co.uk/lounge-live-music"
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			homepageURL: {
				URL:         homepageURL,
				FinalURL:    homepageURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "crookes_club_home.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 1, 0, 0, time.UTC),
			},
			loungeURL: {
				URL:         loungeURL,
				FinalURL:    loungeURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "crookes_club_lounge.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CrookesClubSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := report.Links[0], loungeURL; got != want {
		t.Fatalf("link = %q, want %q", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 3; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}

	if got, want := report.Calendars[0].URL, homepageURL; got != want {
		t.Fatalf("homepage calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].RoomText, crookesClubConcertRoomName; got != want {
		t.Fatalf("homepage room text = %q, want %q", got, want)
	}
	if got, want := report.Calendars[1].URL, loungeURL; got != want {
		t.Fatalf("lounge calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[1].Candidates[0].RoomText, crookesClubLoungeRoomName; got != want {
		t.Fatalf("lounge room text = %q, want %q", got, want)
	}
}

func TestReviewStageCrookesClubAuthoritativeSourceAndRoomEvidence(t *testing.T) {
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}

	report := Report{
		Source:      CrookesClubSource,
		SourceURL:   "https://crookesclub.co.uk/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://crookesclub.co.uk/",
				Candidates: []EventCandidate{
					{
						UID:         "crookes-club|concert-room|kat eaton|2026-10-03T18:00:00Z",
						Summary:     "Kat Eaton",
						Location:    crookesClubVenueName,
						LocationRaw: crookesClubVenueName,
						RoomText:    crookesClubConcertRoomName,
						Rooms:       []RoomCandidate{{Slug: "concert-room", Name: crookesClubConcertRoomName}},
						URL:         "https://crookesclub.co.uk/",
						Status:      "Listed",
						StartAt:     "2026-10-03T18:00:00Z",
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
	if got, want := cluster.Candidates[0].VenueSlug, CrookesClubSource; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceName, "Crookes Club manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceURL, "https://crookesclub.co.uk/"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceEventKey, "uid:crookes-club|concert-room|kat eaton|2026-10-03T18:00:00Z"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}

	inputs := ReviewStageClusterEventReviewEvidenceInputs(cluster)
	if got, want := len(inputs), 1; got != want {
		t.Fatalf("review evidence inputs = %d, want %d", got, want)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(inputs[0].Payload), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got, want := payload["group_authoritative_source_name"], "Crookes Club manual ingest"; got != want {
		t.Fatalf("payload authoritative source name = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_room_text"], crookesClubConcertRoomName; got != want {
		t.Fatalf("payload room text = %#v, want %#v", got, want)
	}
	rooms, ok := payload["candidate_rooms"].([]any)
	if !ok || len(rooms) != 1 {
		t.Fatalf("payload candidate rooms = %#v, want one room", payload["candidate_rooms"])
	}
	room, ok := rooms[0].(map[string]any)
	if !ok {
		t.Fatalf("payload room = %#v, want object", rooms[0])
	}
	if got, want := room["venue_slug"], CrookesClubSource; got != want {
		t.Fatalf("payload room venue slug = %#v, want %#v", got, want)
	}
	if got, want := room["slug"], "concert-room"; got != want {
		t.Fatalf("payload room slug = %#v, want %#v", got, want)
	}
	if got, want := payload["source_authority"], "authoritative"; got != want {
		t.Fatalf("payload source authority = %#v, want %#v", got, want)
	}
}
