package ingest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestHagglersCornerDetailLinksExtractDetailPosts(t *testing.T) {
	got, err := hagglers_corner_detail_links("https://hagglerscorner.co.uk/category/events-gigs/", readFixture(t, "hagglers_corner_events.html"), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/",
		"https://hagglerscorner.co.uk/hagglers-monthly-quiz-night/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseHagglersCornerDetailPageParsesMusicEvent(t *testing.T) {
	result := ParseHagglersCornerDetailPage("https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/", readFixture(t, "hagglers_corner_detail_music.html"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Skips), 0; got != want {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.Summary, "ATTIKK PRESENTS: AMEN TO THAT LAUNCH PARTY WITH SMILEY MAXX"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, hagglersCornerVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.LocationRaw, hagglersCornerVenueEvidence; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
	if got, want := candidate.StartAt, "2026-04-24T20:00:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-04-25T01:30:00Z"; got != want {
		t.Fatalf("end at = %q, want %q", got, want)
	}
	if got, want := candidate.UID, "https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.URL, "https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
}

func TestParseHagglersCornerDetailPageRejectsNonMusicEvents(t *testing.T) {
	tests := []struct {
		name       string
		title      string
		body       string
		wantReason string
	}{
		{
			name:       "quiz",
			title:      "Hagglers Monthly Quiz Night!",
			body:       `<p>SECOND WEDNESDAY OF THE MONTH | FROM 7:30PM | FREE ENTRY</p>`,
			wantReason: "non-music event",
		},
		{
			name:       "market",
			title:      "Hagglers Christmas Market",
			body:       `<p>Sunday 14th December 2025, 11am - 4pm</p>`,
			wantReason: "non-music event",
		},
		{
			name:       "workshop",
			title:      "Macrame Workshop",
			body:       `<p>Fri 24th April 2026, 6pm</p>`,
			wantReason: "non-music event",
		},
		{
			name:       "private",
			title:      "Private Hire Showcase",
			body:       `<p>Sat 18th April 2026, 7pm</p>`,
			wantReason: "non-music event",
		},
		{
			name:       "community",
			title:      "Community Fundraiser",
			body:       `<p>Fri 24th April 2026, 7pm</p>`,
			wantReason: "non-music event",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseHagglersCornerDetailPage("https://hagglerscorner.co.uk/"+tc.name+"/", hagglersCornerDetailHTML(tc.title, tc.body))
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Candidates), 0; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			if got, want := len(result.Skips), 1; got != want {
				t.Fatalf("skips = %#v, want 1", result.Skips)
			}
			if got, want := result.Skips[0].Reason, tc.wantReason; got != want {
				t.Fatalf("skip reason = %q, want %q", got, want)
			}
		})
	}
}

func TestParseHagglersCornerDetailPageSkipsAggregateMonthlyPost(t *testing.T) {
	result := ParseHagglersCornerDetailPage("https://hagglerscorner.co.uk/whats-on-this-april/", hagglersCornerDetailHTML("WHATS ON THIS APRIL", `
		<p>WEDNESDAY 1ST | ATTIKK | 7:30PM</p>
		<p>MONTHLY COMEDY NIGHT | NO HAGGLING ALL COMEDY</p>
		<p>FREE/PAYF</p>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "aggregate monthly post"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseHagglersCornerDetailPageRejectsDateWithoutMusic(t *testing.T) {
	result := ParseHagglersCornerDetailPage("https://hagglerscorner.co.uk/an-evening-at-hagglers/", hagglersCornerDetailHTML("An Evening at Hagglers", `
		<p>Fri 24th April 2026, 7pm</p>
		<p>Hagglers Corner, 586 Queens Road, Sheffield</p>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "missing music signal"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseHagglersCornerDetailPageRejectsMusicWithoutDate(t *testing.T) {
	result := ParseHagglersCornerDetailPage("https://hagglerscorner.co.uk/warm-up-dj-set/", hagglersCornerDetailHTML("ATTIKK PRESENTS: Warm Up DJ Set", `
		<p>House / Disco / Lost Classics</p>
		<p>DJ warm up until late.</p>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "missing event date"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestRunManualHagglersCornerFetchesCategoryThenDetails(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)}
	categoryURL := "https://hagglerscorner.co.uk/category/events-gigs/"
	musicURL := "https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/"
	quizURL := "https://hagglerscorner.co.uk/hagglers-monthly-quiz-night/"
	calls := []string{}
	fetcher := fakeFetcher{
		calls: &calls,
		results: map[string]FetchResult{
			categoryURL: {
				URL:         categoryURL,
				FinalURL:    categoryURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "hagglers_corner_events.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
			},
			musicURL: {
				URL:         musicURL,
				FinalURL:    musicURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "hagglers_corner_detail_music.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
			},
			quizURL: {
				URL:         quizURL,
				FinalURL:    quizURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "hagglers_corner_detail_quiz.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: HagglersCornerSource, Limit: 10})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	wantCalls := []string{categoryURL, musicURL, quizURL}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("fetch calls = %#v, want %#v", calls, wantCalls)
	}
	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 2; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, hagglersCornerVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].StartAt, "2026-04-24T20:00:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].EndAt, "2026-04-25T01:30:00Z"; got != want {
		t.Fatalf("end at = %q, want %q", got, want)
	}
	if got, want := report.Calendars[1].Skips[0].Reason, "non-music event"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestReviewStageHagglersCornerIsAuthoritativeOwnedVenue(t *testing.T) {
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}

	detailURL := "https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/"
	report := Report{
		Source:      HagglersCornerSource,
		SourceURL:   "https://hagglerscorner.co.uk/category/events-gigs/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: detailURL,
				Candidates: []EventCandidate{
					{
						UID:         detailURL,
						Summary:     "ATTIKK PRESENTS: AMEN TO THAT LAUNCH PARTY WITH SMILEY MAXX",
						Location:    hagglersCornerVenueName,
						LocationRaw: hagglersCornerVenueEvidence,
						URL:         detailURL,
						Status:      "Listed",
						StartAt:     "2026-04-24T20:00:00Z",
						EndAt:       "2026-04-25T01:30:00Z",
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
	if got, want := cluster.Candidates[0].VenueSlug, HagglersCornerSource; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceName, "Hagglers Corner manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceURL, detailURL; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if cluster.AuthoritativeSourceEventKey == "" {
		t.Fatal("authoritative source event key is empty")
	}
}

func TestReplayImportRunRebuildsHagglersCornerReportFromLinkedDetailSnapshots(t *testing.T) {
	finishedAt := time.Date(2026, 4, 24, 12, 30, 0, 0, time.UTC)
	categoryURL := "https://hagglerscorner.co.uk/category/events-gigs/"
	musicURL := "https://hagglerscorner.co.uk/attikk-presents-amen-to-that-launch-party-with-smiley-maxx/"
	quizURL := "https://hagglerscorner.co.uk/hagglers-monthly-quiz-night/"
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         314,
			StartedAt:  time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=2 candidates=1 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         401,
					SourceName: "Hagglers Corner events & gigs",
					SourceURL:  categoryURL,
					CapturedAt: time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         categoryURL,
						FinalURL:    categoryURL,
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "hagglers_corner_events.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         402,
					SourceName: "Hagglers Corner event detail page",
					SourceURL:  musicURL,
					CapturedAt: time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         musicURL,
						FinalURL:    musicURL,
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "hagglers_corner_detail_music.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         403,
					SourceName: "Hagglers Corner event detail page",
					SourceURL:  quizURL,
					CapturedAt: time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         quizURL,
						FinalURL:    quizURL,
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        readFixture(t, "hagglers_corner_detail_quiz.html"),
						CapturedAt:  time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 314, ReplayOptions{Limit: 10})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := report.Source, HagglersCornerSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := report.SourceURL, categoryURL; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 2; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].StartAt, "2026-04-24T20:00:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].EndAt, "2026-04-25T01:30:00Z"; got != want {
		t.Fatalf("end at = %q, want %q", got, want)
	}
	if got, want := report.Calendars[1].Skips[0].Reason, "non-music event"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
	if got, want := report.Totals.Candidates, 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Totals.Snapshots, 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
}

func hagglersCornerDetailHTML(title, body string) []byte {
	return []byte(fmt.Sprintf(`<!doctype html>
<html lang="en-GB">
  <head>
    <title>%s | Hagglers Corner</title>
  </head>
  <body>
    <article>
      <h1>%s</h1>
      %s
      <p>The venue – Hagglers Corner – is a much-loved independent venue in Sheffield. 586 Queens Road, Sheffield.</p>
    </article>
  </body>
</html>`, title, title, body))
}
