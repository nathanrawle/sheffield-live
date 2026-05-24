package ingest

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestHallamshireHotelCfgFilestringExtractsPublicCalendarURL(t *testing.T) {
	body := readFixture(t, "hallamshire.html")

	got, err := hallamshire_hotel_cfg_filestring("https://hallamshirehotel.pub/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{"https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestRunManualHallamshireHotelDiscoversAndParsesICS(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)}
	homepageURL := "https://hallamshirehotel.pub/"
	icsURL := "https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics"
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			homepageURL: {
				URL:         homepageURL,
				FinalURL:    homepageURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "hallamshire.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 1, 0, 0, time.UTC),
			},
			icsURL: {
				URL:         icsURL,
				FinalURL:    icsURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/calendar",
				Body:        readFixture(t, "hallamshire.ics"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: HallamshireHotelSource, Limit: 10})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].URL, icsURL; got != want {
		t.Fatalf("calendar url = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, hallamshireHotelDisplayName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestReviewStageAuthoritativeSourceForHallamshireHotel(t *testing.T) {
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	report := Report{
		Source:      HallamshireHotelSource,
		SourceURL:   "https://hallamshirehotel.pub/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics",
				Candidates: []EventCandidate{
					{
						UID:      "hallamshire-one@google.com",
						Summary:  "GIG: Hallamshire Example",
						Location: hallamshireHotelDisplayName,
						StartAt:  "2026-12-05T00:00:00Z",
						EndAt:    "2026-12-06T00:00:00Z",
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
	if got, want := cluster.Candidates[0].VenueSlug, HallamshireHotelSource; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceName, "Hallamshire Hotel manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceURL, "https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceEventKey, "uid:hallamshire-one@google.com"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}
