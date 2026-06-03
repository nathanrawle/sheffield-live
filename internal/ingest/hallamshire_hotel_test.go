package ingest

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestHallamshireHotelCfgFilestringExtractsPublicCalendarURL(t *testing.T) {
	body := readFixture(t, "hallamshire.html")

	got, err := hallamshire_hotel_cfg_filestring("https://hallamshirehotel.pub/", body, 1)
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

	report, err := RunManual(ctx, store, fetcher, Options{Source: HallamshireHotelSource, Limit: 1})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
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
	if got, want := report.Calendars[0].Candidates[0].Location, "Hallamshire Hotel"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got := report.Calendars[0].Candidates[1].Location; got != "" {
		t.Fatalf("missing-location event location = %q, want blank", got)
	}
	allDay := report.Calendars[0].Candidates[2]
	if got, want := allDay.UID, "hallamshire-all-day@google.com"; got != want {
		t.Fatalf("all-day uid = %q, want %q", got, want)
	}
	if got, want := allDay.StartAt, "2026-12-06T19:30:00Z"; got != want {
		t.Fatalf("all-day start = %q, want %q", got, want)
	}
	if !allDay.StartAtInferred || allDay.StartAtBasis != hallamshireHotelAllDayFallbackBasis {
		t.Fatalf("all-day inferred fields = (%v, %q), want fallback basis", allDay.StartAtInferred, allDay.StartAtBasis)
	}
	if got := allDay.EndAt; got != "" {
		t.Fatalf("all-day end = %q, want blank", got)
	}
}

func TestParseHallamshireHotelICSKeepsAllDayMusicWithFallback(t *testing.T) {
	result := ParseHallamshireHotelICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20261205\n" +
		"DTEND;VALUE=DATE:20261206\n" +
		"UID:winter@google.com\n" +
		"SUMMARY:FREE ENTRY GIG: Winter Band\n" +
		"DESCRIPTION:<a href=\"https://www.fatsoma.com/e/winter/winter-band\">Tickets</a>\n" +
		"LOCATION:Hallamshire Hotel\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20260605\n" +
		"DTEND;VALUE=DATE:20260606\n" +
		"UID:summer@google.com\n" +
		"SUMMARY:LIVE: Summer Band\n" +
		"DESCRIPTION:https://www.wegottickets.com/event/700001\n" +
		"LOCATION:Hallamshire Hotel\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	winter := result.Candidates[0]
	if got, want := winter.StartAt, "2026-12-05T19:30:00Z"; got != want {
		t.Fatalf("winter start = %q, want %q", got, want)
	}
	if got, want := winter.URL, "https://www.fatsoma.com/e/winter/winter-band"; got != want {
		t.Fatalf("winter url = %q, want %q", got, want)
	}
	if !winter.StartAtInferred || winter.StartAtBasis != hallamshireHotelAllDayFallbackBasis {
		t.Fatalf("winter inferred fields = (%v, %q), want fallback basis", winter.StartAtInferred, winter.StartAtBasis)
	}
	if !winter.SourceURLSourceIdentityDisabled {
		t.Fatal("winter source URL identity disabled = false, want true")
	}
	if got := winter.EndAt; got != "" {
		t.Fatalf("winter end = %q, want blank", got)
	}

	summer := result.Candidates[1]
	if got, want := summer.StartAt, "2026-06-05T18:30:00Z"; got != want {
		t.Fatalf("summer start = %q, want %q", got, want)
	}
	if got, want := summer.URL, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("summer url = %q, want %q", got, want)
	}
	if !summer.StartAtInferred || summer.StartAtBasis != hallamshireHotelAllDayFallbackBasis {
		t.Fatalf("summer inferred fields = (%v, %q), want fallback basis", summer.StartAtInferred, summer.StartAtBasis)
	}
}

func TestParseHallamshireHotelICSSkipsMultiDayAndNonMusicAllDayRows(t *testing.T) {
	result := ParseHallamshireHotelICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20260724\n" +
		"DTEND;VALUE=DATE:20260726\n" +
		"UID:multi@google.com\n" +
		"SUMMARY:GIG: Multi Day Band\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20260730\n" +
		"DTEND;VALUE=DATE:20260731\n" +
		"UID:quiz@google.com\n" +
		"SUMMARY:QUIZ: Hallamshire Quiz\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20260731\n" +
		"DTEND;VALUE=DATE:20260801\n" +
		"UID:fringe@google.com\n" +
		"SUMMARY:TRAMLINES FRINGE\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 3; got != want {
		t.Fatalf("skips = %d, want %d: %#v", got, want, result.Skips)
	}
	if got, want := result.Skips[0].Reason, "multi-day all-day event"; got != want {
		t.Fatalf("first skip = %q, want %q", got, want)
	}
	for i := 1; i < len(result.Skips); i++ {
		if got, want := result.Skips[i].Reason, "filtered non-music all-day event"; got != want {
			t.Fatalf("skip %d = %q, want %q", i, got, want)
		}
	}
}

func TestParseHallamshireHotelICSLeavesTimedRowsUninferred(t *testing.T) {
	result := ParseHallamshireHotelICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;TZID=Europe/London:20260605T193000\n" +
		"DTEND;TZID=Europe/London:20260605T223000\n" +
		"UID:timed@google.com\n" +
		"SUMMARY:GIG: Timed Band\n" +
		"DESCRIPTION:https://leadmill.co.uk/event/timed-band/\n" +
		"LOCATION:Hallamshire Hotel\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	candidate := result.Candidates[0]
	if got, want := candidate.StartAt, "2026-06-05T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-06-05T21:30:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if candidate.StartAtInferred || candidate.StartAtBasis != "" {
		t.Fatalf("inferred fields = (%v, %q), want unset", candidate.StartAtInferred, candidate.StartAtBasis)
	}
	if got, want := candidate.URL, "https://leadmill.co.uk/event/timed-band/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if !candidate.SourceURLSourceIdentityDisabled {
		t.Fatal("source URL identity disabled = false, want true")
	}
}

func TestParseHallamshireHotelICSIgnoresUntrustedDetailURLs(t *testing.T) {
	result := ParseHallamshireHotelICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;VALUE=DATE:20261205\n" +
		"DTEND;VALUE=DATE:20261206\n" +
		"UID:untrusted@google.com\n" +
		"SUMMARY:GIG: Untrusted Link Band\n" +
		"DESCRIPTION:https://instagram.com/hallamshirehotel\n" +
		"URL:https://tickets.example.test/untrusted\n" +
		"LOCATION:Hallamshire Hotel\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"DTSTART;TZID=Europe/London:20261206T193000\n" +
		"UID:untrusted-timed@google.com\n" +
		"SUMMARY:GIG: Untrusted Timed Link Band\n" +
		"DESCRIPTION:https://instagram.com/hallamshirehotel\n" +
		"URL:https://tickets.example.test/untrusted-timed\n" +
		"LOCATION:Hallamshire Hotel\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	for _, candidate := range result.Candidates {
		if got := candidate.URL; got != "" {
			t.Fatalf("%s url = %q, want blank", candidate.UID, got)
		}
		if candidate.SourceURLSourceIdentityDisabled {
			t.Fatalf("%s source URL identity disabled = true, want false", candidate.UID)
		}
	}
}

func TestParseHallamshireHotelDetailPageUsesStructuredEventData(t *testing.T) {
	detail := ParseHallamshireHotelDetailPage("https://www.fatsoma.com/e/test/hallamshire-band", []byte(`
		<html>
		  <head>
		    <link rel="canonical" href="https://www.fatsoma.com/e/test/hallamshire-band">
		    <script type="application/ld+json">
		      {
		        "@context":"https://schema.org",
		        "@type":"Event",
		        "name":"Hallamshire Band",
		        "url":"https://www.fatsoma.com/e/test/hallamshire-band",
		        "startDate":"2026-10-09T18:00:00Z",
		        "endDate":"2026-10-09T22:00:00Z",
		        "description":"First paragraph.<br><br>Second paragraph.",
		        "image":"https://cdn.example.test/hallamshire.jpg"
		      }
		    </script>
		  </head>
		  <body><h1>Fallback Heading</h1></body>
		</html>
	`))

	if got, want := detail.URL, "https://www.fatsoma.com/e/test/hallamshire-band"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if !detail.DisableSourceURLIdentity {
		t.Fatal("disable source url identity = false, want true")
	}
	if got, want := detail.Summary, "Hallamshire Band"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := detail.StartAt, "2026-10-09T18:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := detail.EndAt, "2026-10-09T22:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if got, want := detail.Description, "First paragraph.\n\nSecond paragraph."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := detail.ImageSourceURL, "https://cdn.example.test/hallamshire.jpg"; got != want {
		t.Fatalf("image = %q, want %q", got, want)
	}
}

func TestParseHallamshireHotelDetailPageTreatsNoOffsetStructuredTimesAsLondon(t *testing.T) {
	detail := ParseHallamshireHotelDetailPage("https://www.fatsoma.com/e/test/summer-band", []byte(`
		<script type="application/ld+json">
		  {
		    "@context":"https://schema.org",
		    "@type":"Event",
		    "name":"Summer Band",
		    "url":"https://www.fatsoma.com/e/test/summer-band",
		    "startDate":"2026-06-05T19:30",
		    "endDate":"2026-06-05T22:30",
		    "description":"Summer band detail."
		  }
		</script>
	`))

	if got, want := detail.StartAt, "2026-06-05T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := detail.EndAt, "2026-06-05T21:30:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
}

func TestParseHallamshireHotelDetailPageFillsStructuredMissingStartFromVisibleDate(t *testing.T) {
	detail := ParseHallamshireHotelDetailPage("https://www.wegottickets.com/event/700002", []byte(`
		<html>
		  <head>
		    <script type="application/ld+json">
		      {
		        "@context":"https://schema.org",
		        "@type":"Event",
		        "name":"Visible Date Band",
		        "url":"https://www.wegottickets.com/event/700002",
		        "description":"Structured description without a start date."
		      }
		    </script>
		  </head>
		  <body>
		    <h1>Visible Date Band</h1>
		    <table>
		      <tr><td>Friday 5th June, 2026</td></tr>
		      <tr><td>7:30pm</td></tr>
		    </table>
		  </body>
		</html>
	`))

	if got, want := detail.StartAt, "2026-06-05T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := detail.Description, "Structured description without a start date."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestParseHallamshireHotelDetailPageParsesWeGotTicketsVisibleDate(t *testing.T) {
	detail := ParseHallamshireHotelDetailPage("https://www.wegottickets.com/event/700001", []byte(`
		<html>
		  <head>
		    <link rel="canonical" href="/event/700001">
		    <meta property="og:image" content="/images/event.jpg">
		  </head>
		  <body>
		    <h1>WeGot Band</h1>
		    <table>
		      <tr><td>Saturday 7th March, 2026</td></tr>
		      <tr><td>7:30pm</td></tr>
		    </table>
		    <h2>Event information</h2>
		    <p>Doors and live music.</p>
		    <h2>Venue information</h2>
		  </body>
		</html>
	`))

	if got, want := detail.URL, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := detail.StartAt, "2026-03-07T19:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := detail.Description, "Doors and live music."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := detail.ImageSourceURL, "https://www.wegottickets.com/images/event.jpg"; got != want {
		t.Fatalf("image = %q, want %q", got, want)
	}
}

func TestMergeHallamshireDetailReplacesOnlyInferredStart(t *testing.T) {
	detail := eventDetailDescription{
		URL:                      "https://www.fatsoma.com/e/test/hallamshire-band",
		URLAliases:               []string{"https://www.fatsoma.com/e/test/hallamshire-band"},
		Summary:                  "Hallamshire Band",
		StartAt:                  "2026-10-09T18:00:00Z",
		EndAt:                    "2026-10-09T22:00:00Z",
		Description:              "Detail description.",
		ImageSourceURL:           "https://cdn.example.test/hallamshire.jpg",
		ImageAlt:                 "Hallamshire Band",
		DisableSourceURLIdentity: true,
		ReplaceInferredStart:     true,
	}
	candidates := []EventCandidate{
		{
			UID:             "inferred@google.com",
			Summary:         "Hallamshire Band",
			URL:             "https://www.fatsoma.com/e/test/hallamshire-band",
			StartAt:         "2026-10-09T18:30:00Z",
			StartAtInferred: true,
			StartAtBasis:    hallamshireHotelAllDayFallbackBasis,
		},
		{
			UID:     "timed@google.com",
			Summary: "Hallamshire Band",
			URL:     "https://www.fatsoma.com/e/test/hallamshire-band",
			StartAt: "2026-10-09T18:30:00Z",
		},
	}

	merged := mergeDetailDescriptions(candidates, []eventDetailDescription{detail})

	inferred := merged[0]
	if got, want := inferred.StartAt, "2026-10-09T18:00:00Z"; got != want {
		t.Fatalf("inferred start = %q, want %q", got, want)
	}
	if inferred.StartAtInferred || inferred.StartAtBasis != "" {
		t.Fatalf("inferred flags = (%v, %q), want cleared", inferred.StartAtInferred, inferred.StartAtBasis)
	}
	if got, want := inferred.EndAt, "2026-10-09T22:00:00Z"; got != want {
		t.Fatalf("inferred end = %q, want %q", got, want)
	}
	if got, want := inferred.Description, "Detail description."; got != want {
		t.Fatalf("inferred description = %q, want %q", got, want)
	}
	if !inferred.SourceURLSourceIdentityDisabled {
		t.Fatal("inferred source URL identity disabled = false, want true")
	}

	timed := merged[1]
	if got, want := timed.StartAt, "2026-10-09T18:30:00Z"; got != want {
		t.Fatalf("timed start = %q, want %q", got, want)
	}
	if timed.StartAtInferred || timed.StartAtBasis != "" {
		t.Fatalf("timed inferred flags = (%v, %q), want unset", timed.StartAtInferred, timed.StartAtBasis)
	}

	noPolicy := mergeDetailDescriptions([]EventCandidate{{
		UID:             "no-policy@google.com",
		Summary:         "No Policy Band",
		URL:             "https://detail.example.test/no-policy",
		StartAt:         "2026-10-09T18:30:00Z",
		StartAtInferred: true,
		StartAtBasis:    hallamshireHotelAllDayFallbackBasis,
	}}, []eventDetailDescription{{
		URL:     "https://detail.example.test/no-policy",
		StartAt: "2026-10-09T18:00:00Z",
		EndAt:   "2026-10-09T22:00:00Z",
	}})
	if got, want := noPolicy[0].StartAt, "2026-10-09T18:30:00Z"; got != want {
		t.Fatalf("no-policy start = %q, want %q", got, want)
	}
	if !noPolicy[0].StartAtInferred || noPolicy[0].StartAtBasis == "" {
		t.Fatalf("no-policy inferred fields = (%v, %q), want preserved", noPolicy[0].StartAtInferred, noPolicy[0].StartAtBasis)
	}
}

func TestRunManualHallamshireHotelEnrichesAllDayFromTrustedDetailPage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)}
	homepageURL := "https://hallamshirehotel.pub/"
	icsURL := "https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics"
	detailURL := "https://www.fatsoma.com/e/test/hallamshire-band"
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
				Body: []byte("BEGIN:VCALENDAR\n" +
					"BEGIN:VEVENT\n" +
					"DTSTART;VALUE=DATE:20261009\n" +
					"DTEND;VALUE=DATE:20261010\n" +
					"UID:detail@google.com\n" +
					"SUMMARY:GIG: Hallamshire Band\n" +
					"DESCRIPTION:https://www.fatsoma.com/e/test/hallamshire-band\n" +
					"LOCATION:Hallamshire Hotel\n" +
					"END:VEVENT\n" +
					"END:VCALENDAR\n"),
				CapturedAt: time.Date(2026, 5, 24, 12, 2, 0, 0, time.UTC),
			},
			detailURL: {
				URL:         detailURL,
				FinalURL:    detailURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<script type="application/ld+json">
					  {
					    "@context":"https://schema.org",
					    "@type":"Event",
					    "name":"Hallamshire Band",
					    "url":"https://www.fatsoma.com/e/test/hallamshire-band",
					    "startDate":"2026-10-09T18:00:00Z",
					    "endDate":"2026-10-09T22:00:00Z",
					    "description":"Detail description. More detail.",
					    "image":"https://cdn.example.test/hallamshire.jpg"
					  }
					</script>
				`),
				CapturedAt: time.Date(2026, 5, 24, 12, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: HallamshireHotelSource, Limit: 1})
	if err != nil {
		t.Fatalf("run manual: %v; report = %#v", err, report)
	}

	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	candidate := report.Calendars[0].Candidates[0]
	if got, want := candidate.StartAt, "2026-10-09T18:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if candidate.StartAtInferred || candidate.StartAtBasis != "" {
		t.Fatalf("inferred fields = (%v, %q), want cleared", candidate.StartAtInferred, candidate.StartAtBasis)
	}
	if got, want := candidate.EndAt, "2026-10-09T22:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if got, want := candidate.Description, "Detail description. More detail."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := candidate.ImageSourceURL, "https://cdn.example.test/hallamshire.jpg"; got != want {
		t.Fatalf("image = %q, want %q", got, want)
	}
	if got, want := candidate.URL, detailURL; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if !candidate.SourceURLSourceIdentityDisabled {
		t.Fatal("source URL identity disabled = false, want true")
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
						Location: "Hallamshire Hotel",
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

func TestReviewStageHallamshireHotelDefaultsMissingLocationToOwnedVenue(t *testing.T) {
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
						UID:     "hallamshire-no-location@google.com",
						Summary: "GIG: No Location Example",
						StartAt: "2026-12-06T19:00:00Z",
						Status:  "CONFIRMED",
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
	if got := cluster.Candidates[0].VenueText; got != "" {
		t.Fatalf("venue text = %q, want blank", got)
	}
	if got := cluster.Candidates[0].VenueLocationRaw; got != "" {
		t.Fatalf("venue location raw = %q, want blank", got)
	}
	if got, want := cluster.AuthoritativeSourceName, "Hallamshire Hotel manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceURL, "https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972%40group.calendar.google.com/public/basic.ics"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := cluster.AuthoritativeSourceEventKey, "uid:hallamshire-no-location@google.com"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestReviewStageHallamshireHotelDoesNotDefaultExplicitWrongLocation(t *testing.T) {
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
						UID:      "hallamshire-wrong-location@google.com",
						Summary:  "GIG: Wrong Location Example",
						Location: "Wrong Hall",
						StartAt:  "2026-12-06T19:00:00Z",
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
	if got, want := cluster.Candidates[0].VenueSlug, "wrong-hall"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if cluster.AuthoritativeSourceName != "" || cluster.AuthoritativeSourceURL != "" || cluster.AuthoritativeSourceEventKey != "" {
		t.Fatalf("authoritative source = (%q, %q, %q), want empty", cluster.AuthoritativeSourceName, cluster.AuthoritativeSourceURL, cluster.AuthoritativeSourceEventKey)
	}
}

func TestReviewStageHallamshireHotelDoesNotDefaultExplicitUnnormalizableLocation(t *testing.T) {
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
						UID:      "hallamshire-unnormalizable-location@google.com",
						Summary:  "GIG: Unnormalizable Location Example",
						Location: "!!!",
						StartAt:  "2026-12-06T19:00:00Z",
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
	if got := cluster.Candidates[0].VenueSlug; got != "" {
		t.Fatalf("venue slug = %q, want blank", got)
	}
	if cluster.AuthoritativeSourceName != "" || cluster.AuthoritativeSourceURL != "" || cluster.AuthoritativeSourceEventKey != "" {
		t.Fatalf("authoritative source = (%q, %q, %q), want empty", cluster.AuthoritativeSourceName, cluster.AuthoritativeSourceURL, cluster.AuthoritativeSourceEventKey)
	}
}
