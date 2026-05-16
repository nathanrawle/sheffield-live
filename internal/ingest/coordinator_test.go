package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunManualSnapshotsAndReportsWithoutEventWrites(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:         "https://www.sidneyandmatilda.com/",
				FinalURL:    "https://www.sidneyandmatilda.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "sidney.html"),
				CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
			"https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live": {
				URL:         "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				FinalURL:    "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/calendar",
				Body:        readFixture(t, "sidney.ics"),
				CapturedAt:  time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 1})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if report.Status != importStatusSucceeded {
		t.Fatalf("status = %q, want %q", report.Status, importStatusSucceeded)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 4; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := store.eventWrites, 0; got != want {
		t.Fatalf("event writes = %d, want %d", got, want)
	}

	var envelope SnapshotEnvelope
	if err := json.Unmarshal([]byte(store.snapshots[0].payload), &envelope); err != nil {
		t.Fatalf("unmarshal snapshot envelope: %v", err)
	}
	if envelope.Body == "" || envelope.SHA256 == "" {
		t.Fatalf("snapshot envelope missing body/sha: %#v", envelope)
	}
}

func TestRunManualYellowArchParsesListingsFromSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.yellowarch.com/events/": {
				URL:         "https://www.yellowarch.com/events/",
				FinalURL:    "https://www.yellowarch.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "yellow_arch.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: yellowArchSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Yellow Arch Studios"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Description, "Doors 7pm. Live from the courtyard bar."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := store.finishedNotes, "links=0 candidates=2 skips=1 errors=0"; got != want {
		t.Fatalf("finished notes = %q, want %q", got, want)
	}
}

func TestRunManualYellowArchPrefersDetailPageDescriptions(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 4, 22, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.yellowarch.com/events/": {
				URL:         "https://www.yellowarch.com/events/",
				FinalURL:    "https://www.yellowarch.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<script type="application/ld+json">
					  [{
					    "@context":"https://schema.org",
					    "@type":"Event",
					    "name":"Jazz Hot Six",
					    "url":"/event/jazz-hot-six-14/",
					    "description":"Martin Winning - ClarinetEmily Chaplais - Violin",
					    "startDate":"2026-05-05T19:00",
					    "endDate":"2026-05-05T22:00",
					    "location":{"name":"Yellow Arch Studios"}
					  }]
					</script>
				`),
				CapturedAt: time.Date(2026, 5, 4, 22, 1, 0, 0, time.UTC),
			},
			"https://www.yellowarch.com/event/jazz-hot-six-14/": {
				URL:         "https://www.yellowarch.com/event/jazz-hot-six-14/",
				FinalURL:    "https://www.yellowarch.com/event/jazz-hot-six-14/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<html>
					  <head><link rel="canonical" href="/event/jazz-hot-six-14/"></head>
					  <body>
					    <h1>Jazz Hot Six</h1>
					    <div class="event-single__content">
					      <p>Martin Winning - Clarinet<br />Emily Chaplais - Violin</p>
					      <div class="event-single__venue-address-wrapper">Yellow Arch Studios</div>
					    </div>
					  </body>
					</html>
				`),
				CapturedAt: time.Date(2026, 5, 4, 22, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: yellowArchSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := report.Totals.Snapshots, 2; got != want {
		t.Fatalf("report snapshots = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Description, "Martin Winning - Clarinet\nEmily Chaplais - Violin"; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestRunManualYellowArchFallsBackWhenDetailDescriptionIsEmpty(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 4, 22, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.yellowarch.com/events/": {
				URL:         "https://www.yellowarch.com/events/",
				FinalURL:    "https://www.yellowarch.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<script type="application/ld+json">
					  [{
					    "@context":"https://schema.org",
					    "@type":"Event",
					    "name":"Jazz Hot Six",
					    "url":"/event/jazz-hot-six-14/",
					    "description":"Source page description.",
					    "startDate":"2026-05-05T19:00",
					    "endDate":"2026-05-05T22:00",
					    "location":{"name":"Yellow Arch Studios"}
					  }]
					</script>
				`),
				CapturedAt: time.Date(2026, 5, 4, 22, 1, 0, 0, time.UTC),
			},
			"https://www.yellowarch.com/event/jazz-hot-six-14/": {
				URL:         "https://www.yellowarch.com/event/jazz-hot-six-14/",
				FinalURL:    "https://www.yellowarch.com/event/jazz-hot-six-14/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        []byte(`<div class="event-single__content"><p>Better description.</p></div>`),
				CapturedAt:  time.Date(2026, 5, 4, 22, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: yellowArchSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Description, "Source page description."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestRunManualCafeNo9ParsesListingsFromSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.wegottickets.com/Cafe9": {
				URL:         "https://www.wegottickets.com/Cafe9",
				FinalURL:    "https://www.wegottickets.com/Cafe9",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_page.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CafeNo9Source, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Summary, "Ellie Gowers"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Cafe No9"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].UID, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := store.finishedNotes, "links=0 candidates=2 skips=2 errors=0"; got != want {
		t.Fatalf("finished notes = %q, want %q", got, want)
	}
}

func TestRunManualCafeNo9PrefersDetailPageDescriptions(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.wegottickets.com/Cafe9": {
				URL:         "https://www.wegottickets.com/Cafe9",
				FinalURL:    "https://www.wegottickets.com/Cafe9",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<h2><a href="/event/700004">An evening with Gideon Conn at Cafe No. 9</a></h2>
					<p>Short listing teaser.</p>
					<p>0 SHEFFIELD: Cafe No. 9</p>
					<p>P Thursday 5th November, 2026</p>
					<p>N Door time: 7:00pm, Start time: 7:30pm</p>
					<p>C Music - General</p>
					<p><a href="/event/700004">Event info</a></p>
				`),
				CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
			"https://www.wegottickets.com/event/700004": {
				URL:         "https://www.wegottickets.com/event/700004",
				FinalURL:    "https://wegottickets.com/f/15737/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_detail.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CafeNo9Source, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Description, "The Leisure Society were founded by Nick Hemming.\n\nExpect oustanding songwriting and production craft."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestRunManualCafeNo9MergesRelativeCanonicalDetailPageDescription(t *testing.T) {
	detail := ParseCafeNo9DetailPage("https://www.wegottickets.com/event/667102", []byte(`
		<html>
		  <body>
		    <link rel="canonical" href="/f/15737/">
		    <h1>An evening with Gideon Conn at Cafe No. 9</h1>
		    <h2>Event information</h2>
		    <main>
		      Gideon Conn is back at Cafe No. 9.<br>
		      <br>
		      Tickets available now.
		    </main>
		  </body>
		</html>
	`))
	if got, want := detail.URLAliases, []string{"https://www.wegottickets.com/f/15737/"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("url aliases = %#v, want %#v", got, want)
	}

	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.wegottickets.com/Cafe9": {
				URL:         "https://www.wegottickets.com/Cafe9",
				FinalURL:    "https://www.wegottickets.com/Cafe9",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<h2><a href="/event/667102">An evening with Gideon Conn at Cafe No. 9</a></h2>
					<p>0 SHEFFIELD: Cafe No. 9</p>
					<p>P Thursday 5th November, 2026</p>
					<p>N Door time: 7:00pm, Start time: 7:30pm</p>
					<p>C Music - General</p>
					<p><a href="/event/667102">Event info</a></p>
				`),
				CapturedAt: time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
			"https://www.wegottickets.com/event/667102": {
				URL:         "https://www.wegottickets.com/event/667102",
				FinalURL:    "https://www.wegottickets.com/f/15737/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: []byte(`
					<html>
					  <body>
					    <link rel="canonical" href="/f/15737/">
					    <h1>An evening with Gideon Conn at Cafe No. 9</h1>
					    <h2>Event information</h2>
					    <main>
					      Gideon Conn is back at Cafe No. 9.<br>
					      <br>
					      Tickets available now.
					    </main>
					  </body>
					</html>
				`),
				CapturedAt: time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CafeNo9Source, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Calendars[0].Candidates[0].Description, "Gideon Conn is back at Cafe No. 9.\n\nTickets available now."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestRunManualSidneyAndMatildaEnrichesICSDescriptionsFromDetailPages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	icsBody := []byte(strings.Join([]string{
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
	}, "\n"))
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/events/",
				Status:     "200 OK",
				StatusCode: 200,
				Body: []byte(`
					<a href="https://calendar.example.test/live.ics">Google Calendar ICS</a>
					<a href="https://calendar.example.test/club.ics">Google Calendar ICS</a>
					<a href="/events/leo-middea-brazil">Leo Middea (Brazil)</a>
				`),
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
			"https://calendar.example.test/live.ics": {
				URL:        "https://calendar.example.test/live.ics",
				FinalURL:   "https://calendar.example.test/live.ics",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       icsBody,
				CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
			},
			"https://calendar.example.test/club.ics": {
				URL:        "https://calendar.example.test/club.ics",
				FinalURL:   "https://calendar.example.test/club.ics",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       icsBody,
				CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
			},
			"https://www.sidneyandmatilda.com/events/leo-middea-brazil": {
				URL:        "https://www.sidneyandmatilda.com/events/leo-middea-brazil",
				FinalURL:   "https://www.sidneyandmatilda.com/events/leo-middea-brazil",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney_detail.html"),
				CapturedAt: time.Date(2026, 4, 20, 12, 4, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(store.snapshots), 4; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Snapshots, 4; got != want {
		t.Fatalf("total snapshots = %d, want %d", got, want)
	}
	wantDescription := "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop."
	for i, calendar := range report.Calendars {
		if got := calendar.Candidates[0].Description; got != wantDescription {
			t.Fatalf("calendar %d description = %q, want %q", i, got, wantDescription)
		}
	}
}

func TestRunManualJazzAtTheLescarParsesListingsFromSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"http://www.jazzatthelescar.com/index.html": {
				URL:         "http://www.jazzatthelescar.com/index.html",
				FinalURL:    "http://www.jazzatthelescar.com/index.html",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "jazz_at_the_lescar.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: JazzAtTheLescarSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
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
	if got, want := report.Calendars[0].Candidates[0].Location, "The Lescar"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if report.Calendars[0].Candidates[0].UID != "" {
		t.Fatalf("uid = %q, want blank", report.Calendars[0].Candidates[0].UID)
	}
}

func TestRunManualTheGreystonesParsesListingsFromMonthPages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.mygreystones.co.uk/events/": {
				URL:         "https://www.mygreystones.co.uk/events/",
				FinalURL:    "https://www.mygreystones.co.uk/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "greystones_events.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
			"https://www.mygreystones.co.uk/april/": {
				URL:         "https://www.mygreystones.co.uk/april/",
				FinalURL:    "https://www.mygreystones.co.uk/april/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "greystones_april.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: TheGreystonesSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
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
	if report.Calendars[0].Candidates[0].UID != "" {
		t.Fatalf("uid = %q, want blank", report.Calendars[0].Candidates[0].UID)
	}
}

func TestRunManualCafeNo9ParsesPaginatedSourcePages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.wegottickets.com/Cafe9": {
				URL:         "https://www.wegottickets.com/Cafe9",
				FinalURL:    "https://www.wegottickets.com/Cafe9",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_paged_1.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
			"https://www.wegottickets.com/Cafe9/page/2": {
				URL:         "https://www.wegottickets.com/Cafe9/page/2",
				FinalURL:    "https://www.wegottickets.com/Cafe9/page/2",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_paged_2.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
			},
			"https://www.wegottickets.com/Cafe9/page/3": {
				URL:         "https://www.wegottickets.com/Cafe9/page/3",
				FinalURL:    "https://www.wegottickets.com/Cafe9/page/3",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        []byte(`<html><body><h2><a href="/event/700201">An evening with Page Three at Cafe No9</a></h2><p>0 SHEFFIELD: Cafe No9</p><p>P Friday 15th May, 2026</p><p>N Door time: 7:00pm, Start time: 7:30pm</p><p><a href="/event/700201">Event info</a></p></body></html>`),
				CapturedAt:  time.Date(2026, 4, 23, 19, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CafeNo9Source, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
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

func TestRunManualCafeNo9PaginationRespectsGlobalLinkedPageLimit(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.wegottickets.com/Cafe9": {
				URL:         "https://www.wegottickets.com/Cafe9",
				FinalURL:    "https://www.wegottickets.com/Cafe9",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_paged_1.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
			"https://www.wegottickets.com/Cafe9/page/2": {
				URL:         "https://www.wegottickets.com/Cafe9/page/2",
				FinalURL:    "https://www.wegottickets.com/Cafe9/page/2",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "cafe9_paged_2.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CafeNo9Source, Limit: 1})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 2; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
}

func TestRunManualLeadmillParsesFilteredListingsFromLinkedICS(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://leadmill.co.uk/live/": {
				URL:         "https://leadmill.co.uk/live/",
				FinalURL:    "https://leadmill.co.uk/live/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        []byte(`<link rel="alternate" type="text/calendar" href="https://leadmill.co.uk/listings/?ical=1">`),
				CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
			},
			"https://leadmill.co.uk/listings/?ical=1": {
				URL:         "https://leadmill.co.uk/listings/?ical=1",
				FinalURL:    "https://leadmill.co.uk/listings/?ical=1",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/calendar",
				Body: []byte("BEGIN:VCALENDAR\n" +
					"BEGIN:VEVENT\n" +
					"UID:live-sheffield\n" +
					"SUMMARY:Maybe Gold - Yellow Arch\n" +
					"LOCATION:Yellow Arch\\, 30-36 Burton Road\\, Neepsend\\, S3 8BX\n" +
					"CATEGORIES:Live\n" +
					"DTSTART:20260501T190000Z\n" +
					"END:VEVENT\n" +
					"BEGIN:VEVENT\n" +
					"UID:club-night\n" +
					"SUMMARY:Club Night\n" +
					"LOCATION:The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE\n" +
					"CATEGORIES:Club\n" +
					"DTSTART:20260502T190000Z\n" +
					"END:VEVENT\n" +
					"END:VCALENDAR\n"),
				CapturedAt: time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: LeadmillSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars[0].Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Summary, "Maybe Gold"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Yellow Arch"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].LocationRaw, "Yellow Arch\\, 30-36 Burton Road\\, Neepsend\\, S3 8BX"; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
}

func TestRunManualCorporationParsesLinkedDetailPages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.corporation.org.uk/live/": {
				URL:         "https://www.corporation.org.uk/live/",
				FinalURL:    "https://www.corporation.org.uk/live/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "corporation_live.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 1, 0, 0, time.UTC),
			},
			"https://www.corporation.org.uk/event/tyketto/": {
				URL:         "https://www.corporation.org.uk/event/tyketto/",
				FinalURL:    "https://www.corporation.org.uk/event/tyketto/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "corporation_tyketto.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 2, 0, 0, time.UTC),
			},
			"https://www.corporation.org.uk/event/frog-lord/": {
				URL:         "https://www.corporation.org.uk/event/frog-lord/",
				FinalURL:    "https://www.corporation.org.uk/event/frog-lord/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "corporation_frog_lord.html"),
				CapturedAt:  time.Date(2026, 4, 24, 12, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: CorporationSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
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
	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].UID, "https://www.corporation.org.uk/event/tyketto/"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Corporation"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestRunManualSourceFetchFailureReturnsErrRunFailed(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		err: errors.New("source fetch failed"),
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], "source fetch failed") {
		t.Fatalf("report errors = %#v, want source fetch failure", report.Errors)
	}
	if store.finishedStatus != importStatusFailed {
		t.Fatalf("finished status = %q, want failed", store.finishedStatus)
	}
	if !strings.Contains(store.finishedNotes, "source fetch failed") {
		t.Fatalf("finished notes = %q, want recorded fetch error", store.finishedNotes)
	}
	if got := len(store.snapshots); got != 0 {
		t.Fatalf("snapshots = %d, want 0", got)
	}
}

func TestRunManualFailsClosedWhenNoLinks(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       []byte(`<a href="/calendar.ics">Other calendar</a>`),
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected report error")
	}
	if store.finishedStatus != importStatusFailed {
		t.Fatalf("finished status = %q, want failed", store.finishedStatus)
	}
}

func TestRunManualSnapshotsThenFailsOnTruncatedSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.html"),
				Truncated:  true,
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if report.Page == nil || !report.Page.Truncated {
		t.Fatalf("page snapshot truncated = %#v, want true", report.Page)
	}
	if got := len(report.Calendars); got != 0 {
		t.Fatalf("calendars = %d, want 0", got)
	}
	if got, want := report.Totals.Errors, 1; got != want {
		t.Fatalf("errors = %d, want %d", got, want)
	}
}

func TestRunManualCalendarErrorsFailStatusAndNotes(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/events/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.html"),
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
			"https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live": {
				URL:        "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				FinalURL:   "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.ics"),
				CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
			},
			"https://calendar.example.test/calendar-two.ics": {
				URL:        "https://calendar.example.test/calendar-two.ics",
				FinalURL:   "https://calendar.example.test/calendar-two.ics",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.ics"),
				Truncated:  true,
				CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 2})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Errors, 1; got != want {
		t.Fatalf("errors = %d, want %d", got, want)
	}
	if len(report.Calendars) != 2 {
		t.Fatalf("calendars = %d, want 2", len(report.Calendars))
	}
	if got := len(report.Calendars[1].Candidates); got != 0 {
		t.Fatalf("truncated calendar candidates = %d, want 0", got)
	}
	if !strings.Contains(store.finishedNotes, "links=2 candidates=3 skips=4 errors=1") {
		t.Fatalf("finished notes = %q, want aggregate summary", store.finishedNotes)
	}
	if !strings.Contains(store.finishedNotes, "ICS response was truncated") {
		t.Fatalf("finished notes = %q, want truncated calendar detail", store.finishedNotes)
	}
	if !strings.Contains(store.finishedNotes, "errors=1") {
		t.Fatalf("finished notes = %q, want errors=1", store.finishedNotes)
	}
}

type fakeFetcher struct {
	results map[string]FetchResult
	err     error
}

func (f fakeFetcher) Fetch(_ context.Context, url string) (FetchResult, error) {
	if f.err != nil {
		return FetchResult{}, f.err
	}
	result, ok := f.results[url]
	if !ok {
		return FetchResult{}, errors.New("unexpected fetch " + url)
	}
	return result, nil
}

type fakeStore struct {
	now            time.Time
	nextSourceID   int64
	nextRunID      int64
	nextSnapshotID int64
	sources        map[string]int64
	snapshots      []fakeSnapshot
	finishedStatus string
	finishedNotes  string
	eventWrites    int
}

type fakeSnapshot struct {
	runID      int64
	sourceID   *int64
	capturedAt time.Time
	payload    string
}

func (s *fakeStore) EnsureSource(_ context.Context, name, url string) (int64, error) {
	if s.sources == nil {
		s.sources = make(map[string]int64)
	}
	key := name + "\x00" + url
	if id, ok := s.sources[key]; ok {
		return id, nil
	}
	s.nextSourceID++
	s.sources[key] = s.nextSourceID
	return s.nextSourceID, nil
}

func (s *fakeStore) CreateImportRun(_ context.Context, _, _ string) (int64, time.Time, error) {
	s.nextRunID++
	return s.nextRunID, s.now, nil
}

func (s *fakeStore) CreateSnapshot(_ context.Context, runID int64, sourceID *int64, capturedAt time.Time, payload string) (int64, time.Time, error) {
	s.nextSnapshotID++
	var sourceCopy *int64
	if sourceID != nil {
		value := *sourceID
		sourceCopy = &value
	}
	s.snapshots = append(s.snapshots, fakeSnapshot{
		runID:      runID,
		sourceID:   sourceCopy,
		capturedAt: capturedAt,
		payload:    payload,
	})
	return s.nextSnapshotID, capturedAt, nil
}

func (s *fakeStore) FinishImportRun(_ context.Context, _ int64, status, notes string) (time.Time, error) {
	s.finishedStatus = status
	s.finishedNotes = notes
	return s.now.Add(time.Minute), nil
}
