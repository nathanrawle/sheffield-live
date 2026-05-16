package ingest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExtractSidneyAndMatildaICSLinks(t *testing.T) {
	body := readFixture(t, "sidney.html")

	got, err := ExtractSidneyAndMatildaICSLinks("https://www.sidneyandmatilda.com/events/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
		"https://calendar.example.test/calendar-two.ics",
		"https://www.sidneyandmatilda.com/events/shattered-cogs?format=ical",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractSidneyAndMatildaICSLinksAppliesLimit(t *testing.T) {
	body := readFixture(t, "sidney.html")

	got, err := ExtractSidneyAndMatildaICSLinks("https://www.sidneyandmatilda.com/events/", body, 1)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(links) = %d, want 1", len(got))
	}
	if got[0] != "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live" {
		t.Fatalf("first link = %q", got[0])
	}
}

func TestExtractSidneyAndMatildaICSLinksAcceptsFormatICALAndLegacyLabel(t *testing.T) {
	body := []byte(`<a href="/events/plain.ics">ICS</a><a href="/events/ical.ics?format=ical">ICS</a><a href="/events/legacy.ics">Google Calendar ICS</a>`)

	got, err := ExtractSidneyAndMatildaICSLinks("https://www.sidneyandmatilda.com/events/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.sidneyandmatilda.com/events/ical.ics?format=ical",
		"https://www.sidneyandmatilda.com/events/legacy.ics",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractSidneyAndMatildaEventDetailLinks(t *testing.T) {
	body := []byte(`
		<a href="/events/leo-middea-brazil">Leo Middea</a>
		<a href="/events/leo-middea-brazil?format=ical">ICS</a>
		<a href="https://tickets.example.test/leo">BUY TICKETS</a>
		<a href="/events/w-i-t-c-h">View Event →</a>
	`)

	got, err := ExtractSidneyAndMatildaEventDetailLinks("https://www.sidneyandmatilda.com/events/", body, 20)
	if err != nil {
		t.Fatalf("extract detail links: %v", err)
	}

	want := []string{
		"https://www.sidneyandmatilda.com/events/leo-middea-brazil",
		"https://www.sidneyandmatilda.com/events/w-i-t-c-h",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractSidneyAndMatildaRoomEvidence(t *testing.T) {
	body := []byte(`
		<article class="eventlist-event">
			<a href="/events/parallel-delusion" class="eventlist-column-thumbnail"></a>
			<h1 class="eventlist-title"><a href="/events/parallel-delusion">S&amp;M Presents: Parallel Delusion</a></h1>
			<div class="eventlist-excerpt"><p>FACTORY</p><p>Tickets available</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/contrails">Contrails</a>
			<div class="eventlist-excerpt"><p>BASEMENT</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/abba-gold">The Belgrave House Band Presents: ABBA Gold</a>
			<div class="eventlist-excerpt"><p>GALLERY</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/two-roomer">Two Roomer</a>
			<div class="eventlist-excerpt"><p>GALLERY + BASEMENT</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/whole-venue">Whole Venue Show</a>
			<div class="eventlist-excerpt"><p>WHOLE VENUE</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/new-space">New Space Show</a>
			<div class="eventlist-excerpt"><p>COURTYARD STAGE</p></div>
		</article>
	`)

	got := ExtractSidneyAndMatildaRoomEvidence("https://www.sidneyandmatilda.com/events/", body)

	assertRoomEvidence(t, got["url:https://www.sidneyandmatilda.com/events/parallel-delusion"], "FACTORY", []RoomCandidate{{Slug: "factory", Name: "Factory"}})
	assertRoomEvidence(t, got[roomEvidenceTitleKey("S&M Presents: Parallel Delusion")], "FACTORY", []RoomCandidate{{Slug: "factory", Name: "Factory"}})
	assertRoomEvidence(t, got[roomEvidenceTitleKey("Contrails")], "BASEMENT", []RoomCandidate{{Slug: "basement", Name: "Basement"}})
	assertRoomEvidence(t, got["url:https://www.sidneyandmatilda.com/events/abba-gold"], "GALLERY", []RoomCandidate{{Slug: "gallery", Name: "Gallery"}})
	assertRoomEvidence(t, got["url:https://www.sidneyandmatilda.com/events/two-roomer"], "GALLERY + BASEMENT", []RoomCandidate{{Slug: "gallery", Name: "Gallery"}, {Slug: "basement", Name: "Basement"}})
	assertRoomEvidence(t, got["url:https://www.sidneyandmatilda.com/events/whole-venue"], "WHOLE VENUE", nil)
	assertRoomEvidence(t, got["url:https://www.sidneyandmatilda.com/events/new-space"], "COURTYARD STAGE", []RoomCandidate{{Slug: "courtyard-stage", Name: "Courtyard Stage"}})
}

func TestSidneyRoomEvidenceSuppressesAmbiguousTitleFallback(t *testing.T) {
	body := []byte(`
		<article class="eventlist-event">
			<a href="/events/club-night-early">Recurring Club Night</a>
			<div class="eventlist-excerpt"><p>FACTORY</p></div>
		</article>
		<article class="eventlist-event">
			<a href="/events/club-night-late">Recurring Club Night</a>
			<div class="eventlist-excerpt"><p>BASEMENT</p></div>
		</article>
	`)

	evidence := ExtractSidneyAndMatildaRoomEvidence("https://www.sidneyandmatilda.com/events/", body)

	assertRoomEvidence(t, evidence["url:https://www.sidneyandmatilda.com/events/club-night-early"], "FACTORY", []RoomCandidate{{Slug: "factory", Name: "Factory"}})
	assertRoomEvidence(t, evidence["url:https://www.sidneyandmatilda.com/events/club-night-late"], "BASEMENT", []RoomCandidate{{Slug: "basement", Name: "Basement"}})
	if _, ok := evidence[roomEvidenceTitleKey("Recurring Club Night")]; ok {
		t.Fatal("ambiguous title fallback evidence was stored")
	}

	merged := mergeRoomEvidence([]EventCandidate{
		{
			Summary: "Recurring Club Night",
			StartAt: "2026-05-01T19:00:00Z",
		},
		{
			Summary: "Recurring Club Night",
			URL:     "https://www.sidneyandmatilda.com/events/club-night-early",
			StartAt: "2026-05-01T21:00:00Z",
		},
	}, evidence)

	if got := merged[0].RoomText; got != "" {
		t.Fatalf("URL-less candidate room text = %q, want blank", got)
	}
	if len(merged[0].Rooms) != 0 {
		t.Fatalf("URL-less candidate rooms = %#v, want none", merged[0].Rooms)
	}
	if got, want := merged[1].RoomText, "FACTORY"; got != want {
		t.Fatalf("URL-matched candidate room text = %q, want %q", got, want)
	}
	if got, want := merged[1].Rooms, []RoomCandidate{{Slug: "factory", Name: "Factory"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("URL-matched candidate rooms = %#v, want %#v", got, want)
	}
}

func assertRoomEvidence(t *testing.T, got sourceRoomEvidence, wantText string, wantRooms []RoomCandidate) {
	t.Helper()

	if got.Text != wantText {
		t.Fatalf("room evidence text = %q, want %q", got.Text, wantText)
	}
	if !reflect.DeepEqual(got.Rooms, wantRooms) {
		t.Fatalf("room evidence rooms = %#v, want %#v", got.Rooms, wantRooms)
	}
}

func TestParseSidneyAndMatildaDetailPageExtractsDescription(t *testing.T) {
	detail := ParseSidneyAndMatildaDetailPage("https://www.sidneyandmatilda.com/events/leo-middea-brazil", readFixture(t, "sidney_detail.html"))

	if got, want := detail.Summary, "Leo Middea (Brazil)"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := detail.StartAt, "2026-05-04T18:30:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := detail.Description, "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestParseSidneyAndMatildaDetailPageResolvesCanonicalAliasFromStructuredJSONLD(t *testing.T) {
	detail := ParseSidneyAndMatildaDetailPage("https://www.sidneyandmatilda.com/events/leo-middea-brazil", []byte(`
		<!doctype html>
		<html>
		  <body>
		    <link rel="canonical" href="/f/15737/">
		    <script type="application/ld+json">
		      {
		        "@context": "https://schema.org",
		        "@type": "Event",
		        "name": "Leo Middea (Brazil)",
		        "description": "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop.",
		        "startDate": "2026-05-04T18:30:00Z",
		        "url": "https://www.sidneyandmatilda.com/events/leo-middea-brazil"
		      }
		    </script>
		  </body>
		</html>
	`))

	if got, want := detail.Summary, "Leo Middea (Brazil)"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := detail.Description, "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := detail.StartAt, "2026-05-04T18:30:00Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
	if got, want := detail.URLAliases, []string{"https://www.sidneyandmatilda.com/f/15737/", "https://www.sidneyandmatilda.com/events/leo-middea-brazil"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("url aliases = %#v, want %#v", got, want)
	}
}

func TestSidneyJSONLDHasSchemaContext(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{
			name: "https string",
			raw:  `{"@context":"https://schema.org","@type":"Event"}`,
			want: true,
		},
		{
			name: "http string with slash",
			raw:  `{"@context":"http://schema.org/","@type":"Event"}`,
			want: true,
		},
		{
			name: "array contains schema org",
			raw:  `{"@context":["https://example.test","https://schema.org/"],"@type":"Event"}`,
			want: true,
		},
		{
			name: "vocab map",
			raw:  `{"@context":{"@vocab":"https://schema.org"},"@type":"Event"}`,
			want: true,
		},
		{
			name: "non schema",
			raw:  `{"@context":"https://example.test","@type":"Event"}`,
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := sidneyJSONLDHasSchemaContext([]byte(tc.raw)); got != tc.want {
				t.Fatalf("sidneyJSONLDHasSchemaContext() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestParseSidneyAndMatildaDetailPageIgnoresCleanEventWithNonSchemaContext(t *testing.T) {
	detail := ParseSidneyAndMatildaDetailPage("https://www.sidneyandmatilda.com/events/leo-middea-brazil", []byte(`
		<!doctype html>
		<html>
		  <body>
		    <script type="application/ld+json">
		      {
		        "@context": "https://example.test",
		        "@type": "Event",
		        "name": "Leo Middea (Brazil)",
		        "description": "Leo Middea returns to Sheffield in 2026.\n\nHis music blends MPB, samba, bossa nova and contemporary Brazilian pop.",
		        "startDate": "2026-05-04T18:30:00Z",
		        "url": "https://www.sidneyandmatilda.com/events/leo-middea-brazil"
		      }
		    </script>
		  </body>
		</html>
	`))

	if detail.Description != "" {
		t.Fatalf("description = %q, want empty", detail.Description)
	}
}

func TestParseSidneyAndMatildaDetailPageIgnoresStaticSquarespaceContext(t *testing.T) {
	detail := ParseSidneyAndMatildaDetailPage("https://www.sidneyandmatilda.com/events/liam-c", []byte(`
		<!doctype html>
		<html>
		  <body>
		    <script>
		      Static.SQUARESPACE_CONTEXT = {"item":{"body":"<p>static context should not leak</p>"}};
		    </script>
		  </body>
		</html>
	`))

	if detail.Description != "" {
		t.Fatalf("description = %q, want empty", detail.Description)
	}
}

func TestParseSidneyAndMatildaDetailPageDoesNotFallBackToWholeBody(t *testing.T) {
	raw := []byte(`
		<html>
			<body>
				<h1>Liam C</h1>
				<style>
					#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; }
					@media screen and (max-width: 767px) { #block-d4b153a9777175667262 { } }
				</style>
				<div>Previous Previous May 6 Flock House Party</div>
				<div>Next Next May 7 S&amp;M Presents: Nick Diver</div>
			</body>
		</html>
	`)

	detail := ParseSidneyAndMatildaDetailPage("https://www.sidneyandmatilda.com/events/liam-c", raw)

	if detail.Description != "" {
		t.Fatalf("description = %q, want empty", detail.Description)
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return raw
}
