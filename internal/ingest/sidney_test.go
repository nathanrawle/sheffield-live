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
