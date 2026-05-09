package ingest

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractCorporationDetailLinks(t *testing.T) {
	body := readFixture(t, "corporation_live.html")

	got, err := ExtractCorporationDetailLinks("https://www.corporation.org.uk/live/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.corporation.org.uk/event/tyketto/",
		"https://www.corporation.org.uk/event/frog-lord/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseCorporationDetailPage(t *testing.T) {
	result := ParseCorporationDetailPage("https://www.corporation.org.uk/event/tyketto/", readFixture(t, "corporation_tyketto.html"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.UID, "https://www.corporation.org.uk/event/tyketto/"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.Summary, "TYKETTO"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, "Corporation"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.StartAt, "2026-04-24T18:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-04-24T21:30:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if candidate.Description == "" {
		t.Fatal("description = empty, want description")
	}
}

func TestParseCorporationDetailPageUsesStructuredEventData(t *testing.T) {
	result := ParseCorporationDetailPage("https://www.corporation.org.uk/event/bat-sabbath-cancer-bats-as-black-sabbath/", []byte(`
		<html>
		  <head>
		    <title>Bat Sabbath | Cancer Bats as Black Sabbath | Corporation Sheffield</title>
		    <script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","name":"Bat Sabbath | Cancer Bats as Black Sabbath | Corporation Sheffield"}</script>
		    <script type="application/ld+json">[{
		      "@context":"http://www.schema.org",
		      "@type":"Event",
		      "name":"Bat Sabbath | Cancer Bats as Black Sabbath",
		      "description":"Canadian Whiskey Metal Punk Bastards.\\nBe sure to catch BAT SABBATH.",
		      "startDate":"2026-07-16T18:00",
		      "endDate":"2026-07-16T21:30",
		      "location":{
		        "@type":"Place",
		        "name":"Corporation",
		        "address":{
		          "@type":"PostalAddress",
		          "streetAddress":"2 Milton St, Sheffield City Centre, Sheffield S1 4JU, UK",
		          "addressLocality":"Sheffield",
		          "addressCountry":"United Kingdom"
		        }
		      },
		      "url":"https://www.fatsoma.com/e/zr4frbv9/bat-sabbath-cancer-bats-as-black-sabbath"
		    }]</script>
		  </head>
		  <body>
		    <h1 class="event-single__heading">Bat Sabbath | Cancer Bats as Black Sabbath</h1>
		  </body>
		</html>
	`))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.UID, "https://www.corporation.org.uk/event/bat-sabbath-cancer-bats-as-black-sabbath/"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.URL, "https://www.corporation.org.uk/event/bat-sabbath-cancer-bats-as-black-sabbath/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := candidate.Summary, "Bat Sabbath | Cancer Bats as Black Sabbath"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, "Corporation"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.LocationRaw, "2 Milton St") {
		t.Fatalf("location raw = %q, want street address", candidate.LocationRaw)
	}
	if got, want := candidate.StartAt, "2026-07-16T18:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-07-16T21:30:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.Description, "Canadian Whiskey Metal Punk Bastards.") {
		t.Fatalf("description = %q, want structured description", candidate.Description)
	}
}

func TestParseCorporationStructuredDateTimeTreatsOffsetlessTimesAsUTC(t *testing.T) {
	structured, err := parseCorporationStructuredDateTime("2026-07-16T18:00")
	if err != nil {
		t.Fatalf("parse structured datetime: %v", err)
	}
	if got, want := formatTime(structured), "2026-07-16T18:00:00Z"; got != want {
		t.Fatalf("structured datetime = %q, want %q", got, want)
	}

	legacy, err := parseCorporationDateTime("2026-07-16T18:00")
	if err != nil {
		t.Fatalf("parse legacy datetime: %v", err)
	}
	if got, want := formatTime(legacy), "2026-07-16T17:00:00Z"; got != want {
		t.Fatalf("legacy datetime = %q, want %q", got, want)
	}
}
