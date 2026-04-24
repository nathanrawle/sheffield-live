package ingest

import (
	"reflect"
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
