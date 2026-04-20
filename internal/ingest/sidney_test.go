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

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return raw
}
