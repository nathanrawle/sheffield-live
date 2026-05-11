package ingest

import (
	"reflect"
	"testing"
)

func TestExtractTheGreystonesMonthLinks(t *testing.T) {
	body := readFixture(t, "greystones_events.html")

	got, err := ExtractTheGreystonesMonthLinks("https://www.mygreystones.co.uk/events/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.mygreystones.co.uk/april/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseTheGreystonesMonthPage(t *testing.T) {
	result := ParseTheGreystonesMonthPage("https://www.mygreystones.co.uk/april/", readFixture(t, "greystones_april.html"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.Summary, "THE FURROW COLLECTIVE TRIO"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := first.Location, "The Greystones"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2026-04-29T19:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := first.URL, "https://tickets.example.test/furrow"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := first.ImageSourceURL, "https://www.mygreystones.co.uk/april/furrow.jpg"; got != want {
		t.Fatalf("image source url = %q, want %q", got, want)
	}
	if got, want := first.Status, "Listed"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if first.EndAt != "" {
		t.Fatalf("end = %q, want blank", first.EndAt)
	}
	if got, want := first.Description, "First description paragraph.\n\nSecond description paragraph."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}

	second := result.Candidates[1]
	if got, want := second.Summary, "JOHN SMITH"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := second.StartAt, "2026-04-30T19:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := second.ImageSourceURL, "https://www.mygreystones.co.uk/april/john.jpg"; got != want {
		t.Fatalf("image source url = %q, want %q", got, want)
	}
}

func TestParseTheGreystonesMonthPageSkipsNonEventHeading(t *testing.T) {
	result := ParseTheGreystonesMonthPage("https://www.mygreystones.co.uk/april/", []byte(`
		<html><body>
		<h1>APRIL 2026</h1>
		<h1>JOHN SMITH</h1>
		<h4><span>Thursday 30th April / 8pm / £22</span></h4>
		<p>Show description.</p>
		</body></html>
	`))

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "missing event metadata"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseTheGreystonesMonthPageParsesAugustDate(t *testing.T) {
	result := ParseTheGreystonesMonthPage("https://www.mygreystones.co.uk/august/", []byte(`
		<html><body>
		<h1>AUGUST 2026</h1>
		<h1>AUGUST ARTIST</h1>
		<h4><span>Saturday 1st August / 8pm / £18</span></h4>
		<p>Show description.</p>
		</body></html>
	`))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].StartAt, "2026-08-01T19:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
}

func TestParseTheGreystonesMonthPageParsesDottedTimes(t *testing.T) {
	result := ParseTheGreystonesMonthPage("https://www.mygreystones.co.uk/august/", []byte(`
		<html><body>
		<h1>AUGUST 2026</h1>
		<h1>SONGS FROM THE BACK OF A RUSTY UTE</h1>
		<h4><span>Thursday 20th August / 7.45pm / £18</span></h4>
		<p>Show description.</p>
		<h1>OPEN MIC NIGHT</h1>
		<h4><span>Monday 24th August / 7.30pm / Free</span></h4>
		<p>Show description.</p>
		</body></html>
	`))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].StartAt, "2026-08-20T18:45:00Z"; got != want {
		t.Fatalf("first start = %q, want %q", got, want)
	}
	if got, want := result.Candidates[1].StartAt, "2026-08-24T18:30:00Z"; got != want {
		t.Fatalf("second start = %q, want %q", got, want)
	}
}
