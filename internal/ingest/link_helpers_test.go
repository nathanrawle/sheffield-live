package ingest

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestExtractSameHostLinks(t *testing.T) {
	body := []byte(`
		<a href="/events/one?ref=home#tickets">One</a>
		<a href="https://example.test/events/two">Two</a>
		<a href="https://other.test/events/skip">Other</a>
		<a href="/news/skip">News</a>
		<a href="/events/one?ref=home#details">Duplicate</a>
	`)

	got, err := ExtractSameHostLinks("https://www.example.test/listings/", body, 10, func(u *url.URL) bool {
		return strings.HasPrefix(u.EscapedPath(), "/events/")
	})
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.example.test/events/one?ref=home",
		"https://example.test/events/two",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractSameHostLinksHonorsLimit(t *testing.T) {
	body := []byte(`<a href="/events/one">One</a><a href="/events/two">Two</a>`)

	got, err := ExtractSameHostLinks("https://example.test/", body, 1, nil)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if got, want := got, []string{"https://example.test/events/one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestExtractHiddenCalendarLinks(t *testing.T) {
	body := []byte(`
		<link rel="alternate" href="https://calendar.google.com/calendar/ical/venue%40example.com/public/basic.ics">
		<script>
			window.calendar = "https%3A%2F%2Fcalendar.google.com%2Fcalendar%2Fical%2Fother%2540example.com%2Fpublic%2Fbasic.ics";
		</script>
		<a href="https://example.test/not-calendar">not calendar</a>
		<a href="https://calendar.google.com/calendar/ical/venue%40example.com/public/basic.ics">duplicate</a>
	`)

	got, err := ExtractHiddenCalendarLinks("https://venue.example.test/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://calendar.google.com/calendar/ical/venue%40example.com/public/basic.ics",
		"https://calendar.google.com/calendar/ical/other%40example.com/public/basic.ics",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseLabeledFields(t *testing.T) {
	got := ParseLabeledFields([]byte(`
		<p><strong>Dates:</strong> Fri 3 July 2026</p>
		<p><strong>Venue</strong></p>
		<p>Firth Hall</p>
		<p><strong>Times:</strong> 7.30pm</p>
		<p><strong>Cost:</strong> Free</p>
	`), "Dates", "Venue", "Times", "Cost")

	want := map[string]string{
		"Dates": "Fri 3 July 2026",
		"Venue": "Firth Hall",
		"Times": "7.30pm",
		"Cost":  "Free",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
}
