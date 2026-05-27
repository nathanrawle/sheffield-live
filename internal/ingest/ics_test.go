package ingest

import "testing"

func TestParseICS(t *testing.T) {
	result := ParseICS(readFixture(t, "sidney.ics"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 3; got != want {
		t.Fatalf("candidates = %d, want %d: %#v", got, want, result.Candidates)
	}
	if got, want := len(result.Skips), 4; got != want {
		t.Fatalf("skips = %d, want %d: %#v", got, want, result.Skips)
	}

	utc := result.Candidates[0]
	if utc.UID != "utc-1" || utc.Summary != "UTC Show" {
		t.Fatalf("first candidate = %#v", utc)
	}
	if utc.Location != "Sidney & Matilda" {
		t.Fatalf("location = %q, want %q", utc.Location, "Sidney & Matilda")
	}
	if utc.Description != "First linecontinued line" {
		t.Fatalf("description = %q", utc.Description)
	}
	if utc.StartAt != "2026-05-01T19:00:00Z" || utc.EndAt != "2026-05-01T22:00:00Z" {
		t.Fatalf("UTC times = %s/%s", utc.StartAt, utc.EndAt)
	}

	london := result.Candidates[1]
	if london.StartAt != "2026-05-02T18:30:00Z" || london.EndAt != "2026-05-02T21:30:00Z" {
		t.Fatalf("London times = %s/%s", london.StartAt, london.EndAt)
	}

	floating := result.Candidates[2]
	if floating.StartAt != "2026-05-03T19:00:00Z" || floating.EndAt != "2026-05-03T22:00:00Z" {
		t.Fatalf("floating times = %s/%s", floating.StartAt, floating.EndAt)
	}

	wantReasons := []string{"all-day event", "cancelled", "missing summary", "malformed DTSTART:"}
	for i, want := range wantReasons {
		if i >= len(result.Skips) {
			t.Fatalf("missing skip %d", i)
		}
		if !hasPrefix(result.Skips[i].Reason, want) {
			t.Fatalf("skip %d reason = %q, want prefix %q", i, result.Skips[i].Reason, want)
		}
	}
}

func TestParseICSReportsStructuralErrors(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\nEND:VEVENT\nEND:VCALENDAR\n"))

	if got, want := len(result.Errors), 1; got != want {
		t.Fatalf("errors = %#v, want %d", result.Errors, want)
	}
}

func TestParseICSExtractsImageURL(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:image-show\n" +
		"SUMMARY:Image Show\n" +
		"LOCATION:Sidney & Matilda\n" +
		"IMAGE:https://example.test/show.webp\n" +
		"DTSTART:20260501T190000Z\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	candidate := result.Candidates[0]
	if got, want := candidate.ImageSourceURL, "https://example.test/show.webp"; got != want {
		t.Fatalf("image source url = %q, want %q", got, want)
	}
	if got, want := candidate.ImageAlt, "Image Show"; got != want {
		t.Fatalf("image alt = %q, want %q", got, want)
	}
}

func TestParseICSUsesRecurrenceIDForOccurrenceUID(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:series-show\n" +
		"RECURRENCE-ID;TZID=Europe/London:20260508T200000\n" +
		"SUMMARY:Series Show\n" +
		"LOCATION:Sidney & Matilda\n" +
		"DTSTART;TZID=Europe/London:20260508T200000\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"UID:series-show\n" +
		"RECURRENCE-ID;TZID=Europe/London:20260515T200000\n" +
		"SUMMARY:Series Show\n" +
		"LOCATION:Sidney & Matilda\n" +
		"DTSTART;TZID=Europe/London:20260515T200000\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].UID, "ics:series-show:2026-05-08T19:00:00Z"; got != want {
		t.Fatalf("first uid = %q, want %q", got, want)
	}
	if got, want := result.Candidates[1].UID, "ics:series-show:2026-05-15T19:00:00Z"; got != want {
		t.Fatalf("second uid = %q, want %q", got, want)
	}
}

func TestParseICSUsesRawRecurrenceIDFallbackForUnsupportedRecurrenceID(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:series-show\n" +
		"RECURRENCE-ID;TZID=Europe/Paris;VALUE=DATE-TIME:20260508T200000\n" +
		"SUMMARY:Series Show\n" +
		"LOCATION:Sidney & Matilda\n" +
		"DTSTART;TZID=Europe/London:20260508T200000\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].UID, "ics:series-show:raw=20260508T200000|TZID=Europe/Paris|VALUE=DATE-TIME"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
}

func TestParseICSUnescapesHTMLEntities(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:escaped-show\n" +
		"SUMMARY:S&amp;amp;M Presents: R&amp;B Night\n" +
		"DESCRIPTION:Line with R&amp;amp;B and A&amp;R\n" +
		"LOCATION:Sidney &amp;amp; Matilda\n" +
		"URL:https://example.test/events?title=S&amp;amp;M\n" +
		"DTSTART:20260501T190000Z\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	candidate := result.Candidates[0]
	if got, want := candidate.Summary, "S&M Presents: R&B Night"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Description, "Line with R&B and A&R"; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := candidate.Location, "Sidney & Matilda"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.URL, "https://example.test/events?title=S&M"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := candidate.ImageAlt, "S&M Presents: R&B Night"; got != want {
		t.Fatalf("image alt = %q, want %q", got, want)
	}
}

func TestParseICSPreservesRawLocationEvidence(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:memorial-hall\n" +
		"SUMMARY:Memorial Hall Show\n" +
		"LOCATION:Memorial Hall\\, Barkers Pool\\, Sheffield\\, S1 2JA\n" +
		"DTSTART:20260501T190000Z\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	candidate := result.Candidates[0]
	if got, want := candidate.Location, "Memorial Hall, Barkers Pool, Sheffield, S1 2JA"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.LocationRaw, "Memorial Hall\\, Barkers Pool\\, Sheffield\\, S1 2JA"; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
}

func TestParseICSPreservesFoldedRawLocationEvidence(t *testing.T) {
	result := ParseICS([]byte("BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:memorial-hall\r\n" +
		"SUMMARY:Memorial Hall Show\r\n" +
		"LOCATION:Memorial Hall\\, Barkers Pool\\,\r\n" +
		" Sheffield\\, S1 2JA\r\n" +
		"DTSTART:20260501T190000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	candidate := result.Candidates[0]
	if got, want := candidate.Location, "Memorial Hall, Barkers Pool,Sheffield, S1 2JA"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.LocationRaw, "Memorial Hall\\, Barkers Pool\\,Sheffield\\, S1 2JA"; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
}

func hasPrefix(value, prefix string) bool {
	if len(value) < len(prefix) {
		return false
	}
	return value[:len(prefix)] == prefix
}
