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

func hasPrefix(value, prefix string) bool {
	if len(value) < len(prefix) {
		return false
	}
	return value[:len(prefix)] == prefix
}
