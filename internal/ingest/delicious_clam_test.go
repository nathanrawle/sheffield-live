package ingest

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func freezeDeliciousClamClock(t *testing.T) {
	t.Helper()
	old := deliciousClamNow
	deliciousClamNow = func() time.Time {
		return time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	}
	t.Cleanup(func() {
		deliciousClamNow = old
	})
}

func TestExtractDeliciousClamTicketLinks(t *testing.T) {
	freezeDeliciousClamClock(t)

	got, err := ExtractDeliciousClamTicketLinks("https://www.deliciousclam.co.uk/events", readFixture(t, "delicious_clam_events.html"), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.skiddle.com/e/42362090",
		"https://www.skiddle.com/e/42138536",
		"https://www.skiddle.com/e/42261454",
		"https://www.skiddle.com/e/42430402",
		"https://www.skiddle.com/e/42421310",
		"https://www.skiddle.com/e/42375475",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
	for _, link := range got {
		if link == "https://www.skiddle.com/e/40000000" {
			t.Fatalf("links include stale delegated link %q", link)
		}
	}
}

func TestExtractDeliciousClamTicketLinksRequiresOfficialEventsPage(t *testing.T) {
	freezeDeliciousClamClock(t)

	got, err := ExtractDeliciousClamTicketLinks("https://www.skiddle.com/e/42362090", readFixture(t, "delicious_clam_events.html"), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("links = %#v, want none", got)
	}
}

func TestParseDeliciousClamTicketPage(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/42261454", readFixture(t, "delicious_clam_detail_good.html"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Skips), 0; got != want {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.UID, "https://www.skiddle.com/e/42261454"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.Summary, "DC Presents: Good Flying Birds + The Cindys"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, deliciousClamVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.StartAt, "2026-06-28T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.LocationRaw, "Delicious Clam") {
		t.Fatalf("location raw = %q, want Delicious Clam evidence", candidate.LocationRaw)
	}
}

func TestParseDeliciousClamTicketPageRejectsMissingVenueEvidence(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/42261454", readFixture(t, "delicious_clam_detail_missing_venue.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "unsupported venue"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseDeliciousClamTicketPageRejectsTitleOnlyVenueEvidence(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/42261454", readFixture(t, "delicious_clam_detail_title_only_venue.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "unsupported venue"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseDeliciousClamTicketPageRejectsDateOnlyItems(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/42138536", readFixture(t, "delicious_clam_detail_date_only.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "missing event start time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseDeliciousClamTicketPageRejectsMultiDayItems(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/99999999", readFixture(t, "delicious_clam_detail_multiday.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "multi-day event"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseDeliciousClamTicketPageRejectsNonMusicItems(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/88888888", readFixture(t, "delicious_clam_detail_non_music.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "non-music event"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseDeliciousClamTicketPageRejectsPastItems(t *testing.T) {
	freezeDeliciousClamClock(t)

	result := ParseDeliciousClamTicketPage("https://www.skiddle.com/e/40000000", readFixture(t, "delicious_clam_detail_past.html"))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want 1", result.Skips)
	}
	if got, want := result.Skips[0].Reason, "past event"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}
