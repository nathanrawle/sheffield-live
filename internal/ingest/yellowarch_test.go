package ingest

import "testing"

func TestParseYellowArchPage(t *testing.T) {
	result := ParseYellowArchPage(readFixture(t, "yellow_arch.html"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d: %#v", got, want, result.Candidates)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.Summary, "Late Junction"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := first.URL, "/event/late-junction/"; got != want {
		t.Fatalf("raw url = %q, want %q", got, want)
	}
	if got, want := first.Location, "Yellow Arch Studios"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2026-05-10T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := first.EndAt, "2026-05-10T22:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}

	if got, want := result.Skips[0].Reason, "missing event start time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseYellowArchSourcePageResolvesRelativeURLsAndAppliesLimit(t *testing.T) {
	result := ParseYellowArchSourcePage("https://www.yellowarch.com/events/", readFixture(t, "yellow_arch.html"), 1)

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].URL, "https://www.yellowarch.com/event/late-junction/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func TestParseYellowArchPageFailsWithoutEventData(t *testing.T) {
	result := ParseYellowArchPage([]byte(`<html><head><script type="application/ld+json">{"@graph":[{"@type":"WebPage"}]}</script></head></html>`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Errors), 1; got != want {
		t.Fatalf("errors = %#v, want %d", result.Errors, want)
	}
}

func TestParseYellowArchPageSkipsEventsMissingTimes(t *testing.T) {
	result := ParseYellowArchPage([]byte(`
		<script type="application/ld+json">
			[{"@type":"Event","name":"Untimed","startDate":"2026-05-01T19:00","location":{"name":"Yellow Arch Studios"}}]
		</script>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want %d", result.Skips, want)
	}
	if got, want := result.Skips[0].Reason, "missing event end time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}
