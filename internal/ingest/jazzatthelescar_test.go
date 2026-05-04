package ingest

import "testing"

func TestParseJazzAtTheLescarPage(t *testing.T) {
	result := ParseJazzAtTheLescarPage(readFixture(t, "jazz_at_the_lescar.html"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.Summary, "KO Quartet"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := first.Location, "The Lescar"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2027-04-15T19:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := first.Status, "Listed"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if first.EndAt != "" {
		t.Fatalf("end = %q, want blank", first.EndAt)
	}
	if first.UID != "" {
		t.Fatalf("uid = %q, want blank", first.UID)
	}
	if first.URL != "" {
		t.Fatalf("url = %q, want blank", first.URL)
	}

	if got, want := result.Skips[0].Summary, "Keystones Piano festival at The Samuel Worth Chapel"; got != want {
		t.Fatalf("skip summary = %q, want %q", got, want)
	}
	if got, want := result.Skips[0].Reason, "multiple event dates"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}

	exception := result.Candidates[2]
	if got, want := exception.StartAt, "2026-05-21T18:00:00Z"; got != want {
		t.Fatalf("exception start = %q, want %q", got, want)
	}
	if got, want := exception.Description, "Please note - this gig is on a Thursday, start 7pm."; got != want {
		t.Fatalf("exception description = %q, want %q", got, want)
	}
}

func TestParseJazzAtTheLescarSourcePageAppliesLimit(t *testing.T) {
	result := ParseJazzAtTheLescarSourcePage("http://www.jazzatthelescar.com/index.html", readFixture(t, "jazz_at_the_lescar.html"), 2)

	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func TestParseJazzAtTheLescarPagePreservesDescriptionBreaks(t *testing.T) {
	result := ParseJazzAtTheLescarPage([]byte(`
		<div class="menu">Doors 8pm, music 8:30pm (unless otherwise stated).</div>
		<div class="art">KO Quartet</div>
		<div class="ttl">15th April / The Lescar / £10</div>
		<div class="dsc">First line<br>Second line<br><br>Next paragraph.</div>
		<div class="footer">Page last updated: 22nd April 2026</div>
	`))

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d: %#v", got, want, result.Candidates)
	}
	want := "First line\nSecond line\n\nNext paragraph."
	if got := result.Candidates[0].Description; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestParseJazzAtTheLescarPageFailsWithoutMetadata(t *testing.T) {
	result := ParseJazzAtTheLescarPage([]byte(`<div class="art">KO Quartet</div><div class="ttl">15th April / The Lescar / £10</div><div class="dsc">Desc</div>`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Errors), 1; got != want {
		t.Fatalf("errors = %#v, want %d", result.Errors, want)
	}
}

func TestParseJazzAtTheLescarPageSkipsMultipleDates(t *testing.T) {
	result := ParseJazzAtTheLescarPage([]byte(`
		<div class="menu">Doors 8pm, music 8:30pm (unless otherwise stated).</div>
		<div class="art">Keystones Piano festival at The Samuel Worth Chapel</div>
		<div class="ttl">Saturday 9th, Sunday 10th May / The Samuel Worth Chapel</div>
		<div class="dsc">A special offsite piano festival weekend.</div>
		<div class="footer">Page last updated: 22nd April 2026</div>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "multiple event dates"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseJazzAtTheLescarPageUsesSingleListingTimeOverride(t *testing.T) {
	result := ParseJazzAtTheLescarPage([]byte(`
		<div class="menu">Doors 8pm, music 8:30pm (unless otherwise stated).</div>
		<div class="art">Emma Coates</div>
		<div class="ttl">THURSDAY 21st May / The Lescar / &pound;10 (&pound;7 NUS)</div>
		<div class="dsc">Please note - this gig is on a Thursday, start 7pm.</div>
		<div class="footer">Page last updated: 22nd April 2026</div>
	`))

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 0; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].StartAt, "2026-05-21T18:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
}
