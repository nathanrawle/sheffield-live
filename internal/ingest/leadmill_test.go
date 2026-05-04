package ingest

import (
	"reflect"
	"testing"
)

func TestExtractLeadmillICSLinks(t *testing.T) {
	body := []byte(`
		<html>
			<head>
				<link rel="alternate" type="text/calendar" href="https://leadmill.co.uk/listings/?ical=1">
				<link rel="alternate" type="application/rss+xml" href="https://leadmill.co.uk/feed/">
			</head>
		</html>
	`)

	got, err := ExtractLeadmillICSLinks("https://leadmill.co.uk/live/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{"https://leadmill.co.uk/listings/?ical=1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestParseLeadmillICSFiltersNonLiveAndNonSheffield(t *testing.T) {
	result := ParseLeadmillICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:live-sheffield\n" +
		"SUMMARY:Maybe Gold - Yellow Arch\n" +
		"LOCATION:Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX\n" +
		"CATEGORIES:Live,Music\n" +
		"DTSTART:20260501T190000Z\n" +
		"DTEND:20260501T220000Z\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"UID:not-live\n" +
		"SUMMARY:Club Night\n" +
		"LOCATION:The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE\n" +
		"CATEGORIES:Club\n" +
		"DTSTART:20260502T190000Z\n" +
		"END:VEVENT\n" +
		"BEGIN:VEVENT\n" +
		"UID:not-sheffield\n" +
		"SUMMARY:Outside Sheffield\n" +
		"LOCATION:Leeds\n" +
		"CATEGORIES:Live\n" +
		"DTSTART:20260503T190000Z\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))

	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].Summary, "Maybe Gold - Yellow Arch"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := result.Candidates[0].Location, "Yellow Arch"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "filtered non-Live category"; got != want {
		t.Fatalf("first skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "filtered non-Sheffield location"; got != want {
		t.Fatalf("second skip reason = %q, want %q", got, want)
	}
}

func TestLeadmillVenueTextUsesFirstLocationSegment(t *testing.T) {
	tests := []struct {
		name     string
		location string
		want     string
	}{
		{
			name:     "location first segment",
			location: "Foundry, SHEFFIELD STUDENTS' UNION, WESTERN BARNK, Sheffield, South Yorkshire, S10 2TG, United Kingdom",
			want:     "Foundry",
		},
		{
			name:     "trims repeated spaces",
			location: "  Yellow   Arch  , 30-36 Burton Road, Neepsend, S3 8BX ",
			want:     "Yellow Arch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := leadmillVenueText(tc.location); got != tc.want {
				t.Fatalf("leadmillVenueText(%q) = %q, want %q", tc.location, got, tc.want)
			}
		})
	}
}

func TestLeadmillVenueTextFromEvidencePreservesEscapedCommas(t *testing.T) {
	location := "Memorial Hall, Barkers Pool, Sheffield, S1 2JA"
	raw := "Memorial Hall\\, Barkers Pool, Sheffield, S1 2JA"

	if got, want := leadmillVenueTextFromEvidence(location, raw), "Memorial Hall, Barkers Pool"; got != want {
		t.Fatalf("leadmillVenueTextFromEvidence(%q, %q) = %q, want %q", location, raw, got, want)
	}
}

func TestParseLeadmillICSPreservesRawLocationEvidence(t *testing.T) {
	result := ParseLeadmillICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:memorial-hall\n" +
		"SUMMARY:Memorial Hall Show\n" +
		"LOCATION:Memorial Hall\\, Barkers Pool\\, Sheffield\\, S1 2JA\n" +
		"CATEGORIES:Live\n" +
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
	if got, want := candidate.Location, "Memorial Hall"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.LocationRaw, "Memorial Hall\\, Barkers Pool\\, Sheffield\\, S1 2JA"; got != want {
		t.Fatalf("location raw = %q, want %q", got, want)
	}
}
