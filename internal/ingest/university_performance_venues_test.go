package ingest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestUniversityPerformanceVenuesDetailLinksExtractOurEventsOnly(t *testing.T) {
	body := universityPerformanceVenuesListingHTML(
		"/our-events/man-in-the-mirror/",
		"/event/ignored/",
		"https://performancevenues.group.shef.ac.uk/our-events/firth-hall-concert/?ref=listing#tickets",
		"/our-events/man-in-the-mirror/#duplicate",
	)

	got, err := university_performance_venues_detail_links("https://performancevenues.group.shef.ac.uk/whats-on/", body, 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://performancevenues.group.shef.ac.uk/our-events/man-in-the-mirror/",
		"https://performancevenues.group.shef.ac.uk/our-events/firth-hall-concert/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestUniversityPerformanceVenuesVenueNormalizerAliases(t *testing.T) {
	cases := []struct {
		value string
		want  string
	}{
		{value: "Octagon", want: "octagon-centre"},
		{value: "Octagon Centre", want: "octagon-centre"},
		{value: "The Octagon Centre", want: "octagon-centre"},
		{value: "Firth Hall", want: "firth-hall"},
		{value: "Drama Studio", want: "drama-studio"},
		{value: "Firth Court", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			if got := VenueSlugForSourceLocation(UniversityOfSheffieldPerformanceVenuesSource, tc.value); got != tc.want {
				t.Fatalf("venue slug = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseUniversityPerformanceVenuesDetailPageParsesRecognizedVenueAndMultipleDates(t *testing.T) {
	tests := []struct {
		name           string
		pageURL        string
		title          string
		dates          string
		venue          string
		times          string
		cost           string
		description    string
		ticketURL      string
		wantCandidates int
		wantUIDs       []string
		wantLocations  []string
		wantStarts     []string
	}{
		{
			name:           "single date octagon",
			pageURL:        "https://performancevenues.group.shef.ac.uk/event/man-in-the-mirror/",
			title:          "Man in the Mirror",
			dates:          "Friday 7th August, 2026",
			venue:          "Octagon Centre",
			times:          "7:30 pm",
			cost:           "£35.75",
			description:    "Join us for a thrilling tribute concert and live music.",
			ticketURL:      "https://performancevenues.gigantic.com/man-in-the-mirror",
			wantCandidates: 1,
			wantUIDs:       []string{"https://performancevenues.gigantic.com/man-in-the-mirror"},
			wantLocations:  []string{"Octagon Centre"},
			wantStarts:     []string{"2026-08-07T18:30:00Z"},
		},
		{
			name:           "multi date firth hall",
			pageURL:        "https://performancevenues.group.shef.ac.uk/event/student-recitals/",
			title:          "Student Recital Series",
			dates:          "Friday 7th August, 2026 & Saturday 8th August, 2026",
			venue:          "Firth Hall",
			times:          "6pm",
			cost:           "Free",
			description:    "A live music recital evening.",
			ticketURL:      "https://performancevenues.gigantic.com/student-recitals",
			wantCandidates: 2,
			wantUIDs: []string{
				"https://performancevenues.gigantic.com/student-recitals|2026-08-07",
				"https://performancevenues.gigantic.com/student-recitals|2026-08-08",
			},
			wantLocations: []string{"Firth Hall", "Firth Hall"},
			wantStarts:    []string{"2026-08-07T17:00:00Z", "2026-08-08T17:00:00Z"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseUniversityPerformanceVenuesDetailPage(tc.pageURL, universityPerformanceVenuesDetailHTML(tc.title, tc.dates, tc.venue, tc.times, tc.cost, tc.description, tc.ticketURL))
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Skips), 0; got != want {
				t.Fatalf("skips = %#v, want none", result.Skips)
			}
			if got, want := len(result.Candidates), tc.wantCandidates; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			for i, candidate := range result.Candidates {
				if got, want := candidate.UID, tc.wantUIDs[i]; got != want {
					t.Fatalf("candidate %d uid = %q, want %q", i, got, want)
				}
				if got, want := candidate.URL, tc.pageURL; got != want {
					t.Fatalf("candidate %d url = %q, want %q", i, got, want)
				}
				if got, want := candidate.Location, tc.wantLocations[i]; got != want {
					t.Fatalf("candidate %d location = %q, want %q", i, got, want)
				}
				if got, want := candidate.LocationRaw, tc.wantLocations[i]; got != want {
					t.Fatalf("candidate %d location raw = %q, want %q", i, got, want)
				}
				if got, want := candidate.StartAt, tc.wantStarts[i]; got != want {
					t.Fatalf("candidate %d start at = %q, want %q", i, got, want)
				}
				if got, want := candidate.Summary, tc.title; got != want {
					t.Fatalf("candidate %d summary = %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestParseUniversityPerformanceVenuesDetailPageSkipsUnknownNonMusicAndInvalidVenueVariants(t *testing.T) {
	tests := []struct {
		name        string
		title       string
		dates       string
		venue       string
		times       string
		cost        string
		description string
		wantReason  string
	}{
		{
			name:        "unknown venue",
			title:       "Unknown Venue Event",
			dates:       "Friday 7th August, 2026",
			venue:       "Firth Court",
			times:       "7:30 pm",
			cost:        "£15",
			description: "Live music at an unrecognized venue.",
			wantReason:  "unsupported venue",
		},
		{
			name:        "ambiguous venue",
			title:       "Ambiguous Venue Event",
			dates:       "Friday 7th August, 2026",
			venue:       "Octagon Centre / Firth Hall",
			times:       "7:30 pm",
			cost:        "£15",
			description: "Live music at two known venues.",
			wantReason:  "ambiguous venue",
		},
		{
			name:        "non music",
			title:       "University Awards Ceremony",
			dates:       "Friday 7th August, 2026",
			venue:       "Drama Studio",
			times:       "7:30 pm",
			cost:        "£15",
			description: "Join us for the annual awards ceremony.",
			wantReason:  "non-music event",
		},
		{
			name:        "generic live wording is not music",
			title:       "Campus Life Live",
			dates:       "Friday 7th August, 2026",
			venue:       "Drama Studio",
			times:       "7:30 pm",
			cost:        "£15",
			description: "A live hosted evening with guests and audience questions.",
			wantReason:  "non-music event",
		},
		{
			name:        "generic tour wording is not music",
			title:       "Octagon Centre Tour",
			dates:       "Friday 7th August, 2026",
			venue:       "Octagon Centre",
			times:       "7:30 pm",
			cost:        "Free",
			description: "Tour the venue after hours with University guides.",
			wantReason:  "non-music event",
		},
		{
			name:        "missing year",
			title:       "Missing Year Event",
			dates:       "Friday 7th August",
			venue:       "Octagon Centre",
			times:       "7:30 pm",
			cost:        "£15",
			description: "Live music.",
			wantReason:  "missing deterministic year",
		},
		{
			name:        "missing time",
			title:       "Missing Time Event",
			dates:       "Friday 7th August, 2026",
			venue:       "Firth Hall",
			times:       "",
			cost:        "£15",
			description: "Live music.",
			wantReason:  "missing event start time",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseUniversityPerformanceVenuesDetailPage(
				"https://performancevenues.group.shef.ac.uk/event/"+strings.ReplaceAll(strings.ToLower(tc.title), " ", "-")+"/",
				universityPerformanceVenuesDetailHTML(tc.title, tc.dates, tc.venue, tc.times, tc.cost, tc.description, ""),
			)
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Candidates), 0; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			if got, want := len(result.Skips), 1; got != want {
				t.Fatalf("skips = %#v, want 1", result.Skips)
			}
			if got, want := result.Skips[0].Reason, tc.wantReason; got != want {
				t.Fatalf("skip reason = %q, want %q", got, want)
			}
		})
	}
}

func TestParseUniversityPerformanceVenuesDetailPageIgnoresChromeMusicText(t *testing.T) {
	raw := []byte(`
		<html>
			<head><title>Guided Open Evening - Performance Venues</title></head>
			<body>
				<header><a href="/whats-on/">Live music, concerts and choir listings</a></header>
				<h1>Guided Open Evening</h1>
				<p>Explore the building after hours with University staff.</p>
				<section class="event-details">
					<p><strong>Dates:</strong> Friday 7th August, 2026</p>
					<p><strong>Venue:</strong> Octagon Centre</p>
					<p><strong>Times:</strong> 7:30 pm</p>
					<p><strong>Cost:</strong> Free</p>
				</section>
				<footer>Music and concert venue information</footer>
			</body>
		</html>
	`)

	result := ParseUniversityPerformanceVenuesDetailPage("https://performancevenues.group.shef.ac.uk/our-events/guided-open-evening/", raw)
	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
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

func universityPerformanceVenuesListingHTML(links ...string) []byte {
	var b strings.Builder
	b.WriteString("<html><body>")
	for _, link := range links {
		fmt.Fprintf(&b, `<a href="%s">Event</a>`, link)
	}
	b.WriteString("</body></html>")
	return []byte(b.String())
}

func universityPerformanceVenuesDetailHTML(title, dates, venue, times, cost, description, ticketURL string) []byte {
	var b strings.Builder
	b.WriteString("<html><head>")
	fmt.Fprintf(&b, "<title>%s - Performance Venues</title>", title)
	b.WriteString("</head><body>")
	fmt.Fprintf(&b, "<h1>%s</h1>", title)
	if strings.TrimSpace(description) != "" {
		fmt.Fprintf(&b, "<p>%s</p>", description)
	}
	b.WriteString("<section class=\"event-details\">")
	fmt.Fprintf(&b, "<p><strong>Dates:</strong> %s</p>", dates)
	fmt.Fprintf(&b, "<p><strong>Venue:</strong> %s</p>", venue)
	if strings.TrimSpace(times) != "" {
		fmt.Fprintf(&b, "<p><strong>Times:</strong> %s</p>", times)
	}
	fmt.Fprintf(&b, "<p><strong>Cost:</strong> %s</p>", cost)
	b.WriteString("</section>")
	if strings.TrimSpace(ticketURL) != "" {
		fmt.Fprintf(&b, `<a href="%s">Book Now</a>`, ticketURL)
	}
	b.WriteString("</body></html>")
	return []byte(b.String())
}
