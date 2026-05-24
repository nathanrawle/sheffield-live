package ingest

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/review"
)

func TestNetworkSheffieldDetailLinksExtractEventPages(t *testing.T) {
	got, err := network_sheffield_detail_links("https://www.networksheffield.co.uk/events/", readFixture(t, "network_sheffield_events.html"), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.networksheffield.co.uk/event/park-drive/",
		"https://www.networksheffield.co.uk/event/godeth-network-sheffield/",
		"https://www.networksheffield.co.uk/event/as-it-is-album-launch-show-matinee-arundel-emporium/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestNetworkSheffieldDetailPageParsesNetworkAndRoomLabels(t *testing.T) {
	tests := []struct {
		name           string
		pageURL        string
		fixture        string
		wantSummary    string
		wantLocation   string
		wantRoomText   string
		wantRoomSlugs  string
		wantExternalID string
	}{
		{
			name:           "room",
			pageURL:        "https://www.networksheffield.co.uk/event/godeth-network-sheffield/",
			fixture:        "network_sheffield_detail_room.html",
			wantSummary:    "GODETH | Network 3",
			wantLocation:   "Network 3",
			wantRoomText:   "Network 3",
			wantRoomSlugs:  "network-3",
			wantExternalID: "https://www.fatsoma.com/e/o0hkx7ur/godeth-network-3",
		},
		{
			name:           "plain",
			pageURL:        "https://www.networksheffield.co.uk/event/park-drive/",
			fixture:        "network_sheffield_detail_plain.html",
			wantSummary:    "Park Drive",
			wantLocation:   "Network",
			wantRoomText:   "",
			wantRoomSlugs:  "",
			wantExternalID: "https://www.fatsoma.com/e/vv1i1awd/park-drive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseNetworkSheffieldDetailPage(tc.pageURL, readFixture(t, tc.fixture))
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
			if got, want := candidate.Summary, tc.wantSummary; got != want {
				t.Fatalf("summary = %q, want %q", got, want)
			}
			if got, want := candidate.Location, tc.wantLocation; got != want {
				t.Fatalf("location = %q, want %q", got, want)
			}
			if got, want := candidate.RoomText, tc.wantRoomText; got != want {
				t.Fatalf("room text = %q, want %q", got, want)
			}
			if got, want := candidate.Rooms, roomCandidatesFromSlugs(tc.wantRoomSlugs); !reflect.DeepEqual(got, want) {
				t.Fatalf("rooms = %#v, want %#v", got, want)
			}
			if got, want := candidate.UID, tc.wantExternalID; got != want {
				t.Fatalf("external id = %q, want %q", got, want)
			}
			if got, want := candidate.URL, tc.pageURL; got != want {
				t.Fatalf("url = %q, want %q", got, want)
			}
			if got, want := VenueSlugForSourceLocation(NetworkSheffieldSource, candidate.Location), networkSheffieldVenueSlug; got != want {
				t.Fatalf("venue slug = %q, want %q", got, want)
			}
		})
	}
}

func TestNetworkSheffieldDetailPageMapsAllExplicitRoomLabels(t *testing.T) {
	tests := []struct {
		label string
		slug  string
	}{
		{label: "Network 1", slug: "network-1"},
		{label: "Network 2", slug: "network-2"},
		{label: "Network 3", slug: "network-3"},
	}

	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			body := networkSheffieldDetailFixture(
				"Room Test | "+tc.label,
				"Network",
				"https://www.fatsoma.com/e/test/"+tc.slug,
				"An explicit room label should map to a concrete room candidate",
			)

			result := ParseNetworkSheffieldDetailPage("https://www.networksheffield.co.uk/event/"+tc.slug+"/", body)
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Candidates), 1; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			candidate := result.Candidates[0]
			if got, want := candidate.RoomText, tc.label; got != want {
				t.Fatalf("room text = %q, want %q", got, want)
			}
			if got, want := candidate.Rooms, roomCandidatesFromSlugs(tc.slug); !reflect.DeepEqual(got, want) {
				t.Fatalf("rooms = %#v, want %#v", got, want)
			}
		})
	}
}

func TestNetworkSheffieldDetailPageDoesNotCreateRoomsForWholeVenueWording(t *testing.T) {
	tests := []string{
		"Three stages at Network",
		"All rooms at Network",
		"Whole venue takeover at Network",
		"Network 3 stages festival",
	}

	for _, title := range tests {
		t.Run(title, func(t *testing.T) {
			body := networkSheffieldDetailFixture(
				title,
				"Network",
				"https://www.fatsoma.com/e/test/whole-venue",
				"Whole-venue wording should not create a concrete room",
			)

			result := ParseNetworkSheffieldDetailPage("https://www.networksheffield.co.uk/event/whole-venue/", body)
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Candidates), 1; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			candidate := result.Candidates[0]
			if got, want := candidate.Location, "Network"; got != want {
				t.Fatalf("location = %q, want %q", got, want)
			}
			if got := candidate.RoomText; got != "" {
				t.Fatalf("room text = %q, want empty", got)
			}
			if got := candidate.Rooms; len(got) != 0 {
				t.Fatalf("rooms = %#v, want none", got)
			}
		})
	}
}

func TestNetworkSheffieldDetailPageAcceptsNetworkSheffieldVenueLabel(t *testing.T) {
	body := networkSheffieldDetailFixture(
		"Network Sheffield Launch Party",
		"Network Sheffield",
		"https://www.fatsoma.com/e/test/network-sheffield-launch-party",
		"A Network Sheffield venue label should be accepted",
	)

	result := ParseNetworkSheffieldDetailPage("https://www.networksheffield.co.uk/event/network-sheffield-launch-party/", body)
	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	if got, want := candidate.Location, "Network Sheffield"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.Summary, "Network Sheffield Launch Party"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := VenueSlugForSourceLocation(NetworkSheffieldSource, candidate.Location), networkSheffieldVenueSlug; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
}

func TestNetworkSheffieldDetailPageRejectsAdjacentAndUnknownVenues(t *testing.T) {
	tests := []struct {
		name        string
		title       string
		venue       string
		wantSummary string
	}{
		{
			name:        "arundel emporium",
			title:       "As It Is - Album Launch Show - Matinee | Arundel Emporium",
			venue:       "The Arundel Emporium",
			wantSummary: "As It Is - Album Launch Show - Matinee | Arundel Emporium",
		},
		{
			name:        "earls yard",
			title:       "Example | Earl's Yard",
			venue:       "Earl's Yard",
			wantSummary: "Example | Earl's Yard",
		},
		{
			name:        "record junkee",
			title:       "Example | Record Junkee",
			venue:       "Record Junkee",
			wantSummary: "Example | Record Junkee",
		},
		{
			name:        "unknown venue",
			title:       "Example | Somewhere Else",
			venue:       "Somewhere Else",
			wantSummary: "Example | Somewhere Else",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := networkSheffieldDetailFixture(tc.title, tc.venue, "https://www.fatsoma.com/e/test/"+strings.ReplaceAll(strings.ToLower(tc.name), " ", "-"), "A rejected listing")
			result := ParseNetworkSheffieldDetailPage("https://www.networksheffield.co.uk/event/test/", body)
			if got, want := len(result.Errors), 0; got != want {
				t.Fatalf("errors = %#v, want none", result.Errors)
			}
			if got, want := len(result.Candidates), 0; got != want {
				t.Fatalf("candidates = %d, want %d", got, want)
			}
			if got, want := len(result.Skips), 1; got != want {
				t.Fatalf("skips = %d, want %d", got, want)
			}
			if got, want := result.Skips[0].Summary, tc.wantSummary; got != want {
				t.Fatalf("skip summary = %q, want %q", got, want)
			}
			if got, want := result.Skips[0].Reason, "unsupported venue"; got != want {
				t.Fatalf("skip reason = %q, want %q", got, want)
			}
		})
	}
}

func TestRunManualNetworkSheffieldParsesListingAndSkipsAdjacentPages(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)}
	listingURL := "https://www.networksheffield.co.uk/events/"
	parkDriveURL := "https://www.networksheffield.co.uk/event/park-drive/"
	godethURL := "https://www.networksheffield.co.uk/event/godeth-network-sheffield/"
	arundelURL := "https://www.networksheffield.co.uk/event/as-it-is-album-launch-show-matinee-arundel-emporium/"
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			listingURL: {
				URL:         listingURL,
				FinalURL:    listingURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "network_sheffield_events.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 1, 0, 0, time.UTC),
			},
			parkDriveURL: {
				URL:         parkDriveURL,
				FinalURL:    parkDriveURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "network_sheffield_detail_plain.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 2, 0, 0, time.UTC),
			},
			godethURL: {
				URL:         godethURL,
				FinalURL:    godethURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "network_sheffield_detail_room.html"),
				CapturedAt:  time.Date(2026, 5, 24, 12, 3, 0, 0, time.UTC),
			},
			arundelURL: {
				URL:         arundelURL,
				FinalURL:    arundelURL,
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: networkSheffieldDetailFixture(
					"As It Is - Album Launch Show - Matinee | Arundel Emporium",
					"The Arundel Emporium",
					"https://www.fatsoma.com/e/nfxp68iq/as-it-is-album-launch-show-matinee-arundel-emporium",
					"Rejected adjacent venue",
				),
				CapturedAt: time.Date(2026, 5, 24, 12, 4, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: NetworkSheffieldSource, Limit: 10})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}
	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 3; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 3; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(store.snapshots), 4; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}

	var roomCluster ReviewStageClusterInput
	var plainCluster ReviewStageClusterInput
	clusters := ReviewClustersFromReportWithCatalog(mustLoadRepoCatalog(t), report)
	if got, want := len(clusters), 2; got != want {
		t.Fatalf("review clusters = %d, want %d", got, want)
	}
	for _, cluster := range clusters {
		if len(cluster.Candidates) == 0 {
			continue
		}
		candidate := cluster.Candidates[0]
		switch candidate.RoomText {
		case "Network 3":
			roomCluster = cluster
		case "":
			plainCluster = cluster
		}
	}

	if len(roomCluster.Candidates) != 1 {
		t.Fatal("room-specific cluster not found")
	}
	roomCandidate := roomCluster.Candidates[0]
	if got, want := roomCandidate.VenueText, "Network 3"; got != want {
		t.Fatalf("room venue text = %q, want %q", got, want)
	}
	if got, want := roomCandidate.VenueLocationRaw, "Network 3, 14 Matilda St, Sheffield City Centre, Sheffield S1, UK"; got != want {
		t.Fatalf("room venue location raw = %q, want %q", got, want)
	}
	if got, want := roomCandidate.RoomText, "Network 3"; got != want {
		t.Fatalf("room text = %q, want %q", got, want)
	}
	if got, want := review.RoomSlugsValue(roomCandidate.Rooms), "network-3"; got != want {
		t.Fatalf("room slugs = %q, want %q", got, want)
	}
	if got, want := roomCluster.AuthoritativeSourceName, "Network Sheffield manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := roomCluster.AuthoritativeSourceURL, godethURL; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if roomCluster.AuthoritativeSourceEventKey == "" {
		t.Fatal("authoritative source event key is empty")
	}

	if len(plainCluster.Candidates) != 1 {
		t.Fatal("plain cluster not found")
	}
	if got, want := plainCluster.Candidates[0].VenueText, "Network"; got != want {
		t.Fatalf("plain venue text = %q, want %q", got, want)
	}
	if got, want := plainCluster.AuthoritativeSourceURL, parkDriveURL; got != want {
		t.Fatalf("plain authoritative source url = %q, want %q", got, want)
	}

	arundelCalendar := findCalendarReport(report.Calendars, arundelURL)
	if arundelCalendar == nil {
		t.Fatal("arundel calendar missing")
	}
	if got, want := len(arundelCalendar.Candidates), 0; got != want {
		t.Fatalf("arundel candidates = %d, want %d", got, want)
	}
	if got, want := len(arundelCalendar.Skips), 1; got != want {
		t.Fatalf("arundel skips = %d, want %d", got, want)
	}
	if got, want := arundelCalendar.Skips[0].Reason, "unsupported venue"; got != want {
		t.Fatalf("arundel skip reason = %q, want %q", got, want)
	}
}

func mustLoadRepoCatalog(t *testing.T) *Catalog {
	t.Helper()
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}
	return catalog
}

func findCalendarReport(calendars []CalendarReport, url string) *CalendarReport {
	for i := range calendars {
		if calendars[i].URL == url {
			return &calendars[i]
		}
	}
	return nil
}

func networkSheffieldDetailFixture(title, venue, externalURL, description string) []byte {
	return []byte(fmt.Sprintf(`<!doctype html>
<html lang="en-GB">
  <head>
    <title>%s | Network Sheffield</title>
    <script type="application/ld+json">[{
      "@context":"http://www.schema.org",
      "@type":"Event",
      "name":%q,
      "url":%q,
      "description":%q,
      "startDate":"2026-07-25T11:00",
      "endDate":"2026-07-25T14:00",
      "location":{
        "@type":"Place",
        "name":%q,
        "address":{
          "@type":"PostalAddress",
          "streetAddress":"16 Matilda St, Sheffield City Centre, Sheffield S1 4QD, UK",
          "addressLocality":"Sheffield",
          "addressCountry":"United Kingdom"
        }
      }
    }]</script>
  </head>
  <body>
    <h1 class="event-single__heading heading">
      <a class="event-single__heading-link" target="_blank" href=%q title=%q>%s</a>
    </h1>
  </body>
</html>`, title, title, externalURL, description, venue, externalURL, title, title))
}

func roomCandidatesFromSlugs(value string) []RoomCandidate {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	switch value {
	case "network-1":
		return []RoomCandidate{{Slug: "network-1", Name: "Network 1"}}
	case "network-2":
		return []RoomCandidate{{Slug: "network-2", Name: "Network 2"}}
	case "network-3":
		return []RoomCandidate{{Slug: "network-3", Name: "Network 3"}}
	default:
		return nil
	}
}
