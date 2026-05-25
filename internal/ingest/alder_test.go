package ingest

import (
	"context"
	"encoding/json"
	"html"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAlderListingLinksExtractOfficialEventPages(t *testing.T) {
	got, err := alder_listing_links("https://linktr.ee/alderbar", alderLinktreeFixture(
		"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195?aff=ebdssbdestsearch",
		"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111?aff=ebdsoporgprofile",
		"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
		"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
		"https://www.bandsintown.com/e/108234702?app_id=spt_feed...",
		"https://www.eventbrite.co.uk/o/kelham-island-film-club-27500055251",
		"https://instagram.com/alder_bar_sheff",
		"https://www.facebook.com/AlderBarSheff",
		"https://linktr.ee/newmustrad",
		"https://www.ticketsource.co.uk/fishpietheatre",
	), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}

	want := []string{
		"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
		"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
		"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
		"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestAlderListingLinksRejectsSocialAndBlockedLinks(t *testing.T) {
	got, err := alder_listing_links("https://linktr.ee/alderbar", alderLinktreeFixture(
		"https://www.bandsintown.com/e/108234702?app_id=spt_feed...",
		"https://www.eventbrite.co.uk/o/kelham-island-film-club-27500055251",
		"https://instagram.com/alder_bar_sheff",
		"https://www.facebook.com/AlderBarSheff",
		"https://linktr.ee/newmustrad",
		"https://www.ticketsource.co.uk/fishpietheatre",
	), 10)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("links = %#v, want none", got)
	}
}

func TestParseAlderEventDetailPageAcceptsEventbriteFestival(t *testing.T) {
	pageURL := "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195?aff=ebdssbdestsearch"
	result := ParseAlderEventDetailPage(pageURL, alderEventbriteFixture(t, map[string]any{
		"@context":    "https://schema.org",
		"@type":       "Festival",
		"name":        "Kelham Pride 26 @ Alder",
		"description": "Nine Ladies Promotions brings Clitspit, Jeanpool, Lavender Gray & Public Commodity to Alder. Crimped closing with 80s bangers till 1am.",
		"url":         pageURL,
		"location": map[string]any{
			"@type": "Place",
			"name":  "Alder",
			"address": map[string]any{
				"@type":           "PostalAddress",
				"streetAddress":   "Percy Street, #Unit 111, Neepsend, S3 8BT",
				"addressLocality": "Neepsend",
				"addressRegion":   "England",
				"addressCountry":  "GB",
			},
		},
		"startDate": "2026-06-20T17:00:00+01:00",
		"endDate":   "2026-06-21T01:00:00+01:00",
		"performer": []any{
			map[string]any{"@type": "Person", "name": "Clitspit"},
			map[string]any{"@type": "Person", "name": "Jeanpool"},
			map[string]any{"@type": "Person", "name": "Lavender Grey"},
			map[string]any{"@type": "Person", "name": "Public Commodity"},
		},
		"organizer": map[string]any{
			"@type":       "Organization",
			"name":        "Alder",
			"description": "Whats on at Alder!",
		},
	}))

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
	if got, want := candidate.UID, pageURL; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := candidate.Summary, "Kelham Pride 26 @ Alder"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, alderVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.LocationRaw, "Percy Street") {
		t.Fatalf("location raw = %q, want Percy Street evidence", candidate.LocationRaw)
	}
	if got, want := candidate.StartAt, "2026-06-20T16:00:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-06-21T00:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.Description, "80s bangers") {
		t.Fatalf("description = %q, want music evidence", candidate.Description)
	}
}

func TestParseAlderEventDetailPageAcceptsFatsomaGigs(t *testing.T) {
	pageURL := "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale"
	result := ParseAlderEventDetailPage(pageURL, alderFatsomaFixture(t, map[string]any{
		"type": "events",
		"id":   "event-1",
		"attributes": map[string]any{
			"name":                 "Bad Luck Crowd / Indigo Run / Tom Hale",
			"description":          `<p>Bad Luck Crowd</p><p>Indigo Run</p><p>Tom Hale</p><p>Alder Bar - Sheffield</p>`,
			"starts-at":            "2026-05-29T19:45:00+01:00",
			"ends-at":              "2026-05-29T23:00:00+01:00",
			"announcement-message": "Really pleased to announce that we'll be back playing Alder in Sheffield on May 29th.",
		},
		"relationships": map[string]any{
			"categories": map[string]any{
				"data": []any{
					map[string]any{"type": "categories", "id": "category-1"},
				},
			},
			"location": map[string]any{
				"data": map[string]any{"type": "locations", "id": "location-1"},
			},
		},
	}, map[string]any{
		"type": "categories",
		"id":   "category-1",
		"attributes": map[string]any{
			"name": "Gigs",
		},
	}, map[string]any{
		"type": "locations",
		"id":   "location-1",
		"attributes": map[string]any{
			"name":    "Alder",
			"address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK",
		},
	}))

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
	if got, want := candidate.Summary, "Bad Luck Crowd / Indigo Run / Tom Hale"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, alderVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.LocationRaw, "Percy St") {
		t.Fatalf("location raw = %q, want address evidence", candidate.LocationRaw)
	}
	if got, want := candidate.StartAt, "2026-05-29T18:45:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-05-29T22:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.Description, "Indigo Run") {
		t.Fatalf("description = %q, want band evidence", candidate.Description)
	}
}

func TestParseAlderEventDetailPageAcceptsTicketpassLiveMusic(t *testing.T) {
	pageURL := "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder"
	result := ParseAlderEventDetailPage(pageURL, alderTicketpassFixture(t, map[string]any{
		"component": "CustomWebsite/MarketingEventPage",
		"props": map[string]any{
			"event": map[string]any{
				"name":        "SHRUB x Worm Boys @ Alder",
				"description": "<p>Double bill of garden themed bands for your live music viewing pleasure down at the lovely Alder.</p><p>Doors: 7.30pm</p><p>Worm Boys: 8pm</p><p>Shrub: 8.50pm</p>",
				"eventDates": []any{
					map[string]any{
						"startTime": "2026-05-21T18:30:00.000000Z",
						"endTime":   "2026-05-21T21:30:00.000000Z",
					},
				},
				"venue": map[string]any{
					"name":    "Alder",
					"address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK",
				},
			},
		},
	}))

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
	if got, want := candidate.Summary, "SHRUB x Worm Boys @ Alder"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := candidate.Location, alderVenueName; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := candidate.StartAt, "2026-05-21T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := candidate.EndAt, "2026-05-21T21:30:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}
	if !strings.Contains(candidate.Description, "live music viewing pleasure") {
		t.Fatalf("description = %q, want live music evidence", candidate.Description)
	}
}

func TestParseAlderEventDetailPageRejectsNonMusicAndVenueIssues(t *testing.T) {
	tests := []struct {
		name       string
		pageURL    string
		body       []byte
		wantReason string
	}{
		{
			name:    "spoken word eventbrite",
			pageURL: "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111?aff=ebdsoporgprofile",
			body: alderEventbriteFixture(t, map[string]any{
				"@context":    "https://schema.org",
				"@type":       "Event",
				"name":        "Dave Tonge - Beguiling for beginners",
				"description": "A medieval motley collection of tales about cunning men and cunning women, coney catching and all manner of counterfeit craftiness.",
				"url":         "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
				"location": map[string]any{
					"@type": "Place",
					"name":  "Alder",
					"address": map[string]any{
						"streetAddress": "Percy Street, #Unit 111, Neepsend, S3 8BT",
					},
				},
				"startDate": "2026-05-19T19:30:00+01:00",
				"endDate":   "2026-05-19T21:30:00+01:00",
				"organizer": map[string]any{
					"@type":       "Organization",
					"name":        "The Story Forge",
					"description": "Meeting every third tuesday of the month we are the only spoken word night in Sheffield with the focus of traditional folktales, myths and legends.",
				},
			}),
			wantReason: "non-music event",
		},
		{
			name:    "ambiguous venue",
			pageURL: "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195?aff=ebdssbdestsearch",
			body: alderEventbriteFixture(t, map[string]any{
				"@context":    "https://schema.org",
				"@type":       "Festival",
				"name":        "Kelham Pride 26 @ Alder",
				"description": "A live music festival with bands and bangers.",
				"url":         "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
				"location": map[string]any{
					"@type": "Place",
					"name":  "Alder / Somewhere Else",
					"address": map[string]any{
						"streetAddress": "Percy Street, #Unit 111, Neepsend, S3 8BT",
					},
				},
				"startDate": "2026-06-20T17:00:00+01:00",
				"endDate":   "2026-06-21T01:00:00+01:00",
			}),
			wantReason: "unsupported venue",
		},
		{
			name:    "missing start time",
			pageURL: "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
			body: alderTicketpassFixture(t, map[string]any{
				"component": "CustomWebsite/MarketingEventPage",
				"props": map[string]any{
					"event": map[string]any{
						"name":        "SHRUB x Worm Boys @ Alder",
						"description": "<p>Double bill of bands for your live music viewing pleasure.</p>",
						"eventDates": []any{
							map[string]any{
								"startTime": "",
								"endTime":   "2026-05-21T21:30:00.000000Z",
							},
						},
						"venue": map[string]any{
							"name":    "Alder",
							"address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK",
						},
					},
				},
			}),
			wantReason: "missing event start time",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseAlderEventDetailPage(tc.pageURL, tc.body)
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

func TestRunManualAlderFetchesOnlyOfficialLinkedDelegatedPages(t *testing.T) {
	ctx := context.Background()
	calls := []string{}
	store := &fakeStore{now: time.Date(2026, 5, 25, 18, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://linktr.ee/alderbar": {
				URL:         "https://linktr.ee/alderbar",
				FinalURL:    "https://linktr.ee/alderbar",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body: alderLinktreeFixture(
					"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195?aff=ebdssbdestsearch",
					"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111?aff=ebdsoporgprofile",
					"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
					"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
					"https://www.bandsintown.com/e/108234702?app_id=spt_feed...",
					"https://www.eventbrite.co.uk/o/kelham-island-film-club-27500055251",
					"https://instagram.com/alder_bar_sheff",
					"https://www.facebook.com/AlderBarSheff",
					"https://linktr.ee/newmustrad",
					"https://www.ticketsource.co.uk/fishpietheatre",
				),
				CapturedAt: time.Date(2026, 5, 25, 18, 1, 0, 0, time.UTC),
			},
			"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195": {
				URL:         "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
				FinalURL:    "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        alderEventbriteFixture(t, alderEventbriteNode("https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195", "Kelham Pride 26 @ Alder", "A live music festival with bands and bangers.", "Alder", "Percy Street, #Unit 111, Neepsend, S3 8BT", "2026-06-20T17:00:00+01:00", "2026-06-21T01:00:00+01:00")),
				CapturedAt:  time.Date(2026, 5, 25, 18, 2, 0, 0, time.UTC),
			},
			"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111": {
				URL:         "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
				FinalURL:    "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        alderEventbriteFixture(t, alderEventbriteNode("https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111", "Dave Tonge - Beguiling for beginners", "A medieval motley collection of tales about cunning men and cunning women, coney catching and all manner of counterfeit craftiness.", "Alder", "Percy Street, #Unit 111, Neepsend, S3 8BT", "2026-05-19T19:30:00+01:00", "2026-05-19T21:30:00+01:00")),
				CapturedAt:  time.Date(2026, 5, 25, 18, 3, 0, 0, time.UTC),
			},
			"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale": {
				URL:         "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
				FinalURL:    "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        alderFatsomaFixture(t, alderFatsomaEventNode("Bad Luck Crowd / Indigo Run / Tom Hale", `<p>Bad Luck Crowd</p><p>Indigo Run</p><p>Tom Hale</p><p>Alder Bar - Sheffield</p>`, "2026-05-29T19:45:00+01:00", "2026-05-29T23:00:00+01:00", "Really pleased to announce that we'll be back playing Alder in Sheffield on May 29th."), map[string]any{"type": "categories", "id": "category-1", "attributes": map[string]any{"name": "Gigs"}}, map[string]any{"type": "locations", "id": "location-1", "attributes": map[string]any{"name": "Alder", "address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK"}}),
				CapturedAt:  time.Date(2026, 5, 25, 18, 4, 0, 0, time.UTC),
			},
			"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder": {
				URL:         "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
				FinalURL:    "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        alderTicketpassFixture(t, alderTicketpassEventPage("SHRUB x Worm Boys @ Alder", "<p>Double bill of bands for your live music viewing pleasure.</p>", "2026-05-21T18:30:00.000000Z", "2026-05-21T21:30:00.000000Z")),
				CapturedAt:  time.Date(2026, 5, 25, 18, 5, 0, 0, time.UTC),
			},
		},
		calls: &calls,
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: AlderSource, Limit: 10})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	wantCalls := []string{
		"https://linktr.ee/alderbar",
		"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
		"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
		"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
		"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("fetch calls = %#v, want %#v", calls, wantCalls)
	}
	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 4; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 4; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func TestReviewClustersFromAlderReportUsesOwnedVenueAuthority(t *testing.T) {
	catalog, err := DefaultCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}

	report := Report{
		Source:      AlderSource,
		SourceURL:   "https://linktr.ee/alderbar",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
				Candidates: []EventCandidate{
					{
						UID:         "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
						Summary:     "Kelham Pride 26 @ Alder",
						Location:    alderVenueName,
						LocationRaw: "Alder, Percy Street, #Unit 111, Neepsend, S3 8BT",
						URL:         "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
						StartAt:     "2026-06-20T16:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReportWithCatalog(catalog, report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, alderVenueNameSlug(); got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceName, "Alder manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceURL, "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceEventKey, "url:https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestReplayImportRunRebuildsAlderReportFromRecognizedDetailPages(t *testing.T) {
	finishedAt := time.Date(2026, 5, 25, 19, 0, 0, 0, time.UTC)
	store := fakeReplayStore{
		run: ReplayRun{
			ID:         91,
			StartedAt:  time.Date(2026, 5, 25, 18, 0, 0, 0, time.UTC),
			FinishedAt: &finishedAt,
			Status:     "succeeded",
			Notes:      "links=4 candidates=3 skips=1 errors=0",
			Snapshots: []ReplaySnapshot{
				{
					ID:         901,
					SourceName: "Alder listings",
					SourceURL:  "https://linktr.ee/alderbar",
					CapturedAt: time.Date(2026, 5, 25, 18, 1, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://linktr.ee/alderbar",
						FinalURL:    "https://linktr.ee/alderbar",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body: alderLinktreeFixture(
							"https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195?aff=ebdssbdestsearch",
							"https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111?aff=ebdsoporgprofile",
							"https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
							"https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
						),
						CapturedAt: time.Date(2026, 5, 25, 18, 1, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         902,
					SourceName: "Alder linked event detail page",
					SourceURL:  "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
					CapturedAt: time.Date(2026, 5, 25, 18, 2, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
						FinalURL:    "https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        alderEventbriteFixture(t, alderEventbriteNode("https://www.eventbrite.com/e/kelham-pride-26-alder-tickets-1989007239195", "Kelham Pride 26 @ Alder", "A live music festival with bands and bangers.", "Alder", "Percy Street, #Unit 111, Neepsend, S3 8BT", "2026-06-20T17:00:00+01:00", "2026-06-21T01:00:00+01:00")),
						CapturedAt:  time.Date(2026, 5, 25, 18, 2, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         903,
					SourceName: "Alder linked event detail page",
					SourceURL:  "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
					CapturedAt: time.Date(2026, 5, 25, 18, 3, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
						FinalURL:    "https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        alderEventbriteFixture(t, alderEventbriteNode("https://www.eventbrite.co.uk/e/dave-tonge-beguiling-for-beginners-tickets-1988271009111", "Dave Tonge - Beguiling for beginners", "A medieval motley collection of tales about cunning men and cunning women, coney catching and all manner of counterfeit craftiness.", "Alder", "Percy Street, #Unit 111, Neepsend, S3 8BT", "2026-05-19T19:30:00+01:00", "2026-05-19T21:30:00+01:00")),
						CapturedAt:  time.Date(2026, 5, 25, 18, 3, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         904,
					SourceName: "Alder linked event detail page",
					SourceURL:  "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
					CapturedAt: time.Date(2026, 5, 25, 18, 4, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
						FinalURL:    "https://www.fatsoma.com/e/8muxb2li/bad-luck-crowd-indigo-run-tom-hale",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        alderFatsomaFixture(t, alderFatsomaEventNode("Bad Luck Crowd / Indigo Run / Tom Hale", `<p>Bad Luck Crowd</p><p>Indigo Run</p><p>Tom Hale</p><p>Alder Bar - Sheffield</p>`, "2026-05-29T19:45:00+01:00", "2026-05-29T23:00:00+01:00", "Really pleased to announce that we'll be back playing Alder in Sheffield on May 29th."), map[string]any{"type": "categories", "id": "category-1", "attributes": map[string]any{"name": "Gigs"}}, map[string]any{"type": "locations", "id": "location-1", "attributes": map[string]any{"name": "Alder", "address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK"}}),
						CapturedAt:  time.Date(2026, 5, 25, 18, 4, 0, 0, time.UTC),
					}, nil),
				},
				{
					ID:         905,
					SourceName: "Alder linked event detail page",
					SourceURL:  "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
					CapturedAt: time.Date(2026, 5, 25, 18, 5, 0, 0, time.UTC),
					Payload: mustReplaySnapshotPayload(t, FetchResult{
						URL:         "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
						FinalURL:    "https://ticketpass.org/event/EJGDKI/shrub-x-worm-boys-at-alder",
						Status:      "200 OK",
						StatusCode:  200,
						ContentType: "text/html",
						Body:        alderTicketpassFixture(t, alderTicketpassEventPage("SHRUB x Worm Boys @ Alder", "<p>Double bill of bands for your live music viewing pleasure.</p>", "2026-05-21T18:30:00.000000Z", "2026-05-21T21:30:00.000000Z")),
						CapturedAt:  time.Date(2026, 5, 25, 18, 5, 0, 0, time.UTC),
					}, nil),
				},
			},
		},
	}

	report, err := ReplayImportRun(context.Background(), store, 91, ReplayOptions{Limit: 10})
	if err != nil {
		t.Fatalf("replay import run: %v", err)
	}
	if got, want := report.Source, AlderSource; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, want := len(report.Links), 4; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 4; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func alderLinktreeFixture(links ...string) []byte {
	var b strings.Builder
	b.WriteString("<!doctype html><html><body>")
	for _, link := range links {
		b.WriteString(`<a href="`)
		b.WriteString(link)
		b.WriteString(`">Link</a>`)
	}
	b.WriteString("</body></html>")
	return []byte(b.String())
}

func alderEventbriteFixture(t *testing.T, node map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("marshal eventbrite fixture: %v", err)
	}
	return []byte(`<!doctype html><html><head><script type="application/ld+json">` + string(raw) + `</script></head><body></body></html>`)
}

func alderEventbriteNode(pageURL, title, description, venueName, venueAddress, startAt, endAt string) map[string]any {
	return map[string]any{
		"@context":    "https://schema.org",
		"@type":       "Festival",
		"name":        title,
		"description": description,
		"url":         pageURL,
		"location": map[string]any{
			"@type": "Place",
			"name":  venueName,
			"address": map[string]any{
				"@type":         "PostalAddress",
				"streetAddress": venueAddress,
			},
		},
		"startDate": startAt,
		"endDate":   endAt,
	}
}

func alderFatsomaFixture(t *testing.T, event map[string]any, included ...map[string]any) []byte {
	t.Helper()
	payload := map[string]any{
		"data":     []any{event},
		"included": make([]any, 0, len(included)),
		"meta":     map[string]any{"author": "Fatsoma"},
	}
	for _, item := range included {
		payload["included"] = append(payload["included"].([]any), item)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal fatsoma fixture: %v", err)
	}
	encoded, err := json.Marshal(string(raw))
	if err != nil {
		t.Fatalf("encode fatsoma fixture: %v", err)
	}
	return []byte(`<!doctype html><html><body><script type="fastboot/shoebox">` + string(encoded) + `</script></body></html>`)
}

func alderFatsomaEventNode(title, description, startAt, endAt, announcement string) map[string]any {
	return map[string]any{
		"type": "events",
		"id":   "event-1",
		"attributes": map[string]any{
			"name":                 title,
			"description":          description,
			"starts-at":            startAt,
			"ends-at":              endAt,
			"announcement-message": announcement,
		},
		"relationships": map[string]any{
			"categories": map[string]any{
				"data": []any{map[string]any{"type": "categories", "id": "category-1"}},
			},
			"location": map[string]any{
				"data": map[string]any{"type": "locations", "id": "location-1"},
			},
		},
	}
}

func alderTicketpassFixture(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal ticketpass fixture: %v", err)
	}
	return []byte(`<!doctype html><html><body><div id="app" data-page="` + html.EscapeString(string(raw)) + `"></div></body></html>`)
}

func alderTicketpassEventPage(title, description, startAt, endAt string) map[string]any {
	return map[string]any{
		"component": "CustomWebsite/MarketingEventPage",
		"props": map[string]any{
			"event": map[string]any{
				"name":        title,
				"description": description,
				"eventDates": []any{
					map[string]any{
						"startTime": startAt,
						"endTime":   endAt,
					},
				},
				"venue": map[string]any{
					"name":    "Alder",
					"address": "Unit 111, J C Albyn Complex, Percy St, Neepsend, Sheffield S3 8BT, UK",
				},
			},
		},
	}
}

func alderVenueNameSlug() string {
	return "alder"
}
