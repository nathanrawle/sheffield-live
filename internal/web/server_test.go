package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store"
	sqlitestore "sheffield-live/internal/store/sqlite"
)

func TestRoutes(t *testing.T) {
	server, err := NewServer(store.NewSeedStore())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	tests := []struct {
		name string
		path string
		code int
		body string
	}{
		{name: "home", path: "/", code: http.StatusOK, body: "Sheffield live music"},
		{name: "events", path: "/events", code: http.StatusOK, body: "Upcoming shows"},
		{name: "event detail", path: "/events/matinee-noise-at-the-leadmill", code: http.StatusOK, body: "The Leadmill live music listings"},
		{name: "venues", path: "/venues", code: http.StatusOK, body: "Sheffield rooms"},
		{name: "venue detail", path: "/venues/leadmill", code: http.StatusOK, body: "Leadmill"},
		{name: "static css", path: "/static/site.css", code: http.StatusOK, body: "color-scheme"},
		{name: "healthz", path: "/healthz", code: http.StatusOK, body: "ok"},
		{name: "missing", path: "/events/missing", code: http.StatusNotFound, body: "404 page not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rr := httptest.NewRecorder()

			server.ServeHTTP(rr, req)

			if rr.Code != tc.code {
				t.Fatalf("status = %d, want %d", rr.Code, tc.code)
			}
			if !strings.Contains(rr.Body.String(), tc.body) {
				t.Fatalf("body missing %q in %q", tc.body, rr.Body.String())
			}
		})
	}
}

func TestNewServerRejectsMissingEventVenue(t *testing.T) {
	st := store.NewStore(nil, []domain.Event{
		{
			Slug:        "missing-venue",
			Name:        "Missing Venue",
			VenueSlug:   "not-a-venue",
			Start:       time.Date(2026, time.May, 8, 18, 0, 0, 0, time.UTC),
			End:         time.Date(2026, time.May, 8, 20, 0, 0, 0, time.UTC),
			SourceName:  "test",
			SourceURL:   "https://example.test",
			LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
		},
	})

	if _, err := NewServer(st); err == nil {
		t.Fatal("expected missing venue validation error")
	}
}

func TestNewServerAcceptsReadOnlyStore(t *testing.T) {
	st := readOnlyStoreStub{}

	if _, err := NewServer(st); err != nil {
		t.Fatalf("new server: %v", err)
	}
}

func TestSQLiteStoreSmoke(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	server, err := NewServer(st)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if !strings.Contains(rr.Body.String(), "ok") {
		t.Fatalf("body = %q, want ok", rr.Body.String())
	}
}

func TestHomeShowsTodayAndThisWeekWithFixedClock(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/")

	assertContains(t, body, "<h2>Today</h2>")
	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "<h2>This week</h2>")
	assertContains(t, body, "Tomorrow Yellow Arch")
	assertContains(t, body, "Friday Leadmill")
	assertNotContains(t, body, "Later Leadmill")
}

func TestHomeShowsEmptyStatesWithFixedClock(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(nil, nil))
	body := renderPath(t, server, "/")

	assertContains(t, body, "No shows listed for today.")
	assertContains(t, body, "No more shows listed this week.")
}

func TestEventsFiltersToday(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=today")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Sunday, 19 April 2026")
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Friday Leadmill")
}

func TestEventsFiltersWeekAndVenue(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=week&venue=leadmill")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Friday Leadmill")
	assertContains(t, body, `option value="week" selected`)
	assertContains(t, body, `option value="leadmill" selected`)
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Later Leadmill")
}

func TestEventsGroupsByLocalDateInOrder(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=week")

	assertInOrder(t, body, []string{
		"Sunday, 19 April 2026",
		"Tonight Leadmill",
		"Monday, 20 April 2026",
		"Tomorrow Yellow Arch",
		"Friday, 24 April 2026",
		"Friday Leadmill",
	})
}

func TestEventsShowsEmptyState(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=today&venue=yellow-arch")

	assertContains(t, body, "No shows match these filters.")
	assertNotContains(t, body, "Tonight Leadmill")
}

func TestEventsUnknownVenueBehavesLikeAllVenues(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?venue=missing")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Tomorrow Yellow Arch")
	assertContains(t, body, "Friday Leadmill")
	assertContains(t, body, "Later Leadmill")
	assertContains(t, body, `<option value="">All venues</option>`)
	assertNotContains(t, body, `option value="missing" selected`)
	assertNotContains(t, body, "No shows match these filters.")
}

func TestVenueDetailShowsEmptyState(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/venues/empty-room")

	assertContains(t, body, "No upcoming shows listed for this venue.")
}

func TestLayoutMetadataAndActiveNav(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events")

	assertContains(t, body, `<meta name="description" content="Browse Sheffield live music by date window and venue.">`)
	assertContains(t, body, `<a class="skip-link" href="#main">Skip to content</a>`)
	assertContains(t, body, `<main id="main" class="shell main">`)
	assertContains(t, body, `<a class="active" aria-current="page" href="/events">Events</a>`)
}

func TestAdminReviewListDetailAndSave(t *testing.T) {
	ctx := httptest.NewRequest(http.MethodGet, "/", nil).Context()
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	listBody := renderPath(t, server, "/admin/review")
	assertContains(t, listBody, "Review queue")
	assertContains(t, listBody, "Fixture review")
	assertContains(t, listBody, "2 candidates")

	detailBody := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, detailBody, "Canonical draft summary")
	assertContains(t, detailBody, "Not selected yet")
	assertContains(t, detailBody, "&mdash;")
	assertInOrder(t, detailBody, []string{"Canonical draft summary", "Saved draft preview"})
	assertContains(t, detailBody, "Saved draft preview")
	assertContains(t, detailBody, "Candidate 1")
	assertContains(t, detailBody, "Candidate 2")
	assertContains(t, detailBody, "fixture UID utc-1")
	assertContains(t, detailBody, `name="choice_name"`)
	assertContains(t, detailBody, `name="choice_start_at"`)

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	form := url.Values{}
	form.Set("choice_name", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_venue_slug", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_start_at", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_end_at", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_genre", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_status", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_description", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_source_name", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_source_url", strconvFormatInt(group.Candidates[1].ID))
	form.Set("action", "save")

	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	location := rr.Header().Get("Location")
	if location != "/admin/review/"+strconvFormatInt(groupID)+"?saved=1" {
		t.Fatalf("Location = %q, want saved review detail redirect", location)
	}

	saveBody := renderPath(t, server, location)
	assertContains(t, saveBody, "Draft saved.")
	assertContains(t, saveBody, "Canonical draft summary")
	assertContains(t, saveBody, "Candidate 1 (utc-1)")
	assertContains(t, saveBody, "Candidate 2 (london-1)")
	assertContains(t, saveBody, "London Show")
	assertNotContains(t, saveBody, "Not selected yet")
	assertInOrder(t, saveBody, []string{"Canonical draft summary", "Saved draft preview"})
	assertContains(t, saveBody, "<strong>Name</strong>: London Show")
	assertContains(t, saveBody, "<strong>Venue slug</strong>: sidney-and-matilda")
	assertContains(t, saveBody, `name="choice_name" value="`+strconvFormatInt(group.Candidates[1].ID)+`" checked`)
}

func TestBuildReviewDetailCanonicalSummaryKeepsBlankSelectionsDistinct(t *testing.T) {
	detail := buildReviewDetail(review.Group{
		Candidates: []review.Candidate{
			{ID: 1, Position: 1, Name: "First"},
			{ID: 2, Position: 2, Name: "Second"},
		},
		DraftChoices: map[review.Field]review.DraftChoice{
			review.FieldName: {
				Field:       review.FieldName,
				CandidateID: 2,
				Value:       "",
			},
		},
	})

	if got, want := len(detail.CanonicalSummaryRows), len(review.CanonicalFields); got != want {
		t.Fatalf("summary rows = %d, want %d", got, want)
	}
	first := detail.CanonicalSummaryRows[0]
	if !first.Selected {
		t.Fatal("name row = unselected, want selected")
	}
	if first.Value != "" {
		t.Fatalf("name value = %q, want blank", first.Value)
	}
	if first.Candidate != "Candidate 2" {
		t.Fatalf("name candidate = %q, want Candidate 2", first.Candidate)
	}
	second := detail.CanonicalSummaryRows[1]
	if second.Selected {
		t.Fatal("venue slug row = selected, want unselected")
	}
	if second.Candidate != "" {
		t.Fatalf("venue slug candidate = %q, want empty", second.Candidate)
	}
}

func TestAdminReviewQueueShowsOnlyOpenGroups(t *testing.T) {
	st, server, openGroupID := mustReviewServerWithGroup(t)
	defer st.Close()

	resolvedID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Resolved review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:resolved.ics",
		Candidates: []review.CandidateInput{
			{
				Name:       "Resolved candidate",
				SourceName: "Fixture ICS",
				SourceURL:  "file:resolved.ics",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	resolved, ok, err := st.LoadReviewGroup(contextForTesting(), resolvedID)
	if err != nil {
		t.Fatalf("load resolved review group: %v", err)
	}
	if !ok {
		t.Fatal("resolved review group not found")
	}
	if err := st.ResolveReviewGroup(contextForTesting(), resolvedID, fullWebReviewChoices(t, resolved)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Fixture review")
	assertNotContains(t, body, "No open review groups.")
	assertNotContains(t, body, "Resolved review")
	assertContains(t, body, "/admin/review/"+strconvFormatInt(openGroupID))
}

func TestAdminReviewRejectRejectsSubmittedChoices(t *testing.T) {
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	form := url.Values{}
	form.Set("action", "rejected")
	form.Set("choice_name", strconvFormatInt(group.Candidates[0].ID))

	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "rejecting a review group does not accept field choices")
}

func TestAdminReviewResolveRequiresAllFields(t *testing.T) {
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	form := url.Values{}
	form.Set("action", "resolved")
	form.Set("choice_name", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_start_at", strconvFormatInt(group.Candidates[0].ID))

	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "all review fields must be selected before resolving")
}

func TestAdminReviewResolveRedirectsAndRemovesFromQueue(t *testing.T) {
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	form := url.Values{}
	form.Set("action", "resolved")
	form.Set("choice_name", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_venue_slug", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_start_at", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_end_at", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_genre", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_status", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_description", strconvFormatInt(group.Candidates[1].ID))
	form.Set("choice_source_name", strconvFormatInt(group.Candidates[0].ID))
	form.Set("choice_source_url", strconvFormatInt(group.Candidates[1].ID))

	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?resolved=1" {
		t.Fatalf("Location = %q, want resolved review queue redirect", location)
	}

	queueBody := renderPath(t, server, "/admin/review?resolved=1")
	assertContains(t, queueBody, "Marked resolved.")
	assertContains(t, queueBody, "No open review groups.")
	assertNotContains(t, queueBody, "Fixture review")

	updated, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if updated.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", updated.Status, review.StatusResolved)
	}
}

func TestAdminReviewSingletonRendersAcceptAndReject(t *testing.T) {
	st, server, groupID := mustReviewServerWithSingletonGroup(t)
	defer st.Close()

	listBody := renderPath(t, server, "/admin/review")
	assertContains(t, listBody, "New listing review")
	assertContains(t, listBody, "1 candidate")

	detailBody := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertNotContains(t, detailBody, "Canonical draft summary")
	assertContains(t, detailBody, "Listing candidate")
	assertContains(t, detailBody, "<strong>Name</strong>: Solo Show")
	assertContains(t, detailBody, "Accept new listing")
	assertContains(t, detailBody, ">Reject</button>")
	assertNotContains(t, detailBody, "Saved draft preview")
	assertNotContains(t, detailBody, `name="choice_name"`)
	assertNotContains(t, detailBody, "review-matrix")
}

func TestAdminReviewSingletonAcceptResolvesWithCanonicalChoices(t *testing.T) {
	st, server, groupID := mustReviewServerWithSingletonGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	candidateID := group.Candidates[0].ID

	form := url.Values{}
	form.Set("action", "accept")
	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?accepted=1" {
		t.Fatalf("Location = %q, want accepted review queue redirect", location)
	}

	queueBody := renderPath(t, server, "/admin/review?accepted=1")
	assertContains(t, queueBody, "Accepted new listing.")
	assertContains(t, queueBody, "No open review groups.")

	updated, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after accept")
	}
	if updated.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", updated.Status, review.StatusResolved)
	}
	if got, want := len(updated.DraftChoices), len(review.CanonicalFields); got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}
	for _, field := range review.CanonicalFields {
		choice, ok := updated.DraftChoices[field]
		if !ok {
			t.Fatalf("missing draft choice for %s", field)
		}
		if choice.CandidateID != candidateID {
			t.Fatalf("choice candidate for %s = %d, want %d", field, choice.CandidateID, candidateID)
		}
	}

	closedBody := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, closedBody, "This review is closed and read-only.")
	assertContains(t, closedBody, "<strong>Name</strong>: Solo Show")
	assertNotContains(t, closedBody, "Accept new listing")
	assertNotContains(t, closedBody, `name="choice_name"`)
}

func TestAdminReviewClosedGroupIsReadOnlyAndRejectsPost(t *testing.T) {
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if err := st.ResolveReviewGroup(contextForTesting(), groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[1].ID},
		{Field: review.FieldVenueSlug, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldStartAt, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldEndAt, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldGenre, CandidateID: group.Candidates[1].ID},
		{Field: review.FieldStatus, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldDescription, CandidateID: group.Candidates[1].ID},
		{Field: review.FieldSourceName, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldSourceURL, CandidateID: group.Candidates[1].ID},
	}); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertInOrder(t, body, []string{"Canonical draft summary", "This review is closed and read-only."})
	assertContains(t, body, "This review is closed and read-only.")
	assertContains(t, body, "Canonical draft summary")
	assertContains(t, body, "Candidate 2 (london-1)")
	assertNotContains(t, body, `name="choice_name"`)
	assertNotContains(t, body, "Mark not duplicate")

	before, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	form := url.Values{}
	form.Set("action", "save")
	form.Set("choice_name", strconvFormatInt(group.Candidates[0].ID))
	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusConflict, rr.Body.String())
	}
	after, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("reload review group after closed post: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after closed post")
	}
	if after.Status != before.Status {
		t.Fatalf("status = %q, want unchanged %q", after.Status, before.Status)
	}
	if len(after.DraftChoices) != len(before.DraftChoices) {
		t.Fatalf("draft choices = %d, want unchanged %d", len(after.DraftChoices), len(before.DraftChoices))
	}
}

func TestAdminReviewEmptyPostDoesNotSaveOrUpdateGroup(t *testing.T) {
	ctx := httptest.NewRequest(http.MethodGet, "/", nil).Context()
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	before, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	waitForNextStoredSecond(t)

	req := httptest.NewRequest(http.MethodPost, "/admin/review/"+strconvFormatInt(groupID), strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "at least one review choice is required")

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after empty post")
	}
	if !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("updated_at = %v, want unchanged %v", after.UpdatedAt, before.UpdatedAt)
	}
	if len(after.DraftChoices) != 0 {
		t.Fatalf("draft choices = %d, want 0", len(after.DraftChoices))
	}
}

func mustReviewServerWithGroup(t *testing.T) (*sqlitestore.Store, *Server, int64) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Fixture review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:sidney.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "utc-1",
				Name:        "UTC Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First line",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/utc-show",
				Provenance:  "fixture UID utc-1",
			},
			{
				ExternalID:  "london-1",
				Name:        "London Show",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-02T18:30:00Z",
				EndAt:       "2026-05-02T21:30:00Z",
				Genre:       "Rock",
				Status:      "Listed",
				Description: "London description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:sidney.ics",
				Provenance:  "fixture UID london-1",
			},
		},
	})
	if err != nil {
		_ = st.Close()
		t.Fatalf("create review group: %v", err)
	}

	server, err := NewServer(st)
	if err != nil {
		_ = st.Close()
		t.Fatalf("new server: %v", err)
	}
	return st, server, groupID
}

func mustReviewServerWithSingletonGroup(t *testing.T) (*sqlitestore.Store, *Server, int64) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "New listing review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:sidney.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "solo-1",
				Name:        "Solo Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-03T19:00:00Z",
				EndAt:       "2026-05-03T22:00:00Z",
				Genre:       "Folk",
				Status:      "Listed",
				Description: "One listing",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/solo-show",
				Provenance:  "fixture UID solo-1",
			},
		},
	})
	if err != nil {
		_ = st.Close()
		t.Fatalf("create review group: %v", err)
	}

	server, err := NewServer(st)
	if err != nil {
		_ = st.Close()
		t.Fatalf("new server: %v", err)
	}
	return st, server, groupID
}

func contextForTesting() context.Context {
	return httptest.NewRequest(http.MethodGet, "/", nil).Context()
}

func waitForNextStoredSecond(t *testing.T) {
	t.Helper()

	start := time.Now().UTC().Truncate(time.Second)
	for !time.Now().UTC().Truncate(time.Second).After(start) {
		time.Sleep(10 * time.Millisecond)
	}
}

func fullWebReviewChoices(t *testing.T, group review.Group) []review.DraftChoiceInput {
	t.Helper()

	if len(group.Candidates) == 0 {
		t.Fatal("review group has no candidates")
	}
	choices := make([]review.DraftChoiceInput, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		choices = append(choices, review.DraftChoiceInput{
			Field:       field,
			CandidateID: group.Candidates[0].ID,
		})
	}
	return choices
}

type readOnlyStoreStub struct{}

func (readOnlyStoreStub) Venues() []domain.Venue { return nil }

func (readOnlyStoreStub) Events() []domain.Event { return nil }

func (readOnlyStoreStub) VenueBySlug(string) (domain.Venue, bool) {
	return domain.Venue{}, false
}

func (readOnlyStoreStub) EventBySlug(string) (domain.Event, bool) {
	return domain.Event{}, false
}

func (readOnlyStoreStub) EventsForVenue(string) []domain.Event { return nil }

func (readOnlyStoreStub) Validate() error { return nil }

func mustFixtureServer(t *testing.T) *Server {
	t.Helper()

	return mustClockedServer(t, store.NewStore(
		[]domain.Venue{
			{
				Slug:          "leadmill",
				Name:          "The Leadmill",
				Address:       "6 Leadmill Road, Sheffield",
				Neighbourhood: "City Centre",
				Description:   "A long-running Sheffield venue.",
				Website:       "https://example.test/leadmill",
				Origin:        domain.OriginSeed,
			},
			{
				Slug:          "yellow-arch",
				Name:          "Yellow Arch Studios",
				Address:       "30 Burton Road, Sheffield",
				Neighbourhood: "Neepsend",
				Description:   "A Sheffield venue.",
				Website:       "https://example.test/yellow-arch",
				Origin:        domain.OriginSeed,
			},
			{
				Slug:          "empty-room",
				Name:          "Empty Room",
				Address:       "1 Quiet Street, Sheffield",
				Neighbourhood: "Centre",
				Description:   "A venue with no listed shows.",
				Website:       "https://example.test/empty",
			},
		},
		[]domain.Event{
			{
				Slug:        "past-leadmill",
				Name:        "Past Leadmill",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 18, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 18, 22, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Past show.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/leadmill",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "tonight-leadmill",
				Name:        "Tonight Leadmill",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Tonight show.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/leadmill",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "tomorrow-yellow-arch",
				Name:        "Tomorrow Yellow Arch",
				VenueSlug:   "yellow-arch",
				Start:       fixtureLocalTime(2026, time.April, 20, 19, 30),
				End:         fixtureLocalTime(2026, time.April, 20, 22, 30),
				Genre:       "Jazz",
				Status:      "Listed",
				Description: "Tomorrow show.",
				SourceName:  "Yellow Arch listings",
				SourceURL:   "https://example.test/yellow-arch",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "friday-leadmill",
				Name:        "Friday Leadmill",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 24, 21, 0),
				End:         fixtureLocalTime(2026, time.April, 24, 23, 0),
				Genre:       "Rock",
				Status:      "Listed",
				Description: "Friday show.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/leadmill",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "later-leadmill",
				Name:        "Later Leadmill",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 27, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 27, 22, 0),
				Genre:       "Folk",
				Status:      "Listed",
				Description: "Later show.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/leadmill",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))
}

func mustClockedServer(t *testing.T, st *store.Store) *Server {
	t.Helper()

	server, err := NewServer(st)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 10, 0)
	})
	return server
}

func fixtureLocalTime(year int, month time.Month, day, hour, minute int) time.Time {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		loc = time.FixedZone("Europe/London", 0)
	}
	return time.Date(year, month, day, hour, minute, 0, 0, loc).UTC()
}

func assertContains(t *testing.T, body, want string) {
	t.Helper()

	if !strings.Contains(body, want) {
		t.Fatalf("body missing %q in %q", want, body)
	}
}

func assertNotContains(t *testing.T, body, unwanted string) {
	t.Helper()

	if strings.Contains(body, unwanted) {
		t.Fatalf("body contains %q in %q", unwanted, body)
	}
}

func assertInOrder(t *testing.T, body string, parts []string) {
	t.Helper()

	offset := 0
	for _, part := range parts {
		index := strings.Index(body[offset:], part)
		if index < 0 {
			t.Fatalf("body missing %q after offset %d in %q", part, offset, body)
		}
		offset += index + len(part)
	}
}

func strconvFormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}
