package web

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
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
		{name: "admin import history missing", path: "/admin/import-runs", code: http.StatusNotFound, body: "404 page not found"},
		{name: "admin review history missing", path: "/admin/review/history", code: http.StatusNotFound, body: "404 page not found"},
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

func TestAdminReviewOmitsLatestImportWithoutImportHistoryStore(t *testing.T) {
	server, err := NewServer(reviewOnlyStoreStub{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Review queue")
	assertNotContains(t, body, "Latest successful import")
	assertNotContains(t, body, `href="/admin/import-runs"`)
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

func TestSQLiteAdminImportRunsEmptyAndPopulated(t *testing.T) {
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

	emptyBody := renderPath(t, server, "/admin/import-runs")
	assertContains(t, emptyBody, "Import history")
	assertContains(t, emptyBody, `href="/admin/review"`)
	assertContains(t, emptyBody, "No import runs recorded yet.")

	if err := seedImportRunHistory(t, path); err != nil {
		t.Fatalf("seed import history: %v", err)
	}

	populatedBody := renderPath(t, server, "/admin/import-runs")
	assertInOrder(t, populatedBody, []string{"Run #3", "Run #2", "Run #1", "Run #4"})
	assertContains(t, populatedBody, "running")
	assertContains(t, populatedBody, "failed")
	assertContains(t, populatedBody, "succeeded")
	assertContains(t, populatedBody, "2 snapshots")
	assertContains(t, populatedBody, "3 snapshots")
	assertContains(t, populatedBody, "1 snapshot")
	assertContains(t, populatedBody, "0 snapshots")
	assertContains(t, populatedBody, "Newest run")
	assertContains(t, populatedBody, "Older failure")
	assertContains(t, populatedBody, "Old success")
	assertContains(t, populatedBody, "Very old success")
	assertContains(t, populatedBody, "&mdash;")
	assertContains(t, populatedBody, `href="/admin/import-runs/3"`)
	assertContains(t, populatedBody, `href="/admin/import-runs/1"`)
}

func TestSQLiteAdminImportRunsRenderReviewGroupStatusSummary(t *testing.T) {
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
	if err := seedImportRunHistory(t, path); err != nil {
		t.Fatalf("seed import history: %v", err)
	}

	_ = mustCreateWebReviewGroupForImportRun(t, st, "Open import group", "Created from manual ingest run 1 review staging.", 1)
	rejectedID := mustCreateWebReviewGroupForImportRun(t, st, "Rejected import group", "Created from manual ingest run 1 review staging.", 1)
	secondRejectedID := mustCreateWebReviewGroupForImportRun(t, st, "Second rejected import group", "Created from import run 1 review staging.", 1)
	if err := st.UpdateReviewGroupStatus(contextForTesting(), rejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject review group: %v", err)
	}
	if err := st.UpdateReviewGroupStatus(contextForTesting(), secondRejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject second review group: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "<th scope=\"col\">Review groups</th>")
	assertContains(t, body, `href="/admin/import-runs/1">1 open, 2 rejected</a>`)
	assertContains(t, body, `href="/admin/import-runs/4">none</a>`)
	assertInOrder(t, body, []string{"Run #3", "none", "Run #2", "none", "Run #1", "1 open, 2 rejected", "Run #4", "none"})
}

func TestSQLiteAdminImportRunDetailRendersMetadataOnly(t *testing.T) {
	st, server, runID, bodyText := mustImportRunDetailServer(t, false)
	defer st.Close()

	body := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, body, "Import run #"+strconvFormatInt(runID))
	assertContains(t, body, `href="/admin/review"`)
	assertContains(t, body, "succeeded")
	assertContains(t, body, "links=1 candidates=2")
	assertContains(t, body, "Snapshot metadata")
	assertContains(t, body, "Metadata available")
	assertContains(t, body, "Fixture Source")
	assertContains(t, body, "https://snapshot.example.test/source")
	assertContains(t, body, "https://snapshot.example.test/final")
	assertContains(t, body, "200 OK")
	assertNotContains(t, body, "200 200 OK")
	assertContains(t, body, "text/calendar")
	assertContains(t, body, "no")
	assertNotContains(t, body, bodyText)
	assertNotContains(t, body, base64.StdEncoding.EncodeToString([]byte(bodyText)))
	assertNotContains(t, body, `href="https://snapshot.example.test/source"`)
	assertNotContains(t, body, `body_base64`)
}

func TestSQLiteAdminImportRunDetailRendersReviewGroupsForRun(t *testing.T) {
	st, server, runID, bodyText := mustImportRunDetailServer(t, false)
	defer st.Close()

	openID := mustCreateWebReviewGroupForImportRun(t, st, "Open import group", "Created from manual ingest run "+strconvFormatInt(runID)+" review staging.", 2)
	resolvedID := mustCreateWebPublishableReviewGroupForImportRun(t, st, "Resolved import group", "Created from import run "+strconvFormatInt(runID)+" review staging.")
	rejectedID := mustCreateWebReviewGroupForImportRun(t, st, "Rejected import group", "Created from manual ingest run "+strconvFormatInt(runID)+" review staging.", 1)
	_ = mustCreateWebReviewGroupForImportRun(t, st, "Wrong import group", "Created from manual ingest run 123 review staging.", 1)
	_ = mustCreateWebReviewGroupForImportRun(t, st, "Malformed import group", "Created from manual ingest run "+strconvFormatInt(runID)+"abc review staging.", 1)

	open, ok, err := st.LoadReviewGroup(contextForTesting(), openID)
	if err != nil {
		t.Fatalf("load open review group: %v", err)
	}
	if !ok {
		t.Fatal("open review group not found")
	}
	if err := st.SaveReviewDraftChoices(contextForTesting(), openID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: open.Candidates[0].ID},
	}); err != nil {
		t.Fatalf("save open draft: %v", err)
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
	if err := st.UpdateReviewGroupStatus(contextForTesting(), rejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject review group: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, body, "Review groups from this import run")
	assertContains(t, body, `href="/admin/review/`+strconvFormatInt(openID)+`"`)
	assertContains(t, body, "Open import group")
	assertContains(t, body, "open")
	assertContains(t, body, ">2</td>")
	assertContains(t, body, ">1</td>")
	assertContains(t, body, `href="/admin/review/`+strconvFormatInt(resolvedID)+`"`)
	assertContains(t, body, "Resolved import group")
	assertContains(t, body, "resolved")
	assertContains(t, body, `href="/admin/review/`+strconvFormatInt(rejectedID)+`"`)
	assertContains(t, body, "Rejected import group")
	assertContains(t, body, "rejected")
	assertNotContains(t, body, "Wrong import group")
	assertNotContains(t, body, "Malformed import group")
	assertNotContains(t, body, bodyText)
}

func TestSQLiteAdminImportRunDetailInvalidAndMissingIDs(t *testing.T) {
	st, server, _, _ := mustImportRunDetailServer(t, false)
	defer st.Close()

	tests := []struct {
		method string
		path   string
		code   int
	}{
		{method: http.MethodGet, path: "/admin/import-runs/not-an-id", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/0", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/-1", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/1/extra", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/1/", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/1/..", code: http.StatusNotFound},
		{method: http.MethodGet, path: "/admin/import-runs/999", code: http.StatusNotFound},
		{method: http.MethodPost, path: "/admin/import-runs/1", code: http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rr := httptest.NewRecorder()

			server.ServeHTTP(rr, req)

			if rr.Code != tc.code {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, tc.code, rr.Body.String())
			}
		})
	}
}

func TestAdminImportRunDetailMissingStoreSupport404(t *testing.T) {
	server, err := NewServer(store.NewSeedStore())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/import-runs/1", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusNotFound, rr.Body.String())
	}
}

func TestAdminImportRunPagesOmitReviewQueueWithoutReviewStorage(t *testing.T) {
	server, err := NewServer(importHistoryWithDetailNoReviewStoreStub{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	listBody := renderPath(t, server, "/admin/import-runs")
	assertContains(t, listBody, "Import history")
	assertNotContains(t, listBody, `href="/admin/review"`)

	detailBody := renderPath(t, server, "/admin/import-runs/1")
	assertContains(t, detailBody, "Import run #1")
	assertContains(t, detailBody, "Fixture review group")
	assertNotContains(t, detailBody, `href="/admin/review"`)
}

func TestAdminImportRunsOmitsDetailLinksWithoutReplayStore(t *testing.T) {
	server, err := NewServer(importHistoryOnlyStoreStub{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "Run #1")
	assertNotContains(t, body, `href="/admin/import-runs/1"`)
	assertNotContains(t, body, "<th scope=\"col\">Review groups</th>")
}

func TestAdminImportRunsReviewGroupSummaryIsPlainTextWithoutDetailStore(t *testing.T) {
	server, err := NewServer(importHistoryWithReviewGroupsNoDetailStoreStub{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "<th scope=\"col\">Review groups</th>")
	assertContains(t, body, ">1 open, 2 resolved</td>")
	assertNotContains(t, body, `href="/admin/import-runs/1"`)
}

func TestSQLiteAdminImportRunDetailMalformedPayloadDoesNotCrash(t *testing.T) {
	st, server, runID, rawPayloadText := mustImportRunDetailServer(t, true)
	defer st.Close()

	body := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, body, "Import run #"+strconvFormatInt(runID))
	assertContains(t, body, "Metadata unavailable")
	assertContains(t, body, "Fixture Source")
	assertNotContains(t, body, rawPayloadText)
}

func TestBuildImportRunDetailDoesNotExposeRawReplayPayload(t *testing.T) {
	finishedAt := time.Date(2026, time.April, 20, 10, 5, 0, 0, time.UTC)
	payloadText := "SECRET SNAPSHOT PAYLOAD"
	detail := buildImportRunDetail(ingest.ReplayRun{
		ID:         12,
		StartedAt:  time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		FinishedAt: &finishedAt,
		Status:     "succeeded",
		Notes:      "links=1 candidates=2",
		Snapshots: []ingest.ReplaySnapshot{
			{
				ID:         34,
				SourceName: "Fixture Source",
				SourceURL:  "https://snapshot.example.test/source",
				CapturedAt: time.Date(2026, time.April, 20, 10, 1, 0, 0, time.UTC),
				Payload:    payloadText,
			},
		},
	})

	if detail.ID != 12 || detail.Status != "succeeded" || detail.SnapshotCount != 1 {
		t.Fatalf("summary fields were not preserved: %+v", detail)
	}
	assertTemplateFacingValueSafe(t, reflect.ValueOf(PageData{ImportRunDetail: detail}), payloadText)
}

func TestSQLiteEventDetailRendersResolvedReviewSource(t *testing.T) {
	st, server, groupID := mustReviewServerWithGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if err := st.ResolveReviewGroup(contextForTesting(), groupID, fullWebReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	body := renderPath(t, server, "/events/live-utc-show-sidney-and-matilda-20260501190000")
	assertContains(t, body, "Fixture ICS")
	assertContains(t, body, `href="https://example.test/utc-show"`)
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
	assertContains(t, listBody, `href="/admin/review/history"`)

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
	assertContains(t, detailBody, `href="/admin/review/history"`)

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

func TestSQLiteAdminReviewHistoryListsClosedGroupsNewestFirst(t *testing.T) {
	st, server, openID, path := mustReviewServerWithGroupPath(t)
	defer st.Close()

	resolvedID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Resolved review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:resolved.ics",
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
		},
	})
	if err != nil {
		t.Fatalf("create resolved review group: %v", err)
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

	rejectedID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Rejected review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:rejected.ics",
		Candidates: []review.CandidateInput{
			{
				Name:       "Rejected candidate",
				StartAt:    "2026-05-01T19:00:00Z",
				SourceName: "Fixture ICS",
				SourceURL:  "file:rejected.ics",
			},
		},
	})
	if err != nil {
		t.Fatalf("create rejected review group: %v", err)
	}
	if err := st.UpdateReviewGroupStatus(contextForTesting(), rejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject review group: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, "2026-04-20T12:00:00Z", rejectedID); err != nil {
		t.Fatalf("set rejected updated_at: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, "2026-04-20T11:00:00Z", resolvedID); err != nil {
		t.Fatalf("set resolved updated_at: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	body := renderPath(t, server, "/admin/review/history")
	assertContains(t, body, "Review history")
	assertContains(t, body, `href="/admin/review"`)
	assertContains(t, body, `href="/admin/review/`+strconvFormatInt(rejectedID)+`"`)
	assertContains(t, body, `href="/admin/review/`+strconvFormatInt(resolvedID)+`"`)
	assertContains(t, body, "rejected")
	assertContains(t, body, "resolved")
	assertInOrder(t, body, []string{"Rejected review", "Resolved review"})
	assertNotContains(t, body, `href="/admin/review/`+strconvFormatInt(openID)+`"`)

	req := httptest.NewRequest(http.MethodPost, "/admin/review/history", strings.NewReader(""))
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
	}
}

func TestAdminReviewShowsLatestSuccessfulImportLink(t *testing.T) {
	st, server, _, path := mustReviewServerWithGroupPath(t)
	defer st.Close()

	if err := seedImportRunHistory(t, path); err != nil {
		t.Fatalf("seed import history: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Latest successful import")
	assertContains(t, body, "run #1")
	assertContains(t, body, `href="/admin/import-runs"`)
	assertContains(t, body, `href="/admin/import-runs/1"`)
	assertContains(t, body, "1 snapshot")
}

func TestAdminReviewShowsLatestSuccessfulImportWithoutDetailLink(t *testing.T) {
	server, err := NewServer(reviewImportHistoryOnlyStoreStub{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Latest successful import")
	assertContains(t, body, "run #1")
	assertContains(t, body, `href="/admin/import-runs"`)
	assertNotContains(t, body, `href="/admin/import-runs/1"`)
}

func TestAdminReviewDetailShowsOriginImportRunLinkFromNotes(t *testing.T) {
	tests := []struct {
		name  string
		notes string
		id    string
	}{
		{name: "manual ingest wording", notes: "Created from manual ingest run 123 review staging.", id: "123"},
		{name: "import run wording", notes: "Created from import run 456 review staging.", id: "456"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, server, groupID, _ := mustReviewServerWithGroupPathAndNotes(t, tc.notes)
			defer st.Close()

			body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
			assertContains(t, body, "Review notes")
			assertContains(t, body, tc.notes)
			assertContains(t, body, `href="/admin/import-runs"`)
			assertContains(t, body, `href="/admin/import-runs/`+tc.id+`"`)
			assertContains(t, body, "Import run #"+tc.id)
		})
	}
}

func TestAdminReviewDetailOmitsOriginImportRunLinkWhenUnavailable(t *testing.T) {
	tests := []struct {
		name  string
		notes string
	}{
		{name: "unparseable", notes: "Created from offline fixture."},
		{name: "zero", notes: "Created from manual ingest run 0 review staging."},
		{name: "negative", notes: "Created from manual ingest run -12 review staging."},
		{name: "not a strict id", notes: "Created from manual ingest run 12abc review staging."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, server, groupID, _ := mustReviewServerWithGroupPathAndNotes(t, tc.notes)
			defer st.Close()

			body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
			assertContains(t, body, "Review notes")
			assertContains(t, body, tc.notes)
			assertNotContains(t, body, `href="/admin/import-runs/`)
		})
	}

	server, err := NewServer(reviewOnlyStoreStub{
		group: review.Group{
			ID:           1,
			Title:        "Fixture review",
			SourceName:   "Fixture ICS",
			SourceURL:    "file:sidney.ics",
			Status:       review.StatusOpen,
			Notes:        "Created from manual ingest run 123 review staging.",
			DraftChoices: map[review.Field]review.DraftChoice{},
			Candidates: []review.Candidate{
				{ID: 1, Position: 1, Name: "Solo Show"},
			},
		},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	body := renderPath(t, server, "/admin/review/1")
	assertContains(t, body, "Created from manual ingest run 123 review staging.")
	assertNotContains(t, body, `href="/admin/import-runs/123"`)
}

func TestAdminReviewDetailFallsBackToCandidateNumberWhenExternalIDIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Sparse metadata review",
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
		t.Fatalf("create review group: %v", err)
	}

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	candidateID := group.Candidates[0].ID
	rawDB := mustRawDB(t, path)
	if _, err := rawDB.Exec(`
		UPDATE review_candidates
		SET external_id = ''
		WHERE id = ?
	`, candidateID); err != nil {
		t.Fatalf("blank candidate external id: %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	server, err := NewServer(st)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, body, "<span>Candidate 1</span>")
	assertNotContains(t, body, "Candidate 1 (utc-1)")
	assertContains(t, body, "fixture UID utc-1")
	assertContains(t, body, "https://example.test/utc-show")
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
				SourceURL:   "file:resolved.ics",
				Provenance:  "fixture UID london-1",
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
	beforeEventCount := len(st.Events())

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
	if got := len(st.Events()); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
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
	eventSlug := "live-london-show-sidney-and-matilda-20260501190000"
	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if event.Name != "London Show" {
		t.Fatalf("name = %q, want %q", event.Name, "London Show")
	}
	if event.VenueSlug != "sidney-and-matilda" {
		t.Fatalf("venue slug = %q, want %q", event.VenueSlug, "sidney-and-matilda")
	}
	if event.SourceName != "Fixture ICS" {
		t.Fatalf("source name = %q, want %q", event.SourceName, "Fixture ICS")
	}
	if event.SourceURL != "file:sidney.ics" {
		t.Fatalf("source url = %q, want %q", event.SourceURL, "file:sidney.ics")
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
	}
}

func TestAdminReviewSingletonRendersAcceptAndReject(t *testing.T) {
	st, server, groupID, _ := mustReviewServerWithSingletonGroup(t)
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
	st, server, groupID, path := mustReviewServerWithSingletonGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	candidateID := group.Candidates[0].ID
	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET source_name = '', source_url = ''
		WHERE id = ?
	`, candidateID); err != nil {
		t.Fatalf("blank candidate source fields: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

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
	eventSlug := "live-solo-show-sidney-and-matilda-20260503190000"
	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if event.SourceName != group.SourceName {
		t.Fatalf("source name = %q, want %q", event.SourceName, group.SourceName)
	}
	if event.SourceURL != group.SourceURL {
		t.Fatalf("source url = %q, want %q", event.SourceURL, group.SourceURL)
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
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

func mustReviewServerWithGroupPath(t *testing.T) (*sqlitestore.Store, *Server, int64, string) {
	t.Helper()

	return mustReviewServerWithGroupPathAndNotes(t, "")
}

func mustReviewServerWithGroupPathAndNotes(t *testing.T, notes string) (*sqlitestore.Store, *Server, int64, string) {
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
		Notes:      notes,
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
	return st, server, groupID, path
}

func mustReviewServerWithSingletonGroup(t *testing.T) (*sqlitestore.Store, *Server, int64, string) {
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
	return st, server, groupID, path
}

func mustRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file://"+filepath.ToSlash(path)+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping raw db: %v", err)
	}
	return db
}

func mustImportRunDetailServer(t *testing.T, malformed bool) (*sqlitestore.Store, *Server, int64, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	server, err := NewServer(st)
	if err != nil {
		_ = st.Close()
		t.Fatalf("new server: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()
	runID := int64(12)
	sourceID := int64(10)
	if _, err := db.Exec(`
		INSERT INTO sources (id, name, url)
		VALUES (?, ?, ?)
	`, sourceID, "Fixture Source", "https://snapshot.example.test/source"); err != nil {
		_ = st.Close()
		t.Fatalf("insert source: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, runID, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "links=1 candidates=2"); err != nil {
		_ = st.Close()
		t.Fatalf("insert import run: %v", err)
	}

	bodyText := "SECRET SNAPSHOT BODY STRING"
	payload := mustWebSnapshotPayload(t, ingest.FetchResult{
		URL:           "https://snapshot.example.test/source",
		FinalURL:      "https://snapshot.example.test/final",
		Status:        "200 OK",
		StatusCode:    200,
		ContentType:   "text/calendar",
		ContentLength: int64(len(bodyText)),
		Body:          []byte(bodyText),
		CapturedAt:    time.Date(2026, time.April, 20, 10, 1, 0, 0, time.UTC),
	})
	if malformed {
		payload = "malformed snapshot payload " + bodyText
	}

	if _, err := db.Exec(`
		INSERT INTO snapshots (id, import_run_id, source_id, captured_at, payload)
		VALUES (?, ?, ?, ?, ?)
	`, 50, runID, sourceID, "2026-04-20T10:01:00Z", payload); err != nil {
		_ = st.Close()
		t.Fatalf("insert snapshot: %v", err)
	}

	return st, server, runID, bodyText
}

func mustWebSnapshotPayload(t *testing.T, result ingest.FetchResult) string {
	t.Helper()

	payload, err := ingest.NewSnapshotEnvelope(result).JSON()
	if err != nil {
		t.Fatalf("snapshot payload: %v", err)
	}
	return payload
}

func seedImportRunHistory(t *testing.T, path string) error {
	t.Helper()

	db := mustRawDB(t, path)
	defer db.Close()
	return seedImportRunHistoryWithDB(db)
}

func seedImportRunHistoryWithDB(db *sql.DB) error {
	sizes := map[int64]int{
		1: 1,
		2: 3,
		3: 2,
		4: 0,
	}
	rows := []struct {
		id         int64
		startedAt  string
		finishedAt sql.NullString
		status     string
		notes      string
	}{
		{id: 1, startedAt: "2026-04-20T10:00:00Z", finishedAt: sql.NullString{String: "2026-04-20T10:05:00Z", Valid: true}, status: "succeeded", notes: "Old success"},
		{id: 2, startedAt: "2026-04-20T11:00:00Z", finishedAt: sql.NullString{String: "2026-04-20T11:05:00Z", Valid: true}, status: "failed", notes: "Older failure"},
		{id: 3, startedAt: "2026-04-20T12:00:00Z", finishedAt: sql.NullString{}, status: "running", notes: "Newest run"},
		{id: 4, startedAt: "2026-04-20T09:00:00Z", finishedAt: sql.NullString{String: "2026-04-20T09:10:00Z", Valid: true}, status: "succeeded", notes: "Very old success"},
	}
	for _, row := range rows {
		if _, err := db.Exec(`
			INSERT INTO import_runs (id, started_at, finished_at, status, notes)
			VALUES (?, ?, ?, ?, ?)
		`, row.id, row.startedAt, nullableString(row.finishedAt), row.status, row.notes); err != nil {
			return err
		}
		for i := 0; i < sizes[row.id]; i++ {
			if _, err := db.Exec(`
				INSERT INTO snapshots (import_run_id, captured_at, payload)
				VALUES (?, ?, ?)
			`, row.id, row.startedAt, `{"version":1}`); err != nil {
				return err
			}
		}
	}
	return nil
}

func nullableString(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func contextForTesting() context.Context {
	return httptest.NewRequest(http.MethodGet, "/", nil).Context()
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

func mustCreateWebReviewGroupForImportRun(t *testing.T, st *sqlitestore.Store, title, notes string, candidateCount int) int64 {
	t.Helper()

	candidates := make([]review.CandidateInput, 0, candidateCount)
	for i := 0; i < candidateCount; i++ {
		candidates = append(candidates, review.CandidateInput{
			Name:       fmt.Sprintf("%s candidate %d", title, i+1),
			StartAt:    "2026-05-01T19:00:00Z",
			SourceName: "Fixture ICS",
			SourceURL:  "file:test.ics",
		})
	}
	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:test.ics",
		Notes:      notes,
		Candidates: candidates,
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	return groupID
}

func mustCreateWebPublishableReviewGroupForImportRun(t *testing.T, st *sqlitestore.Store, title, notes string) int64 {
	t.Helper()

	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:published.ics",
		Notes:      notes,
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
		},
	})
	if err != nil {
		t.Fatalf("create publishable review group: %v", err)
	}
	return groupID
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

type reviewOnlyStoreStub struct {
	readOnlyStoreStub
	group review.Group
}

func (reviewOnlyStoreStub) ListOpenReviewGroups(context.Context) ([]review.GroupSummary, error) {
	return nil, nil
}

func (reviewOnlyStoreStub) ListClosedReviewGroups(context.Context, int) ([]review.GroupSummary, error) {
	return nil, nil
}

func (s reviewOnlyStoreStub) LoadReviewGroup(_ context.Context, id int64) (review.Group, bool, error) {
	if s.group.ID == id {
		return s.group, true, nil
	}
	return review.Group{}, false, nil
}

func (reviewOnlyStoreStub) SaveReviewDraftChoices(context.Context, int64, []review.DraftChoiceInput) error {
	return nil
}

func (reviewOnlyStoreStub) ResolveReviewGroup(context.Context, int64, []review.DraftChoiceInput) error {
	return nil
}

func (reviewOnlyStoreStub) UpdateReviewGroupStatus(context.Context, int64, string) error {
	return nil
}

type importHistoryOnlyStoreStub struct {
	readOnlyStoreStub
}

func (importHistoryOnlyStoreStub) ListImportRuns(context.Context, int) ([]ingest.ImportRunSummary, error) {
	return []ingest.ImportRunSummary{
		{
			ID:            1,
			StartedAt:     time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			Status:        "succeeded",
			SnapshotCount: 1,
		},
	}, nil
}

func (importHistoryOnlyStoreStub) LatestSuccessfulImport(context.Context) (*ingest.ImportRunSummary, error) {
	return &ingest.ImportRunSummary{
		ID:            1,
		StartedAt:     time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:        "succeeded",
		SnapshotCount: 1,
	}, nil
}

type reviewImportHistoryOnlyStoreStub struct {
	reviewOnlyStoreStub
}

func (reviewImportHistoryOnlyStoreStub) ListImportRuns(ctx context.Context, limit int) ([]ingest.ImportRunSummary, error) {
	return importHistoryOnlyStoreStub{}.ListImportRuns(ctx, limit)
}

func (reviewImportHistoryOnlyStoreStub) LatestSuccessfulImport(ctx context.Context) (*ingest.ImportRunSummary, error) {
	return importHistoryOnlyStoreStub{}.LatestSuccessfulImport(ctx)
}

type importHistoryWithReviewGroupsNoDetailStoreStub struct {
	importHistoryOnlyStoreStub
}

func (importHistoryWithReviewGroupsNoDetailStoreStub) ListReviewGroupsForImportRun(context.Context, int64) ([]review.GroupSummary, error) {
	return []review.GroupSummary{
		{ID: 1, Status: review.StatusOpen},
		{ID: 2, Status: review.StatusResolved},
		{ID: 3, Status: review.StatusResolved},
	}, nil
}

type importHistoryWithDetailNoReviewStoreStub struct {
	importHistoryOnlyStoreStub
}

func (importHistoryWithDetailNoReviewStoreStub) LoadImportRun(context.Context, int64) (ingest.ReplayRun, error) {
	return ingest.ReplayRun{
		ID:        1,
		StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:    "succeeded",
		Notes:     "fixture",
	}, nil
}

func (importHistoryWithDetailNoReviewStoreStub) ListReviewGroupsForImportRun(context.Context, int64) ([]review.GroupSummary, error) {
	return []review.GroupSummary{
		{
			ID:             1,
			Title:          "Fixture review group",
			Status:         review.StatusOpen,
			CandidateCount: 1,
			UpdatedAt:      time.Date(2026, time.April, 20, 10, 1, 0, 0, time.UTC),
		},
	}, nil
}

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

func assertTemplateFacingValueSafe(t *testing.T, value reflect.Value, payloadText string) {
	t.Helper()

	rawRunType := reflect.TypeOf(ingest.ReplayRun{})
	rawSnapshotType := reflect.TypeOf(ingest.ReplaySnapshot{})
	assertTemplateFacingValueSafeAt(t, value, "detail", payloadText, rawRunType, rawSnapshotType)
}

func assertTemplateFacingValueSafeAt(t *testing.T, value reflect.Value, path, payloadText string, rawTypes ...reflect.Type) {
	t.Helper()

	if !value.IsValid() {
		return
	}
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	valueType := value.Type()
	for _, rawType := range rawTypes {
		if valueType == rawType {
			t.Fatalf("%s exposes raw payload-bearing type %s", path, valueType)
		}
	}

	switch value.Kind() {
	case reflect.String:
		if strings.Contains(value.String(), payloadText) {
			t.Fatalf("%s exposes raw payload text", path)
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := valueType.Field(i)
			if field.PkgPath != "" {
				continue
			}
			if strings.Contains(strings.ToLower(field.Name), "payload") {
				t.Fatalf("%s.%s exposes a payload field", path, field.Name)
			}
			assertTemplateFacingValueSafeAt(t, value.Field(i), path+"."+field.Name, payloadText, rawTypes...)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			assertTemplateFacingValueSafeAt(t, value.Index(i), fmt.Sprintf("%s[%d]", path, i), payloadText, rawTypes...)
		}
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			assertTemplateFacingValueSafeAt(t, iter.Value(), path+"[map value]", payloadText, rawTypes...)
		}
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
