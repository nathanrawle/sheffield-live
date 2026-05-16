package web

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/genre"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/logging"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store"
	sqlitestore "sheffield-live/internal/store/sqlite"
)

const testAdminPasswordHash = "$2a$12$Np7G88kWczQUXP1fhca9..B9Gv1N55toTxUHQ02rBkN0c1QJggkMW"

func TestRoutes(t *testing.T) {
	server, err := NewServer(testServerDeps(store.NewSeedStore()))
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
		{name: "venues", path: "/venues", code: http.StatusOK, body: "Sheffield rooms"},
		{name: "venue detail", path: "/venues/leadmill", code: http.StatusOK, body: "Leadmill"},
		{name: "static css", path: "/static/site.css", code: http.StatusOK, body: "color-scheme"},
		{name: "admin missing", path: "/admin", code: http.StatusNotFound, body: "404 page not found"},
		{name: "admin import history missing", path: "/admin/import-runs", code: http.StatusNotFound, body: "404 page not found"},
		{name: "admin review history missing", path: "/admin/review/history", code: http.StatusNotFound, body: "404 page not found"},
		{name: "healthz", path: "/healthz", code: http.StatusOK, body: "ok"},
		{name: "readyz", path: "/readyz", code: http.StatusOK, body: "ok"},
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

func TestRequestLoggingCapturesOperationalFields(t *testing.T) {
	var logs bytes.Buffer
	logger, err := logging.NewLogger(&logs, logging.Config{})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	deps := testServerDeps(store.NewSeedStore())
	deps.Logger = logger
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set("User-Agent", "test-agent")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	got := logs.String()
	for _, want := range []string{
		`msg="http request"`,
		`method=GET`,
		`path=/events`,
		`status=200`,
		`user_agent=test-agent`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs = %q, want %q", got, want)
		}
	}
}

func TestRequestLoggingSkipsSuccessfulHealthChecks(t *testing.T) {
	var logs bytes.Buffer
	logger, err := logging.NewLogger(&logs, logging.Config{})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	deps := testServerDeps(store.NewSeedStore())
	deps.Logger = logger
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if got := logs.String(); got != "" {
		t.Fatalf("logs = %q, want none", got)
	}
}

func TestNormalizeMediaURLPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "default", prefix: "", want: "/media"},
		{name: "adds leading slash", prefix: "assets", want: "/assets"},
		{name: "trims trailing slash", prefix: "/assets/", want: "/assets"},
		{name: "root falls back", prefix: "/", want: "/media"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMediaURLPrefix(tc.prefix); got != tc.want {
				t.Fatalf("prefix = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMediaRouteServesLocalFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "events"), 0o755); err != nil {
		t.Fatalf("make media dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "events", "poster.jpg"), []byte("poster bytes"), 0o644); err != nil {
		t.Fatalf("write media file: %v", err)
	}

	deps := testServerDeps(store.NewSeedStore())
	deps.MediaRoot = root
	deps.MediaURLPrefix = "media"
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/media/events/poster.jpg", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	if rr.Body.String() != "poster bytes" {
		t.Fatalf("body = %q, want media file bytes", rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/media", nil)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("directory status = %d, want %d", rr.Code, http.StatusNotFound)
	}

	for _, path := range []string{"/media/events", "/media/events/"} {
		req = httptest.NewRequest(http.MethodGet, path, nil)
		rr = httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("path %s status = %d, want %d", path, rr.Code, http.StatusNotFound)
		}
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

	if _, err := NewServer(testServerDeps(st)); err == nil {
		t.Fatal("new server error = nil, want validation error")
	}
}

func TestNewServerAcceptsReadOnlyStore(t *testing.T) {
	st := readOnlyStoreStub{}

	if _, err := NewServer(testServerDeps(st)); err != nil {
		t.Fatalf("new server: %v", err)
	}
}

func TestNewServerRequiresExplicitAdminAuthConfig(t *testing.T) {
	_, err := NewServer(ServerDeps{Catalog: store.NewSeedStore()})
	if err == nil {
		t.Fatal("new server error = nil, want admin auth config error")
	}
	if !strings.Contains(err.Error(), "admin password hash") {
		t.Fatalf("new server error = %q, want admin password hash", err)
	}
}

func TestAdminReviewOmitsLatestImportWithoutImportHistoryStore(t *testing.T) {
	server, err := NewServer(testServerDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Review queue")
	assertNotContains(t, body, "Latest successful import")
	assertNotContains(t, body, `href="/admin/import-runs"`)
}

func TestAdminLandingPageLinksAvailableAdminTools(t *testing.T) {
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin")
	assertContains(t, body, "Admin")
	assertContains(t, body, `href="/admin/review"`)
	assertContains(t, body, `href="/admin/review/history"`)
	assertContains(t, body, `href="/admin/import-runs"`)
	assertContains(t, body, `href="/admin/venues"`)
}

func TestAdminLandingPageOmitsUnsupportedAdminTools(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin")
	assertContains(t, body, `href="/admin/import-runs"`)
	assertContains(t, body, `href="/admin/venues"`)
	assertNotContains(t, body, `href="/admin/review"`)
	assertNotContains(t, body, `href="/admin/review/history"`)
}

func TestAdminLandingPageRejectsPost(t *testing.T) {
	server, err := NewServer(testServerDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin", strings.NewReader(""))
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
	}
}

func TestAdminAuthRedirectsUnauthenticatedAdminRequests(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
	if location := rr.Header().Get("Location"); location != "/admin/login?next=%2Fadmin%2Freview" {
		t.Fatalf("Location = %q, want login redirect", location)
	}

	req = httptest.NewRequest(http.MethodGet, "/events", nil)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("public status = %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestAdminLoginSetsSecureSessionCookieAndAllowsAdminAccess(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, location := loginAdmin(t, server, "/admin/review")
	if location != "/admin/review" {
		t.Fatalf("login Location = %q, want /admin/review", location)
	}
	if cookie.Name != adminSessionCookieName {
		t.Fatalf("cookie name = %q, want %q", cookie.Name, adminSessionCookieName)
	}
	if !cookie.HttpOnly {
		t.Fatal("session cookie is not HttpOnly")
	}
	if !cookie.Secure {
		t.Fatal("session cookie is not Secure")
	}
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("SameSite = %v, want strict", cookie.SameSite)
	}
	if cookie.Path != "/admin" {
		t.Fatalf("Path = %q, want /admin", cookie.Path)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "Review queue")
	assertContains(t, rr.Body.String(), `name="csrf_token"`)
}

func TestAdminLoginRejectsBadPassword(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	form := url.Values{
		"password": {"wrong"},
		"next":     {"/admin/review"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
	assertContains(t, rr.Body.String(), "Sign in failed.")
	if cookies := rr.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("cookies = %v, want none", cookies)
	}
}

func TestAdminLoginRejectsPasswordInQueryString(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/login?password=correct+horse+battery+staple&next=/admin/review", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
	if cookies := rr.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("cookies = %v, want none", cookies)
	}
}

func TestAdminLoginRejectsUnsafeNextRedirect(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	_, location := loginAdmin(t, server, "https://example.test/admin")
	if location != "/admin" {
		t.Fatalf("login Location = %q, want /admin", location)
	}
}

func TestAdminLoginThrottlesRepeatedFailures(t *testing.T) {
	deps := testAdminAuthDeps(reviewOnlyStoreStub{})
	deps.AdminAuth.MaxFailures = 2
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	for i := 0; i < 2; i++ {
		form := url.Values{
			"password": {"wrong"},
			"next":     {"/admin/review"},
		}
		req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status = %d, want %d", i+1, rr.Code, http.StatusUnauthorized)
		}
	}

	form := url.Values{
		"password": {"correct horse battery staple"},
		"next":     {"/admin/review"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("locked status = %d, want %d", rr.Code, http.StatusTooManyRequests)
	}
}

func TestAdminPostRequiresCSRF(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")
	form := url.Values{"action": {"rejected"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/review/1", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAdminPostRejectsCSRFTokenInQueryString(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin/review")
	req := httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	csrfToken := extractCSRFToken(t, rr.Body.String())

	form := url.Values{"action": {"rejected"}}
	req = httptest.NewRequest(http.MethodPost, "/admin/review/1?csrf_token="+url.QueryEscape(csrfToken), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAdminVenuePostRequiresCSRF(t *testing.T) {
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

	server, err := NewServer(testAdminAuthDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")
	form := url.Values{"action": {"validate"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/venues/imaginary-hall", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAdminRoomPostRequiresCSRF(t *testing.T) {
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

	_, err = st.StageReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Staged room",
		SourceName: "Fixture ICS",
		SourceURL:  "file:staged-room.ics",
		StagingKey: "v1:admin-room-csrf",
		Candidates: []review.CandidateInput{{
			ExternalID:  "candidate-a",
			Name:        "Staged room show",
			VenueSlug:   "sidney-and-matilda",
			RoomText:    "COURTYARD STAGE",
			Rooms:       []domain.VenueRoom{{Slug: "courtyard-stage", Name: "Courtyard Stage"}},
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T22:00:00Z",
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Staged room fixture.",
			SourceName:  "Fixture ICS",
			SourceURL:   "https://example.test/staged-room-show",
			Provenance:  "fixture UID candidate-a",
		}},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}

	server, err := NewServer(testAdminAuthDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin")

	form := url.Values{"action": {"validate"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/rooms/sidney-and-matilda/courtyard-stage", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/rooms/sidney-and-matilda/courtyard-stage", nil)
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d; body %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	body := rr.Body.String()
	assertContains(t, body, `<form method="post" class="review-actions">
    <input type="hidden" name="csrf_token" value="`)
	assertContains(t, body, `<form method="post" class="venue-edit-form">
    <input type="hidden" name="csrf_token" value="`)
	csrfToken := extractCSRFToken(t, body)

	form = url.Values{
		"action":     {"validate"},
		"csrf_token": {csrfToken},
	}
	req = httptest.NewRequest(http.MethodPost, "/admin/rooms/sidney-and-matilda/courtyard-stage", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/rooms?validated=1" {
		t.Fatalf("Location = %q, want %q", location, "/admin/rooms?validated=1")
	}
}

func TestAdminConfigurationPostRequiresCSRF(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(failingGenreConfigurationStore{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")
	form := url.Values{
		"action":     {"save"},
		"key":        {"doom-metal"},
		"name":       {"Doom metal"},
		"match_type": {"plain"},
		"pattern":    {"doom metal"},
		"enabled":    {"1"},
		"sort_order": {"320"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/configuration", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAdminLogoutInvalidatesSession(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin/review")
	req := httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	csrfToken := extractCSRFToken(t, rr.Body.String())

	form := url.Values{"csrf_token": {csrfToken}}
	req = httptest.NewRequest(http.MethodPost, "/admin/logout", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("logout status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
	if location := rr.Header().Get("Location"); location != "/admin/login" {
		t.Fatalf("logout Location = %q, want /admin/login", location)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("post-logout status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
}

func TestAdminSessionExpires(t *testing.T) {
	deps := testAdminAuthDeps(reviewOnlyStoreStub{})
	deps.AdminAuth.SessionIdleTimeout = time.Minute
	deps.AdminAuth.SessionAbsoluteTimeout = time.Hour
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	now := fixtureLocalTime(2026, time.May, 13, 10, 0)
	server.SetClockForTesting(func() time.Time { return now })
	cookie, _ := loginAdmin(t, server, "/admin/review")

	now = now.Add(2 * time.Minute)
	req := httptest.NewRequest(http.MethodGet, "/admin/review", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
}

func TestPublicPagesDoNotLinkToAdmin(t *testing.T) {
	server := mustFixtureServer(t)

	for _, path := range []string{
		"/",
		"/events",
		"/events/tonight-leadmill",
		"/venues",
		"/venues/leadmill",
	} {
		body := renderPath(t, server, path)
		assertNotContains(t, body, `href="/admin`)
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

	server, err := NewServer(testServerDeps(st))
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

	server, err := NewServer(testServerDeps(st))
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := seedImportRunHistory(t, path); err != nil {
		t.Fatalf("seed import history: %v", err)
	}

	_ = mustCreateWebReviewGroupForImportRun(t, st, path, "Open import group", "Created from manual ingest run 1 review staging.", 1)
	rejectedID := mustCreateWebReviewGroupForImportRun(t, st, path, "Rejected import group", "Created from manual ingest run 1 review staging.", 1)
	secondRejectedID := mustCreateWebReviewGroupForImportRun(t, st, path, "Second rejected import group", "Created from import run 1 review staging.", 1)
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
	st, server, runID, bodyText, _ := mustImportRunDetailServer(t, false)
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
	st, server, runID, bodyText, path := mustImportRunDetailServer(t, false)
	defer st.Close()

	openID := mustCreateWebReviewGroupForImportRun(t, st, path, "Open import group", "Created from manual ingest run "+strconvFormatInt(runID)+" review staging.", 2)
	resolvedID := mustCreateWebPublishableReviewGroupForImportRun(t, st, path, "Resolved import group", "Created from import run "+strconvFormatInt(runID)+" review staging.")
	rejectedID := mustCreateWebReviewGroupForImportRun(t, st, path, "Rejected import group", "Created from manual ingest run "+strconvFormatInt(runID)+" review staging.", 1)
	_ = mustCreateWebReviewGroupForImportRun(t, st, path, "Wrong import group", "Created from manual ingest run 123 review staging.", 1)
	_ = mustCreateWebReviewGroupForImportRun(t, st, path, "Malformed import group", "Created from manual ingest run "+strconvFormatInt(runID)+"abc review staging.", 1)

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

func TestSQLiteAdminVenuesEmptyState(t *testing.T) {
	st, server, _ := mustAdminVenuesServer(t)
	defer st.Close()

	body := renderPath(t, server, "/admin/venues")
	assertContains(t, body, "Provisional venues")
	assertContains(t, body, "No provisional venues are queued right now.")
	assertContains(t, body, `href="/admin/review"`)
}

func TestAdminVenuePagesMissingWithoutAdminStores(t *testing.T) {
	server, err := NewServer(testServerDeps(store.NewSeedStore()))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	tests := []string{
		"/admin/venues",
		"/admin/venues/imaginary-hall",
		"/admin/rooms",
		"/admin/rooms/sidney-and-matilda/factory",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", path, rr.Code, http.StatusNotFound)
		}
		assertContains(t, rr.Body.String(), "404 page not found")
	}
}

func TestSQLiteAdminRoomsShowStagedProvisionalRoom(t *testing.T) {
	st, server, _ := mustAdminVenuesServer(t)
	defer st.Close()

	result, err := st.StageReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Staged new room",
		SourceName: "Fixture ICS",
		SourceURL:  "file:staged-new-room.ics",
		StagingKey: "v1:staged-new-room",
		Candidates: []review.CandidateInput{{
			ExternalID:  "candidate-a",
			Name:        "Staged room show",
			VenueSlug:   "sidney-and-matilda",
			RoomText:    "COURTYARD STAGE",
			Rooms:       []domain.VenueRoom{{Slug: "courtyard-stage", Name: "Courtyard Stage"}},
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T22:00:00Z",
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Staged room fixture.",
			SourceName:  "Fixture ICS",
			SourceURL:   "https://example.test/staged-room-show",
			Provenance:  "fixture UID candidate-a",
		}},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatalf("created = false, want true")
	}

	body := renderPath(t, server, "/admin/rooms")
	assertContains(t, body, "Provisional rooms")
	assertContains(t, body, "Courtyard Stage")
	assertContains(t, body, "Sidney &amp; Matilda")
	assertContains(t, body, `href="/admin/rooms/sidney-and-matilda/courtyard-stage"`)
	assertContains(t, body, ">0</td>")

	detailBody := renderPath(t, server, "/admin/rooms/sidney-and-matilda/courtyard-stage")
	assertContains(t, detailBody, "Validate room")
	assertContains(t, detailBody, `name="name" value="Courtyard Stage"`)
	assertContains(t, detailBody, ">provisional</dd>")
	assertContains(t, detailBody, "No upcoming linked events for this provisional room.")
}

func TestSQLiteAdminVenuesListOnlyProvisionalVenues(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	body := renderPath(t, server, "/admin/venues")
	assertContains(t, body, "Provisional venues")
	assertContains(t, body, "Imaginary Hall marketing copy")
	assertContains(t, body, "Quiet Room")
	assertContains(t, body, `href="/admin/venues/imaginary-hall"`)
	assertContains(t, body, `href="/admin/venues/quiet-room"`)
	assertContains(t, body, `href="/events/imaginary-hall-future-show"`)
	assertContains(t, body, "Future Show")
	assertContains(t, body, ">1</td>")
	assertNotContains(t, body, `href="/admin/venues/validated-room"`)
	assertNotContains(t, body, "Validated Room")
}

func TestSQLiteAdminVenuesShowStagedProvisionalVenueWithoutEvents(t *testing.T) {
	st, server, _ := mustAdminVenuesServer(t)
	defer st.Close()

	result, err := st.StageReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Staged new venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:staged-new-venue.ics",
		StagingKey: "v1:staged-new-venue",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Staged venue show",
			VenueSlug:        "imagniary-hal-temp",
			VenueText:        "Imaginary Hall",
			VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Staged without publishing an event.",
		}},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}

	body := renderPath(t, server, "/admin/venues")
	assertContains(t, body, "Queue of provisional venue rows created from newly detected venue evidence.")
	assertContains(t, body, `href="/admin/venues/imaginary-hall"`)
	assertContains(t, body, "Imaginary Hall")
	assertContains(t, body, ">0</td>")

	detailBody := renderPath(t, server, "/admin/venues/imaginary-hall")
	assertContains(t, detailBody, "No upcoming linked events for this provisional venue.")
}

func TestSQLitePublicVenuePagesRenderDerivedProvisionalVenueAddress(t *testing.T) {
	st, server, _ := mustAdminVenuesServer(t)
	defer st.Close()

	result := ingest.ParseLeadmillICS([]byte("BEGIN:VCALENDAR\n" +
		"BEGIN:VEVENT\n" +
		"UID:memorial-hall-show\n" +
		"SUMMARY:Memorial Hall Show\n" +
		"LOCATION:Memorial Hall\\, Barkers Pool\\, Sheffield City Centre\\, Sheffield\\, S1 2JA\n" +
		"CATEGORIES:Live\n" +
		"DTSTART:20260501T190000Z\n" +
		"DTEND:20260501T220000Z\n" +
		"END:VEVENT\n" +
		"END:VCALENDAR\n"))
	if got, want := len(result.Errors), 0; got != want {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}

	candidate := result.Candidates[0]
	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(contextForTesting(), review.GroupInput{
		Title:      "Memorial Hall imported",
		SourceName: "Leadmill ICS",
		SourceURL:  "file:memorial-hall.ics",
		Candidates: []review.CandidateInput{{
			ExternalID:       candidate.UID,
			Name:             candidate.Summary,
			VenueSlug:        ingest.VenueSlugFromText(candidate.Location),
			VenueText:        candidate.Location,
			VenueLocationRaw: candidate.LocationRaw,
			StartAt:          candidate.StartAt,
			EndAt:            candidate.EndAt,
			Status:           "Listed",
			Description:      "Imported from escaped ICS venue evidence.",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}

	adminQueueBody := renderPath(t, server, "/admin/venues")
	assertContains(t, adminQueueBody, "Memorial Hall")
	assertContains(t, adminQueueBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertNotContains(t, adminQueueBody, "Memorial Hall,\nBarkers Pool")

	venueBody := renderPath(t, server, "/venues/memorial-hall")
	assertContains(t, venueBody, "Memorial Hall")
	assertContains(t, venueBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertContains(t, venueBody, "City Centre")
	assertNotContains(t, venueBody, "Memorial Hall,\nBarkers Pool")

	eventBody := renderPath(t, server, "/events/"+eventSlug)
	assertContains(t, eventBody, "Memorial Hall Show")
	assertContains(t, eventBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertContains(t, eventBody, "City Centre")
	assertNotContains(t, eventBody, "Memorial Hall,\nBarkers Pool")

	venuesBody := renderPath(t, server, "/venues")
	assertContains(t, venuesBody, "Memorial Hall")
	assertContains(t, venuesBody, "City Centre · Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertNotContains(t, venuesBody, "City Centre · Memorial Hall,\nBarkers Pool")
}

func TestStoredDuplicateVenueAddressLineIsHiddenAcrossPages(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		UPDATE venues
		SET address = ?
		WHERE slug = ?
	`, "Imaginary Hall marketing copy,\n1 Void Street,\nSheffield", "imaginary-hall"); err != nil {
		t.Fatalf("update venue address: %v", err)
	}

	adminQueueBody := renderPath(t, server, "/admin/venues")
	assertContains(t, adminQueueBody, "1 Void Street,\nSheffield")
	assertNotContains(t, adminQueueBody, "Imaginary Hall marketing copy,\n1 Void Street")

	adminDetailBody := renderPath(t, server, "/admin/venues/imaginary-hall")
	assertContains(t, adminDetailBody, "<textarea name=\"address\" rows=\"4\">1 Void Street,\nSheffield</textarea>")
	assertNotContains(t, adminDetailBody, "Imaginary Hall marketing copy,\n1 Void Street")

	venueBody := renderPath(t, server, "/venues/imaginary-hall")
	assertContains(t, venueBody, "1 Void Street,\nSheffield")
	assertNotContains(t, venueBody, "Imaginary Hall marketing copy,\n1 Void Street")

	eventBody := renderPath(t, server, "/events/imaginary-hall-future-show")
	assertContains(t, eventBody, "1 Void Street,\nSheffield")
	assertNotContains(t, eventBody, "Imaginary Hall marketing copy,\n1 Void Street")

	venuesBody := renderPath(t, server, "/venues")
	assertContains(t, venuesBody, "City Centre · 1 Void Street,\nSheffield")
	assertNotContains(t, venuesBody, "City Centre · Imaginary Hall marketing copy,\n1 Void Street")
}

func TestSQLiteAdminVenueDetailRendersStoredFieldsAndUpcomingEvents(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	body := renderPath(t, server, "/admin/venues/imaginary-hall")
	assertContains(t, body, "Imaginary Hall marketing copy")
	assertContains(t, body, `href="/admin/venues"`)
	assertContains(t, body, `name="action" value="save"`)
	assertContains(t, body, `name="name" value="Imaginary Hall marketing copy"`)
	assertContains(t, body, "<textarea name=\"address\" rows=\"4\">1 Void Street,\nSheffield</textarea>")
	assertContains(t, body, `name="neighbourhood" value="City Centre"`)
	assertContains(t, body, `name="website" value="https://example.test/imaginary-hall"`)
	assertContains(t, body, `name="coverage_kind"`)
	assertContains(t, body, `name="coverage_note"`)
	assertContains(t, body, `method="post"`)
	assertContains(t, body, "Validate venue")
	assertContains(t, body, "Save venue fields")
	assertContains(t, body, "Stored venue fields")
	assertContains(t, body, ">imaginary-hall</dd>")
	assertContains(t, body, "1 Void Street,\nSheffield")
	assertContains(t, body, "City Centre")
	assertContains(t, body, "Pop-up room for test fixtures.")
	assertContains(t, body, "https://example.test/imaginary-hall")
	assertContains(t, body, ">provisional</dd>")
	assertContains(t, body, ">venue</dd>")
	assertContains(t, body, ">live</dd>")
	assertContains(t, body, "1 upcoming linked events")
	assertContains(t, body, `href="/events/imaginary-hall-future-show"`)
	assertContains(t, body, "Future Show")
	assertContains(t, body, "Fixture ICS")
	assertContains(t, body, "Upcoming linked event description.")
	assertNotContains(t, body, "Past Show")
}

func TestAdminVenueDetailHidesWriteControlsWithoutVenueAdminWrites(t *testing.T) {
	server, err := NewServer(testServerDeps(provisionalVenueReadOnlyReviewStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	queueBody := renderPath(t, server, "/admin/venues")
	assertContains(t, queueBody, "Imaginary Hall marketing copy")

	body := renderPath(t, server, "/admin/venues/imaginary-hall")
	assertContains(t, body, "Imaginary Hall marketing copy")
	assertContains(t, body, "Stored venue fields")
	assertNotContains(t, body, "Validate venue")
	assertNotContains(t, body, "Save venue fields")
	assertNotContains(t, body, `name="action" value="save"`)
	assertNotContains(t, body, `name="action" value="validate"`)

	for _, form := range []string{"action=validate", "action=save"} {
		req := httptest.NewRequest(http.MethodPost, "/admin/venues/imaginary-hall", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d; body %q", form, rr.Code, http.StatusNotFound, rr.Body.String())
		}
		assertContains(t, rr.Body.String(), "404 page not found")
	}
}

func TestSQLiteAdminVenueDetailValidatedSlugReturnsNotFound(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	req := httptest.NewRequest(http.MethodGet, "/admin/venues/validated-room", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	assertContains(t, rr.Body.String(), "404 page not found")
}

func TestSQLiteAdminVenueDetailUnknownSlugReturnsNotFound(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	req := httptest.NewRequest(http.MethodGet, "/admin/venues/missing-room", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	assertContains(t, rr.Body.String(), "404 page not found")
}

func TestSQLiteAdminVenueListRejectsPost(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	req := httptest.NewRequest(http.MethodPost, "/admin/venues", strings.NewReader(""))
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "method not allowed")
}

func TestAdminVenuePagesRequireAdminSurface(t *testing.T) {
	server, err := NewServer(testServerDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	for _, path := range []string{"/admin/venues", "/admin/venues/imaginary-hall"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d; body %q", path, rr.Code, http.StatusNotFound, rr.Body.String())
		}
		assertContains(t, rr.Body.String(), "404 page not found")
	}
}

func TestSQLiteAdminVenueDetailPostValidatesVenueAndRedirects(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	beforeVenueBody := renderPath(t, server, "/venues/imaginary-hall")
	assertContains(t, beforeVenueBody, "Imaginary Hall marketing copy")
	beforeEventBody := renderPath(t, server, "/events/imaginary-hall-future-show")
	assertContains(t, beforeEventBody, "Future Show")
	assertContains(t, beforeEventBody, "Imaginary Hall marketing copy")

	req := httptest.NewRequest(http.MethodPost, "/admin/venues/imaginary-hall", strings.NewReader("action=validate"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	location := rr.Header().Get("Location")
	if location != "/admin/venues?validated=1" {
		t.Fatalf("Location = %q, want %q", location, "/admin/venues?validated=1")
	}

	queueBody := renderPath(t, server, location)
	assertContains(t, queueBody, "Venue validated.")
	assertNotContains(t, queueBody, "Imaginary Hall marketing copy")
	assertNotContains(t, queueBody, `href="/admin/venues/imaginary-hall"`)

	db := mustRawDB(t, path)
	defer db.Close()
	var validationState string
	if err := db.QueryRow(`SELECT validation_state FROM venues WHERE slug = ?`, "imaginary-hall").Scan(&validationState); err != nil {
		t.Fatalf("lookup validation state: %v", err)
	}
	if got, want := validationState, string(domain.ValidationStateValidated); got != want {
		t.Fatalf("validation_state = %q, want %q", got, want)
	}

	afterVenueBody := renderPath(t, server, "/venues/imaginary-hall")
	assertContains(t, afterVenueBody, "Imaginary Hall marketing copy")
	afterEventBody := renderPath(t, server, "/events/imaginary-hall-future-show")
	assertContains(t, afterEventBody, "Future Show")
	assertContains(t, afterEventBody, "Imaginary Hall marketing copy")
}

func TestSQLiteAdminVenueDetailPostSavesEditedFields(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	form := strings.NewReader(url.Values{
		"action":        {"save"},
		"name":          {"Imaginary Hall"},
		"address":       {"99 Updated Street, Sheffield"},
		"neighbourhood": {"Kelham"},
		"description":   {"Updated venue description."},
		"website":       {"https://example.test/imaginary-hall-updated"},
		"coverage_kind": {"program"},
		"coverage_note": {"Programme-only while listings settle."},
	}.Encode())
	req := httptest.NewRequest(http.MethodPost, "/admin/venues/imaginary-hall", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/venues/imaginary-hall?saved=1" {
		t.Fatalf("Location = %q, want %q", location, "/admin/venues/imaginary-hall?saved=1")
	}

	venue, ok, err := st.LoadVenueBySlug(contextForTesting(), "imaginary-hall")
	if err != nil {
		t.Fatalf("load venue: %v", err)
	}
	if !ok {
		t.Fatal("saved venue not found")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "99 Updated Street, Sheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "99 Updated Street, Sheffield")
	}
	if venue.Neighbourhood != "Kelham" {
		t.Fatalf("venue neighbourhood = %q, want %q", venue.Neighbourhood, "Kelham")
	}
	if venue.Description != "Updated venue description." {
		t.Fatalf("venue description = %q, want %q", venue.Description, "Updated venue description.")
	}
	if venue.Website != "https://example.test/imaginary-hall-updated" {
		t.Fatalf("venue website = %q, want %q", venue.Website, "https://example.test/imaginary-hall-updated")
	}
	if venue.CoverageKind != domain.CoverageKindProgram {
		t.Fatalf("venue coverage kind = %q, want %q", venue.CoverageKind, domain.CoverageKindProgram)
	}
	if venue.CoverageNote != "Programme-only while listings settle." {
		t.Fatalf("venue coverage note = %q, want %q", venue.CoverageNote, "Programme-only while listings settle.")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}

	body := renderPath(t, server, "/admin/venues/imaginary-hall?saved=1")
	assertContains(t, body, "Venue saved.")
	assertContains(t, body, `name="name" value="Imaginary Hall"`)
	assertContains(t, body, "99 Updated Street,\nSheffield")
	assertContains(t, body, "Programme-only while listings settle.")
}

func TestFormatVenueAddress(t *testing.T) {
	tests := []struct {
		name      string
		venueName string
		address   string
		want      string
	}{
		{
			name:      "drops duplicate first line",
			venueName: "Memorial Hall",
			address:   `Memorial Hall\, Barkers Pool\, Sheffield\, S1 2JA`,
			want:      "Barkers Pool,\nSheffield,\nS1 2JA",
		},
		{
			name:      "keeps non duplicate first line",
			venueName: "Yellow Arch Studios",
			address:   "Yellow Arch Road, Neepsend, Sheffield, S3 8BX",
			want:      "Yellow Arch Road,\nNeepsend,\nSheffield,\nS3 8BX",
		},
		{
			name:      "drops leading the variant",
			venueName: "Leadmill",
			address:   "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
			want:      "6 Leadmill Road,\nSheffield City Centre,\nSheffield S1 4SE",
		},
		{
			name:      "drops ampersand and and variant",
			venueName: "Sidney & Matilda",
			address:   "Sidney and Matilda, Rivelin Works, Sheffield",
			want:      "Rivelin Works,\nSheffield",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatVenueAddress(tc.venueName, tc.address)
			if got != tc.want {
				t.Fatalf("formatVenueAddress() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSQLiteAdminVenueDetailPostRejectsMissingAndNonProvisionalVenues(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	tests := []string{
		"/admin/venues/missing-room",
		"/admin/venues/validated-room",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("action=validate"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d; body %q", path, rr.Code, http.StatusNotFound, rr.Body.String())
		}
		assertContains(t, rr.Body.String(), "404 page not found")
	}
}

func TestSQLiteAdminVenueDetailPostRejectsInvalidCoverageKind(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	req := httptest.NewRequest(http.MethodPost, "/admin/venues/imaginary-hall", strings.NewReader("action=save&coverage_kind=sideways"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "invalid coverage kind")
}

func TestAdminPagesLinkToProvisionalVenues(t *testing.T) {
	st, server, runID, _, path := mustImportRunDetailServer(t, false)
	defer st.Close()

	groupID := mustCreateWebReviewGroupForImportRun(t, st, path, "Fixture review group", "Created from manual ingest run "+strconvFormatInt(runID)+" review staging.", 2)

	reviewBody := renderPath(t, server, "/admin/review")
	assertContains(t, reviewBody, `href="/admin/venues"`)

	reviewDetailBody := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, reviewDetailBody, `href="/admin/venues"`)

	importRunsBody := renderPath(t, server, "/admin/import-runs")
	assertContains(t, importRunsBody, `href="/admin/venues"`)

	importRunDetailBody := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, importRunDetailBody, `href="/admin/venues"`)
}

func TestAdminReviewHistoryLinksToProvisionalVenues(t *testing.T) {
	server, err := NewServer(testServerDeps(reviewOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review/history")
	assertContains(t, body, `href="/admin/venues"`)
}

func TestAdminImportRunDetailReplayOnlyShowsProvisionalVenuesLink(t *testing.T) {
	server, err := NewServer(testServerDeps(replayOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs/1")
	assertContains(t, body, "Import run #1")
	assertContains(t, body, `href="/admin/venues"`)
	assertNotContains(t, body, `href="/admin/review"`)
	assertNotContains(t, body, `href="/admin/import-runs"`)
}

func TestSQLiteAdminImportRunDetailInvalidAndMissingIDs(t *testing.T) {
	st, server, _, _, _ := mustImportRunDetailServer(t, false)
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
	server, err := NewServer(testServerDeps(store.NewSeedStore()))
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
	server, err := NewServer(testServerDeps(importHistoryWithDetailNoReviewStoreStub{}))
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
	server, err := NewServer(testServerDeps(importHistoryOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "Run #1")
	assertNotContains(t, body, `href="/admin/import-runs/1"`)
	assertNotContains(t, body, "<th scope=\"col\">Review groups</th>")
}

func TestAdminImportRunsReviewGroupSummaryIsPlainTextWithoutDetailStore(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryWithReviewGroupsNoDetailStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "<th scope=\"col\">Review groups</th>")
	assertContains(t, body, ">1 open, 2 resolved</td>")
	assertNotContains(t, body, `href="/admin/import-runs/1"`)
}

func TestSQLiteAdminImportRunDetailMalformedPayloadDoesNotCrash(t *testing.T) {
	st, server, runID, rawPayloadText, _ := mustImportRunDetailServer(t, true)
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
	assertNotContains(t, body, "Also seen from other sources")
}

func TestSQLiteEventDetailRendersSecondarySourceInfo(t *testing.T) {
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

	db := mustRawDB(t, path)
	defer db.Close()
	primarySourceID := mustInsertAdminSource(t, db, "Primary source", "https://example.test/primary")
	mustInsertAdminEvent(
		t,
		db,
		primarySourceID,
		"secondary-source-event",
		"leadmill",
		"Secondary Source Event",
		fixtureLocalTime(2026, time.May, 8, 19, 30),
		fixtureLocalTime(2026, time.May, 8, 22, 30),
		"Primary description.",
	)
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "secondary-source-event").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Songkick mirror", "https://example.test/songkick/matinee-noise"); err != nil {
		t.Fatalf("insert secondary source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Songkick mirror", "https://example.test/songkick/matinee-noise").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	for _, row := range []struct {
		infoType string
		value    string
	}{
		{infoType: "genre", value: "Post-punk"},
		{infoType: "description", value: "Late update from a secondary listing."},
	} {
		if _, err := db.Exec(`
			INSERT INTO event_secondary_source_info (
				event_id,
				source_id,
				venue_slug,
				event_name,
				start_at,
				info_type,
				value,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, eventID, sourceID, "leadmill", "Secondary Source Event", "2026-05-08T18:30:00Z", row.infoType, row.value, "2026-04-21T10:00:00Z", "2026-04-21T10:00:00Z"); err != nil {
			t.Fatalf("insert secondary source info row: %v", err)
		}
	}

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/events/secondary-source-event")
	assertContains(t, body, "Also seen from other sources")
	assertContains(t, body, `href="https://example.test/songkick/matinee-noise"`)
	assertContains(t, body, "Songkick mirror")
	assertContains(t, body, "<strong>Genre</strong>: Post-punk")
	assertContains(t, body, "Late update from a secondary listing.")
	assertContains(t, body, "Primary source")
}

func TestSQLiteEventDetailOmitsSecondarySourceInfoWhenNoneExists(t *testing.T) {
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

	db := mustRawDB(t, path)
	defer db.Close()
	primarySourceID := mustInsertAdminSource(t, db, "Primary source", "https://example.test/primary")
	mustInsertAdminEvent(
		t,
		db,
		primarySourceID,
		"primary-only-event",
		"leadmill",
		"Primary Only Event",
		fixtureLocalTime(2026, time.May, 8, 19, 30),
		fixtureLocalTime(2026, time.May, 8, 22, 30),
		"Primary description.",
	)

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/events/primary-only-event")
	assertNotContains(t, body, "Also seen from other sources")
	assertContains(t, body, "Primary source")
}

func TestSQLiteEventDetailRendersAllInferredGenres(t *testing.T) {
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

	db := mustRawDB(t, path)
	defer db.Close()
	sourceID := mustInsertAdminSource(t, db, "Primary source", "https://example.test/primary-genres")
	mustInsertAdminEvent(
		t,
		db,
		sourceID,
		"inferred-genre-event",
		"leadmill",
		"Inferred Genre Event",
		fixtureLocalTime(2026, time.May, 8, 19, 30),
		fixtureLocalTime(2026, time.May, 8, 22, 30),
		"Jazz, rock and experimental jazz.",
	)
	if err := st.RecomputeEventGenres(context.Background()); err != nil {
		t.Fatalf("recompute event genres: %v", err)
	}

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/events/inferred-genre-event")
	assertContains(t, body, "All genres")
	assertContains(t, body, "Jazz, Rock, Experimental")
	assertContains(t, body, "<p>Jazz, Rock</p>")
	assertNotContains(t, body, "Jazz, Rock · Listed")
}

func TestSQLiteAdminConfigurationListsAndSavesGenreRules(t *testing.T) {
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/configuration")
	assertContains(t, body, "Genre rules")
	assertContains(t, body, "jazz")
	assertContains(t, body, "regex")

	form := url.Values{}
	form.Set("action", "save")
	form.Set("key", "doom-metal")
	form.Set("name", "Doom metal")
	form.Set("match_type", "plain")
	form.Set("pattern", "doom metal")
	form.Set("enabled", "1")
	form.Set("sort_order", "320")
	req := httptest.NewRequest(http.MethodPost, "/admin/configuration", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/configuration?saved=1" {
		t.Fatalf("Location = %q, want saved redirect", location)
	}

	body = renderPath(t, server, "/admin/configuration?saved=1")
	assertContains(t, body, "Genre rule saved.")
	assertContains(t, body, "Doom metal")

	badForm := url.Values{}
	badForm.Set("action", "save")
	badForm.Set("key", "broken")
	badForm.Set("name", "Broken")
	badForm.Set("match_type", "regex")
	badForm.Set("pattern", "[")
	req = httptest.NewRequest(http.MethodPost, "/admin/configuration", strings.NewReader(badForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	assertContains(t, rr.Body.String(), "invalid regex")
}

func TestAdminConfigurationPostLogsStoreFailure(t *testing.T) {
	var logs bytes.Buffer
	logger, err := logging.NewLogger(&logs, logging.Config{})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	deps := testServerDeps(failingGenreConfigurationStore{saveErr: fmt.Errorf("save failed")})
	deps.Logger = logger
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	form := url.Values{}
	form.Set("action", "save")
	form.Set("key", "doom-metal")
	form.Set("name", "Doom metal")
	form.Set("match_type", "plain")
	form.Set("pattern", "doom metal")
	form.Set("enabled", "1")
	form.Set("sort_order", "320")
	req := httptest.NewRequest(http.MethodPost, "/admin/configuration", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	assertContains(t, rr.Body.String(), "save failed")
	got := logs.String()
	for _, want := range []string{
		`msg="save genre rule"`,
		`error="save failed"`,
		`path=/admin/configuration`,
		`msg="http request"`,
		`status=400`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs = %q, want %q", got, want)
		}
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

func TestHomeVenueCardsUseAddressFallbackMeta(t *testing.T) {
	server := mustClockedServer(t, store.NewStore([]domain.Venue{
		{
			Slug:          "neighbourhood-room",
			Name:          "Neighbourhood Room",
			Address:       "1 Neighbourhood Road, Sheffield",
			Neighbourhood: "Kelham",
		},
		{
			Slug:    "address-room",
			Name:    "Address Room",
			Address: "12 Address Street, Sheffield",
		},
		{
			Slug:    "duplicate-room",
			Name:    "Duplicate Room",
			Address: "Duplicate Room, Hidden Lane, Sheffield",
		},
		{
			Slug: "blank-room",
			Name: "Blank Room",
		},
	}, nil))

	body := renderPath(t, server, "/")

	assertContains(t, body, `<span class="venue-title">Neighbourhood Room</span>`)
	assertContains(t, body, `<span class="venue-meta">Kelham</span>`)
	assertContains(t, body, `<span class="venue-title">Address Room</span>`)
	assertContains(t, body, `<span class="venue-meta">12 Address Street</span>`)
	assertContains(t, body, `<span class="venue-title">Duplicate Room</span>`)
	assertContains(t, body, `<span class="venue-meta">Hidden Lane</span>`)
	assertNotContains(t, body, `<span class="venue-meta">Duplicate Room</span>`)
	assertContains(t, body, `<span class="venue-title">Blank Room</span>
      <span class="venue-meta"></span>`)
}

func TestEventPagesRenderVenueRoom(t *testing.T) {
	server := mustClockedServer(t, store.NewStore([]domain.Venue{
		{
			Slug:          "sidney-and-matilda",
			Name:          "Sidney & Matilda",
			Address:       "Rivelin Works, 46 Sidney Street, Sheffield",
			Neighbourhood: "Cultural Industries Quarter",
			Website:       "https://www.sidneyandmatilda.com/",
		},
	}, []domain.Event{
		{
			Slug:      "parallel-delusion",
			Name:      "Parallel Delusion",
			VenueSlug: "sidney-and-matilda",
			Rooms: []domain.VenueRoom{{
				VenueSlug: "sidney-and-matilda",
				Slug:      "factory",
				Name:      "Factory",
			}},
			Start:       fixtureLocalTime(2026, time.April, 20, 19, 30),
			End:         fixtureLocalTime(2026, time.April, 20, 22, 0),
			Genre:       "Experimental",
			Status:      "Listed",
			Description: "Factory room fixture.",
			SourceName:  "Fixture ICS",
			SourceURL:   "file:fixture.ics",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			Origin:      domain.OriginLive,
		},
	}))

	eventsBody := renderPath(t, server, "/events")
	assertContains(t, eventsBody, "Sidney &amp; Matilda (Factory) · Experimental")
	assertNotContains(t, eventsBody, "Experimental · Listed")

	eventBody := renderPath(t, server, "/events/parallel-delusion")
	assertContains(t, eventBody, `<p><a href="/venues/sidney-and-matilda">Sidney &amp; Matilda</a> (Factory)</p>`)

	venueBody := renderPath(t, server, "/venues/sidney-and-matilda")
	assertContains(t, venueBody, `<span class="event-meta">Factory</span>`)
}

func TestPublicEventTitleCleansSourcePresentationLeaks(t *testing.T) {
	venueNames := map[string]string{
		"foundry":             "Foundry",
		"hallamshire-hotel":   "Hallamshire Hotel",
		"memorial-hall":       "Memorial Hall",
		"sidney-and-matilda":  "Sidney & Matilda",
		"yellow-arch-studios": "Yellow Arch Studios",
	}

	tests := []struct {
		name      string
		eventName string
		venueSlug string
		want      string
	}{
		{
			name:      "double escaped ampersand",
			eventName: "S&amp;amp;M Presents: Dealbreaker",
			venueSlug: "sidney-and-matilda",
			want:      "S&M Presents: Dealbreaker",
		},
		{
			name:      "all caps title",
			eventName: "DANSETTE SPRINGS",
			venueSlug: "sidney-and-matilda",
			want:      "Dansette Springs",
		},
		{
			name:      "all caps artist name",
			eventName: "EDWINA HAYES",
			venueSlug: "sidney-and-matilda",
			want:      "Edwina Hayes",
		},
		{
			name:      "single token all caps name",
			eventName: "SLACKRR",
			venueSlug: "sidney-and-matilda",
			want:      "SLACKRR",
		},
		{
			name:      "leading delimiter",
			eventName: "| Sorebones | EP Release Show w/ YURN & Ella Wingfield",
			venueSlug: "sidney-and-matilda",
			want:      "Sorebones | EP Release Show w/ YURN & Ella Wingfield",
		},
		{
			name:      "dash venue suffix",
			eventName: "Marmozets - Foundry",
			venueSlug: "foundry",
			want:      "Marmozets",
		},
		{
			name:      "city venue suffix",
			eventName: "The Bootleg Beatles - Foundry, Sheffield",
			venueSlug: "foundry",
			want:      "The Bootleg Beatles",
		},
		{
			name:      "parenthetical venue suffix",
			eventName: "Dylan Flynn & The Dead Poets (Hallamshire Hotel)",
			venueSlug: "hallamshire-hotel",
			want:      "Dylan Flynn & The Dead Poets",
		},
		{
			name:      "live at venue suffix",
			eventName: "Tom Smith (Editors) live at Memorial Hall",
			venueSlug: "memorial-hall",
			want:      "Tom Smith (Editors)",
		},
		{
			name:      "non matching venue text remains",
			eventName: "PINS plus Gia Ford & Gelder - Yellow Arch (Rescheduled Date)",
			venueSlug: "yellow-arch-studios",
			want:      "PINS plus Gia Ford & Gelder - Yellow Arch (Rescheduled Date)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := domain.Event{Name: tc.eventName, VenueSlug: tc.venueSlug}
			if got := publicEventTitle(event, venueNames); got != tc.want {
				t.Fatalf("publicEventTitle() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPublicEventMetadataOmitsEmptyAndListedStatus(t *testing.T) {
	venueNames := map[string]string{"sidney-and-matilda": "Sidney & Matilda"}

	tests := []struct {
		name       string
		event      domain.Event
		wantCard   string
		wantDetail string
	}{
		{
			name:       "venue only",
			event:      domain.Event{VenueSlug: "sidney-and-matilda"},
			wantCard:   "Sidney & Matilda",
			wantDetail: "",
		},
		{
			name:       "listed hidden",
			event:      domain.Event{VenueSlug: "sidney-and-matilda", Genre: "Indie", Status: "Listed"},
			wantCard:   "Sidney & Matilda · Indie",
			wantDetail: "Indie",
		},
		{
			name:       "confirmed hidden",
			event:      domain.Event{VenueSlug: "sidney-and-matilda", Genre: "Folk", Status: "CONFIRMED"},
			wantCard:   "Sidney & Matilda · Folk",
			wantDetail: "Folk",
		},
		{
			name:       "meaningful status shown",
			event:      domain.Event{VenueSlug: "sidney-and-matilda", Status: "POSTPONED"},
			wantCard:   "Sidney & Matilda · Postponed",
			wantDetail: "Postponed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := publicEventCardMeta(tc.event, venueNames); got != tc.wantCard {
				t.Fatalf("publicEventCardMeta() = %q, want %q", got, tc.wantCard)
			}
			if got := publicEventDetailMeta(tc.event); got != tc.wantDetail {
				t.Fatalf("publicEventDetailMeta() = %q, want %q", got, tc.wantDetail)
			}
		})
	}
}

func TestPublicPagesCleanEventPresentationLeaks(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{
			{
				Slug:          "sidney-and-matilda",
				Name:          "Sidney & Matilda",
				Address:       "Rivelin Works, Sheffield",
				Neighbourhood: "Cultural Industries Quarter",
				Description:   "A Sheffield venue.",
				Website:       "https://example.test/sidney",
			},
			{
				Slug:          "foundry",
				Name:          "Foundry",
				Address:       "Western Bank, Sheffield",
				Neighbourhood: "Broomhall",
				Description:   "A Sheffield venue.",
				Website:       "https://example.test/foundry",
			},
		},
		[]domain.Event{
			{
				Slug:        "double-escaped",
				Name:        "S&amp;M Presents: Dealbreaker",
				VenueSlug:   "sidney-and-matilda",
				Start:       fixtureLocalTime(2026, time.April, 19, 19, 30),
				Description: "Source title contains an escaped ampersand.",
				SourceName:  "Sidney & Matilda manual ingest",
				SourceURL:   "https://example.test/sidney/dealbreaker",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "all-caps-listed",
				Name:        "DANSETTE SPRINGS",
				VenueSlug:   "foundry",
				Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
				Genre:       "Blues",
				Status:      "Listed",
				Description: "All caps source title.",
				SourceName:  "The Greystones manual ingest",
				SourceURL:   "https://example.test/greystones/dansette",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "venue-suffix",
				Name:        "Marmozets - Foundry",
				VenueSlug:   "foundry",
				Start:       fixtureLocalTime(2026, time.April, 20, 20, 0),
				Status:      "Postponed",
				Description: "Venue suffix source title.",
				SourceName:  "The Leadmill manual ingest",
				SourceURL:   "https://example.test/leadmill/marmozets",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))

	homeBody := renderPath(t, server, "/")
	assertContains(t, homeBody, `S&amp;M Presents: Dealbreaker`)
	assertNotContains(t, homeBody, `S&amp;amp;M Presents`)
	assertContains(t, homeBody, `<span class="event-meta">Sidney &amp; Matilda</span>`)
	assertNotContains(t, homeBody, `Sidney &amp; Matilda ·`)

	eventsBody := renderPath(t, server, "/events?window=all")
	assertContains(t, eventsBody, `Dansette Springs`)
	assertContains(t, eventsBody, `<span class="event-meta">Foundry · Blues</span>`)
	assertNotContains(t, eventsBody, `Blues · Listed`)
	assertContains(t, eventsBody, `Marmozets`)
	assertNotContains(t, eventsBody, `Marmozets - Foundry`)
	assertContains(t, eventsBody, `Foundry · Postponed`)

	detailBody := renderPath(t, server, "/events/double-escaped")
	assertContains(t, detailBody, `<h1>S&amp;M Presents: Dealbreaker</h1>`)
	assertNotContains(t, detailBody, `S&amp;amp;M Presents`)
	assertNotContains(t, detailBody, `<p> · </p>`)

	venueBody := renderPath(t, server, "/venues/foundry")
	assertContains(t, venueBody, `<span class="event-title">Marmozets</span>`)
	assertNotContains(t, venueBody, `Marmozets - Foundry`)
}

func TestEventsFiltersToday(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=today")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Sunday, 19 April 2026")
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Friday Leadmill")
}

func TestEventsFiltersTonight(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=tonight")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, `option value="tonight" selected`)
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

func TestEventsFiltersWeekendAndArea(t *testing.T) {
	server := mustFixtureServer(t)

	weekendBody := renderPath(t, server, "/events?window=weekend")
	assertContains(t, weekendBody, "Tonight Leadmill")
	assertNotContains(t, weekendBody, "Friday Leadmill")

	areaBody := renderPath(t, server, "/events?area=Neepsend")
	assertContains(t, areaBody, "Tomorrow Yellow Arch")
	assertContains(t, areaBody, `option value="Neepsend" selected`)
	assertNotContains(t, areaBody, "Tonight Leadmill")
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

func TestEventCardsRenderImagesOnSummaryPages(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
		}},
		[]domain.Event{{
			Slug:        "poster-show",
			Name:        "Poster Show",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Poster description.",
			ImageURL:    "/media/events/poster.jpg",
			ImageAlt:    "Poster Show artwork",
			ImageWidth:  1200,
			ImageHeight: 800,
			ImageFocusX: 35,
			ImageFocusY: 65,
			SourceName:  "Fixture listings",
			SourceURL:   "https://example.test/poster-show",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			Origin:      domain.OriginLive,
		}},
	))

	for _, tc := range []struct {
		name      string
		path      string
		cardClass string
	}{
		{name: "home", path: "/", cardClass: `class="event-card has-image"`},
		{name: "events", path: "/events?window=today", cardClass: `class="event-card wide has-image"`},
		{name: "venue", path: "/venues/leadmill", cardClass: `class="event-card has-image"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := renderPath(t, server, tc.path)
			assertContains(t, body, tc.cardClass)
			assertContains(t, body, `<img class="event-card-image" src="/media/events/poster.jpg" alt="Poster Show artwork" style="--image-focus-x: 35%; --image-focus-y: 65%;" loading="lazy" decoding="async">`)
		})
	}
}

func TestEventCardsPreserveExplicitTopLeftImageFocus(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
		}},
		[]domain.Event{{
			Slug:        "top-left-poster-show",
			Name:        "Top Left Poster Show",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Poster description.",
			ImageURL:    "/media/events/top-left-poster.jpg",
			ImageWidth:  1200,
			ImageHeight: 800,
			ImageFocusX: 0,
			ImageFocusY: 0,
			SourceName:  "Fixture listings",
			SourceURL:   "https://example.test/top-left-poster-show",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			Origin:      domain.OriginLive,
		}},
	))

	body := renderPath(t, server, "/")
	assertContains(t, body, `<img class="event-card-image" src="/media/events/top-left-poster.jpg" alt="Top Left Poster Show" style="--image-focus-x: 0%; --image-focus-y: 0%;" loading="lazy" decoding="async">`)
}

func TestEventDetailRendersHeroAndPortraitImages(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
		}},
		[]domain.Event{
			{
				Slug:        "landscape-show",
				Name:        "Landscape Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Landscape description.",
				ImageURL:    "/media/events/landscape.jpg",
				ImageAlt:    "Landscape Show poster",
				ImageWidth:  1600,
				ImageHeight: 900,
				ImageFocusX: 25,
				ImageFocusY: 75,
				SourceName:  "Fixture listings",
				SourceURL:   "https://example.test/landscape-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:      domain.OriginLive,
			},
			{
				Slug:        "portrait-show",
				Name:        "Portrait Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 20, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 20, 22, 0),
				Genre:       "Rock",
				Status:      "Listed",
				Description: "Portrait description.",
				ImageURL:    "/media/events/portrait.jpg",
				ImageWidth:  800,
				ImageHeight: 1200,
				ImageFocusX: 60,
				ImageFocusY: 40,
				SourceName:  "Fixture listings",
				SourceURL:   "https://example.test/portrait-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:      domain.OriginLive,
			},
		},
	))

	landscapeBody := renderPath(t, server, "/events/landscape-show")
	assertContains(t, landscapeBody, `<header class="event-detail-head hero-image">`)
	assertContains(t, landscapeBody, `<img src="/media/events/landscape.jpg" alt="Landscape Show poster" style="--image-focus-x: 25%; --image-focus-y: 75%;" loading="eager" decoding="async">`)

	portraitBody := renderPath(t, server, "/events/portrait-show")
	assertContains(t, portraitBody, `<header class="event-detail-head portrait-image">`)
	assertContains(t, portraitBody, `<img src="/media/events/portrait.jpg" alt="Portrait Show" style="--image-focus-x: 60%; --image-focus-y: 40%;" loading="eager" decoding="async">`)
}

func TestVenueDetailShowsEmptyState(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/venues/empty-room")

	assertContains(t, body, "No upcoming shows listed for this venue.")
}

func TestVenueAndEventDetailShowCoverageNote(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "lescar",
			Name:          "The Lescar",
			Address:       "303 Sharrow Vale Road, Sheffield",
			Neighbourhood: "Sharrow Vale",
			Description:   "Venue",
			Website:       "https://example.test/lescar",
			CoverageKind:  domain.CoverageKindProgram,
			CoverageNote:  "Programme-only coverage.",
		}},
		[]domain.Event{{
			Slug:        "lescar-jazz",
			Name:        "Lescar Jazz",
			VenueSlug:   "lescar",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:       "Jazz",
			Status:      "Listed",
			Description: "Event",
			SourceName:  "Jazz at The Lescar manual ingest",
			SourceURL:   "https://example.test/lescar-jazz",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
		}},
	))

	venueBody := renderPath(t, server, "/venues/lescar")
	assertContains(t, venueBody, "Coverage note")
	assertContains(t, venueBody, "Programme-only coverage.")

	eventBody := renderPath(t, server, "/events/lescar-jazz")
	assertContains(t, eventBody, "Coverage note")
	assertContains(t, eventBody, "Programme-only coverage.")
	assertContains(t, eventBody, "View official listing")
}

func TestEventDetailSeparatesOfficialListingAndCalendarLinks(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
			CoverageKind:  domain.CoverageKindVenue,
		}},
		[]domain.Event{{
			Slug:               "leadmill-calendar",
			Name:               "Leadmill Calendar",
			VenueSlug:          "leadmill",
			Start:              fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:                fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:              "Indie",
			Status:             "Listed",
			Description:        "Event",
			SourceName:         "The Leadmill manual ingest",
			SourceURL:          "https://leadmill.co.uk/listings/?ical=1",
			OfficialListingURL: "https://leadmill.co.uk/event/leadmill-calendar/",
			CalendarURL:        "https://leadmill.co.uk/listings/?ical=1",
			LastChecked:        fixtureLocalTime(2026, time.April, 19, 9, 0),
		}},
	))

	body := renderPath(t, server, "/events/leadmill-calendar")
	assertContains(t, body, `class="primary-button" href="https://leadmill.co.uk/event/leadmill-calendar/">View official listing`)
	assertContains(t, body, `class="icon-button" href="https://leadmill.co.uk/listings/?ical=1" aria-label="Open calendar item"`)
	assertNotContains(t, body, `class="primary-button" href="https://leadmill.co.uk/listings/?ical=1"`)
}

func TestEventDetailFallsBackToVenueWebsiteWhenOnlyCalendarSourceIsAvailable(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
			CoverageKind:  domain.CoverageKindVenue,
		}},
		[]domain.Event{{
			Slug:        "legacy-calendar-source",
			Name:        "Legacy Calendar Source",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Event",
			SourceName:  "Legacy calendar source",
			SourceURL:   "https://example.test/events.ics",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
		}},
	))

	body := renderPath(t, server, "/events/legacy-calendar-source")
	assertContains(t, body, `class="primary-button" href="https://example.test/leadmill">View official listing`)
	assertContains(t, body, `class="icon-button" href="https://example.test/events.ics" aria-label="Open calendar item"`)
	assertNotContains(t, body, `class="primary-button" href="https://example.test/events.ics"`)
}

func TestEventDetailShowsStartOnlyCopyWhenEndIsUnknown(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
			CoverageKind:  domain.CoverageKindVenue,
		}},
		[]domain.Event{{
			Slug:        "leadmill-unknown-end",
			Name:        "Leadmill Unknown End",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Event",
			SourceName:  "Leadmill manual ingest",
			SourceURL:   "https://example.test/leadmill-unknown-end",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
		}},
	))

	body := renderPath(t, server, "/events/leadmill-unknown-end")
	assertContains(t, body, "Starts at 20:00")
	assertNotContains(t, body, "20:00 to")
}

func TestReadyzReturnsServiceUnavailableWhenReadinessFails(t *testing.T) {
	server, err := NewServer(ServerDeps{
		Catalog:      store.NewSeedStore(),
		ReadyChecker: failingReadyChecker{},
		AdminAuth:    AdminAuthConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

func TestReadyzLogsReadinessFailure(t *testing.T) {
	var logs bytes.Buffer
	logger, err := logging.NewLogger(&logs, logging.Config{})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	server, err := NewServer(ServerDeps{
		Catalog:      store.NewSeedStore(),
		ReadyChecker: failingReadyChecker{},
		Logger:       logger,
		AdminAuth:    AdminAuthConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
	got := logs.String()
	for _, want := range []string{
		`msg="readiness check failed"`,
		`error="not ready"`,
		`msg="http request"`,
		`path=/readyz`,
		`status=503`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs = %q, want %q", got, want)
		}
	}
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
	form.Set("choice_image_url", strconvFormatInt(group.Candidates[0].ID))
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
	assertContains(t, body, `href="/admin/venues"`)
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

func TestSQLiteAdminReviewHistoryRendersOriginImportRunColumn(t *testing.T) {
	st, server, _, path := mustReviewServerWithGroupPath(t)
	defer st.Close()

	linkedID := mustCreateWebReviewGroupForImportRun(t, st, path, "Linked history group", "Created from manual ingest run 12 review staging.", 1)
	noLinkID := mustCreateWebReviewGroupForImportRun(t, st, path, "Offline history group", "Created from offline fixture.", 1)
	if err := st.UpdateReviewGroupStatus(contextForTesting(), linkedID, review.StatusRejected); err != nil {
		t.Fatalf("reject linked review group: %v", err)
	}
	if err := st.UpdateReviewGroupStatus(contextForTesting(), noLinkID, review.StatusRejected); err != nil {
		t.Fatalf("reject offline review group: %v", err)
	}

	db := mustRawDB(t, path)
	if err := setWebReviewGroupUpdatedAt(db, linkedID, "2026-04-20T11:00:00Z"); err != nil {
		t.Fatalf("set linked updated_at: %v", err)
	}
	if err := setWebReviewGroupUpdatedAt(db, noLinkID, "2026-04-20T12:00:00Z"); err != nil {
		t.Fatalf("set offline updated_at: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	body := renderPath(t, server, "/admin/review/history")
	assertContains(t, body, `<th scope="col">Import run</th>`)
	assertContains(t, body, `href="/admin/import-runs/12">Import run #12</a>`)
	assertInOrder(t, body, []string{"Offline history group", "<td>&mdash;</td>"})
	assertInOrder(t, body, []string{"Offline history group", "Linked history group"})
}

func TestAdminReviewHistoryHidesOriginImportRunColumnWithoutDetailSupport(t *testing.T) {
	server, err := NewServer(testServerDeps(reviewOnlyStoreStub{
		closedGroups: []review.GroupSummary{
			{
				ID:             1,
				Title:          "Linked history group",
				SourceName:     "Fixture ICS",
				SourceURL:      "file:test.ics",
				Status:         review.StatusRejected,
				Notes:          "Created from manual ingest run 12 review staging.",
				CreatedAt:      time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
				UpdatedAt:      time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC),
				CandidateCount: 1,
			},
		},
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review/history")
	assertContains(t, body, "Linked history group")
	assertNotContains(t, body, `<th scope="col">Import run</th>`)
	assertNotContains(t, body, `href="/admin/import-runs/12"`)
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
	server, err := NewServer(testServerDeps(reviewImportHistoryOnlyStoreStub{}))
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

func TestAdminReviewDetailShowsOriginImportRunWithoutDetailLink(t *testing.T) {
	server, err := NewServer(testServerDeps(reviewImportHistoryOnlyStoreStub{
		reviewOnlyStoreStub: reviewOnlyStoreStub{
			group: review.Group{
				ID:                   12,
				Title:                "Linked review",
				SourceName:           "Fixture ICS",
				SourceURL:            "file:test.ics",
				Status:               review.StatusOpen,
				Notes:                "Created from manual ingest run 123 review staging.",
				StagedCandidateCount: 1,
				LatestImportRunID:    123,
			},
		},
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review/12")
	assertContains(t, body, "Import run #123")
	assertNotContains(t, body, `href="/admin/import-runs/123"`)
	assertContains(t, body, "Run #123")
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

	server, err := NewServer(testServerDeps(reviewOnlyStoreStub{
		group: review.Group{
			ID:                   1,
			Title:                "Fixture review",
			SourceName:           "Fixture ICS",
			SourceURL:            "file:sidney.ics",
			Status:               review.StatusOpen,
			Notes:                "Created from manual ingest run 123 review staging.",
			StagedCandidateCount: 1,
			DraftChoices:         map[review.Field]review.DraftChoice{},
			Candidates: []review.Candidate{
				{ID: 1, Position: 1, Name: "Solo Show"},
			},
		},
	}))
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

	server, err := NewServer(testServerDeps(st))
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
		StagedCandidateCount: 2,
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

func TestBuildReviewDetailPrefersDraftChoicesOverDefaultsAndMarksConsensus(t *testing.T) {
	detail := buildReviewDetail(review.Group{
		StagedCandidateCount: 2,
		Candidates: []review.Candidate{
			{ID: 1, Position: 1, Name: "Staged Alpha"},
			{ID: 2, Position: 2, Name: "Staged Beta"},
			{ID: 3, Position: 3, CanonicalEventID: 99, Name: "Live Canonical"},
		},
		DraftChoices: map[review.Field]review.DraftChoice{
			review.FieldName: {
				Field:       review.FieldName,
				CandidateID: 2,
				Value:       "Staged Beta",
			},
		},
		DefaultChoices: map[review.Field]review.DraftChoice{
			review.FieldName: {
				Field:       review.FieldName,
				CandidateID: 3,
				Value:       "Live Canonical",
			},
		},
	})

	if !detail.IsDuplicate {
		t.Fatal("duplicate review = false, want true")
	}
	row := detail.Rows[0]
	if !row.Cells[1].Checked {
		t.Fatal("draft-selected cell = unchecked, want checked")
	}
	if row.Cells[1].SelectedConsensus {
		t.Fatal("draft-selected cell = selected consensus, want false")
	}
	if !row.Cells[2].Consensus {
		t.Fatal("default cell = non-consensus, want consensus")
	}
	if got, want := detail.CanonicalSummaryRows[0].Candidate, "Candidate 2"; got != want {
		t.Fatalf("summary candidate = %q, want %q", got, want)
	}
	if !detail.CanonicalSummaryRows[0].Selected {
		t.Fatal("summary row = unselected, want selected")
	}
}

func TestBuildReviewDetailFallsBackToDefaultChoicesAndLabelsCanonicalSnapshots(t *testing.T) {
	detail := buildReviewDetail(review.Group{
		StagedCandidateCount: 2,
		Candidates: []review.Candidate{
			{ID: 1, Position: 1, Name: "Staged Alpha"},
			{ID: 2, Position: 2, Name: "Staged Beta"},
			{ID: 3, Position: 3, CanonicalEventID: 99, Name: "Live Canonical"},
		},
		DefaultChoices: map[review.Field]review.DraftChoice{
			review.FieldName: {
				Field:       review.FieldName,
				CandidateID: 3,
				Value:       "Live Canonical",
			},
		},
	})

	row := detail.Rows[0]
	if !row.Cells[2].Checked {
		t.Fatal("default-selected cell = unchecked, want checked")
	}
	if !row.Cells[2].Consensus {
		t.Fatal("default-selected cell = non-consensus, want consensus")
	}
	if !row.Cells[2].SelectedConsensus {
		t.Fatal("default-selected cell = non-selected-consensus, want true")
	}
	if got, want := detail.CanonicalSummaryRows[0].Candidate, "Live canonical snapshot"; got != want {
		t.Fatalf("summary candidate = %q, want %q", got, want)
	}
	if got, want := detail.CanonicalSummaryRows[0].Value, "Live Canonical"; got != want {
		t.Fatalf("summary value = %q, want %q", got, want)
	}
}

func TestSQLiteAdminReviewDetailRendersCanonicalSnapshotRowsAndConsensusStyles(t *testing.T) {
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	groupID := mustCreateWebCanonicalReviewGroupForImportRun(t, st, path, "Canonical snapshot review", "Created from manual ingest run 1 review staging.")
	body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, body, "Live canonical snapshot")
	assertContains(t, body, "selected-consensus")
	assertContains(t, body, "consensus")
	assertContains(t, body, "No draft choices saved yet.")
}

func TestSQLiteAdminReviewDetailClosedViewStillShowsCanonicalRowsAndFinalSelections(t *testing.T) {
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	notes := "Created from manual ingest run 1 review staging."
	ensureImportRunFixtureForNotes(t, path, notes)
	groupID, err := st.CreateReviewGroup(contextForTesting(), review.GroupInput{
		Title:      "Canonical snapshot review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:canonical.ics",
		Notes:      notes,
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "staged-1",
				Name:        "Staged Alpha",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First staged description",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/staged-alpha",
				Provenance:  "fixture UID staged-1",
			},
			{
				ExternalID:  "staged-2",
				Name:        "Staged Beta",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-02T18:30:00Z",
				EndAt:       "2026-05-02T21:30:00Z",
				Genre:       "Rock",
				Status:      "Listed",
				Description: "Second staged description",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/staged-beta",
				Provenance:  "fixture UID staged-2",
			},
			{
				ExternalID:       "canonical-1",
				CanonicalEventID: 77,
				Name:             "Live Canonical",
				VenueSlug:        "leadmill",
				StartAt:          "2026-05-02T18:30:00Z",
				EndAt:            "2026-05-02T21:30:00Z",
				Genre:            "Rock",
				Status:           "Listed",
				Description:      "Second staged description",
				SourceName:       "Fixture ICS",
				SourceURL:        "https://example.test/canonical",
				Provenance:       "fixture canonical snapshot",
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
	if len(group.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3", len(group.Candidates))
	}
	canonical := group.Candidates[2]
	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		DELETE FROM review_field_defaults
		WHERE group_id = ?
	`, groupID); err != nil {
		t.Fatalf("clear review field defaults: %v", err)
	}
	for _, field := range review.CanonicalFields {
		if _, err := db.Exec(`
			INSERT INTO review_field_defaults (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
		`, groupID, string(field), canonical.ID, review.CandidateValue(canonical, field), "2026-04-21T10:00:00Z"); err != nil {
			t.Fatalf("set review field default for %s: %v", field, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	group, ok, err = st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after defaults update")
	}
	if err := st.ResolveReviewGroup(contextForTesting(), groupID, fullWebReviewChoicesForCandidate(t, canonical.ID)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	body := renderPath(t, server, "/admin/review/"+strconvFormatInt(groupID))
	assertContains(t, body, "This review is closed and read-only.")
	assertContains(t, body, "Live canonical snapshot")
	assertContains(t, body, "selected-consensus")
}

func TestAdminReviewQueueAndImportRunCountsStayStagedOnly(t *testing.T) {
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	groupID := mustCreateWebCanonicalReviewGroupForImportRun(t, st, path, "Canonical snapshot review", "Created from manual ingest run 1 review staging.")
	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Canonical snapshot review")
	assertContains(t, body, "Duplicate review - 2 candidates")
	assertNotContains(t, body, "Duplicate review - 3 candidates")

	importRunBody := renderPath(t, server, "/admin/import-runs/1")
	assertContains(t, importRunBody, "Review groups from this import run")
	assertContains(t, importRunBody, "Canonical snapshot review")
	assertContains(t, importRunBody, ">2</td>")
	assertNotContains(t, importRunBody, ">3</td>")
	if groupID <= 0 {
		t.Fatal("group ID = 0, want positive")
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
	form.Set("choice_image_url", strconvFormatInt(group.Candidates[0].ID))
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

func TestAdminReviewSingletonAcceptPublishesUnknownEndWhenCandidateEndIsBlank(t *testing.T) {
	st, server, groupID, path := mustReviewServerWithSingletonGroup(t)
	defer st.Close()

	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET end_at = ''
		WHERE id = ?
	`, group.Candidates[0].ID); err != nil {
		t.Fatalf("blank candidate end field: %v", err)
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

	event, ok := st.EventBySlug("live-solo-show-sidney-and-matilda-20260503190000")
	if !ok {
		t.Fatal("published event not found")
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
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
		{Field: review.FieldImageURL, CandidateID: group.Candidates[0].ID},
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

	server, err := NewServer(testServerDeps(st))
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
	ensureImportRunFixtureForNotes(t, path, notes)

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

	server, err := NewServer(testServerDeps(st))
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		_ = st.Close()
		t.Fatalf("new server: %v", err)
	}
	return st, server, groupID, path
}

func mustAdminVenuesServer(t *testing.T) (*sqlitestore.Store, *Server, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		_ = st.Close()
		t.Fatalf("new server: %v", err)
	}
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 10, 0)
	})
	return st, server, path
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

func setWebReviewGroupUpdatedAt(db *sql.DB, groupID int64, updatedAt string) error {
	_, err := db.Exec(`
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, updatedAt, groupID)
	return err
}

func mustImportRunDetailServer(t *testing.T, malformed bool) (*sqlitestore.Store, *Server, int64, string, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	server, err := NewServer(testServerDeps(st))
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

	return st, server, runID, bodyText, path
}

func seedAdminVenueFixtures(t *testing.T, path string) {
	t.Helper()

	db := mustRawDB(t, path)
	defer db.Close()

	mustInsertAdminVenue(t, db, domain.Venue{
		Slug:            "imaginary-hall",
		Name:            "Imaginary Hall marketing copy",
		Address:         "1 Void Street, Sheffield",
		Neighbourhood:   "City Centre",
		Description:     "Pop-up room for test fixtures.",
		Website:         "https://example.test/imaginary-hall",
		ValidationState: domain.ValidationStateProvisional,
		CoverageKind:    domain.CoverageKindVenue,
		Origin:          domain.OriginLive,
	})
	mustInsertAdminVenue(t, db, domain.Venue{
		Slug:            "quiet-room",
		Name:            "Quiet Room",
		ValidationState: domain.ValidationStateProvisional,
		Origin:          domain.OriginLive,
	})
	mustInsertAdminVenue(t, db, domain.Venue{
		Slug:            "validated-room",
		Name:            "Validated Room",
		Address:         "10 Verified Street, Sheffield",
		ValidationState: domain.ValidationStateValidated,
		Origin:          domain.OriginLive,
	})

	sourceID := mustInsertAdminSource(t, db, "Fixture ICS", "https://example.test/fixture")
	mustInsertAdminEvent(t, db, sourceID, "imaginary-hall-future-show", "imaginary-hall", "Future Show", fixtureLocalTime(2026, time.April, 20, 19, 30), fixtureLocalTime(2026, time.April, 20, 22, 0), "Upcoming linked event description.")
	mustInsertAdminEvent(t, db, sourceID, "imaginary-hall-past-show", "imaginary-hall", "Past Show", fixtureLocalTime(2026, time.April, 18, 19, 0), fixtureLocalTime(2026, time.April, 18, 21, 0), "Past linked event description.")
	mustInsertAdminEvent(t, db, sourceID, "validated-room-future-show", "validated-room", "Validated Venue Show", fixtureLocalTime(2026, time.April, 21, 20, 0), fixtureLocalTime(2026, time.April, 21, 22, 0), "Validated venue event.")
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

func ensureImportRunFixtureForNotes(t *testing.T, path, notes string) {
	t.Helper()

	importRunID, ok := review.ParseOriginImportRunID(notes)
	if !ok {
		return
	}
	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, importRunID, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "fixture import run"); err != nil {
		t.Fatalf("insert import run fixture %d: %v", importRunID, err)
	}
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

func fullWebReviewChoicesForCandidate(t *testing.T, candidateID int64) []review.DraftChoiceInput {
	t.Helper()

	if candidateID <= 0 {
		t.Fatal("candidate ID is required")
	}
	choices := make([]review.DraftChoiceInput, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		choices = append(choices, review.DraftChoiceInput{
			Field:       field,
			CandidateID: candidateID,
		})
	}
	return choices
}

func mustCreateWebCanonicalReviewGroupForImportRun(t *testing.T, st *sqlitestore.Store, path, title, notes string) int64 {
	t.Helper()

	groupID := mustCreateWebReviewGroupForImportRun(t, st, path, title, notes, 3)
	group, ok, err := st.LoadReviewGroup(contextForTesting(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if len(group.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3", len(group.Candidates))
	}
	canonical := group.Candidates[2]

	db := mustRawDB(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close raw db: %v", err)
		}
	}()

	if _, err := db.Exec(`
		UPDATE review_candidates
		SET canonical_event_id = ?
		WHERE id = ?
	`, canonical.ID+1000, canonical.ID); err != nil {
		t.Fatalf("mark canonical snapshot candidate: %v", err)
	}
	if _, err := db.Exec(`
		DELETE FROM review_field_defaults
		WHERE group_id = ?
	`, groupID); err != nil {
		t.Fatalf("clear review field defaults: %v", err)
	}
	for _, field := range review.CanonicalFields {
		if _, err := db.Exec(`
			INSERT INTO review_field_defaults (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
		`, groupID, string(field), canonical.ID, review.CandidateValue(canonical, field), "2026-04-21T10:00:00Z"); err != nil {
			t.Fatalf("set review field default for %s: %v", field, err)
		}
	}

	return groupID
}

func mustCreateWebReviewGroupForImportRun(t *testing.T, st *sqlitestore.Store, path, title, notes string, candidateCount int) int64 {
	t.Helper()
	ensureImportRunFixtureForNotes(t, path, notes)

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

func mustCreateWebPublishableReviewGroupForImportRun(t *testing.T, st *sqlitestore.Store, path, title, notes string) int64 {
	t.Helper()
	ensureImportRunFixtureForNotes(t, path, notes)

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

func mustInsertAdminVenue(t *testing.T, db *sql.DB, venue domain.Venue) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO venues (slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, venue.Slug, venue.Name, venue.Address, venue.Neighbourhood, venue.Description, venue.Website, string(venue.ValidationState), string(venue.CoverageKind), venue.CoverageNote, string(venue.Origin)); err != nil {
		t.Fatalf("insert venue %q: %v", venue.Slug, err)
	}
}

func mustInsertAdminSource(t *testing.T, db *sql.DB, name, sourceURL string) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO sources (name, url)
		VALUES (?, ?)
	`, name, sourceURL)
	if err != nil {
		t.Fatalf("insert source %q: %v", name, err)
	}
	sourceID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("source last insert id: %v", err)
	}
	return sourceID
}

func mustInsertAdminEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name string, startAt, endAt time.Time, description string) {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id for %q: %v", venueSlug, err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, startAt.Format(time.RFC3339), endAt.Format(time.RFC3339), "Indie", "Listed", description, fixtureLocalTime(2026, time.April, 19, 9, 0).Format(time.RFC3339), string(domain.OriginLive)); err != nil {
		t.Fatalf("insert event %q: %v", slug, err)
	}
}

type readOnlyStoreStub struct{}

func (readOnlyStoreStub) ListVenues(context.Context) ([]domain.Venue, error) { return nil, nil }

func (readOnlyStoreStub) ListEvents(context.Context) ([]domain.Event, error) { return nil, nil }

func (readOnlyStoreStub) LoadVenueBySlug(context.Context, string) (domain.Venue, bool, error) {
	return domain.Venue{}, false, nil
}

func (readOnlyStoreStub) LoadEventBySlug(context.Context, string) (domain.Event, bool, error) {
	return domain.Event{}, false, nil
}

func (readOnlyStoreStub) ListEventsForVenue(context.Context, string) ([]domain.Event, error) {
	return nil, nil
}

func (readOnlyStoreStub) Validate(context.Context) error { return nil }

func (readOnlyStoreStub) Ready(context.Context) error { return nil }

type reviewOnlyStoreStub struct {
	readOnlyStoreStub
	group        review.Group
	closedGroups []review.GroupSummary
}

func (reviewOnlyStoreStub) ListOpenReviewGroups(context.Context) ([]review.GroupSummary, error) {
	return nil, nil
}

func (s reviewOnlyStoreStub) ListClosedReviewGroups(context.Context, int) ([]review.GroupSummary, error) {
	return s.closedGroups, nil
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

type provisionalVenueReadOnlyReviewStoreStub struct {
	reviewOnlyStoreStub
}

func (provisionalVenueReadOnlyReviewStoreStub) ListVenues(context.Context) ([]domain.Venue, error) {
	return []domain.Venue{readOnlyProvisionalVenueFixture()}, nil
}

func (provisionalVenueReadOnlyReviewStoreStub) LoadVenueBySlug(_ context.Context, slug string) (domain.Venue, bool, error) {
	if slug != "imaginary-hall" {
		return domain.Venue{}, false, nil
	}
	return readOnlyProvisionalVenueFixture(), true, nil
}

func (provisionalVenueReadOnlyReviewStoreStub) ListEventsForVenue(context.Context, string) ([]domain.Event, error) {
	return nil, nil
}

func readOnlyProvisionalVenueFixture() domain.Venue {
	return domain.Venue{
		Slug:            "imaginary-hall",
		Name:            "Imaginary Hall marketing copy",
		Address:         "1 Void Street, Sheffield",
		Neighbourhood:   "City Centre",
		Description:     "Pop-up room for test fixtures.",
		Website:         "https://example.test/imaginary-hall",
		ValidationState: domain.ValidationStateProvisional,
		CoverageKind:    domain.CoverageKindVenue,
		Origin:          domain.OriginLive,
	}
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

type replayOnlyStoreStub struct {
	readOnlyStoreStub
}

type failingReadyChecker struct{}

func (failingReadyChecker) Ready(context.Context) error {
	return fmt.Errorf("not ready")
}

type failingGenreConfigurationStore struct {
	saveErr      error
	deleteErr    error
	recomputeErr error
}

func (failingGenreConfigurationStore) ListGenreRules(context.Context) ([]genre.Rule, error) {
	return nil, nil
}

func (s failingGenreConfigurationStore) SaveGenreRule(context.Context, genre.RuleInput) error {
	return s.saveErr
}

func (s failingGenreConfigurationStore) DeleteGenreRule(context.Context, int64) error {
	return s.deleteErr
}

func (s failingGenreConfigurationStore) RecomputeEventGenres(context.Context) error {
	return s.recomputeErr
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

func (replayOnlyStoreStub) LoadImportRun(context.Context, int64) (ingest.ReplayRun, error) {
	return ingest.ReplayRun{
		ID:        1,
		StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:    "succeeded",
		Notes:     "fixture",
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

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 10, 0)
	})
	return server
}

func testServerDeps(value any) ServerDeps {
	deps := ServerDeps{
		Catalog:   store.NewSeedStore(),
		AdminAuth: AdminAuthConfig{Disabled: true},
	}
	if catalog, ok := value.(store.CatalogStore); ok {
		deps.Catalog = catalog
	}
	if reviewStore, ok := value.(ReviewStore); ok {
		deps.ReviewStore = reviewStore
	}
	if importRunStore, ok := value.(ingest.ImportRunStore); ok {
		deps.ImportRunStore = importRunStore
	}
	if replayStore, ok := value.(ingest.ReplayStore); ok {
		deps.ReplayStore = replayStore
	}
	if importRunReviewGroupStore, ok := value.(ImportRunReviewGroupStore); ok {
		deps.ImportRunReviewGroupStore = importRunReviewGroupStore
	}
	if secondarySourceStore, ok := value.(EventSecondarySourceInfoStore); ok {
		deps.EventSecondarySourceStore = secondarySourceStore
	}
	if eventGenreStore, ok := value.(EventGenreStore); ok {
		deps.EventGenreStore = eventGenreStore
	}
	if genreConfigStore, ok := value.(GenreConfigurationStore); ok {
		deps.GenreConfigurationStore = genreConfigStore
	}
	if readyChecker, ok := value.(ReadyChecker); ok {
		deps.ReadyChecker = readyChecker
	} else if readyChecker, ok := deps.Catalog.(ReadyChecker); ok {
		deps.ReadyChecker = readyChecker
	}
	return deps
}

func testAdminAuthDeps(value any) ServerDeps {
	deps := testServerDeps(value)
	deps.AdminAuth = AdminAuthConfig{
		PasswordHash: testAdminPasswordHash,
	}
	return deps
}

func loginAdmin(t *testing.T, server *Server, next string) (*http.Cookie, string) {
	t.Helper()

	form := url.Values{
		"password": {"correct horse battery staple"},
		"next":     {next},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	for _, cookie := range rr.Result().Cookies() {
		if cookie.Name == adminSessionCookieName {
			return cookie, rr.Header().Get("Location")
		}
	}
	t.Fatalf("login did not set %s cookie", adminSessionCookieName)
	return nil, ""
}

func extractCSRFToken(t *testing.T, body string) string {
	t.Helper()

	marker := `name="csrf_token" value="`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("body missing CSRF token: %q", body)
	}
	rest := body[start+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("body has unterminated CSRF token: %q", body)
	}
	return rest[:end]
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
