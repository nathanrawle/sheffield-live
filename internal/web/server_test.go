package web

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	pathpkg "path"
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
		{name: "home", path: "/", code: http.StatusOK, body: "Upcoming shows"},
		{name: "events", path: "/events", code: http.StatusOK, body: "Upcoming shows"},
		{name: "venues", path: "/venues", code: http.StatusOK, body: "Sheffield venues"},
		{name: "venue detail", path: "/venues/leadmill", code: http.StatusOK, body: "Leadmill"},
		{name: "static css", path: "/static/site.css", code: http.StatusOK, body: "color-scheme"},
		{name: "static js", path: "/static/site.js", code: http.StatusOK, body: "data-venue-timeline"},
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
	server, err := NewServer(testServerDeps(&adminReviewEventReviewStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:               41,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				DisplayTitle:     "Queue event",
				DisplayVenueSlug: "queue-venue",
			},
		},
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Review queue")
	assertContains(t, body, "Event review clusters")
	assertNotContains(t, body, "Latest successful import")
	assertNotContains(t, body, `href="/admin/import-runs"`)
}

func TestAdminLandingPageShowsEventReviewPrimaryCopy(t *testing.T) {
	server, err := NewServer(testServerDeps(&adminReviewEventReviewStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin")
	assertContains(t, body, "Review queue")
	assertContains(t, body, "Review event-review clusters, inspect ingest runs, and validate provisional venues and rooms.")
	assertContains(t, body, "Event review history")
	assertNotContains(t, body, "Resolve duplicate groups and accept or reject new listings.")
}

func TestAdminReviewRendersEventReviewSectionAndReadOnlyDetail(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	importRunID := int64(12)
	repairRunID := int64(34)
	stagingKey := "repair-queue-a"
	clusterID := int64(41)
	store := &adminReviewEventReviewStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:                 clusterID,
				Status:             store.EventReviewClusterStatusOpen,
				Version:            3,
				StagingKey:         &stagingKey,
				StagingKeyVersion:  3,
				ConflictType:       "historical_duplicate",
				ConflictReason:     "reason-a",
				CanonicalEventID:   int64Ptr(88),
				CanonicalEventSlug: "canonical-event",
				DisplayTitle:       "canonical-event",
				DisplayVenueSlug:   "event-review-hall",
				DisplayVenueName:   "Event Review Hall",
				DisplayStartAt:     &openTime,
				EvidenceCount:      1,
				UpdatedAt:          openTime,
				LatestImportRunID:  &importRunID,
				LatestRepairRunID:  &repairRunID,
			},
		},
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                 clusterID,
				Status:             store.EventReviewClusterStatusOpen,
				Version:            3,
				StagingKey:         &stagingKey,
				StagingKeyVersion:  3,
				ConflictType:       "historical_duplicate",
				ConflictReason:     "reason-a",
				CanonicalEventID:   int64Ptr(88),
				CanonicalEventSlug: "canonical-event",
				DisplayTitle:       "canonical-event",
				DisplayVenueSlug:   "event-review-hall",
				DisplayVenueName:   "Event Review Hall",
				DisplayStartAt:     &openTime,
				EvidenceCount:      1,
				UpdatedAt:          openTime,
				LatestImportRunID:  &importRunID,
				LatestRepairRunID:  &repairRunID,
			},
			Evidence: []store.EventReviewClusterEvidenceSummary{
				{
					ID:                  91,
					EvidenceID:          91,
					SourceID:            5,
					SourceName:          "Fixture ICS",
					SourceURL:           "https://example.test/fixture",
					EventSlug:           "evidence-event",
					EvidenceFingerprint: "fingerprint-1",
					Payload:             `{"payload":"evidence"}`,
					LinkedAt:            openTime,
					LinkReason:          "active evidence",
				},
			},
			ClusterIdentityKeys: []store.EventReviewClusterIdentityKeySummary{
				{
					ID:              111,
					IdentityKeyID:   222,
					IdentityKeyHash: "exact-hash",
					KeyKind:         store.EventReviewIdentityKeyKindExact,
					KeyVersion:      1,
					NormalizedKey:   "exact-normalized",
					LinkedAt:        openTime,
				},
				{
					ID:              112,
					IdentityKeyID:   223,
					IdentityKeyHash: "source-hash",
					KeyKind:         store.EventReviewIdentityKeyKindSource,
					KeyVersion:      1,
					NormalizedKey:   "source-normalized",
					LinkedAt:        openTime,
				},
			},
			EvidenceIdentityKeys: []store.EventReviewEvidenceIdentityKeySummary{
				{
					ID:                  121,
					EvidenceID:          91,
					EvidenceFingerprint: "fingerprint-1",
					IdentityKeyID:       222,
					IdentityKeyHash:     "exact-hash",
					KeyKind:             store.EventReviewIdentityKeyKindExact,
					KeyVersion:          1,
					NormalizedKey:       "exact-normalized",
					SourceID:            int64Ptr(5),
					Role:                store.EventReviewEvidenceIdentityKeyRoleExact,
				},
				{
					ID:                  122,
					EvidenceID:          91,
					EvidenceFingerprint: "fingerprint-1",
					IdentityKeyID:       223,
					IdentityKeyHash:     "source-hash",
					KeyKind:             store.EventReviewIdentityKeyKindSource,
					KeyVersion:          1,
					NormalizedKey:       "source-normalized",
					Role:                store.EventReviewEvidenceIdentityKeyRoleObserved,
				},
			},
			CanonicalChoices: []store.EventReviewClusterChoiceSummary{
				{
					ID:         101,
					FieldName:  "canonical_event_id",
					ChoiceKind: store.EventReviewChoiceKindEvent,
					EventID:    int64Ptr(88),
					EventSlug:  "action-event",
					Value:      "canonical",
					UpdatedAt:  openTime,
				},
			},
			DraftChoices: []store.EventReviewClusterChoiceSummary{
				{
					ID:                  102,
					FieldName:           "title",
					ChoiceKind:          store.EventReviewChoiceKindEvidence,
					EvidenceID:          int64Ptr(91),
					EvidenceFingerprint: "fingerprint-1",
					Value:               "draft title",
					UpdatedAt:           openTime,
				},
			},
			LiveActions: []store.EventReviewClusterLiveActionSummary{
				{
					ID:        103,
					EventID:   88,
					EventSlug: "action-event",
					Action:    store.EventReviewLiveActionKindWithholdDuplicate,
					Reason:    "withhold action",
					CreatedAt: openTime,
					UpdatedAt: openTime,
				},
			},
		},
	}

	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Event review clusters")
	assertContains(t, body, `href="/admin/event-review/history"`)
	assertContains(t, body, `href="/admin/event-review/41"`)
	assertContains(t, body, "canonical-event")
	assertContains(t, body, "Event Review Hall")
	assertContains(t, body, "15 May 2026")
	assertContains(t, body, "12:00")
	assertNotContains(t, body, "Cluster #41: repair-queue-a")

	detailBody := renderPath(t, server, "/admin/event-review/41")
	assertContains(t, detailBody, "Event review detail.")
	assertContains(t, detailBody, "This cluster is open.")
	assertContains(t, detailBody, `href="/admin/event-review/history"`)
	assertContains(t, detailBody, "Event summary")
	assertContains(t, detailBody, "canonical-event")
	assertContains(t, detailBody, "Event Review Hall")
	assertContains(t, detailBody, "15 May 2026")
	assertContains(t, detailBody, "12:00")
	assertContains(t, detailBody, "historical_duplicate")
	assertContains(t, detailBody, "Cluster identity keys")
	assertContains(t, detailBody, "exact-normalized")
	assertContains(t, detailBody, "source-normalized")
	assertContains(t, detailBody, "Evidence identity keys")
	assertContains(t, detailBody, "fingerprint-1")
	assertContains(t, detailBody, "source #5")
	assertContains(t, detailBody, "Stored/planned live actions")
	assertContains(t, detailBody, `name="csrf_token"`)
	assertContains(t, detailBody, `name="expected_version" value="3"`)
	assertContains(t, detailBody, `name="action" value="discard"`)
	assertContains(t, detailBody, `name="discard_reason"`)
	assertContains(t, detailBody, "Discard cluster")
	assertContains(t, detailBody, "Supersede cluster")
	assertContains(t, detailBody, `name="action" value="supersede"`)
	assertContains(t, detailBody, `name="superseded_by_cluster_id"`)
	assertContains(t, detailBody, "Apply stored live actions")
	assertContains(t, detailBody, `name="action" value="resolve_live_actions"`)
	assertContains(t, detailBody, "Fixture ICS")
	assertContains(t, detailBody, "evidence-event")
	assertContains(t, detailBody, "draft title")
	assertContains(t, detailBody, "withhold action")
	assertNotContains(t, detailBody, `name="action" value="resolve"`)
}

func TestAdminReviewRendersEventReviewTriageLabels(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	store := &adminReviewEventReviewStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:               41,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				ConflictType:     store.EventReviewConflictTypeImportReview,
				ConflictReason:   store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:    1,
				DisplayTitle:     "Import singleton",
				DisplayVenueSlug: "leadmill",
				DisplayVenueName: "Leadmill",
				DisplayStartAt:   &openTime,
			},
			{
				ID:               42,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				ConflictType:     store.EventReviewConflictTypeImportReview,
				ConflictReason:   store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:    2,
				DisplayTitle:     "Import comparison",
				DisplayVenueSlug: "leadmill",
				DisplayVenueName: "Leadmill",
				DisplayStartAt:   &openTime,
			},
			{
				ID:               43,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				ConflictType:     "title_repair",
				ConflictReason:   "supporting_clean_title",
				EvidenceCount:    1,
				DisplayTitle:     "Title repair",
				DisplayVenueSlug: "leadmill",
				DisplayVenueName: "Leadmill",
				DisplayStartAt:   &openTime,
			},
			{
				ID:               44,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				ConflictType:     "historical_duplicate",
				ConflictReason:   "reviewed_duplicate",
				EvidenceCount:    1,
				DisplayTitle:     "Historical duplicate",
				DisplayVenueSlug: "leadmill",
				DisplayVenueName: "Leadmill",
				DisplayStartAt:   &openTime,
			},
			{
				ID:               45,
				Status:           store.EventReviewClusterStatusOpen,
				Version:          1,
				ConflictType:     "mystery_type",
				ConflictReason:   "mystery_reason",
				EvidenceCount:    1,
				DisplayTitle:     "Unknown cluster",
				DisplayVenueSlug: "leadmill",
				DisplayVenueName: "Leadmill",
				DisplayStartAt:   &openTime,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/review")
	assertContains(t, body, "Import listing candidate")
	assertContains(t, body, "Review as a possible new event")
	assertContains(t, body, "Import candidate comparison")
	assertContains(t, body, "Compare normalized candidates")
	assertContains(t, body, "Title repair")
	assertContains(t, body, "Review clean title evidence")
	assertContains(t, body, "Historical duplicate")
	assertContains(t, body, "Review stored duplicate actions")
	assertContains(t, body, "Mystery Type")
	assertContains(t, body, "Mystery Reason")
}

func TestAdminReviewSuppressesEventReviewResolveFormWhenNotApplicable(t *testing.T) {
	stagingKey := "repair-queue-terminal"
	tests := []struct {
		name   string
		detail store.EventReviewClusterDetail
	}{
		{
			name: "title repair",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:                41,
					Status:            store.EventReviewClusterStatusOpen,
					Version:           3,
					StagingKey:        &stagingKey,
					StagingKeyVersion: 3,
					ConflictType:      "title_repair",
					ConflictReason:    "reason-a",
					CanonicalEventID:  int64Ptr(88),
					EvidenceCount:     1,
				},
				LiveActions: []store.EventReviewClusterLiveActionSummary{
					{ID: 1, EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
				},
			},
		},
		{
			name: "no live actions",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:                42,
					Status:            store.EventReviewClusterStatusOpen,
					Version:           3,
					StagingKey:        &stagingKey,
					StagingKeyVersion: 3,
					ConflictType:      "historical_duplicate",
					ConflictReason:    "reason-b",
					CanonicalEventID:  int64Ptr(88),
					EvidenceCount:     1,
				},
			},
		},
		{
			name: "no canonical",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:                43,
					Status:            store.EventReviewClusterStatusOpen,
					Version:           3,
					StagingKey:        &stagingKey,
					StagingKeyVersion: 3,
					ConflictType:      "historical_duplicate",
					ConflictReason:    "reason-c",
					EvidenceCount:     1,
				},
				LiveActions: []store.EventReviewClusterLiveActionSummary{
					{ID: 1, EventID: 90, EventSlug: "loser-event", Action: store.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold"},
				},
			},
		},
		{
			name: "terminal",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:                44,
					Status:            store.EventReviewClusterStatusResolved,
					Version:           3,
					StagingKey:        &stagingKey,
					StagingKeyVersion: 3,
					ConflictType:      "historical_duplicate",
					ConflictReason:    "reason-d",
					CanonicalEventID:  int64Ptr(88),
					EvidenceCount:     1,
				},
				LiveActions: []store.EventReviewClusterLiveActionSummary{
					{ID: 1, EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
					{ID: 2, EventID: 90, EventSlug: "loser-event", Action: store.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold"},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{detail: tc.detail}
			server, err := NewServer(testServerDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			body := renderPath(t, server, "/admin/event-review/"+strconv.FormatInt(tc.detail.Summary.ID, 10))
			assertNotContains(t, body, "Apply stored live actions")
			assertNotContains(t, body, `name="action" value="resolve_live_actions"`)
			assertContains(t, body, "No active cluster identity keys.")
			assertContains(t, body, "No active evidence identity keys.")
		})
	}
}

func TestAdminReviewRendersTitleRepairReadiness(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	clusterID := int64(52)
	stagingKey := "repair-queue-title"
	baseSummary := store.EventReviewClusterSummary{
		ID:                 clusterID,
		Status:             store.EventReviewClusterStatusOpen,
		Version:            3,
		StagingKey:         &stagingKey,
		StagingKeyVersion:  1,
		ConflictType:       "title_repair",
		ConflictReason:     "supporting_clean_title",
		CanonicalEventID:   int64Ptr(88),
		CanonicalEventSlug: "title-repair-current",
		DisplayTitle:       "Title Repair Current",
		DisplayVenueSlug:   "title-repair-hall",
		DisplayVenueName:   "Title Repair Hall",
		DisplayStartAt:     &openTime,
		EvidenceCount:      1,
		UpdatedAt:          openTime,
	}

	tests := []struct {
		name           string
		readiness      *store.EventReviewTitleRepairReadiness
		wantContains   []string
		wantNotContain []string
	}{
		{
			name: "eligible",
			readiness: &store.EventReviewTitleRepairReadiness{
				CanonicalEventID: 88,
				CurrentTitle:     "Legacy Event",
				CurrentSlug:      "title-repair-current",
				CurrentEventLive: true,
				DraftTitle:       "Proposed Title",
				DraftSlug:        "proposed-title",
				Eligible:         true,
			},
			wantContains: []string{
				"Title repair readiness",
				"This section shows whether the stored title repair is eligible to be applied.",
				"Current event",
				"Legacy Event",
				"event #88",
				"Current slug",
				"title-repair-current",
				"live/non-withheld: yes",
				"Proposed title",
				"Proposed Title",
				"Proposed slug",
				"proposed-title",
				"Ready to apply",
				"No blocking reasons.",
				"Apply title repair",
				`name="action" value="resolve_title_repair"`,
			},
			wantNotContain: []string{
				"Apply stored live actions",
				`name="action" value="resolve_live_actions"`,
			},
		},
		{
			name: "blocked",
			readiness: &store.EventReviewTitleRepairReadiness{
				CanonicalEventID: 88,
				CurrentTitle:     "Legacy Event",
				CurrentSlug:      "title-repair-current",
				CurrentEventLive: false,
				DraftTitle:       "",
				DraftSlug:        "",
				Eligible:         false,
				BlockingReasons: []string{
					"draft title is required",
					"draft slug is required",
				},
			},
			wantContains: []string{
				"Ready to apply",
				"draft title is required",
				"draft slug is required",
			},
			wantNotContain: []string{
				"Apply stored live actions",
				`name="action" value="resolve_live_actions"`,
				`name="action" value="resolve_title_repair"`,
			},
		},
		{
			name: "slug conflict",
			readiness: &store.EventReviewTitleRepairReadiness{
				CanonicalEventID:      88,
				CurrentTitle:          "Legacy Event",
				CurrentSlug:           "title-repair-current",
				CurrentEventLive:      true,
				DraftTitle:            "Conflict Title",
				DraftSlug:             "title-repair-slug-conflict",
				Eligible:              false,
				BlockingReasons:       []string{"target slug already belongs to another live event"},
				SlugConflictEventID:   int64Ptr(91),
				SlugConflictEventSlug: "title-repair-slug-conflict",
			},
			wantContains: []string{
				"target slug already belongs to another live event",
				`href="/admin/events/title-repair-slug-conflict"`,
				"event #91",
				"title-repair-slug-conflict",
			},
			wantNotContain: []string{
				"Apply stored live actions",
				`name="action" value="resolve_live_actions"`,
				`href="/events/title-repair-slug-conflict"`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary:              baseSummary,
					TitleRepairReadiness: tc.readiness,
				},
			}
			server, err := NewServer(testServerDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			body := renderPath(t, server, "/admin/event-review/52")
			for _, want := range tc.wantContains {
				assertContains(t, body, want)
			}
			for _, want := range tc.wantNotContain {
				assertNotContains(t, body, want)
			}
		})
	}
}

func TestAdminReviewOnlyEventReviewStoreRendersHistoryLinkAndQueue(t *testing.T) {
	historyTime := time.Date(2026, time.May, 15, 13, 0, 0, 0, time.UTC)
	stagingKey := "repair-queue-history"
	clusterID := int64(71)
	store := &eventReviewOnlyStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "type-a",
				ConflictReason:    "reason-a",
				DisplayTitle:      "queue-event",
				DisplayVenueSlug:  "queue-venue",
				DisplayVenueName:  "Queue Venue",
				DisplayStartAt:    &historyTime,
				EvidenceCount:     1,
				UpdatedAt:         historyTime,
			},
		},
		closedClusters: []store.EventReviewClusterHistorySummary{
			{
				ID:                    clusterID,
				Status:                store.EventReviewClusterStatusDiscarded,
				Version:               2,
				StagingKey:            &stagingKey,
				StagingKeyVersion:     1,
				ConflictType:          "type-h",
				ConflictReason:        "history-reason",
				CanonicalEventID:      int64Ptr(88),
				CanonicalEventSlug:    "history-event",
				DisplayTitle:          "history-event",
				DisplayVenueSlug:      "event-review-hall",
				DisplayVenueName:      "Event Review Hall",
				DisplayStartAt:        &historyTime,
				EvidenceCount:         2,
				UpdatedAt:             historyTime,
				SupersededByClusterID: int64Ptr(99),
				ResolutionID:          201,
				ResolutionCreatedAt:   historyTime,
				ResolvedAt:            historyTime,
				ResolutionStatus:      store.EventReviewResolutionStatusDiscarded,
				DiscardReason:         "discarded in admin",
			},
		},
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "type-a",
				ConflictReason:    "reason-a",
				DisplayTitle:      "queue-event",
				DisplayVenueSlug:  "queue-venue",
				DisplayVenueName:  "Queue Venue",
				DisplayStartAt:    &historyTime,
				EvidenceCount:     1,
				UpdatedAt:         historyTime,
			},
		},
	}

	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	queueBody := renderPath(t, server, "/admin/review")
	assertContains(t, queueBody, "Event review clusters")
	assertContains(t, queueBody, `href="/admin/event-review/history"`)
	assertContains(t, queueBody, `href="/admin/event-review/41"`)
	assertContains(t, queueBody, "queue-event")
	assertContains(t, queueBody, "Queue Venue")
	assertContains(t, queueBody, "14:00")

	historyBody := renderPath(t, server, "/admin/event-review/history")
	assertContains(t, historyBody, "Event review history")
	assertContains(t, historyBody, `href="/admin/event-review/71"`)
	assertContains(t, historyBody, "history-event")
	assertContains(t, historyBody, "Event Review Hall")
	assertContains(t, historyBody, "14:00")
	assertContains(t, historyBody, "resolution #201")
	assertContains(t, historyBody, "event #88")
	assertContains(t, historyBody, "superseded by cluster #99")
	assertContains(t, historyBody, "discarded in admin")
	assertContains(t, historyBody, "discarded")
	assertNotContains(t, historyBody, "Discard cluster")

	detailBody := renderPath(t, server, "/admin/event-review/41")
	assertContains(t, detailBody, `href="/admin/event-review/history"`)
}

func TestAdminReviewRendersTerminalEventReviewDetailWithoutDiscardForm(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	clusterID := int64(51)
	stagingKey := "repair-queue-terminal"
	store := &eventReviewOnlyStoreStub{
		readOnlyStoreStub: readOnlyStoreStub{},
		clusters: []store.EventReviewClusterSummary{
			{
				ID:                clusterID,
				Status:            store.EventReviewClusterStatusResolved,
				Version:           2,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 1,
				ConflictType:      "type-b",
				ConflictReason:    "reason-b",
				EvidenceCount:     0,
				UpdatedAt:         openTime,
			},
		},
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                clusterID,
				Status:            store.EventReviewClusterStatusResolved,
				Version:           2,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 1,
				ConflictType:      "type-b",
				ConflictReason:    "reason-b",
				EvidenceCount:     0,
				UpdatedAt:         openTime,
			},
			Resolution: &store.EventReviewResolutionSummary{
				ID:          101,
				ClusterID:   clusterID,
				Status:      store.EventReviewResolutionStatusResolved,
				CreatedAt:   openTime,
				UpdatedAt:   openTime,
				RepairRunID: int64Ptr(34),
				AppliedLiveActions: []store.EventReviewResolutionAppliedLiveActionSummary{
					{EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
				},
			},
		},
	}

	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/51")
	assertContains(t, body, "This cluster is resolved and read-only.")
	assertContains(t, body, `href="/admin/event-review/history"`)
	assertContains(t, body, "Resolution")
	assertContains(t, body, "Applied live actions")
	assertContains(t, body, "Stored/planned live actions")
	assertContains(t, body, "run #34")
	assertContains(t, body, "canonical-event")
	assertNotContains(t, body, "Discard cluster")
	assertNotContains(t, body, "Apply stored live actions")
	assertNotContains(t, body, `name="discard_reason"`)
	assertNotContains(t, body, `name="action" value="resolve_live_actions"`)
	assertContains(t, body, `href="/admin/review"`)
}

func TestAdminEventReviewDetailShowsAppliedTitleRepair(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                 61,
				Status:             store.EventReviewClusterStatusResolved,
				Version:            2,
				ConflictType:       "title_repair",
				ConflictReason:     "supporting_clean_title",
				CanonicalEventID:   int64Ptr(88),
				CanonicalEventSlug: "title-repair-event",
				DisplayTitle:       "Legacy Event",
				DisplayVenueSlug:   "title-repair-hall",
				DisplayVenueName:   "Title Repair Hall",
				DisplayStartAt:     &openTime,
				EvidenceCount:      0,
				UpdatedAt:          openTime,
			},
			Resolution: &store.EventReviewResolutionSummary{
				ID:        201,
				ClusterID: 61,
				Status:    store.EventReviewResolutionStatusResolved,
				AppliedTitleRepair: &store.EventReviewResolutionAppliedTitleRepairSummary{
					EventID:  88,
					OldTitle: "Legacy Event",
					NewTitle: "Updated Title",
					OldSlug:  "title-repair-event",
					NewSlug:  "title-repair-event-renamed",
				},
				CreatedAt: openTime,
				UpdatedAt: openTime,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/61")
	assertContains(t, body, "Resolution")
	assertContains(t, body, "Applied title repair")
	assertContains(t, body, "event #88")
	assertContains(t, body, "Legacy Event")
	assertContains(t, body, "Updated Title")
	assertContains(t, body, "title-repair-event-renamed")
	assertNotContains(t, body, "Apply title repair")
	assertNotContains(t, body, "Applied live actions")
}

func TestAdminEventReviewDetailShowsImportReviewReadiness(t *testing.T) {
	startAt := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
	endAt := time.Date(2026, time.May, 10, 21, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             71,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount:  1,
				NewListingScope: true,
				Candidates: []store.EventReviewImportCandidateSummary{{
					EvidenceID:          301,
					EvidenceFingerprint: "import-ready-fingerprint",
					SourceID:            41,
					SourceName:          "Fixture source",
					SourceURL:           "https://source.example.test/events",
					SourceAuthority:     store.SourceAuthoritySupporting,
					EventID:             int64Ptr(72),
					EventSlug:           "import-ready-existing-event",
					ExternalID:          "external-ready",
					Title:               "Import Ready Title",
					VenueSlug:           "leadmill",
					VenueText:           "Leadmill",
					StartAt:             &startAt,
					EndAt:               &endAt,
					CalendarURL:         "https://calendar.example.test/listing.ics",
				}},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/71")
	assertContains(t, body, "Import review readiness")
	assertContains(t, body, "Ready for new-listing resolver")
	assertContains(t, body, "yes")
	assertContains(t, body, "Fixture source")
	assertContains(t, body, "import-ready-fingerprint")
	assertContains(t, body, `href="/admin/events/import-ready-existing-event"`)
	assertNotContains(t, body, `href="/events/import-ready-existing-event"`)
	assertContains(t, body, "external-ready")
	assertContains(t, body, "Import Ready Title")
	assertContains(t, body, "leadmill")
	assertContains(t, body, "Leadmill")
	assertContains(t, body, "calendar.example.test/listing.ics")
	assertContains(t, body, "10 May 2026")
	assertContains(t, body, "Accept new listing")
	assertContains(t, body, `name="action" value="resolve_import_new_listing"`)
}

func TestAdminEventReviewDetailShowsImportReviewComparison(t *testing.T) {
	startAt := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
	startAtSecond := startAt.Add(30 * time.Minute)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             75,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        3,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount:           2,
				NewListingScope:          false,
				CandidateComparisonScope: true,
				Candidates: []store.EventReviewImportCandidateSummary{
					{
						EvidenceID:          401,
						EvidenceFingerprint: "import-comparison-fingerprint-a",
						SourceID:            51,
						SourceName:          "Fixture source",
						SourceURL:           "https://source.example.test/events",
						SourceAuthority:     store.SourceAuthoritySupporting,
						ExternalID:          "external-a",
						Title:               "Same Show - Leadmill",
						VenueText:           "Leadmill",
						StartAt:             &startAt,
						CalendarURL:         "https://calendar.example.test/listing-a.ics",
					},
					{
						EvidenceID:          402,
						EvidenceFingerprint: "import-comparison-fingerprint-b",
						SourceID:            51,
						SourceName:          "Fixture source",
						SourceURL:           "https://source.example.test/events",
						SourceAuthority:     store.SourceAuthoritySupporting,
						ExternalID:          "external-b",
						Title:               "Same Show",
						VenueSlug:           "leadmill",
						VenueText:           "Leadmill",
						StartAt:             &startAtSecond,
						CalendarURL:         "https://calendar.example.test/listing-b.ics",
					},
				},
				IdentityRows: []store.EventReviewImportIdentityRow{
					{
						FieldName: "clean_title",
						Label:     "Clean title",
						Consensus: true,
						Values: []store.EventReviewImportIdentityValue{
							{EvidenceID: 401, Normalized: "Same Show", Raw: "Same Show - Leadmill"},
							{EvidenceID: 402, Normalized: "Same Show", Raw: "Same Show"},
						},
					},
					{
						FieldName: "venue_slug",
						Label:     "Venue slug",
						Consensus: true,
						Values: []store.EventReviewImportIdentityValue{
							{EvidenceID: 401, Normalized: "leadmill", Raw: "Leadmill", Warning: "venue normalized from raw text"},
							{EvidenceID: 402, Normalized: "leadmill", Raw: "leadmill"},
						},
					},
					{
						FieldName: "date",
						Label:     "Date",
						Consensus: true,
						Values: []store.EventReviewImportIdentityValue{
							{EvidenceID: 401, Normalized: "2026-05-10", Raw: "2026-05-10T19:00:00Z"},
							{EvidenceID: 402, Normalized: "2026-05-10", Raw: "2026-05-10T19:30:00Z"},
						},
					},
					{
						FieldName: "start_time",
						Label:     "Start time",
						Consensus: false,
						Values: []store.EventReviewImportIdentityValue{
							{EvidenceID: 401, Normalized: "2026-05-10T19:00:00Z", Raw: "2026-05-10T19:00:00Z"},
							{EvidenceID: 402, Normalized: "2026-05-10T19:30:00Z", Raw: "2026-05-10T19:30:00Z"},
						},
					},
					{
						FieldName: "exact_identity",
						Label:     "Exact identity",
						Consensus: false,
						Values: []store.EventReviewImportIdentityValue{
							{EvidenceID: 401, Normalized: "exact-a", Raw: "venue=leadmill start=2026-05-10T19:00:00Z title=Same Show"},
							{EvidenceID: 402, Normalized: "exact-b", Raw: "venue=leadmill start=2026-05-10T19:30:00Z title=Same Show"},
						},
					},
				},
				RawRows: []store.EventReviewImportComparisonRow{
					{
						FieldName: "source",
						Label:     "Source",
						Consensus: true,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "Fixture source"},
							{EvidenceID: 402, Value: "Fixture source"},
						},
					},
					{
						FieldName: "source_url",
						Label:     "Source URL",
						Consensus: true,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "https://source.example.test/events"},
							{EvidenceID: 402, Value: "https://source.example.test/events"},
						},
					},
					{
						FieldName: "external_id",
						Label:     "External ID",
						Consensus: false,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "external-a"},
							{EvidenceID: 402, Value: "external-b"},
						},
					},
					{
						FieldName: "calendar_url",
						Label:     "Calendar URL",
						Consensus: false,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "https://calendar.example.test/listing-a.ics"},
							{EvidenceID: 402, Value: "https://calendar.example.test/listing-b.ics"},
						},
					},
					{
						FieldName: "raw_title",
						Label:     "Raw title",
						Consensus: false,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "Same Show - Leadmill"},
							{EvidenceID: 402, Value: "Same Show"},
						},
					},
					{
						FieldName: "raw_venue_text",
						Label:     "Raw venue text",
						Consensus: true,
						Values: []store.EventReviewImportComparisonValue{
							{EvidenceID: 401, Value: "Leadmill"},
							{EvidenceID: 402, Value: "Leadmill"},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/75")
	assertContains(t, body, "Normalized identity comparison")
	assertContains(t, body, "Comparison scope")
	assertContains(t, body, "yes")
	assertContains(t, body, "Same Show")
	assertContains(t, body, "venue normalized from raw text")
	assertContains(t, body, "Source values")
	assertContains(t, body, "external-a")
	assertContains(t, body, "external-b")
	assertContains(t, body, "same")
	assertContains(t, body, "differs")
	assertNotContains(t, body, "resolve_import_new_listing")
}

func TestAdminEventReviewDetailShowsSeparations(t *testing.T) {
	sepTime := time.Date(2026, time.May, 15, 11, 30, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             79,
				Status:         store.EventReviewClusterStatusResolved,
				Version:        4,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reviewed_duplicate",
				EvidenceCount:  1,
			},
			Separations: []store.EventReviewClusterSeparationSummary{
				{
					ID: 11,
					EndpointA: store.EventReviewSeparationEndpointSummary{
						Kind:               store.EventReviewSeparationEndpointKindEvent,
						Key:                "event:201",
						EventID:            int64Ptr(201),
						EventSlug:          "separation-event",
						CanonicalEventID:   int64Ptr(202),
						CanonicalEventSlug: "canonical-event",
					},
					EndpointB: store.EventReviewSeparationEndpointSummary{
						Kind:            store.EventReviewSeparationEndpointKindIdentityKey,
						Key:             "identity:cluster-hash",
						IdentityKeyID:   int64Ptr(901),
						IdentityKeyHash: "cluster-hash",
						IdentityKeyKind: store.EventReviewIdentityKeyKindExact,
						NormalizedKey:   "cluster-normalized",
					},
					Reason:    "duplicate evidence",
					CreatedAt: sepTime,
					UpdatedAt: sepTime,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/79")
	assertContains(t, body, "Separations")
	assertContains(t, body, "duplicate evidence")
	assertContains(t, body, "event #201")
	assertContains(t, body, "separation-event")
	assertContains(t, body, "canonical event #202")
	assertContains(t, body, "canonical-event")
	assertContains(t, body, "identity #901")
	assertContains(t, body, "cluster-normalized")
	assertNotContains(t, body, `name="action"`)
}

func TestAdminEventReviewDetailShowsSourceObservations(t *testing.T) {
	obsTime := time.Date(2026, time.May, 15, 11, 45, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             81,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			Observations: []store.EventReviewClusterObservationSummary{
				{
					ID:                        901,
					RunScope:                  "import:301",
					SourceID:                  42,
					SourceName:                "Store test source",
					SourceURL:                 "https://example.test/store-test",
					SourceIdentityKey:         "alpha-identity",
					SourceAuthority:           store.SourceAuthoritySupporting,
					FieldName:                 "title",
					IncomingRaw:               "Raw title",
					IncomingNormalized:        "Normalized title",
					CanonicalBeforeRaw:        "Canonical title",
					CanonicalBeforeNormalized: "Canonical normalized title",
					Outcome:                   "applied",
					UpdatedAt:                 obsTime,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/81")
	assertContains(t, body, "Source observations")
	assertContains(t, body, "Store test source")
	assertContains(t, body, "Source #42")
	assertContains(t, body, "alpha-identity")
	assertContains(t, body, "import:301")
	assertContains(t, body, "title")
	assertContains(t, body, "Raw title")
	assertContains(t, body, "Normalized title")
	assertContains(t, body, "Canonical title")
	assertContains(t, body, "Canonical normalized title")
	assertContains(t, body, "applied")
}

func TestAdminEventReviewDetailShowsSourceIdentityLinks(t *testing.T) {
	linkTime := time.Date(2026, time.May, 15, 11, 55, 0, 0, time.UTC)
	linkedEventID := int64(88)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             81,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			SourceIdentityLinks: []store.EventReviewClusterSourceIdentityLinkSummary{
				{
					SourceID:          42,
					SourceName:        "Store test source",
					SourceURL:         "https://example.test/store-test",
					SourceIdentityKey: "source-linked",
					EvidenceCount:     2,
					LinkedEventID:     &linkedEventID,
					LinkedEventSlug:   "source-link-linked-event",
					LinkedEventTitle:  "Legacy Event",
					Authoritative:     true,
					LinkUpdatedAt:     &linkTime,
				},
				{
					SourceID:          42,
					SourceName:        "Store test source",
					SourceURL:         "https://example.test/store-test",
					SourceIdentityKey: "source-unlinked",
					EvidenceCount:     1,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/81")
	assertContains(t, body, "Source identity links")
	assertContains(t, body, "source-linked")
	assertContains(t, body, "source-unlinked")
	assertContains(t, body, "Store test source")
	assertContains(t, body, "Source #42")
	assertContains(t, body, "2 evidence rows")
	assertContains(t, body, "event #88")
	assertContains(t, body, `href="/admin/events/source-link-linked-event"`)
	assertNotContains(t, body, `href="/events/source-link-linked-event"`)
	assertContains(t, body, "Legacy Event")
	assertContains(t, body, "authoritative")
	assertContains(t, body, "unlinked")
}

func TestAdminEventReviewDetailShowsExactIdentityMatches(t *testing.T) {
	matchTime := time.Date(2026, time.May, 15, 12, 5, 0, 0, time.UTC)
	linkedEventID := int64(177)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             83,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			ExactIdentityMatches: []store.EventReviewClusterExactIdentityMatchSummary{
				{
					IdentityKeyID:        901,
					IdentityKeyHash:      "exact-hash-linked",
					KeyVersion:           1,
					NormalizedKey:        "exact-linked-key",
					EvidenceCount:        2,
					LinkedEventID:        &linkedEventID,
					LinkedEventSlug:      "exact-linked-event",
					LinkedEventTitle:     "Exact Linked Event",
					LinkedEventVenueSlug: "exact-linked-venue",
					LinkedEventStartAt:   &matchTime,
				},
				{
					IdentityKeyID:   902,
					IdentityKeyHash: "exact-hash-unlinked",
					KeyVersion:      1,
					NormalizedKey:   "exact-unlinked-key",
					EvidenceCount:   0,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/83")
	assertContains(t, body, "Exact identity matches")
	assertContains(t, body, "exact-linked-key")
	assertContains(t, body, "2 evidence rows")
	assertContains(t, body, "event #177")
	assertContains(t, body, `href="/admin/events/exact-linked-event"`)
	assertNotContains(t, body, `href="/events/exact-linked-event"`)
	assertContains(t, body, "Exact Linked Event")
	assertContains(t, body, "exact-linked-venue")
	assertContains(t, body, "13:05")
	assertContains(t, body, "exact-unlinked-key")
	assertContains(t, body, "No live match")
}

func TestAdminEventReviewDetailShowsEmptySourceIdentityLinksState(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             82,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reviewed_duplicate",
				EvidenceCount:  1,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/82")
	assertContains(t, body, "No source identity links recorded for this cluster.")
}

func TestAdminEventReviewDetailShowsEmptyExactIdentityMatchesState(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             84,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reviewed_duplicate",
				EvidenceCount:  1,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/84")
	assertContains(t, body, "No exact identity keys recorded for this cluster.")
}

func TestAdminEventReviewDetailShowsEmptySeparationsState(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             80,
				Status:         store.EventReviewClusterStatusResolved,
				Version:        2,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reviewed_duplicate",
				EvidenceCount:  0,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/80")
	assertContains(t, body, "No active separations related to this cluster.")
}

func TestAdminEventReviewDetailShowsEmptySourceObservationsState(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             82,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reviewed_duplicate",
				EvidenceCount:  1,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/82")
	assertContains(t, body, "No source observations recorded for this cluster.")
}

func TestAdminEventReviewDetailShowsBlockedImportReviewReadiness(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             72,
				Status:         store.EventReviewClusterStatusResolved,
				Version:        2,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
				UpdatedAt:      openTime,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount:  2,
				NewListingScope: false,
				BlockingReasons: []string{
					"cluster is not open",
					"multiple active evidence rows are present",
					"payload could not be parsed",
				},
				PayloadWarnings: []string{"evidence #401: invalid character 'b' looking for beginning of object key string"},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/72")
	assertContains(t, body, "Import review readiness")
	assertContains(t, body, "cluster is not open")
	assertContains(t, body, "multiple active evidence rows are present")
	assertContains(t, body, "payload could not be parsed")
	assertContains(t, body, "looking for beginning of object key string")
	assertNotContains(t, body, "Accept new listing")
	assertNotContains(t, body, `name="action" value="resolve_import_new_listing"`)
}

func TestAdminEventReviewDetailShowsCandidateIdentityStatuses(t *testing.T) {
	startAt := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             75,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        1,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 2,
				CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
					{
						EvidenceID:          401,
						EvidenceFingerprint: "import-candidate-fingerprint-a",
						SourceID:            11,
						SourceName:          "Source A",
						Title:               "Alpha Candidate",
						VenueSlug:           "leadmill",
						StartAt:             &startAt,
						ExactKeys: []store.EventReviewImportCandidateExactIdentityStatus{
							{
								NormalizedKey:    "exact-alpha",
								IdentityKeyHash:  "exact-alpha-hash",
								LinkedEventID:    int64Ptr(501),
								LinkedEventSlug:  "alpha-linked-event",
								LinkedEventTitle: "Alpha Linked Event",
							},
						},
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          11,
								SourceName:        "Source A",
								SourceIdentityKey: "source-alpha",
								LinkedEventID:     int64Ptr(601),
								LinkedEventSlug:   "source-linked-event",
								LinkedEventTitle:  "Source Linked Event",
								Authoritative:     true,
							},
							{
								SourceID:          13,
								SourceName:        "Source C",
								SourceIdentityKey: "source-charlie",
								LinkedEventID:     int64Ptr(602),
								LinkedEventSlug:   "source-supporting-event",
								LinkedEventTitle:  "Source Supporting Event",
							},
						},
					},
					{
						EvidenceID:          402,
						EvidenceFingerprint: "import-candidate-fingerprint-b",
						SourceID:            12,
						SourceName:          "Source B",
						ParseWarning:        "payload could not be parsed",
						ExactKeys: []store.EventReviewImportCandidateExactIdentityStatus{
							{
								NormalizedKey:   "exact-bravo",
								IdentityKeyHash: "exact-bravo-hash",
							},
						},
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          12,
								SourceName:        "Source B",
								SourceIdentityKey: "source-bravo",
							},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/75")
	assertContains(t, body, "Candidate identity status")
	assertContains(t, body, "Alpha Candidate")
	assertContains(t, body, "Alpha Linked Event")
	assertContains(t, body, "event #501")
	assertContains(t, body, `href="/admin/events/alpha-linked-event"`)
	assertNotContains(t, body, `href="/events/alpha-linked-event"`)
	assertContains(t, body, "Source Linked Event")
	assertContains(t, body, "event #601")
	assertContains(t, body, `href="/admin/events/source-linked-event"`)
	assertContains(t, body, "Source Supporting Event")
	assertContains(t, body, "event #602")
	assertContains(t, body, `href="/admin/events/source-supporting-event"`)
	assertContains(t, body, "Linked to event #501")
	assertContains(t, body, "Linked to event #601")
	assertContains(t, body, "Linked to event #602")
	assertContains(t, body, "No live match")
	assertContains(t, body, "No source link")
	assertContains(t, body, "authoritative")
	assertContains(t, body, "supporting")
	assertContains(t, body, "payload could not be parsed")
}

func TestAdminEventReviewDetailShowsSelectedCandidateReadiness(t *testing.T) {
	readyStartAt := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
	readyUpdatedAt := fixtureLocalTime(2026, time.May, 15, 11, 5)
	blockedStartAt := time.Date(2026, time.May, 10, 20, 0, 0, 0, time.UTC)
	blockedUpdatedAt := fixtureLocalTime(2026, time.May, 15, 11, 10)

	tests := []struct {
		name           string
		selectedReady  *store.EventReviewImportSelectedCandidateReadiness
		wantContains   []string
		wantNotContain []string
	}{
		{
			name: "ready",
			selectedReady: &store.EventReviewImportSelectedCandidateReadiness{
				Eligible:            true,
				EvidenceID:          701,
				EvidenceFingerprint: "selected-ready-fingerprint",
				Title:               "Eligible Selected Candidate",
				VenueSlug:           "event-review-selected-ready",
				VenueText:           "Event Review Selected Ready",
				StartAt:             &readyStartAt,
				SelectedSourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
					{
						SourceID:          11,
						SourceName:        "Source A",
						SourceIdentityKey: "selected-ready",
						ChoiceSelected:    true,
						ChoiceReason:      "selected ready",
						ChoiceUpdatedAt:   &readyUpdatedAt,
					},
				},
				ExactKeys: []store.EventReviewImportCandidateExactIdentityStatus{
					{
						NormalizedKey:   "selected-ready-exact",
						IdentityKeyHash: "selected-ready-exact-hash",
					},
				},
				SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
					{
						SourceID:          11,
						SourceName:        "Source A",
						SourceIdentityKey: "selected-ready",
						ChoiceSelected:    true,
						ChoiceReason:      "selected ready",
						ChoiceUpdatedAt:   &readyUpdatedAt,
					},
				},
			},
			wantContains: []string{
				"Selected candidate readiness",
				"Ready to accept selected candidate",
				"yes",
				"Eligible Selected Candidate",
				"event-review-selected-ready",
				"selected-ready",
				"No live match",
				"No source link",
			},
		},
		{
			name: "blocked",
			selectedReady: &store.EventReviewImportSelectedCandidateReadiness{
				Eligible:            false,
				BlockingReasons:     []string{"selected source identity choices span multiple candidates", "selected candidate source identity already links to live event"},
				EvidenceID:          702,
				EvidenceFingerprint: "selected-blocked-fingerprint",
				Title:               "Blocked Selected Candidate",
				VenueSlug:           "event-review-selected-blocked",
				VenueText:           "Event Review Selected Blocked",
				StartAt:             &blockedStartAt,
				SelectedSourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
					{
						SourceID:          12,
						SourceName:        "Source B",
						SourceIdentityKey: "selected-blocked",
						LinkedEventID:     int64Ptr(601),
						LinkedEventSlug:   "selected-blocked-link",
						LinkedEventTitle:  "Selected Blocked Link",
						ChoiceSelected:    true,
						ChoiceReason:      "selected blocked",
						ChoiceUpdatedAt:   &blockedUpdatedAt,
					},
				},
				ExactKeys: []store.EventReviewImportCandidateExactIdentityStatus{
					{
						NormalizedKey:    "selected-blocked-exact",
						IdentityKeyHash:  "selected-blocked-exact-hash",
						LinkedEventID:    int64Ptr(501),
						LinkedEventSlug:  "selected-blocked-exact-event",
						LinkedEventTitle: "Selected Blocked Exact",
					},
				},
				SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
					{
						SourceID:          12,
						SourceName:        "Source B",
						SourceIdentityKey: "selected-blocked",
						LinkedEventID:     int64Ptr(601),
						LinkedEventSlug:   "selected-blocked-link",
						LinkedEventTitle:  "Selected Blocked Link",
						Authoritative:     true,
						ChoiceSelected:    true,
						ChoiceReason:      "selected blocked",
						ChoiceUpdatedAt:   &blockedUpdatedAt,
					},
				},
			},
			wantContains: []string{
				"Selected candidate readiness",
				"Ready to accept selected candidate",
				"no",
				"selected source identity choices span multiple candidates",
				"selected candidate source identity already links to live event",
				"Linked to event #501",
				"Linked to event #601",
				"authoritative",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{
						ID:             76,
						Status:         store.EventReviewClusterStatusOpen,
						Version:        1,
						ConflictType:   store.EventReviewConflictTypeImportReview,
						ConflictReason: store.EventReviewConflictReasonIngestCandidate,
						EvidenceCount:  1,
					},
					ImportReadiness: &store.EventReviewImportReadiness{
						CandidateCount:             1,
						SelectedCandidateReadiness: tc.selectedReady,
					},
				},
			}
			server, err := NewServer(testServerDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}

			body := renderPath(t, server, "/admin/event-review/76")
			for _, want := range tc.wantContains {
				assertContains(t, body, want)
			}
			for _, want := range tc.wantNotContain {
				assertNotContains(t, body, want)
			}
		})
	}
}

func TestAdminEventReviewDetailShowsSelectedCandidateAcceptForm(t *testing.T) {
	selectedStartAt := time.Date(2026, time.May, 12, 19, 30, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             91,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        6,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 2,
				SelectedCandidateReadiness: &store.EventReviewImportSelectedCandidateReadiness{
					Eligible:            true,
					EvidenceID:          801,
					EvidenceFingerprint: "selected-eligible-fingerprint",
					Title:               "Selected Candidate Title",
					VenueSlug:           "selected-candidate-venue",
					VenueText:           "Selected Candidate Venue",
					StartAt:             &selectedStartAt,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/91")
	assertContains(t, body, "Accept selected candidate")
	assertContains(t, body, `name="action" value="resolve_import_selected_candidate"`)
	assertNotContains(t, body, `name="action" value="resolve_import_new_listing"`)
}

func TestAdminEventReviewDetailAcceptsSelectedImportCandidate(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             92,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        6,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  2,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 2,
				SelectedCandidateReadiness: &store.EventReviewImportSelectedCandidateReadiness{
					Eligible:   true,
					EvidenceID: 802,
				},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/92", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "6")
	form.Set("action", "resolve_import_selected_candidate")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/92", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_resolved=1" {
		t.Fatalf("Location = %q, want resolve redirect", location)
	}
	if !store.resolveCalled {
		t.Fatal("expected resolve store method to be called")
	}
	if store.resolveInput.ClusterID != 92 || store.resolveInput.ExpectedVersion != 6 {
		t.Fatalf("resolve input = %#v", store.resolveInput)
	}
}

func TestAdminEventReviewDetailRejectsIneligibleSelectedImportCandidateResolution(t *testing.T) {
	tests := []struct {
		name          string
		selectedReady *store.EventReviewImportSelectedCandidateReadiness
	}{
		{
			name: "blocked readiness",
			selectedReady: &store.EventReviewImportSelectedCandidateReadiness{
				Eligible:        false,
				BlockingReasons: []string{"selected candidate does not satisfy the resolver criteria"},
				EvidenceID:      803,
			},
		},
		{
			name:          "missing readiness",
			selectedReady: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{
						ID:             93,
						Status:         store.EventReviewClusterStatusOpen,
						Version:        6,
						ConflictType:   store.EventReviewConflictTypeImportReview,
						ConflictReason: store.EventReviewConflictReasonIngestCandidate,
						EvidenceCount:  2,
					},
					ImportReadiness: &store.EventReviewImportReadiness{
						CandidateCount:             2,
						SelectedCandidateReadiness: tc.selectedReady,
					},
				},
			}
			server, err := NewServer(testAdminAuthDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			cookie, _ := loginAdmin(t, server, "/admin/review")

			getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/93", nil)
			getReq.AddCookie(cookie)
			getRR := httptest.NewRecorder()
			server.ServeHTTP(getRR, getReq)
			if getRR.Code != http.StatusOK {
				t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
			}
			body := getRR.Body.String()
			assertNotContains(t, body, "Accept selected candidate")
			assertNotContains(t, body, `name="action" value="resolve_import_selected_candidate"`)
			csrfToken := extractCSRFToken(t, getRR.Body.String())

			form := url.Values{}
			form.Set("csrf_token", csrfToken)
			form.Set("expected_version", "6")
			form.Set("action", "resolve_import_selected_candidate")
			req := httptest.NewRequest(http.MethodPost, "/admin/event-review/93", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			assertContains(t, rr.Body.String(), "event review cluster is not eligible for selected import candidate resolution")
			if store.resolveCalled {
				t.Fatal("expected resolve store method not to be called")
			}
		})
	}
}

func TestAdminEventReviewDetailShowsSourceIdentityChoiceSaveForm(t *testing.T) {
	choiceUpdatedAt := fixtureLocalTime(2026, time.May, 15, 10, 55)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             86,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        7,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 1,
				CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
					{
						EvidenceID:          501,
						EvidenceFingerprint: "source-choice-fingerprint",
						SourceID:            11,
						SourceName:          "Source A",
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          11,
								SourceName:        "Source A",
								SourceIdentityKey: "source-alpha",
								LinkedEventID:     int64Ptr(601),
								LinkedEventSlug:   "source-alpha-event",
								LinkedEventTitle:  "Alpha Source Event",
								Authoritative:     true,
								ChoiceSelected:    true,
								ChoiceReason:      "admin source identity choice",
								ChoiceUpdatedAt:   &choiceUpdatedAt,
							},
							{
								SourceID:          12,
								SourceName:        "Source B",
								SourceIdentityKey: "source-bravo",
								ChoiceSelected:    false,
								ChoiceReason:      "admin source identity choice cleared",
								ChoiceUpdatedAt:   &choiceUpdatedAt,
							},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/86")
	assertContains(t, body, "Save source identity choices")
	assertContains(t, body, `name="action" value="save_source_identity_choices"`)
	assertContains(t, body, `name="source_identity_choice" value="11|source-alpha" checked`)
	assertContains(t, body, `name="source_identity_choice" value="12|source-bravo"`)
	assertContains(t, body, "admin source identity choice")
	assertContains(t, body, "admin source identity choice cleared")
	assertContains(t, body, "15 May 2026 10:55")
	assertContains(t, body, "Alpha Source Event")
	assertContains(t, body, `href="/admin/events/source-alpha-event"`)
	assertNotContains(t, body, `href="/events/source-alpha-event"`)
}

func TestAdminEventReviewDetailHidesSourceIdentityChoiceSaveFormForTerminalOrNonImportClusters(t *testing.T) {
	tests := []struct {
		name   string
		detail store.EventReviewClusterDetail
	}{
		{
			name: "terminal cluster",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:             87,
					Status:         store.EventReviewClusterStatusResolved,
					Version:        4,
					ConflictType:   store.EventReviewConflictTypeImportReview,
					ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				},
			},
		},
		{
			name: "non-import cluster",
			detail: store.EventReviewClusterDetail{
				Summary: store.EventReviewClusterSummary{
					ID:             88,
					Status:         store.EventReviewClusterStatusOpen,
					Version:        4,
					ConflictType:   "historical_duplicate",
					ConflictReason: "reason-a",
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{detail: tc.detail}
			server, err := NewServer(testServerDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}

			body := renderPath(t, server, "/admin/event-review/"+strconvFormatInt(tc.detail.Summary.ID))
			assertNotContains(t, body, "Save source identity choices")
			assertNotContains(t, body, `name="action" value="save_source_identity_choices"`)
		})
	}
}

func TestAdminEventReviewDetailAcceptsImportReviewNewListing(t *testing.T) {
	startAt := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
	endAt := time.Date(2026, time.May, 10, 21, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             73,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        4,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount:  1,
				NewListingScope: true,
				Candidates: []store.EventReviewImportCandidateSummary{{
					EvidenceID:          302,
					EvidenceFingerprint: "import-accept-fingerprint",
					SourceID:            42,
					SourceName:          "Fixture source",
					SourceURL:           "https://source.example.test/events",
					SourceAuthority:     store.SourceAuthoritySupporting,
					ExternalID:          "external-accept",
					Title:               "Accept Me Title",
					VenueSlug:           "leadmill",
					VenueText:           "Leadmill",
					StartAt:             &startAt,
					EndAt:               &endAt,
					CalendarURL:         "https://calendar.example.test/listing.ics",
				}},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/73", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "4")
	form.Set("action", "resolve_import_new_listing")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/73", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_resolved=1" {
		t.Fatalf("Location = %q, want resolve redirect", location)
	}
	if !store.resolveCalled {
		t.Fatal("expected resolve store method to be called")
	}
	if store.resolveInput.ClusterID != 73 || store.resolveInput.ExpectedVersion != 4 {
		t.Fatalf("resolve input = %#v", store.resolveInput)
	}
}

func TestAdminEventReviewDetailSavesSourceIdentityChoicesAndRedirects(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             89,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        5,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 1,
				CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
					{
						EvidenceID:          701,
						EvidenceFingerprint: "source-choice-post-fingerprint",
						SourceID:            21,
						SourceName:          "Source Alpha",
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          21,
								SourceName:        "Source Alpha",
								SourceIdentityKey: "source-alpha",
								ChoiceSelected:    false,
							},
							{
								SourceID:          22,
								SourceName:        "Source Bravo",
								SourceIdentityKey: "source-bravo",
								ChoiceSelected:    false,
							},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/89", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "5")
	form.Set("action", "save_source_identity_choices")
	form.Add("source_identity_choice", "21|source-alpha")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/89", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/event-review/89?source_identity_choices_saved=1" {
		t.Fatalf("Location = %q, want detail redirect", location)
	}
	if !store.sourceIdentityChoicesCalled {
		t.Fatal("expected source identity choice store method to be called")
	}
	if store.sourceIdentityChoicesInput.ClusterID != 89 || store.sourceIdentityChoicesInput.ExpectedVersion != 5 {
		t.Fatalf("source identity choices input = %#v", store.sourceIdentityChoicesInput)
	}
	if got, want := len(store.sourceIdentityChoicesInput.Choices), 2; got != want {
		t.Fatalf("source identity choices count = %d, want %d", got, want)
	}
	if got := store.sourceIdentityChoicesInput.Choices[0]; got.SourceID != 21 || got.SourceIdentityKey != "source-alpha" || !got.Selected || got.SelectionReason != "admin source identity choice" {
		t.Fatalf("first source identity choice = %#v", got)
	}
	if got := store.sourceIdentityChoicesInput.Choices[1]; got.SourceID != 22 || got.SourceIdentityKey != "source-bravo" || got.Selected || got.SelectionReason != "admin source identity choice cleared" {
		t.Fatalf("second source identity choice = %#v", got)
	}

	redirectReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/89?source_identity_choices_saved=1", nil)
	redirectReq.AddCookie(cookie)
	redirectRR := httptest.NewRecorder()
	server.ServeHTTP(redirectRR, redirectReq)
	if redirectRR.Code != http.StatusOK {
		t.Fatalf("redirect detail status = %d, want %d", redirectRR.Code, http.StatusOK)
	}
	redirectBody := redirectRR.Body.String()
	assertContains(t, redirectBody, "Source identity choices saved.")
	assertContains(t, redirectBody, `name="source_identity_choice" value="21|source-alpha" checked`)
	assertContains(t, redirectBody, `name="source_identity_choice" value="22|source-bravo"`)
	assertContains(t, redirectBody, "15 May 2026 10:55")
	if store.detail.Summary.Version != 6 {
		t.Fatalf("detail version = %d, want 6", store.detail.Summary.Version)
	}
	if !store.detail.ImportReadiness.CandidateIdentityStatuses[0].SourceKeys[0].ChoiceSelected {
		t.Fatal("expected first source identity choice to be updated")
	}
	if store.detail.ImportReadiness.CandidateIdentityStatuses[0].SourceKeys[1].ChoiceSelected {
		t.Fatal("expected second source identity choice to remain cleared")
	}
}

func TestAdminEventReviewSaveSourceIdentityChoicesIgnoresQueryStringSelections(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             90,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        6,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:  1,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 1,
				CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
					{
						EvidenceID:          702,
						EvidenceFingerprint: "source-choice-query-fingerprint",
						SourceID:            31,
						SourceName:          "Source Alpha",
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          31,
								SourceName:        "Source Alpha",
								SourceIdentityKey: "source-alpha",
							},
							{
								SourceID:          32,
								SourceName:        "Source Bravo",
								SourceIdentityKey: "source-bravo",
							},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/90", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "6")
	form.Set("action", "save_source_identity_choices")
	form.Add("source_identity_choice", "31|source-alpha")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/90?source_identity_choice=32%7Csource-bravo", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/event-review/90?source_identity_choices_saved=1" {
		t.Fatalf("Location = %q, want detail redirect", location)
	}
	if !store.sourceIdentityChoicesCalled {
		t.Fatal("expected source identity choice store method to be called")
	}
	if got, want := len(store.sourceIdentityChoicesInput.Choices), 2; got != want {
		t.Fatalf("source identity choices count = %d, want %d", got, want)
	}
	if got := store.sourceIdentityChoicesInput.Choices[0]; got.SourceID != 31 || got.SourceIdentityKey != "source-alpha" || !got.Selected {
		t.Fatalf("first source identity choice = %#v", got)
	}
	if got := store.sourceIdentityChoicesInput.Choices[1]; got.SourceID != 32 || got.SourceIdentityKey != "source-bravo" || got.Selected {
		t.Fatalf("second source identity choice = %#v, want query-string value ignored", got)
	}
}

func TestAdminEventReviewSaveSourceIdentityChoicesRejectsInvalidFormsAndStoreErrors(t *testing.T) {
	tests := []struct {
		name     string
		form     url.Values
		err      error
		want     string
		wantCode int
	}{
		{
			name:     "invalid version",
			form:     url.Values{"expected_version": {"not-a-number"}, "action": {"save_source_identity_choices"}, "source_identity_choice": {"21|source-alpha"}},
			want:     "expected version is required",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "stale version",
			form:     url.Values{"expected_version": {"5"}, "action": {"save_source_identity_choices"}, "source_identity_choice": {"21|source-alpha"}},
			err:      errors.New("event review cluster 89 update was rejected"),
			want:     "event review cluster 89 update was rejected",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "store error",
			form:     url.Values{"expected_version": {"5"}, "action": {"save_source_identity_choices"}, "source_identity_choice": {"21|source-alpha"}},
			err:      errors.New("database unavailable"),
			want:     "database unavailable",
			wantCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{
						ID:             89,
						Status:         store.EventReviewClusterStatusOpen,
						Version:        5,
						ConflictType:   store.EventReviewConflictTypeImportReview,
						ConflictReason: store.EventReviewConflictReasonIngestCandidate,
					},
					ImportReadiness: &store.EventReviewImportReadiness{
						CandidateCount: 1,
						CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
							{
								EvidenceID:          701,
								EvidenceFingerprint: "source-choice-post-fingerprint",
								SourceID:            21,
								SourceName:          "Source Alpha",
								SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
									{
										SourceID:          21,
										SourceName:        "Source Alpha",
										SourceIdentityKey: "source-alpha",
									},
								},
							},
						},
					},
				},
				sourceIdentityChoicesErr: tc.err,
			}
			server, err := NewServer(testAdminAuthDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			cookie, _ := loginAdmin(t, server, "/admin/review")

			getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/89", nil)
			getReq.AddCookie(cookie)
			getRR := httptest.NewRecorder()
			server.ServeHTTP(getRR, getReq)
			if getRR.Code != http.StatusOK {
				t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
			}
			csrfToken := extractCSRFToken(t, getRR.Body.String())

			if tc.form == nil {
				tc.form = url.Values{}
			}
			if tc.form.Get("expected_version") == "" {
				tc.form.Set("expected_version", "5")
			}
			tc.form.Set("csrf_token", csrfToken)

			req := httptest.NewRequest(http.MethodPost, "/admin/event-review/89", strings.NewReader(tc.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, tc.wantCode, rr.Body.String())
			}
			assertContains(t, rr.Body.String(), tc.want)
			if tc.err == nil && store.sourceIdentityChoicesCalled {
				t.Fatal("expected source identity choice store method to not be called")
			}
			if tc.err != nil && !store.sourceIdentityChoicesCalled {
				t.Fatal("expected source identity choice store method to be called")
			}
		})
	}
}

func TestAdminEventReviewSaveSourceIdentityChoicesRejectsInvalidCSRF(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             89,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        5,
				ConflictType:   store.EventReviewConflictTypeImportReview,
				ConflictReason: store.EventReviewConflictReasonIngestCandidate,
			},
			ImportReadiness: &store.EventReviewImportReadiness{
				CandidateCount: 1,
				CandidateIdentityStatuses: []store.EventReviewImportCandidateIdentityStatus{
					{
						EvidenceID:          701,
						EvidenceFingerprint: "source-choice-post-fingerprint",
						SourceID:            21,
						SourceName:          "Source Alpha",
						SourceKeys: []store.EventReviewImportCandidateSourceIdentityStatus{
							{
								SourceID:          21,
								SourceName:        "Source Alpha",
								SourceIdentityKey: "source-alpha",
							},
						},
					},
				},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	form := url.Values{
		"csrf_token":             {"wrong"},
		"expected_version":       {"5"},
		"action":                 {"save_source_identity_choices"},
		"source_identity_choice": {"21|source-alpha"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/89", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "invalid CSRF token")
	if store.sourceIdentityChoicesCalled {
		t.Fatal("source identity choice store method should not be called")
	}
}

func TestAdminEventReviewDetailShowsAppliedImportListingResolution(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                 74,
				Status:             store.EventReviewClusterStatusResolved,
				Version:            2,
				ConflictType:       store.EventReviewConflictTypeImportReview,
				ConflictReason:     store.EventReviewConflictReasonIngestCandidate,
				EvidenceCount:      1,
				UpdatedAt:          openTime,
				CanonicalEventID:   int64Ptr(901),
				CanonicalEventSlug: "accept-new-listing-leadmill-20260522190000",
			},
			Resolution: &store.EventReviewResolutionSummary{
				ID:        301,
				ClusterID: 74,
				Status:    store.EventReviewResolutionStatusResolved,
				CreatedAt: openTime,
				UpdatedAt: openTime,
				AppliedImportListing: &store.EventReviewResolutionAppliedImportListingSummary{
					EventID:    901,
					EventSlug:  "accept-new-listing-leadmill-20260522190000",
					Title:      "Accept New Listing",
					VenueSlug:  "leadmill",
					VenueName:  "Leadmill",
					StartAt:    openTime,
					SourceID:   42,
					SourceName: "Fixture source",
					SourceURL:  "https://source.example.test/listing",
					EvidenceID: 302,
				},
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/74")
	assertContains(t, body, "Applied import listing")
	assertContains(t, body, "event #901")
	assertContains(t, body, "Accept New Listing")
	assertContains(t, body, "Leadmill")
	assertContains(t, body, "Fixture source")
	assertContains(t, body, "source.example.test/listing")
	assertNotContains(t, body, "Accept new listing")
	assertNotContains(t, body, `name="action" value="resolve_import_new_listing"`)
}

func TestAdminEventReviewDetailShowsMalformedResolutionSnapshotWarning(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             61,
				Status:         store.EventReviewClusterStatusResolved,
				Version:        2,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reason-c",
				EvidenceCount:  0,
				UpdatedAt:      openTime,
			},
			Resolution: &store.EventReviewResolutionSummary{
				ID:                   201,
				ClusterID:            61,
				Status:               store.EventReviewResolutionStatusResolved,
				SnapshotRaw:          "{bad snapshot",
				SnapshotParseWarning: "invalid character 'b' looking for beginning of object key string",
				CreatedAt:            openTime,
				UpdatedAt:            openTime,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/61")
	assertContains(t, body, "Resolution snapshot warning")
	assertContains(t, body, "Raw snapshot")
	assertContains(t, body, "{bad snapshot")
	assertContains(t, body, "Resolution")
}

func TestAdminEventReviewDetailShowsSupersededByClusterLink(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	supersedingID := int64(99)
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:               62,
				Status:           store.EventReviewClusterStatusSuperseded,
				Version:          2,
				ConflictType:     "historical_duplicate",
				ConflictReason:   "reason-d",
				CanonicalEventID: int64Ptr(88),
				EvidenceCount:    0,
				UpdatedAt:        openTime,
			},
			Resolution: &store.EventReviewResolutionSummary{
				ID:                    202,
				ClusterID:             62,
				Status:                store.EventReviewResolutionStatusSuperseded,
				SupersededByClusterID: &supersedingID,
				CreatedAt:             openTime,
				UpdatedAt:             openTime,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/62")
	assertContains(t, body, "cluster #99")
	assertContains(t, body, "Resolution")
	assertContains(t, body, "superseded")
}

func TestAdminEventReviewDetailShowsSupersedeForm(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             83,
				Status:         store.EventReviewClusterStatusOpen,
				Version:        5,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reason-e",
				EvidenceCount:  1,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/83")
	assertContains(t, body, "Supersede cluster")
	assertContains(t, body, "Mark this cluster as superseded by another event-review cluster.")
	assertContains(t, body, `name="action" value="supersede"`)
	assertContains(t, body, `name="superseded_by_cluster_id"`)
}

func TestAdminEventReviewDetailHidesSupersedeFormForTerminalCluster(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:             84,
				Status:         store.EventReviewClusterStatusResolved,
				Version:        5,
				ConflictType:   "historical_duplicate",
				ConflictReason: "reason-f",
				EvidenceCount:  1,
			},
		},
	}
	server, err := NewServer(testServerDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/event-review/84")
	assertNotContains(t, body, "Supersede cluster")
	assertNotContains(t, body, `name="action" value="supersede"`)
}

func TestAdminEventReviewDiscardPostsAndRedirects(t *testing.T) {
	stagingKey := "repair-queue-a"
	store := &eventReviewOnlyStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "type-a",
				ConflictReason:    "reason-a",
				EvidenceCount:     1,
			},
		},
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "type-a",
				ConflictReason:    "reason-a",
				EvidenceCount:     1,
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "3")
	form.Set("action", "discard")
	form.Set("discard_reason", "duplicate staging cluster")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_discarded=1" {
		t.Fatalf("Location = %q, want discard redirect", location)
	}
	if !store.discardCalled {
		t.Fatal("expected discard store method to be called")
	}
	if store.discardInput.ClusterID != 41 || store.discardInput.ExpectedVersion != 3 || store.discardInput.Reason != "duplicate staging cluster" {
		t.Fatalf("discard input = %#v", store.discardInput)
	}

	queueReq := httptest.NewRequest(http.MethodGet, "/admin/review?event_review_discarded=1", nil)
	queueReq.AddCookie(cookie)
	queueRR := httptest.NewRecorder()
	server.ServeHTTP(queueRR, queueReq)
	if queueRR.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want %d", queueRR.Code, http.StatusOK)
	}
	assertContains(t, queueRR.Body.String(), "Discarded event review cluster.")
	assertContains(t, queueRR.Body.String(), "Event review clusters")
}

func TestAdminEventReviewDiscardRejectsInvalidForms(t *testing.T) {
	tests := []struct {
		name   string
		form   url.Values
		status int
		want   string
	}{
		{name: "missing reason", form: url.Values{"expected_version": {"3"}, "action": {"discard"}}, status: http.StatusBadRequest, want: "discard reason is required"},
		{name: "missing version", form: url.Values{"action": {"discard"}, "discard_reason": {"reason"}}, status: http.StatusBadRequest, want: "expected version is required"},
		{name: "invalid version", form: url.Values{"expected_version": {"not-a-number"}, "action": {"discard"}, "discard_reason": {"reason"}}, status: http.StatusBadRequest, want: "expected version is required"},
		{name: "unknown action", form: url.Values{"expected_version": {"3"}, "action": {"resolve"}, "discard_reason": {"reason"}}, status: http.StatusBadRequest, want: "invalid event review action"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{ID: 41, Status: store.EventReviewClusterStatusOpen, Version: 3},
				},
			}
			server, err := NewServer(testAdminAuthDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			cookie, _ := loginAdmin(t, server, "/admin/review")
			getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
			getReq.AddCookie(cookie)
			getRR := httptest.NewRecorder()
			server.ServeHTTP(getRR, getReq)
			if getRR.Code != http.StatusOK {
				t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
			}
			csrfToken := extractCSRFToken(t, getRR.Body.String())
			tc.form.Set("csrf_token", csrfToken)

			req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(tc.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != tc.status {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, tc.status, rr.Body.String())
			}
			assertContains(t, rr.Body.String(), tc.want)
			if store.discardCalled {
				t.Fatal("discard store method should not be called")
			}
		})
	}
}

func TestAdminEventReviewDiscardRejectsInvalidCSRF(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{ID: 41, Status: store.EventReviewClusterStatusOpen, Version: 3},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	form := url.Values{
		"csrf_token":       {"wrong"},
		"expected_version": {"3"},
		"action":           {"discard"},
		"discard_reason":   {"reason"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "invalid CSRF token")
	if store.discardCalled {
		t.Fatal("discard store method should not be called")
	}
}

func TestAdminEventReviewSupersedePostsAndRedirects(t *testing.T) {
	stagingKey := "repair-queue-a"
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "historical_duplicate",
				ConflictReason:    "reason-a",
				EvidenceCount:     1,
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "3")
	form.Set("action", "supersede")
	form.Set("superseded_by_cluster_id", "99")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_superseded=1" {
		t.Fatalf("Location = %q, want supersede redirect", location)
	}
	if !store.supersedeCalled {
		t.Fatal("expected supersede store method to be called")
	}
	if store.supersedeInput.ClusterID != 41 || store.supersedeInput.ExpectedVersion != 3 || store.supersedeInput.SupersededByClusterID != 99 {
		t.Fatalf("supersede input = %#v", store.supersedeInput)
	}

	queueReq := httptest.NewRequest(http.MethodGet, "/admin/review?event_review_superseded=1", nil)
	queueReq.AddCookie(cookie)
	queueRR := httptest.NewRecorder()
	server.ServeHTTP(queueRR, queueReq)
	if queueRR.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want %d", queueRR.Code, http.StatusOK)
	}
	assertContains(t, queueRR.Body.String(), "Superseded event review cluster.")
}

func TestAdminEventReviewSupersedeRejectsInvalidFormsAndCSRF(t *testing.T) {
	stagingKey := "repair-queue-a"
	tests := []struct {
		name string
		form url.Values
		want string
	}{
		{name: "missing version", form: url.Values{"action": {"supersede"}, "superseded_by_cluster_id": {"99"}}, want: "expected version is required"},
		{name: "invalid version", form: url.Values{"expected_version": {"not-a-number"}, "action": {"supersede"}, "superseded_by_cluster_id": {"99"}}, want: "expected version is required"},
		{name: "missing target", form: url.Values{"expected_version": {"3"}, "action": {"supersede"}}, want: "superseded by cluster ID is required"},
		{name: "invalid target", form: url.Values{"expected_version": {"3"}, "action": {"supersede"}, "superseded_by_cluster_id": {"not-a-number"}}, want: "superseded by cluster ID is required"},
		{name: "unknown action", form: url.Values{"expected_version": {"3"}, "action": {"resolve"}, "superseded_by_cluster_id": {"99"}}, want: "invalid event review action"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{
						ID:                41,
						Status:            store.EventReviewClusterStatusOpen,
						Version:           3,
						StagingKey:        &stagingKey,
						StagingKeyVersion: 3,
						ConflictType:      "historical_duplicate",
						ConflictReason:    "reason-a",
						EvidenceCount:     1,
					},
				},
			}
			server, err := NewServer(testAdminAuthDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			cookie, _ := loginAdmin(t, server, "/admin/review")
			getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
			getReq.AddCookie(cookie)
			getRR := httptest.NewRecorder()
			server.ServeHTTP(getRR, getReq)
			if getRR.Code != http.StatusOK {
				t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
			}
			csrfToken := extractCSRFToken(t, getRR.Body.String())
			tc.form.Set("csrf_token", csrfToken)

			req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(tc.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			assertContains(t, rr.Body.String(), tc.want)
			if store.supersedeCalled {
				t.Fatal("supersede store method should not be called")
			}
		})
	}
}

func TestAdminEventReviewSupersedeRejectsInvalidCSRF(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{ID: 41, Status: store.EventReviewClusterStatusOpen, Version: 3},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	form := url.Values{
		"csrf_token":               {"wrong"},
		"expected_version":         {"3"},
		"action":                   {"supersede"},
		"superseded_by_cluster_id": {"99"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "invalid CSRF token")
	if store.supersedeCalled {
		t.Fatal("supersede store method should not be called")
	}
}

func TestAdminEventReviewSupersedeReturnsStoreError(t *testing.T) {
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{ID: 41, Status: store.EventReviewClusterStatusOpen, Version: 3},
		},
		supersedeErr: fmt.Errorf("cluster cannot supersede itself"),
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{
		"csrf_token":               {csrfToken},
		"expected_version":         {"3"},
		"action":                   {"supersede"},
		"superseded_by_cluster_id": {"41"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "cluster cannot supersede itself")
	if !store.supersedeCalled {
		t.Fatal("expected supersede store method to be called")
	}
}

func TestAdminEventReviewResolvePostsAndRedirects(t *testing.T) {
	stagingKey := "repair-queue-a"
	store := &eventReviewOnlyStoreStub{
		clusters: []store.EventReviewClusterSummary{
			{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "historical_duplicate",
				ConflictReason:    "reason-a",
				CanonicalEventID:  int64Ptr(88),
				EvidenceCount:     2,
			},
		},
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "historical_duplicate",
				ConflictReason:    "reason-a",
				CanonicalEventID:  int64Ptr(88),
				EvidenceCount:     2,
			},
			LiveActions: []store.EventReviewClusterLiveActionSummary{
				{ID: 1, EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
				{ID: 2, EventID: 90, EventSlug: "loser-event", Action: store.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold"},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")
	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "3")
	form.Set("action", "resolve_live_actions")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_resolved=1" {
		t.Fatalf("Location = %q, want resolve redirect", location)
	}
	if !store.resolveCalled {
		t.Fatal("expected resolve store method to be called")
	}
	if store.resolveInput.ClusterID != 41 || store.resolveInput.ExpectedVersion != 3 {
		t.Fatalf("resolve input = %#v", store.resolveInput)
	}

	queueReq := httptest.NewRequest(http.MethodGet, "/admin/review?event_review_resolved=1", nil)
	queueReq.AddCookie(cookie)
	queueRR := httptest.NewRecorder()
	server.ServeHTTP(queueRR, queueReq)
	if queueRR.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want %d", queueRR.Code, http.StatusOK)
	}
	assertContains(t, queueRR.Body.String(), "Resolved event review cluster.")
}

func TestAdminEventReviewResolveTitleRepairPostsAndRedirects(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	stagingKey := "repair-queue-title"
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                 41,
				Status:             store.EventReviewClusterStatusOpen,
				Version:            4,
				StagingKey:         &stagingKey,
				StagingKeyVersion:  1,
				ConflictType:       "title_repair",
				ConflictReason:     "supporting_clean_title",
				CanonicalEventID:   int64Ptr(88),
				CanonicalEventSlug: "title-repair-current",
				DisplayTitle:       "Title Repair Current",
				DisplayVenueSlug:   "title-repair-hall",
				DisplayVenueName:   "Title Repair Hall",
				DisplayStartAt:     &openTime,
				EvidenceCount:      1,
			},
			TitleRepairReadiness: &store.EventReviewTitleRepairReadiness{
				CanonicalEventID: 88,
				CurrentTitle:     "Legacy Event",
				CurrentSlug:      "title-repair-current",
				CurrentEventLive: true,
				DraftTitle:       "Updated Title",
				DraftSlug:        "title-repair-current-renamed",
				Eligible:         true,
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")
	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "4")
	form.Set("action", "resolve_title_repair")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_resolved=1" {
		t.Fatalf("Location = %q, want resolve redirect", location)
	}
	if !store.resolveCalled {
		t.Fatal("expected resolve store method to be called")
	}
	if store.resolveInput.ClusterID != 41 || store.resolveInput.ExpectedVersion != 4 {
		t.Fatalf("resolve input = %#v", store.resolveInput)
	}

	queueReq := httptest.NewRequest(http.MethodGet, "/admin/review?event_review_resolved=1", nil)
	queueReq.AddCookie(cookie)
	queueRR := httptest.NewRecorder()
	server.ServeHTTP(queueRR, queueReq)
	if queueRR.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want %d", queueRR.Code, http.StatusOK)
	}
	assertContains(t, queueRR.Body.String(), "Resolved event review cluster.")
}

func TestAdminEventReviewResolveLiveActionsRejectsTitleRepairCluster(t *testing.T) {
	openTime := time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)
	stagingKey := "repair-queue-title"
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                 41,
				Status:             store.EventReviewClusterStatusOpen,
				Version:            4,
				StagingKey:         &stagingKey,
				StagingKeyVersion:  1,
				ConflictType:       "title_repair",
				ConflictReason:     "supporting_clean_title",
				CanonicalEventID:   int64Ptr(88),
				CanonicalEventSlug: "title-repair-current",
				DisplayTitle:       "Title Repair Current",
				DisplayVenueSlug:   "title-repair-hall",
				DisplayVenueName:   "Title Repair Hall",
				DisplayStartAt:     &openTime,
				EvidenceCount:      1,
			},
			TitleRepairReadiness: &store.EventReviewTitleRepairReadiness{
				CanonicalEventID: 88,
				CurrentTitle:     "Legacy Event",
				CurrentSlug:      "title-repair-current",
				CurrentEventLive: true,
				DraftTitle:       "Updated Title",
				DraftSlug:        "title-repair-current-renamed",
				Eligible:         true,
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")
	getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
	getReq.AddCookie(cookie)
	getRR := httptest.NewRecorder()
	server.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	csrfToken := extractCSRFToken(t, getRR.Body.String())

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "4")
	form.Set("action", "resolve_live_actions")
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "not eligible for historical duplicate resolution")
	if store.resolveCalled {
		t.Fatal("resolve store method should not be called")
	}
}

func TestAdminEventReviewResolveRejectsInvalidFormsAndCSRF(t *testing.T) {
	stagingKey := "repair-queue-a"
	tests := []struct {
		name string
		form url.Values
		want string
	}{
		{name: "missing version", form: url.Values{"action": {"resolve_live_actions"}}, want: "expected version is required"},
		{name: "invalid version", form: url.Values{"expected_version": {"not-a-number"}, "action": {"resolve_live_actions"}}, want: "expected version is required"},
		{name: "unknown action", form: url.Values{"expected_version": {"3"}, "action": {"resolve"}}, want: "invalid event review action"},
		{name: "title repair missing version", form: url.Values{"action": {"resolve_title_repair"}}, want: "expected version is required"},
		{name: "title repair invalid version", form: url.Values{"expected_version": {"not-a-number"}, "action": {"resolve_title_repair"}}, want: "expected version is required"},
		{name: "title repair wrong cluster", form: url.Values{"expected_version": {"3"}, "action": {"resolve_title_repair"}}, want: "not eligible for title repair resolution"},
		{name: "title repair unknown action", form: url.Values{"expected_version": {"3"}, "action": {"resolve_title_repair_now"}}, want: "invalid event review action"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &eventReviewOnlyStoreStub{
				detail: store.EventReviewClusterDetail{
					Summary: store.EventReviewClusterSummary{
						ID:                41,
						Status:            store.EventReviewClusterStatusOpen,
						Version:           3,
						StagingKey:        &stagingKey,
						StagingKeyVersion: 3,
						ConflictType:      "historical_duplicate",
						ConflictReason:    "reason-a",
						CanonicalEventID:  int64Ptr(88),
						EvidenceCount:     2,
					},
					LiveActions: []store.EventReviewClusterLiveActionSummary{
						{ID: 1, EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
						{ID: 2, EventID: 90, EventSlug: "loser-event", Action: store.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold"},
					},
				},
			}
			server, err := NewServer(testAdminAuthDeps(store))
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			cookie, _ := loginAdmin(t, server, "/admin/review")
			getReq := httptest.NewRequest(http.MethodGet, "/admin/event-review/41", nil)
			getReq.AddCookie(cookie)
			getRR := httptest.NewRecorder()
			server.ServeHTTP(getRR, getReq)
			if getRR.Code != http.StatusOK {
				t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
			}
			csrfToken := extractCSRFToken(t, getRR.Body.String())
			tc.form.Set("csrf_token", csrfToken)

			req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(tc.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			assertContains(t, rr.Body.String(), tc.want)
			if store.resolveCalled {
				t.Fatal("resolve store method should not be called")
			}
		})
	}
}

func TestAdminEventReviewResolveRejectsInvalidCSRF(t *testing.T) {
	stagingKey := "repair-queue-a"
	store := &eventReviewOnlyStoreStub{
		detail: store.EventReviewClusterDetail{
			Summary: store.EventReviewClusterSummary{
				ID:                41,
				Status:            store.EventReviewClusterStatusOpen,
				Version:           3,
				StagingKey:        &stagingKey,
				StagingKeyVersion: 3,
				ConflictType:      "historical_duplicate",
				ConflictReason:    "reason-a",
				CanonicalEventID:  int64Ptr(88),
				EvidenceCount:     2,
			},
			LiveActions: []store.EventReviewClusterLiveActionSummary{
				{ID: 1, EventID: 88, EventSlug: "canonical-event", Action: store.EventReviewLiveActionKindKeepSeparate, Reason: "keep"},
				{ID: 2, EventID: 90, EventSlug: "loser-event", Action: store.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold"},
			},
		},
	}
	server, err := NewServer(testAdminAuthDeps(store))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	form := url.Values{
		"csrf_token":       {"wrong"},
		"expected_version": {"3"},
		"action":           {"resolve_live_actions"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/event-review/41", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "invalid CSRF token")
	if store.resolveCalled {
		t.Fatal("resolve store method should not be called")
	}
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
	assertContains(t, body, `href="/admin/event-review/history"`)
	assertContains(t, body, "Event review history")
	assertContains(t, body, `href="/admin/import-runs"`)
	assertContains(t, body, `href="/admin/venues"`)
	assertContains(t, body, "Review event-review clusters, inspect ingest runs, and validate provisional venues and rooms.")
	assertNotContains(t, body, "Resolve duplicate groups and accept or reject new listings.")
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
	assertNotContains(t, body, `href="/admin/event-review/history"`)
}

func TestAdminLandingPageRejectsPost(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryOnlyStoreStub{}))
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
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
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

func TestAdminRemovedRoutesRedirectUnauthenticatedUsers(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	tests := []string{
		"/admin/legacy-review",
		"/admin/legacy-review/",
		"/admin/legacy-review/history",
		"/admin/legacy-review/17",
		"/admin/legacy-review/17/",
		"/admin/legacy-review/17?view=full",
		"/admin/review/history",
		"/admin/review/history/",
		"/admin/review/history?tab=closed",
		"/admin/review/17",
		"/admin/review/17/",
		"/admin/review/17?view=full",
		"/admin/review/not-a-number",
		"/admin/review/0",
		"/admin/review/-1",
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusSeeOther {
				t.Fatalf("status = %d, want %d", rr.Code, http.StatusSeeOther)
			}
			cleanedPath, rawQuery, _ := strings.Cut(path, "?")
			wantNext := pathpkg.Clean(cleanedPath)
			if rawQuery != "" {
				wantNext += "?" + rawQuery
			}
			wantLocation := "/admin/login?next=" + url.QueryEscape(wantNext)
			if location := rr.Header().Get("Location"); location != wantLocation {
				t.Fatalf("Location = %q, want %q", location, wantLocation)
			}
		})
	}
}

func TestAdminRemovedRoutesReturn404WhenAuthenticated(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")

	tests := []string{
		"/admin/legacy-review",
		"/admin/legacy-review/",
		"/admin/legacy-review/history",
		"/admin/legacy-review/17",
		"/admin/legacy-review/17/",
		"/admin/legacy-review/17?view=full",
		"/admin/review/history",
		"/admin/review/history/",
		"/admin/review/history?tab=closed",
		"/admin/review/17",
		"/admin/review/17/",
		"/admin/review/17?view=full",
		"/admin/review/not-a-number",
		"/admin/review/0",
		"/admin/review/-1",
		"/admin/review/17/extra",
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.AddCookie(cookie)
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusNotFound, rr.Body.String())
			}
		})
	}
}

func TestAdminLoginSetsSecureSessionCookieAndAllowsAdminAccess(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(importHistoryOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, location := loginAdmin(t, server, "/admin")
	if location != "/admin" {
		t.Fatalf("login Location = %q, want /admin", location)
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

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "Admin")
	assertContains(t, rr.Body.String(), `name="csrf_token"`)
}

func TestAdminLoginRejectsBadPassword(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
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
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
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
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	_, location := loginAdmin(t, server, "https://example.test/admin")
	if location != "/admin" {
		t.Fatalf("login Location = %q, want /admin", location)
	}
}

func TestAdminLoginThrottlesRepeatedFailures(t *testing.T) {
	deps := testAdminAuthDeps(readOnlyStoreStub{})
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
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")
	form := url.Values{"action": {"rejected"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/review", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
	}
	if allow := rr.Header().Get("Allow"); allow != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", allow)
	}
}

func TestAdminPostRejectsCSRFTokenInQueryString(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(readOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin/review")
	form := url.Values{"action": {"rejected"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/review?csrf_token=bogus", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
	}
	if allow := rr.Header().Get("Allow"); allow != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", allow)
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
	defer st.Close()
	seedAdminVenueFixtures(t, path)
	seedAdminRoomFixture(t, path, "quiet-room", "courtyard-stage", "Courtyard Stage", 1)

	server, err := NewServer(testAdminAuthDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin")

	form := url.Values{"action": {"validate"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/rooms/quiet-room/courtyard-stage", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusForbidden, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/rooms/quiet-room/courtyard-stage", nil)
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
	req = httptest.NewRequest(http.MethodPost, "/admin/rooms/quiet-room/courtyard-stage", strings.NewReader(form.Encode()))
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
	server, err := NewServer(testAdminAuthDeps(importHistoryOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	cookie, _ := loginAdmin(t, server, "/admin")
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
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

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("post-logout status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
}

func TestAdminSessionExpires(t *testing.T) {
	deps := testAdminAuthDeps(importHistoryOnlyStoreStub{})
	deps.AdminAuth.SessionIdleTimeout = time.Minute
	deps.AdminAuth.SessionAbsoluteTimeout = time.Hour
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	now := fixtureLocalTime(2026, time.May, 13, 10, 0)
	server.SetClockForTesting(func() time.Time { return now })
	cookie, _ := loginAdmin(t, server, "/admin")

	now = now.Add(2 * time.Minute)
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
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
	assertContains(t, populatedBody, "<th scope=\"col\">Event reviews</th>")
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

func TestSQLiteAdminImportRunsRenderEventReviewStatusSummary(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryWithEventReviewRowsStoreStub{
		runs: []ingest.ImportRunSummary{
			{ID: 1, StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC), Status: "succeeded", SnapshotCount: 1, Notes: "mixed statuses"},
			{ID: 2, StartedAt: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC), Status: "succeeded", SnapshotCount: 0, Notes: "no event reviews"},
		},
		clusters: map[int64][]store.EventReviewClusterSummary{
			1: {
				{ID: 11, Status: store.EventReviewClusterStatusOpen},
				{ID: 12, Status: store.EventReviewClusterStatusResolved},
				{ID: 13, Status: store.EventReviewClusterStatusDiscarded},
			},
		},
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "<th scope=\"col\">Event reviews</th>")
	assertContains(t, body, `href="/admin/import-runs/1">1 open, 1 resolved, 1 discarded</a>`)
	assertContains(t, body, `href="/admin/import-runs/2">none</a>`)
}

func TestAdminImportRunsEventReviewSummaryErrorReturns500(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryWithEventReviewErrorStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/import-runs", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusInternalServerError, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "load import run summaries")
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
	assertContains(t, body, "Event review clusters from this import run")
	assertContains(t, body, "No event review clusters are linked to this import run.")
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

func TestSQLiteAdminImportRunDetailDerivesEventReviewStoreFromCatalog(t *testing.T) {
	st, _, runID, _, path := mustImportRunDetailServer(t, false)
	defer st.Close()

	openClusterID, _ := seedImportRunEventReviewClusters(t, path, runID)
	deps := testServerDeps(st)
	deps.ImportRunEventReviewClusterStore = nil
	server, err := NewServer(deps)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, body, "Event review clusters from this import run")
	assertContains(t, body, `href="/admin/event-review/`+strconvFormatInt(openClusterID)+`"`)
	assertContains(t, body, "Import Run Event Review Open")
}

func TestSQLiteAdminImportRunDetailShowsEmptyEventReviewSectionWhenUnlinked(t *testing.T) {
	st, server, runID, _, _ := mustImportRunDetailServer(t, false)
	defer st.Close()

	body := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, body, "Event review clusters from this import run")
	assertContains(t, body, "No event review clusters are linked to this import run.")
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
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()
	seedAdminVenueFixtures(t, path)
	seedAdminRoomFixture(t, path, "quiet-room", "courtyard-stage", "Courtyard Stage", 1)

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	body := renderPath(t, server, "/admin/rooms")
	assertContains(t, body, "Provisional rooms")
	assertContains(t, body, "Courtyard Stage")
	assertContains(t, body, "Quiet Room")
	assertContains(t, body, `href="/admin/rooms/quiet-room/courtyard-stage"`)
	assertContains(t, body, ">0</td>")

	detailBody := renderPath(t, server, "/admin/rooms/quiet-room/courtyard-stage")
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
	assertContains(t, body, `href="/admin/events/imaginary-hall-future-show"`)
	assertContains(t, body, "Future Show")
	assertContains(t, body, ">1</td>")
	assertNotContains(t, body, `href="/admin/venues/validated-room"`)
	assertNotContains(t, body, "Validated Room")
}

func TestSQLiteAdminVenuesShowStagedProvisionalVenueWithoutEvents(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
	defer st.Close()
	seedAdminVenueFixtures(t, path)

	body := renderPath(t, server, "/admin/venues")
	assertContains(t, body, "Queue of provisional venue rows created from newly detected venue evidence.")
	assertContains(t, body, `href="/admin/venues/quiet-room"`)
	assertContains(t, body, "Quiet Room")
	assertContains(t, body, ">0</td>")

	detailBody := renderPath(t, server, "/admin/venues/quiet-room")
	assertContains(t, detailBody, "No upcoming linked events for this provisional venue.")
}

func TestSQLitePublicVenuePagesRenderDerivedProvisionalVenueAddress(t *testing.T) {
	st, server, path := mustAdminVenuesServer(t)
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

	db := mustRawDB(t, path)
	defer db.Close()
	sourceID := mustInsertAdminSource(t, db, "Leadmill ICS", "file:memorial-hall.ics")
	mustInsertAdminVenue(t, db, domain.Venue{
		Slug:            "memorial-hall",
		Name:            "Memorial Hall",
		Address:         "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA",
		Neighbourhood:   "City Centre",
		ValidationState: domain.ValidationStateProvisional,
		CoverageKind:    domain.CoverageKindVenue,
		Origin:          domain.OriginLive,
	})
	mustInsertAdminEvent(
		t,
		db,
		sourceID,
		"memorial-hall-show",
		"memorial-hall",
		result.Candidates[0].Summary,
		time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC),
		time.Date(2026, time.May, 1, 22, 0, 0, 0, time.UTC),
		"Imported from escaped ICS venue evidence.",
	)

	adminQueueBody := renderPath(t, server, "/admin/venues")
	assertContains(t, adminQueueBody, "Memorial Hall")
	assertContains(t, adminQueueBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertNotContains(t, adminQueueBody, "Memorial Hall,\nBarkers Pool")

	venueBody := renderPath(t, server, "/venues/memorial-hall")
	assertContains(t, venueBody, "Memorial Hall")
	assertContains(t, venueBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertContains(t, venueBody, "City Centre")
	assertNotContains(t, venueBody, "Memorial Hall,\nBarkers Pool")

	eventBody := renderPath(t, server, "/events/memorial-hall-show")
	assertContains(t, eventBody, "Memorial Hall Show")
	assertContains(t, eventBody, "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA")
	assertContains(t, eventBody, "City Centre")
	assertNotContains(t, eventBody, "Memorial Hall,\nBarkers Pool")

	venuesBody := renderPath(t, server, "/venues")
	assertContains(t, venuesBody, "Memorial Hall")
	assertContains(t, venuesBody, `<span class="venue-area">City Centre</span>`)
	assertContains(t, venuesBody, `<span class="venue-address">Barkers Pool,
Sheffield City Centre,
Sheffield,
S1 2JA</span>`)
	assertNotContains(t, venuesBody, "Memorial Hall,\nBarkers Pool")
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
	assertContains(t, venuesBody, `<span class="venue-area">City Centre</span>`)
	assertContains(t, venuesBody, `<span class="venue-address">1 Void Street,
Sheffield</span>`)
	assertNotContains(t, venuesBody, "Imaginary Hall marketing copy,\n1 Void Street")
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
	assertContains(t, body, `href="/admin/events/imaginary-hall-future-show"`)
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
	st, server, runID, _, _ := mustImportRunDetailServer(t, false)
	defer st.Close()

	reviewBody := renderPath(t, server, "/admin/review")
	assertContains(t, reviewBody, `href="/admin/venues"`)

	importRunsBody := renderPath(t, server, "/admin/import-runs")
	assertContains(t, importRunsBody, `href="/admin/venues"`)

	importRunDetailBody := renderPath(t, server, "/admin/import-runs/"+strconvFormatInt(runID))
	assertContains(t, importRunDetailBody, `href="/admin/venues"`)
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

func TestAdminImportRunDetailEventReviewStoreErrorReturns500(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryWithDetailEventReviewErrorStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/import-runs/1", nil)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusInternalServerError, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "load import run event review clusters")
}

func TestAdminImportRunsOmitsDetailLinksWithoutReplayStore(t *testing.T) {
	server, err := NewServer(testServerDeps(importHistoryOnlyStoreStub{}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	body := renderPath(t, server, "/admin/import-runs")
	assertContains(t, body, "Run #1")
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
	sourceID := mustInsertAdminSource(t, db, "Fixture ICS", "https://example.test/utc-show")
	mustInsertAdminEvent(t, db, sourceID, "live-utc-show-sidney-and-matilda-20260501190000", "sidney-and-matilda", "UTC Show", fixtureLocalTime(2026, time.May, 1, 19, 0), fixtureLocalTime(2026, time.May, 1, 22, 0), "Fixture listing.")
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	server, err := NewServer(testServerDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
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

func TestHomeShowsDefaultEventBoard(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/")

	assertContains(t, body, "Upcoming shows")
	assertInOrder(t, body, []string{
		"Tonight",
		"Tonight Leadmill",
		"Tomorrow",
		"Tomorrow Yellow Arch",
		"Friday",
		"Friday Leadmill",
	})
	assertContains(t, body, `<a class="active" aria-current="page" href="/">Events</a>`)
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
	assertContains(t, eventsBody, `<span class="event-location has-room has-venue has-area">`)
	assertContains(t, eventsBody, `<span class="event-location-venue">Sidney &amp; Matilda</span><span class="event-location-separator">:</span><span class="event-location-room">Factory</span>`)
	assertContains(t, eventsBody, `<span class="event-location-area">Cultural Industries Quarter</span>`)
	assertNotContains(t, eventsBody, "Experimental · Listed")

	eventBody := renderPath(t, server, "/events/parallel-delusion")
	assertInOrder(t, eventBody, []string{
		`<p class="event-detail-room">Factory</p>`,
		`<p class="event-detail-venue"><a href="/venues/sidney-and-matilda">Sidney &amp; Matilda</a></p>`,
		`<p class="event-detail-area">Cultural Industries Quarter</p>`,
		`<p class="address-text">Rivelin Works`,
	})

	venueBody := renderPath(t, server, "/venues/sidney-and-matilda")
	assertContains(t, venueBody, `<span class="event-location has-room">`)
	assertContains(t, venueBody, `<span class="event-location-room">Factory</span>`)
	assertNotContains(t, venueBody, `<span class="event-location-venue">Sidney &amp; Matilda</span>`)
}

func TestPublicEventTitleCleansSourcePresentationLeaks(t *testing.T) {
	venueNames := map[string]string{
		"cafe-no-9":           "Cafe No. 9",
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
			name:      "double slash venue suffix",
			eventName: "Club Night // Yellow Arch",
			venueSlug: "yellow-arch-studios",
			want:      "Club Night",
		},
		{
			name:      "parenthetical alias venue suffix",
			eventName: "Club Night (Yellow Arch)",
			venueSlug: "yellow-arch-studios",
			want:      "Club Night",
		},
		{
			name:      "venue suffix with parenthetical qualifier",
			eventName: "PINS plus Gia Ford & Gelder - Yellow Arch (Rescheduled Date)",
			venueSlug: "yellow-arch-studios",
			want:      "PINS plus Gia Ford & Gelder",
		},
		{
			name:      "at venue suffix with back to back qualifier",
			eventName: "An evening with Artist at Cafe No. 9 (the first of two back to back shows)",
			venueSlug: "cafe-no-9",
			want:      "Artist",
		},
		{
			name:      "at venue suffix with dash qualifier",
			eventName: "An evening with The 20ft Squid Blues Band at Cafe No9 - The first of two back to back shows",
			venueSlug: "cafe-no-9",
			want:      "The 20ft Squid Blues Band",
		},
		{
			name:      "cafe no 9 house prefix without venue suffix",
			eventName: "An evening with Ellie Gowers",
			venueSlug: "cafe-no-9",
			want:      "Ellie Gowers",
		},
		{
			name:      "cafe no 9 house prefix is venue scoped",
			eventName: "An evening with Ellie Gowers",
			venueSlug: "sidney-and-matilda",
			want:      "An evening with Ellie Gowers",
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
	assertContains(t, homeBody, `<span class="event-location has-venue has-area">`)
	assertContains(t, homeBody, `<span class="event-location-venue">Sidney &amp; Matilda</span>`)
	assertContains(t, homeBody, `<span class="event-location-area">Cultural Industries Quarter</span>`)
	assertNotContains(t, homeBody, `Sidney &amp; Matilda ·`)

	eventsBody := renderPath(t, server, "/events?window=all")
	assertContains(t, eventsBody, `Dansette Springs`)
	assertContains(t, eventsBody, `<span class="event-location-venue">Foundry</span>`)
	assertContains(t, eventsBody, `<span class="event-location-area">Broomhall</span>`)
	assertContains(t, eventsBody, `<span class="event-genre">Blues</span>`)
	assertNotContains(t, eventsBody, `Blues · Listed`)
	assertContains(t, eventsBody, `Marmozets`)
	assertNotContains(t, eventsBody, `Marmozets - Foundry`)
	assertContains(t, eventsBody, `<span class="event-status">Postponed</span>`)

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
	assertContains(t, body, "19 Apr - 1 show")
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Friday Leadmill")
}

func TestEventsDefaultShowsOnlyDaysWithCurrentOrUpcomingEvents(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events")

	assertInOrder(t, body, []string{
		"Tonight",
		"19 Apr - 1 show",
		"Tonight Leadmill",
		"Tomorrow",
		"20 Apr - 1 show",
		"Tomorrow Yellow Arch",
		"Friday",
		"24 Apr - 1 show",
		"Friday Leadmill",
		"Monday",
		"27 Apr - 1 show",
		"Later Leadmill",
	})
	assertNotContains(t, body, "Past Leadmill")
	assertNotContains(t, body, "No shows listed")
	assertNotContains(t, body, "21 Apr - 0 shows")
}

func TestEventsDefaultShowsOngoingTodayEventsAndOmitsEndedDays(t *testing.T) {
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
				Slug:        "ended-show",
				Name:        "Ended Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 18, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 20, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Ended today.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/ended-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "ongoing-show",
				Name:        "Ongoing Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Still running.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/ongoing-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "tomorrow-show",
				Name:        "Tomorrow Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 20, 19, 0),
				End:         fixtureLocalTime(2026, time.April, 20, 21, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Tomorrow.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/tomorrow-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 21, 0)
	})

	body := renderPath(t, server, "/events")

	assertInOrder(t, body, []string{
		"Tonight",
		"19 Apr - 1 show",
		"Ongoing Show",
		"Tomorrow",
		"20 Apr - 1 show",
		"Tomorrow Show",
	})
	assertNotContains(t, body, "Ended Show")
	assertNotContains(t, body, "No shows listed")
}

func TestEventsShowOngoingPriorDayEventsInCurrentDay(t *testing.T) {
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
				Slug:        "ended-overnight-show",
				Name:        "Ended Overnight Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 21, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 23, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Ended before midnight.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/ended-overnight-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "ongoing-overnight-show",
				Name:        "Ongoing Overnight Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 23, 0),
				End:         fixtureLocalTime(2026, time.April, 20, 2, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Still running after midnight.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/ongoing-overnight-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "later-today-show",
				Name:        "Later Today Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 20, 19, 0),
				End:         fixtureLocalTime(2026, time.April, 20, 21, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Later today.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/later-today-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 20, 0, 30)
	})

	homeBody := renderPath(t, server, "/")
	assertInOrder(t, homeBody, []string{
		"Tonight",
		"20 Apr - 2 shows",
		"Ongoing Overnight Show",
		"Later Today Show",
	})
	assertNotContains(t, homeBody, "Ended Overnight Show")
	assertNotContains(t, homeBody, "19 Apr - 1 show")

	filteredBody := renderPath(t, server, "/events?window=today")
	assertInOrder(t, filteredBody, []string{
		"Tonight",
		"20 Apr - 2 shows",
		"Ongoing Overnight Show",
		"Later Today Show",
	})
	assertNotContains(t, filteredBody, "Ended Overnight Show")
	assertNotContains(t, filteredBody, "19 Apr - 1 show")
}

func TestEventsKeepStartOnlyShowsVisibleThroughLocalDay(t *testing.T) {
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
				Slug:        "start-only-yesterday",
				Name:        "Start Only Yesterday",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Started yesterday with no listed end time.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/start-only-yesterday",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "start-only-today",
				Name:        "Start Only Today",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 20, 20, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Started today with no listed end time.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/start-only-today",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "tomorrow-show",
				Name:        "Tomorrow Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 21, 19, 0),
				End:         fixtureLocalTime(2026, time.April, 21, 21, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Tomorrow.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/tomorrow-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 20, 21, 0)
	})

	homeBody := renderPath(t, server, "/")
	assertInOrder(t, homeBody, []string{
		"Tonight",
		"20 Apr - 1 show",
		"Start Only Today",
	})
	assertNotContains(t, homeBody, "Start Only Yesterday")

	filteredBody := renderPath(t, server, "/events?window=today")
	assertInOrder(t, filteredBody, []string{
		"Tonight",
		"20 Apr - 1 show",
		"Start Only Today",
	})
	assertNotContains(t, filteredBody, "Start Only Yesterday")

	venueBody := renderPath(t, server, "/venues/leadmill")
	assertInOrder(t, venueBody, []string{
		"Today",
		"20 Apr - 1 show",
		"Start Only Today",
	})
	assertNotContains(t, venueBody, "Start Only Yesterday")
}

func TestEventsDefaultOmitsTodayWhenNoCurrentOrUpcomingShowsRemain(t *testing.T) {
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
				Slug:        "ended-show",
				Name:        "Ended Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 19, 18, 0),
				End:         fixtureLocalTime(2026, time.April, 19, 20, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Ended today.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/ended-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
			{
				Slug:        "tomorrow-show",
				Name:        "Tomorrow Show",
				VenueSlug:   "leadmill",
				Start:       fixtureLocalTime(2026, time.April, 20, 19, 0),
				End:         fixtureLocalTime(2026, time.April, 20, 21, 0),
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Tomorrow.",
				SourceName:  "Leadmill listings",
				SourceURL:   "https://example.test/tomorrow-show",
				LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			},
		},
	))
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 21, 0)
	})

	body := renderPath(t, server, "/events")

	assertInOrder(t, body, []string{
		"Tomorrow",
		"20 Apr - 1 show",
		"Tomorrow Show",
	})
	assertNotContains(t, body, "Tonight")
	assertNotContains(t, body, "Ended Show")
	assertNotContains(t, body, "No shows listed")
}

func TestEventsDefaultShowsEmptyState(t *testing.T) {
	server := mustClockedServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "Venue",
			Website:       "https://example.test/leadmill",
		}},
		[]domain.Event{},
	))

	body := renderPath(t, server, "/events")

	assertContains(t, body, "No upcoming shows listed.")
	assertNotContains(t, body, "No shows match these filters.")
}

func TestEventsFiltersTonight(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=tonight")

	assertContains(t, body, "Tonight Leadmill")
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Friday Leadmill")
}

func TestEventsFiltersWeekAndVenue(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=week&venue=leadmill")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Friday Leadmill")
	assertNotContains(t, body, "Tomorrow Yellow Arch")
	assertNotContains(t, body, "Later Leadmill")
}

func TestEventsGroupsByLocalDateInOrder(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?window=week")

	assertInOrder(t, body, []string{
		"Tonight",
		"19 Apr - 1 show",
		"Tonight Leadmill",
		"Tomorrow",
		"20 Apr - 1 show",
		"Tomorrow Yellow Arch",
		"Friday",
		"24 Apr - 1 show",
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
	assertNotContains(t, areaBody, "Tonight Leadmill")
}

func TestEventsUnknownVenueBehavesLikeAllVenues(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/events?venue=missing")

	assertContains(t, body, "Tonight Leadmill")
	assertContains(t, body, "Tomorrow Yellow Arch")
	assertContains(t, body, "Friday Leadmill")
	assertContains(t, body, "Later Leadmill")
	assertNotContains(t, body, "No shows match these filters.")
}

func TestPublicListsHideWithheldEvents(t *testing.T) {
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
				Slug:             "visible-show",
				Name:             "Visible Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "Visible description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/visible-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateReviewed,
			},
			{
				Slug:             "withheld-show",
				Name:             "Withheld Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
				Genre:            "Rock",
				Status:           "Listed",
				Description:      "Withheld description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/withheld-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateWithheld,
			},
		},
	))

	for _, path := range []string{"/", "/events"} {
		body := renderPath(t, server, path)
		assertContains(t, body, "Visible Show")
		assertNotContains(t, body, "Withheld Show")
	}
}

func TestPublicVenueDetailHidesWithheldEvents(t *testing.T) {
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
				Slug:             "visible-show",
				Name:             "Visible Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "Visible description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/visible-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateReviewed,
			},
			{
				Slug:             "withheld-show",
				Name:             "Withheld Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
				Genre:            "Rock",
				Status:           "Listed",
				Description:      "Withheld description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/withheld-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateWithheld,
			},
		},
	))

	body := renderPath(t, server, "/venues/leadmill")
	assertContains(t, body, "Visible Show")
	assertNotContains(t, body, "Withheld Show")
}

func TestPublicWithheldEventSlugWithoutAliasReturns404(t *testing.T) {
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
			Slug:             "withheld-show",
			Name:             "Withheld Show",
			VenueSlug:        "leadmill",
			Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
			End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
			Genre:            "Rock",
			Status:           "Listed",
			Description:      "Withheld description.",
			SourceName:       "Fixture listings",
			SourceURL:        "https://example.test/withheld-show",
			LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
			Origin:           domain.OriginLive,
			PublicationState: domain.PublicationStateWithheld,
		}},
	))

	rr := requestPath(t, server, http.MethodGet, "/events/withheld-show", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	assertContains(t, rr.Body.String(), "404 page not found")
}

func TestPublicMissingEventSlugAliasRedirectsToNonWithheldTarget(t *testing.T) {
	server := mustClockedServer(t, aliasResolverStore{
		Store: store.NewStore(
			[]domain.Venue{{
				Slug:          "leadmill",
				Name:          "The Leadmill",
				Address:       "6 Leadmill Road, Sheffield",
				Neighbourhood: "City Centre",
				Description:   "Venue",
				Website:       "https://example.test/leadmill",
			}},
			[]domain.Event{{
				Slug:             "visible-show",
				Name:             "Visible Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "Visible description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/visible-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateReviewed,
			}},
		),
		aliases: map[string]string{
			"old-show": "visible-show",
		},
	})

	rr := requestPath(t, server, http.MethodGet, "/events/old-show", nil)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
	if got, want := rr.Header().Get("Location"), "/events/visible-show"; got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
}

func TestPublicWithheldEventSlugAliasRedirectsToNonWithheldTarget(t *testing.T) {
	server := mustClockedServer(t, aliasResolverStore{
		Store: store.NewStore(
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
					Slug:             "visible-show",
					Name:             "Visible Show",
					VenueSlug:        "leadmill",
					Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
					End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
					Genre:            "Indie",
					Status:           "Listed",
					Description:      "Visible description.",
					SourceName:       "Fixture listings",
					SourceURL:        "https://example.test/visible-show",
					LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
					Origin:           domain.OriginLive,
					PublicationState: domain.PublicationStateReviewed,
				},
				{
					Slug:             "withheld-show",
					Name:             "Withheld Show",
					VenueSlug:        "leadmill",
					Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
					End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
					Genre:            "Rock",
					Status:           "Listed",
					Description:      "Withheld description.",
					SourceName:       "Fixture listings",
					SourceURL:        "https://example.test/withheld-show",
					LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
					Origin:           domain.OriginLive,
					PublicationState: domain.PublicationStateWithheld,
				},
			},
		),
		aliases: map[string]string{
			"withheld-show": "visible-show",
		},
	})

	rr := requestPath(t, server, http.MethodGet, "/events/withheld-show", nil)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusSeeOther)
	}
	if got, want := rr.Header().Get("Location"), "/events/visible-show"; got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
}

func TestPublicAliasToWithheldTargetReturns404(t *testing.T) {
	server := mustClockedServer(t, aliasResolverStore{
		Store: store.NewStore(
			[]domain.Venue{{
				Slug:          "leadmill",
				Name:          "The Leadmill",
				Address:       "6 Leadmill Road, Sheffield",
				Neighbourhood: "City Centre",
				Description:   "Venue",
				Website:       "https://example.test/leadmill",
			}},
			[]domain.Event{{
				Slug:             "withheld-target",
				Name:             "Withheld Target",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
				Genre:            "Rock",
				Status:           "Listed",
				Description:      "Withheld description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/withheld-target",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateWithheld,
			}},
		),
		aliases: map[string]string{
			"old-show": "withheld-target",
		},
	})

	rr := requestPath(t, server, http.MethodGet, "/events/old-show", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	assertContains(t, rr.Body.String(), "404 page not found")
}

func TestAdminEventDetailRendersWithheldEventForAuthenticatedAdmin(t *testing.T) {
	server, err := NewServer(testAdminAuthDeps(aliasResolverStore{
		Store: store.NewStore(
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
					Slug:             "visible-show",
					Name:             "Visible Show",
					VenueSlug:        "leadmill",
					Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
					End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
					Genre:            "Indie",
					Status:           "Listed",
					Description:      "Visible description.",
					SourceName:       "Fixture listings",
					SourceURL:        "https://example.test/visible-show",
					LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
					Origin:           domain.OriginLive,
					PublicationState: domain.PublicationStateReviewed,
				},
				{
					Slug:             "withheld-show",
					Name:             "Withheld Show",
					VenueSlug:        "leadmill",
					Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
					End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
					Genre:            "Rock",
					Status:           "Listed",
					Description:      "Withheld description.",
					SourceName:       "Fixture listings",
					SourceURL:        "https://example.test/withheld-show",
					LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
					Origin:           domain.OriginLive,
					PublicationState: domain.PublicationStateWithheld,
				},
			},
		),
		aliases: map[string]string{
			"withheld-show": "visible-show",
		},
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 10, 0)
	})

	cookie, _ := loginAdmin(t, server, "/admin/events/withheld-show")
	rr := requestPath(t, server, http.MethodGet, "/admin/events/withheld-show", nil, cookie)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	assertContains(t, rr.Body.String(), "Withheld Show")
	assertContains(t, rr.Body.String(), "The Leadmill")
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
		{name: "events", path: "/events?window=today", cardClass: `class="event-card has-image"`},
		{name: "venue", path: "/venues/leadmill", cardClass: `class="event-card venue-event-card has-image"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := renderPath(t, server, tc.path)
			assertContains(t, body, tc.cardClass)
			assertContains(t, body, `<img class="event-card-image" src="/media/events/poster.jpg" alt="Poster Show artwork" style="--image-focus-x: 35%; --image-focus-y: 65%;" loading="lazy" decoding="async">`)
		})
	}
}

func TestEventCardsRenderMissingImagePlaceholder(t *testing.T) {
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
			Slug:        "no-poster-show",
			Name:        "No Poster Show",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 20, 0),
			End:         fixtureLocalTime(2026, time.April, 19, 22, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "No poster description.",
			SourceName:  "Fixture listings",
			SourceURL:   "https://example.test/no-poster-show",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
			Origin:      domain.OriginLive,
		}},
	))

	body := renderPath(t, server, "/events?window=today")

	assertContains(t, body, `class="event-card missing-image"`)
	assertContains(t, body, `<span class="event-card-media event-card-placeholder" aria-hidden="true"></span>`)
	assertNotContains(t, body, "No image")
}

func TestEventCardsShowOnlyPublicUnconfirmedStatus(t *testing.T) {
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
				Slug:             "listed-show",
				Name:             "Listed Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 20, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 22, 0),
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "Listed description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/listed-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateReviewed,
			},
			{
				Slug:             "provisional-show",
				Name:             "Provisional Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 21, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 23, 0),
				Genre:            "Rock",
				Status:           "Listed",
				Description:      "Provisional description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/provisional-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateProvisional,
			},
			{
				Slug:             "cancelled-provisional-show",
				Name:             "Cancelled Provisional Show",
				VenueSlug:        "leadmill",
				Start:            fixtureLocalTime(2026, time.April, 19, 22, 0),
				End:              fixtureLocalTime(2026, time.April, 19, 23, 30),
				Genre:            "Rock",
				Status:           "Cancelled",
				Description:      "Cancelled provisional description.",
				SourceName:       "Fixture listings",
				SourceURL:        "https://example.test/cancelled-provisional-show",
				LastChecked:      fixtureLocalTime(2026, time.April, 19, 9, 0),
				Origin:           domain.OriginLive,
				PublicationState: domain.PublicationStateProvisional,
			},
		},
	))

	body := renderPath(t, server, "/events?window=today")

	assertContains(t, body, `<span class="event-status">Unconfirmed</span>`)
	assertContains(t, body, `<span class="event-status">Cancelled · Unconfirmed</span>`)
	assertNotContains(t, body, `>Listed<`)
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

func TestVenueDetailEventCardsShowDates(t *testing.T) {
	server := mustFixtureServer(t)
	body := renderPath(t, server, "/venues/leadmill")

	assertInOrder(t, body, []string{
		"Today",
		"19 Apr - 1 show",
		"20:00",
		"Friday",
		"24 Apr - 1 show",
		"21:00",
		"Monday",
		"27 Apr - 1 show",
		"20:00",
	})
	assertContains(t, body, `data-venue-timeline`)
	assertContains(t, body, `class="venue-timeline-item venue-timeline-day-start venue-day-tone-0" data-venue-day="0"`)
	assertContains(t, body, `class="venue-timeline-item venue-timeline-day-start venue-day-tone-1" data-venue-day="1"`)
	assertNotContains(t, body, "Past Leadmill")
	assertNotContains(t, body, "Nothing Listed")
	assertNotContains(t, body, "19 Apr 2026 · 20:00")
	assertNotContains(t, body, `class="event-venue"`)
}

func TestVenueDetailShowsOngoingPriorDayEventsInTodaySection(t *testing.T) {
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
			Slug:        "overnight-leadmill",
			Name:        "Overnight Leadmill",
			VenueSlug:   "leadmill",
			Start:       fixtureLocalTime(2026, time.April, 19, 23, 0),
			End:         fixtureLocalTime(2026, time.April, 20, 2, 0),
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Still running after midnight.",
			SourceName:  "Leadmill listings",
			SourceURL:   "https://example.test/overnight-leadmill",
			LastChecked: fixtureLocalTime(2026, time.April, 19, 9, 0),
		}},
	))
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 20, 0, 30)
	})

	body := renderPath(t, server, "/venues/leadmill")

	assertInOrder(t, body, []string{
		"Today",
		"20 Apr - 1 show",
		"23:00",
		"Overnight Leadmill",
	})
	assertNotContains(t, body, "19 Apr - 1 show")
	assertNotContains(t, body, "No upcoming shows listed for this venue.")
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
	body := renderPath(t, server, "/")

	assertContains(t, body, `<meta name="description" content="Browse Sheffield live music by date and venue.">`)
	assertContains(t, body, `<a class="skip-link" href="#main">Skip to content</a>`)
	assertContains(t, body, `<main id="main" class="shell main">`)
	assertContains(t, body, `<a class="active" aria-current="page" href="/">Events</a>`)
	assertNotContains(t, body, `href="/">Home</a>`)
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

func seedImportRunEventReviewClusters(t *testing.T, path string, runID int64) (int64, int64) {
	t.Helper()

	db := mustRawDB(t, path)
	defer db.Close()

	sourceID := mustInsertAdminSource(t, db, "Event review source", "https://example.test/event-review")
	mustInsertAdminVenue(t, db, domain.Venue{
		Slug:            "event-review-hall",
		Name:            "Event Review Venue",
		ValidationState: domain.ValidationStateValidated,
		Origin:          domain.OriginLive,
	})
	mustInsertAdminEvent(t, db, sourceID, "import-run-event-review-open", "event-review-hall", "Import Run Event Review Open", fixtureLocalTime(2026, time.April, 20, 19, 30), fixtureLocalTime(2026, time.April, 20, 21, 30), "Open event review fixture.")
	mustInsertAdminEvent(t, db, sourceID, "import-run-event-review-resolved", "event-review-hall", "Import Run Event Review Resolved", fixtureLocalTime(2026, time.April, 20, 20, 30), fixtureLocalTime(2026, time.April, 20, 22, 30), "Resolved event review fixture.")
	mustInsertAdminEvent(t, db, sourceID, "import-run-event-review-other", "event-review-hall", "Import Run Event Review Other", fixtureLocalTime(2026, time.April, 20, 18, 30), fixtureLocalTime(2026, time.April, 20, 19, 15), "Other run fixture.")

	openEventID := mustWebEventIDBySlug(t, db, "import-run-event-review-open")
	resolvedEventID := mustWebEventIDBySlug(t, db, "import-run-event-review-resolved")
	otherEventID := mustWebEventIDBySlug(t, db, "import-run-event-review-other")

	openClusterID := mustInsertWebEventReviewCluster(t, db, "open", openEventID, "historical_duplicate", "supporting_clean_title", "2026-04-20T10:02:00Z")
	resolvedClusterID := mustInsertWebEventReviewCluster(t, db, "resolved", resolvedEventID, "historical_duplicate", "supporting_clean_title", "2026-04-20T10:03:00Z")
	otherRunID := runID + 1
	if _, err := db.Exec(`INSERT INTO import_runs (id, started_at, finished_at, status, notes) VALUES (?, ?, ?, ?, ?)`, otherRunID, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "other import run"); err != nil {
		t.Fatalf("insert other import run: %v", err)
	}
	otherClusterID := mustInsertWebEventReviewCluster(t, db, "open", otherEventID, "historical_duplicate", "supporting_clean_title", "2026-04-20T10:04:00Z")

	openEvidenceID := mustInsertWebEventReviewEvidence(t, db, sourceID, openEventID, "import-run-open-fingerprint", `{"payload":"open"}`)
	mustInsertWebEventReviewClusterEvidence(t, db, openClusterID, openEvidenceID, true, "2026-04-20T10:02:30Z", "", "open evidence")
	resolvedEvidenceID := mustInsertWebEventReviewEvidence(t, db, sourceID, resolvedEventID, "import-run-resolved-fingerprint", `{"payload":"resolved"}`)
	mustInsertWebEventReviewClusterEvidence(t, db, resolvedClusterID, resolvedEvidenceID, true, "2026-04-20T10:03:30Z", "", "resolved evidence")
	otherEvidenceID := mustInsertWebEventReviewEvidence(t, db, sourceID, otherEventID, "import-run-other-fingerprint", `{"payload":"other"}`)
	mustInsertWebEventReviewClusterEvidence(t, db, otherClusterID, otherEvidenceID, true, "2026-04-20T10:04:30Z", "", "other evidence")

	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, runID, openClusterID, "2026-04-20T10:02:00Z"); err != nil {
		t.Fatalf("link open cluster: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, runID, resolvedClusterID, "2026-04-20T10:03:00Z"); err != nil {
		t.Fatalf("link resolved cluster: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, otherRunID, otherClusterID, "2026-04-20T10:04:00Z"); err != nil {
		t.Fatalf("link other cluster: %v", err)
	}

	mustInsertWebEventReviewResolution(t, db, resolvedClusterID, "resolved", `{"repair_run_id":77,"applied_live_actions":[{"event_id":`+strconvFormatInt(resolvedEventID)+`,"event_slug":"import-run-event-review-resolved","action":"keep_separate","reason":"kept"},{"event_id":`+strconvFormatInt(openEventID)+`,"event_slug":"import-run-event-review-open","action":"withhold_duplicate","reason":"withhold"}]}`, "")

	return openClusterID, resolvedClusterID
}

func mustWebEventIDBySlug(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var id int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, slug).Scan(&id); err != nil {
		t.Fatalf("lookup event %q: %v", slug, err)
	}
	return id
}

func mustInsertWebEventReviewCluster(t *testing.T, db *sql.DB, status string, canonicalEventID int64, conflictType, conflictReason, linkedAt string) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_clusters (
			status,
			version,
			staging_key,
			staging_key_version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		) VALUES (?, 1, NULL, 0, NULL, NULL, ?, ?, ?, ?, ?)
	`, status, canonicalEventID, conflictType, conflictReason, linkedAt, linkedAt)
	if err != nil {
		t.Fatalf("insert event review cluster: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("cluster last insert id: %v", err)
	}
	return id
}

func mustInsertWebEventReviewEvidence(t *testing.T, db *sql.DB, sourceID, eventID int64, fingerprint, payload string) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_evidence (
			source_id,
			event_id,
			evidence_fingerprint,
			fingerprint_version,
			payload,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, sourceID, eventID, fingerprint, 1, payload, "2026-04-20T10:00:00Z", "2026-04-20T10:00:00Z")
	if err != nil {
		t.Fatalf("insert event review evidence: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("evidence last insert id: %v", err)
	}
	return id
}

func mustInsertWebEventReviewClusterEvidence(t *testing.T, db *sql.DB, clusterID, evidenceID int64, active bool, linkedAt, unlinkedAt, reason string) int64 {
	t.Helper()

	var unlinkedAtValue any
	if unlinkedAt != "" {
		unlinkedAtValue = unlinkedAt
	}
	activeValue := 0
	if active {
		activeValue = 1
	}
	res, err := db.Exec(`
		INSERT INTO event_review_cluster_evidence (
			cluster_id,
			evidence_id,
			active,
			linked_at,
			unlinked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, evidenceID, activeValue, linkedAt, unlinkedAtValue, reason)
	if err != nil {
		t.Fatalf("insert cluster evidence: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("cluster evidence last insert id: %v", err)
	}
	return id
}

func mustInsertWebEventReviewResolution(t *testing.T, db *sql.DB, clusterID int64, status, snapshot, discardReason string) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, status, snapshot, discardReason, "2026-04-20T10:05:00Z", "2026-04-20T10:05:00Z")
	if err != nil {
		t.Fatalf("insert event review resolution: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("resolution last insert id: %v", err)
	}
	return id
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

func seedAdminRoomFixture(t *testing.T, path, venueSlug, roomSlug, roomName string, sortOrder int) {
	t.Helper()

	db := mustRawDB(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close raw db: %v", err)
		}
	}()

	venueID := lookupWebVenueID(t, db, venueSlug)
	if _, err := db.Exec(`
		INSERT INTO venue_rooms (venue_id, slug, name, sort_order, validation_state, origin)
		VALUES (?, ?, ?, ?, ?, ?)
	`, venueID, roomSlug, roomName, sortOrder, string(domain.ValidationStateProvisional), string(domain.OriginLive)); err != nil {
		t.Fatalf("insert room %q: %v", roomSlug, err)
	}
}

func lookupWebVenueID(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, slug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id %q: %v", slug, err)
	}
	return venueID
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

type adminReviewEventReviewStoreStub struct {
	readOnlyStoreStub
	clusters                    []store.EventReviewClusterSummary
	closedClusters              []store.EventReviewClusterHistorySummary
	detail                      store.EventReviewClusterDetail
	sourceIdentityChoicesCalled bool
	sourceIdentityChoicesInput  store.SetEventReviewSourceIdentityChoicesInput
	sourceIdentityChoicesErr    error
	discardCalled               bool
	discardInput                store.EventReviewDiscardInput
	discardErr                  error
	resolveCalled               bool
	resolveInput                store.EventReviewResolutionInput
	resolveErr                  error
	supersedeCalled             bool
	supersedeInput              store.EventReviewSupersedeInput
	supersedeErr                error
}

func (s adminReviewEventReviewStoreStub) ListOpenEventReviewClusters(context.Context) ([]store.EventReviewClusterSummary, error) {
	return s.clusters, nil
}

func (s adminReviewEventReviewStoreStub) ListClosedEventReviewClusters(context.Context, int) ([]store.EventReviewClusterHistorySummary, error) {
	return s.closedClusters, nil
}

func (s adminReviewEventReviewStoreStub) LoadEventReviewCluster(_ context.Context, id int64) (store.EventReviewClusterDetail, bool, error) {
	if s.detail.Summary.ID != id {
		return store.EventReviewClusterDetail{}, false, nil
	}
	return s.detail, true, nil
}

func (s *adminReviewEventReviewStoreStub) SetEventReviewSourceIdentityChoices(_ context.Context, input store.SetEventReviewSourceIdentityChoicesInput) error {
	s.sourceIdentityChoicesCalled = true
	s.sourceIdentityChoicesInput = input
	return s.sourceIdentityChoicesErr
}

func (s *adminReviewEventReviewStoreStub) DiscardEventReviewCluster(_ context.Context, input store.EventReviewDiscardInput) error {
	s.discardCalled = true
	s.discardInput = input
	return s.discardErr
}

func (s *adminReviewEventReviewStoreStub) ResolveEventReviewCluster(_ context.Context, input store.EventReviewResolutionInput) error {
	s.resolveCalled = true
	s.resolveInput = input
	return s.resolveErr
}

func (s *adminReviewEventReviewStoreStub) SupersedeEventReviewCluster(_ context.Context, input store.EventReviewSupersedeInput) error {
	s.supersedeCalled = true
	s.supersedeInput = input
	return s.supersedeErr
}

type eventReviewOnlyStoreStub struct {
	readOnlyStoreStub
	clusters                    []store.EventReviewClusterSummary
	closedClusters              []store.EventReviewClusterHistorySummary
	detail                      store.EventReviewClusterDetail
	sourceIdentityChoicesCalled bool
	sourceIdentityChoicesInput  store.SetEventReviewSourceIdentityChoicesInput
	sourceIdentityChoicesErr    error
	discardCalled               bool
	discardInput                store.EventReviewDiscardInput
	discardErr                  error
	resolveCalled               bool
	resolveInput                store.EventReviewResolutionInput
	resolveErr                  error
	supersedeCalled             bool
	supersedeInput              store.EventReviewSupersedeInput
	supersedeErr                error
}

func (s eventReviewOnlyStoreStub) ListOpenEventReviewClusters(context.Context) ([]store.EventReviewClusterSummary, error) {
	return s.clusters, nil
}

func (s eventReviewOnlyStoreStub) ListClosedEventReviewClusters(context.Context, int) ([]store.EventReviewClusterHistorySummary, error) {
	return s.closedClusters, nil
}

func (s eventReviewOnlyStoreStub) LoadEventReviewCluster(_ context.Context, id int64) (store.EventReviewClusterDetail, bool, error) {
	if s.detail.Summary.ID != id {
		return store.EventReviewClusterDetail{}, false, nil
	}
	return s.detail, true, nil
}

func (s *eventReviewOnlyStoreStub) DiscardEventReviewCluster(_ context.Context, input store.EventReviewDiscardInput) error {
	s.discardCalled = true
	s.discardInput = input
	return s.discardErr
}

func (s *eventReviewOnlyStoreStub) SetEventReviewSourceIdentityChoices(_ context.Context, input store.SetEventReviewSourceIdentityChoicesInput) error {
	s.sourceIdentityChoicesCalled = true
	s.sourceIdentityChoicesInput = input
	if s.sourceIdentityChoicesErr != nil {
		return s.sourceIdentityChoicesErr
	}
	if s.detail.Summary.ID != input.ClusterID {
		return nil
	}
	s.detail.Summary.Version++
	if s.detail.ImportReadiness == nil {
		return nil
	}
	updatedAt := fixtureLocalTime(2026, time.May, 15, 10, 55)
	choiceByKey := make(map[eventReviewSourceIdentityChoiceKey]store.EventReviewImportCandidateSourceIdentityStatus, len(input.Choices))
	for _, choice := range input.Choices {
		key := eventReviewSourceIdentityChoiceKey{sourceID: choice.SourceID, sourceIdentityKey: strings.TrimSpace(choice.SourceIdentityKey)}
		choiceByKey[key] = store.EventReviewImportCandidateSourceIdentityStatus{
			SourceID:          choice.SourceID,
			SourceIdentityKey: key.sourceIdentityKey,
			ChoiceSelected:    choice.Selected,
			ChoiceReason:      strings.TrimSpace(choice.SelectionReason),
			ChoiceUpdatedAt:   &updatedAt,
		}
	}
	for i := range s.detail.ImportReadiness.CandidateIdentityStatuses {
		for j := range s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys {
			sourceKey := strings.TrimSpace(s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys[j].SourceIdentityKey)
			key := eventReviewSourceIdentityChoiceKey{
				sourceID:          s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys[j].SourceID,
				sourceIdentityKey: sourceKey,
			}
			if choice, ok := choiceByKey[key]; ok {
				s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys[j].ChoiceSelected = choice.ChoiceSelected
				s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys[j].ChoiceReason = choice.ChoiceReason
				s.detail.ImportReadiness.CandidateIdentityStatuses[i].SourceKeys[j].ChoiceUpdatedAt = choice.ChoiceUpdatedAt
			}
		}
	}
	return nil
}

func (s *eventReviewOnlyStoreStub) ResolveEventReviewCluster(_ context.Context, input store.EventReviewResolutionInput) error {
	s.resolveCalled = true
	s.resolveInput = input
	return s.resolveErr
}

func (s *eventReviewOnlyStoreStub) SupersedeEventReviewCluster(_ context.Context, input store.EventReviewSupersedeInput) error {
	s.supersedeCalled = true
	s.supersedeInput = input
	return s.supersedeErr
}

type provisionalVenueReadOnlyReviewStoreStub struct {
	readOnlyStoreStub
}

func (provisionalVenueReadOnlyReviewStoreStub) ListImportRuns(context.Context, int) ([]ingest.ImportRunSummary, error) {
	return nil, nil
}

func (provisionalVenueReadOnlyReviewStoreStub) LatestSuccessfulImport(context.Context) (*ingest.ImportRunSummary, error) {
	return nil, nil
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

type importHistoryWithDetailEventReviewErrorStoreStub struct {
	importHistoryOnlyStoreStub
}

type importHistoryWithEventReviewRowsStoreStub struct {
	runs     []ingest.ImportRunSummary
	clusters map[int64][]store.EventReviewClusterSummary
}

type importHistoryWithEventReviewErrorStoreStub struct {
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

func (importHistoryWithDetailEventReviewErrorStoreStub) ListEventReviewClustersForImportRun(context.Context, int64) ([]store.EventReviewClusterSummary, error) {
	return nil, fmt.Errorf("event review clusters failed")
}

func (importHistoryWithDetailEventReviewErrorStoreStub) LoadImportRun(context.Context, int64) (ingest.ReplayRun, error) {
	return ingest.ReplayRun{
		ID:        1,
		StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:    "succeeded",
		Notes:     "fixture",
	}, nil
}

func (s importHistoryWithEventReviewRowsStoreStub) ListImportRuns(context.Context, int) ([]ingest.ImportRunSummary, error) {
	return append([]ingest.ImportRunSummary(nil), s.runs...), nil
}

func (s importHistoryWithEventReviewRowsStoreStub) LatestSuccessfulImport(context.Context) (*ingest.ImportRunSummary, error) {
	if len(s.runs) == 0 {
		return nil, nil
	}
	run := s.runs[0]
	return &run, nil
}

func (s importHistoryWithEventReviewRowsStoreStub) LoadImportRun(_ context.Context, id int64) (ingest.ReplayRun, error) {
	for _, run := range s.runs {
		if run.ID == id {
			return ingest.ReplayRun{
				ID:        run.ID,
				StartedAt: run.StartedAt,
				Status:    run.Status,
				Notes:     run.Notes,
			}, nil
		}
	}
	return ingest.ReplayRun{
		ID:        id,
		StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:    "succeeded",
		Notes:     "fixture",
	}, nil
}

func (s importHistoryWithEventReviewRowsStoreStub) ListEventReviewClustersForImportRun(_ context.Context, importRunID int64) ([]store.EventReviewClusterSummary, error) {
	return s.clusters[importRunID], nil
}

func (importHistoryWithEventReviewErrorStoreStub) ListEventReviewClustersForImportRun(context.Context, int64) ([]store.EventReviewClusterSummary, error) {
	return nil, fmt.Errorf("event review clusters failed")
}

func (importHistoryWithEventReviewErrorStoreStub) LoadImportRun(context.Context, int64) (ingest.ReplayRun, error) {
	return ingest.ReplayRun{
		ID:        1,
		StartedAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
		Status:    "succeeded",
		Notes:     "fixture",
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

func mustClockedServer(t *testing.T, st store.CatalogStore) *Server {
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
	if importRunStore, ok := value.(ingest.ImportRunStore); ok {
		deps.ImportRunStore = importRunStore
	}
	if replayStore, ok := value.(ingest.ReplayStore); ok {
		deps.ReplayStore = replayStore
	}
	if importRunEventReviewClusterStore, ok := value.(ImportRunEventReviewClusterStore); ok {
		deps.ImportRunEventReviewClusterStore = importRunEventReviewClusterStore
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

func requestPath(t *testing.T, server http.Handler, method, path string, body io.Reader, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, body)
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	return rr
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

type aliasResolverStore struct {
	*store.Store
	aliases map[string]string
}

func (s aliasResolverStore) ResolveEventSlugAlias(_ context.Context, aliasSlug string) (string, bool, error) {
	if s.aliases == nil {
		return "", false, nil
	}
	targetSlug, ok := s.aliases[strings.TrimSpace(aliasSlug)]
	if !ok {
		return "", false, nil
	}
	return targetSlug, true, nil
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

func int64Ptr(v int64) *int64 {
	return &v
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
