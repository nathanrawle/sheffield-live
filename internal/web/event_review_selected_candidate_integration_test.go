package web

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
	sqlitestore "sheffield-live/internal/store/sqlite"
)

type selectedCandidateEventReviewFixture struct {
	path                 string
	clusterID            int64
	sourceID             int64
	selectedEvidenceID   int64
	unselectedEvidenceID int64
	selectedSourceKey    string
	unselectedSourceKey  string
	selectedExternalID   string
	selectedSourceURL    string
	selectedCalendarURL  string
}

func TestAdminEventReviewSelectedCandidateFlowPersistsOnlySelectedSourceLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	fixture := mustSeedSelectedCandidateEventReviewFixture(t, path)

	server, err := NewServer(testAdminAuthDeps(st))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cookie, _ := loginAdmin(t, server, "/admin/review")

	detailPath := "/admin/event-review/" + strconvFormatInt(fixture.clusterID)
	getRR := requestPath(t, server, http.MethodGet, detailPath, nil, cookie)
	if getRR.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	body := getRR.Body.String()
	assertContains(t, body, "Save source identity choices")
	assertContains(t, body, `name="action" value="save_source_identity_choices"`)
	csrfToken := extractCSRFToken(t, body)

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "1")
	form.Set("action", "save_source_identity_choices")
	form.Add("source_identity_choice", strconvFormatInt(fixture.sourceID)+"|"+fixture.selectedSourceKey)
	req := httptest.NewRequest(http.MethodPost, detailPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != detailPath+"?source_identity_choices_saved=1" {
		t.Fatalf("Location = %q, want detail redirect", location)
	}

	db := mustRawDB(t, path)
	assertEventReviewSourceIdentityChoiceRowState(t, db, fixture.clusterID, fixture.sourceID, fixture.selectedSourceKey, true, "admin source identity choice")
	assertEventReviewSourceIdentityChoiceRowState(t, db, fixture.clusterID, fixture.sourceID, fixture.unselectedSourceKey, false, "admin source identity choice cleared")
	if got := mustCountRows(t, db, "event_review_source_identity_choices"); got != 2 {
		t.Fatalf("event_review_source_identity_choices rows = %d, want 2", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	getRR = requestPath(t, server, http.MethodGet, detailPath, nil, cookie)
	if getRR.Code != http.StatusOK {
		t.Fatalf("post-save detail status = %d, want %d", getRR.Code, http.StatusOK)
	}
	body = getRR.Body.String()
	assertContains(t, body, "Accept selected candidate")
	assertContains(t, body, `name="action" value="resolve_import_selected_candidate"`)
	csrfToken = extractCSRFToken(t, body)

	form = url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("expected_version", "2")
	form.Set("action", "resolve_import_selected_candidate")
	req = httptest.NewRequest(http.MethodPost, detailPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rr = httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body %q", rr.Code, http.StatusSeeOther, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/admin/review?event_review_resolved=1" {
		t.Fatalf("Location = %q, want resolve redirect", location)
	}

	db = mustRawDB(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close raw db: %v", err)
		}
	}()

	var status string
	var version int
	var canonicalEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT status, version, canonical_event_id
		FROM event_review_clusters
		WHERE id = ?
	`, fixture.clusterID).Scan(&status, &version, &canonicalEventID); err != nil {
		t.Fatalf("load resolved cluster: %v", err)
	}
	if status != string(seedstore.EventReviewClusterStatusResolved) {
		t.Fatalf("cluster status = %q, want %q", status, seedstore.EventReviewClusterStatusResolved)
	}
	if version != 3 {
		t.Fatalf("cluster version = %d, want 3", version)
	}
	if !canonicalEventID.Valid || canonicalEventID.Int64 <= 0 {
		t.Fatalf("canonical event id = %v, want positive", canonicalEventID)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved cluster: %v", err)
	}
	if !ok {
		t.Fatal("load resolved cluster ok = false")
	}
	if detail.Resolution == nil || detail.Resolution.AppliedImportListing == nil {
		t.Fatal("resolved cluster missing applied import listing")
	}
	if got := detail.Resolution.AppliedImportListing.EvidenceID; got != fixture.selectedEvidenceID {
		t.Fatalf("applied import listing evidence id = %d, want %d", got, fixture.selectedEvidenceID)
	}
	if got := detail.Resolution.AppliedImportListing.EventID; got <= 0 || !canonicalEventID.Valid || got != canonicalEventID.Int64 {
		t.Fatalf("applied import listing event id = %d, want %d", got, canonicalEventID.Int64)
	}

	var selectedEvidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, fixture.selectedEvidenceID).Scan(&selectedEvidenceEventID); err != nil {
		t.Fatalf("load selected evidence event id: %v", err)
	}
	if !selectedEvidenceEventID.Valid || selectedEvidenceEventID.Int64 != canonicalEventID.Int64 {
		t.Fatalf("selected evidence event id = %v, want %d", selectedEvidenceEventID, canonicalEventID.Int64)
	}

	var unselectedEvidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, fixture.unselectedEvidenceID).Scan(&unselectedEvidenceEventID); err != nil {
		t.Fatalf("load unselected evidence event id: %v", err)
	}
	if unselectedEvidenceEventID.Valid {
		t.Fatalf("unselected evidence event id = %v, want NULL", unselectedEvidenceEventID)
	}

	assertEventSourceLinkCount(t, db, canonicalEventID.Int64, fixture.sourceID, fixture.selectedSourceKey, 1)
	assertEventSourceLinkCount(t, db, canonicalEventID.Int64, fixture.sourceID, fixture.unselectedSourceKey, 0)

	payloadIdentities := ingest.SourceIdentities(ingest.SourceIdentityInput{
		ExternalID:  fixture.selectedExternalID,
		SourceURL:   fixture.selectedSourceURL,
		CalendarURL: fixture.selectedCalendarURL,
	})
	for _, payloadKey := range payloadIdentities.Keys() {
		assertEventSourceLinkCount(t, db, canonicalEventID.Int64, fixture.sourceID, payloadKey, 0)
	}
	assertEventSourceLinkCountForEventSource(t, db, canonicalEventID.Int64, fixture.sourceID, 1)
}

func mustSeedSelectedCandidateEventReviewFixture(t *testing.T, path string) selectedCandidateEventReviewFixture {
	t.Helper()

	db := mustRawDB(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close seed db: %v", err)
		}
	}()

	sourceID := mustInsertSelectedCandidateSource(t, db)
	mustInsertSelectedCandidateVenue(t, db)

	selectedExternalID := "selected-external-id"
	selectedSourceURL := "https://source.example.test/listing"
	selectedCalendarURL := "https://calendar.example.test/listing.ics"

	clusterID := mustInsertImportReviewClusterWithNullCanonical(t, db)
	selectedEvidenceID := mustInsertImportReviewEvidenceWithNullEvent(
		t,
		db,
		sourceID,
		"selected-source-key-evidence",
		mustImportReviewPayload(t,
			"Fixture source",
			selectedSourceURL,
			selectedCalendarURL,
			"supporting",
			"Selected Candidate Show",
			"selected-candidate-hall",
			time.Date(2026, time.May, 15, 19, 0, 0, 0, time.UTC),
			time.Date(2026, time.May, 15, 21, 0, 0, 0, time.UTC),
			selectedExternalID,
		),
	)
	mustInsertActiveClusterEvidence(t, db, clusterID, selectedEvidenceID, "2026-05-15T10:03:00Z", "selected evidence")
	mustInsertEvidenceSourceIdentityKey(t, db, selectedEvidenceID, sourceID, "selected-source-key")

	unselectedEvidenceID := mustInsertImportReviewEvidenceWithNullEvent(
		t,
		db,
		sourceID,
		"unselected-source-key-evidence",
		mustImportReviewPayload(t,
			"Fixture source",
			"https://source.example.test/listing",
			"https://calendar.example.test/listing.ics",
			"supporting",
			"Unselected Candidate Show",
			"selected-candidate-hall",
			time.Date(2026, time.May, 15, 20, 0, 0, 0, time.UTC),
			time.Date(2026, time.May, 15, 22, 0, 0, 0, time.UTC),
			"unselected-external-id",
		),
	)
	mustInsertActiveClusterEvidence(t, db, clusterID, unselectedEvidenceID, "2026-05-15T10:04:00Z", "unselected evidence")
	mustInsertEvidenceSourceIdentityKey(t, db, unselectedEvidenceID, sourceID, "unselected-source-key")

	return selectedCandidateEventReviewFixture{
		path:                 path,
		clusterID:            clusterID,
		sourceID:             sourceID,
		selectedEvidenceID:   selectedEvidenceID,
		unselectedEvidenceID: unselectedEvidenceID,
		selectedSourceKey:    "selected-source-key",
		unselectedSourceKey:  "unselected-source-key",
		selectedExternalID:   selectedExternalID,
		selectedSourceURL:    selectedSourceURL,
		selectedCalendarURL:  selectedCalendarURL,
	}
}

func mustInsertSelectedCandidateSource(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO sources (name, url)
		VALUES (?, ?)
	`, "Fixture source", "https://source.example.test/listing")
	if err != nil {
		t.Fatalf("insert source: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("source last insert id: %v", err)
	}
	return id
}

func mustInsertSelectedCandidateVenue(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO venues (
			slug,
			name,
			address,
			neighbourhood,
			description,
			website,
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "selected-candidate-hall", "Selected Candidate Hall", "", "", "", "", string(domain.OriginLive))
	if err != nil {
		t.Fatalf("insert venue: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("venue last insert id: %v", err)
	}
	return id
}

func mustInsertImportReviewClusterWithNullCanonical(t *testing.T, db *sql.DB) int64 {
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
		) VALUES (?, 1, NULL, 0, NULL, NULL, NULL, ?, ?, ?, ?)
	`, string(seedstore.EventReviewClusterStatusOpen), seedstore.EventReviewConflictTypeImportReview, seedstore.EventReviewConflictReasonIngestCandidate, "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z")
	if err != nil {
		t.Fatalf("insert event review cluster: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("cluster last insert id: %v", err)
	}
	return id
}

func mustInsertImportReviewEvidenceWithNullEvent(t *testing.T, db *sql.DB, sourceID int64, fingerprint, payload string) int64 {
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
		) VALUES (?, NULL, ?, 1, ?, ?, ?)
	`, sourceID, fingerprint, payload, "2026-05-15T10:01:00Z", "2026-05-15T10:01:00Z")
	if err != nil {
		t.Fatalf("insert event review evidence: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("evidence last insert id: %v", err)
	}
	return id
}

func mustInsertActiveClusterEvidence(t *testing.T, db *sql.DB, clusterID, evidenceID int64, linkedAt, reason string) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_cluster_evidence (
			cluster_id,
			evidence_id,
			active,
			linked_at,
			unlinked_at,
			link_reason
		) VALUES (?, ?, 1, ?, NULL, ?)
	`, clusterID, evidenceID, linkedAt, reason)
	if err != nil {
		t.Fatalf("insert cluster evidence: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("cluster evidence last insert id: %v", err)
	}
	return id
}

func mustInsertEvidenceSourceIdentityKey(t *testing.T, db *sql.DB, evidenceID, sourceID int64, normalizedKey string) int64 {
	t.Helper()

	identityKeyHash := mustEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindSource, normalizedKey)
	res, err := db.Exec(`
		INSERT INTO event_review_identity_keys (
			identity_key_hash,
			key_kind,
			key_version,
			normalized_key,
			created_at
		) VALUES (?, ?, 1, ?, ?)
	`, identityKeyHash, string(seedstore.EventReviewIdentityKeyKindSource), normalizedKey, "2026-05-15T10:02:00Z")
	if err != nil {
		t.Fatalf("insert event review identity key: %v", err)
	}
	identityKeyID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("identity key last insert id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_review_evidence_identity_keys (
			evidence_id,
			identity_key_id,
			source_id,
			role
		) VALUES (?, ?, ?, ?)
	`, evidenceID, identityKeyID, sourceID, string(seedstore.EventReviewEvidenceIdentityKeyRoleObserved)); err != nil {
		t.Fatalf("insert evidence identity key: %v", err)
	}
	return identityKeyID
}

func mustImportReviewPayload(t *testing.T, sourceName, sourceURL, calendarURL, sourceAuthority, title, venueSlug string, startAt, endAt time.Time, externalID string) string {
	t.Helper()

	payload := map[string]any{
		"source_authority":      sourceAuthority,
		"source_name":           sourceName,
		"source_url":            sourceURL,
		"calendar_url":          calendarURL,
		"candidate_external_id": externalID,
		"candidate_title":       title,
		"candidate_venue_slug":  venueSlug,
		"candidate_start_at":    startAt.UTC().Format(time.RFC3339),
		"candidate_end_at":      endAt.UTC().Format(time.RFC3339),
		"candidate_description": "Fixture import review candidate.",
		"candidate_status":      "Listed",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal import review payload: %v", err)
	}
	return string(data)
}

func mustEventReviewIdentityKeyHash(kind seedstore.EventReviewIdentityKeyKind, normalizedKey string) string {
	material := fmt.Sprintf("event-review:v%d:%s:%s", 1, kind, strings.TrimSpace(normalizedKey))
	sum := sha256.Sum256([]byte(material))
	return "event-review:v1:" + hex.EncodeToString(sum[:])
}

func assertEventReviewSourceIdentityChoiceRowState(t *testing.T, db *sql.DB, clusterID, sourceID int64, sourceIdentityKey string, selected bool, selectionReason string) {
	t.Helper()

	var gotSelected int
	var gotReason string
	if err := db.QueryRow(`
		SELECT selected, selection_reason
		FROM event_review_source_identity_choices
		WHERE cluster_id = ? AND source_id = ? AND source_identity_key = ?
	`, clusterID, sourceID, sourceIdentityKey).Scan(&gotSelected, &gotReason); err != nil {
		t.Fatalf("load source identity choice %q: %v", sourceIdentityKey, err)
	}
	if gotSelected != boolToInt(selected) {
		t.Fatalf("source identity choice %q selected = %d, want %d", sourceIdentityKey, gotSelected, boolToInt(selected))
	}
	if gotReason != selectionReason {
		t.Fatalf("source identity choice %q reason = %q, want %q", sourceIdentityKey, gotReason, selectionReason)
	}
}

func assertEventSourceLinkCount(t *testing.T, db *sql.DB, eventID, sourceID int64, sourceEventKey string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE event_id = ? AND source_id = ? AND source_event_key = ?
	`, eventID, sourceID, sourceEventKey).Scan(&got); err != nil {
		t.Fatalf("load event source link count for %q: %v", sourceEventKey, err)
	}
	if got != want {
		t.Fatalf("event source link count for %q = %d, want %d", sourceEventKey, got, want)
	}
}

func assertEventSourceLinkCountForEventSource(t *testing.T, db *sql.DB, eventID, sourceID int64, want int) {
	t.Helper()

	var got int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE event_id = ? AND source_id = ?
	`, eventID, sourceID).Scan(&got); err != nil {
		t.Fatalf("load event source link count for event %d source %d: %v", eventID, sourceID, err)
	}
	if got != want {
		t.Fatalf("event source link count for event %d source %d = %d, want %d", eventID, sourceID, got, want)
	}
}

func mustCountRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatalf("count rows in %s: %v", table, err)
	}
	return got
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
