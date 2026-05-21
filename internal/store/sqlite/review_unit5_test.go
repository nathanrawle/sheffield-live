package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
)

const (
	reviewUnit5SourceName = "Review test source"
	reviewUnit5SourceURL  = "https://example.test/review-test"
)

func TestAuthoritativeLinkedEventIDTxResolvesAllowlistedCalendarIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	sourceName := testSupportingSourceName
	sourceURL := testSupportingSourceURL
	sourceID := mustEnsureSourceID(t, st, sourceName, sourceURL)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	start := time.Date(2026, time.May, 18, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	slug := "sidney-calendar-authority"
	eventID := mustInsertExactIdentityEvent(t, db, slug, "Calendar Authority", venueID, sourceID, start, end, start.Add(-24*time.Hour), domain.OriginLive)
	mustActivateExactIdentity(t, db, eventID, slug, "Calendar Authority", "leadmill", start)

	calendarURL := "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical"
	sourceKey, ok := ingest.SourceIdentityKey(calendarURL)
	if !ok {
		t.Fatal("calendar source identity key not derived")
	}
	mustInsertAuthoritativeSourceLink(t, db, slug, sourceName, sourceURL, sourceKey)

	targetID, ok, ambiguous, err := authoritativeLinkedEventIDTx(ctx, db, reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceKey,
	})
	if err != nil {
		t.Fatalf("authoritative linked event id: %v", err)
	}
	if ambiguous {
		t.Fatal("authoritative linked event ambiguous = true, want false")
	}
	if !ok {
		t.Fatal("authoritative link not resolved")
	}
	if got, want := targetID, eventID; got != want {
		t.Fatalf("authoritative linked event id = %d, want %d", got, want)
	}
}

func TestEnsureEventSourceLinkTxReturnsAmbiguousForDifferentLiveTarget(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	sourceID := mustEnsureSourceID(t, st, reviewUnit5SourceName, reviewUnit5SourceURL)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	start := time.Date(2026, time.May, 19, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	targetSlug := "ambiguous-target-a"
	conflictSlug := "ambiguous-target-b"
	targetID := mustInsertExactIdentityEvent(t, db, targetSlug, "Ambiguous Target A", venueID, sourceID, start, end, start.Add(-24*time.Hour), domain.OriginLive)
	mustActivateExactIdentity(t, db, targetID, targetSlug, "Ambiguous Target A", "leadmill", start)
	conflictID := mustInsertExactIdentityEvent(t, db, conflictSlug, "Ambiguous Target B", venueID, sourceID, start.Add(30*time.Minute), end.Add(30*time.Minute), start.Add(-24*time.Hour), domain.OriginLive)
	mustActivateExactIdentity(t, db, conflictID, conflictSlug, "Ambiguous Target B", "leadmill", start.Add(30*time.Minute))

	conflictKey, ok := ingest.SourceIdentityKey("conflict-uid")
	if !ok {
		t.Fatal("conflict source identity key not derived")
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, conflictID, sourceID, conflictKey, "2026-05-01T10:00:00Z", "2026-05-01T10:00:00Z"); err != nil {
		t.Fatalf("insert conflicting source link: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	writeResult, err := ensureEventSourceLinkTx(ctx, tx, targetID, sourceID, ingest.SourceIdentities(ingest.SourceIdentityInput{ExternalID: "conflict-uid"}), sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, time.Now().UTC())
	if err != nil {
		t.Fatalf("ensure event source link: %v", err)
	}
	if !writeResult.Ambiguous {
		t.Fatal("write result ambiguous = false, want true")
	}
	if writeResult.Applied {
		t.Fatal("write result applied = true, want false")
	}

	gotEventID, authoritative := mustEventSourceLinkState(t, db, sourceID, conflictKey)
	if got, want := gotEventID, conflictID; got != want {
		t.Fatalf("conflicting source link event_id = %d, want %d", got, want)
	}
	if !authoritative {
		t.Fatal("conflicting source link authoritative = false, want true")
	}
}

func mustEventSourceLinkState(t *testing.T, db *sql.DB, sourceID int64, sourceEventKey string) (int64, bool) {
	t.Helper()

	var eventID int64
	var authoritative int
	if err := db.QueryRow(`
		SELECT event_id, is_authoritative
		FROM event_source_links
		WHERE source_id = ? AND source_event_key = ?
	`, sourceID, sourceEventKey).Scan(&eventID, &authoritative); err != nil {
		t.Fatalf("load event source link: %v", err)
	}
	return eventID, authoritative == 1
}

func mustActivateExactIdentity(t *testing.T, db *sql.DB, eventID int64, slug, name, venueSlug string, start time.Time) {
	t.Helper()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin exact identity tx: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	event := domain.Event{
		Slug:      slug,
		Name:      name,
		VenueSlug: venueSlug,
		Start:     start,
		Origin:    domain.OriginLive,
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, eventID, event, 0, start); err != nil {
		t.Fatalf("ensure active exact identity for event %d: %v", eventID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity tx: %v", err)
	}
}
