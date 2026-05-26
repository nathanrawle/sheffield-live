package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func TestEventSourceImagesByEventSlugOrdersAndNormalizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	eventID := insertEventSourceImageTestEvent(t, db, "source-image-read")
	zSourceID := insertMediaCleanupSource(t, db, "Zulu source", "https://example.test/zulu")
	aSourceID := insertMediaCleanupSource(t, db, "Alpha source", "https://example.test/alpha")
	insertEventSourceImageRow(t, db, eventID, zSourceID, "url:z", "/media/events/z.jpg", "https://images.example.test/z.jpg", "Zulu", 1200, 800, 120, -10)
	insertEventSourceImageRow(t, db, eventID, aSourceID, "url:a", "/media/events/a.jpg", "https://images.example.test/a.jpg", "Alpha", 1200, 800, 0, 0)

	images, err := st.EventSourceImagesByEventSlug(context.Background(), "source-image-read")
	if err != nil {
		t.Fatalf("load event source images: %v", err)
	}
	if len(images) != 2 {
		t.Fatalf("images = %d, want 2", len(images))
	}
	if got, want := images[0].ImageURL, "/media/events/a.jpg"; got != want {
		t.Fatalf("first image url = %q, want %q", got, want)
	}
	if images[0].FocusX != 0 || images[0].FocusY != 0 {
		t.Fatalf("first image focus = %d,%d want 0,0", images[0].FocusX, images[0].FocusY)
	}
	if images[1].FocusX != 100 || images[1].FocusY != 0 {
		t.Fatalf("second image focus = %d,%d want 100,0", images[1].FocusX, images[1].FocusY)
	}
}

func TestUpsertEventSourceImagesKeepsOneCurrentImagePerSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	eventID := insertEventSourceImageTestEvent(t, db, "source-image-upsert")
	now := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := upsertEventSourceImagesTx(context.Background(), tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{{
		ExternalID:     "old",
		Name:           "Source Image Upsert",
		SourceName:     "Mirror source",
		SourceURL:      "https://example.test/mirror",
		ImageURL:       "/media/events/old.jpg",
		ImageSourceURL: "https://images.example.test/old.jpg",
		ImageAlt:       "Old",
		ImageWidth:     1200,
		ImageHeight:    800,
		ImageFocusX:    30,
		ImageFocusY:    70,
	}}, now); err != nil {
		t.Fatalf("upsert old image: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit old image: %v", err)
	}

	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin second tx: %v", err)
	}
	if err := upsertEventSourceImagesTx(context.Background(), tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{{
		ExternalID:     "new",
		Name:           "Source Image Upsert",
		SourceName:     "Mirror source",
		SourceURL:      "https://example.test/mirror",
		ImageURL:       "/media/events/new.jpg",
		ImageSourceURL: "https://images.example.test/new.jpg",
		ImageAlt:       "New",
		ImageWidth:     1200,
		ImageHeight:    800,
		ImageFocusX:    40,
		ImageFocusY:    60,
	}}, now.Add(time.Hour)); err != nil {
		t.Fatalf("upsert new image: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit new image: %v", err)
	}

	if got := mustCount(t, db, "event_source_images"); got != 1 {
		t.Fatalf("event_source_images rows = %d, want 1", got)
	}
	var imageURL string
	var sourceIdentityKey string
	if err := db.QueryRow(`SELECT image_url, source_identity_key FROM event_source_images WHERE event_id = ?`, eventID).Scan(&imageURL, &sourceIdentityKey); err != nil {
		t.Fatalf("query source image: %v", err)
	}
	if got, want := imageURL, "/media/events/new.jpg"; got != want {
		t.Fatalf("image url = %q, want %q", got, want)
	}
	if got, want := sourceIdentityKey, "uid:new"; got != want {
		t.Fatalf("source identity key = %q, want %q", got, want)
	}

	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin delete tx: %v", err)
	}
	if err := upsertEventSourceImagesTx(context.Background(), tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{{
		ExternalID: "new",
		Name:       "Source Image Upsert",
		SourceName: "Mirror source",
		SourceURL:  "https://example.test/mirror",
	}}, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("upsert blank image: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit blank image: %v", err)
	}
	if got := mustCount(t, db, "event_source_images"); got != 0 {
		t.Fatalf("event_source_images rows after blank = %d, want 0", got)
	}
}

func TestUpsertEventSecondarySourceInfoRemovesOmittedSourceImages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	eventID := insertEventSourceImageTestEvent(t, db, "source-image-secondary-sync")
	now := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := upsertEventSecondarySourceInfoTx(context.Background(), tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{
		{
			ExternalID:     "alpha",
			Name:           "Source Image Secondary Sync",
			SourceName:     "Alpha mirror",
			SourceURL:      "https://example.test/alpha-secondary-sync",
			ImageURL:       "/media/events/alpha-secondary-sync.jpg",
			ImageSourceURL: "https://images.example.test/alpha-secondary-sync.jpg",
			ImageAlt:       "Alpha poster",
			ImageWidth:     1200,
			ImageHeight:    800,
		},
		{
			ExternalID:     "bravo",
			Name:           "Source Image Secondary Sync",
			SourceName:     "Bravo mirror",
			SourceURL:      "https://example.test/bravo-secondary-sync",
			ImageURL:       "/media/events/bravo-secondary-sync.jpg",
			ImageSourceURL: "https://images.example.test/bravo-secondary-sync.jpg",
			ImageAlt:       "Bravo poster",
			ImageWidth:     1200,
			ImageHeight:    800,
		},
	}, now); err != nil {
		t.Fatalf("upsert initial secondary source images: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit initial secondary source images: %v", err)
	}
	if got := mustCount(t, db, "event_source_images"); got != 2 {
		t.Fatalf("event_source_images rows = %d, want 2", got)
	}

	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin recompute tx: %v", err)
	}
	if err := upsertEventSecondarySourceInfoTx(context.Background(), tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{{
		ExternalID:     "alpha",
		Name:           "Source Image Secondary Sync",
		SourceName:     "Alpha mirror",
		SourceURL:      "https://example.test/alpha-secondary-sync",
		ImageURL:       "/media/events/alpha-secondary-sync-new.jpg",
		ImageSourceURL: "https://images.example.test/alpha-secondary-sync-new.jpg",
		ImageAlt:       "Alpha poster updated",
		ImageWidth:     1600,
		ImageHeight:    900,
	}}, now.Add(time.Hour)); err != nil {
		t.Fatalf("upsert recomputed secondary source images: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit recomputed secondary source images: %v", err)
	}

	if got := mustCount(t, db, "event_source_images"); got != 1 {
		t.Fatalf("event_source_images rows after recompute = %d, want 1", got)
	}
	var sourceName string
	var imageURL string
	if err := db.QueryRow(`
		SELECT s.name, i.image_url
		FROM event_source_images i
		JOIN sources s ON s.id = i.source_id
		WHERE i.event_id = ?
	`, eventID).Scan(&sourceName, &imageURL); err != nil {
		t.Fatalf("query remaining source image: %v", err)
	}
	if got, want := sourceName, "Alpha mirror"; got != want {
		t.Fatalf("remaining source name = %q, want %q", got, want)
	}
	if got, want := imageURL, "/media/events/alpha-secondary-sync-new.jpg"; got != want {
		t.Fatalf("remaining image url = %q, want %q", got, want)
	}
}

func TestOpenBackfillsEventSourceImagesFromResolvedClusterCanonicalEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	db := mustRawDB(t, path)
	canonicalSourceID := insertMediaCleanupSource(t, db, "Primary source", "https://example.test/primary-backfill")
	eventID := insertEventSourceImageTestEventWithSource(t, db, "source-image-backfill", canonicalSourceID)
	sourceID := insertMediaCleanupSource(t, db, "Supporting source", "https://example.test/support-backfill")
	when := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)
	clusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, 0, &eventID, "import_review", "ingest_candidate", when)
	payload := `{"candidate_external_id":"supporting-1","candidate_title":"Backfill","candidate_start_at":"2026-05-10T18:00:00Z","candidate_image_url":"/media/events/backfill.jpg","candidate_image_source_url":"https://images.example.test/backfill.jpg","candidate_image_alt":"Backfill poster","candidate_image_width":1200,"candidate_image_height":800,"candidate_image_focus_x":15,"candidate_image_focus_y":85,"source_url":"https://example.test/support-backfill"}`
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-image-backfill-evidence", payload)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, evidenceID, true, when, nil, "test")
	insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, "{}", "")
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer st.Close()

	images, err := st.EventSourceImagesByEventSlug(context.Background(), "source-image-backfill")
	if err != nil {
		t.Fatalf("load backfilled source images: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("backfilled images = %d, want 1", len(images))
	}
	if got, want := images[0].ImageURL, "/media/events/backfill.jpg"; got != want {
		t.Fatalf("backfilled image = %q, want %q", got, want)
	}
}

func TestOpenBackfillsEventSourceImagesOnlyFromResolvedReviewCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	db := mustRawDB(t, path)
	canonicalSourceID := insertMediaCleanupSource(t, db, "Primary candidate source", "https://example.test/primary-candidate-backfill")
	eventID := insertEventSourceImageTestEventWithSource(t, db, "source-image-candidate-backfill", canonicalSourceID)
	openGroupID := insertReviewGroupForEventSourceImageBackfill(t, db, "Open candidate backfill", review.StatusOpen)
	insertReviewCandidateForEventSourceImageBackfill(t, db, openGroupID, eventID, "Open mirror", "https://example.test/open-candidate-backfill", "/media/events/open-candidate-backfill.jpg")
	resolvedGroupID := insertReviewGroupForEventSourceImageBackfill(t, db, "Resolved candidate backfill", review.StatusResolved)
	insertReviewCandidateForEventSourceImageBackfill(t, db, resolvedGroupID, eventID, "Resolved mirror", "https://example.test/resolved-candidate-backfill", "/media/events/resolved-candidate-backfill.jpg")
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer st.Close()

	images, err := st.EventSourceImagesByEventSlug(context.Background(), "source-image-candidate-backfill")
	if err != nil {
		t.Fatalf("load backfilled candidate source images: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("backfilled candidate images = %d, want 1", len(images))
	}
	if got, want := images[0].ImageURL, "/media/events/resolved-candidate-backfill.jpg"; got != want {
		t.Fatalf("backfilled candidate image = %q, want %q", got, want)
	}
	if got, want := images[0].SourceName, "Resolved mirror"; got != want {
		t.Fatalf("backfilled candidate source = %q, want %q", got, want)
	}
}

func insertEventSourceImageTestEvent(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()
	sourceID := insertMediaCleanupSource(t, db, "Canonical source "+slug, "https://example.test/"+slug+"/canonical")
	return insertEventSourceImageTestEventWithSource(t, db, slug, sourceID)
}

func insertEventSourceImageTestEventWithSource(t *testing.T, db *sql.DB, slug string, sourceID int64) int64 {
	t.Helper()
	venueID := mediaCleanupVenueID(t, db, "leadmill")
	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID: sourceID,
		VenueID:  venueID,
		Slug:     slug,
		Name:     "Source Image Test",
		Start:    time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC),
		End:      time.Date(2026, time.May, 10, 22, 0, 0, 0, time.UTC),
	})
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, slug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	return eventID
}

func insertEventSourceImageRow(t *testing.T, db *sql.DB, eventID, sourceID int64, identityKey, imageURL, imageSourceURL, alt string, width, height, focusX, focusY int) {
	t.Helper()
	now := "2026-05-01T10:00:00Z"
	if _, err := db.Exec(`
		INSERT INTO event_source_images (
			event_id,
			source_id,
			source_identity_key,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			first_seen_at,
			last_seen_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, eventID, sourceID, identityKey, imageURL, imageSourceURL, alt, width, height, focusX, focusY, now, now, now); err != nil {
		t.Fatalf("insert event source image: %v", err)
	}
}

func insertReviewGroupForEventSourceImageBackfill(t *testing.T, db *sql.DB, title, status string) int64 {
	t.Helper()
	when := formatRFC3339UTC(time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC))
	res, err := db.Exec(`
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, title, "Fixture reviews", "https://example.test/reviews", status, "", when, when)
	if err != nil {
		t.Fatalf("insert review group: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("review group id: %v", err)
	}
	return id
}

func insertReviewCandidateForEventSourceImageBackfill(t *testing.T, db *sql.DB, groupID, eventID int64, sourceName, sourceURL, imageURL string) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			external_id,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			source_name,
			source_url,
			provenance,
			canonical_event_id,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, 1, sourceURL, "Candidate Backfill", "leadmill", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Candidate description", sourceName, sourceURL, "fixture", eventID, imageURL, sourceURL+"/image.jpg", "Candidate poster", 1200, 800, 30, 70); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}
}
