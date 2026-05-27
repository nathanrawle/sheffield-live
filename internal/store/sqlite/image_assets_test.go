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

func TestImageAssetFocusRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	copiedAt := time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC)
	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   "https://example.test/poster.jpg",
		PublicURL:   "/media/events/poster.jpg",
		StoragePath: "events/poster.jpg",
		ContentType: "image/jpeg",
		Width:       1200,
		Height:      800,
		FocusX:      12,
		FocusY:      88,
		Bytes:       1234,
		SHA256:      "abc123",
		CopiedAt:    copiedAt,
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}

	got, ok, err := st.LoadImageAsset(ctx, "https://example.test/poster.jpg")
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if got.FocusX != 12 || got.FocusY != 88 {
		t.Fatalf("focus = %d,%d, want 12,88", got.FocusX, got.FocusY)
	}
	if !got.CopiedAt.Equal(copiedAt) {
		t.Fatalf("copied at = %v, want %v", got.CopiedAt, copiedAt)
	}
}

func TestImageAssetFocusAllowsEdges(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   "https://example.test/edge.jpg",
		PublicURL:   "/media/events/edge.jpg",
		StoragePath: "events/edge.jpg",
		ContentType: "image/jpeg",
		FocusX:      0,
		FocusY:      100,
		CopiedAt:    time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}

	got, ok, err := st.LoadImageAsset(ctx, "https://example.test/edge.jpg")
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if got.FocusX != 0 || got.FocusY != 100 {
		t.Fatalf("focus = %d,%d, want 0,100", got.FocusX, got.FocusY)
	}
}

func TestImageAssetFocusRoundTripListAndUpdatePreservesExplicitZeroZero(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	copiedAt := time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC)
	sourceURL := "https://example.test/zero.jpg"
	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   sourceURL,
		PublicURL:   "/media/events/zero.jpg",
		StoragePath: "events/zero.jpg",
		ContentType: "image/jpeg",
		Width:       1200,
		Height:      800,
		FocusX:      0,
		FocusY:      0,
		Bytes:       1234,
		SHA256:      "abc123",
		CopiedAt:    copiedAt,
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}

	got, ok, err := st.LoadImageAsset(ctx, sourceURL)
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if got.FocusX != 0 || got.FocusY != 0 {
		t.Fatalf("load focus = %d,%d, want 0,0", got.FocusX, got.FocusY)
	}

	assets, err := st.ListImageAssets(ctx)
	if err != nil {
		t.Fatalf("list image assets: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("image asset count = %d, want 1", len(assets))
	}
	if assets[0].FocusX != 0 || assets[0].FocusY != 0 {
		t.Fatalf("list focus = %d,%d, want 0,0", assets[0].FocusX, assets[0].FocusY)
	}

	db := mustRawDB(t, path)
	defer db.Close()
	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Fixture", "https://example.test/source"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Fixture", "https://example.test/source").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "zero-focus-event", venueID, sourceID, "Zero Focus Event", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Zero focus description", "/media/events/zero.jpg", sourceURL, "Zero focus", 1200, 800, 0, 0, "2026-05-09T10:00:00Z", string(domain.OriginLive), string(domain.PublicationStateReviewed)); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "zero-focus-event").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	sourceImageSourceID := insertMediaCleanupSource(t, db, "Zero focus source image", "https://example.test/zero-source-image")
	insertEventSourceImageRow(t, db, eventID, sourceImageSourceID, "uid:zero-source", "/media/events/zero.jpg", sourceURL, "Zero source", 1200, 800, 0, 0)
	groupID := mustInsertReviewGroupRow(t, db, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "Zero focus review", "Fixture", "https://example.test/source", "open", "", "2026-05-09T10:00:00Z", "2026-05-09T10:00:00Z")
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
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			source_name,
			source_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, 1, "candidate-zero", "Zero Focus Candidate", "leadmill", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Zero focus description", "/media/events/zero.jpg", sourceURL, "Zero focus", 1200, 800, 0, 0, "Fixture", "https://example.test/source", "fixture UID candidate-zero"); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}

	if err := st.UpdateImageAssetFocus(ctx, sourceURL, ingest.ImageFocus{X: 0, Y: 0}); err != nil {
		t.Fatalf("update image asset focus: %v", err)
	}
	assertStoredImageAssetFocus(t, db, sourceURL, 0, 0)
	assertStoredFocus(t, db, "events", "image_source_url", sourceURL, 0, 0)
	assertStoredFocus(t, db, "event_source_images", "image_source_url", sourceURL, 0, 0)
	assertStoredFocus(t, db, "review_candidates", "image_source_url", sourceURL, 0, 0)
}

func TestUpdateImageAssetFocusUpdatesDenormalizedRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceURL := "https://example.test/poster.jpg"
	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   sourceURL,
		PublicURL:   "/media/events/poster.jpg",
		StoragePath: "events/poster.jpg",
		ContentType: "image/jpeg",
		FocusX:      50,
		FocusY:      50,
		CopiedAt:    time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Fixture", "https://example.test/source"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Fixture", "https://example.test/source").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			last_checked_at,
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "poster-show", venueID, sourceID, "Poster Show", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Poster description.", "/media/events/poster.jpg", sourceURL, "Poster", "2026-05-09T10:00:00Z", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "poster-show").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	sourceImageSourceID := insertMediaCleanupSource(t, db, "Poster source image", "https://example.test/poster-source-image")
	publicOnlySourceID := insertMediaCleanupSource(t, db, "Poster public source image", "https://example.test/poster-public-source-image")
	insertEventSourceImageRow(t, db, eventID, sourceImageSourceID, "uid:poster-source", "/media/events/poster.jpg", sourceURL, "Poster source", 1200, 800, 50, 50)
	insertEventSourceImageRow(t, db, eventID, publicOnlySourceID, "uid:poster-public", "/media/events/poster.jpg", "", "Poster public", 1200, 800, 50, 50)
	groupID := mustInsertReviewGroupRow(t, db, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "Poster review", "Fixture", "https://example.test/source", "open", "", "2026-05-09T10:00:00Z", "2026-05-09T10:00:00Z")
	if _, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			source_name,
			source_url
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, 1, "Poster Show", "leadmill", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Poster description.", "/media/events/poster.jpg", sourceURL, "Poster", "Fixture", "https://example.test/source"); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}

	if err := st.UpdateImageAssetFocus(ctx, sourceURL, ingest.ImageFocus{X: 25, Y: 75}); err != nil {
		t.Fatalf("update image asset focus: %v", err)
	}

	asset, ok, err := st.LoadImageAsset(ctx, sourceURL)
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if asset.FocusX != 25 || asset.FocusY != 75 {
		t.Fatalf("asset focus = %d,%d, want 25,75", asset.FocusX, asset.FocusY)
	}
	assertStoredFocus(t, db, "events", "image_source_url", sourceURL, 25, 75)
	assertStoredFocus(t, db, "event_source_images", "source_identity_key", "uid:poster-source", 25, 75)
	assertStoredFocus(t, db, "event_source_images", "source_identity_key", "uid:poster-public", 25, 75)
	assertStoredFocus(t, db, "review_candidates", "image_source_url", sourceURL, 25, 75)
}

func assertStoredFocus(t *testing.T, db *sql.DB, table, whereColumn, whereValue string, wantX, wantY int) {
	t.Helper()
	var gotX, gotY int
	if err := db.QueryRow("SELECT image_focus_x, image_focus_y FROM "+table+" WHERE "+whereColumn+" = ?", whereValue).Scan(&gotX, &gotY); err != nil {
		t.Fatalf("scan %s focus: %v", table, err)
	}
	if gotX != wantX || gotY != wantY {
		t.Fatalf("%s focus = %d,%d, want %d,%d", table, gotX, gotY, wantX, wantY)
	}
}

func assertStoredImageAssetFocus(t *testing.T, db *sql.DB, whereValue string, wantX, wantY int) {
	t.Helper()

	var gotX, gotY int
	if err := db.QueryRow("SELECT focus_x, focus_y FROM image_assets WHERE source_url = ?", whereValue).Scan(&gotX, &gotY); err != nil {
		t.Fatalf("scan image_assets focus: %v", err)
	}
	if gotX != wantX || gotY != wantY {
		t.Fatalf("image_assets focus = %d,%d, want %d,%d", gotX, gotY, wantX, wantY)
	}
}
