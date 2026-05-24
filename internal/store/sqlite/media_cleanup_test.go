package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

func TestCleanupMediaClearsPassedManagedEventsAndDeletesUnreferencedAssets(t *testing.T) {
	ctx := context.Background()
	st, db := openMediaCleanupFixture(t)

	sourceID := insertMediaCleanupSource(t, db, "Media cleanup source", "https://example.test/source")
	venueID := mediaCleanupVenueID(t, db, "leadmill")
	copiedAt := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)

	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/passed.jpg", "/media/events/passed.jpg", "events/passed.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/future.jpg", "/media/events/future.jpg", "events/future.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/unused.jpg", "/media/events/unused.jpg", "events/unused.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/missing.jpg", "/media/events/missing.jpg", "events/missing.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/shared-old.jpg", "/media/events/shared-old.jpg", "events/shared.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/shared-retained.jpg", "/media/events/shared.jpg", "events/shared.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/public-only.jpg", "/media/events/public-only.jpg", "events/public-only.jpg", copiedAt)

	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID:         sourceID,
		VenueID:          venueID,
		Slug:             "passed-managed-image",
		Name:             "Passed Managed Image",
		Start:            time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 1, 22, 0, 0, 0, time.UTC),
		ImageURL:         "/media/events/passed.jpg",
		ImageSourceURL:   "https://images.example.test/passed.jpg",
		ImageAlt:         "Passed poster",
		ImageWidth:       640,
		ImageHeight:      480,
		ImageFocusX:      20,
		ImageFocusY:      80,
		PublicationState: domain.PublicationStateReviewed,
	})
	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID:         sourceID,
		VenueID:          venueID,
		Slug:             "future-withheld-image",
		Name:             "Future Withheld Image",
		Start:            time.Date(2026, time.May, 25, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 25, 22, 0, 0, 0, time.UTC),
		ImageURL:         "/media/events/future.jpg",
		ImageSourceURL:   "https://images.example.test/future.jpg",
		PublicationState: domain.PublicationStateWithheld,
	})
	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID:         sourceID,
		VenueID:          venueID,
		Slug:             "future-shared-image",
		Name:             "Future Shared Image",
		Start:            time.Date(2026, time.May, 26, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 26, 22, 0, 0, 0, time.UTC),
		ImageURL:         "/media/events/shared.jpg",
		ImageSourceURL:   "https://images.example.test/shared-retained.jpg",
		PublicationState: domain.PublicationStateReviewed,
	})
	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID:         sourceID,
		VenueID:          venueID,
		Slug:             "future-public-only-image",
		Name:             "Future Public Only Image",
		Start:            time.Date(2026, time.May, 27, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 27, 22, 0, 0, 0, time.UTC),
		ImageURL:         "/media/events/public-only.jpg",
		PublicationState: domain.PublicationStateReviewed,
	})
	insertMediaCleanupEvent(t, db, mediaCleanupEventInput{
		SourceID:         sourceID,
		VenueID:          venueID,
		Slug:             "passed-remote-image",
		Name:             "Passed Remote Image",
		Start:            time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 1, 22, 0, 0, 0, time.UTC),
		ImageURL:         "https://remote.example.test/poster.jpg",
		PublicationState: domain.PublicationStateReviewed,
	})

	report, err := st.CleanupMedia(ctx, MediaCleanupOptions{
		Apply:          true,
		Now:            time.Date(2026, time.May, 20, 12, 0, 0, 0, time.UTC),
		MediaURLPrefix: "/media",
		ExistingFiles: []string{
			"events/passed.jpg",
			"events/future.jpg",
			"events/unused.jpg",
			"events/shared.jpg",
			"events/public-only.jpg",
		},
	})
	if err != nil {
		t.Fatalf("cleanup media: %v", err)
	}

	if report.ClearedEventImages != 1 {
		t.Fatalf("cleared event images = %d, want 1", report.ClearedEventImages)
	}
	if report.DeletedAssetRows != 4 {
		t.Fatalf("deleted asset rows = %d, want 4", report.DeletedAssetRows)
	}
	if report.MissingFiles != 1 {
		t.Fatalf("missing files = %d, want 1", report.MissingFiles)
	}
	if got, want := report.FilesToDelete, []string{"events/passed.jpg", "events/unused.jpg"}; !equalStringSlices(got, want) {
		t.Fatalf("files to delete = %#v, want %#v", got, want)
	}

	assertMediaCleanupEventImage(t, db, "passed-managed-image", "", "", 0, 0, 50, 50)
	assertMediaCleanupEventImage(t, db, "passed-remote-image", "https://remote.example.test/poster.jpg", "", 0, 0, 50, 50)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/future.jpg", true)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/shared-retained.jpg", true)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/public-only.jpg", true)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/shared-old.jpg", false)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/passed.jpg", false)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/missing.jpg", false)
	if !stringSliceContains(report.RetainedPublicURLs, "/media/events/public-only.jpg") {
		t.Fatalf("retained public URLs = %#v, want public-only image URL", report.RetainedPublicURLs)
	}
}

func TestCleanupMediaRetainsOpenReviewEvidenceWithInvalidPayloads(t *testing.T) {
	ctx := context.Background()
	st, db := openMediaCleanupFixture(t)

	sourceID := insertMediaCleanupSource(t, db, "Media cleanup source", "https://example.test/source")
	copiedAt := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/malformed.jpg", "/media/events/malformed.jpg", "events/malformed.jpg", copiedAt)
	saveMediaCleanupAsset(t, ctx, st, "https://images.example.test/invalid-date.jpg", "/media/events/invalid-date.jpg", "events/invalid-date.jpg", copiedAt)

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	malformedEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "media-cleanup-malformed", `{"candidate_image_source_url":"https://images.example.test/malformed.jpg"`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, malformedEvidenceID, true, copiedAt, nil, "media cleanup test")
	invalidDatePayload := `{"candidate_start_at":"not-a-date","candidate_image_url":"/media/events/invalid-date.jpg","candidate_image_source_url":"https://images.example.test/invalid-date.jpg"}`
	invalidDateEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "media-cleanup-invalid-date", invalidDatePayload)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, invalidDateEvidenceID, true, copiedAt, nil, "media cleanup test")

	report, err := st.CleanupMedia(ctx, MediaCleanupOptions{
		Apply:          true,
		Now:            time.Date(2026, time.May, 20, 12, 0, 0, 0, time.UTC),
		MediaURLPrefix: "/media",
		ExistingFiles:  []string{"events/malformed.jpg", "events/invalid-date.jpg"},
	})
	if err != nil {
		t.Fatalf("cleanup media: %v", err)
	}
	if report.DeletedAssetRows != 0 {
		t.Fatalf("deleted asset rows = %d, want 0", report.DeletedAssetRows)
	}
	if len(report.Warnings) != 2 {
		t.Fatalf("warnings = %#v, want two invalid evidence warnings", report.Warnings)
	}
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/malformed.jpg", true)
	assertMediaCleanupAssetExists(t, st, "https://images.example.test/invalid-date.jpg", true)
	if !stringSliceContains(report.RetainedStoragePaths, "events/malformed.jpg") || !stringSliceContains(report.RetainedStoragePaths, "events/invalid-date.jpg") {
		t.Fatalf("retained storage paths = %#v, want both open review files", report.RetainedStoragePaths)
	}
}

func openMediaCleanupFixture(t *testing.T) (*Store, *sql.DB) {
	t.Helper()

	path := t.TempDir() + "/sheffield-live.db"
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	db := mustRawDB(t, path)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close raw db: %v", err)
		}
	})
	return st, db
}

func saveMediaCleanupAsset(t *testing.T, ctx context.Context, st *Store, sourceURL, publicURL, storagePath string, copiedAt time.Time) {
	t.Helper()

	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   sourceURL,
		PublicURL:   publicURL,
		StoragePath: storagePath,
		ContentType: "image/jpeg",
		Width:       640,
		Height:      480,
		FocusX:      50,
		FocusY:      50,
		Bytes:       10,
		SHA256:      strings.TrimPrefix(storagePath, "events/"),
		CopiedAt:    copiedAt,
	}); err != nil {
		t.Fatalf("save image asset %q: %v", sourceURL, err)
	}
}

func insertMediaCleanupSource(t *testing.T, db *sql.DB, name, sourceURL string) int64 {
	t.Helper()

	res, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, name, sourceURL)
	if err != nil {
		t.Fatalf("insert source: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("source id: %v", err)
	}
	return id
}

func mediaCleanupVenueID(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, slug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue %q: %v", slug, err)
	}
	return venueID
}

type mediaCleanupEventInput struct {
	SourceID         int64
	VenueID          int64
	Slug             string
	Name             string
	Start            time.Time
	End              time.Time
	ImageURL         string
	ImageSourceURL   string
	ImageAlt         string
	ImageWidth       int
	ImageHeight      int
	ImageFocusX      int
	ImageFocusY      int
	PublicationState domain.PublicationState
}

func insertMediaCleanupEvent(t *testing.T, db *sql.DB, input mediaCleanupEventInput) {
	t.Helper()

	if input.PublicationState == "" {
		input.PublicationState = domain.PublicationStateReviewed
	}
	var end any
	if !input.End.IsZero() {
		end = formatRFC3339UTC(input.End)
	}
	if input.ImageFocusX == 0 && input.ImageFocusY == 0 {
		input.ImageFocusX = 50
		input.ImageFocusY = 50
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
	`,
		input.Slug,
		input.VenueID,
		input.SourceID,
		input.Name,
		formatRFC3339UTC(input.Start),
		end,
		"Indie",
		"Listed",
		"Media cleanup event.",
		input.ImageURL,
		input.ImageSourceURL,
		input.ImageAlt,
		input.ImageWidth,
		input.ImageHeight,
		input.ImageFocusX,
		input.ImageFocusY,
		"2026-05-01T10:00:00Z",
		string(domain.OriginLive),
		string(input.PublicationState),
	); err != nil {
		t.Fatalf("insert media cleanup event %q: %v", input.Slug, err)
	}
}

func assertMediaCleanupEventImage(t *testing.T, db *sql.DB, slug, wantURL, wantSourceURL string, wantWidth, wantHeight, wantFocusX, wantFocusY int) {
	t.Helper()

	var gotURL, gotSourceURL string
	var gotWidth, gotHeight, gotFocusX, gotFocusY int
	if err := db.QueryRow(`
		SELECT image_url, image_source_url, image_width, image_height, image_focus_x, image_focus_y
		FROM events
		WHERE slug = ?
	`, slug).Scan(&gotURL, &gotSourceURL, &gotWidth, &gotHeight, &gotFocusX, &gotFocusY); err != nil {
		t.Fatalf("load event image %q: %v", slug, err)
	}
	if gotURL != wantURL || gotSourceURL != wantSourceURL || gotWidth != wantWidth || gotHeight != wantHeight || gotFocusX != wantFocusX || gotFocusY != wantFocusY {
		t.Fatalf("event %q image = (%q,%q,%d,%d,%d,%d), want (%q,%q,%d,%d,%d,%d)", slug, gotURL, gotSourceURL, gotWidth, gotHeight, gotFocusX, gotFocusY, wantURL, wantSourceURL, wantWidth, wantHeight, wantFocusX, wantFocusY)
	}
}

func assertMediaCleanupAssetExists(t *testing.T, st *Store, sourceURL string, want bool) {
	t.Helper()

	_, ok, err := st.LoadImageAsset(context.Background(), sourceURL)
	if err != nil {
		t.Fatalf("load image asset %q: %v", sourceURL, err)
	}
	if ok != want {
		t.Fatalf("image asset %q exists = %v, want %v", sourceURL, ok, want)
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
