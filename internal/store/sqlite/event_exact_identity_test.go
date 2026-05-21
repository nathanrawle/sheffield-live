package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/domain"
)

type exactIdentityTestRow struct {
	ID                int64
	EventID           int64
	IdentityKey       string
	Active            int
	DeactivatedAt     string
	DeactivatedReason string
	RepairRunID       sql.NullInt64
}

func TestExactIdentityKeyConstructionAndNormalization(t *testing.T) {
	t.Parallel()

	utc := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	bst := time.Date(2026, time.May, 12, 20, 0, 0, 0, time.FixedZone("BST", 3600))

	keyA := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", utc, "  The   Exact   Title  ")
	keyB := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", bst, "the exact title")
	if keyA != keyB {
		t.Fatalf("keys differ for equivalent inputs: %q vs %q", keyA, keyB)
	}

	keyVenue := buildExactIdentityKey(exactIdentityKeyVersion, "other-venue", utc, "the exact title")
	if keyVenue == keyA {
		t.Fatalf("key did not change when venue changed: %q", keyVenue)
	}

	keyStart := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", utc.Add(time.Hour), "the exact title")
	if keyStart == keyA {
		t.Fatalf("key did not change when start changed: %q", keyStart)
	}

	keyTitle := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", utc, "the different title")
	if keyTitle == keyA {
		t.Fatalf("key did not change when title changed: %q", keyTitle)
	}

	materialA, ok, err := exactIdentityMaterialForEvent(domain.Event{
		Slug:      "exact-identity-a",
		Name:      "  THE   EXACT   TITLE  ",
		VenueSlug: "leadmill",
		Start:     utc,
		Origin:    domain.OriginLive,
	})
	if err != nil {
		t.Fatalf("material A: %v", err)
	}
	if !ok {
		t.Fatal("material A not available")
	}

	materialB, ok, err := exactIdentityMaterialForEvent(domain.Event{
		Slug:      "exact-identity-b",
		Name:      "the exact title",
		VenueSlug: "leadmill",
		Start:     bst,
		Origin:    domain.OriginLive,
	})
	if err != nil {
		t.Fatalf("material B: %v", err)
	}
	if !ok {
		t.Fatal("material B not available")
	}
	if materialA.cleanTitle != materialB.cleanTitle {
		t.Fatalf("normalized titles differ: %q vs %q", materialA.cleanTitle, materialB.cleanTitle)
	}
	if materialA.material != materialB.material {
		t.Fatalf("normalized materials differ: %q vs %q", materialA.material, materialB.material)
	}
}

func TestExactIdentityLifecycleHelpers(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	ensureRepairRunID := mustInsertRepairRun(t, st.db)
	secondEnsureRepairRunID := mustInsertRepairRun(t, st.db)
	replaceRepairRunID := mustInsertRepairRun(t, st.db)
	eventStart := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	eventEnd := eventStart.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	eventID := mustInsertExactIdentityEvent(t, st.db, "exact-identity-lifecycle", "  The   Exact   Title  ", venueID, sourceID, eventStart, eventEnd, lastChecked, domain.OriginLive)

	baseEvent := domain.Event{
		Slug:             "exact-identity-lifecycle",
		Name:             "  The   Exact   Title  ",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin ensure tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, baseEvent, ensureRepairRunID, time.Date(2026, time.May, 12, 9, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("ensure active exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit ensure tx: %v", err)
	}

	rows := mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("exact identity rows = %d, want %d", got, want)
	}
	if rows[0].Active != 1 {
		t.Fatalf("exact identity active = %d, want 1", rows[0].Active)
	}
	if !rows[0].RepairRunID.Valid || rows[0].RepairRunID.Int64 != ensureRepairRunID {
		t.Fatalf("repair_run_id = %d, want %d", rows[0].RepairRunID.Int64, ensureRepairRunID)
	}
	key := rows[0].IdentityKey

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin idempotent tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, baseEvent, secondEnsureRepairRunID, time.Date(2026, time.May, 12, 9, 6, 0, 0, time.UTC)); err != nil {
		t.Fatalf("idempotent ensure: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit idempotent tx: %v", err)
	}

	rows = mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("exact identity rows after idempotent ensure = %d, want %d", got, want)
	}
	if rows[0].IdentityKey != key {
		t.Fatalf("identity key changed on idempotent ensure: %q -> %q", key, rows[0].IdentityKey)
	}
	if !rows[0].RepairRunID.Valid || rows[0].RepairRunID.Int64 != ensureRepairRunID {
		t.Fatalf("repair_run_id changed on idempotent ensure: %d, want %d", rows[0].RepairRunID.Int64, ensureRepairRunID)
	}

	updatedEvent := baseEvent
	updatedEvent.Slug = "exact-identity-lifecycle-updated"
	updatedEvent.Name = "  The   Exact   Headline  "
	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin replace tx: %v", err)
	}
	if err := replaceActiveExactIdentityForLiveEventTx(ctx, tx, eventID, updatedEvent, "title repaired", replaceRepairRunID, time.Date(2026, time.May, 12, 9, 7, 0, 0, time.UTC)); err != nil {
		t.Fatalf("replace exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit replace tx: %v", err)
	}

	rows = mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("exact identity rows after replace = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("old exact identity active = %d, want 0", rows[0].Active)
	}
	if rows[0].DeactivatedAt == "" || rows[0].DeactivatedReason != "title repaired" {
		t.Fatalf("old exact identity deactivation metadata = (%q, %q)", rows[0].DeactivatedAt, rows[0].DeactivatedReason)
	}
	if rows[1].Active != 1 {
		t.Fatalf("new exact identity active = %d, want 1", rows[1].Active)
	}
	if rows[1].IdentityKey == key {
		t.Fatalf("replacement kept the old identity key: %q", rows[1].IdentityKey)
	}

	if _, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, key); err != nil {
		t.Fatalf("lookup old identity key: %v", err)
	} else if found {
		t.Fatal("old exact identity key still matched a live event")
	}
	if record, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, rows[1].IdentityKey); err != nil {
		t.Fatalf("lookup new identity key: %v", err)
	} else if !found || record.ID != eventID {
		t.Fatalf("lookup new identity key returned (%v, %v), want event %d", found, record.ID, eventID)
	}

	nonLiveEvent := updatedEvent
	nonLiveEvent.Origin = domain.OriginTest
	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin deactivate tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, nonLiveEvent, 0, time.Date(2026, time.May, 12, 9, 8, 0, 0, time.UTC)); err != nil {
		t.Fatalf("deactivate exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deactivate tx: %v", err)
	}

	rows = mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("exact identity rows after non-live deactivate = %d, want %d", got, want)
	}
	if rows[1].Active != 0 {
		t.Fatalf("latest exact identity active after deactivation = %d, want 0", rows[1].Active)
	}
	if _, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, rows[1].IdentityKey); err != nil {
		t.Fatalf("lookup inactive identity: %v", err)
	} else if found {
		t.Fatal("inactive exact identity still matched a live event")
	}
}

func TestExactIdentityActiveUniqueIndexBlocksDuplicateLiveKeys(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	start := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	name := "Shared exact title"
	key := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", start, name)

	firstEventID := mustInsertExactIdentityEvent(t, st.db, "exact-identity-unique-a", name, venueID, sourceID, start, end, lastChecked, domain.OriginLive)
	secondEventID := mustInsertExactIdentityEvent(t, st.db, "exact-identity-unique-b", name, venueID, sourceID, start, end, lastChecked, domain.OriginLive)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first insert tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, firstEventID, domain.Event{
		Slug:             "exact-identity-unique-a",
		Name:             name,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure first exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit first insert tx: %v", err)
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second insert tx: %v", err)
	}
	err = ensureActiveExactIdentityTx(ctx, tx, secondEventID, domain.Event{
		Slug:             "exact-identity-unique-b",
		Name:             name,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked)
	if err == nil {
		t.Fatal("expected duplicate active exact identity error")
	}
	_ = tx.Rollback()

	if rows := mustExactIdentityRowsByEvent(t, st.db, firstEventID); len(rows) != 1 || rows[0].IdentityKey != key || rows[0].Active != 1 {
		t.Fatalf("first event exact identities = %+v, want one active row for %q", rows, key)
	}
}

func TestExactIdentityLookupOnlyReturnsLiveEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	start := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	name := "Non-live exact title"
	eventID := mustInsertExactIdentityEvent(t, st.db, "exact-identity-non-live", name, venueID, sourceID, start, end, lastChecked, domain.OriginTest)
	key := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", start, name)

	if _, err := st.db.Exec(`
		INSERT INTO event_exact_identities (
			event_id,
			identity_key,
			key_version,
			venue_slug,
			utc_start_at,
			clean_title,
			active,
			created_at,
			updated_at,
			deactivated_at,
			deactivated_reason,
			repair_run_id
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, '', NULL)
	`, eventID, key, exactIdentityKeyVersion, "leadmill", formatRFC3339UTC(start), name, formatRFC3339UTC(lastChecked), formatRFC3339UTC(lastChecked)); err != nil {
		t.Fatalf("seed non-live exact identity row: %v", err)
	}

	if rows := mustExactIdentityRowsByEvent(t, st.db, eventID); len(rows) != 1 || rows[0].Active != 1 || rows[0].IdentityKey != key {
		t.Fatalf("seeded non-live exact identities = %+v, want one active row for %q", rows, key)
	}

	if record, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, key); err != nil {
		t.Fatalf("lookup live exact identity by key: %v", err)
	} else if found {
		t.Fatalf("unexpected live match for non-live event: %+v", record)
	}
}

func TestLiveMatchersIgnoreWithheldTargets(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	start := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	slug := "withheld-target-event"
	name := "Withheld target title"
	eventID := mustInsertExactIdentityEvent(t, st.db, slug, name, venueID, sourceID, start, end, lastChecked, domain.OriginLive)
	key := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", start, name)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, domain.Event{
		Slug:             slug,
		Name:             name,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure active exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity tx: %v", err)
	}

	if _, err := st.db.Exec(`
		UPDATE events
		SET publication_state = ?, withheld_reason = ?
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), "duplicate listing", eventID); err != nil {
		t.Fatalf("withhold event: %v", err)
	}

	withheldInput := domain.Event{
		Slug:             slug,
		Name:             name,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateWithheld,
	}
	if _, ok, err := exactIdentityMaterialForEvent(withheldInput); err != nil {
		t.Fatalf("withheld exact identity material: %v", err)
	} else if ok {
		t.Fatal("withheld event produced exact identity material")
	}

	if record, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, key); err != nil {
		t.Fatalf("lookup withheld exact identity: %v", err)
	} else if found {
		t.Fatalf("withheld event still matched by exact identity: %+v", record)
	}

	liveInput := domain.Event{
		Slug:             slug,
		Name:             name,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}
	if records, err := matchLiveEventsByIdentityTx(ctx, st.db, liveInput.Slug, liveInput.Name, liveInput.VenueSlug, liveInput.Start); err != nil {
		t.Fatalf("slug/fingerprint match: %v", err)
	} else if len(records) != 0 {
		t.Fatalf("slug/fingerprint match returned withheld event: %+v", records)
	}
	if records, ok, err := matchLiveEventsByGuardedNearIdentityTx(ctx, st.db, liveInput, 75*time.Minute); err != nil {
		t.Fatalf("guarded near match: %v", err)
	} else if !ok {
		t.Fatal("guarded near match input was unexpectedly rejected")
	} else if len(records) != 0 {
		t.Fatalf("guarded near match returned withheld event: %+v", records)
	}
	if records, ok, err := matchLiveEventsByExactIdentityTx(ctx, st.db, withheldInput); err != nil {
		t.Fatalf("withheld exact match input: %v", err)
	} else if ok {
		t.Fatalf("withheld exact match input unexpectedly accepted: %+v", records)
	}
}

func TestUpdateEventRecordFieldsTxDeactivatesExactIdentitiesForWithheldEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	eventStart := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	eventEnd := eventStart.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	eventID := mustInsertExactIdentityEvent(t, st.db, "withheld-deactivation-event", "Withheld deactivation title", venueID, sourceID, eventStart, eventEnd, lastChecked, domain.OriginLive)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, domain.Event{
		Slug:             "withheld-deactivation-event",
		Name:             "Withheld deactivation title",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure active exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}

	withheldEvent := domain.Event{
		Slug:             "withheld-deactivation-event",
		Name:             "Withheld deactivation title",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		End:              eventEnd,
		Genre:            "Indie",
		Status:           "Listed",
		Description:      "Exact identity test event",
		SourceName:       "Store test source",
		SourceURL:        "https://example.test/store-test",
		LastChecked:      time.Date(2026, time.May, 12, 9, 10, 0, 0, time.UTC),
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateWithheld,
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin withheld update tx: %v", err)
	}
	if err := updateEventRecordFieldsTx(ctx, tx, eventID, venueID, sourceID, withheldEvent, time.Date(2026, time.May, 12, 9, 15, 0, 0, time.UTC)); err != nil {
		t.Fatalf("update withheld event record: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit withheld update tx: %v", err)
	}

	rows := mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("exact identity rows after withholding = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("active exact identity after withholding = %d, want 0", rows[0].Active)
	}
	if rows[0].DeactivatedReason != "event is withheld" {
		t.Fatalf("deactivated reason = %q, want %q", rows[0].DeactivatedReason, "event is withheld")
	}
	if rows[0].DeactivatedAt == "" {
		t.Fatal("withheld exact identity missing deactivated_at")
	}
	if _, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, st.db, rows[0].IdentityKey); err != nil {
		t.Fatalf("lookup withheld exact identity after update: %v", err)
	} else if found {
		t.Fatal("withheld exact identity still matched a live event after update")
	}
}

func TestUpdateEventTitleTxRefreshesExactIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	eventStart := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	eventEnd := eventStart.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	eventID := mustInsertExactIdentityEvent(t, st.db, "title-update-event", "Original title", venueID, sourceID, eventStart, eventEnd, lastChecked, domain.OriginLive)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin ensure tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, domain.Event{
		Slug:             "title-update-event",
		Name:             "Original title",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure active exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit ensure tx: %v", err)
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin title update tx: %v", err)
	}
	if err := updateEventTitleTx(ctx, tx, eventID, "title-update-event-renamed", "Updated title", time.Date(2026, time.May, 12, 9, 10, 0, 0, time.UTC)); err != nil {
		t.Fatalf("update event title: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit title update tx: %v", err)
	}

	rows := mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("exact identity rows after title update = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("old exact identity active = %d, want 0", rows[0].Active)
	}
	if rows[1].Active != 1 {
		t.Fatalf("new exact identity active = %d, want 1", rows[1].Active)
	}
}

func TestUpdateCanonicalMatchedEventTxRefreshesExactIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	eventStart := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	eventEnd := eventStart.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	eventID := mustInsertExactIdentityEvent(t, st.db, "canonical-update-event", "Canonical title", venueID, sourceID, eventStart, eventEnd, lastChecked, domain.OriginLive)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin ensure tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, domain.Event{
		Slug:             "canonical-update-event",
		Name:             "Canonical title",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure active exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit ensure tx: %v", err)
	}

	updatedEvent := domain.Event{
		Slug:             "canonical-update-event-renamed",
		Name:             "Canonical title updated",
		VenueSlug:        "leadmill",
		Start:            eventStart,
		End:              eventEnd,
		Genre:            "Indie",
		Status:           "Listed",
		Description:      "Updated canonical event",
		SourceName:       "Store test source",
		SourceURL:        "https://example.test/store-test",
		LastChecked:      time.Date(2026, time.May, 12, 9, 15, 0, 0, time.UTC),
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin canonical update tx: %v", err)
	}
	if _, err := updateCanonicalMatchedEventTx(ctx, tx, eventID, updatedEvent, time.Date(2026, time.May, 12, 9, 15, 0, 0, time.UTC)); err != nil {
		t.Fatalf("update canonical matched event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit canonical update tx: %v", err)
	}

	rows := mustExactIdentityRowsByEvent(t, st.db, eventID)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("exact identity rows after canonical update = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("old exact identity active = %d, want 0", rows[0].Active)
	}
	if rows[1].Active != 1 {
		t.Fatalf("new exact identity active = %d, want 1", rows[1].Active)
	}
}

func TestMergeDuplicateEventRecordTxKeepsOneActiveIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	start := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC)
	targetID := mustInsertExactIdentityEvent(t, st.db, "merge-target", "Merge target title", venueID, sourceID, start, end, lastChecked, domain.OriginLive)
	duplicateID := mustInsertExactIdentityEvent(t, st.db, "merge-duplicate", "Merge duplicate title", venueID, sourceID, start, end, lastChecked, domain.OriginLive)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin target ensure tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, targetID, domain.Event{
		Slug:             "merge-target",
		Name:             "Merge target title",
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure target exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit target ensure tx: %v", err)
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin duplicate ensure tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, duplicateID, domain.Event{
		Slug:             "merge-duplicate",
		Name:             "Merge duplicate title",
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure duplicate exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit duplicate ensure tx: %v", err)
	}

	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin merge tx: %v", err)
	}
	if err := mergeDuplicateEventRecordTx(ctx, tx, duplicateID, targetID, time.Date(2026, time.May, 12, 9, 20, 0, 0, time.UTC)); err != nil {
		t.Fatalf("merge duplicate event record: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit merge tx: %v", err)
	}

	rows := mustExactIdentityRowsByEvent(t, st.db, targetID)
	active := 0
	for _, row := range rows {
		if row.Active == 1 {
			active++
		}
		if row.EventID != targetID {
			t.Fatalf("exact identity still points at duplicate event %d", row.EventID)
		}
	}
	if active != 1 {
		t.Fatalf("active exact identities on survivor = %d, want 1", active)
	}

	var duplicateExists int
	if err := st.db.QueryRow(`SELECT 1 FROM events WHERE id = ?`, duplicateID).Scan(&duplicateExists); err == nil {
		t.Fatal("duplicate event still exists after merge")
	} else if err != sql.ErrNoRows {
		t.Fatalf("lookup duplicate event after merge: %v", err)
	}
}

func mustInsertExactIdentityEvent(t *testing.T, db *sql.DB, slug, name string, venueID, sourceID int64, start, end, lastChecked time.Time, origin domain.Origin) int64 {
	t.Helper()

	res, err := db.Exec(`
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
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, formatRFC3339UTC(start), formatRFC3339UTC(end), "Indie", "Listed", "Exact identity test event", formatRFC3339UTC(lastChecked), string(origin), string(domain.PublicationStateReviewed))
	if err != nil {
		t.Fatalf("insert event %q: %v", slug, err)
	}
	eventID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("event id %q: %v", slug, err)
	}
	return eventID
}

func mustExactIdentityRowsByEvent(t *testing.T, db *sql.DB, eventID int64) []exactIdentityTestRow {
	t.Helper()

	rows, err := db.Query(`
		SELECT id, event_id, identity_key, active, COALESCE(deactivated_at, ''), COALESCE(deactivated_reason, ''), repair_run_id
		FROM event_exact_identities
		WHERE event_id = ?
		ORDER BY id
	`, eventID)
	if err != nil {
		t.Fatalf("query exact identities for event %d: %v", eventID, err)
	}
	defer rows.Close()

	var out []exactIdentityTestRow
	for rows.Next() {
		var row exactIdentityTestRow
		if err := rows.Scan(&row.ID, &row.EventID, &row.IdentityKey, &row.Active, &row.DeactivatedAt, &row.DeactivatedReason, &row.RepairRunID); err != nil {
			t.Fatalf("scan exact identity row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate exact identity rows: %v", err)
	}
	return out
}
