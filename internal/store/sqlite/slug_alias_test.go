package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func TestResolveEventSlugAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := st.db
	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "alias-target-event", venueID, sourceID, domain.OriginLive)

	var targetEventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "alias-target-event").Scan(&targetEventID); err != nil {
		t.Fatalf("lookup target event id: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "old-event-slug", string(seedstore.SlugAliasTargetKindEvent), targetEventID, nil, nil, "event slug fix", "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
		t.Fatalf("insert event alias: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "old-venue-slug", string(seedstore.SlugAliasTargetKindVenue), nil, venueID, nil, "venue slug fix", "2026-05-12T09:01:00Z", "2026-05-12T09:01:00Z"); err != nil {
		t.Fatalf("insert venue alias: %v", err)
	}

	targetSlug, ok, err := st.ResolveEventSlugAlias(context.Background(), "old-event-slug")
	if err != nil {
		t.Fatalf("resolve event alias: %v", err)
	}
	if !ok {
		t.Fatal("event alias not resolved")
	}
	if got, want := targetSlug, "alias-target-event"; got != want {
		t.Fatalf("target slug = %q, want %q", got, want)
	}

	if targetSlug, ok, err := st.ResolveEventSlugAlias(context.Background(), "old-venue-slug"); err != nil {
		t.Fatalf("resolve venue alias: %v", err)
	} else if ok || targetSlug != "" {
		t.Fatalf("venue alias resolved to %q, %v; want not found", targetSlug, ok)
	}

	if targetSlug, ok, err := st.ResolveEventSlugAlias(context.Background(), "missing-alias"); err != nil {
		t.Fatalf("resolve missing alias: %v", err)
	} else if ok || targetSlug != "" {
		t.Fatalf("missing alias resolved to %q, %v; want not found", targetSlug, ok)
	}

	if targetSlug, ok, err := st.ResolveEventSlugAlias(context.Background(), "   "); err != nil {
		t.Fatalf("resolve blank alias: %v", err)
	} else if ok || targetSlug != "" {
		t.Fatalf("blank alias resolved to %q, %v; want not found", targetSlug, ok)
	}
}
