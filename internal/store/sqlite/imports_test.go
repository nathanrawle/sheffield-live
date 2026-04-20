package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestImportWriteMethods(t *testing.T) {
	ctx := context.Background()
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

	db := mustRawDB(t, path)
	defer db.Close()
	venueCount := mustCount(t, db, "venues")
	eventCount := mustCount(t, db, "events")
	sourceCount := mustCount(t, db, "sources")

	sourceID, err := st.EnsureSource(ctx, "Sidney & Matilda listings", "https://www.sidneyandmatilda.com/")
	if err != nil {
		t.Fatalf("ensure existing source: %v", err)
	}
	if got := mustCount(t, db, "sources"); got != sourceCount {
		t.Fatalf("sources after existing ensure = %d, want %d", got, sourceCount)
	}

	icsSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda Google Calendar ICS", "https://calendar.example.test/basic.ics")
	if err != nil {
		t.Fatalf("ensure ICS source: %v", err)
	}
	if icsSourceID == sourceID {
		t.Fatalf("ICS source ID = existing source ID %d", sourceID)
	}
	if got, want := mustCount(t, db, "sources"), sourceCount+1; got != want {
		t.Fatalf("sources after new ensure = %d, want %d", got, want)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "running", "manual test")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}
	if runID == 0 || startedAt.IsZero() {
		t.Fatalf("runID=%d startedAt=%v", runID, startedAt)
	}

	payload := `{"version":1,"body_base64":"Ym9keQ==","sha256":"abc","truncated":false,"metadata":{"url":"https://example.test","body_bytes":4,"captured_at":"2026-04-20T12:00:00Z"}}`
	capturedAt := time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC)
	snapshotID, storedAt, err := st.CreateSnapshot(ctx, runID, &icsSourceID, capturedAt, payload)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if snapshotID == 0 || !storedAt.Equal(capturedAt) {
		t.Fatalf("snapshotID=%d storedAt=%v", snapshotID, storedAt)
	}

	finishedAt, err := st.FinishImportRun(ctx, runID, "succeeded", "links=1 candidates=0 skips=0 errors=0")
	if err != nil {
		t.Fatalf("finish import run: %v", err)
	}
	if finishedAt.IsZero() {
		t.Fatal("finishedAt is zero")
	}

	assertImportRun(t, db, runID, "succeeded")
	assertSnapshotPayload(t, db, snapshotID, payload)
	if got := mustCount(t, db, "venues"); got != venueCount {
		t.Fatalf("venues = %d, want %d", got, venueCount)
	}
	if got := mustCount(t, db, "events"); got != eventCount {
		t.Fatalf("events = %d, want %d", got, eventCount)
	}
}

func TestFinishImportRunRejectsMissingRun(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := st.FinishImportRun(context.Background(), 999999, "failed", "missing"); err == nil {
		t.Fatal("expected missing import run error")
	}
}

func assertImportRun(t *testing.T, db *sql.DB, id int64, wantStatus string) {
	t.Helper()

	var startedAt string
	var finishedAt string
	var status string
	var notes string
	if err := db.QueryRow(`
		SELECT started_at, finished_at, status, notes
		FROM import_runs
		WHERE id = ?
	`, id).Scan(&startedAt, &finishedAt, &status, &notes); err != nil {
		t.Fatalf("scan import run: %v", err)
	}
	if _, err := time.Parse(time.RFC3339, startedAt); err != nil {
		t.Fatalf("started_at %q is not RFC3339: %v", startedAt, err)
	}
	if _, err := time.Parse(time.RFC3339, finishedAt); err != nil {
		t.Fatalf("finished_at %q is not RFC3339: %v", finishedAt, err)
	}
	if status != wantStatus {
		t.Fatalf("status = %q, want %q", status, wantStatus)
	}
	if notes == "" {
		t.Fatal("notes are empty")
	}
}

func assertSnapshotPayload(t *testing.T, db *sql.DB, id int64, want string) {
	t.Helper()

	var payload string
	if err := db.QueryRow(`
		SELECT payload
		FROM snapshots
		WHERE id = ?
	`, id).Scan(&payload); err != nil {
		t.Fatalf("scan snapshot: %v", err)
	}
	if payload != want {
		t.Fatalf("payload = %q, want %q", payload, want)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
}
