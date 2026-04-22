package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/ingest"
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

func TestEnsureSourceReturnsStableID(t *testing.T) {
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
	beforeCount := mustCount(t, db, "sources")

	const sourceName = "Replay source"
	const sourceURL = "https://example.test/replay"

	firstID, err := st.EnsureSource(ctx, sourceName, sourceURL)
	if err != nil {
		t.Fatalf("first ensure source: %v", err)
	}
	secondID, err := st.EnsureSource(ctx, sourceName, sourceURL)
	if err != nil {
		t.Fatalf("second ensure source: %v", err)
	}
	if firstID != secondID {
		t.Fatalf("source IDs differ: first %d second %d", firstID, secondID)
	}
	if got, want := mustCount(t, db, "sources"), beforeCount+1; got != want {
		t.Fatalf("sources rows = %d, want %d", got, want)
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

func TestListImportRunsOrdersByStartedAtAndCountsSnapshots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	olderStarted := time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC)
	tiedStarted := time.Date(2026, time.April, 20, 11, 0, 0, 0, time.UTC)
	newestStarted := time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC)

	insertImportRunSummaryFixture(t, db, 10, olderStarted, olderStarted.Add(time.Minute), "failed", "older")
	insertSnapshotsFixture(t, db, 10, olderStarted, 1)
	insertImportRunSummaryFixture(t, db, 20, tiedStarted, tiedStarted.Add(time.Minute), "succeeded", "tied lower id")
	insertSnapshotsFixture(t, db, 20, tiedStarted, 2)
	insertImportRunSummaryFixture(t, db, 30, tiedStarted, tiedStarted.Add(time.Minute), "failed", "tied higher id")
	insertImportRunSummaryFixture(t, db, 40, newestStarted, newestStarted.Add(time.Minute), "running", "newest")
	insertSnapshotsFixture(t, db, 40, newestStarted, 3)

	runs, err := st.ListImportRuns(ctx, 3)
	if err != nil {
		t.Fatalf("list import runs: %v", err)
	}
	if got, want := len(runs), 3; got != want {
		t.Fatalf("runs = %d, want %d", got, want)
	}

	wantIDs := []int64{40, 30, 20}
	wantSnapshotCounts := []int{3, 0, 2}
	for i := range wantIDs {
		if runs[i].ID != wantIDs[i] {
			t.Fatalf("run[%d].ID = %d, want %d", i, runs[i].ID, wantIDs[i])
		}
		if runs[i].SnapshotCount != wantSnapshotCounts[i] {
			t.Fatalf("run[%d].SnapshotCount = %d, want %d", i, runs[i].SnapshotCount, wantSnapshotCounts[i])
		}
	}
}

func TestListImportRunsRejectsInvalidLimit(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	for _, limit := range []int{0, -1} {
		if _, err := st.ListImportRuns(context.Background(), limit); err == nil {
			t.Fatalf("ListImportRuns(%d) error = nil, want error", limit)
		}
	}
}

func TestLatestSuccessfulImportReturnsLatestFinishedSucceededRun(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	baseStarted := time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC)
	insertImportRunSummaryFixture(t, db, 10, baseStarted, baseStarted.Add(time.Minute), "succeeded", "older success")
	insertSnapshotsFixture(t, db, 10, baseStarted, 1)

	latestFinishedAt := baseStarted.Add(2 * time.Hour)
	insertImportRunSummaryFixture(t, db, 20, baseStarted.Add(time.Hour), latestFinishedAt, " SUCCEEDED ", "latest success")
	insertSnapshotsFixture(t, db, 20, baseStarted.Add(time.Hour), 2)

	insertImportRunSummaryFixture(t, db, 30, baseStarted.Add(2*time.Hour), latestFinishedAt.Add(time.Hour), "failed", "newer failure")
	insertImportRunSummaryFixture(t, db, 40, baseStarted.Add(3*time.Hour), time.Time{}, "succeeded", "unfinished success")

	run, err := st.LatestSuccessfulImport(ctx)
	if err != nil {
		t.Fatalf("latest successful import: %v", err)
	}
	if run == nil {
		t.Fatal("latest successful import = nil, want run")
	}
	if run.ID != 20 {
		t.Fatalf("run ID = %d, want 20", run.ID)
	}
	if run.Status != " SUCCEEDED " {
		t.Fatalf("status = %q, want %q", run.Status, " SUCCEEDED ")
	}
	wantFinishedAt := latestFinishedAt.UTC().Truncate(time.Second)
	if run.FinishedAt == nil || !run.FinishedAt.Equal(wantFinishedAt) {
		t.Fatalf("finished_at = %v, want %v", run.FinishedAt, wantFinishedAt)
	}
	if got, want := run.SnapshotCount, 2; got != want {
		t.Fatalf("snapshot count = %d, want %d", got, want)
	}
}

func TestLatestSuccessfulImportReturnsNilWithoutSuccessfulRun(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	startedAt := time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC)
	insertImportRunSummaryFixture(t, db, 10, startedAt, startedAt.Add(time.Minute), "failed", "failed")
	insertImportRunSummaryFixture(t, db, 20, startedAt.Add(time.Hour), time.Time{}, "running", "running")

	run, err := st.LatestSuccessfulImport(ctx)
	if err != nil {
		t.Fatalf("latest successful import: %v", err)
	}
	if run != nil {
		t.Fatalf("latest successful import = %#v, want nil", run)
	}
}

func TestLoadImportRunReturnsOrderedSnapshots(t *testing.T) {
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

	pageSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda listings", "https://www.sidneyandmatilda.com/")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}
	icsSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda Google Calendar ICS", "https://legacy.example.test/calendar.ics")
	if err != nil {
		t.Fatalf("ensure ICS source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=2 candidates=3 skips=4 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	tieCapturedAt := startedAt.Add(2 * time.Minute)
	icsPayload := mustSnapshotPayload(t, ingest.FetchResult{
		URL:         "https://legacy.example.test/calendar.ics",
		FinalURL:    "https://legacy.example.test/calendar.ics",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/calendar",
		Body:        []byte("BEGIN:VCALENDAR\nEND:VCALENDAR\n"),
		CapturedAt:  tieCapturedAt,
	})

	pagePayload := mustSnapshotPayload(t, ingest.FetchResult{
		URL:         "https://www.sidneyandmatilda.com/",
		FinalURL:    "https://www.sidneyandmatilda.com/events/",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        []byte("<html><body>page</body></html>"),
		CapturedAt:  tieCapturedAt,
	})
	db := mustRawDB(t, path)
	defer db.Close()

	const (
		icsSnapshotID  int64 = 8000
		pageSnapshotID int64 = 9000
	)
	if _, err := db.Exec(`
		INSERT INTO snapshots (id, import_run_id, source_id, captured_at, payload)
		VALUES (?, ?, ?, ?, ?)
	`, pageSnapshotID, runID, pageSourceID, formatRFC3339UTC(tieCapturedAt), pagePayload); err != nil {
		t.Fatalf("insert page snapshot with explicit ID: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO snapshots (id, import_run_id, source_id, captured_at, payload)
		VALUES (?, ?, ?, ?, ?)
	`, icsSnapshotID, runID, icsSourceID, formatRFC3339UTC(tieCapturedAt), icsPayload); err != nil {
		t.Fatalf("insert ICS snapshot with explicit ID: %v", err)
	}

	earlierCapturedAt := startedAt.Add(time.Minute)
	earlierPayload := mustSnapshotPayload(t, ingest.FetchResult{
		URL:         "https://legacy.example.test/earlier.ics",
		FinalURL:    "https://legacy.example.test/earlier.ics",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/calendar",
		Body:        []byte("BEGIN:VCALENDAR\nEND:VCALENDAR\n"),
		CapturedAt:  earlierCapturedAt,
	})
	earlierSnapshotID, _, err := st.CreateSnapshot(ctx, runID, &icsSourceID, earlierCapturedAt, earlierPayload)
	if err != nil {
		t.Fatalf("create earlier snapshot: %v", err)
	}

	finishedAt, err := st.FinishImportRun(ctx, runID, "succeeded", "links=1 candidates=0 skips=0 errors=0")
	if err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	run, err := st.LoadImportRun(ctx, runID)
	if err != nil {
		t.Fatalf("load import run: %v", err)
	}
	if run.ID != runID {
		t.Fatalf("run ID = %d, want %d", run.ID, runID)
	}
	if run.Status != "succeeded" {
		t.Fatalf("run status = %q, want succeeded", run.Status)
	}
	if run.Notes == "" {
		t.Fatal("run notes are empty")
	}
	wantStartedAt := startedAt.UTC().Truncate(time.Second)
	if !run.StartedAt.Equal(wantStartedAt) {
		t.Fatalf("started_at = %v, want %v", run.StartedAt, wantStartedAt)
	}
	wantFinishedAt := finishedAt.UTC().Truncate(time.Second)
	if run.FinishedAt == nil || !run.FinishedAt.Equal(wantFinishedAt) {
		t.Fatalf("finished_at = %v, want %v", run.FinishedAt, wantFinishedAt)
	}
	if got, want := len(run.Snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := run.Snapshots[0].ID, earlierSnapshotID; got != want {
		t.Fatalf("first snapshot ID = %d, want %d", got, want)
	}
	if got, want := run.Snapshots[1].ID, icsSnapshotID; got != want {
		t.Fatalf("second snapshot ID = %d, want %d", got, want)
	}
	if got, want := run.Snapshots[2].ID, pageSnapshotID; got != want {
		t.Fatalf("third snapshot ID = %d, want %d", got, want)
	}
	if got, want := run.Snapshots[2].SourceName, "Sidney & Matilda listings"; got != want {
		t.Fatalf("third snapshot source name = %q, want %q", got, want)
	}
	if got, want := run.Snapshots[2].SourceURL, "https://www.sidneyandmatilda.com/"; got != want {
		t.Fatalf("third snapshot source URL = %q, want %q", got, want)
	}
}

func insertImportRunSummaryFixture(t *testing.T, db *sql.DB, id int64, startedAt, finishedAt time.Time, status, notes string) {
	t.Helper()

	var finishedAtValue any
	if !finishedAt.IsZero() {
		finishedAtValue = formatRFC3339UTC(finishedAt)
	}
	if _, err := db.Exec(`
		INSERT INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, id, formatRFC3339UTC(startedAt), finishedAtValue, status, notes); err != nil {
		t.Fatalf("insert import run %d: %v", id, err)
	}
}

func insertSnapshotsFixture(t *testing.T, db *sql.DB, importRunID int64, capturedAt time.Time, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		if _, err := db.Exec(`
			INSERT INTO snapshots (import_run_id, captured_at, payload)
			VALUES (?, ?, ?)
		`, importRunID, formatRFC3339UTC(capturedAt.Add(time.Duration(i)*time.Second)), "{}"); err != nil {
			t.Fatalf("insert snapshot %d for import run %d: %v", i+1, importRunID, err)
		}
	}
}

func mustSnapshotPayload(t *testing.T, result ingest.FetchResult) string {
	t.Helper()

	payload, err := ingest.NewSnapshotEnvelope(result).JSON()
	if err != nil {
		t.Fatalf("encode snapshot payload: %v", err)
	}
	return payload
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
