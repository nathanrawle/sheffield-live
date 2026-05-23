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
		t.Fatalf("ensure new source: %v", err)
	}
	if got, want := mustCount(t, db, "sources"), sourceCount+1; got != want {
		t.Fatalf("sources after new ensure = %d, want %d", got, want)
	}

	existingSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda listings", "https://www.sidneyandmatilda.com/")
	if err != nil {
		t.Fatalf("ensure existing source: %v", err)
	}
	if existingSourceID != sourceID {
		t.Fatalf("existing source ID = %d, want %d", existingSourceID, sourceID)
	}
	if got, want := mustCount(t, db, "sources"), sourceCount+1; got != want {
		t.Fatalf("sources after existing ensure = %d, want %d", got, want)
	}

	icsSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda Google Calendar ICS", "https://calendar.example.test/basic.ics")
	if err != nil {
		t.Fatalf("ensure ICS source: %v", err)
	}
	if icsSourceID == sourceID {
		t.Fatalf("ICS source ID = existing source ID %d", sourceID)
	}
	if got, want := mustCount(t, db, "sources"), sourceCount+2; got != want {
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

func TestLatestFinishedImportRunReturnsHighestFinishedID(t *testing.T) {
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
	insertImportRunSummaryFixture(t, db, 10, startedAt, startedAt.Add(time.Minute), "succeeded", "older success")
	insertImportRunSummaryFixture(t, db, 30, startedAt.Add(time.Hour), startedAt.Add(2*time.Hour), "failed", "latest finished failure")
	insertImportRunSummaryFixture(t, db, 40, startedAt.Add(3*time.Hour), time.Time{}, "succeeded", "unfinished higher id")
	insertSnapshotsFixture(t, db, 30, startedAt.Add(time.Hour), 2)

	run, err := st.LatestFinishedImportRun(ctx)
	if err != nil {
		t.Fatalf("latest finished import run: %v", err)
	}
	if run == nil {
		t.Fatal("latest finished import run = nil, want run")
	}
	if run.ID != 30 {
		t.Fatalf("run ID = %d, want 30", run.ID)
	}
	if run.Status != "failed" {
		t.Fatalf("status = %q, want failed", run.Status)
	}
	if got, want := run.SnapshotCount, 2; got != want {
		t.Fatalf("snapshot count = %d, want %d", got, want)
	}
}

func TestLatestFinishedImportRunReturnsNilWithoutFinishedRun(t *testing.T) {
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
	insertImportRunSummaryFixture(t, db, 10, startedAt, time.Time{}, "running", "running")

	run, err := st.LatestFinishedImportRun(ctx)
	if err != nil {
		t.Fatalf("latest finished import run: %v", err)
	}
	if run != nil {
		t.Fatalf("latest finished import run = %#v, want nil", run)
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

func TestUpsertImportRunSnapshotRetention(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	startedAt := time.Date(2026, time.May, 10, 10, 0, 0, 0, time.UTC)
	insertImportRunSummaryFixture(t, db, 10, startedAt, startedAt.Add(time.Minute), "succeeded", "retention")

	latest := time.Date(2026, time.May, 12, 18, 30, 0, 0, time.UTC)
	recordedAt := time.Date(2026, time.May, 10, 10, 2, 0, 0, time.UTC)
	if err := st.UpsertImportRunSnapshotRetention(ctx, ImportRunSnapshotRetentionInput{
		ImportRunID:         10,
		LatestStartAt:       &latest,
		CandidateCount:      3,
		ParseableStartCount: 2,
		RecordedAt:          recordedAt,
	}); err != nil {
		t.Fatalf("upsert retention: %v", err)
	}

	var latestText string
	var candidateCount int
	var parseableCount int
	var recordedText string
	if err := db.QueryRow(`
		SELECT latest_start_at, candidate_count, parseable_start_count, recorded_at
		FROM import_run_snapshot_retention
		WHERE import_run_id = 10
	`).Scan(&latestText, &candidateCount, &parseableCount, &recordedText); err != nil {
		t.Fatalf("scan retention: %v", err)
	}
	if latestText != "2026-05-12T18:30:00Z" || recordedText != "2026-05-10T10:02:00Z" {
		t.Fatalf("timestamps = %q/%q, want latest/recorded UTC", latestText, recordedText)
	}
	if candidateCount != 3 || parseableCount != 2 {
		t.Fatalf("counts = %d/%d, want 3/2", candidateCount, parseableCount)
	}

	if err := st.UpsertImportRunSnapshotRetention(ctx, ImportRunSnapshotRetentionInput{
		ImportRunID:         10,
		CandidateCount:      4,
		ParseableStartCount: 0,
		RecordedAt:          recordedAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert retention without parseable starts: %v", err)
	}
	var nullLatest sql.NullString
	if err := db.QueryRow(`
		SELECT latest_start_at, candidate_count, parseable_start_count
		FROM import_run_snapshot_retention
		WHERE import_run_id = 10
	`).Scan(&nullLatest, &candidateCount, &parseableCount); err != nil {
		t.Fatalf("scan updated retention: %v", err)
	}
	if nullLatest.Valid {
		t.Fatalf("latest_start_at = %q, want NULL", nullLatest.String)
	}
	if candidateCount != 4 || parseableCount != 0 {
		t.Fatalf("updated counts = %d/%d, want 4/0", candidateCount, parseableCount)
	}
}

func TestImportRunSnapshotRetentionRejectsInvalidCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.UpsertImportRunSnapshotRetention(context.Background(), ImportRunSnapshotRetentionInput{
		ImportRunID:         1,
		CandidateCount:      1,
		ParseableStartCount: 2,
	}); err == nil {
		t.Fatal("upsert invalid counts error = nil, want error")
	}
}

func TestImportRunSnapshotRetentionSchemaConstraintsAndIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()

	indexRows, err := db.Query(`PRAGMA index_list(import_run_snapshot_retention)`)
	if err != nil {
		t.Fatalf("index list: %v", err)
	}
	defer indexRows.Close()
	foundLatestStartIndex := false
	for indexRows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := indexRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		if name == "idx_import_run_snapshot_retention_latest_start" {
			foundLatestStartIndex = true
		}
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate index list: %v", err)
	}
	if !foundLatestStartIndex {
		t.Fatal("missing import_run_snapshot_retention latest_start_at index")
	}

	startedAt := time.Date(2026, time.May, 10, 10, 0, 0, 0, time.UTC)
	insertImportRunSummaryFixture(t, db, 10, startedAt, startedAt.Add(time.Minute), "succeeded", "retention constraints")
	expectInsertErr := func(name string, latestStartAt any, candidateCount, parseableStartCount int, pruneReason string) {
		t.Helper()
		_, err := db.Exec(`
			INSERT INTO import_run_snapshot_retention (
				import_run_id,
				latest_start_at,
				candidate_count,
				parseable_start_count,
				recorded_at,
				prune_reason
			) VALUES (?, ?, ?, ?, ?, ?)
		`, 10, latestStartAt, candidateCount, parseableStartCount, "2026-05-10T10:01:00Z", pruneReason)
		if err == nil {
			t.Fatalf("%s insert succeeded, want CHECK constraint failure", name)
		}
	}

	expectInsertErr("negative candidate count", nil, -1, 0, "")
	expectInsertErr("parseable count above candidate count", nil, 1, 2, "")
	expectInsertErr("latest start with zero parseable starts", "2026-05-10T10:01:00Z", 1, 0, "")
	expectInsertErr("invalid prune reason", nil, 0, 0, "not-a-reason")
}

func TestDeleteStaleImportRunSnapshots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	now := time.Date(2026, time.May, 23, 12, 0, 0, 0, time.UTC)
	london, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Fatalf("load London location: %v", err)
	}

	insertImportRunSummaryFixture(t, db, 10, now.Add(-48*time.Hour), now.Add(-47*time.Hour), "succeeded", "stale bounded")
	insertSnapshotsFixture(t, db, 10, now.Add(-47*time.Hour), 2)
	staleLatest := time.Date(2026, time.May, 22, 21, 0, 0, 0, time.UTC)
	if err := st.UpsertImportRunSnapshotRetention(ctx, ImportRunSnapshotRetentionInput{
		ImportRunID:         10,
		LatestStartAt:       &staleLatest,
		CandidateCount:      2,
		ParseableStartCount: 2,
		RecordedAt:          now.Add(-47 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert stale retention: %v", err)
	}

	insertImportRunSummaryFixture(t, db, 20, now.Add(-48*time.Hour), now.Add(-47*time.Hour), "succeeded", "today bounded")
	insertSnapshotsFixture(t, db, 20, now.Add(-47*time.Hour), 1)
	todayLatest := time.Date(2026, time.May, 22, 23, 30, 0, 0, time.UTC) // 2026-05-23 in Europe/London.
	if err := st.UpsertImportRunSnapshotRetention(ctx, ImportRunSnapshotRetentionInput{
		ImportRunID:         20,
		LatestStartAt:       &todayLatest,
		CandidateCount:      1,
		ParseableStartCount: 1,
		RecordedAt:          now.Add(-47 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert today retention: %v", err)
	}

	insertImportRunSummaryFixture(t, db, 30, now.Add(-10*24*time.Hour), now.Add(-9*24*time.Hour), "failed", "old unknown no bounds")
	insertSnapshotsFixture(t, db, 30, now.Add(-9*24*time.Hour), 3)

	insertImportRunSummaryFixture(t, db, 40, now.Add(-10*24*time.Hour), now.Add(-9*24*time.Hour), "succeeded", "old unknown no starts")
	insertSnapshotsFixture(t, db, 40, now.Add(-9*24*time.Hour), 4)
	if err := st.UpsertImportRunSnapshotRetention(ctx, ImportRunSnapshotRetentionInput{
		ImportRunID:         40,
		CandidateCount:      3,
		ParseableStartCount: 0,
		RecordedAt:          now.Add(-9 * 24 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert unknown retention: %v", err)
	}

	insertImportRunSummaryFixture(t, db, 50, now.Add(-24*time.Hour), now.Add(-23*time.Hour), "succeeded", "young unknown")
	insertSnapshotsFixture(t, db, 50, now.Add(-23*time.Hour), 5)

	insertImportRunSummaryFixture(t, db, 60, now.Add(-10*24*time.Hour), time.Time{}, "running", "unfinished")
	insertSnapshotsFixture(t, db, 60, now.Add(-9*24*time.Hour), 6)

	report, err := st.DeleteStaleImportRunSnapshots(ctx, SnapshotCleanupOptions{
		Now:          now,
		Location:     london,
		UnknownGrace: DefaultSnapshotUnknownRetentionGrace,
	})
	if err != nil {
		t.Fatalf("delete stale snapshots: %v", err)
	}
	if got, want := report.ScannedRuns, 5; got != want {
		t.Fatalf("scanned runs = %d, want %d", got, want)
	}
	if got, want := report.DeletedRuns, 3; got != want {
		t.Fatalf("deleted runs = %d, want %d", got, want)
	}
	if got, want := report.DeletedSnapshots, int64(9); got != want {
		t.Fatalf("deleted snapshots = %d, want %d", got, want)
	}
	for _, runID := range []int64{10, 30, 40} {
		if got := countSnapshotsForRun(t, db, runID); got != 0 {
			t.Fatalf("snapshots for pruned run %d = %d, want 0", runID, got)
		}
	}
	for _, runID := range []int64{20, 50, 60} {
		if got := countSnapshotsForRun(t, db, runID); got == 0 {
			t.Fatalf("snapshots for retained run %d = 0, want retained", runID)
		}
	}
	assertPruneReason(t, db, 10, SnapshotPruneReasonBoundedStale, 2)
	assertPruneReason(t, db, 30, SnapshotPruneReasonUnknownNoBounds, 3)
	assertPruneReason(t, db, 40, SnapshotPruneReasonUnknownNoParseableStart, 4)
	if got := mustCount(t, db, "import_runs"); got < 6 {
		t.Fatalf("import_runs rows = %d, want retained rows", got)
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

func countSnapshotsForRun(t *testing.T, db *sql.DB, importRunID int64) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM snapshots
		WHERE import_run_id = ?
	`, importRunID).Scan(&count); err != nil {
		t.Fatalf("count snapshots for run %d: %v", importRunID, err)
	}
	return count
}

func assertPruneReason(t *testing.T, db *sql.DB, importRunID int64, wantReason string, wantCount int64) {
	t.Helper()

	var reason string
	var count int64
	var prunedAt sql.NullString
	if err := db.QueryRow(`
		SELECT prune_reason, snapshots_pruned_count, snapshots_pruned_at
		FROM import_run_snapshot_retention
		WHERE import_run_id = ?
	`, importRunID).Scan(&reason, &count, &prunedAt); err != nil {
		t.Fatalf("scan prune metadata for run %d: %v", importRunID, err)
	}
	if reason != wantReason {
		t.Fatalf("prune reason for run %d = %q, want %q", importRunID, reason, wantReason)
	}
	if count != wantCount {
		t.Fatalf("pruned count for run %d = %d, want %d", importRunID, count, wantCount)
	}
	if !prunedAt.Valid || prunedAt.String == "" {
		t.Fatalf("snapshots_pruned_at for run %d is empty", importRunID)
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
