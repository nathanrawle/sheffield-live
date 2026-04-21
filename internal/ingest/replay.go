package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ReplayStore interface {
	LoadImportRun(ctx context.Context, id int64) (ReplayRun, error)
}

type ReplayRun struct {
	ID         int64
	StartedAt  time.Time
	FinishedAt *time.Time
	Status     string
	Notes      string
	Snapshots  []ReplaySnapshot
}

type ReplaySnapshot struct {
	ID         int64
	SourceID   *int64
	SourceName string
	SourceURL  string
	CapturedAt time.Time
	Payload    string
}

type ReplayOptions struct {
	Limit int
}

func ReplayImportRun(ctx context.Context, st ReplayStore, importRunID int64, opts ReplayOptions) (Report, error) {
	if st == nil {
		return Report{}, errors.New("replay store is nil")
	}
	if importRunID <= 0 {
		return Report{}, errors.New("import run ID is required")
	}
	if opts.Limit == 0 {
		return Report{}, fmt.Errorf("limit must be between 1 and %d", MaxLimit)
	}
	if opts.Limit < 0 || opts.Limit > MaxLimit {
		return Report{}, fmt.Errorf("limit must be between 1 and %d", MaxLimit)
	}

	run, err := st.LoadImportRun(ctx, importRunID)
	if err != nil {
		return Report{}, fmt.Errorf("load import run %d: %w", importRunID, err)
	}
	if run.FinishedAt == nil || run.FinishedAt.IsZero() {
		return Report{}, fmt.Errorf("import run %d is unfinished", importRunID)
	}
	if !strings.EqualFold(strings.TrimSpace(run.Status), importStatusSucceeded) {
		return Report{}, fmt.Errorf("import run %d status is %q, want %q", importRunID, run.Status, importStatusSucceeded)
	}
	if len(run.Snapshots) == 0 {
		return Report{}, fmt.Errorf("import run %d has no snapshots", importRunID)
	}

	decoded := make([]decodedReplaySnapshot, len(run.Snapshots))
	for i, snapshot := range run.Snapshots {
		decoded[i], err = decodeReplaySnapshot(snapshot)
		if err != nil {
			return Report{}, err
		}
	}

	sourceCfg, err := configForSource(DefaultSource)
	if err != nil {
		return Report{}, err
	}
	page, err := replaySourcePageSnapshot(decoded, sourceCfg)
	if err != nil {
		return Report{}, fmt.Errorf("import run %d: %w", importRunID, err)
	}
	pageBaseURL := firstNonEmpty(page.envelope.Metadata.FinalURL, page.envelope.Metadata.URL)
	if pageBaseURL == "" {
		return Report{}, fmt.Errorf("import run %d page snapshot %d has no URL", importRunID, page.snapshot.ID)
	}

	report := Report{
		Source:      sourceCfg.Key,
		SourceURL:   sourceCfg.URL,
		ImportRunID: run.ID,
		StartedAt:   formatTime(run.StartedAt),
		Status:      importStatusRunning,
		Limit:       opts.Limit,
	}
	if run.FinishedAt != nil && !run.FinishedAt.IsZero() {
		report.FinishedAt = formatTime(*run.FinishedAt)
	}
	report.Page = &SnapshotReport{
		ID:         page.snapshot.ID,
		URL:        page.envelope.Metadata.URL,
		FinalURL:   page.envelope.Metadata.FinalURL,
		StatusCode: page.envelope.Metadata.StatusCode,
		BodyBytes:  len(page.body),
		SHA256:     page.envelope.SHA256,
		Truncated:  page.envelope.Truncated,
	}
	report.Totals.Snapshots++

	if page.envelope.Truncated {
		report.Errors = append(report.Errors, "source page response was truncated")
		return replayFinalizeReport(report)
	}
	if !statusIsOK(page.envelope.Metadata.StatusCode) {
		report.Errors = append(report.Errors, fmt.Sprintf("source page returned HTTP %d", page.envelope.Metadata.StatusCode))
		return replayFinalizeReport(report)
	}

	links, err := ExtractSidneyAndMatildaICSLinks(pageBaseURL, page.body, opts.Limit)
	if err != nil {
		report.Errors = append(report.Errors, "extract ICS links: "+err.Error())
		return replayFinalizeReport(report)
	}
	report.Links = append(report.Links, links...)
	report.Totals.Links = len(report.Links)
	if len(report.Links) == 0 {
		report.Errors = append(report.Errors, "no ICS links found")
		return replayFinalizeReport(report)
	}

	snapshotsByURL, err := replayICSSnapshotsByLookupKey(decoded, page.snapshot.ID)
	if err != nil {
		return Report{}, fmt.Errorf("import run %d: %w", importRunID, err)
	}

	for _, link := range report.Links {
		snapshot, ok := snapshotsByURL[replaySnapshotKey(link)]
		if !ok {
			return Report{}, fmt.Errorf("missing ICS snapshot for %q in import run %d", link, importRunID)
		}

		calendar := CalendarReport{
			URL:      link,
			Snapshot: snapshotReportFromEnvelope(snapshot.snapshot, snapshot.envelope, snapshot.body),
		}
		if snapshot.envelope.Truncated {
			calendar.Errors = append(calendar.Errors, "ICS response was truncated")
			report.Calendars = append(report.Calendars, calendar)
			report.Totals.Snapshots++
			continue
		}
		if !statusIsOK(snapshot.envelope.Metadata.StatusCode) {
			calendar.Errors = append(calendar.Errors, fmt.Sprintf("ICS returned HTTP %d", snapshot.envelope.Metadata.StatusCode))
			report.Calendars = append(report.Calendars, calendar)
			report.Totals.Snapshots++
			continue
		}
		parse := ParseICS(snapshot.body)
		calendar.Candidates = parse.Candidates
		calendar.Skips = parse.Skips
		calendar.Errors = append(calendar.Errors, parse.Errors...)
		report.Calendars = append(report.Calendars, calendar)
		report.Totals.Snapshots++
	}

	return replayFinalizeReport(report)
}

type decodedReplaySnapshot struct {
	snapshot ReplaySnapshot
	envelope SnapshotEnvelope
	body     []byte
}

func decodeReplaySnapshot(snapshot ReplaySnapshot) (decodedReplaySnapshot, error) {
	var envelope SnapshotEnvelope
	if err := json.Unmarshal([]byte(snapshot.Payload), &envelope); err != nil {
		return decodedReplaySnapshot{}, fmt.Errorf("decode snapshot %d payload: %w", snapshot.ID, err)
	}
	if envelope.Version != 1 {
		return decodedReplaySnapshot{}, fmt.Errorf("snapshot %d version %d: want 1", snapshot.ID, envelope.Version)
	}

	body, err := base64.StdEncoding.DecodeString(envelope.Body)
	if err != nil {
		return decodedReplaySnapshot{}, fmt.Errorf("decode snapshot %d body: %w", snapshot.ID, err)
	}

	sum := sha256.Sum256(body)
	if got, want := hex.EncodeToString(sum[:]), strings.TrimSpace(envelope.SHA256); got != want {
		return decodedReplaySnapshot{}, fmt.Errorf("snapshot %d hash mismatch: got %s, want %s", snapshot.ID, got, want)
	}

	return decodedReplaySnapshot{
		snapshot: snapshot,
		envelope: envelope,
		body:     body,
	}, nil
}

func replaySourcePageSnapshot(decoded []decodedReplaySnapshot, cfg sourceConfig) (decodedReplaySnapshot, error) {
	var matches []decodedReplaySnapshot
	for _, snapshot := range decoded {
		if strings.TrimSpace(snapshot.snapshot.SourceName) != cfg.Name {
			continue
		}
		if strings.TrimSpace(snapshot.snapshot.SourceURL) != cfg.URL {
			continue
		}
		if strings.TrimSpace(snapshot.envelope.Metadata.URL) != cfg.URL {
			continue
		}
		matches = append(matches, snapshot)
	}
	switch len(matches) {
	case 0:
		return decodedReplaySnapshot{}, fmt.Errorf("no source page snapshot for %q at %q", cfg.Name, cfg.URL)
	case 1:
		return matches[0], nil
	default:
		return decodedReplaySnapshot{}, fmt.Errorf("multiple source page snapshots for %q at %q", cfg.Name, cfg.URL)
	}
}

func snapshotReportFromEnvelope(snapshot ReplaySnapshot, envelope SnapshotEnvelope, body []byte) *SnapshotReport {
	return &SnapshotReport{
		ID:         snapshot.ID,
		URL:        envelope.Metadata.URL,
		FinalURL:   envelope.Metadata.FinalURL,
		StatusCode: envelope.Metadata.StatusCode,
		BodyBytes:  len(body),
		SHA256:     envelope.SHA256,
		Truncated:  envelope.Truncated,
	}
}

func replayFinalizeReport(report Report) (Report, error) {
	report = recalculateReportTotals(report)
	if noUsableCalendar(report.Calendars) {
		report.Errors = append(report.Errors, "no ICS calendars parsed successfully")
		report.Totals.Errors++
	}
	if report.Totals.Errors > 0 {
		report.Status = importStatusFailed
		return report, ErrRunFailed
	}
	report.Status = importStatusSucceeded
	return report, nil
}

func replayICSSnapshotsByLookupKey(decoded []decodedReplaySnapshot, pageSnapshotID int64) (map[string]decodedReplaySnapshot, error) {
	snapshotsByURL := make(map[string]decodedReplaySnapshot, len(decoded)-1)
	for _, snapshot := range decoded {
		if snapshot.snapshot.ID == pageSnapshotID {
			continue
		}
		for _, key := range replaySnapshotLookupKeys(snapshot.envelope.Metadata) {
			if key == "" {
				continue
			}
			if existing, exists := snapshotsByURL[key]; exists {
				return nil, fmt.Errorf("duplicate ICS snapshot lookup key %q for snapshots %d and %d", key, existing.snapshot.ID, snapshot.snapshot.ID)
			}
			snapshotsByURL[key] = snapshot
		}
	}
	return snapshotsByURL, nil
}

func replaySnapshotLookupKeys(metadata SnapshotContentMetadata) []string {
	keys := []string{replaySnapshotKey(metadata.URL)}
	if finalURL := replaySnapshotKey(metadata.FinalURL); finalURL != keys[0] {
		keys = append(keys, finalURL)
	}
	return keys
}

func replaySnapshotKey(value string) string {
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
