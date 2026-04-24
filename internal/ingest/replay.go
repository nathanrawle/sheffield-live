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

	sourceCfg, page, err := detectReplaySourcePageSnapshot(decoded)
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
		return replayFinalizeReport(report, sourceCfg)
	}
	if !statusIsOK(page.envelope.Metadata.StatusCode) {
		report.Errors = append(report.Errors, fmt.Sprintf("source page returned HTTP %d", page.envelope.Metadata.StatusCode))
		return replayFinalizeReport(report, sourceCfg)
	}

	pageParse, err := parseSourcePage(sourceCfg, pageBaseURL, page.body, opts.Limit)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return replayFinalizeReport(report, sourceCfg)
	}

	switch sourceCfg.PageMode {
	case pageProcessLinkedICS:
		report.Links = append(report.Links, pageParse.Links...)
		report.Totals.Links = len(report.Links)
		if len(report.Links) == 0 {
			report.Errors = append(report.Errors, "no ICS links found")
			return replayFinalizeReport(report, sourceCfg)
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
			parse := parseICSForSource(sourceCfg, snapshot.body)
			calendar.Candidates = parse.Candidates
			calendar.Skips = parse.Skips
			calendar.Errors = append(calendar.Errors, parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
			report.Totals.Snapshots++
		}
	case pageProcessLinkedDetailPages:
		report.Links = append(report.Links, pageParse.Links...)
		report.Totals.Links = len(report.Links)
		if len(report.Links) == 0 {
			report.Errors = append(report.Errors, "no detail page links found")
			return replayFinalizeReport(report, sourceCfg)
		}

		snapshotsByURL, err := replaySnapshotsByLookupKey(decoded, page.snapshot.ID, "detail page")
		if err != nil {
			return Report{}, fmt.Errorf("import run %d: %w", importRunID, err)
		}

		for _, link := range report.Links {
			snapshot, ok := snapshotsByURL[replaySnapshotKey(link)]
			if !ok {
				return Report{}, fmt.Errorf("missing detail page snapshot for %q in import run %d", link, importRunID)
			}

			calendar := CalendarReport{
				URL:      link,
				Snapshot: snapshotReportFromEnvelope(snapshot.snapshot, snapshot.envelope, snapshot.body),
			}
			if snapshot.envelope.Truncated {
				calendar.Errors = append(calendar.Errors, "detail page response was truncated")
				report.Calendars = append(report.Calendars, calendar)
				report.Totals.Snapshots++
				continue
			}
			if !statusIsOK(snapshot.envelope.Metadata.StatusCode) {
				calendar.Errors = append(calendar.Errors, fmt.Sprintf("detail page returned HTTP %d", snapshot.envelope.Metadata.StatusCode))
				report.Calendars = append(report.Calendars, calendar)
				report.Totals.Snapshots++
				continue
			}
			parse, err := parseLinkedPageForSource(sourceCfg, firstNonEmpty(snapshot.envelope.Metadata.FinalURL, snapshot.envelope.Metadata.URL), snapshot.body)
			if err != nil {
				return Report{}, fmt.Errorf("import run %d parse linked page %q: %w", importRunID, link, err)
			}
			calendar.Candidates = parse.Candidates
			calendar.Skips = parse.Skips
			calendar.Errors = append(calendar.Errors, parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
			report.Totals.Snapshots++
		}
	case pageProcessSourcePage:
		report.Links = appendUniqueStringsWithLimit(report.Links, opts.Limit, pageParse.Links...)
		report.Calendars = append(report.Calendars, CalendarReport{
			URL:        pageBaseURL,
			Snapshot:   snapshotReportFromEnvelope(page.snapshot, page.envelope, page.body),
			Candidates: pageParse.Parse.Candidates,
			Skips:      pageParse.Parse.Skips,
			Errors:     append([]string{}, pageParse.Parse.Errors...),
		})

		snapshotsByURL, err := replaySourcePageSnapshotsByLookupKey(decoded, page.snapshot.ID, sourceCfg.Name)
		if err != nil {
			return Report{}, fmt.Errorf("import run %d: %w", importRunID, err)
		}
		seenLinks := make(map[string]struct{}, len(report.Links)+1)
		seenLinks[pageBaseURL] = struct{}{}
		for _, link := range report.Links {
			seenLinks[link] = struct{}{}
		}

		for i := 0; i < len(report.Links); i++ {
			link := report.Links[i]
			snapshot, ok := snapshotsByURL[replaySnapshotKey(link)]
			if !ok {
				return Report{}, fmt.Errorf("missing source page snapshot for %q in import run %d", link, importRunID)
			}

			calendar := CalendarReport{
				URL:      link,
				Snapshot: snapshotReportFromEnvelope(snapshot.snapshot, snapshot.envelope, snapshot.body),
			}
			if snapshot.envelope.Truncated {
				calendar.Errors = append(calendar.Errors, "source page response was truncated")
				report.Calendars = append(report.Calendars, calendar)
				report.Totals.Snapshots++
				continue
			}
			if !statusIsOK(snapshot.envelope.Metadata.StatusCode) {
				calendar.Errors = append(calendar.Errors, fmt.Sprintf("source page returned HTTP %d", snapshot.envelope.Metadata.StatusCode))
				report.Calendars = append(report.Calendars, calendar)
				report.Totals.Snapshots++
				continue
			}
			linkedParse, err := parseSourcePage(sourceCfg, firstNonEmpty(snapshot.envelope.Metadata.FinalURL, snapshot.envelope.Metadata.URL), snapshot.body, opts.Limit)
			if err != nil {
				return Report{}, fmt.Errorf("import run %d parse source page %q: %w", importRunID, link, err)
			}
			calendar.Candidates = linkedParse.Parse.Candidates
			calendar.Skips = linkedParse.Parse.Skips
			calendar.Errors = append(calendar.Errors, linkedParse.Parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
			report.Totals.Snapshots++

			for _, next := range linkedParse.Links {
				if opts.Limit > 0 && len(report.Links) >= opts.Limit {
					break
				}
				if next == "" {
					continue
				}
				if _, ok := seenLinks[next]; ok {
					continue
				}
				seenLinks[next] = struct{}{}
				report.Links = append(report.Links, next)
			}
		}
	default:
		return Report{}, fmt.Errorf("import run %d unsupported source mode %q", importRunID, sourceCfg.PageMode)
	}

	return replayFinalizeReport(report, sourceCfg)
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

func replayFinalizeReport(report Report, cfg sourceConfig) (Report, error) {
	report = recalculateReportTotals(report)
	if noUsableCalendar(report.Calendars) {
		report.Errors = append(report.Errors, noUsableListingsMessage(cfg))
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
	return replaySnapshotsByLookupKey(decoded, pageSnapshotID, "ICS")
}

func replaySourcePageSnapshotsByLookupKey(decoded []decodedReplaySnapshot, pageSnapshotID int64, sourceName string) (map[string]decodedReplaySnapshot, error) {
	snapshotsByURL := make(map[string]decodedReplaySnapshot, len(decoded)-1)
	for _, snapshot := range decoded {
		if snapshot.snapshot.ID == pageSnapshotID || strings.TrimSpace(snapshot.snapshot.SourceName) != strings.TrimSpace(sourceName) {
			continue
		}
		for _, key := range replaySnapshotLookupKeys(snapshot.envelope.Metadata) {
			if key == "" {
				continue
			}
			if existing, exists := snapshotsByURL[key]; exists {
				return nil, fmt.Errorf("duplicate source page snapshot lookup key %q for snapshots %d and %d", key, existing.snapshot.ID, snapshot.snapshot.ID)
			}
			snapshotsByURL[key] = snapshot
		}
	}
	return snapshotsByURL, nil
}

func replaySnapshotsByLookupKey(decoded []decodedReplaySnapshot, pageSnapshotID int64, label string) (map[string]decodedReplaySnapshot, error) {
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
				return nil, fmt.Errorf("duplicate %s snapshot lookup key %q for snapshots %d and %d", label, key, existing.snapshot.ID, snapshot.snapshot.ID)
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
