package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultSource = "sidney-and-matilda"
	DefaultLimit  = 20
	MaxLimit      = 50

	importStatusRunning   = "running"
	importStatusSucceeded = "succeeded"
	importStatusFailed    = "failed"
)

var ErrRunFailed = errors.New("ingest run failed")

type Store interface {
	EnsureSource(ctx context.Context, name, url string) (int64, error)
	CreateImportRun(ctx context.Context, status, notes string) (int64, time.Time, error)
	CreateSnapshot(ctx context.Context, importRunID int64, sourceID *int64, capturedAt time.Time, payload string) (int64, time.Time, error)
	FinishImportRun(ctx context.Context, id int64, status, notes string) (time.Time, error)
}

type Options struct {
	Source       string
	Limit        int
	ImageFetcher Fetcher
	ImageStorage ImageStorage
}

type Report struct {
	Source      string           `json:"source"`
	SourceURL   string           `json:"source_url"`
	ImportRunID int64            `json:"import_run_id"`
	StartedAt   string           `json:"started_at"`
	FinishedAt  string           `json:"finished_at,omitempty"`
	Status      string           `json:"status"`
	Limit       int              `json:"limit"`
	Page        *SnapshotReport  `json:"page,omitempty"`
	Links       []string         `json:"links"`
	Calendars   []CalendarReport `json:"calendars"`
	Totals      ReportTotals     `json:"totals"`
	Errors      []string         `json:"errors,omitempty"`
}

type SnapshotReport struct {
	ID         int64  `json:"id"`
	URL        string `json:"url"`
	FinalURL   string `json:"final_url,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
	BodyBytes  int    `json:"body_bytes"`
	SHA256     string `json:"sha256"`
	Truncated  bool   `json:"truncated"`
}

type CalendarReport struct {
	URL        string           `json:"url"`
	Snapshot   *SnapshotReport  `json:"snapshot,omitempty"`
	Candidates []EventCandidate `json:"candidates"`
	Skips      []ParseSkip      `json:"skips"`
	Errors     []string         `json:"errors,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

type ReportTotals struct {
	Links      int `json:"links"`
	Snapshots  int `json:"snapshots"`
	Candidates int `json:"candidates"`
	Skips      int `json:"skips"`
	Errors     int `json:"errors"`
}

func RunManual(ctx context.Context, st Store, fetcher Fetcher, opts Options) (Report, error) {
	catalog, err := DefaultCatalog()
	if err != nil {
		return Report{}, err
	}
	return RunManualWithCatalog(ctx, st, fetcher, catalog, opts)
}

func RunManualWithCatalog(ctx context.Context, st Store, fetcher Fetcher, catalog *Catalog, opts Options) (Report, error) {
	if catalog == nil {
		return Report{}, errors.New("catalog is nil")
	}
	cfg, err := catalog.configForSource(opts.Source)
	if err != nil {
		return Report{}, err
	}
	if opts.Limit == 0 {
		opts.Limit = DefaultLimit
	}
	if opts.Limit < 0 || opts.Limit > MaxLimit {
		return Report{}, fmt.Errorf("limit must be between 1 and %d", MaxLimit)
	}

	sourceID, err := st.EnsureSource(ctx, cfg.Name, cfg.URL)
	if err != nil {
		return Report{}, fmt.Errorf("ensure source: %w", err)
	}
	runID, startedAt, err := st.CreateImportRun(ctx, importStatusRunning, cfg.ImportRunNotes)
	if err != nil {
		return Report{}, fmt.Errorf("create import run: %w", err)
	}

	report := Report{
		Source:      cfg.Key,
		SourceURL:   cfg.URL,
		ImportRunID: runID,
		StartedAt:   formatTime(startedAt),
		Status:      importStatusRunning,
		Limit:       opts.Limit,
	}

	pageResult, err := fetcher.Fetch(ctx, cfg.URL)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return finishReport(ctx, st, report, importStatusFailed)
	}

	pageSnapshot, err := createSnapshot(ctx, st, runID, sourceID, pageResult)
	if err != nil {
		report.Errors = append(report.Errors, "snapshot source page: "+err.Error())
		return finishReport(ctx, st, report, importStatusFailed)
	}
	report.Page = &pageSnapshot
	report.Totals.Snapshots++

	if pageResult.Truncated {
		report.Errors = append(report.Errors, "source page response was truncated")
		return finishReport(ctx, st, report, importStatusFailed)
	}

	if !statusIsOK(pageResult.StatusCode) {
		report.Errors = append(report.Errors, fmt.Sprintf("source page returned HTTP %d", pageResult.StatusCode))
		return finishReport(ctx, st, report, importStatusFailed)
	}

	pageURL := firstNonEmpty(pageResult.FinalURL, pageResult.URL)
	pageParse, err := parseSourcePage(cfg, pageURL, pageResult.Body, opts.Limit)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return finishReport(ctx, st, report, importStatusFailed)
	}

	switch cfg.PageMode {
	case pageProcessLinkedICS:
		report.Links = pageParse.Links
		report.Totals.Links = len(report.Links)
		roomEvidence := roomEvidenceForSourcePage(cfg, pageURL, pageResult.Body)
		if len(report.Links) == 0 {
			report.Errors = append(report.Errors, "no ICS links found")
			return finishReport(ctx, st, report, importStatusFailed)
		}

		usableICSParsed := false
		detailCandidates := make([]EventCandidate, 0)
		for _, link := range report.Links {
			calendar := CalendarReport{URL: link}
			icsSourceID, err := st.EnsureSource(ctx, cfg.CalendarSourceName, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "ensure source: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			icsResult, err := fetcher.Fetch(ctx, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			snapshot, err := createSnapshot(ctx, st, runID, icsSourceID, icsResult)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "snapshot ICS: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			calendar.Snapshot = &snapshot
			report.Totals.Snapshots++

			if icsResult.Truncated {
				calendar.Errors = append(calendar.Errors, "ICS response was truncated")
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			if !statusIsOK(icsResult.StatusCode) {
				calendar.Errors = append(calendar.Errors, fmt.Sprintf("ICS returned HTTP %d", icsResult.StatusCode))
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			parse := parseICSForSource(cfg, icsResult.Body)
			parse.Candidates = mergeRoomEvidence(parse.Candidates, roomEvidence)
			if len(parse.Candidates) > 0 {
				usableICSParsed = true
			}
			detailCandidates = append(detailCandidates, parse.Candidates...)
			calendar.Candidates = parse.Candidates
			calendar.Skips = parse.Skips
			calendar.Errors = append(calendar.Errors, parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
		}

		if usableICSParsed {
			detailResult := liveDetailDescriptionsForCandidates(ctx, st, fetcher, runID, cfg, detailLinksForSource(cfg, pageURL, pageResult.Body, detailCandidates, opts.Limit))
			report.Totals.Snapshots += detailResult.Snapshots
			for i := range report.Calendars {
				report.Calendars[i].Candidates = mergeDetailDescriptions(report.Calendars[i].Candidates, detailResult.Descriptions)
				copyCalendarCandidateImages(ctx, st, opts, &report.Calendars[i])
			}
		}
	case pageProcessLinkedDetailPages:
		report.Links = pageParse.Links
		report.Totals.Links = len(report.Links)
		if len(report.Links) == 0 {
			report.Errors = append(report.Errors, "no detail page links found")
			return finishReport(ctx, st, report, importStatusFailed)
		}

		for _, link := range report.Links {
			calendar := CalendarReport{URL: link}
			detailSourceName := firstNonEmpty(cfg.LinkedPageSourceName, cfg.Name)
			detailSourceID, err := st.EnsureSource(ctx, detailSourceName, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "ensure source: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			detailResult, err := fetcher.Fetch(ctx, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			snapshot, err := createSnapshot(ctx, st, runID, detailSourceID, detailResult)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "snapshot detail page: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			calendar.Snapshot = &snapshot
			report.Totals.Snapshots++

			if detailResult.Truncated {
				calendar.Errors = append(calendar.Errors, "detail page response was truncated")
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			if !statusIsOK(detailResult.StatusCode) {
				calendar.Errors = append(calendar.Errors, fmt.Sprintf("detail page returned HTTP %d", detailResult.StatusCode))
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			parse, err := parseLinkedPageForSource(cfg, firstNonEmpty(detailResult.FinalURL, detailResult.URL), detailResult.Body)
			if err != nil {
				calendar.Errors = append(calendar.Errors, err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			calendar.Candidates = parse.Candidates
			copyCalendarCandidateImages(ctx, st, opts, &calendar)
			calendar.Skips = parse.Skips
			calendar.Errors = append(calendar.Errors, parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
		}
	case pageProcessSourcePage:
		report.Links = appendUniqueStringsWithLimit(report.Links, opts.Limit, pageParse.Links...)
		detailResult := liveDetailDescriptionsForCandidates(ctx, st, fetcher, runID, cfg, detailLinksForSource(cfg, pageURL, pageResult.Body, pageParse.Parse.Candidates, opts.Limit))
		report.Totals.Snapshots += detailResult.Snapshots
		calendar := CalendarReport{
			URL:        pageURL,
			Snapshot:   report.Page,
			Candidates: mergeDetailDescriptions(pageParse.Parse.Candidates, detailResult.Descriptions),
			Skips:      pageParse.Parse.Skips,
			Errors:     append([]string{}, pageParse.Parse.Errors...),
		}
		copyCalendarCandidateImages(ctx, st, opts, &calendar)
		report.Calendars = append(report.Calendars, calendar)

		seenLinks := make(map[string]struct{}, len(report.Links)+1)
		seenLinks[pageURL] = struct{}{}
		for _, link := range report.Links {
			seenLinks[link] = struct{}{}
		}

		for i := 0; i < len(report.Links); i++ {
			link := report.Links[i]
			calendar := CalendarReport{URL: link}
			pageSourceID, err := st.EnsureSource(ctx, cfg.Name, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "ensure source: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			pageResult, err := fetcher.Fetch(ctx, link)
			if err != nil {
				calendar.Errors = append(calendar.Errors, err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			snapshot, err := createSnapshot(ctx, st, runID, pageSourceID, pageResult)
			if err != nil {
				calendar.Errors = append(calendar.Errors, "snapshot source page: "+err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			calendar.Snapshot = &snapshot
			report.Totals.Snapshots++

			if pageResult.Truncated {
				calendar.Errors = append(calendar.Errors, "source page response was truncated")
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			if !statusIsOK(pageResult.StatusCode) {
				calendar.Errors = append(calendar.Errors, fmt.Sprintf("source page returned HTTP %d", pageResult.StatusCode))
				report.Calendars = append(report.Calendars, calendar)
				continue
			}

			linkedParse, err := parseSourcePage(cfg, firstNonEmpty(pageResult.FinalURL, pageResult.URL), pageResult.Body, opts.Limit)
			if err != nil {
				calendar.Errors = append(calendar.Errors, err.Error())
				report.Calendars = append(report.Calendars, calendar)
				continue
			}
			detailResult := liveDetailDescriptionsForCandidates(ctx, st, fetcher, runID, cfg, detailLinksForSource(cfg, firstNonEmpty(pageResult.FinalURL, pageResult.URL), pageResult.Body, linkedParse.Parse.Candidates, opts.Limit))
			report.Totals.Snapshots += detailResult.Snapshots
			calendar.Candidates = mergeDetailDescriptions(linkedParse.Parse.Candidates, detailResult.Descriptions)
			copyCalendarCandidateImages(ctx, st, opts, &calendar)
			calendar.Skips = linkedParse.Parse.Skips
			calendar.Errors = append(calendar.Errors, linkedParse.Parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)

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
		return Report{}, fmt.Errorf("unsupported source mode %q", cfg.PageMode)
	}

	for _, calendar := range report.Calendars {
		report.Totals.Candidates += len(calendar.Candidates)
		report.Totals.Skips += len(calendar.Skips)
		report.Totals.Errors += len(calendar.Errors)
	}
	report.Totals.Errors += len(report.Errors)

	status := importStatusSucceeded
	if report.Totals.Errors > 0 || noUsableCalendar(report.Calendars) {
		if noUsableCalendar(report.Calendars) {
			report.Errors = append(report.Errors, noUsableListingsMessage(cfg))
			report.Totals.Errors++
		}
		status = importStatusFailed
	}
	return finishReport(ctx, st, report, status)
}

func copyCalendarCandidateImages(ctx context.Context, st Store, opts Options, calendar *CalendarReport) {
	if calendar == nil || len(calendar.Candidates) == 0 {
		return
	}
	candidates, warnings := copyCandidateImages(ctx, st, opts.ImageFetcher, opts.ImageStorage, calendar.Candidates)
	calendar.Candidates = candidates
	calendar.Warnings = append(calendar.Warnings, warnings...)
}

func createSnapshot(ctx context.Context, st Store, runID int64, sourceID int64, result FetchResult) (SnapshotReport, error) {
	envelope := NewSnapshotEnvelope(result)
	payload, err := envelope.JSON()
	if err != nil {
		return SnapshotReport{}, err
	}
	id, _, err := st.CreateSnapshot(ctx, runID, &sourceID, result.CapturedAt, payload)
	if err != nil {
		return SnapshotReport{}, err
	}
	return SnapshotReport{
		ID:         id,
		URL:        result.URL,
		FinalURL:   result.FinalURL,
		StatusCode: result.StatusCode,
		BodyBytes:  len(result.Body),
		SHA256:     envelope.SHA256,
		Truncated:  result.Truncated,
	}, nil
}

func finishReport(ctx context.Context, st Store, report Report, status string) (Report, error) {
	report = recalculateReportTotals(report)
	finishedAt, err := st.FinishImportRun(ctx, report.ImportRunID, status, notesForReport(report))
	if err != nil {
		return report, fmt.Errorf("finish import run: %w", err)
	}
	report.Status = status
	report.FinishedAt = formatTime(finishedAt)
	report = recalculateReportTotals(report)
	if status == importStatusFailed {
		return report, ErrRunFailed
	}
	return report, nil
}

func recalculateReportTotals(report Report) Report {
	report.Totals.Links = len(report.Links)
	report.Totals.Errors = len(report.Errors)
	report.Totals.Candidates = 0
	report.Totals.Skips = 0
	for _, calendar := range report.Calendars {
		report.Totals.Candidates += len(calendar.Candidates)
		report.Totals.Skips += len(calendar.Skips)
		report.Totals.Errors += len(calendar.Errors)
	}
	return report
}

func notesForReport(report Report) string {
	summary := fmt.Sprintf("links=%d candidates=%d skips=%d errors=%d", report.Totals.Links, report.Totals.Candidates, report.Totals.Skips, report.Totals.Errors)
	details := append([]string{}, report.Errors...)
	details = append(details, calendarErrorNotes(report.Calendars, 3)...)
	if len(details) == 0 {
		return summary
	}
	return summary + "; " + strings.Join(details, "; ")
}

func calendarErrorNotes(calendars []CalendarReport, limit int) []string {
	if limit <= 0 {
		return nil
	}
	details := make([]string, 0, min(limit+1, len(calendars)))
	more := 0
	for _, calendar := range calendars {
		if len(calendar.Errors) == 0 {
			continue
		}
		if len(details) < limit {
			details = append(details, calendarErrorNote(calendar))
			continue
		}
		more++
	}
	if more > 0 {
		details = append(details, fmt.Sprintf("... and %d more calendar errors", more))
	}
	return details
}

func calendarErrorNote(calendar CalendarReport) string {
	label := calendar.URL
	if label == "" {
		label = "calendar"
	}
	return fmt.Sprintf("%s: %s", label, calendar.Errors[0])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func appendUniqueStrings(dst []string, values ...string) []string {
	return appendUniqueStringsWithLimit(dst, 0, values...)
}

func appendUniqueStringsWithLimit(dst []string, limit int, values ...string) []string {
	seen := make(map[string]struct{}, len(dst))
	for _, item := range dst {
		seen[item] = struct{}{}
	}
	for _, value := range values {
		if limit > 0 && len(dst) >= limit {
			break
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		dst = append(dst, value)
	}
	return dst
}

func noUsableCalendar(calendars []CalendarReport) bool {
	if len(calendars) == 0 {
		return true
	}
	for _, calendar := range calendars {
		if calendar.Snapshot != nil && len(calendar.Errors) == 0 && len(calendar.Candidates) > 0 {
			return false
		}
	}
	return true
}

func noUsableListingsMessage(cfg sourceConfig) string {
	switch cfg.PageMode {
	case pageProcessLinkedICS:
		return "no ICS calendars parsed successfully"
	case pageProcessSourcePage:
		return "no listings parsed successfully"
	default:
		return "no listings parsed successfully"
	}
}
