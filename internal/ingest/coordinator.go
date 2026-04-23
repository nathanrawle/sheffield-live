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
	Source string
	Limit  int
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
}

type ReportTotals struct {
	Links      int `json:"links"`
	Snapshots  int `json:"snapshots"`
	Candidates int `json:"candidates"`
	Skips      int `json:"skips"`
	Errors     int `json:"errors"`
}

func RunManual(ctx context.Context, st Store, fetcher Fetcher, opts Options) (Report, error) {
	cfg, err := configForSource(opts.Source)
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
		if len(report.Links) == 0 {
			report.Errors = append(report.Errors, "no ICS links found")
			return finishReport(ctx, st, report, importStatusFailed)
		}

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

			parse := ParseICS(icsResult.Body)
			calendar.Candidates = parse.Candidates
			calendar.Skips = parse.Skips
			calendar.Errors = append(calendar.Errors, parse.Errors...)
			report.Calendars = append(report.Calendars, calendar)
		}
	case pageProcessSourcePage:
		parse := pageParse.Parse
		report.Calendars = append(report.Calendars, CalendarReport{
			URL:        pageURL,
			Snapshot:   report.Page,
			Candidates: parse.Candidates,
			Skips:      parse.Skips,
			Errors:     append([]string{}, parse.Errors...),
		})
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
