package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		source    = flag.String("source", ingest.DefaultSource, "source to ingest")
		limit     = flag.Int("limit", ingest.DefaultLimit, "maximum ICS links to fetch")
		timeout   = flag.Duration("timeout", 10*time.Second, "HTTP timeout")
		userAgent = flag.String("user-agent", "", "HTTP User-Agent header")
		dbPath    = flag.String("db", "", "SQLite database path")
		fixture   = flag.String("review-fixture", "", "offline ICS fixture path used to create an admin review group")
		title     = flag.String("review-title", "", "title for a review group created from -review-fixture")
		stage     = flag.Bool("stage-review", false, "stage ingest candidates into admin review groups")
	)
	flag.Parse()

	fixtureMode := strings.TrimSpace(*fixture) != ""
	if !fixtureMode {
		if *userAgent == "" {
			return errors.New("-user-agent is required")
		}
		if *limit < 1 || *limit > ingest.MaxLimit {
			return fmt.Errorf("-limit must be between 1 and %d", ingest.MaxLimit)
		}
		if *timeout <= 0 {
			return errors.New("-timeout must be positive")
		}
	}

	path := *dbPath
	if path == "" {
		path = os.Getenv("DB_PATH")
	}
	if path == "" {
		path = "./data/sheffield-live.db"
	}

	st, err := sqlite.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "close sqlite store: %v\n", closeErr)
		}
	}()

	if fixtureMode {
		return createReviewGroupFromFixture(context.Background(), st, *fixture, *title)
	}

	fetcher, err := ingest.NewHTTPFetcher(*timeout, *userAgent)
	if err != nil {
		return err
	}

	report, runErr := ingest.RunManual(context.Background(), st, fetcher, ingest.Options{
		Source: *source,
		Limit:  *limit,
	})
	if runErr != nil && report.ImportRunID == 0 {
		return runErr
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if *stage {
		stageReport, stageErr := reviewStageForReport(context.Background(), st, report, runErr)
		if err := encoder.Encode(manualIngestReport{
			Report:      report,
			ReviewStage: stageReport,
		}); err != nil {
			return err
		}
		if stageErr != nil {
			return stageErr
		}
		return runErr
	}

	if err := encoder.Encode(report); err != nil {
		return err
	}
	return runErr
}

type reviewStageStore interface {
	CreateReviewGroup(ctx context.Context, input review.GroupInput) (int64, error)
}

type manualIngestReport struct {
	Report      ingest.Report     `json:"report"`
	ReviewStage reviewStageReport `json:"review_stage"`
}

type reviewStageReport struct {
	Enabled        bool                     `json:"enabled"`
	GroupsCreated  int                      `json:"groups_created"`
	CandidateCount int                      `json:"candidate_count"`
	Groups         []reviewStageGroupReport `json:"groups"`
	Errors         []string                 `json:"errors"`
}

type reviewStageGroupReport struct {
	ID             int64  `json:"id"`
	Title          string `json:"title"`
	CandidateCount int    `json:"candidate_count"`
	SourceURL      string `json:"source_url"`
}

func reviewStageForReport(ctx context.Context, st reviewStageStore, report ingest.Report, runErr error) (reviewStageReport, error) {
	if runErr != nil {
		return emptyReviewStageReport(), nil
	}
	return createReviewGroupsFromReport(ctx, st, report)
}

func createReviewGroupsFromReport(ctx context.Context, st reviewStageStore, report ingest.Report) (reviewStageReport, error) {
	groups := ingest.ReviewGroupsFromReport(report)
	stage := emptyReviewStageReport()
	stage.Groups = make([]reviewStageGroupReport, 0, len(groups))
	for _, group := range groups {
		stage.CandidateCount += len(group.Candidates)
	}

	for _, group := range groups {
		groupID, err := st.CreateReviewGroup(ctx, group)
		if err != nil {
			message := fmt.Sprintf("create review group %q: %v", group.Title, err)
			stage.Errors = append(stage.Errors, message)
			return stage, errors.New(message)
		}
		stage.Groups = append(stage.Groups, reviewStageGroupReport{
			ID:             groupID,
			Title:          group.Title,
			CandidateCount: len(group.Candidates),
			SourceURL:      group.SourceURL,
		})
		stage.GroupsCreated = len(stage.Groups)
	}
	return stage, nil
}

func emptyReviewStageReport() reviewStageReport {
	return reviewStageReport{
		Enabled: true,
		Groups:  []reviewStageGroupReport{},
		Errors:  []string{},
	}
}

type reviewFixtureReport struct {
	Fixture    string             `json:"fixture"`
	GroupID    int64              `json:"group_id"`
	Candidates int                `json:"candidates"`
	Skips      []ingest.ParseSkip `json:"skips,omitempty"`
	Errors     []string           `json:"errors,omitempty"`
}

func createReviewGroupFromFixture(ctx context.Context, st *sqlite.Store, fixturePath, title string) error {
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		return fmt.Errorf("read review fixture: %w", err)
	}
	parse := ingest.ParseICS(raw)
	sourceURL := "file:" + fixturePath
	sourceName := "Fixture ICS"
	if strings.TrimSpace(title) == "" {
		title = "Fixture review: " + filepath.Base(fixturePath)
	}

	candidates := make([]review.CandidateInput, 0, len(parse.Candidates))
	for _, candidate := range parse.Candidates {
		status := strings.TrimSpace(candidate.Status)
		if strings.EqualFold(status, "CONFIRMED") {
			status = "Listed"
		}
		candidates = append(candidates, review.CandidateInput{
			ExternalID:  candidate.UID,
			Name:        candidate.Summary,
			VenueSlug:   slugFromText(candidate.Location),
			StartAt:     candidate.StartAt,
			EndAt:       candidate.EndAt,
			Genre:       "",
			Status:      status,
			Description: candidate.Description,
			SourceName:  sourceName,
			SourceURL:   firstNonEmpty(candidate.URL, sourceURL),
			Provenance:  provenanceForFixtureCandidate(candidate),
		})
	}

	groupID, err := st.CreateReviewGroup(ctx, review.GroupInput{
		Title:      title,
		SourceName: sourceName,
		SourceURL:  sourceURL,
		Notes:      "Created from offline fixture.",
		Candidates: candidates,
	})
	if err != nil {
		return fmt.Errorf("create review group: %w", err)
	}

	report := reviewFixtureReport{
		Fixture:    fixturePath,
		GroupID:    groupID,
		Candidates: len(candidates),
		Skips:      parse.Skips,
		Errors:     parse.Errors,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func provenanceForFixtureCandidate(candidate ingest.EventCandidate) string {
	if candidate.UID != "" {
		return "fixture UID " + candidate.UID
	}
	if candidate.URL != "" {
		return "fixture URL " + candidate.URL
	}
	return "fixture ICS"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func slugFromText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var out strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		case !lastDash:
			out.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(out.String(), "-")
}
