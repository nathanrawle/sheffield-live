package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	return runWithArgs(os.Args[1:], os.Stdout, os.Stderr)
}

type ingestCommandConfig struct {
	source            string
	limit             int
	timeout           time.Duration
	httpUserAgent     string
	dbPath            string
	reviewICSFixture  string
	reviewTitle       string
	stageReviewGroups bool
	importRunID       int64
}

func runWithArgs(args []string, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	cfg, err := parseIngestArgs(args)
	if err != nil {
		return err
	}

	path := cfg.dbPath
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
			fmt.Fprintf(stderr, "close sqlite store: %v\n", closeErr)
		}
	}()

	fixtureMode := strings.TrimSpace(cfg.reviewICSFixture) != ""
	if fixtureMode {
		return createReviewGroupFromFixture(context.Background(), st, stdout, cfg.reviewICSFixture, cfg.reviewTitle)
	}

	var (
		report ingest.Report
		runErr error
	)
	if cfg.limit < 1 || cfg.limit > ingest.MaxLimit {
		return fmt.Errorf("-limit must be between 1 and %d", ingest.MaxLimit)
	}
	if cfg.importRunID > 0 {
		report, runErr = ingest.ReplayImportRun(context.Background(), st, cfg.importRunID, ingest.ReplayOptions{
			Limit: cfg.limit,
		})
	} else {
		if cfg.httpUserAgent == "" {
			return errors.New("-http-user-agent is required")
		}
		if cfg.timeout <= 0 {
			return errors.New("-timeout must be positive")
		}

		fetcher, err := ingest.NewHTTPFetcher(cfg.timeout, cfg.httpUserAgent)
		if err != nil {
			return err
		}

		report, runErr = ingest.RunManual(context.Background(), st, fetcher, ingest.Options{
			Source: cfg.source,
			Limit:  cfg.limit,
		})
	}
	if runErr != nil && report.ImportRunID == 0 {
		return runErr
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if cfg.stageReviewGroups {
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

func parseIngestArgs(args []string) (ingestCommandConfig, error) {
	var cfg ingestCommandConfig
	var (
		canonicalHTTPUserAgent trackedStringFlag
		aliasHTTPUserAgent     trackedStringFlag
		canonicalFixture       trackedStringFlag
		aliasFixture           trackedStringFlag
		canonicalStage         trackedBoolFlag
		aliasStage             trackedBoolFlag
	)
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&cfg.source, "source", ingest.DefaultSource, "source to ingest (sidney-and-matilda or yellow-arch)")
	fs.IntVar(&cfg.limit, "limit", ingest.DefaultLimit, "maximum ICS links to fetch")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "HTTP timeout")
	fs.Var(&canonicalHTTPUserAgent, "http-user-agent", "HTTP User-Agent header")
	fs.Var(&aliasHTTPUserAgent, "user-agent", "HTTP User-Agent header")
	fs.StringVar(&cfg.dbPath, "db", "", "SQLite database path")
	fs.Var(&canonicalFixture, "review-ics-fixture", "offline ICS fixture path used to create an admin review group")
	fs.Var(&aliasFixture, "review-fixture", "offline ICS fixture path used to create an admin review group")
	fs.StringVar(&cfg.reviewTitle, "review-title", "", "title for a review group created from -review-ics-fixture")
	fs.Var(&canonicalStage, "stage-review-groups", "stage ingest candidates into admin review groups")
	fs.Var(&aliasStage, "stage-review", "stage ingest candidates into admin review groups")
	fs.Int64Var(&cfg.importRunID, "import-run-id", 0, "replay an existing import run from stored snapshots")

	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if conflictOnTrackedValues(canonicalHTTPUserAgent.values, aliasHTTPUserAgent.values) {
		return ingestCommandConfig{}, errors.New("-http-user-agent and -user-agent must match")
	}
	if canonicalHTTPUserAgent.set {
		cfg.httpUserAgent = canonicalHTTPUserAgent.value
	} else {
		cfg.httpUserAgent = aliasHTTPUserAgent.value
	}
	if conflictOnTrackedValues(canonicalFixture.values, aliasFixture.values) {
		return ingestCommandConfig{}, errors.New("-review-ics-fixture and -review-fixture must match")
	}
	if canonicalFixture.set {
		cfg.reviewICSFixture = canonicalFixture.value
	} else {
		cfg.reviewICSFixture = aliasFixture.value
	}
	if conflictOnTrackedValues(canonicalStage.values, aliasStage.values) {
		return ingestCommandConfig{}, errors.New("-stage-review-groups and -stage-review must match")
	}
	if canonicalStage.set {
		cfg.stageReviewGroups = canonicalStage.value
	} else {
		cfg.stageReviewGroups = aliasStage.value
	}
	if cfg.importRunID < 0 {
		return ingestCommandConfig{}, errors.New("-import-run-id must be positive")
	}
	if strings.TrimSpace(cfg.reviewICSFixture) != "" && cfg.importRunID > 0 {
		return ingestCommandConfig{}, errors.New("-review-ics-fixture and -import-run-id are mutually exclusive")
	}
	return cfg, nil
}

type trackedStringFlag struct {
	value  string
	set    bool
	values []string
}

func (f *trackedStringFlag) String() string {
	return f.value
}

func (f *trackedStringFlag) Set(value string) error {
	f.value = value
	f.set = true
	f.values = append(f.values, value)
	return nil
}

type trackedBoolFlag struct {
	value  bool
	set    bool
	values []bool
}

func (f *trackedBoolFlag) String() string {
	return strconv.FormatBool(f.value)
}

func (f *trackedBoolFlag) IsBoolFlag() bool {
	return true
}

func (f *trackedBoolFlag) Set(value string) error {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	f.value = parsed
	f.set = true
	f.values = append(f.values, parsed)
	return nil
}

func conflictOnTrackedValues[T comparable](canonical, alias []T) bool {
	if len(canonical)+len(alias) == 0 {
		return false
	}
	seen := make(map[T]struct{}, len(canonical)+len(alias))
	for _, value := range canonical {
		seen[value] = struct{}{}
	}
	for _, value := range alias {
		seen[value] = struct{}{}
	}
	return len(seen) > 1
}

type reviewStageStore interface {
	StageReviewGroup(ctx context.Context, input review.GroupInput) (int64, bool, error)
}

type manualIngestReport struct {
	Report      ingest.Report     `json:"report"`
	ReviewStage reviewStageReport `json:"review_stage"`
}

type reviewStageReport struct {
	Enabled        bool                     `json:"enabled"`
	GroupsCreated  int                      `json:"groups_created"`
	GroupsReused   int                      `json:"groups_reused"`
	CandidateCount int                      `json:"candidate_count"`
	Groups         []reviewStageGroupReport `json:"groups"`
	Errors         []string                 `json:"errors"`
}

type reviewStageGroupReport struct {
	ID             int64  `json:"id"`
	Title          string `json:"title"`
	CandidateCount int    `json:"candidate_count"`
	SourceURL      string `json:"source_url"`
	Result         string `json:"result"`
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
		groupID, created, err := st.StageReviewGroup(ctx, group)
		if err != nil {
			message := fmt.Sprintf("stage review group %q: %v", group.Title, err)
			stage.Errors = append(stage.Errors, message)
			return stage, errors.New(message)
		}
		result := "reused"
		if created {
			stage.GroupsCreated++
			result = "created"
		} else {
			stage.GroupsReused++
		}
		stage.Groups = append(stage.Groups, reviewStageGroupReport{
			ID:             groupID,
			Title:          group.Title,
			CandidateCount: len(group.Candidates),
			SourceURL:      group.SourceURL,
			Result:         result,
		})
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

func createReviewGroupFromFixture(ctx context.Context, st *sqlite.Store, stdout io.Writer, fixturePath, title string) error {
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
			VenueSlug:   ingest.VenueSlugFromText(candidate.Location),
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
	encoder := json.NewEncoder(stdout)
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
