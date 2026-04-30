package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	allSources        bool
	limit             int
	timeout           time.Duration
	httpUserAgent     string
	contact           string
	dbPath            string
	reviewICSFixture  string
	reviewTitle       string
	stageReviewGroups bool
	importRunID       int64
}

var (
	openSQLiteStore = sqlite.Open
	newHTTPFetcher  = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return ingest.NewHTTPFetcher(timeout, userAgent)
	}
	runManualImport = func(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		return ingest.RunManualWithCatalog(ctx, st, fetcher, catalog, opts)
	}
	replayImportRun = func(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, importRunID int64, opts ingest.ReplayOptions) (ingest.Report, error) {
		return ingest.ReplayImportRunWithCatalog(ctx, st, catalog, importRunID, opts)
	}
	lookupGitUserEmail = func(ctx context.Context) string {
		for _, args := range [][]string{
			{"config", "--get", "user.email"},
			{"config", "--global", "--get", "user.email"},
		} {
			cmd := exec.CommandContext(ctx, "git", args...)
			output, err := cmd.Output()
			if err != nil {
				continue
			}
			email := strings.TrimSpace(string(output))
			if email != "" {
				return email
			}
		}
		return ""
	}
)

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

	catalog, err := ingest.LoadRepoCatalog()
	if err != nil {
		return err
	}

	st, err := openSQLiteStore(path, catalog)
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

	if cfg.limit < 1 || cfg.limit > ingest.MaxLimit {
		return fmt.Errorf("-limit must be between 1 and %d", ingest.MaxLimit)
	}
	if cfg.allSources {
		cfg.httpUserAgent = effectiveHTTPUserAgent(context.Background(), cfg)
		if cfg.timeout <= 0 {
			return errors.New("-timeout must be positive")
		}

		fetcher, err := newHTTPFetcher(cfg.timeout, cfg.httpUserAgent)
		if err != nil {
			return err
		}
		return runAllSources(context.Background(), st, fetcher, catalog, cfg, stdout)
	}

	var result manualRunExecution
	if cfg.importRunID > 0 {
		report, runErr := replayImportRun(context.Background(), st, catalog, cfg.importRunID, ingest.ReplayOptions{
			Limit: cfg.limit,
		})
		result = manualRunExecution{Report: report, Err: runErr}
		if cfg.stageReviewGroups && !(runErr != nil && report.ImportRunID == 0) {
			stageReport, stageErr := reviewStageForReport(context.Background(), st, catalog, report, runErr)
			result.ReviewStage = &stageReport
			if stageErr != nil {
				result.Err = stageErr
			}
		}
	} else {
		cfg.httpUserAgent = effectiveHTTPUserAgent(context.Background(), cfg)
		if cfg.timeout <= 0 {
			return errors.New("-timeout must be positive")
		}

		fetcher, err := newHTTPFetcher(cfg.timeout, cfg.httpUserAgent)
		if err != nil {
			return err
		}
		result = runSingleManualSource(context.Background(), st, fetcher, catalog, cfg, cfg.source)
	}
	return encodeManualRunResult(stdout, cfg.stageReviewGroups, result)
}

func parseIngestArgs(args []string) (ingestCommandConfig, error) {
	var cfg ingestCommandConfig
	var (
		sourceFlag             trackedStringFlag
		canonicalHTTPUserAgent trackedStringFlag
		aliasHTTPUserAgent     trackedStringFlag
		canonicalFixture       trackedStringFlag
		aliasFixture           trackedStringFlag
		canonicalStage         trackedBoolFlag
		aliasStage             trackedBoolFlag
	)
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.Var(&sourceFlag, "source", "source to ingest (sidney-and-matilda, yellow-arch, cafe-no-9, jazz-at-the-lescar, the-greystones, leadmill, or corporation)")
	fs.BoolVar(&cfg.allSources, "all-sources", false, "run all registered manual sources sequentially")
	fs.IntVar(&cfg.limit, "limit", ingest.DefaultLimit, "maximum linked pages to fetch from a source page")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "HTTP timeout")
	fs.Var(&canonicalHTTPUserAgent, "http-user-agent", "HTTP User-Agent header")
	fs.Var(&aliasHTTPUserAgent, "user-agent", "HTTP User-Agent header")
	fs.StringVar(&cfg.contact, "contact", "", "contact detail for the default HTTP User-Agent header; use none|null|false to suppress contact info")
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
	if sourceFlag.set {
		cfg.source = sourceFlag.value
	} else {
		cfg.source = ingest.DefaultSource
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
	if cfg.allSources && sourceFlag.set {
		return ingestCommandConfig{}, errors.New("-all-sources and -source are mutually exclusive")
	}
	if cfg.allSources && cfg.importRunID > 0 {
		return ingestCommandConfig{}, errors.New("-all-sources and -import-run-id are mutually exclusive")
	}
	if cfg.allSources && strings.TrimSpace(cfg.reviewICSFixture) != "" {
		return ingestCommandConfig{}, errors.New("-all-sources and -review-ics-fixture are mutually exclusive")
	}
	return cfg, nil
}

func effectiveHTTPUserAgent(ctx context.Context, cfg ingestCommandConfig) string {
	if strings.TrimSpace(cfg.httpUserAgent) != "" {
		return strings.TrimSpace(cfg.httpUserAgent)
	}
	contact, include := effectiveContact(ctx, cfg.contact)
	if include {
		return fmt.Sprintf("sheffield-live ingest/1.0 (contact: %s)", contact)
	}
	return "sheffield-live ingest/1.0"
}

func effectiveContact(ctx context.Context, contactFlag string) (string, bool) {
	contactFlag = strings.TrimSpace(contactFlag)
	switch {
	case contactFlag == "":
		derived := strings.TrimSpace(lookupGitUserEmail(ctx))
		if derived == "" {
			return "", false
		}
		return derived, true
	case strings.EqualFold(contactFlag, "none"), strings.EqualFold(contactFlag, "null"), strings.EqualFold(contactFlag, "false"):
		return "", false
	default:
		return contactFlag, true
	}
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
	StageReviewGroup(ctx context.Context, input review.GroupInput) (review.StageGroupResult, error)
	PromoteSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput) (string, bool, error)
}

type manualIngestReport struct {
	Report      ingest.Report     `json:"report"`
	ReviewStage reviewStageReport `json:"review_stage"`
}

type manualRunExecution struct {
	Report      ingest.Report
	ReviewStage *reviewStageReport
	Err         error
}

type batchManualIngestReport struct {
	Results []batchManualIngestResult `json:"results"`
}

type batchManualIngestResult struct {
	Source      string             `json:"source"`
	Report      ingest.Report      `json:"report"`
	ReviewStage *reviewStageReport `json:"review_stage,omitempty"`
	Error       string             `json:"error,omitempty"`
}

type reviewStageReport struct {
	Enabled                    bool                                     `json:"enabled"`
	GroupsCreated              int                                      `json:"groups_created"`
	GroupsReused               int                                      `json:"groups_reused"`
	CandidateCount             int                                      `json:"candidate_count"`
	ReviewCandidateCount       int                                      `json:"review_candidate_count"`
	AutoPromotedCount          int                                      `json:"auto_promoted_count"`
	DuplicateAutoResolvedCount int                                      `json:"duplicate_auto_resolved_count"`
	Groups                     []reviewStageGroupReport                 `json:"groups"`
	AutoPromoted               []reviewStageAutoPromotedReport          `json:"auto_promoted"`
	DuplicateAutoResolved      []reviewStageDuplicateAutoResolvedReport `json:"duplicate_auto_resolved"`
	Errors                     []string                                 `json:"errors"`
}

type reviewStageGroupReport struct {
	ID             int64  `json:"id"`
	Title          string `json:"title"`
	CandidateCount int    `json:"candidate_count"`
	SourceURL      string `json:"source_url"`
	Result         string `json:"result"`
}

type reviewStageAutoPromotedReport struct {
	Title     string `json:"title"`
	EventSlug string `json:"event_slug"`
	SourceURL string `json:"source_url"`
	Result    string `json:"result"`
}

type reviewStageDuplicateAutoResolvedReport struct {
	Title              string `json:"title"`
	Result             string `json:"result"`
	ReviewGroupID      int64  `json:"review_group_id"`
	CandidateCount     int    `json:"candidate_count"`
	CanonicalEventSlug string `json:"canonical_event_slug,omitempty"`
}

func reviewStageForReport(ctx context.Context, st reviewStageStore, catalog *ingest.Catalog, report ingest.Report, runErr error) (reviewStageReport, error) {
	if runErr != nil {
		return emptyReviewStageReport(), nil
	}
	return createReviewGroupsFromReport(ctx, st, catalog, report)
}

func runSingleManualSource(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, source string) manualRunExecution {
	report, runErr := runManualImport(ctx, st, fetcher, catalog, ingest.Options{
		Source: source,
		Limit:  cfg.limit,
	})
	if runErr != nil && report.ImportRunID == 0 {
		return manualRunExecution{Report: report, Err: runErr}
	}
	if !cfg.stageReviewGroups {
		return manualRunExecution{Report: report, Err: runErr}
	}
	stageReport, stageErr := reviewStageForReport(ctx, st, catalog, report, runErr)
	if stageErr != nil {
		return manualRunExecution{Report: report, ReviewStage: &stageReport, Err: stageErr}
	}
	return manualRunExecution{Report: report, ReviewStage: &stageReport, Err: runErr}
}

func encodeManualRunResult(stdout io.Writer, stageEnabled bool, result manualRunExecution) error {
	if result.Err != nil && result.Report.ImportRunID == 0 {
		return result.Err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if stageEnabled {
		stage := emptyReviewStageReport()
		if result.ReviewStage != nil {
			stage = *result.ReviewStage
		}
		if err := encoder.Encode(manualIngestReport{
			Report:      result.Report,
			ReviewStage: stage,
		}); err != nil {
			return err
		}
		return result.Err
	}
	if err := encoder.Encode(result.Report); err != nil {
		return err
	}
	return result.Err
}

func runAllSources(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer) error {
	results := make([]batchManualIngestResult, 0, len(catalog.Keys()))
	var failed bool
	for _, source := range catalog.Keys() {
		result := runSingleManualSource(ctx, st, fetcher, catalog, cfg, source)
		batchResult := batchManualIngestResult{
			Source: source,
			Report: result.Report,
		}
		if result.ReviewStage != nil {
			stageCopy := *result.ReviewStage
			batchResult.ReviewStage = &stageCopy
		}
		if result.Err != nil {
			batchResult.Error = result.Err.Error()
			failed = true
		}
		results = append(results, batchResult)
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(batchManualIngestReport{Results: results}); err != nil {
		return err
	}
	if failed {
		return errors.New("one or more source ingests failed")
	}
	return nil
}

func createReviewGroupsFromReport(ctx context.Context, st reviewStageStore, catalog *ingest.Catalog, report ingest.Report) (reviewStageReport, error) {
	groups := ingest.ReviewGroupsFromReportWithCatalog(catalog, report)
	stage := emptyReviewStageReport()
	stage.Groups = make([]reviewStageGroupReport, 0, len(groups))
	for _, group := range groups {
		stage.CandidateCount += len(group.Candidates)
	}

	for _, group := range groups {
		autoPromote := false
		if authoritativeSingletonAutoPromoteEligible(catalog, report.Source, group) {
			autoPromote = true
		} else if nonAuthoritativeSingletonAutoPromoteEligible(catalog, report.Source, group) {
			autoPromote = true
		}
		if autoPromote {
			eventSlug, applied, err := st.PromoteSingletonReviewGroupIfMissing(ctx, group)
			if err != nil {
				message := fmt.Sprintf("auto-promote review group %q: %v", group.Title, err)
				stage.Errors = append(stage.Errors, message)
				return stage, errors.New(message)
			}
			if applied {
				stage.AutoPromotedCount++
				stage.AutoPromoted = append(stage.AutoPromoted, reviewStageAutoPromotedReport{
					Title:     group.Title,
					EventSlug: eventSlug,
					SourceURL: group.SourceURL,
					Result:    "applied",
				})
				continue
			}
		}

		stage.ReviewCandidateCount += len(group.Candidates)
		result, err := st.StageReviewGroup(ctx, group)
		if err != nil {
			message := fmt.Sprintf("stage review group %q: %v", group.Title, err)
			stage.Errors = append(stage.Errors, message)
			return stage, errors.New(message)
		}
		if result.AutoResolved {
			stage.DuplicateAutoResolvedCount++
			stage.DuplicateAutoResolved = append(stage.DuplicateAutoResolved, reviewStageDuplicateAutoResolvedReport{
				Title:              group.Title,
				Result:             result.AutoResolvedResult,
				ReviewGroupID:      result.ID,
				CandidateCount:     len(group.Candidates),
				CanonicalEventSlug: result.CanonicalEventSlug,
			})
			continue
		}

		reportResult := "reused"
		if result.Created {
			stage.GroupsCreated++
			reportResult = "created"
		} else {
			stage.GroupsReused++
		}
		stage.Groups = append(stage.Groups, reviewStageGroupReport{
			ID:             result.ID,
			Title:          group.Title,
			CandidateCount: len(group.Candidates),
			SourceURL:      group.SourceURL,
			Result:         reportResult,
		})
	}
	return stage, nil
}

func emptyReviewStageReport() reviewStageReport {
	return reviewStageReport{
		Enabled:               true,
		Groups:                []reviewStageGroupReport{},
		AutoPromoted:          []reviewStageAutoPromotedReport{},
		DuplicateAutoResolved: []reviewStageDuplicateAutoResolvedReport{},
		Errors:                []string{},
	}
}

func authoritativeSingletonAutoPromoteEligible(catalog *ingest.Catalog, source string, group review.GroupInput) bool {
	if len(group.Candidates) != 1 {
		return false
	}
	ownedVenueSlug := strings.TrimSpace(catalog.OwnedVenueSlugForSource(source))
	if ownedVenueSlug == "" {
		return false
	}
	return strings.TrimSpace(group.Candidates[0].VenueSlug) == ownedVenueSlug
}

func nonAuthoritativeSingletonAutoPromoteEligible(catalog *ingest.Catalog, source string, group review.GroupInput) bool {
	if len(group.Candidates) != 1 {
		return false
	}
	expectedVenueSlug := strings.TrimSpace(catalog.NonAuthoritativeSingletonVenueSlugForSource(source))
	if expectedVenueSlug == "" {
		return false
	}
	return strings.TrimSpace(group.Candidates[0].VenueSlug) == expectedVenueSlug
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
