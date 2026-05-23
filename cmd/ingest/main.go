package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/logging"
	seedstore "sheffield-live/internal/store"
	"sheffield-live/internal/store/sqlite"
)

func main() {
	logger, err := logging.NewLoggerFromEnv(os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure logging: %v\n", err)
		os.Exit(1)
	}
	if err := runWithArgsAndLogger(os.Args[1:], os.Stdout, os.Stderr, logger); err != nil {
		logger.Error("ingest exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	return runWithArgs(os.Args[1:], os.Stdout, os.Stderr)
}

type ingestCommandConfig struct {
	source                   string
	allSources               bool
	limit                    int
	timeout                  time.Duration
	httpUserAgent            string
	contact                  string
	dbPath                   string
	stageEventReviewClusters bool
	repairDescriptions       bool
	repairEventTitles        bool
	applyTitleRepairs        bool
	backfillImageFocus       bool
	cleanupStaleSnapshots    bool
	importRunID              int64
	imageFetcher             ingest.Fetcher
	imageStorage             ingest.ImageStorage
}

var (
	openSQLiteStore = sqlite.Open
	newHTTPFetcher  = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return ingest.NewHTTPFetcher(timeout, userAgent)
	}
	newHTTPImageFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return ingest.NewHTTPFetcherWithMaxBodyBytes(timeout, userAgent, ingest.DefaultMaxImageBytes)
	}
	newLocalImageStorage = func(root, urlPrefix string) (ingest.ImageStorage, error) {
		return ingest.NewLocalImageStorage(root, urlPrefix)
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
	nowUTC = func() time.Time {
		return time.Now().UTC()
	}
)

func runWithArgs(args []string, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	logger, err := logging.NewLoggerFromEnv(stderr)
	if err != nil {
		return err
	}
	return runWithArgsAndLogger(args, stdout, stderr, logger)
}

func runWithArgsAndLogger(args []string, stdout, stderr io.Writer, logger *slog.Logger) (err error) {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if logger == nil {
		logger, err = logging.NewLoggerFromEnv(stderr)
		if err != nil {
			return err
		}
	} else {
		logger = logging.EnsureLogger(logger)
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

	started := time.Now()
	summary := newIngestLogSummary(cfg, path)
	logger.Info("ingest starting", summary.startAttrs()...)
	defer func() {
		if summary.Status == "" {
			if err != nil {
				summary.Status = "failed"
			} else {
				summary.Status = "succeeded"
			}
		}
		attrs := summary.finishAttrs(time.Since(started))
		if err != nil {
			attrs = append(attrs, "error", err)
			logger.Error("ingest finished", attrs...)
			return
		}
		logger.Info("ingest finished", attrs...)
	}()

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
			logger.Error("close sqlite store", "error", closeErr)
		}
	}()

	if cfg.backfillImageFocus {
		return runImageFocusBackfill(context.Background(), st, stdout, env("MEDIA_ROOT", "./data/media"))
	}
	if cfg.cleanupStaleSnapshots {
		return runSnapshotCleanup(context.Background(), st, stdout)
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
		if cfg.repairEventTitles {
			return runAllSourcesTitleRepair(context.Background(), st, fetcher, catalog, cfg, stdout, logger, &summary)
		}
		if err := configureImageIngest(&cfg); err != nil {
			return err
		}
		return runAllSources(context.Background(), st, fetcher, catalog, cfg, stdout, logger, &summary)
	}

	var result manualRunExecution
	if cfg.importRunID > 0 {
		report, runErr := replayImportRun(context.Background(), st, catalog, cfg.importRunID, ingest.ReplayOptions{
			Limit: cfg.limit,
		})
		if cfg.repairDescriptions {
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
			return runDescriptionRepair(context.Background(), st, stdout, catalog, report, runErr)
		}
		if cfg.repairEventTitles {
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
			return runEventTitleRepair(context.Background(), st, stdout, catalog, report, runErr, cfg.applyTitleRepairs)
		}
		result = manualRunExecution{Report: report, Err: runErr}
		if cfg.stageEventReviewClusters && !(runErr != nil && report.ImportRunID == 0) {
			stageReport, stageErr := eventReviewClustersForReport(context.Background(), st, catalog, report, runErr)
			result.EventReviewClusters = &stageReport
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
		if cfg.repairDescriptions {
			report, runErr := runLiveManualImportWithRetention(context.Background(), st, fetcher, catalog, ingest.Options{
				Source: cfg.source,
				Limit:  cfg.limit,
			})
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
			repairErr := runDescriptionRepair(context.Background(), st, stdout, catalog, report, runErr)
			runAutomaticSnapshotCleanup(context.Background(), st, logger)
			return repairErr
		}
		if cfg.repairEventTitles {
			report, runErr := runLiveManualImportWithRetention(context.Background(), st, fetcher, catalog, ingest.Options{
				Source: cfg.source,
				Limit:  cfg.limit,
			})
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
			repairErr := runEventTitleRepair(context.Background(), st, stdout, catalog, report, runErr, cfg.applyTitleRepairs)
			runAutomaticSnapshotCleanup(context.Background(), st, logger)
			return repairErr
		}
		if err := configureImageIngest(&cfg); err != nil {
			return err
		}
		result = runSingleManualSource(context.Background(), st, fetcher, catalog, cfg, cfg.source)
	}
	summary.applyManualRun(result)
	if cfg.importRunID == 0 {
		runAutomaticSnapshotCleanup(context.Background(), st, logger)
	}
	return encodeManualRunResult(stdout, cfg.stageEventReviewClusters, result)
}

func configureImageIngest(cfg *ingestCommandConfig) error {
	if cfg == nil {
		return nil
	}
	imageFetcher, err := newHTTPImageFetcher(cfg.timeout, cfg.httpUserAgent)
	if err != nil {
		return err
	}
	imageStorage, err := newLocalImageStorage(env("MEDIA_ROOT", "./data/media"), env("MEDIA_URL_PREFIX", "/media"))
	if err != nil {
		return err
	}
	cfg.imageFetcher = imageFetcher
	cfg.imageStorage = imageStorage
	return nil
}

func runLiveManualImportWithRetention(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
	report, runErr := runManualImport(ctx, st, fetcher, catalog, opts)
	if report.ImportRunID <= 0 {
		return report, runErr
	}
	if err := recordImportRunSnapshotRetention(ctx, st, report); err != nil {
		retentionErr := fmt.Errorf("record snapshot retention metadata: %w", err)
		if runErr != nil {
			return report, errors.Join(runErr, retentionErr)
		}
		return report, retentionErr
	}
	return report, runErr
}

func recordImportRunSnapshotRetention(ctx context.Context, st *sqlite.Store, report ingest.Report) error {
	retention := ingest.SnapshotRetentionForReport(report)
	return st.UpsertImportRunSnapshotRetention(ctx, sqlite.ImportRunSnapshotRetentionInput{
		ImportRunID:         retention.ImportRunID,
		LatestStartAt:       retention.LatestStartAt,
		CandidateCount:      retention.CandidateCount,
		ParseableStartCount: retention.ParseableStartCount,
		RecordedAt:          nowUTC(),
	})
}

func runSnapshotCleanup(ctx context.Context, st *sqlite.Store, stdout io.Writer) error {
	report, err := st.DeleteStaleImportRunSnapshots(ctx, sqlite.SnapshotCleanupOptions{Now: nowUTC()})
	if err != nil {
		return err
	}
	var vacuumErr error
	if report.DeletedSnapshots > 0 {
		if err := st.Vacuum(ctx); err != nil {
			report.VacuumError = err.Error()
			vacuumErr = err
		} else {
			report.Vacuumed = true
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	return vacuumErr
}

func runAutomaticSnapshotCleanup(ctx context.Context, st *sqlite.Store, logger *slog.Logger) {
	logger = logging.EnsureLogger(logger)
	report, err := st.DeleteStaleImportRunSnapshots(ctx, sqlite.SnapshotCleanupOptions{Now: nowUTC()})
	if err != nil {
		logger.Warn("snapshot cleanup failed", "error", err)
		return
	}
	if report.DeletedSnapshots == 0 {
		return
	}
	attrs := []any{
		"deleted_runs", report.DeletedRuns,
		"deleted_snapshots", report.DeletedSnapshots,
	}
	if err := st.Vacuum(ctx); err != nil {
		logger.Warn("snapshot cleanup vacuum failed", append(attrs, "error", err)...)
		return
	}
	logger.Info("snapshot cleanup finished", append(attrs, "vacuumed", true)...)
}

func parseIngestArgs(args []string) (ingestCommandConfig, error) {
	var cfg ingestCommandConfig
	var (
		sourceFlag             trackedStringFlag
		canonicalHTTPUserAgent trackedStringFlag
		aliasHTTPUserAgent     trackedStringFlag
		canonicalStage         trackedBoolFlag
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
	fs.Var(&canonicalStage, "stage-event-reviews", "stage ingest candidates into admin event-review clusters")
	fs.BoolVar(&cfg.repairDescriptions, "repair-descriptions", false, "repair existing event descriptions from ingest candidates without staging or promotion")
	fs.BoolVar(&cfg.repairEventTitles, "repair-event-titles", false, "repair existing event titles from ingest candidates; dry-run unless -apply-title-repairs is set")
	fs.BoolVar(&cfg.applyTitleRepairs, "apply-title-repairs", false, "apply event title repairs instead of reporting a dry-run")
	fs.BoolVar(&cfg.backfillImageFocus, "backfill-image-focus", false, "recompute focus points for copied local images and update stored image metadata")
	fs.BoolVar(&cfg.cleanupStaleSnapshots, "cleanup-stale-snapshots", false, "delete stale import-run snapshots and vacuum the SQLite database")
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
	if canonicalStage.set {
		cfg.stageEventReviewClusters = canonicalStage.value
	}
	if cfg.importRunID < 0 {
		return ingestCommandConfig{}, errors.New("-import-run-id must be positive")
	}
	if cfg.allSources && sourceFlag.set {
		return ingestCommandConfig{}, errors.New("-all-sources and -source are mutually exclusive")
	}
	if cfg.allSources && cfg.importRunID > 0 {
		return ingestCommandConfig{}, errors.New("-all-sources and -import-run-id are mutually exclusive")
	}
	if cfg.repairDescriptions && cfg.stageEventReviewClusters {
		return ingestCommandConfig{}, errors.New("-repair-descriptions and -stage-event-reviews are mutually exclusive")
	}
	if cfg.repairEventTitles && cfg.stageEventReviewClusters {
		return ingestCommandConfig{}, errors.New("-repair-event-titles and -stage-event-reviews are mutually exclusive")
	}
	if cfg.repairDescriptions && cfg.allSources {
		return ingestCommandConfig{}, errors.New("-repair-descriptions and -all-sources are mutually exclusive")
	}
	if cfg.repairEventTitles && cfg.repairDescriptions {
		return ingestCommandConfig{}, errors.New("-repair-event-titles and -repair-descriptions are mutually exclusive")
	}
	if cfg.applyTitleRepairs && !cfg.repairEventTitles {
		return ingestCommandConfig{}, errors.New("-apply-title-repairs requires -repair-event-titles")
	}
	if cfg.repairDescriptions && cfg.importRunID == 0 {
		source := strings.TrimSpace(cfg.source)
		if source != ingest.DefaultSource && source != ingest.CafeNo9Source {
			return ingestCommandConfig{}, errors.New("-repair-descriptions supports only -source sidney-and-matilda or -source cafe-no-9")
		}
	}
	if cfg.backfillImageFocus {
		if cfg.allSources {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -all-sources are mutually exclusive")
		}
		if sourceFlag.set {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -source are mutually exclusive")
		}
		if cfg.importRunID > 0 {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -import-run-id are mutually exclusive")
		}
		if cfg.stageEventReviewClusters {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -stage-event-reviews are mutually exclusive")
		}
		if cfg.repairDescriptions {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -repair-descriptions are mutually exclusive")
		}
		if cfg.repairEventTitles {
			return ingestCommandConfig{}, errors.New("-backfill-image-focus and -repair-event-titles are mutually exclusive")
		}
	}
	if cfg.cleanupStaleSnapshots {
		if cfg.allSources {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -all-sources are mutually exclusive")
		}
		if sourceFlag.set {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -source are mutually exclusive")
		}
		if cfg.importRunID > 0 {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -import-run-id are mutually exclusive")
		}
		if cfg.stageEventReviewClusters {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -stage-event-reviews are mutually exclusive")
		}
		if cfg.repairDescriptions {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -repair-descriptions are mutually exclusive")
		}
		if cfg.repairEventTitles {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -repair-event-titles are mutually exclusive")
		}
		if cfg.backfillImageFocus {
			return ingestCommandConfig{}, errors.New("-cleanup-stale-snapshots and -backfill-image-focus are mutually exclusive")
		}
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

func conflictOnTrackedValues[T comparable](valueSets ...[]T) bool {
	total := 0
	for _, values := range valueSets {
		total += len(values)
	}
	if total == 0 {
		return false
	}
	seen := make(map[T]struct{}, total)
	for _, values := range valueSets {
		for _, value := range values {
			seen[value] = struct{}{}
		}
	}
	return len(seen) > 1
}

type eventReviewClusterStore interface {
	EnsureSource(ctx context.Context, name, sourceURL string) (int64, error)
	StageEventReviewEvidence(ctx context.Context, input seedstore.StageEventReviewEvidenceInput) (seedstore.StageEventReviewEvidenceResult, error)
	PromoteSingletonReviewClusterIfMissing(ctx context.Context, input ingest.ReviewStageClusterInput) (string, bool, error)
	FinalizeOpenEventReviewClusterRestage(ctx context.Context, clusterID int64, evidenceIDs []int64) (*seedstore.EventReviewResolutionSummary, error)
}

type manualIngestReport struct {
	Report              ingest.Report            `json:"report"`
	EventReviewClusters eventReviewClusterReport `json:"review_stage"`
}

type descriptionRepairRunReport struct {
	Report            ingest.Report                  `json:"report"`
	DescriptionRepair sqlite.DescriptionRepairReport `json:"description_repair"`
}

type titleRepairRunReport struct {
	Report      ingest.Report                 `json:"report"`
	TitleRepair sqlite.EventTitleRepairReport `json:"title_repair"`
}

type imageFocusBackfillReport struct {
	Updated        int      `json:"updated"`
	Defaulted      int      `json:"defaulted"`
	MissingFiles   int      `json:"missing_files"`
	DecodeFailures int      `json:"decode_failures"`
	Errors         []string `json:"errors,omitempty"`
}

type manualRunExecution struct {
	Report              ingest.Report
	EventReviewClusters *eventReviewClusterReport
	Err                 error
}

type ingestLogSummary struct {
	Mode                            string
	DBPath                          string
	Source                          string
	AllSources                      bool
	StageEventReviewClusters        bool
	ImportRunID                     int64
	Status                          string
	Totals                          ingest.ReportTotals
	SourceCount                     int
	FailedSourceCount               int
	EventReviewClustersCreated      int
	EventReviewClustersReused       int
	AutoPromoted                    int
	EventReviewClustersAutoResolved int
}

type batchManualIngestReport struct {
	Results []batchManualIngestResult `json:"results"`
}

type batchManualIngestResult struct {
	Source              string                         `json:"source"`
	Report              ingest.Report                  `json:"report"`
	EventReviewClusters *eventReviewClusterReport      `json:"review_stage,omitempty"`
	TitleRepair         *sqlite.EventTitleRepairReport `json:"title_repair,omitempty"`
	Error               string                         `json:"error,omitempty"`
}

type eventReviewClusterReport struct {
	Enabled                              bool                                   `json:"enabled"`
	EventReviewClustersCreated           int                                    `json:"event_review_clusters_created"`
	EventReviewClustersReused            int                                    `json:"event_review_clusters_reused"`
	CandidateCount                       int                                    `json:"candidate_count"`
	ReviewCandidateCount                 int                                    `json:"review_candidate_count"`
	AutoPromotedCount                    int                                    `json:"auto_promoted_count"`
	EventReviewClustersAutoResolvedCount int                                    `json:"event_review_clusters_auto_resolved_count"`
	EventReviewClusters                  []eventReviewClusterReportItem         `json:"event_review_clusters"`
	AutoPromoted                         []eventReviewClusterAutoPromotedReport `json:"auto_promoted"`
	EventReviewClustersAutoResolved      []eventReviewClusterAutoResolvedReport `json:"event_review_clusters_auto_resolved"`
	Errors                               []string                               `json:"errors"`
}

type eventReviewClusterReportItem struct {
	ID                   int64   `json:"-"`
	ClusterID            int64   `json:"cluster_id,omitempty"`
	Title                string  `json:"title"`
	CandidateCount       int     `json:"candidate_count"`
	SourceURL            string  `json:"source_url"`
	Result               string  `json:"result"`
	SupersededClusterIDs []int64 `json:"superseded_cluster_ids,omitempty"`
}

type eventReviewClusterAutoPromotedReport struct {
	Title     string `json:"title"`
	EventSlug string `json:"event_slug"`
	SourceURL string `json:"source_url"`
	Result    string `json:"result"`
}

type eventReviewClusterAutoResolvedReport struct {
	Title              string `json:"title"`
	Result             string `json:"result"`
	ClusterID          int64  `json:"cluster_id,omitempty"`
	CandidateCount     int    `json:"candidate_count"`
	CanonicalEventSlug string `json:"canonical_event_slug,omitempty"`
}

func newIngestLogSummary(cfg ingestCommandConfig, dbPath string) ingestLogSummary {
	return ingestLogSummary{
		Mode:                     ingestMode(cfg),
		DBPath:                   dbPath,
		Source:                   cfg.source,
		AllSources:               cfg.allSources,
		StageEventReviewClusters: cfg.stageEventReviewClusters,
		ImportRunID:              cfg.importRunID,
	}
}

func ingestMode(cfg ingestCommandConfig) string {
	switch {
	case cfg.backfillImageFocus:
		return "image_focus_backfill"
	case cfg.cleanupStaleSnapshots:
		return "snapshot_cleanup"
	case cfg.repairEventTitles && cfg.allSources:
		return "title_repair_all_sources"
	case cfg.allSources:
		return "all_sources"
	case cfg.repairDescriptions && cfg.importRunID > 0:
		return "description_repair_replay"
	case cfg.repairDescriptions:
		return "description_repair_live"
	case cfg.repairEventTitles && cfg.importRunID > 0:
		return "title_repair_replay"
	case cfg.repairEventTitles:
		return "title_repair_live"
	case cfg.importRunID > 0:
		return "replay"
	default:
		return "live"
	}
}

func (s ingestLogSummary) startAttrs() []any {
	return []any{
		"mode", s.Mode,
		"source", s.Source,
		"all_sources", s.AllSources,
		"import_run_id", s.ImportRunID,
		"stage_event_reviews", s.StageEventReviewClusters,
		"db_path", s.DBPath,
	}
}

func (s ingestLogSummary) finishAttrs(duration time.Duration) []any {
	attrs := []any{
		"mode", s.Mode,
		"source", s.Source,
		"all_sources", s.AllSources,
		"import_run_id", s.ImportRunID,
		"status", s.Status,
		"duration", duration,
		"links", s.Totals.Links,
		"snapshots", s.Totals.Snapshots,
		"candidates", s.Totals.Candidates,
		"skips", s.Totals.Skips,
		"errors", s.Totals.Errors,
	}
	if s.SourceCount > 0 {
		attrs = append(attrs,
			"sources", s.SourceCount,
			"failed_sources", s.FailedSourceCount,
		)
	}
	if s.StageEventReviewClusters {
		attrs = append(attrs,
			"event_review_clusters_created", s.EventReviewClustersCreated,
			"event_review_clusters_reused", s.EventReviewClustersReused,
			"auto_promoted", s.AutoPromoted,
			"event_review_clusters_auto_resolved_count", s.EventReviewClustersAutoResolved,
		)
	}
	return attrs
}

func (s *ingestLogSummary) applyManualRun(result manualRunExecution) {
	if s == nil {
		return
	}
	s.Source = result.Report.Source
	s.ImportRunID = result.Report.ImportRunID
	s.Status = result.Report.Status
	s.Totals = result.Report.Totals
	if result.EventReviewClusters != nil {
		s.EventReviewClustersCreated = result.EventReviewClusters.EventReviewClustersCreated
		s.EventReviewClustersReused = result.EventReviewClusters.EventReviewClustersReused
		s.AutoPromoted = result.EventReviewClusters.AutoPromotedCount
		s.EventReviewClustersAutoResolved = result.EventReviewClusters.EventReviewClustersAutoResolvedCount
	}
}

func eventReviewClustersForReport(ctx context.Context, st eventReviewClusterStore, catalog *ingest.Catalog, report ingest.Report, runErr error) (eventReviewClusterReport, error) {
	if runErr != nil {
		return emptyEventReviewClusterReport(), nil
	}
	return createEventReviewClustersFromReport(ctx, st, catalog, report)
}

func runSingleManualSource(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, source string) manualRunExecution {
	report, runErr := runLiveManualImportWithRetention(ctx, st, fetcher, catalog, ingest.Options{
		Source:       source,
		Limit:        cfg.limit,
		ImageFetcher: cfg.imageFetcher,
		ImageStorage: cfg.imageStorage,
	})
	if runErr != nil && report.ImportRunID == 0 {
		return manualRunExecution{Report: report, Err: runErr}
	}
	if !cfg.stageEventReviewClusters {
		return manualRunExecution{Report: report, Err: runErr}
	}
	stageReport, stageErr := eventReviewClustersForReport(ctx, st, catalog, report, runErr)
	if stageErr != nil {
		return manualRunExecution{Report: report, EventReviewClusters: &stageReport, Err: stageErr}
	}
	return manualRunExecution{Report: report, EventReviewClusters: &stageReport, Err: runErr}
}

func encodeManualRunResult(stdout io.Writer, stageEnabled bool, result manualRunExecution) error {
	if result.Err != nil && result.Report.ImportRunID == 0 {
		return result.Err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if stageEnabled {
		stage := emptyEventReviewClusterReport()
		if result.EventReviewClusters != nil {
			stage = *result.EventReviewClusters
		}
		if err := encoder.Encode(manualIngestReport{
			Report:              result.Report,
			EventReviewClusters: stage,
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

func runDescriptionRepair(ctx context.Context, st *sqlite.Store, stdout io.Writer, catalog *ingest.Catalog, report ingest.Report, runErr error) error {
	if runErr != nil && report.ImportRunID == 0 {
		return runErr
	}
	repair := sqlite.DescriptionRepairReport{
		RepairedSlugs:  []string{},
		UnchangedSlugs: []string{},
		SkippedTitles:  []string{},
	}
	if runErr == nil {
		var err error
		repair, err = st.RepairEventDescriptionsFromReport(ctx, catalog, report)
		if err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(descriptionRepairRunReport{
		Report:            report,
		DescriptionRepair: repair,
	}); err != nil {
		return err
	}
	return runErr
}

func runEventTitleRepair(ctx context.Context, st *sqlite.Store, stdout io.Writer, catalog *ingest.Catalog, report ingest.Report, runErr error, apply bool) error {
	if runErr != nil && report.ImportRunID == 0 {
		return runErr
	}
	repair := sqlite.EventTitleRepairReport{
		DryRun:  !apply,
		Applied: apply,
		Changes: []sqlite.EventTitleRepairChange{},
	}
	if runErr == nil {
		var err error
		repair, err = st.RepairEventTitlesFromReport(ctx, catalog, report, apply)
		if err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(titleRepairRunReport{
		Report:      report,
		TitleRepair: repair,
	}); err != nil {
		return err
	}
	return runErr
}

func runImageFocusBackfill(ctx context.Context, st *sqlite.Store, stdout io.Writer, mediaRoot string) error {
	assets, err := st.ListImageAssets(ctx)
	if err != nil {
		return err
	}
	report := imageFocusBackfillReport{}
	for _, asset := range assets {
		assetPath, err := localMediaAssetPath(mediaRoot, asset.StoragePath)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", asset.SourceURL, err))
			continue
		}
		body, err := os.ReadFile(assetPath)
		if errors.Is(err, os.ErrNotExist) {
			report.MissingFiles++
			continue
		}
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: read image: %v", asset.SourceURL, err))
			continue
		}
		focus, err := ingest.EstimateImageFocusWithinLimits(asset.ContentType, body)
		if err != nil {
			report.Defaulted++
			if !errors.Is(err, ingest.ErrImageFocusUnsupported) && !errors.Is(err, ingest.ErrImageFocusTooLarge) && !errors.Is(err, ingest.ErrImageFocusNoSignal) {
				report.DecodeFailures++
			}
		}
		if err := st.UpdateImageAssetFocus(ctx, asset.SourceURL, focus); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: update focus: %v", asset.SourceURL, err))
			continue
		}
		report.Updated++
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func localMediaAssetPath(mediaRoot, storagePath string) (string, error) {
	mediaRoot = strings.TrimSpace(mediaRoot)
	if mediaRoot == "" {
		return "", errors.New("media root is required")
	}
	storagePath = strings.TrimSpace(storagePath)
	if storagePath == "" {
		return "", errors.New("storage path is required")
	}
	clean := filepath.Clean(filepath.FromSlash(storagePath))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid storage path %q", storagePath)
	}
	return filepath.Join(mediaRoot, clean), nil
}

func runAllSources(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	logger = logging.EnsureLogger(logger)
	results := make([]batchManualIngestResult, 0, len(catalog.Keys()))
	var failed bool
	failedSources := 0
	for _, source := range catalog.Keys() {
		result := runSingleManualSource(ctx, st, fetcher, catalog, cfg, source)
		batchResult := batchManualIngestResult{
			Source: source,
			Report: result.Report,
		}
		if result.EventReviewClusters != nil {
			stageCopy := *result.EventReviewClusters
			batchResult.EventReviewClusters = &stageCopy
		}
		if result.Err != nil {
			batchResult.Error = result.Err.Error()
			failed = true
			failedSources++
		}
		results = append(results, batchResult)
		logIngestSourceFinished(logger, source, result)
	}
	if summary != nil {
		summary.Source = ""
		summary.Status = "succeeded"
		summary.SourceCount = len(results)
		summary.FailedSourceCount = failedSources
		if failed {
			summary.Status = "failed"
		}
		for _, result := range results {
			summary.Totals.Links += result.Report.Totals.Links
			summary.Totals.Snapshots += result.Report.Totals.Snapshots
			summary.Totals.Candidates += result.Report.Totals.Candidates
			summary.Totals.Skips += result.Report.Totals.Skips
			summary.Totals.Errors += result.Report.Totals.Errors
			if result.EventReviewClusters != nil {
				summary.EventReviewClustersCreated += result.EventReviewClusters.EventReviewClustersCreated
				summary.EventReviewClustersReused += result.EventReviewClusters.EventReviewClustersReused
				summary.AutoPromoted += result.EventReviewClusters.AutoPromotedCount
				summary.EventReviewClustersAutoResolved += result.EventReviewClusters.EventReviewClustersAutoResolvedCount
			}
		}
	}

	runAutomaticSnapshotCleanup(ctx, st, logger)

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

func runAllSourcesTitleRepair(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	logger = logging.EnsureLogger(logger)
	results := make([]batchManualIngestResult, 0, len(catalog.Keys()))
	var failed bool
	failedSources := 0
	for _, source := range catalog.Keys() {
		report, runErr := runLiveManualImportWithRetention(ctx, st, fetcher, catalog, ingest.Options{
			Source: source,
			Limit:  cfg.limit,
		})
		batchResult := batchManualIngestResult{
			Source: source,
			Report: report,
		}
		if runErr == nil {
			repair, err := st.RepairEventTitlesFromReport(ctx, catalog, report, cfg.applyTitleRepairs)
			if err != nil {
				runErr = err
			} else {
				batchResult.TitleRepair = &repair
			}
		}
		if runErr != nil {
			batchResult.Error = runErr.Error()
			failed = true
			failedSources++
		}
		results = append(results, batchResult)
		logIngestSourceFinished(logger, source, manualRunExecution{Report: report, Err: runErr})
	}
	if summary != nil {
		summary.Source = ""
		summary.Status = "succeeded"
		summary.SourceCount = len(results)
		summary.FailedSourceCount = failedSources
		if failed {
			summary.Status = "failed"
		}
		for _, result := range results {
			summary.Totals.Links += result.Report.Totals.Links
			summary.Totals.Snapshots += result.Report.Totals.Snapshots
			summary.Totals.Candidates += result.Report.Totals.Candidates
			summary.Totals.Skips += result.Report.Totals.Skips
			summary.Totals.Errors += result.Report.Totals.Errors
		}
	}

	runAutomaticSnapshotCleanup(ctx, st, logger)

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(batchManualIngestReport{Results: results}); err != nil {
		return err
	}
	if failed {
		return errors.New("one or more source title repairs failed")
	}
	return nil
}

func logIngestSourceFinished(logger *slog.Logger, source string, result manualRunExecution) {
	attrs := []any{
		"source", source,
		"import_run_id", result.Report.ImportRunID,
		"status", result.Report.Status,
		"links", result.Report.Totals.Links,
		"snapshots", result.Report.Totals.Snapshots,
		"candidates", result.Report.Totals.Candidates,
		"skips", result.Report.Totals.Skips,
		"errors", result.Report.Totals.Errors,
	}
	if result.EventReviewClusters != nil {
		attrs = append(attrs,
			"event_review_clusters_created", result.EventReviewClusters.EventReviewClustersCreated,
			"event_review_clusters_reused", result.EventReviewClusters.EventReviewClustersReused,
			"auto_promoted", result.EventReviewClusters.AutoPromotedCount,
			"event_review_clusters_auto_resolved_count", result.EventReviewClusters.EventReviewClustersAutoResolvedCount,
		)
	}
	if result.Err != nil {
		attrs = append(attrs, "error", result.Err)
		logger.Warn("ingest source finished", attrs...)
		return
	}
	logger.Info("ingest source finished", attrs...)
}

func createEventReviewClustersFromReport(ctx context.Context, st eventReviewClusterStore, catalog *ingest.Catalog, report ingest.Report) (eventReviewClusterReport, error) {
	clusters := ingest.ReviewClustersFromReportWithCatalog(catalog, report)
	stage := emptyEventReviewClusterReport()
	stage.EventReviewClusters = make([]eventReviewClusterReportItem, 0, len(clusters))
	sourceIDs := make(map[string]int64)
	for _, cluster := range clusters {
		stage.CandidateCount += len(cluster.Candidates)
	}

	for _, cluster := range clusters {
		if len(cluster.Candidates) == 1 {
			eventSlug, applied, err := st.PromoteSingletonReviewClusterIfMissing(ctx, cluster)
			if err != nil {
				message := fmt.Sprintf("auto-promote event-review cluster %q: %v", cluster.Title, err)
				stage.Errors = append(stage.Errors, message)
				return stage, errors.New(message)
			}
			if applied {
				stage.AutoPromotedCount++
				stage.AutoPromoted = append(stage.AutoPromoted, eventReviewClusterAutoPromotedReport{
					Title:     cluster.Title,
					EventSlug: eventSlug,
					SourceURL: cluster.SourceURL,
					Result:    "applied",
				})
				continue
			}
		}

		stage.ReviewCandidateCount += len(cluster.Candidates)
		evidenceInputs := ingest.ReviewStageClusterEventReviewEvidenceInputs(cluster)
		if len(evidenceInputs) == 0 {
			continue
		}
		results := make([]seedstore.StageEventReviewEvidenceResult, 0, len(evidenceInputs))
		for _, evidenceInput := range evidenceInputs {
			sourceID, err := resolveEventReviewClusterSourceID(ctx, st, sourceIDs, evidenceInput.SourceName, evidenceInput.SourceURL)
			if err != nil {
				message := fmt.Sprintf("ensure source for event-review cluster %q: %v", cluster.Title, err)
				stage.Errors = append(stage.Errors, message)
				return stage, errors.New(message)
			}
			evidenceInput.RunRef = seedstore.EventReviewRunRef{
				Kind: seedstore.EventReviewRunKindImport,
				ID:   report.ImportRunID,
			}
			evidenceInput.SourceID = sourceID
			result, err := st.StageEventReviewEvidence(ctx, evidenceInput)
			if err != nil {
				message := fmt.Sprintf("stage event review evidence for %q: %v", cluster.Title, err)
				stage.Errors = append(stage.Errors, message)
				return stage, errors.New(message)
			}
			results = append(results, result)
		}

		reportResult := "reused"
		clusterID := int64(0)
		openClusterID := int64(0)
		openClusterEvidenceIDs := make(map[int64]map[int64]struct{})
		autoResolved := false
		autoResolvedResult := ""
		autoResolvedClusterID := int64(0)
		autoResolvedCanonicalSlug := ""
		for _, result := range results {
			if clusterID == 0 && result.ClusterID != 0 {
				clusterID = result.ClusterID
			}
			if result.ClusterCreated {
				reportResult = "created"
			}
			if result.AutoResolved && result.ClusterStatus != seedstore.EventReviewClusterStatusOpen && !autoResolved {
				autoResolved = true
				autoResolvedResult = result.AutoResolvedResult
				autoResolvedClusterID = result.ClusterID
				autoResolvedCanonicalSlug = result.CanonicalEventSlug
			}
			if result.ClusterStatus != seedstore.EventReviewClusterStatusOpen || result.ClusterID == 0 || result.EvidenceID == 0 {
				continue
			}
			if openClusterID == 0 {
				openClusterID = result.ClusterID
			}
			evidenceIDs, ok := openClusterEvidenceIDs[result.ClusterID]
			if !ok {
				evidenceIDs = make(map[int64]struct{})
				openClusterEvidenceIDs[result.ClusterID] = evidenceIDs
			}
			evidenceIDs[result.EvidenceID] = struct{}{}
		}
		if reportResult == "created" {
			stage.EventReviewClustersCreated++
		} else {
			stage.EventReviewClustersReused++
		}
		if autoResolved {
			stage.EventReviewClustersAutoResolvedCount++
			stage.EventReviewClustersAutoResolved = append(stage.EventReviewClustersAutoResolved, eventReviewClusterAutoResolvedReport{
				Title:              cluster.Title,
				Result:             autoResolvedResult,
				ClusterID:          autoResolvedClusterID,
				CandidateCount:     len(cluster.Candidates),
				CanonicalEventSlug: autoResolvedCanonicalSlug,
			})
		}
		if openClusterID != 0 {
			clusterID = openClusterID
		}
		supersededClusterIDs := mergeStageSupersededClusterIDs(results)
		stage.EventReviewClusters = append(stage.EventReviewClusters, eventReviewClusterReportItem{
			ID:                   clusterID,
			ClusterID:            clusterID,
			Title:                cluster.Title,
			CandidateCount:       len(cluster.Candidates),
			SourceURL:            cluster.SourceURL,
			Result:               reportResult,
			SupersededClusterIDs: supersededClusterIDs,
		})

		if len(openClusterEvidenceIDs) > 0 {
			openClusterIDs := make([]int64, 0, len(openClusterEvidenceIDs))
			for openClusterID := range openClusterEvidenceIDs {
				openClusterIDs = append(openClusterIDs, openClusterID)
			}
			sort.Slice(openClusterIDs, func(i, j int) bool { return openClusterIDs[i] < openClusterIDs[j] })
			for _, openClusterID := range openClusterIDs {
				ids := openClusterEvidenceIDs[openClusterID]
				evidenceIDs := make([]int64, 0, len(ids))
				for evidenceID := range ids {
					evidenceIDs = append(evidenceIDs, evidenceID)
				}
				sort.Slice(evidenceIDs, func(i, j int) bool { return evidenceIDs[i] < evidenceIDs[j] })
				resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, openClusterID, evidenceIDs)
				if err != nil {
					message := fmt.Sprintf("finalize event-review cluster %q: %v", cluster.Title, err)
					stage.Errors = append(stage.Errors, message)
					return stage, errors.New(message)
				}
				if resolution != nil && resolution.AppliedAutoResolution != nil {
					stage.EventReviewClustersAutoResolvedCount++
					stage.EventReviewClustersAutoResolved = append(stage.EventReviewClustersAutoResolved, eventReviewClusterAutoResolvedReport{
						Title:              cluster.Title,
						Result:             resolution.AppliedAutoResolution.Result,
						ClusterID:          resolution.ClusterID,
						CandidateCount:     len(cluster.Candidates),
						CanonicalEventSlug: resolution.AppliedAutoResolution.EventSlug,
					})
				}
			}
		}
	}
	return stage, nil
}

func mergeStageSupersededClusterIDs(results []seedstore.StageEventReviewEvidenceResult) []int64 {
	seen := make(map[int64]struct{})
	var ids []int64
	for _, result := range results {
		for _, id := range result.SupersededClusterIDs {
			if id <= 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids
}

func resolveEventReviewClusterSourceID(ctx context.Context, st eventReviewClusterStore, cache map[string]int64, name, sourceURL string) (int64, error) {
	name = strings.TrimSpace(name)
	sourceURL = strings.TrimSpace(sourceURL)
	key := name + "\x00" + sourceURL
	if sourceID, ok := cache[key]; ok {
		return sourceID, nil
	}
	sourceID, err := st.EnsureSource(ctx, name, sourceURL)
	if err != nil {
		return 0, err
	}
	cache[key] = sourceID
	return sourceID, nil
}

func emptyEventReviewClusterReport() eventReviewClusterReport {
	return eventReviewClusterReport{
		Enabled:                         true,
		EventReviewClusters:             []eventReviewClusterReportItem{},
		AutoPromoted:                    []eventReviewClusterAutoPromotedReport{},
		EventReviewClustersAutoResolved: []eventReviewClusterAutoResolvedReport{},
		Errors:                          []string{},
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
