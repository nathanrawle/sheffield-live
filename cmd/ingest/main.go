package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
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
	command                  ingestCommand
	fixKind                  fixCommandKind
	source                   string
	sourceSet                bool
	allSources               bool
	limit                    int
	timeout                  time.Duration
	httpUserAgent            string
	contact                  string
	dbPath                   string
	dryRun                   bool
	stageEventReviewClusters bool
	replayImportRunID        int64
	replayUseLatest          bool
	repairDescriptions       bool
	repairEventTitles        bool
	mediaRoot                string
	mediaURLPrefix           string
	imageFetcher             ingest.Fetcher
	imageStorage             ingest.ImageStorage
}

type ingestCommand string

const (
	ingestCommandLive   ingestCommand = "live"
	ingestCommandReplay ingestCommand = "replay"
	ingestCommandFix    ingestCommand = "fix"
)

type fixCommandKind string

const (
	fixCommandTitles               fixCommandKind = "titles"
	fixCommandDescriptions         fixCommandKind = "descriptions"
	fixCommandHistoricalDuplicates fixCommandKind = "historical-duplicates"
	fixCommandImageFocus           fixCommandKind = "image-focus"
	fixCommandMedia                fixCommandKind = "media"
	fixCommandSnapshots            fixCommandKind = "snapshots"
)

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

	return runConfiguredCommand(context.Background(), st, catalog, cfg, stdout, logger, &summary)
}

func runConfiguredCommand(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	switch cfg.command {
	case ingestCommandLive:
		return runLiveIngestCommand(ctx, st, catalog, cfg, stdout, logger, summary)
	case ingestCommandReplay:
		return runReplayCommand(ctx, st, catalog, cfg, stdout, summary)
	case ingestCommandFix:
		return runFixCommand(ctx, st, catalog, cfg, stdout, logger, summary)
	default:
		return fmt.Errorf("unsupported ingest command %q", cfg.command)
	}
}

func runLiveIngestCommand(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	if err := validateFetchConfig(cfg); err != nil {
		return err
	}
	cfg.httpUserAgent = effectiveHTTPUserAgent(ctx, cfg)
	fetcher, err := newHTTPFetcher(cfg.timeout, cfg.httpUserAgent)
	if err != nil {
		return err
	}
	if err := configureImageIngest(&cfg); err != nil {
		return err
	}
	if cfg.allSources {
		return runAllSources(ctx, st, fetcher, catalog, cfg, stdout, logger, summary)
	}
	result := runSingleManualSource(ctx, st, fetcher, catalog, cfg, cfg.source)
	if summary != nil {
		summary.applyManualRun(result)
	}
	err = encodeManualRunResult(stdout, result)
	if err == nil && !cfg.dryRun {
		runAutomaticMediaCleanup(ctx, st, logger)
	}
	runAutomaticSnapshotCleanup(ctx, st, logger)
	return err
}

func runReplayCommand(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, summary *ingestLogSummary) error {
	if err := validateLimit(cfg.limit); err != nil {
		return err
	}
	importRunID := cfg.replayImportRunID
	if cfg.replayUseLatest {
		latest, err := st.LatestFinishedImportRun(ctx)
		if err != nil {
			return err
		}
		if latest == nil {
			return errors.New("no finished import runs found")
		}
		if strings.EqualFold(strings.TrimSpace(latest.Status), "failed") {
			return fmt.Errorf("latest finished import run %d failed; replay requires a succeeded run", latest.ID)
		}
		importRunID = latest.ID
	}
	report, runErr := replayImportRun(ctx, st, catalog, importRunID, ingest.ReplayOptions{
		Limit: cfg.limit,
	})
	if cfg.repairDescriptions {
		if summary != nil {
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
		}
		return runDescriptionRepair(ctx, st, stdout, catalog, report, runErr, !cfg.dryRun)
	}
	if cfg.repairEventTitles {
		if summary != nil {
			summary.applyManualRun(manualRunExecution{Report: report, Err: runErr})
		}
		return runEventTitleRepair(ctx, st, stdout, catalog, report, runErr, !cfg.dryRun)
	}
	result := manualRunExecution{Report: report, Err: runErr}
	if cfg.stageEventReviewClusters && !(runErr != nil && report.ImportRunID == 0) {
		stageReport, stageErr := eventReviewClustersForReport(ctx, st, catalog, report, runErr)
		result.EventReviewClusters = &stageReport
		if stageErr != nil {
			result.Err = stageErr
		}
	} else {
		stageReport := disabledEventReviewClusterReport()
		result.EventReviewClusters = &stageReport
	}
	if summary != nil {
		summary.applyManualRun(result)
	}
	return encodeManualRunResult(stdout, result)
}

func runFixCommand(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	switch cfg.fixKind {
	case fixCommandTitles:
		return runLiveSourceRepairs(ctx, st, catalog, cfg, repairKindTitles, stdout, logger, summary)
	case fixCommandDescriptions:
		return runLiveSourceRepairs(ctx, st, catalog, cfg, repairKindDescriptions, stdout, logger, summary)
	case fixCommandHistoricalDuplicates:
		return runHistoricalDuplicateRepair(ctx, st, stdout, !cfg.dryRun)
	case fixCommandImageFocus:
		mediaRoot := strings.TrimSpace(cfg.mediaRoot)
		if mediaRoot == "" {
			mediaRoot = env("MEDIA_ROOT", "./data/media")
		}
		return runImageFocusBackfill(ctx, st, stdout, mediaRoot, !cfg.dryRun)
	case fixCommandMedia:
		mediaRoot := strings.TrimSpace(cfg.mediaRoot)
		if mediaRoot == "" {
			mediaRoot = env("MEDIA_ROOT", "./data/media")
		}
		mediaURLPrefix := strings.TrimSpace(cfg.mediaURLPrefix)
		if mediaURLPrefix == "" {
			mediaURLPrefix = env("MEDIA_URL_PREFIX", "/media")
		}
		return runMediaCleanup(ctx, st, stdout, mediaRoot, mediaURLPrefix, !cfg.dryRun)
	case fixCommandSnapshots:
		return runSnapshotCleanup(ctx, st, stdout)
	default:
		return fmt.Errorf("unsupported fix command %q", cfg.fixKind)
	}
}

func validateFetchConfig(cfg ingestCommandConfig) error {
	if err := validateLimit(cfg.limit); err != nil {
		return err
	}
	if cfg.timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	return nil
}

func validateLimit(limit int) error {
	if limit < 1 || limit > ingest.MaxLimit {
		return fmt.Errorf("-limit must be between 1 and %d", ingest.MaxLimit)
	}
	return nil
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
	if globalDB, rest, ok, err := leadingGlobalDBForSubcommand(args); err != nil {
		return ingestCommandConfig{}, err
	} else if ok {
		return parseSubcommandArgs(rest, globalDB)
	}
	if len(args) > 0 {
		switch args[0] {
		case string(ingestCommandReplay), string(ingestCommandFix):
			return parseSubcommandArgs(args, trackedStringFlag{})
		}
	}
	return parseLiveIngestArgs(args, trackedStringFlag{})
}

func parseSubcommandArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	if len(args) == 0 {
		return ingestCommandConfig{}, errors.New("subcommand is required")
	}
	switch args[0] {
	case string(ingestCommandReplay):
		return parseReplayArgs(args[1:], globalDB)
	case string(ingestCommandFix):
		return parseFixArgs(args[1:], globalDB)
	default:
		return ingestCommandConfig{}, fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func parseLiveIngestArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandLive)
	var sourceFlag, userAgentFlag, dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest")
	fs.Var(&sourceFlag, "source", "source to ingest")
	fs.IntVar(&cfg.limit, "limit", ingest.DefaultLimit, "maximum linked pages to fetch from a source page")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "HTTP timeout")
	fs.Var(&userAgentFlag, "user-agent", "HTTP User-Agent header")
	fs.StringVar(&cfg.contact, "contact", "", "contact detail for the default HTTP User-Agent header; use none|null|false to suppress contact info")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "skip event-review staging; import snapshots are still written")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	cfg.httpUserAgent = userAgentFlag.value
	cfg.sourceSet = sourceFlag.set
	cfg.allSources = !sourceFlag.set
	if sourceFlag.set {
		cfg.source = sourceFlag.value
	}
	cfg.stageEventReviewClusters = !cfg.dryRun
	return cfg, nil
}

func parseReplayArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandReplay)
	cfg.replayUseLatest = true
	var dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest replay")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.IntVar(&cfg.limit, "limit", ingest.DefaultLimit, "maximum linked pages to replay from a source page")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "skip event-review staging or repair writes")
	fs.BoolVar(&cfg.repairEventTitles, "titles", false, "repair event titles from the replayed report")
	fs.BoolVar(&cfg.repairDescriptions, "descriptions", false, "repair event descriptions from the replayed report")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 1 {
		return ingestCommandConfig{}, errors.New("replay accepts at most one import run ID; flags must precede the ID")
	}
	if cfg.repairEventTitles && cfg.repairDescriptions {
		return ingestCommandConfig{}, errors.New("replay -titles and -descriptions are mutually exclusive")
	}
	if fs.NArg() == 1 {
		id, err := strconv.ParseInt(strings.TrimSpace(fs.Arg(0)), 10, 64)
		if err != nil {
			return ingestCommandConfig{}, fmt.Errorf("parse replay import run ID: %w", err)
		}
		if id <= 0 {
			return ingestCommandConfig{}, errors.New("replay import run ID must be positive")
		}
		cfg.replayImportRunID = id
		cfg.replayUseLatest = false
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	cfg.stageEventReviewClusters = !cfg.dryRun && !cfg.repairEventTitles && !cfg.repairDescriptions
	return cfg, nil
}

func parseFixArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	if len(args) == 0 {
		return ingestCommandConfig{}, errors.New("fix subcommand is required")
	}
	kind := fixCommandKind(args[0])
	switch kind {
	case fixCommandTitles, fixCommandDescriptions:
		return parseLiveFixArgs(kind, args[1:], globalDB)
	case fixCommandHistoricalDuplicates:
		return parseHistoricalDuplicateFixArgs(args[1:], globalDB)
	case fixCommandImageFocus:
		return parseImageFocusFixArgs(args[1:], globalDB)
	case fixCommandMedia:
		return parseMediaFixArgs(args[1:], globalDB)
	case fixCommandSnapshots:
		return parseSnapshotFixArgs(args[1:], globalDB)
	default:
		return ingestCommandConfig{}, fmt.Errorf("unknown fix subcommand %q", args[0])
	}
}

func parseLiveFixArgs(kind fixCommandKind, args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandFix)
	cfg.fixKind = kind
	cfg.repairEventTitles = kind == fixCommandTitles
	cfg.repairDescriptions = kind == fixCommandDescriptions
	var sourceFlag, userAgentFlag, dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest fix " + string(kind))
	fs.Var(&sourceFlag, "source", "source to repair")
	fs.IntVar(&cfg.limit, "limit", ingest.DefaultLimit, "maximum linked pages to fetch from a source page")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "HTTP timeout")
	fs.Var(&userAgentFlag, "user-agent", "HTTP User-Agent header")
	fs.StringVar(&cfg.contact, "contact", "", "contact detail for the default HTTP User-Agent header; use none|null|false to suppress contact info")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "skip repair writes; import snapshots are still written")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	cfg.httpUserAgent = userAgentFlag.value
	cfg.sourceSet = sourceFlag.set
	cfg.allSources = !sourceFlag.set
	if sourceFlag.set {
		cfg.source = sourceFlag.value
	}
	if kind == fixCommandDescriptions && sourceFlag.set && !descriptionRepairSourceSupported(sourceFlag.value) {
		return ingestCommandConfig{}, fmt.Errorf("fix descriptions does not support -source %s", sourceFlag.value)
	}
	return cfg, nil
}

func parseHistoricalDuplicateFixArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandFix)
	cfg.fixKind = fixCommandHistoricalDuplicates
	var dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest fix historical-duplicates")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "report historical duplicate repairs without writing")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	return cfg, nil
}

func parseImageFocusFixArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandFix)
	cfg.fixKind = fixCommandImageFocus
	var dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest fix image-focus")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.StringVar(&cfg.mediaRoot, "media-root", "", "local media root")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "report image focus repairs without writing")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	return cfg, nil
}

func parseMediaFixArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandFix)
	cfg.fixKind = fixCommandMedia
	var dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest fix media")
	fs.Var(&dbFlag, "db", "SQLite database path")
	fs.StringVar(&cfg.mediaRoot, "media-root", "", "local media root")
	fs.StringVar(&cfg.mediaURLPrefix, "media-url-prefix", "", "public media URL prefix")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "report media cleanup without writing")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	return cfg, nil
}

func parseSnapshotFixArgs(args []string, globalDB trackedStringFlag) (ingestCommandConfig, error) {
	cfg := defaultCommandConfig(ingestCommandFix)
	cfg.fixKind = fixCommandSnapshots
	var dbFlag trackedStringFlag
	fs := newIngestFlagSet("ingest fix snapshots")
	fs.Var(&dbFlag, "db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return ingestCommandConfig{}, err
	}
	if fs.NArg() > 0 {
		return ingestCommandConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	dbPath, err := mergeDBPath(globalDB, dbFlag)
	if err != nil {
		return ingestCommandConfig{}, err
	}
	cfg.dbPath = dbPath
	return cfg, nil
}

func defaultCommandConfig(command ingestCommand) ingestCommandConfig {
	return ingestCommandConfig{
		command: command,
		limit:   ingest.DefaultLimit,
		timeout: 10 * time.Second,
	}
}

func newIngestFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func leadingGlobalDBForSubcommand(args []string) (trackedStringFlag, []string, bool, error) {
	var db trackedStringFlag
	i := 0
	for i < len(args) {
		arg := args[i]
		switch {
		case arg == "-db" || arg == "--db":
			if i+1 >= len(args) {
				return trackedStringFlag{}, nil, false, errors.New("-db/--db requires a value")
			}
			if err := db.Set(args[i+1]); err != nil {
				return trackedStringFlag{}, nil, false, err
			}
			i += 2
		case strings.HasPrefix(arg, "-db="):
			if err := db.Set(strings.TrimPrefix(arg, "-db=")); err != nil {
				return trackedStringFlag{}, nil, false, err
			}
			i++
		case strings.HasPrefix(arg, "--db="):
			if err := db.Set(strings.TrimPrefix(arg, "--db=")); err != nil {
				return trackedStringFlag{}, nil, false, err
			}
			i++
		default:
			if db.set && (arg == string(ingestCommandReplay) || arg == string(ingestCommandFix)) {
				if conflictOnTrackedValues(db.values) {
					return trackedStringFlag{}, nil, false, errors.New("global -db values must match")
				}
				return db, args[i:], true, nil
			}
			return trackedStringFlag{}, nil, false, nil
		}
	}
	return trackedStringFlag{}, nil, false, nil
}

func mergeDBPath(global, local trackedStringFlag) (string, error) {
	if conflictOnTrackedValues(global.values, local.values) {
		return "", errors.New("global -db and command -db values must match")
	}
	if local.set {
		return local.value, nil
	}
	if global.set {
		return global.value, nil
	}
	return "", nil
}

func descriptionRepairSourceSupported(source string) bool {
	source = strings.TrimSpace(source)
	return source == ingest.DefaultSource || source == ingest.CafeNo9Source
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
	DryRun         bool     `json:"dry_run"`
	Applied        bool     `json:"applied"`
	Updated        int      `json:"updated"`
	Defaulted      int      `json:"defaulted"`
	MissingFiles   int      `json:"missing_files"`
	DecodeFailures int      `json:"decode_failures"`
	Errors         []string `json:"errors,omitempty"`
}

type imageFocusRepairRunReport struct {
	ImageFocus imageFocusBackfillReport `json:"image_focus"`
}

type mediaCleanupRunReport struct {
	MediaCleanup sqlite.MediaCleanupReport `json:"media_cleanup"`
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
	Source              string                          `json:"source"`
	Report              ingest.Report                   `json:"report"`
	EventReviewClusters *eventReviewClusterReport       `json:"review_stage,omitempty"`
	TitleRepair         *sqlite.EventTitleRepairReport  `json:"title_repair,omitempty"`
	DescriptionRepair   *sqlite.DescriptionRepairReport `json:"description_repair,omitempty"`
	Error               string                          `json:"error,omitempty"`
}

type batchSourceResult struct {
	Result    batchManualIngestResult
	LogResult manualRunExecution
	Err       error
}

type eventReviewClusterReport struct {
	Enabled                              bool                                   `json:"enabled"`
	Applied                              bool                                   `json:"applied"`
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
		ImportRunID:              cfg.replayImportRunID,
	}
}

func ingestMode(cfg ingestCommandConfig) string {
	switch cfg.command {
	case ingestCommandReplay:
		switch {
		case cfg.repairDescriptions:
			return "description_repair_replay"
		case cfg.repairEventTitles:
			return "title_repair_replay"
		default:
			return "replay"
		}
	case ingestCommandFix:
		switch cfg.fixKind {
		case fixCommandTitles:
			return "title_repair_live"
		case fixCommandDescriptions:
			return "description_repair_live"
		case fixCommandHistoricalDuplicates:
			return "historical_duplicate_repair"
		case fixCommandImageFocus:
			return "image_focus_backfill"
		case fixCommandMedia:
			return "media_cleanup"
		case fixCommandSnapshots:
			return "snapshot_cleanup"
		default:
			return "fix"
		}
	case ingestCommandLive:
		if cfg.allSources {
			return "all_sources"
		}
		return "live"
	default:
		return "unknown"
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
		return skippedEventReviewClusterReport("skipped event review staging because the ingest run failed"), nil
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
		stageReport := disabledEventReviewClusterReport()
		return manualRunExecution{Report: report, EventReviewClusters: &stageReport, Err: runErr}
	}
	stageReport, stageErr := eventReviewClustersForReport(ctx, st, catalog, report, runErr)
	if stageErr != nil {
		return manualRunExecution{Report: report, EventReviewClusters: &stageReport, Err: stageErr}
	}
	return manualRunExecution{Report: report, EventReviewClusters: &stageReport, Err: runErr}
}

func encodeManualRunResult(stdout io.Writer, result manualRunExecution) error {
	if result.Err != nil && result.Report.ImportRunID == 0 {
		return result.Err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	stage := disabledEventReviewClusterReport()
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

func runDescriptionRepair(ctx context.Context, st *sqlite.Store, stdout io.Writer, catalog *ingest.Catalog, report ingest.Report, runErr error, apply bool) error {
	if runErr != nil && report.ImportRunID == 0 {
		return runErr
	}
	repair := sqlite.DescriptionRepairReport{
		DryRun:         !apply,
		Applied:        apply,
		RepairedSlugs:  []string{},
		UnchangedSlugs: []string{},
		SkippedTitles:  []string{},
	}
	if runErr == nil {
		var err error
		repair, err = st.RepairEventDescriptionsFromReportWithApply(ctx, catalog, report, apply)
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

type historicalDuplicateRepairRunReport struct {
	HistoricalDuplicateRepair sqlite.HistoricalDuplicateRepairReport `json:"historical_duplicate_repair"`
}

func runHistoricalDuplicateRepair(ctx context.Context, st *sqlite.Store, stdout io.Writer, apply bool) error {
	repair, err := st.RepairHistoricalDuplicateEvents(ctx, sqlite.HistoricalDuplicateRepairOptions{
		Apply: apply,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(historicalDuplicateRepairRunReport{HistoricalDuplicateRepair: repair})
}

func runImageFocusBackfill(ctx context.Context, st *sqlite.Store, stdout io.Writer, mediaRoot string, apply bool) error {
	assets, err := st.ListImageAssets(ctx)
	if err != nil {
		return err
	}
	report := imageFocusBackfillReport{
		DryRun:  !apply,
		Applied: apply,
	}
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
		if apply {
			if err := st.UpdateImageAssetFocus(ctx, asset.SourceURL, focus); err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("%s: update focus: %v", asset.SourceURL, err))
				continue
			}
		}
		report.Updated++
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(imageFocusRepairRunReport{ImageFocus: report}); err != nil {
		return err
	}
	if len(report.Errors) > 0 {
		return errors.New("one or more image focus repairs failed")
	}
	return nil
}

func runMediaCleanup(ctx context.Context, st *sqlite.Store, stdout io.Writer, mediaRoot, mediaURLPrefix string, apply bool) error {
	report, cleanupErr := cleanupLocalMedia(ctx, st, mediaRoot, mediaURLPrefix, apply)
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(mediaCleanupRunReport{MediaCleanup: report}); err != nil {
		return err
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	if len(report.Errors) > 0 {
		return errors.New("one or more media cleanup operations failed")
	}
	return nil
}

func runAutomaticMediaCleanup(ctx context.Context, st *sqlite.Store, logger *slog.Logger) {
	logger = logging.EnsureLogger(logger)
	report, err := cleanupLocalMedia(ctx, st, env("MEDIA_ROOT", "./data/media"), env("MEDIA_URL_PREFIX", "/media"), true)
	attrs := []any{
		"cleared_event_images", report.ClearedEventImages,
		"deleted_asset_rows", report.DeletedAssetRows,
		"deleted_files", report.DeletedFiles,
		"missing_files", report.MissingFiles,
		"warnings", len(report.Warnings),
		"errors", len(report.Errors),
	}
	if err != nil || len(report.Errors) > 0 {
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		logger.Warn("media cleanup failed", attrs...)
		return
	}
	if report.ClearedEventImages == 0 && report.DeletedAssetRows == 0 && report.DeletedFiles == 0 && report.MissingFiles == 0 && len(report.Warnings) == 0 {
		return
	}
	logger.Info("media cleanup finished", attrs...)
}

func cleanupLocalMedia(ctx context.Context, st *sqlite.Store, mediaRoot, mediaURLPrefix string, apply bool) (sqlite.MediaCleanupReport, error) {
	existingFiles, err := localMediaEventFiles(mediaRoot)
	if err != nil {
		return sqlite.MediaCleanupReport{}, err
	}
	report, err := st.CleanupMedia(ctx, sqlite.MediaCleanupOptions{
		Apply:          apply,
		Now:            nowUTC(),
		MediaURLPrefix: mediaURLPrefix,
		ExistingFiles:  existingFiles,
	})
	if err != nil {
		return report, err
	}
	completeLocalMediaCleanup(&report, mediaRoot, mediaURLPrefix, existingFiles, apply)
	return report, nil
}

func completeLocalMediaCleanup(report *sqlite.MediaCleanupReport, mediaRoot, mediaURLPrefix string, existingFiles []string, apply bool) {
	if report == nil {
		return
	}
	knownStorage := stringSetFromSlice(report.KnownStoragePaths)
	retainedStorage := stringSetFromSlice(report.RetainedStoragePaths)
	retainedPublicURLs := stringSetFromSlice(report.RetainedPublicURLs)
	deleteCandidates := stringSetFromSlice(report.FilesToDelete)

	for _, storagePath := range report.FilesToDelete {
		if _, ok := retainedStorage[storagePath]; ok {
			report.RetainedFiles++
			continue
		}
		publicURL := localMediaPublicURL(mediaURLPrefix, storagePath)
		if _, ok := retainedPublicURLs[publicURL]; ok {
			report.RetainedFiles++
			continue
		}
		recordMediaFileDeletion(report, mediaRoot, mediaURLPrefix, storagePath, "unreferenced_asset_file", apply)
	}

	for _, storagePath := range existingFiles {
		storagePath = filepath.ToSlash(strings.TrimSpace(storagePath))
		if storagePath == "" {
			continue
		}
		if _, ok := knownStorage[storagePath]; ok {
			continue
		}
		report.ScannedOrphanFiles++
		if _, ok := retainedStorage[storagePath]; ok {
			report.RetainedFiles++
			continue
		}
		publicURL := localMediaPublicURL(mediaURLPrefix, storagePath)
		if _, ok := retainedPublicURLs[publicURL]; ok {
			report.RetainedFiles++
			continue
		}
		if _, ok := deleteCandidates[storagePath]; ok {
			continue
		}
		recordMediaFileDeletion(report, mediaRoot, mediaURLPrefix, storagePath, "orphan_file", apply)
	}
}

func recordMediaFileDeletion(report *sqlite.MediaCleanupReport, mediaRoot, mediaURLPrefix, storagePath, reason string, apply bool) {
	storagePath = filepath.ToSlash(strings.TrimSpace(storagePath))
	if storagePath == "" {
		return
	}
	item := sqlite.MediaCleanupItem{
		Action:      "delete_file",
		Reason:      reason,
		StoragePath: storagePath,
		PublicURL:   localMediaPublicURL(mediaURLPrefix, storagePath),
	}
	if !apply {
		report.DeletedFiles++
		report.Items = append(report.Items, item)
		return
	}
	if err := deleteLocalMediaFile(mediaRoot, storagePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			report.MissingFiles++
			item.Action = "missing_file"
			report.Items = append(report.Items, item)
			return
		}
		message := fmt.Sprintf("%s: delete file: %v", storagePath, err)
		report.Errors = append(report.Errors, message)
		item.Action = "delete_file_failed"
		item.Reason = message
		report.Items = append(report.Items, item)
		return
	}
	report.DeletedFiles++
	report.Items = append(report.Items, item)
}

func localMediaEventFiles(mediaRoot string) ([]string, error) {
	mediaRoot = strings.TrimSpace(mediaRoot)
	if mediaRoot == "" {
		return nil, errors.New("media root is required")
	}
	eventsRoot := filepath.Join(mediaRoot, "events")
	info, err := os.Stat(eventsRoot)
	if err != nil {
		return nil, fmt.Errorf("media events directory %q: %w", eventsRoot, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("media events path %q is not a directory", eventsRoot)
	}
	var files []string
	err = filepath.WalkDir(eventsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(mediaRoot, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

var deleteLocalMediaFile = func(mediaRoot, storagePath string) error {
	path, err := localMediaAssetPath(mediaRoot, storagePath)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	return os.Remove(path)
}

func localMediaPublicURL(mediaURLPrefix, storagePath string) string {
	storagePath = strings.TrimLeft(filepath.ToSlash(strings.TrimSpace(storagePath)), "/")
	if storagePath == "" {
		return ""
	}
	prefix := strings.TrimSpace(mediaURLPrefix)
	if prefix == "" {
		prefix = "/media"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		prefix = "/media"
	}
	return prefix + "/" + storagePath
}

func stringSetFromSlice(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
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

func runSourceBatch(sources []string, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary, failureErr error, runSource func(source string) batchSourceResult) error {
	logger = logging.EnsureLogger(logger)
	results := make([]batchManualIngestResult, 0, len(sources))
	var failed bool
	failedSources := 0
	for _, source := range sources {
		sourceResult := runSource(source)
		batchResult := sourceResult.Result
		batchResult.Source = source
		if sourceResult.Err != nil {
			batchResult.Error = sourceResult.Err.Error()
			failed = true
			failedSources++
		}
		results = append(results, batchResult)

		logResult := sourceResult.LogResult
		if logResult.Report.Source == "" && logResult.Report.ImportRunID == 0 {
			logResult.Report = batchResult.Report
		}
		if logResult.Err == nil {
			logResult.Err = sourceResult.Err
		}
		logIngestSourceFinished(logger, source, logResult)
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

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(batchManualIngestReport{Results: results}); err != nil {
		return err
	}
	if failed {
		return failureErr
	}
	return nil
}

func runAllSources(ctx context.Context, st *sqlite.Store, fetcher ingest.Fetcher, catalog *ingest.Catalog, cfg ingestCommandConfig, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	err := runSourceBatch(catalog.Keys(), stdout, logger, summary, errors.New("one or more source ingests failed"), func(source string) batchSourceResult {
		result := runSingleManualSource(ctx, st, fetcher, catalog, cfg, source)
		batchResult := batchManualIngestResult{
			Report: result.Report,
		}
		if result.EventReviewClusters != nil {
			stageCopy := *result.EventReviewClusters
			batchResult.EventReviewClusters = &stageCopy
		}
		return batchSourceResult{
			Result:    batchResult,
			LogResult: result,
			Err:       result.Err,
		}
	})
	if err == nil && !cfg.dryRun {
		runAutomaticMediaCleanup(ctx, st, logger)
	}
	runAutomaticSnapshotCleanup(ctx, st, logger)
	return err
}

type sourceRepairKind string

const (
	repairKindTitles       sourceRepairKind = "titles"
	repairKindDescriptions sourceRepairKind = "descriptions"
)

func runLiveSourceRepairs(ctx context.Context, st *sqlite.Store, catalog *ingest.Catalog, cfg ingestCommandConfig, kind sourceRepairKind, stdout io.Writer, logger *slog.Logger, summary *ingestLogSummary) error {
	if err := validateFetchConfig(cfg); err != nil {
		return err
	}
	cfg.httpUserAgent = effectiveHTTPUserAgent(ctx, cfg)
	fetcher, err := newHTTPFetcher(cfg.timeout, cfg.httpUserAgent)
	if err != nil {
		return err
	}
	sources := repairSourcesForConfig(catalog, cfg, kind)
	err = runSourceBatch(sources, stdout, logger, summary, fmt.Errorf("one or more source %s repairs failed", kind), func(source string) batchSourceResult {
		report, runErr := runLiveManualImportWithRetention(ctx, st, fetcher, catalog, ingest.Options{
			Source: source,
			Limit:  cfg.limit,
		})
		batchResult := batchManualIngestResult{
			Report: report,
		}
		if runErr == nil {
			switch kind {
			case repairKindTitles:
				repair, err := st.RepairEventTitlesFromReport(ctx, catalog, report, !cfg.dryRun)
				if err != nil {
					runErr = err
				} else {
					batchResult.TitleRepair = &repair
				}
			case repairKindDescriptions:
				repair, err := st.RepairEventDescriptionsFromReportWithApply(ctx, catalog, report, !cfg.dryRun)
				if err != nil {
					runErr = err
				} else {
					batchResult.DescriptionRepair = &repair
				}
			default:
				runErr = fmt.Errorf("unsupported source repair kind %q", kind)
			}
		}
		return batchSourceResult{
			Result:    batchResult,
			LogResult: manualRunExecution{Report: report, Err: runErr},
			Err:       runErr,
		}
	})
	runAutomaticSnapshotCleanup(ctx, st, logger)
	return err
}

func repairSourcesForConfig(catalog *ingest.Catalog, cfg ingestCommandConfig, kind sourceRepairKind) []string {
	if cfg.sourceSet {
		return []string{cfg.source}
	}
	if kind == repairKindDescriptions {
		return []string{ingest.DefaultSource, ingest.CafeNo9Source}
	}
	return catalog.Keys()
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
		Applied:                         true,
		EventReviewClusters:             []eventReviewClusterReportItem{},
		AutoPromoted:                    []eventReviewClusterAutoPromotedReport{},
		EventReviewClustersAutoResolved: []eventReviewClusterAutoResolvedReport{},
		Errors:                          []string{},
	}
}

func disabledEventReviewClusterReport() eventReviewClusterReport {
	return eventReviewClusterReport{
		Enabled:                         false,
		Applied:                         false,
		EventReviewClusters:             []eventReviewClusterReportItem{},
		AutoPromoted:                    []eventReviewClusterAutoPromotedReport{},
		EventReviewClustersAutoResolved: []eventReviewClusterAutoResolvedReport{},
		Errors:                          []string{},
	}
}

func skippedEventReviewClusterReport(reason string) eventReviewClusterReport {
	report := emptyEventReviewClusterReport()
	report.Applied = false
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "skipped event review staging"
	}
	report.Errors = []string{reason}
	return report
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
