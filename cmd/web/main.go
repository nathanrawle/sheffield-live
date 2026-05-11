package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/logging"
	"sheffield-live/internal/store/sqlite"
	"sheffield-live/internal/web"
)

func main() {
	logger, err := logging.NewLoggerFromEnv(os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure logging: %v\n", err)
		os.Exit(1)
	}
	if err := runWithLogger(logger); err != nil {
		logger.Error("web exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger, err := logging.NewLoggerFromEnv(os.Stderr)
	if err != nil {
		return err
	}
	return runWithLogger(logger)
}

func runWithLogger(logger *slog.Logger) error {
	logger = logging.EnsureLogger(logger)

	addr := env("ADDR", ":8080")
	dbPath := env("DB_PATH", "./data/sheffield-live.db")
	mediaRoot := env("MEDIA_ROOT", "./data/media")
	mediaURLPrefix := env("MEDIA_URL_PREFIX", "/media")

	logger.Info("web starting",
		"addr", addr,
		"db_path", dbPath,
		"media_root", mediaRoot,
		"media_url_prefix", mediaURLPrefix,
	)

	sourceCatalog, err := ingest.LoadRepoCatalog()
	if err != nil {
		return err
	}

	st, err := sqlite.Open(dbPath, sourceCatalog)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			logger.Error("close sqlite store", "error", closeErr)
		}
	}()

	if err := st.Validate(context.Background()); err != nil {
		return err
	}

	server, err := web.NewServer(web.ServerDeps{
		Catalog:                   st,
		ReviewStore:               st,
		ImportRunStore:            st,
		ReplayStore:               st,
		ImportRunReviewGroupStore: st,
		EventSecondarySourceStore: st,
		EventGenreStore:           st,
		GenreConfigurationStore:   st,
		ReadyChecker:              st,
		MediaRoot:                 mediaRoot,
		MediaURLPrefix:            mediaURLPrefix,
		Logger:                    logger,
	})
	if err != nil {
		return err
	}

	logger.Info("web listening", "addr", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		return err
	}
	return nil
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
