package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

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
	adminAuth, err := adminAuthConfigFromEnv()
	if err != nil {
		return err
	}

	logger.Info("web starting",
		"addr", addr,
		"db_path", dbPath,
		"media_root", mediaRoot,
		"media_url_prefix", mediaURLPrefix,
		"admin_auth_enabled", !adminAuth.Disabled,
		"admin_cookie_secure", !adminAuth.AllowInsecureCookie,
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
		ImportRunStore:            st,
		ReplayStore:               st,
		EventSecondarySourceStore: st,
		EventGenreStore:           st,
		GenreConfigurationStore:   st,
		ReadyChecker:              st,
		MediaRoot:                 mediaRoot,
		MediaURLPrefix:            mediaURLPrefix,
		AdminAuth:                 adminAuth,
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

func adminAuthConfigFromEnv() (web.AdminAuthConfig, error) {
	disabled, err := boolEnv("ADMIN_AUTH_DISABLED", false)
	if err != nil {
		return web.AdminAuthConfig{}, err
	}
	if disabled {
		return web.AdminAuthConfig{Disabled: true}, nil
	}

	passwordHash := strings.TrimSpace(os.Getenv("ADMIN_PASSWORD_HASH"))
	if passwordHash == "" {
		return web.AdminAuthConfig{}, fmt.Errorf("ADMIN_PASSWORD_HASH is required unless ADMIN_AUTH_DISABLED=1")
	}

	cookieSecure, err := boolEnv("ADMIN_COOKIE_SECURE", true)
	if err != nil {
		return web.AdminAuthConfig{}, err
	}
	return web.AdminAuthConfig{
		PasswordHash:        passwordHash,
		AllowInsecureCookie: !cookieSecure,
	}, nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean value", key)
	}
}
