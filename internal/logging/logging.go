package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	defaultLevel  = slog.LevelInfo
	defaultFormat = "text"
)

type Config struct {
	Level  string
	Format string
}

func ConfigFromEnv() Config {
	return ConfigFromGetter(os.Getenv)
}

func ConfigFromGetter(getenv func(string) string) Config {
	if getenv == nil {
		getenv = os.Getenv
	}
	return Config{
		Level:  getenv("LOG_LEVEL"),
		Format: getenv("LOG_FORMAT"),
	}
}

func NewLoggerFromEnv(w io.Writer) (*slog.Logger, error) {
	return NewLogger(w, ConfigFromEnv())
}

func NopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func EnsureLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return NopLogger()
}

func NewLogger(w io.Writer, cfg Config) (*slog.Logger, error) {
	if w == nil {
		w = io.Discard
	}

	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{Level: level}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
}

func parseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return defaultLevel, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return defaultLevel, fmt.Errorf("unsupported LOG_LEVEL %q; want debug, info, warn, or error", value)
	}
}

func parseFormat(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return defaultFormat, nil
	case "text":
		return "text", nil
	case "json":
		return "json", nil
	default:
		return defaultFormat, fmt.Errorf("unsupported LOG_FORMAT %q; want text or json", value)
	}
}
