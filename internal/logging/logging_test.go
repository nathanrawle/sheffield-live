package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestConfigFromGetterReadsLogEnvironment(t *testing.T) {
	cfg := ConfigFromGetter(func(key string) string {
		switch key {
		case "LOG_LEVEL":
			return "debug"
		case "LOG_FORMAT":
			return "json"
		default:
			return ""
		}
	})

	if cfg.Level != "debug" {
		t.Fatalf("level = %q, want debug", cfg.Level)
	}
	if cfg.Format != "json" {
		t.Fatalf("format = %q, want json", cfg.Format)
	}
}

func TestNewLoggerDefaultsToTextInfo(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(&buf, Config{})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	logger.Debug("hidden")
	logger.Info("visible", "component", "test")

	got := buf.String()
	if strings.Contains(got, "hidden") {
		t.Fatalf("debug log was emitted at default level: %q", got)
	}
	if !strings.Contains(got, "msg=visible") || !strings.Contains(got, "component=test") {
		t.Fatalf("text log = %q, want message and field", got)
	}
}

func TestNewLoggerSupportsJSONAndDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(&buf, Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	logger.Debug("visible", slog.String("component", "test"))

	got := buf.String()
	if !strings.Contains(got, `"msg":"visible"`) || !strings.Contains(got, `"component":"test"`) {
		t.Fatalf("json log = %q, want message and field", got)
	}
}

func TestNewLoggerRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "level", cfg: Config{Level: "trace"}, want: "LOG_LEVEL"},
		{name: "format", cfg: Config{Format: "xml"}, want: "LOG_FORMAT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewLogger(ioDiscard{}, tc.cfg)
			if err == nil {
				t.Fatal("error = nil, want config error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
