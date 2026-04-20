package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"sheffield-live/internal/ingest"
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
	)
	flag.Parse()

	if *userAgent == "" {
		return errors.New("-user-agent is required")
	}
	if *limit < 1 || *limit > ingest.MaxLimit {
		return fmt.Errorf("-limit must be between 1 and %d", ingest.MaxLimit)
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
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
	if err := encoder.Encode(report); err != nil {
		return err
	}
	return runErr
}
