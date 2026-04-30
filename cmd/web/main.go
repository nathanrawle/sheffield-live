package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/store/sqlite"
	"sheffield-live/internal/web"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := env("ADDR", ":8080")
	dbPath := env("DB_PATH", "./data/sheffield-live.db")

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
			log.Printf("close sqlite store: %v", closeErr)
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
		ReadyChecker:              st,
	})
	if err != nil {
		return err
	}

	log.Printf("listening on %s", addr)
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
