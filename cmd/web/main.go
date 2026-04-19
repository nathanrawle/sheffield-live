package main

import (
	"log"
	"net/http"
	"os"

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

	st, err := sqlite.Open(dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			log.Printf("close sqlite store: %v", closeErr)
		}
	}()

	server, err := web.NewServer(st)
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
