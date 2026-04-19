package main

import (
	"log"
	"net/http"
	"os"

	"sheffield-live/internal/store"
	"sheffield-live/internal/web"
)

func main() {
	addr := env("ADDR", ":8080")

	st := store.NewSeedStore()
	server, err := web.NewServer(st)
	if err != nil {
		log.Fatalf("build server: %v", err)
	}

	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
