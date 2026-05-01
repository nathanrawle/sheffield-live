package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunFailsFastWhenRepoCatalogIsMissing(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	defer func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()

	t.Setenv("DB_PATH", filepath.Join(tmp, "sheffield-live.db"))
	err = run()
	if err == nil {
		t.Fatal("run error = nil, want catalog load failure")
	}
	if !strings.Contains(err.Error(), filepath.Join("config", "sources")) {
		t.Fatalf("run error = %q, want config/sources path", err)
	}
}
