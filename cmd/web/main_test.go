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
	t.Setenv("ADMIN_AUTH_DISABLED", "1")
	err = run()
	if err == nil {
		t.Fatal("run error = nil, want catalog load failure")
	}
	if !strings.Contains(err.Error(), filepath.Join("config", "sources")) {
		t.Fatalf("run error = %q, want config/sources path", err)
	}
}

func TestAdminAuthConfigRequiresPasswordHashByDefault(t *testing.T) {
	t.Setenv("ADMIN_AUTH_DISABLED", "")
	t.Setenv("ADMIN_PASSWORD_HASH", "")

	_, err := adminAuthConfigFromEnv()
	if err == nil {
		t.Fatal("adminAuthConfigFromEnv error = nil, want missing hash error")
	}
	if !strings.Contains(err.Error(), "ADMIN_PASSWORD_HASH") {
		t.Fatalf("error = %q, want ADMIN_PASSWORD_HASH", err)
	}
}

func TestAdminAuthConfigCanBeDisabledForLocalDevelopment(t *testing.T) {
	t.Setenv("ADMIN_AUTH_DISABLED", "1")
	t.Setenv("ADMIN_PASSWORD_HASH", "")

	config, err := adminAuthConfigFromEnv()
	if err != nil {
		t.Fatalf("adminAuthConfigFromEnv: %v", err)
	}
	if !config.Disabled {
		t.Fatal("config.Disabled = false, want true")
	}
}

func TestAdminAuthConfigAllowsInsecureCookieOptOut(t *testing.T) {
	t.Setenv("ADMIN_AUTH_DISABLED", "0")
	t.Setenv("ADMIN_PASSWORD_HASH", "$2a$12$Np7G88kWczQUXP1fhca9..B9Gv1N55toTxUHQ02rBkN0c1QJggkMW")
	t.Setenv("ADMIN_COOKIE_SECURE", "false")

	config, err := adminAuthConfigFromEnv()
	if err != nil {
		t.Fatalf("adminAuthConfigFromEnv: %v", err)
	}
	if config.Disabled {
		t.Fatal("config.Disabled = true, want false")
	}
	if !config.AllowInsecureCookie {
		t.Fatal("config.AllowInsecureCookie = false, want true")
	}
}
