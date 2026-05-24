package ingest

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadRepoCatalogIncludesCurrentSourcesInOrder(t *testing.T) {
	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}

	got := catalog.Keys()
	want := []string{
		DefaultSource,
		YellowArchSource,
		CafeNo9Source,
		JazzAtTheLescarSource,
		TheGreystonesSource,
		LeadmillSource,
		CorporationSource,
		HallamshireHotelSource,
		NetworkSheffieldSource,
		CrookesClubSource,
		DeliciousClamSource,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog keys = %v, want %v", got, want)
	}

	cfg, err := catalog.ConfigForSource(LeadmillSource)
	if err != nil {
		t.Fatalf("config for leadmill: %v", err)
	}
	if got, want := cfg.ICSParserFamily, "leadmill"; got != want {
		t.Fatalf("leadmill ics parser family = %q, want %q", got, want)
	}

	cfg, err = catalog.ConfigForSource(NetworkSheffieldSource)
	if err != nil {
		t.Fatalf("config for network sheffield: %v", err)
	}
	if got, want := cfg.LinkedPageParserFamily, "network_sheffield_detail_page"; got != want {
		t.Fatalf("network sheffield linked page parser family = %q, want %q", got, want)
	}
	if got, want := cfg.VenueNormalizerFamily, "network_sheffield"; got != want {
		t.Fatalf("network sheffield venue normalizer family = %q, want %q", got, want)
	}

	cfg, err = catalog.ConfigForSource(CrookesClubSource)
	if err != nil {
		t.Fatalf("config for crookes club: %v", err)
	}
	if got, want := cfg.SourcePageParserFamily, "crookes_club"; got != want {
		t.Fatalf("crookes club source page parser family = %q, want %q", got, want)
	}
	if got, want := cfg.SourcePageLinkExtractorFamily, "crookes_club_secondary_pages"; got != want {
		t.Fatalf("crookes club source page link extractor family = %q, want %q", got, want)
	}
	if got, want := cfg.OwnedVenueSlug, CrookesClubSource; got != want {
		t.Fatalf("crookes club owned venue slug = %q, want %q", got, want)
	}

	cfg, err = catalog.ConfigForSource(DeliciousClamSource)
	if err != nil {
		t.Fatalf("config for delicious clam: %v", err)
	}
	if got, want := cfg.LinkedPageLinkExtractorFamily, "delicious_clam_ticket_links"; got != want {
		t.Fatalf("delicious clam linked page link extractor family = %q, want %q", got, want)
	}
	if got, want := cfg.LinkedPageParserFamily, "delicious_clam_ticket_page"; got != want {
		t.Fatalf("delicious clam linked page parser family = %q, want %q", got, want)
	}
	if got, want := cfg.OwnedVenueSlug, DeliciousClamSource; got != want {
		t.Fatalf("delicious clam owned venue slug = %q, want %q", got, want)
	}
}

func TestLoadRepoCatalogFindsRepoFromNestedWorkingDirectory(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot := filepath.Dir(filepath.Dir(defaultCatalogDir()))
	nested := filepath.Join(repoRoot, "internal", "store", "sqlite")
	if err := os.Chdir(nested); err != nil {
		t.Fatalf("chdir nested: %v", err)
	}
	defer func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()

	catalog, err := LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog from nested cwd: %v", err)
	}
	if got, want := catalog.Keys()[0], DefaultSource; got != want {
		t.Fatalf("first catalog key = %q, want %q", got, want)
	}
}

func TestLoadCatalogRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{
			name: "duplicate key",
			files: map[string]string{
				"01-one.yaml": minimalSourceYAML("sidney-and-matilda", "One", "https://one.example.test/", "One manual ingest"),
				"02-two.yaml": minimalSourceYAML("sidney-and-matilda", "Two", "https://two.example.test/", "Two manual ingest"),
			},
			wantErr: `duplicate source key "sidney-and-matilda"`,
		},
		{
			name: "duplicate review stage source name",
			files: map[string]string{
				"01-one.yaml": minimalSourceYAML("one", "One", "https://one.example.test/", "Shared manual ingest"),
				"02-two.yaml": minimalSourceYAML("two", "Two", "https://two.example.test/", "Shared manual ingest"),
			},
			wantErr: `duplicate review stage source name "Shared manual ingest"`,
		},
		{
			name: "conflicting ownership",
			files: map[string]string{
				"01-one.yaml": strings.TrimSpace(`
key: one
name: One
url: https://one.example.test/
review_stage_source_name: One manual ingest
owned_venue_slug: one
non_authoritative_singleton_venue_slug: one
mode: source_page
source_page:
  source_page_parser: yellow_arch_jsonld
`) + "\n",
			},
			wantErr: "owned_venue_slug and non_authoritative_singleton_venue_slug cannot both be set",
		},
		{
			name: "mode mismatch",
			files: map[string]string{
				"01-one.yaml": strings.TrimSpace(`
key: one
name: One
url: https://one.example.test/
review_stage_source_name: One manual ingest
mode: source_page
linked_ics:
  secondary_source_name: One ICS
  ics_link_extractor: sidney_and_matilda
  ics_parser: generic
`) + "\n",
			},
			wantErr: "source_page config is required for mode source_page",
		},
		{
			name: "unknown family",
			files: map[string]string{
				"01-one.yaml": strings.TrimSpace(`
key: one
name: One
url: https://one.example.test/
review_stage_source_name: One manual ingest
mode: source_page
source_page:
  source_page_parser: not_real
`) + "\n",
			},
			wantErr: `unknown source page parser family "not_real"`,
		},
		{
			name: "unknown yaml field",
			files: map[string]string{
				"01-one.yaml": strings.TrimSpace(`
key: one
name: One
url: https://one.example.test/
review_stage_source_name: One manual ingest
mode: source_page
source_page:
  source_page_parser: yellow_arch_jsonld
  venue_normaliser: leadmill
`) + "\n",
			},
			wantErr: "field venue_normaliser not found",
		},
		{
			name: "negative guarded near-match window",
			files: map[string]string{
				"01-one.yaml": strings.TrimSpace(`
key: one
name: One
url: https://one.example.test/
review_stage_source_name: One manual ingest
guarded_near_match_window_minutes: -1
mode: source_page
source_page:
  source_page_parser: yellow_arch_jsonld
`) + "\n",
			},
			wantErr: "guarded_near_match_window_minutes must not be negative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, body := range tc.files {
				path := filepath.Join(dir, name)
				if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
			}

			_, err := LoadCatalog(dir)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadCatalog() error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func minimalSourceYAML(key, name, url, reviewStageSourceName string) string {
	return strings.TrimSpace(`
key: `+key+`
name: `+name+`
url: `+url+`
review_stage_source_name: `+reviewStageSourceName+`
mode: source_page
source_page:
  source_page_parser: yellow_arch_jsonld
`) + "\n"
}

func TestLoadCatalogAppliesGuardedNearMatchDefaultsAndOverrides(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"01-default.yaml": minimalSourceYAML("default", "Default", "https://default.example.test/", "Default manual ingest"),
		"02-custom.yaml": strings.TrimSpace(`
key: custom
name: Custom
url: https://custom.example.test/
review_stage_source_name: Custom manual ingest
guarded_near_match_disabled: true
guarded_near_match_window_minutes: 30
mode: source_page
source_page:
  source_page_parser: yellow_arch_jsonld
`) + "\n",
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	catalog, err := LoadCatalog(dir)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	if got, want := catalog.GuardedNearMatchWindowForSource("default"), 75*time.Minute; got != want {
		t.Fatalf("default source near-match window = %s, want %s", got, want)
	}
	if got := catalog.GuardedNearMatchDisabledForSource("default"); got {
		t.Fatal("default source near-match disabled = true, want false")
	}
	if got, want := catalog.GuardedNearMatchWindowForReviewStageSourceName("Custom manual ingest"), 30*time.Minute; got != want {
		t.Fatalf("custom review-stage near-match window = %s, want %s", got, want)
	}
	if got := catalog.GuardedNearMatchDisabledForReviewStageSourceName("Custom manual ingest"); !got {
		t.Fatal("custom review-stage near-match disabled = false, want true")
	}
}
