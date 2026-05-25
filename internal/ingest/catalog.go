package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const YellowArchSource = "yellow-arch"
const yellowArchSource = YellowArchSource
const CafeNo9Source = "cafe-no-9"
const JazzAtTheLescarSource = "jazz-at-the-lescar"
const TheGreystonesSource = "the-greystones"
const LeadmillSource = "leadmill"
const CorporationSource = "corporation"
const HallamshireHotelSource = "hallamshire-hotel"
const NetworkSheffieldSource = "network-sheffield"
const CrookesClubSource = "crookes-club"
const DeliciousClamSource = "delicious-clam"
const HagglersCornerSource = "hagglers-corner"
const UniversityOfSheffieldPerformanceVenuesSource = "university-of-sheffield-performance-venues"

type pageProcessMode string

const (
	pageProcessLinkedICS         pageProcessMode = "linked_ics"
	pageProcessSourcePage        pageProcessMode = "source_page"
	pageProcessLinkedDetailPages pageProcessMode = "linked_detail_pages"
)

type sourceConfig struct {
	Key                                            string
	Name                                           string
	URL                                            string
	OwnedVenueSlug                                 string
	OwnedVenueSlugs                                []string
	NonAuthoritativeSingletonVenueSlug             string
	NonAuthoritativeSingletonAutoPromotionDisabled bool
	GuardedNearMatchDisabled                       bool
	GuardedNearMatchWindowMinutes                  int
	CalendarSourceName                             string
	LinkedPageSourceName                           string
	PageMode                                       pageProcessMode
	ReviewStageSourceName                          string
	ImportRunNotes                                 string
	SourcePageParserFamily                         string
	SourcePageLinkExtractorFamily                  string
	ICSLinkExtractorFamily                         string
	ICSParserFamily                                string
	LinkedPageLinkExtractorFamily                  string
	LinkedPageParserFamily                         string
	VenueNormalizerFamily                          string
}

func (cfg sourceConfig) nonAuthoritativeSingletonVenueSlug() string {
	if len(cfg.ownedVenueSlugs()) > 0 || cfg.NonAuthoritativeSingletonAutoPromotionDisabled {
		return ""
	}
	return strings.TrimSpace(cfg.NonAuthoritativeSingletonVenueSlug)
}

func (cfg sourceConfig) ownedVenueSlugs() []string {
	if len(cfg.OwnedVenueSlugs) > 0 {
		return append([]string(nil), cfg.OwnedVenueSlugs...)
	}
	if strings.TrimSpace(cfg.OwnedVenueSlug) != "" {
		return []string{strings.TrimSpace(cfg.OwnedVenueSlug)}
	}
	return nil
}

func (cfg sourceConfig) ownedVenueSlug() string {
	slugs := cfg.ownedVenueSlugs()
	if len(slugs) != 1 {
		return ""
	}
	return slugs[0]
}

func (cfg sourceConfig) guardedNearMatchDisabled() bool {
	return cfg.GuardedNearMatchDisabled
}

func (cfg sourceConfig) guardedNearMatchWindow() time.Duration {
	if cfg.GuardedNearMatchWindowMinutes > 0 {
		return time.Duration(cfg.GuardedNearMatchWindowMinutes) * time.Minute
	}
	return 75 * time.Minute
}

func (cfg sourceConfig) matchesReplayPageSnapshot(snapshot decodedReplaySnapshot) bool {
	if strings.TrimSpace(snapshot.snapshot.SourceName) != cfg.Name {
		return false
	}
	if strings.TrimSpace(snapshot.snapshot.SourceURL) != cfg.URL {
		return false
	}
	if strings.TrimSpace(snapshot.envelope.Metadata.URL) != cfg.URL {
		return false
	}
	return true
}

type linkedICSRuntimeDefinition struct {
	SecondarySourceName string `yaml:"secondary_source_name"`
	ICSLinkExtractor    string `yaml:"ics_link_extractor"`
	ICSParser           string `yaml:"ics_parser"`
	VenueNormalizer     string `yaml:"venue_normalizer"`
}

type sourcePageRuntimeDefinition struct {
	SourcePageParser        string `yaml:"source_page_parser"`
	SourcePageLinkExtractor string `yaml:"source_page_link_extractor"`
	VenueNormalizer         string `yaml:"venue_normalizer"`
}

type linkedDetailPagesRuntimeDefinition struct {
	SecondarySourceName     string `yaml:"secondary_source_name"`
	LinkedPageLinkExtractor string `yaml:"linked_page_link_extractor"`
	LinkedPageParser        string `yaml:"linked_page_parser"`
	VenueNormalizer         string `yaml:"venue_normalizer"`
}

type sourceDefinition struct {
	Key                                            string                              `yaml:"key"`
	Name                                           string                              `yaml:"name"`
	URL                                            string                              `yaml:"url"`
	ReviewStageSourceName                          string                              `yaml:"review_stage_source_name"`
	ImportRunNotes                                 string                              `yaml:"import_run_notes"`
	OwnedVenueSlug                                 string                              `yaml:"owned_venue_slug"`
	OwnedVenueSlugs                                []string                            `yaml:"owned_venue_slugs"`
	NonAuthoritativeSingletonVenueSlug             string                              `yaml:"non_authoritative_singleton_venue_slug"`
	NonAuthoritativeSingletonAutoPromotionDisabled bool                                `yaml:"non_authoritative_singleton_auto_promotion_disabled"`
	GuardedNearMatchDisabled                       bool                                `yaml:"guarded_near_match_disabled"`
	GuardedNearMatchWindowMinutes                  int                                 `yaml:"guarded_near_match_window_minutes"`
	Mode                                           pageProcessMode                     `yaml:"mode"`
	LinkedICS                                      *linkedICSRuntimeDefinition         `yaml:"linked_ics"`
	SourcePage                                     *sourcePageRuntimeDefinition        `yaml:"source_page"`
	LinkedDetailPages                              *linkedDetailPagesRuntimeDefinition `yaml:"linked_detail_pages"`
}

type Catalog struct {
	sourceOrder             []string
	byKey                   map[string]sourceConfig
	byReviewStageSourceName map[string]sourceConfig
}

type SourceMetadataLookup interface {
	OwnedVenueSlugForSource(source string) string
	ReviewStageSourceNameForSource(source string) string
	OwnedVenueSlugForReviewStageSourceName(sourceName string) string
	NonAuthoritativeSingletonVenueSlugForSource(source string) string
	NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName string) string
	GuardedNearMatchDisabledForSource(source string) bool
	GuardedNearMatchWindowForSource(source string) time.Duration
	GuardedNearMatchDisabledForReviewStageSourceName(sourceName string) bool
	GuardedNearMatchWindowForReviewStageSourceName(sourceName string) time.Duration
	ListingsURLForSourceName(sourceName string) string
}

func LoadCatalog(dir string) (*Catalog, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("catalog directory is required")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read catalog directory: %w", err)
	}

	sourceFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		sourceFiles = append(sourceFiles, filepath.Join(dir, name))
	}
	slices.Sort(sourceFiles)
	if len(sourceFiles) == 0 {
		return nil, fmt.Errorf("no source definition files found in %s", dir)
	}

	catalog := &Catalog{
		sourceOrder:             make([]string, 0, len(sourceFiles)),
		byKey:                   make(map[string]sourceConfig, len(sourceFiles)),
		byReviewStageSourceName: make(map[string]sourceConfig, len(sourceFiles)),
	}
	identityPairs := make(map[string]string, len(sourceFiles))

	for _, path := range sourceFiles {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read source definition %s: %w", path, err)
		}

		var def sourceDefinition
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		decoder.KnownFields(true)
		if err := decoder.Decode(&def); err != nil {
			return nil, fmt.Errorf("decode source definition %s: %w", path, err)
		}

		cfg, err := sourceConfigFromDefinition(def)
		if err != nil {
			return nil, fmt.Errorf("invalid source definition %s: %w", path, err)
		}
		if _, exists := catalog.byKey[cfg.Key]; exists {
			return nil, fmt.Errorf("duplicate source key %q", cfg.Key)
		}
		if reviewStageName := strings.TrimSpace(cfg.ReviewStageSourceName); reviewStageName != "" {
			if _, exists := catalog.byReviewStageSourceName[reviewStageName]; exists {
				return nil, fmt.Errorf("duplicate review stage source name %q", reviewStageName)
			}
		}

		identityKey := cfg.Name + "\x00" + cfg.URL
		if priorKey, exists := identityPairs[identityKey]; exists {
			return nil, fmt.Errorf("duplicate source identity pair (%q, %q) for %q and %q", cfg.Name, cfg.URL, priorKey, cfg.Key)
		}
		identityPairs[identityKey] = cfg.Key

		if err := validateSourceFamilies(cfg); err != nil {
			return nil, fmt.Errorf("source %q: %w", cfg.Key, err)
		}

		catalog.sourceOrder = append(catalog.sourceOrder, cfg.Key)
		catalog.byKey[cfg.Key] = cfg
		if reviewStageName := strings.TrimSpace(cfg.ReviewStageSourceName); reviewStageName != "" {
			catalog.byReviewStageSourceName[reviewStageName] = cfg
		}
	}

	return catalog, nil
}

func sourceConfigFromDefinition(def sourceDefinition) (sourceConfig, error) {
	cfg := sourceConfig{
		Key:                                strings.TrimSpace(def.Key),
		Name:                               strings.TrimSpace(def.Name),
		URL:                                strings.TrimSpace(def.URL),
		OwnedVenueSlug:                     strings.TrimSpace(def.OwnedVenueSlug),
		OwnedVenueSlugs:                    normalizeSourceVenueSlugs(def.OwnedVenueSlugs),
		NonAuthoritativeSingletonVenueSlug: strings.TrimSpace(def.NonAuthoritativeSingletonVenueSlug),
		NonAuthoritativeSingletonAutoPromotionDisabled: def.NonAuthoritativeSingletonAutoPromotionDisabled,
		GuardedNearMatchDisabled:                       def.GuardedNearMatchDisabled,
		GuardedNearMatchWindowMinutes:                  def.GuardedNearMatchWindowMinutes,
		PageMode:                                       def.Mode,
		ReviewStageSourceName:                          strings.TrimSpace(def.ReviewStageSourceName),
		ImportRunNotes:                                 strings.TrimSpace(def.ImportRunNotes),
	}

	switch {
	case cfg.Key == "":
		return sourceConfig{}, errors.New("key is required")
	case cfg.Name == "":
		return sourceConfig{}, errors.New("name is required")
	case cfg.URL == "":
		return sourceConfig{}, errors.New("url is required")
	case cfg.ReviewStageSourceName == "":
		return sourceConfig{}, errors.New("review_stage_source_name is required")
	case cfg.PageMode == "":
		return sourceConfig{}, errors.New("mode is required")
	}

	if cfg.OwnedVenueSlug != "" && len(cfg.OwnedVenueSlugs) > 0 {
		return sourceConfig{}, errors.New("owned_venue_slug and owned_venue_slugs cannot both be set")
	}
	if cfg.OwnedVenueSlug != "" && cfg.NonAuthoritativeSingletonVenueSlug != "" {
		return sourceConfig{}, errors.New("owned_venue_slug and non_authoritative_singleton_venue_slug cannot both be set")
	}
	if len(cfg.OwnedVenueSlugs) > 0 && cfg.NonAuthoritativeSingletonVenueSlug != "" {
		return sourceConfig{}, errors.New("owned_venue_slugs and non_authoritative_singleton_venue_slug cannot both be set")
	}
	if cfg.GuardedNearMatchWindowMinutes < 0 {
		return sourceConfig{}, errors.New("guarded_near_match_window_minutes must not be negative")
	}

	switch cfg.PageMode {
	case pageProcessLinkedICS:
		if def.LinkedICS == nil {
			return sourceConfig{}, errors.New("linked_ics config is required for mode linked_ics")
		}
		if def.SourcePage != nil || def.LinkedDetailPages != nil {
			return sourceConfig{}, errors.New("only linked_ics config is allowed for mode linked_ics")
		}
		cfg.CalendarSourceName = strings.TrimSpace(def.LinkedICS.SecondarySourceName)
		cfg.ICSLinkExtractorFamily = strings.TrimSpace(def.LinkedICS.ICSLinkExtractor)
		cfg.ICSParserFamily = strings.TrimSpace(def.LinkedICS.ICSParser)
		cfg.VenueNormalizerFamily = strings.TrimSpace(def.LinkedICS.VenueNormalizer)
		if cfg.CalendarSourceName == "" {
			return sourceConfig{}, errors.New("linked_ics.secondary_source_name is required")
		}
	case pageProcessSourcePage:
		if def.SourcePage == nil {
			return sourceConfig{}, errors.New("source_page config is required for mode source_page")
		}
		if def.LinkedICS != nil || def.LinkedDetailPages != nil {
			return sourceConfig{}, errors.New("only source_page config is allowed for mode source_page")
		}
		cfg.SourcePageParserFamily = strings.TrimSpace(def.SourcePage.SourcePageParser)
		cfg.SourcePageLinkExtractorFamily = strings.TrimSpace(def.SourcePage.SourcePageLinkExtractor)
		cfg.VenueNormalizerFamily = strings.TrimSpace(def.SourcePage.VenueNormalizer)
	case pageProcessLinkedDetailPages:
		if def.LinkedDetailPages == nil {
			return sourceConfig{}, errors.New("linked_detail_pages config is required for mode linked_detail_pages")
		}
		if def.LinkedICS != nil || def.SourcePage != nil {
			return sourceConfig{}, errors.New("only linked_detail_pages config is allowed for mode linked_detail_pages")
		}
		cfg.LinkedPageSourceName = strings.TrimSpace(def.LinkedDetailPages.SecondarySourceName)
		cfg.LinkedPageLinkExtractorFamily = strings.TrimSpace(def.LinkedDetailPages.LinkedPageLinkExtractor)
		cfg.LinkedPageParserFamily = strings.TrimSpace(def.LinkedDetailPages.LinkedPageParser)
		cfg.VenueNormalizerFamily = strings.TrimSpace(def.LinkedDetailPages.VenueNormalizer)
		if cfg.LinkedPageSourceName == "" {
			return sourceConfig{}, errors.New("linked_detail_pages.secondary_source_name is required")
		}
	default:
		return sourceConfig{}, fmt.Errorf("unsupported mode %q", cfg.PageMode)
	}

	return cfg, nil
}

func validateSourceFamilies(cfg sourceConfig) error {
	switch cfg.PageMode {
	case pageProcessLinkedICS:
		if cfg.ICSLinkExtractorFamily == "" {
			return errors.New("ics link extractor family is required")
		}
		if cfg.ICSParserFamily == "" {
			return errors.New("ics parser family is required")
		}
		if !hasICSLinkExtractorFamily(cfg.ICSLinkExtractorFamily) {
			return fmt.Errorf("unknown ics link extractor family %q", cfg.ICSLinkExtractorFamily)
		}
		if !hasICSParserFamily(cfg.ICSParserFamily) {
			return fmt.Errorf("unknown ics parser family %q", cfg.ICSParserFamily)
		}
	case pageProcessSourcePage:
		if cfg.SourcePageParserFamily == "" {
			return errors.New("source page parser family is required")
		}
		if !hasSourcePageParserFamily(cfg.SourcePageParserFamily) {
			return fmt.Errorf("unknown source page parser family %q", cfg.SourcePageParserFamily)
		}
		if cfg.SourcePageLinkExtractorFamily != "" && !hasSourcePageLinkExtractorFamily(cfg.SourcePageLinkExtractorFamily) {
			return fmt.Errorf("unknown source page link extractor family %q", cfg.SourcePageLinkExtractorFamily)
		}
	case pageProcessLinkedDetailPages:
		if cfg.LinkedPageLinkExtractorFamily == "" {
			return errors.New("linked page link extractor family is required")
		}
		if cfg.LinkedPageParserFamily == "" {
			return errors.New("linked page parser family is required")
		}
		if !hasLinkedPageLinkExtractorFamily(cfg.LinkedPageLinkExtractorFamily) {
			return fmt.Errorf("unknown linked page link extractor family %q", cfg.LinkedPageLinkExtractorFamily)
		}
		if !hasLinkedPageParserFamily(cfg.LinkedPageParserFamily) {
			return fmt.Errorf("unknown linked page parser family %q", cfg.LinkedPageParserFamily)
		}
	default:
		return fmt.Errorf("unsupported page mode %q", cfg.PageMode)
	}

	if cfg.VenueNormalizerFamily != "" && !hasVenueNormalizerFamily(cfg.VenueNormalizerFamily) {
		return fmt.Errorf("unknown venue normalizer family %q", cfg.VenueNormalizerFamily)
	}
	return nil
}

func normalizeSourceVenueSlugs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (c *Catalog) registeredSourceKeys() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.sourceOrder...)
}

func (c *Catalog) Keys() []string {
	return c.registeredSourceKeys()
}

func (c *Catalog) configForSource(source string) (sourceConfig, error) {
	if c == nil {
		return sourceConfig{}, errors.New("catalog is nil")
	}
	key := strings.TrimSpace(source)
	if key == "" {
		key = DefaultSource
	}
	cfg, ok := c.byKey[key]
	if !ok {
		return sourceConfig{}, fmt.Errorf("unsupported source %q", source)
	}
	return cfg, nil
}

func (c *Catalog) ConfigForSource(source string) (sourceConfig, error) {
	return c.configForSource(source)
}

func (c *Catalog) OwnedVenueSlugForSource(source string) string {
	cfg, err := c.configForSource(source)
	if err != nil {
		return ""
	}
	return cfg.ownedVenueSlug()
}

func (c *Catalog) OwnedVenueSlugsForSource(source string) []string {
	cfg, err := c.configForSource(source)
	if err != nil {
		return nil
	}
	return cfg.ownedVenueSlugs()
}

func (c *Catalog) ReviewStageSourceNameForSource(source string) string {
	cfg, err := c.configForSource(source)
	if err != nil {
		source = strings.TrimSpace(source)
		if source == "" {
			return reviewStageDefaultSourceName
		}
		return source + " manual ingest"
	}
	if strings.TrimSpace(cfg.ReviewStageSourceName) != "" {
		return cfg.ReviewStageSourceName
	}
	if strings.TrimSpace(cfg.Key) == "" || cfg.Key == DefaultSource {
		return reviewStageDefaultSourceName
	}
	return cfg.Key + " manual ingest"
}

func (c *Catalog) ownedVenueSlugForReviewStageSourceName(sourceName string) string {
	if c == nil {
		return ""
	}
	cfg, ok := c.byReviewStageSourceName[strings.TrimSpace(sourceName)]
	if !ok {
		return ""
	}
	return cfg.ownedVenueSlug()
}

func (c *Catalog) OwnedVenueSlugForReviewStageSourceName(sourceName string) string {
	return c.ownedVenueSlugForReviewStageSourceName(sourceName)
}

func (c *Catalog) ownedVenueSlugsForReviewStageSourceName(sourceName string) []string {
	if c == nil {
		return nil
	}
	cfg, ok := c.byReviewStageSourceName[strings.TrimSpace(sourceName)]
	if !ok {
		return nil
	}
	return cfg.ownedVenueSlugs()
}

func (c *Catalog) OwnedVenueSlugsForReviewStageSourceName(sourceName string) []string {
	return c.ownedVenueSlugsForReviewStageSourceName(sourceName)
}

func (c *Catalog) nonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName string) string {
	if c == nil {
		return ""
	}
	cfg, ok := c.byReviewStageSourceName[strings.TrimSpace(sourceName)]
	if !ok {
		return ""
	}
	return cfg.nonAuthoritativeSingletonVenueSlug()
}

func (c *Catalog) NonAuthoritativeSingletonVenueSlugForSource(source string) string {
	cfg, err := c.configForSource(source)
	if err != nil {
		return ""
	}
	return cfg.nonAuthoritativeSingletonVenueSlug()
}

func (c *Catalog) NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName string) string {
	return c.nonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName)
}

func (c *Catalog) guardedNearMatchDisabledForSource(source string) bool {
	cfg, err := c.configForSource(source)
	if err != nil {
		return false
	}
	return cfg.guardedNearMatchDisabled()
}

func (c *Catalog) GuardedNearMatchDisabledForSource(source string) bool {
	return c.guardedNearMatchDisabledForSource(source)
}

func (c *Catalog) guardedNearMatchWindowForSource(source string) time.Duration {
	cfg, err := c.configForSource(source)
	if err != nil {
		return 75 * time.Minute
	}
	return cfg.guardedNearMatchWindow()
}

func (c *Catalog) GuardedNearMatchWindowForSource(source string) time.Duration {
	return c.guardedNearMatchWindowForSource(source)
}

func (c *Catalog) guardedNearMatchDisabledForReviewStageSourceName(sourceName string) bool {
	if c == nil {
		return false
	}
	cfg, ok := c.byReviewStageSourceName[strings.TrimSpace(sourceName)]
	if !ok {
		return false
	}
	return cfg.guardedNearMatchDisabled()
}

func (c *Catalog) GuardedNearMatchDisabledForReviewStageSourceName(sourceName string) bool {
	return c.guardedNearMatchDisabledForReviewStageSourceName(sourceName)
}

func (c *Catalog) guardedNearMatchWindowForReviewStageSourceName(sourceName string) time.Duration {
	if c == nil {
		return 75 * time.Minute
	}
	cfg, ok := c.byReviewStageSourceName[strings.TrimSpace(sourceName)]
	if !ok {
		return 75 * time.Minute
	}
	return cfg.guardedNearMatchWindow()
}

func (c *Catalog) GuardedNearMatchWindowForReviewStageSourceName(sourceName string) time.Duration {
	return c.guardedNearMatchWindowForReviewStageSourceName(sourceName)
}

func (c *Catalog) ListingsURLForSourceName(sourceName string) string {
	if c == nil {
		return ""
	}
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return ""
	}
	for _, cfg := range c.byKey {
		if sourceName == strings.TrimSpace(cfg.ReviewStageSourceName) ||
			sourceName == strings.TrimSpace(cfg.Name) ||
			sourceName == strings.TrimSpace(cfg.CalendarSourceName) ||
			sourceName == strings.TrimSpace(cfg.LinkedPageSourceName) {
			return strings.TrimSpace(cfg.URL)
		}
	}
	return ""
}

func (c *Catalog) VenueSlugForSourceLocation(source, value string) string {
	if c == nil {
		return VenueSlugFromText(value)
	}
	cfg, err := c.configForSource(source)
	if err != nil {
		return VenueSlugFromText(value)
	}
	family := strings.TrimSpace(cfg.VenueNormalizerFamily)
	if family == "" {
		return VenueSlugFromText(value)
	}
	return venueSlugForNormalizerFamily(family, value)
}

func (c *Catalog) detectReplaySourcePageSnapshot(decoded []decodedReplaySnapshot) (sourceConfig, decodedReplaySnapshot, error) {
	if c == nil {
		return sourceConfig{}, decodedReplaySnapshot{}, errors.New("catalog is nil")
	}
	var matchedCfg sourceConfig
	var matchedSnapshot decodedReplaySnapshot
	found := false

	for _, key := range c.sourceOrder {
		cfg := c.byKey[key]
		var matches []decodedReplaySnapshot
		for _, snapshot := range decoded {
			if !cfg.matchesReplayPageSnapshot(snapshot) {
				continue
			}
			matches = append(matches, snapshot)
		}
		switch len(matches) {
		case 0:
			continue
		case 1:
			if found {
				return sourceConfig{}, decodedReplaySnapshot{}, fmt.Errorf("multiple source page snapshots matched supported sources")
			}
			matchedCfg = cfg
			matchedSnapshot = matches[0]
			found = true
		default:
			return sourceConfig{}, decodedReplaySnapshot{}, fmt.Errorf("multiple source page snapshots for %q at %q", cfg.Name, cfg.URL)
		}
	}

	if !found {
		return sourceConfig{}, decodedReplaySnapshot{}, fmt.Errorf("no source page snapshot matched a supported source")
	}
	return matchedCfg, matchedSnapshot, nil
}

var defaultCatalogState struct {
	mu      sync.RWMutex
	catalog *Catalog
	err     error
}

func defaultCatalogDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return filepath.Join("config", "sources")
	}
	for {
		candidate := filepath.Join(dir, "config", "sources")
		info, statErr := os.Stat(candidate)
		if statErr == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return filepath.Join("config", "sources")
		}
		dir = parent
	}
}

func SetDefaultCatalog(catalog *Catalog) {
	defaultCatalogState.mu.Lock()
	defer defaultCatalogState.mu.Unlock()
	defaultCatalogState.catalog = catalog
	defaultCatalogState.err = nil
}

func LoadRepoCatalog() (*Catalog, error) {
	return LoadCatalog(defaultCatalogDir())
}

func DefaultCatalog() (*Catalog, error) {
	defaultCatalogState.mu.RLock()
	catalog := defaultCatalogState.catalog
	err := defaultCatalogState.err
	defaultCatalogState.mu.RUnlock()
	if catalog != nil || err != nil {
		return catalog, err
	}

	catalog, err = LoadRepoCatalog()

	defaultCatalogState.mu.Lock()
	defaultCatalogState.catalog = catalog
	defaultCatalogState.err = err
	defaultCatalogState.mu.Unlock()
	return catalog, err
}

func RegisteredSourceKeys() []string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return nil
	}
	return catalog.registeredSourceKeys()
}

func configForSource(source string) (sourceConfig, error) {
	catalog, err := DefaultCatalog()
	if err != nil {
		return sourceConfig{}, err
	}
	return catalog.configForSource(source)
}

func OwnedVenueSlugForSource(source string) string {
	cfg, err := configForSource(source)
	if err != nil {
		return ""
	}
	return cfg.ownedVenueSlug()
}

func OwnedVenueSlugsForSource(source string) []string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return nil
	}
	return catalog.OwnedVenueSlugsForSource(source)
}

func ReviewStageSourceNameForSource(source string) string {
	cfg, err := configForSource(source)
	if err != nil {
		source = strings.TrimSpace(source)
		if source == "" {
			return reviewStageDefaultSourceName
		}
		return source + " manual ingest"
	}
	if strings.TrimSpace(cfg.ReviewStageSourceName) != "" {
		return cfg.ReviewStageSourceName
	}
	if strings.TrimSpace(cfg.Key) == "" || cfg.Key == DefaultSource {
		return reviewStageDefaultSourceName
	}
	return cfg.Key + " manual ingest"
}

func OwnedVenueSlugForReviewStageSourceName(sourceName string) string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return ""
	}
	return catalog.ownedVenueSlugForReviewStageSourceName(sourceName)
}

func OwnedVenueSlugsForReviewStageSourceName(sourceName string) []string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return nil
	}
	return catalog.OwnedVenueSlugsForReviewStageSourceName(sourceName)
}

func NonAuthoritativeSingletonVenueSlugForSource(source string) string {
	cfg, err := configForSource(source)
	if err != nil {
		return ""
	}
	return cfg.nonAuthoritativeSingletonVenueSlug()
}

func NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName string) string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return ""
	}
	return catalog.nonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName)
}

func GuardedNearMatchDisabledForSource(source string) bool {
	catalog, err := DefaultCatalog()
	if err != nil {
		return false
	}
	return catalog.guardedNearMatchDisabledForSource(source)
}

func GuardedNearMatchWindowForSource(source string) time.Duration {
	catalog, err := DefaultCatalog()
	if err != nil {
		return 75 * time.Minute
	}
	return catalog.guardedNearMatchWindowForSource(source)
}

func GuardedNearMatchDisabledForReviewStageSourceName(sourceName string) bool {
	catalog, err := DefaultCatalog()
	if err != nil {
		return false
	}
	return catalog.guardedNearMatchDisabledForReviewStageSourceName(sourceName)
}

func GuardedNearMatchWindowForReviewStageSourceName(sourceName string) time.Duration {
	catalog, err := DefaultCatalog()
	if err != nil {
		return 75 * time.Minute
	}
	return catalog.guardedNearMatchWindowForReviewStageSourceName(sourceName)
}

func ListingsURLForSourceName(sourceName string) string {
	catalog, err := DefaultCatalog()
	if err != nil {
		return ""
	}
	return catalog.ListingsURLForSourceName(sourceName)
}
