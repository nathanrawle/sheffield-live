package ingest

import (
	"fmt"
	"strings"
)

const YellowArchSource = "yellow-arch"
const yellowArchSource = YellowArchSource
const CafeNo9Source = "cafe-no-9"
const LeadmillSource = "leadmill"
const CorporationSource = "corporation"

type pageProcessMode string

const (
	pageProcessLinkedICS         pageProcessMode = "linked_ics"
	pageProcessSourcePage        pageProcessMode = "source_page"
	pageProcessLinkedDetailPages pageProcessMode = "linked_detail_pages"
)

type sourceConfig struct {
	Key                   string
	Name                  string
	URL                   string
	OwnedVenueSlug        string
	CalendarSourceName    string
	LinkedPageSourceName  string
	PageMode              pageProcessMode
	ReviewStageSourceName string
	ImportRunNotes        string
}

var sourceRegistry = []sourceConfig{
	{
		Key:                   DefaultSource,
		Name:                  "Sidney & Matilda listings",
		URL:                   "https://www.sidneyandmatilda.com/",
		OwnedVenueSlug:        "sidney-and-matilda",
		CalendarSourceName:    "Sidney & Matilda Google Calendar ICS",
		PageMode:              pageProcessLinkedICS,
		ReviewStageSourceName: "Sidney & Matilda manual ingest",
		ImportRunNotes:        "manual Sidney & Matilda snapshot + ICS parse report",
	},
	{
		Key:                   YellowArchSource,
		Name:                  "Yellow Arch listings",
		URL:                   "https://www.yellowarch.com/events/",
		OwnedVenueSlug:        "yellow-arch",
		PageMode:              pageProcessSourcePage,
		ReviewStageSourceName: "Yellow Arch manual ingest",
		ImportRunNotes:        "manual Yellow Arch source-page parse report",
	},
	{
		Key:                   CafeNo9Source,
		Name:                  "Cafe No. 9 listings",
		URL:                   "https://www.wegottickets.com/Cafe9",
		OwnedVenueSlug:        "cafe-no-9",
		PageMode:              pageProcessSourcePage,
		ReviewStageSourceName: "Cafe No. 9 manual ingest",
		ImportRunNotes:        "manual Cafe No. 9 source-page parse report",
	},
	{
		Key:                   LeadmillSource,
		Name:                  "The Leadmill listings",
		URL:                   "https://leadmill.co.uk/live/",
		OwnedVenueSlug:        "leadmill",
		CalendarSourceName:    "The Leadmill iCal feed",
		PageMode:              pageProcessLinkedICS,
		ReviewStageSourceName: "The Leadmill manual ingest",
		ImportRunNotes:        "manual The Leadmill snapshot + ICS parse report",
	},
	{
		Key:                   CorporationSource,
		Name:                  "Corporation Sheffield live listings",
		URL:                   "https://www.corporation.org.uk/live/",
		OwnedVenueSlug:        "corporation",
		LinkedPageSourceName:  "Corporation Sheffield event detail page",
		PageMode:              pageProcessLinkedDetailPages,
		ReviewStageSourceName: "Corporation Sheffield manual ingest",
		ImportRunNotes:        "manual Corporation Sheffield snapshot + detail-page parse report",
	},
}

func RegisteredSourceKeys() []string {
	keys := make([]string, 0, len(sourceRegistry))
	for _, cfg := range sourceRegistry {
		keys = append(keys, cfg.Key)
	}
	return keys
}

func configForSource(source string) (sourceConfig, error) {
	key := strings.TrimSpace(source)
	if key == "" {
		key = DefaultSource
	}
	for _, cfg := range sourceRegistry {
		if cfg.Key == key {
			return cfg, nil
		}
	}
	return sourceConfig{}, fmt.Errorf("unsupported source %q", source)
}

func OwnedVenueSlugForSource(source string) string {
	cfg, err := configForSource(source)
	if err != nil {
		return ""
	}
	return cfg.OwnedVenueSlug
}

func detectReplaySourcePageSnapshot(decoded []decodedReplaySnapshot) (sourceConfig, decodedReplaySnapshot, error) {
	var matchedCfg sourceConfig
	var matchedSnapshot decodedReplaySnapshot
	found := false

	for _, cfg := range sourceRegistry {
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

type sourcePageParseResult struct {
	Links []string
	Parse ParseResult
}

func parseSourcePage(cfg sourceConfig, pageURL string, body []byte, limit int) (sourcePageParseResult, error) {
	switch cfg.PageMode {
	case pageProcessLinkedICS:
		links, err := extractLinkedICSLinks(cfg, pageURL, body, limit)
		if err != nil {
			return sourcePageParseResult{}, fmt.Errorf("extract ICS links: %w", err)
		}
		return sourcePageParseResult{Links: links}, nil
	case pageProcessSourcePage:
		parse, err := parseSourcePageForSource(cfg, pageURL, body, limit)
		if err != nil {
			return sourcePageParseResult{}, err
		}
		links, err := extractSourcePageLinksForSource(cfg, pageURL, body, limit)
		if err != nil {
			return sourcePageParseResult{}, err
		}
		return sourcePageParseResult{Links: links, Parse: parse}, nil
	case pageProcessLinkedDetailPages:
		links, err := extractLinkedDetailPageLinks(cfg, pageURL, body, limit)
		if err != nil {
			return sourcePageParseResult{}, fmt.Errorf("extract detail page links: %w", err)
		}
		return sourcePageParseResult{Links: links}, nil
	default:
		return sourcePageParseResult{}, fmt.Errorf("unsupported source mode %q", cfg.PageMode)
	}
}

func parseSourcePageForSource(cfg sourceConfig, pageURL string, body []byte, limit int) (ParseResult, error) {
	switch cfg.Key {
	case YellowArchSource:
		return ParseYellowArchSourcePage(pageURL, body, limit), nil
	case CafeNo9Source:
		return ParseCafeNo9SourcePage(pageURL, body, limit), nil
	default:
		return ParseResult{}, fmt.Errorf("unsupported source page parser %q", cfg.Key)
	}
}

func extractSourcePageLinksForSource(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	switch cfg.Key {
	case CafeNo9Source:
		return ExtractCafeNo9SourcePageLinks(pageURL, body, limit)
	default:
		return nil, nil
	}
}

func extractLinkedICSLinks(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	switch cfg.Key {
	case DefaultSource:
		return ExtractSidneyAndMatildaICSLinks(pageURL, body, limit)
	case LeadmillSource:
		return ExtractLeadmillICSLinks(pageURL, body, limit)
	default:
		return nil, fmt.Errorf("unsupported linked ICS source %q", cfg.Key)
	}
}

func parseICSForSource(cfg sourceConfig, body []byte) ParseResult {
	switch cfg.Key {
	case LeadmillSource:
		return ParseLeadmillICS(body)
	default:
		return ParseICS(body)
	}
}

func extractLinkedDetailPageLinks(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	switch cfg.Key {
	case CorporationSource:
		return ExtractCorporationDetailLinks(pageURL, body, limit)
	default:
		return nil, fmt.Errorf("unsupported linked detail page source %q", cfg.Key)
	}
}

func parseLinkedPageForSource(cfg sourceConfig, pageURL string, body []byte) (ParseResult, error) {
	switch cfg.Key {
	case CorporationSource:
		return ParseCorporationDetailPage(pageURL, body), nil
	default:
		return ParseResult{}, fmt.Errorf("unsupported linked page source %q", cfg.Key)
	}
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

func limitParseResult(parse ParseResult, limit int) ParseResult {
	if limit <= 0 || len(parse.Candidates) <= limit {
		return parse
	}
	parse.Candidates = append([]EventCandidate(nil), parse.Candidates[:limit]...)
	return parse
}
