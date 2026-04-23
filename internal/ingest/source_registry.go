package ingest

import (
	"fmt"
	"strings"
)

const YellowArchSource = "yellow-arch"
const yellowArchSource = YellowArchSource

type pageProcessMode string

const (
	pageProcessLinkedICS  pageProcessMode = "linked_ics"
	pageProcessSourcePage pageProcessMode = "source_page"
)

type sourceConfig struct {
	Key                   string
	Name                  string
	URL                   string
	CalendarSourceName    string
	PageMode              pageProcessMode
	ReviewStageSourceName string
	ImportRunNotes        string
}

var sourceRegistry = []sourceConfig{
	{
		Key:                   DefaultSource,
		Name:                  "Sidney & Matilda listings",
		URL:                   "https://www.sidneyandmatilda.com/",
		CalendarSourceName:    "Sidney & Matilda Google Calendar ICS",
		PageMode:              pageProcessLinkedICS,
		ReviewStageSourceName: "Sidney & Matilda manual ingest",
		ImportRunNotes:        "manual Sidney & Matilda snapshot + ICS parse report",
	},
	{
		Key:                   YellowArchSource,
		Name:                  "Yellow Arch listings",
		URL:                   "https://www.yellowarch.com/events/",
		PageMode:              pageProcessSourcePage,
		ReviewStageSourceName: "Yellow Arch manual ingest",
		ImportRunNotes:        "manual Yellow Arch source-page parse report",
	},
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
		links, err := ExtractSidneyAndMatildaICSLinks(pageURL, body, limit)
		if err != nil {
			return sourcePageParseResult{}, fmt.Errorf("extract ICS links: %w", err)
		}
		return sourcePageParseResult{Links: links}, nil
	case pageProcessSourcePage:
		return sourcePageParseResult{Parse: ParseYellowArchSourcePage(pageURL, body, limit)}, nil
	default:
		return sourcePageParseResult{}, fmt.Errorf("unsupported source mode %q", cfg.PageMode)
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
