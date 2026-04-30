package ingest

import (
	"fmt"
	"strings"
)

type sourcePageParseResult struct {
	Links []string
	Parse ParseResult
}

type sourcePageParserFunc func(pageURL string, body []byte, limit int) ParseResult
type pageLinkExtractorFunc func(pageURL string, body []byte, limit int) ([]string, error)
type icsParserFunc func(body []byte) ParseResult
type venueNormalizerFunc func(value string) string

var sourcePageParserFamilies = map[string]sourcePageParserFunc{
	"yellow_arch_jsonld": ParseYellowArchSourcePage,
	"cafe_no_9":          ParseCafeNo9SourcePage,
	"jazz_at_the_lescar": ParseJazzAtTheLescarSourcePage,
}

var sourcePageLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"cafe_no_9_pagination": ExtractCafeNo9SourcePageLinks,
}

var icsLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"sidney_and_matilda": ExtractSidneyAndMatildaICSLinks,
	"leadmill_calendar":  ExtractLeadmillICSLinks,
}

var icsParserFamilies = map[string]icsParserFunc{
	"generic":  ParseICS,
	"leadmill": ParseLeadmillICS,
}

var linkedPageLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"corporation_detail_links": ExtractCorporationDetailLinks,
	"greystones_month_links":   ExtractTheGreystonesMonthLinks,
}

var linkedPageParserFamilies = map[string]func(pageURL string, body []byte) ParseResult{
	"corporation_detail_page": ParseCorporationDetailPage,
	"greystones_month_page":   ParseTheGreystonesMonthPage,
}

var venueNormalizerFamilies = map[string]venueNormalizerFunc{
	"default":  VenueSlugFromText,
	"leadmill": func(value string) string { return VenueSlugFromText(leadmillVenueText(value)) },
}

func hasSourcePageParserFamily(name string) bool {
	_, ok := sourcePageParserFamilies[strings.TrimSpace(name)]
	return ok
}

func hasSourcePageLinkExtractorFamily(name string) bool {
	_, ok := sourcePageLinkExtractorFamilies[strings.TrimSpace(name)]
	return ok
}

func hasICSLinkExtractorFamily(name string) bool {
	_, ok := icsLinkExtractorFamilies[strings.TrimSpace(name)]
	return ok
}

func hasICSParserFamily(name string) bool {
	_, ok := icsParserFamilies[strings.TrimSpace(name)]
	return ok
}

func hasLinkedPageLinkExtractorFamily(name string) bool {
	_, ok := linkedPageLinkExtractorFamilies[strings.TrimSpace(name)]
	return ok
}

func hasLinkedPageParserFamily(name string) bool {
	_, ok := linkedPageParserFamilies[strings.TrimSpace(name)]
	return ok
}

func hasVenueNormalizerFamily(name string) bool {
	_, ok := venueNormalizerFamilies[strings.TrimSpace(name)]
	return ok
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
	parser, ok := sourcePageParserFamilies[strings.TrimSpace(cfg.SourcePageParserFamily)]
	if !ok {
		return ParseResult{}, fmt.Errorf("unsupported source page parser family %q", cfg.SourcePageParserFamily)
	}
	return parser(pageURL, body, limit), nil
}

func extractSourcePageLinksForSource(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	if strings.TrimSpace(cfg.SourcePageLinkExtractorFamily) == "" {
		return nil, nil
	}
	extractor, ok := sourcePageLinkExtractorFamilies[strings.TrimSpace(cfg.SourcePageLinkExtractorFamily)]
	if !ok {
		return nil, fmt.Errorf("unsupported source page link extractor family %q", cfg.SourcePageLinkExtractorFamily)
	}
	return extractor(pageURL, body, limit)
}

func extractLinkedICSLinks(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	extractor, ok := icsLinkExtractorFamilies[strings.TrimSpace(cfg.ICSLinkExtractorFamily)]
	if !ok {
		return nil, fmt.Errorf("unsupported linked ICS source family %q", cfg.ICSLinkExtractorFamily)
	}
	return extractor(pageURL, body, limit)
}

func parseICSForSource(cfg sourceConfig, body []byte) ParseResult {
	parser, ok := icsParserFamilies[strings.TrimSpace(cfg.ICSParserFamily)]
	if !ok {
		return ParseResult{Errors: []string{fmt.Sprintf("unsupported ICS parser family %q", cfg.ICSParserFamily)}}
	}
	return parser(body)
}

func extractLinkedDetailPageLinks(cfg sourceConfig, pageURL string, body []byte, limit int) ([]string, error) {
	extractor, ok := linkedPageLinkExtractorFamilies[strings.TrimSpace(cfg.LinkedPageLinkExtractorFamily)]
	if !ok {
		return nil, fmt.Errorf("unsupported linked detail page source family %q", cfg.LinkedPageLinkExtractorFamily)
	}
	return extractor(pageURL, body, limit)
}

func parseLinkedPageForSource(cfg sourceConfig, pageURL string, body []byte) (ParseResult, error) {
	parser, ok := linkedPageParserFamilies[strings.TrimSpace(cfg.LinkedPageParserFamily)]
	if !ok {
		return ParseResult{}, fmt.Errorf("unsupported linked page source family %q", cfg.LinkedPageParserFamily)
	}
	return parser(pageURL, body), nil
}

func VenueSlugForSourceLocation(source, value string) string {
	cfg, err := configForSource(source)
	if err != nil {
		return VenueSlugFromText(value)
	}
	family := strings.TrimSpace(cfg.VenueNormalizerFamily)
	if family == "" {
		return VenueSlugFromText(value)
	}
	normalizer, ok := venueNormalizerFamilies[family]
	if !ok {
		return VenueSlugFromText(value)
	}
	return normalizer(value)
}

func limitParseResult(parse ParseResult, limit int) ParseResult {
	if limit <= 0 || len(parse.Candidates) <= limit {
		return parse
	}
	parse.Candidates = append([]EventCandidate(nil), parse.Candidates[:limit]...)
	return parse
}
