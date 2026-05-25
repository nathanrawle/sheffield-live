package ingest

import (
	"fmt"
	"strings"
)

type sourcePageParseResult struct {
	Links []string
	Parse ParseResult
}

type sourceRoomEvidence struct {
	Text  string
	Rooms []RoomCandidate
}

type sourcePageParserFunc func(pageURL string, body []byte, limit int) ParseResult
type pageLinkExtractorFunc func(pageURL string, body []byte, limit int) ([]string, error)
type icsParserFunc func(body []byte) ParseResult
type venueNormalizerFunc func(value string) string

var sourcePageParserFamilies = map[string]sourcePageParserFunc{
	"yellow_arch_jsonld": ParseYellowArchSourcePage,
	"cafe_no_9":          ParseCafeNo9SourcePage,
	"jazz_at_the_lescar": ParseJazzAtTheLescarSourcePage,
	"crookes_club":       ParseCrookesClubSourcePage,
}

var sourcePageLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"cafe_no_9_pagination":         ExtractCafeNo9SourcePageLinks,
	"crookes_club_secondary_pages": ExtractCrookesClubSecondaryPages,
}

var icsLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"sidney_and_matilda":               ExtractSidneyAndMatildaICSLinks,
	"leadmill_calendar":                ExtractLeadmillICSLinks,
	"hallamshire_hotel_cfg_filestring": hallamshire_hotel_cfg_filestring,
}

var icsParserFamilies = map[string]icsParserFunc{
	"generic":           ParseICS,
	"leadmill":          ParseLeadmillICS,
	"hallamshire_hotel": ParseHallamshireHotelICS,
}

var linkedPageLinkExtractorFamilies = map[string]pageLinkExtractorFunc{
	"corporation_detail_links":                   ExtractCorporationDetailLinks,
	"alder_listing_links":                        alder_listing_links,
	"delicious_clam_ticket_links":                ExtractDeliciousClamTicketLinks,
	"hagglers_corner_detail_links":               hagglers_corner_detail_links,
	"greystones_month_links":                     ExtractTheGreystonesMonthLinks,
	"the_washington_api_links":                   the_washington_api_links,
	"network_sheffield_detail_links":             network_sheffield_detail_links,
	"university_performance_venues_detail_links": university_performance_venues_detail_links,
}

var linkedPageParserFamilies = map[string]func(pageURL string, body []byte) ParseResult{
	"corporation_detail_page":                   ParseCorporationDetailPage,
	"alder_event_detail_page":                   ParseAlderEventDetailPage,
	"delicious_clam_ticket_page":                ParseDeliciousClamTicketPage,
	"hagglers_corner_detail_page":               ParseHagglersCornerDetailPage,
	"greystones_month_page":                     ParseTheGreystonesMonthPage,
	"the_washington_api":                        ParseTheWashingtonAPIDetailPage,
	"network_sheffield_detail_page":             ParseNetworkSheffieldDetailPage,
	"university_performance_venues_detail_page": ParseUniversityPerformanceVenuesDetailPage,
}

var venueNormalizerFamilies = map[string]venueNormalizerFunc{
	"default":           VenueSlugFromText,
	"leadmill":          func(value string) string { return VenueSlugFromText(leadmillVenueText(value)) },
	"network_sheffield": func(value string) string { return networkSheffieldVenueSlugFromText(value) },
	"university_performance_venues": func(value string) string {
		slug, _, _ := universityPerformanceVenuesVenueSlugFromText(value)
		return slug
	},
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
	return normalizeParseResultEventTitles(cfg, parser(pageURL, body, limit)), nil
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
	return normalizeParseResultEventTitles(cfg, parser(body))
}

func roomEvidenceForSourcePage(cfg sourceConfig, pageURL string, body []byte) map[string]sourceRoomEvidence {
	switch strings.TrimSpace(cfg.Key) {
	case DefaultSource:
		return ExtractSidneyAndMatildaRoomEvidence(pageURL, body)
	default:
		return nil
	}
}

func mergeRoomEvidence(candidates []EventCandidate, evidence map[string]sourceRoomEvidence) []EventCandidate {
	if len(candidates) == 0 || len(evidence) == 0 {
		return candidates
	}
	out := append([]EventCandidate(nil), candidates...)
	for i := range out {
		match, ok := evidence[roomEvidenceCandidateKey(out[i])]
		if !ok {
			match, ok = evidence[roomEvidenceTitleKey(out[i].Summary)]
		}
		if !ok {
			continue
		}
		out[i].RoomText = strings.TrimSpace(match.Text)
		out[i].Rooms = append([]RoomCandidate(nil), match.Rooms...)
	}
	return out
}

func roomEvidenceCandidateKey(candidate EventCandidate) string {
	if value := strings.TrimSpace(candidate.URL); value != "" {
		return "url:" + strings.TrimRight(value, "/")
	}
	return roomEvidenceTitleKey(candidate.Summary)
}

func roomEvidenceTitleKey(title string) string {
	return "title:" + strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(title)), " "))
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
	return normalizeParseResultEventTitles(cfg, parser(pageURL, body)), nil
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
	return venueSlugForNormalizerFamily(family, value)
}

func limitParseResult(parse ParseResult, limit int) ParseResult {
	if limit <= 0 || len(parse.Candidates) <= limit {
		return parse
	}
	parse.Candidates = append([]EventCandidate(nil), parse.Candidates[:limit]...)
	return parse
}

func venueSlugForNormalizerFamily(family, value string) string {
	family = strings.TrimSpace(family)
	if family == "" {
		return VenueSlugFromText(value)
	}
	normalizer, ok := venueNormalizerFamilies[family]
	if !ok {
		return VenueSlugFromText(value)
	}
	return normalizer(value)
}
