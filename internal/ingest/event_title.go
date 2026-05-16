package ingest

import "strings"

var eventTitleVenueAliases = map[string][]string{
	"cafe-no-9":          {"Cafe No. 9", "Cafe No9"},
	"corporation":        {"Corporation", "Corporation Sheffield"},
	"leadmill":           {"The Leadmill", "Leadmill"},
	"lescar":             {"The Lescar", "Lescar"},
	"greystones":         {"The Greystones", "Greystones"},
	"sidney-and-matilda": {"Sidney & Matilda", "Sidney and Matilda"},
	"yellow-arch":        {"Yellow Arch", "Yellow Arch Studios"},
}

func normalizeParseResultEventTitles(cfg sourceConfig, parse ParseResult) ParseResult {
	if len(parse.Candidates) == 0 {
		return parse
	}

	candidates := append([]EventCandidate(nil), parse.Candidates...)
	for i := range candidates {
		candidates[i].Summary = cleanEventCandidateSummaryForConfig(cfg, candidates[i])
	}
	parse.Candidates = candidates
	return parse
}

func cleanEventCandidateSummaryForConfig(cfg sourceConfig, candidate EventCandidate) string {
	venueSlug := venueSlugForNormalizerFamily(cfg.VenueNormalizerFamily, candidate.Location)
	return stripVenueNameFromEventTitle(candidate.Summary, venueSlug)
}

func cleanEventCandidateSummaryForCatalog(catalog *Catalog, source string, candidate EventCandidate) string {
	venueSlug := VenueSlugFromText(candidate.Location)
	if catalog != nil {
		venueSlug = catalog.VenueSlugForSourceLocation(source, candidate.Location)
	}
	return stripVenueNameFromEventTitle(candidate.Summary, venueSlug)
}

func CleanEventTitleForVenue(title, venueSlug string) string {
	return stripVenueNameFromEventTitle(title, venueSlug)
}

func stripVenueNameFromEventTitle(title, venueSlug string) string {
	original := strings.TrimSpace(title)
	cleaned := normalizeEventTitleSpacing(original)
	if cleaned == "" {
		return ""
	}

	for _, alias := range eventTitleVenueAliases[strings.TrimSpace(venueSlug)] {
		alias = normalizeEventTitleSpacing(alias)
		if alias == "" {
			continue
		}
		if stripped, ok := stripVenueTitlePrefixColon(cleaned, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitlePrefixDash(cleaned, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffix(cleaned, alias, " - "); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffix(cleaned, alias, " @ "); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffixAt(cleaned, alias); ok {
			return stripped
		}
	}
	return original
}

func stripVenueTitlePrefixColon(title, alias string) (string, bool) {
	prefix, rest, ok := strings.Cut(title, ":")
	if !ok || !sameEventTitleText(prefix, alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(rest)
}

func stripVenueTitlePrefixDash(title, alias string) (string, bool) {
	prefix, rest, ok := strings.Cut(title, " - ")
	if !ok || !sameEventTitleText(prefix, alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(rest)
}

func stripVenueTitleSuffix(title, alias, marker string) (string, bool) {
	idx := strings.LastIndex(title, marker)
	if idx < 0 || !sameEventTitleText(title[idx+len(marker):], alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(title[:idx])
}

func stripVenueTitleSuffixAt(title, alias string) (string, bool) {
	const marker = " at "
	idx := strings.LastIndex(strings.ToLower(title), marker)
	if idx < 0 || !sameEventTitleText(title[idx+len(marker):], alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(title[:idx])
}

func nonEmptyEventTitleRemainder(value string) (string, bool) {
	value = normalizeEventTitleSpacing(value)
	return value, value != ""
}

func sameEventTitleText(a, b string) bool {
	return strings.EqualFold(normalizeEventTitleSpacing(a), normalizeEventTitleSpacing(b))
}

func normalizeEventTitleSpacing(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}
