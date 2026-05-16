package ingest

import (
	"html"
	"strings"
	"unicode"
	"unicode/utf8"
)

var eventTitleVenueAliases = map[string][]string{
	"cafe-no-9":          {"Cafe No. 9", "Cafe No9"},
	"corporation":        {"Corporation", "Corporation Sheffield"},
	"leadmill":           {"The Leadmill", "Leadmill"},
	"lescar":             {"The Lescar", "Lescar"},
	"greystones":         {"The Greystones", "Greystones"},
	"sidney-and-matilda": {"Sidney & Matilda", "Sidney and Matilda"},
	"yellow-arch":        {"Yellow Arch", "Yellow Arch Studios"},
}

var eventTitleBoilerplatePrefixes = map[string][]string{
	"cafe-no-9": {"An evening with "},
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
	venueSlug = strings.TrimSpace(venueSlug)
	cleaned := normalizeEventTitleSpacing(decodeEventTitleHTMLEntities(strings.TrimSpace(title)))
	if cleaned == "" {
		return ""
	}
	if trimmed := trimLeadingEventTitlePunctuation(cleaned); trimmed != "" {
		cleaned = trimmed
	}

	cleaned = stripVenueAffixFromEventTitle(cleaned, venueSlug)
	cleaned = stripEventTitleBoilerplatePrefix(cleaned, venueSlug)
	return cleaned
}

func stripVenueAffixFromEventTitle(title, venueSlug string) string {
	for _, alias := range eventTitleVenueAliases[venueSlug] {
		alias = normalizeEventTitleSpacing(alias)
		if alias == "" {
			continue
		}
		if stripped, ok := stripVenueTitlePrefixColon(title, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitlePrefixDash(title, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleParentheticalSuffix(title, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleOccurrenceSuffix(title, alias); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffix(title, alias, " - "); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffix(title, alias, " @ "); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffix(title, alias, " // "); ok {
			return stripped
		}
		if stripped, ok := stripVenueTitleSuffixAt(title, alias); ok {
			return stripped
		}
	}
	return title
}

func decodeEventTitleHTMLEntities(value string) string {
	for i := 0; i < 4; i++ {
		decoded := html.UnescapeString(value)
		if decoded == value {
			break
		}
		value = decoded
	}
	return value
}

func trimLeadingEventTitlePunctuation(value string) string {
	trimmed := strings.TrimLeftFunc(value, func(r rune) bool {
		switch r {
		case '|', '-', '–', '—', ':', '•', '·':
			return true
		default:
			return unicode.IsSpace(r)
		}
	})
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return value
	}
	return trimmed
}

func stripEventTitleBoilerplatePrefix(title, venueSlug string) string {
	for _, prefix := range eventTitleBoilerplatePrefixes[venueSlug] {
		if len(title) < len(prefix) || !strings.EqualFold(title[:len(prefix)], prefix) {
			continue
		}
		if stripped, ok := nonEmptyEventTitleRemainder(title[len(prefix):]); ok {
			return stripped
		}
	}
	return title
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

func stripVenueTitleParentheticalSuffix(title, alias string) (string, bool) {
	if !strings.HasSuffix(title, ")") {
		return "", false
	}
	start := strings.LastIndex(title, "(")
	if start < 0 || !sameEventTitleText(title[start+1:len(title)-1], alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(title[:start])
}

func stripVenueTitleOccurrenceSuffix(title, alias string) (string, bool) {
	searchTitle := lowerASCII(title)
	searchAlias := lowerASCII(alias)
	for searchEnd := len(title); searchEnd > 0; {
		idx := strings.LastIndex(searchTitle[:searchEnd], searchAlias)
		if idx < 0 {
			break
		}
		end := idx + len(alias)
		if eventTitleAliasOccurrenceHasBoundaries(title, idx, end) && eventTitleVenueOccurrenceCanRunToEnd(title[end:]) {
			if stripStart, ok := eventTitleVenueOccurrenceStripStart(title, idx); ok {
				if stripped, ok := nonEmptyEventTitleRemainder(title[:stripStart]); ok {
					return stripped, true
				}
			}
		}
		searchEnd = idx
	}
	return "", false
}

func eventTitleAliasOccurrenceHasBoundaries(title string, start, end int) bool {
	if start > 0 {
		r, _ := lastRuneBefore(title, start)
		if isEventTitleWordRune(r) {
			return false
		}
	}
	if end < len(title) {
		r, _ := utf8.DecodeRuneInString(title[end:])
		if isEventTitleWordRune(r) {
			return false
		}
	}
	return true
}

func eventTitleVenueOccurrenceCanRunToEnd(remainder string) bool {
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(remainder)
	return !isEventTitleWordRune(r)
}

func eventTitleVenueOccurrenceStripStart(title string, aliasStart int) (int, bool) {
	start := aliasStart
	for start > 0 {
		r, size := lastRuneBefore(title, start)
		if isEventTitleWordRune(r) {
			break
		}
		start -= size
	}

	connector := title[start:aliasStart]
	if containsEventTitleConnectorPunctuation(connector) {
		return start, true
	}

	wordStart, word, ok := eventTitleWordBefore(title, start)
	if !ok || !strings.EqualFold(word, "at") {
		return 0, false
	}
	if liveStart, liveWord, ok := eventTitleWordBefore(title, eventTitleTrimSpaceLeft(title, wordStart)); ok && strings.EqualFold(liveWord, "live") {
		return liveStart, true
	}
	return wordStart, true
}

func containsEventTitleConnectorPunctuation(value string) bool {
	for _, r := range value {
		if !unicode.IsSpace(r) && !isEventTitleWordRune(r) {
			return true
		}
	}
	return false
}

func eventTitleWordBefore(title string, end int) (int, string, bool) {
	if end <= 0 {
		return 0, "", false
	}
	r, size := lastRuneBefore(title, end)
	if !isEventTitleWordRune(r) {
		return 0, "", false
	}
	start := end - size
	for start > 0 {
		r, size = lastRuneBefore(title, start)
		if !isEventTitleWordRune(r) {
			break
		}
		start -= size
	}
	return start, title[start:end], true
}

func eventTitleTrimSpaceLeft(title string, end int) int {
	for end > 0 {
		r, size := lastRuneBefore(title, end)
		if !unicode.IsSpace(r) {
			break
		}
		end -= size
	}
	return end
}

func lastRuneBefore(value string, end int) (rune, int) {
	r, size := utf8.DecodeLastRuneInString(value[:end])
	return r, size
}

func isEventTitleWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func lowerASCII(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
	return b.String()
}

func stripVenueTitleSuffix(title, alias, marker string) (string, bool) {
	idx := strings.LastIndex(title, marker)
	if idx < 0 || !eventTitleVenueSuffixMatches(title[idx+len(marker):], alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(title[:idx])
}

func stripVenueTitleSuffixAt(title, alias string) (string, bool) {
	const marker = " at "
	idx := strings.LastIndex(strings.ToLower(title), marker)
	if idx < 0 || !eventTitleVenueSuffixMatches(title[idx+len(marker):], alias) {
		return "", false
	}
	return nonEmptyEventTitleRemainder(title[:idx])
}

func eventTitleVenueSuffixMatches(suffix, alias string) bool {
	if sameEventTitleText(suffix, alias) {
		return true
	}
	if head, ok := eventTitleSuffixBeforeTrailingParenthetical(suffix); ok {
		return sameEventTitleText(head, alias)
	}
	return false
}

func eventTitleSuffixBeforeTrailingParenthetical(suffix string) (string, bool) {
	suffix = normalizeEventTitleSpacing(suffix)
	if !strings.HasSuffix(suffix, ")") {
		return "", false
	}
	start := strings.LastIndex(suffix, "(")
	if start < 0 {
		return "", false
	}
	head := strings.TrimSpace(suffix[:start])
	qualifier := strings.TrimSpace(suffix[start+1 : len(suffix)-1])
	return head, head != "" && qualifier != ""
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
