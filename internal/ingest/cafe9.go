package ingest

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var (
	cafe9ListingPattern    = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>\s*<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>\s*</h[1-6]>`)
	cafe9PageLinkPattern   = regexp.MustCompile(`(?is)<a\b[^>]*href\s*=\s*["']([^"']*?/Cafe9/page/\d+/?[^"']*)["'][^>]*>`)
	cafe9LineBreakPattern  = regexp.MustCompile(`(?i)<br\s*/?>|</(?:p|div|li|td|tr|section|article|ul|ol|h[1-6])>`)
	cafe9TagPattern        = regexp.MustCompile(`(?is)<[^>]+>`)
	cafe9WhitespacePattern = regexp.MustCompile(`\s+`)
	cafe9OrdinalPattern    = regexp.MustCompile(`\b(\d{1,2})(st|nd|rd|th)\b`)
)

func ParseCafeNo9SourcePage(pageURL string, raw []byte, limit int) ParseResult {
	result := limitParseResult(ParseCafeNo9Page(pageURL, raw), limit)
	return result
}

func ParseCafeNo9Page(pageURL string, raw []byte) ParseResult {
	matches := cafe9ListingPattern.FindAllSubmatchIndex(raw, -1)
	if len(matches) == 0 {
		return ParseResult{Errors: []string{"no Cafe No. 9 listing blocks found"}}
	}

	baseURL, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return ParseResult{Errors: []string{fmt.Sprintf("parse Cafe No. 9 source page URL: %v", err)}}
	}
	allowMissingCategory := cafe9AllowsMissingCategory(baseURL)

	var result ParseResult
	for i, match := range matches {
		bodyStart := match[1]
		if len(match) >= 6 {
			bodyStart = match[5]
		}
		bodyEnd := len(raw)
		if i+1 < len(matches) {
			bodyEnd = matches[i+1][0]
		}
		candidate, skip, err := parseCafeNo9Listing(baseURL, raw[match[2]:match[3]], raw[match[4]:match[5]], raw[bodyStart:bodyEnd], allowMissingCategory)
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			continue
		}
		if skip.Reason != "" {
			result.Skips = append(result.Skips, skip)
			continue
		}
		result.Candidates = append(result.Candidates, candidate)
	}
	return result
}

func ExtractCafeNo9SourcePageLinks(pageURL string, raw []byte, limit int) ([]string, error) {
	baseURL, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return nil, fmt.Errorf("parse Cafe No. 9 source page URL: %w", err)
	}

	current := normalizeCafeNo9SourcePageURL(baseURL)
	seen := map[string]struct{}{current: {}}
	links := make([]string, 0)
	matches := cafe9PageLinkPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		resolved, err := resolvePageURL(baseURL, string(match[1]))
		if err != nil {
			return nil, fmt.Errorf("resolve Cafe No. 9 page link %q: %w", string(match[1]), err)
		}
		resolved = normalizeCafeNo9SourcePageURLString(resolved)
		if _, ok := seen[resolved]; ok {
			continue
		}
		seen[resolved] = struct{}{}
		links = append(links, resolved)
		if limit > 0 && len(links) >= limit {
			break
		}
	}
	return links, nil
}

func parseCafeNo9Listing(baseURL *url.URL, rawURL, rawTitle, rawBody []byte, allowMissingCategory bool) (EventCandidate, ParseSkip, error) {
	title := cafe9CleanText(string(rawTitle))
	skip := ParseSkip{Summary: title}
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}

	resolvedURL, err := resolvePageURL(baseURL, string(rawURL))
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("resolve Cafe No. 9 event URL for %q: %w", title, err)
	}

	lines := cafe9ListingLines(string(rawBody))
	var (
		venue       string
		dateText    string
		startText   string
		category    string
		description []string
	)
	for _, line := range lines {
		switch {
		case strings.EqualFold(line, "Sold out"):
			continue
		case strings.HasPrefix(strings.ToLower(line), "join waiting list"):
			continue
		case strings.EqualFold(line, "Event info"):
			continue
		case cafe9IsVenueLine(line):
			venue = cafe9VenueFromLine(line)
		case cafe9IsDateLine(line):
			dateText = cafe9TrimPrefixRune(line)
		case strings.Contains(strings.ToLower(line), "start time:"):
			startText = cafe9StartTimeFromLine(line)
		case cafe9IsCategoryLine(line):
			category = cafe9CategoryFromLine(line)
		case cafe9IsIgnorableDescriptionLine(line):
			continue
		default:
			description = append(description, line)
		}
	}

	switch {
	case venue == "":
		skip.Reason = "missing venue"
		return EventCandidate{}, skip, nil
	case VenueSlugFromText(venue) != "cafe-no-9":
		skip.Reason = "not a Cafe No. 9 venue listing"
		return EventCandidate{}, skip, nil
	case dateText == "":
		skip.Reason = "missing event date"
		return EventCandidate{}, skip, nil
	case startText == "":
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	case category == "" && allowMissingCategory && !cafe9AllowsMissingCategoryTitle(title):
		skip.Reason = "missing music category"
		return EventCandidate{}, skip, nil
	case category == "" && !allowMissingCategory:
		skip.Reason = "missing category"
		return EventCandidate{}, skip, nil
	case category != "" && !cafe9IsMusicCategory(category):
		skip.Reason = "non-music category"
		return EventCandidate{}, skip, nil
	}

	startAt, err := parseCafeNo9DateTime(dateText, startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Cafe No. 9 start time for %q: %w", title, err)
	}

	return EventCandidate{
		UID:         resolvedURL,
		Summary:     title,
		Description: strings.Join(description, "\n"),
		Location:    venue,
		URL:         resolvedURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
	}, ParseSkip{}, nil
}

func cafe9ListingLines(raw string) []string {
	text := cafe9LineBreakPattern.ReplaceAllString(raw, "\n")
	text = cafe9TagPattern.ReplaceAllString(text, "\n")
	text = html.UnescapeString(text)

	parts := strings.Split(text, "\n")
	lines := make([]string, 0, len(parts))
	for _, part := range parts {
		line := cafe9CleanText(part)
		if line == "" || line == "* * *" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func cafe9CleanText(value string) string {
	value = html.UnescapeString(value)
	value = cafe9WhitespacePattern.ReplaceAllString(strings.TrimSpace(value), " ")
	return strings.TrimSpace(value)
}

func cafe9IsVenueLine(line string) bool {
	return strings.Contains(strings.ToUpper(line), "SHEFFIELD:")
}

func cafe9VenueFromLine(line string) string {
	_, venue, found := strings.Cut(line, ":")
	if !found {
		return ""
	}
	return cafe9CleanText(venue)
}

func cafe9IsDateLine(line string) bool {
	line = cafe9TrimPrefixRune(line)
	for _, day := range []string{"Monday ", "Tuesday ", "Wednesday ", "Thursday ", "Friday ", "Saturday ", "Sunday "} {
		if strings.HasPrefix(line, day) {
			return true
		}
	}
	return false
}

func cafe9StartTimeFromLine(line string) string {
	lower := strings.ToLower(line)
	idx := strings.Index(lower, "start time:")
	if idx < 0 {
		return ""
	}
	value := strings.TrimSpace(line[idx+len("start time:"):])
	if comma := strings.IndexByte(value, ','); comma >= 0 {
		value = value[:comma]
	}
	return cafe9CleanText(value)
}

func cafe9IsCategoryLine(line string) bool {
	line = strings.ToLower(cafe9TrimPrefixRune(line))
	return strings.HasPrefix(line, "music") || line == "literature" || line == "other" || line == "comedy"
}

func cafe9CategoryFromLine(line string) string {
	return cafe9TrimPrefixRune(line)
}

func cafe9IsMusicCategory(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "music" || strings.HasPrefix(value, "music - ")
}

func cafe9IsIgnorableDescriptionLine(line string) bool {
	line = strings.ToLower(strings.TrimSpace(cafe9TrimPrefixRune(line)))
	return strings.HasPrefix(line, "all ages") ||
		strings.HasPrefix(line, "age restrictions") ||
		strings.HasPrefix(line, "under ")
}

func cafe9TrimPrefixRune(line string) string {
	line = strings.TrimSpace(line)
	if len(line) >= 2 && line[1] == ' ' {
		switch line[0] {
		case '0', 'P', 'N', 'C', '.':
			return strings.TrimSpace(line[2:])
		}
	}
	return line
}

func cafe9AllowsMissingCategory(pageURL *url.URL) bool {
	path := strings.TrimSpace(pageURL.Path)
	if strings.Contains(path, "/page/") {
		return true
	}
	path = strings.TrimSuffix(path, "/")
	return strings.EqualFold(path, "/Cafe9")
}

func cafe9AllowsMissingCategoryTitle(title string) bool {
	title = strings.ToLower(strings.TrimSpace(title))
	return strings.HasPrefix(title, "an evening with ") &&
		(strings.Contains(title, " at cafe no9") || strings.Contains(title, " at cafe no. 9"))
}

func parseCafeNo9DateTime(dateText, timeText string) (time.Time, error) {
	dateText = cafe9OrdinalPattern.ReplaceAllString(strings.TrimSpace(dateText), "$1")
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}

	for _, layout := range []string{
		"Monday 2 January, 2006 3:04pm",
		"Monday 2 January, 2006 3pm",
	} {
		parsed, err := time.ParseInLocation(layout, dateText+" "+strings.ToLower(strings.TrimSpace(timeText)), loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime %q %q", dateText, timeText)
}

func resolvePageURL(baseURL *url.URL, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	return baseURL.ResolveReference(parsed).String(), nil
}

func normalizeCafeNo9SourcePageURLString(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	return normalizeCafeNo9SourcePageURL(parsed)
}

func normalizeCafeNo9SourcePageURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	normalized := *u
	normalized.RawQuery = ""
	normalized.Fragment = ""

	path := strings.TrimSpace(normalized.Path)
	switch {
	case strings.EqualFold(strings.TrimSuffix(path, "/"), "/Cafe9"):
		normalized.Path = "/Cafe9"
	case cafe9IsFirstPaginationPath(path):
		normalized.Path = "/Cafe9"
	default:
		normalized.Path = strings.TrimSuffix(path, "/")
	}
	return normalized.String()
}

func cafe9IsFirstPaginationPath(path string) bool {
	trimmed := strings.TrimSuffix(strings.TrimSpace(path), "/")
	return strings.EqualFold(trimmed, "/Cafe9/page/1")
}
