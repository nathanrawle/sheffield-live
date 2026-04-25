package ingest

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var greystonesAnchorPattern = regexp.MustCompile(`(?is)<a\b[^>]*>`)
var greystonesMonthPathPattern = regexp.MustCompile(`^/(january|february|march|april|may|june|july|august|september|october|november|december)/?$`)
var greystonesTitlePattern = regexp.MustCompile(`(?is)<h1\b[^>]*>(.*?)</h1>`)
var greystonesMetaPattern = regexp.MustCompile(`(?is)<h4\b[^>]*>(.*?)</h4>`)
var greystonesParagraphPattern = regexp.MustCompile(`(?is)<p\b[^>]*>(.*?)</p>`)
var greystonesPageYearPattern = regexp.MustCompile(`(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b`)
var greystonesTagPattern = regexp.MustCompile(`(?is)<[^>]+>`)
var greystonesSpacePattern = regexp.MustCompile(`\s+`)
var greystonesOrdinalPattern = regexp.MustCompile(`\b(\d{1,2})(st|nd|rd|th)\b`)

func ExtractTheGreystonesMonthLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}

	matches := greystonesAnchorPattern.FindAll(body, -1)
	seen := make(map[string]bool)
	links := make([]string, 0, min(limit, len(matches)))
	for _, match := range matches {
		attrs := parseHTMLAttributes(string(match))
		resolved, ok := greystonesMonthURL(parsedBase, attrs["href"])
		if !ok || seen[resolved] {
			continue
		}
		seen[resolved] = true
		links = append(links, resolved)
		if len(links) >= limit {
			break
		}
	}
	return links, nil
}

func ParseTheGreystonesMonthPage(pageURL string, raw []byte) ParseResult {
	year, err := greystonesPageYear(raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}

	titleMatches := greystonesTitlePattern.FindAllSubmatchIndex(raw, -1)
	if len(titleMatches) == 0 {
		return ParseResult{Errors: []string{"no The Greystones event rows found"}}
	}

	var result ParseResult
	for i, match := range titleMatches {
		sectionEnd := len(raw)
		if i+1 < len(titleMatches) {
			sectionEnd = titleMatches[i+1][0]
		}
		section := raw[match[0]:sectionEnd]
		candidate, skip, err := greystonesCandidateFromSection(pageURL, section, year)
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

func greystonesMonthURL(base *url.URL, raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	if normalizeCorporationHost(resolved.Host) != normalizeCorporationHost(base.Host) {
		return "", false
	}
	if !greystonesMonthPathPattern.MatchString(strings.ToLower(resolved.EscapedPath())) {
		return "", false
	}
	resolved.RawQuery = ""
	resolved.Fragment = ""
	return resolved.String(), true
}

func greystonesPageYear(raw []byte) (int, error) {
	match := greystonesPageYearPattern.FindSubmatch(raw)
	if len(match) < 3 {
		return 0, fmt.Errorf("missing The Greystones page year")
	}
	parsed, err := time.Parse("January 2006", string(match[1])+" "+string(match[2]))
	if err != nil {
		return 0, fmt.Errorf("parse The Greystones page year: %w", err)
	}
	return parsed.Year(), nil
}

func greystonesCandidateFromSection(pageURL string, section []byte, year int) (EventCandidate, ParseSkip, error) {
	title := greystonesMatchText(greystonesTitlePattern.FindSubmatch(section))
	meta := greystonesMatchText(greystonesMetaPattern.FindSubmatch(section))
	skip := ParseSkip{Summary: title}

	switch {
	case title == "":
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	case meta == "":
		skip.Reason = "missing event metadata"
		return EventCandidate{}, skip, nil
	}

	startAt, err := greystonesStartAt(meta, year)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse The Greystones metadata for %q: %w", title, err)
	}

	return EventCandidate{
		Summary:     title,
		Description: greystonesDescription(section),
		Location:    "The Greystones",
		URL:         greystonesSectionURL(pageURL, section),
		Status:      "Listed",
		StartAt:     formatTime(startAt),
	}, ParseSkip{}, nil
}

func greystonesStartAt(meta string, year int) (time.Time, error) {
	parts := strings.Split(meta, "/")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("expected date / time / price")
	}
	datePart := strings.TrimSpace(parts[0])
	timePart := strings.TrimSpace(parts[1])
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	for _, layout := range []string{
		"Monday 2 January 2006 3:04pm",
		"Monday 2 January 2006 3pm",
	} {
		parsed, err := time.ParseInLocation(layout, greystonesDatePart(datePart, year)+" "+strings.ToLower(timePart), loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime %q / %q", datePart, timePart)
}

func greystonesDatePart(value string, year int) string {
	value = strings.TrimSpace(value)
	value = greystonesOrdinalPattern.ReplaceAllString(value, "$1")
	return value + fmt.Sprintf(" %d", year)
}

func greystonesDescription(section []byte) string {
	matches := greystonesParagraphPattern.FindAllSubmatch(section, -1)
	parts := make([]string, 0, len(matches))
	for _, match := range matches {
		text := greystonesMatchText(match)
		if text == "" {
			continue
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, "\n")
}

func greystonesSectionURL(pageURL string, section []byte) string {
	base, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return strings.TrimSpace(pageURL)
	}
	matches := greystonesAnchorPattern.FindAll(section, -1)
	for _, match := range matches {
		attrs := parseHTMLAttributes(string(match))
		href := strings.TrimSpace(attrs["href"])
		if href == "" {
			continue
		}
		resolved, err := resolveURL(base, html.UnescapeString(href))
		if err == nil && strings.TrimSpace(resolved) != "" {
			return resolved
		}
	}
	return strings.TrimSpace(pageURL)
}

func greystonesMatchText(match [][]byte) string {
	if len(match) < 2 {
		return ""
	}
	return greystonesText(match[1])
}

func greystonesText(match []byte) string {
	text := html.UnescapeString(string(match))
	text = greystonesTagPattern.ReplaceAllString(text, " ")
	return greystonesSpacePattern.ReplaceAllString(strings.TrimSpace(text), " ")
}
