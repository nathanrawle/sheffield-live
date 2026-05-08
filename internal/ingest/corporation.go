package ingest

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var corporationAnchorPattern = regexp.MustCompile(`(?is)<a\b[^>]*>`)
var corporationEventPathPattern = regexp.MustCompile(`^/event/[^?#]+/?$`)
var corporationTitlePattern = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*class\s*=\s*["'][^"']*\btribe-events-single-event-title\b[^"']*["'][^>]*>(.*?)</h[1-6]>`)
var corporationPageTitlePattern = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
var corporationDescriptionPattern = regexp.MustCompile(`(?is)<div\b[^>]*class\s*=\s*["'][^"']*\btribe-events-single-event-description\b[^"']*["'][^>]*>(.*?)</div>`)
var corporationStartDateTimePattern = regexp.MustCompile(`(?is)<[^>]*class\s*=\s*["'][^"']*\btribe-events-start-date\b[^"']*["'][^>]*datetime\s*=\s*["']([^"']+)["'][^>]*>`)
var corporationEndDateTimePattern = regexp.MustCompile(`(?is)<[^>]*class\s*=\s*["'][^"']*\btribe-events-end-date\b[^"']*["'][^>]*datetime\s*=\s*["']([^"']+)["'][^>]*>`)
var corporationTagPattern = regexp.MustCompile(`(?is)<[^>]+>`)

func ExtractCorporationDetailLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}

	matches := corporationAnchorPattern.FindAll(body, -1)
	seen := make(map[string]bool)
	links := make([]string, 0, min(limit, len(matches)))
	for _, match := range matches {
		attrs := parseHTMLAttributes(string(match))
		resolved, ok := corporationDetailURL(parsedBase, attrs["href"])
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

func ParseCorporationDetailPage(pageURL string, raw []byte) ParseResult {
	candidate, skip, err := corporationCandidateFromPage(pageURL, raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}
	if skip.Reason != "" {
		return ParseResult{Skips: []ParseSkip{skip}}
	}
	return ParseResult{Candidates: []EventCandidate{candidate}}
}

func corporationCandidateFromPage(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	if candidate, skip, found, err := corporationCandidateFromStructuredData(pageURL, raw); found || err != nil {
		return candidate, skip, err
	}
	return corporationCandidateFromLegacyPage(pageURL, raw)
}

func corporationCandidateFromStructuredData(pageURL string, raw []byte) (EventCandidate, ParseSkip, bool, error) {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		nodes, found, err := parseYellowArchJSONLDScript(match[1])
		if err != nil || !found {
			continue
		}
		for _, node := range nodes {
			candidate, skip, err := corporationCandidateFromJSONLDNode(pageURL, node)
			return candidate, skip, true, err
		}
	}
	return EventCandidate{}, ParseSkip{}, false, nil
}

func corporationCandidateFromJSONLDNode(pageURL string, node map[string]any) (EventCandidate, ParseSkip, error) {
	detailURL := strings.TrimSpace(pageURL)
	skip := ParseSkip{UID: detailURL}

	title := yellowArchJSONString(node["name"])
	skip.Summary = title
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}

	startText := yellowArchJSONString(node["startDate"])
	if startText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	}
	startAt, err := parseCorporationDateTime(startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Corporation start time for %q: %w", title, err)
	}

	endText := yellowArchJSONString(node["endDate"])
	if endText == "" {
		skip.Reason = "missing event end time"
		return EventCandidate{}, skip, nil
	}
	endAt, err := parseCorporationDateTime(endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Corporation end time for %q: %w", title, err)
	}

	location := yellowArchLocationName(node["location"])
	if location == "" {
		location = "Corporation"
	}

	return EventCandidate{
		UID:         detailURL,
		Summary:     title,
		Description: semanticDescriptionText(yellowArchJSONString(node["description"])),
		Location:    location,
		LocationRaw: corporationLocationRaw(node["location"]),
		URL:         detailURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
		EndAt:       formatTime(endAt),
	}, ParseSkip{}, nil
}

func corporationCandidateFromLegacyPage(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	detailURL := strings.TrimSpace(pageURL)
	skip := ParseSkip{UID: detailURL}

	title := corporationText(corporationTitlePattern.FindStringSubmatch(string(raw)))
	if title == "" {
		title = corporationPageTitle(corporationText(corporationPageTitlePattern.FindStringSubmatch(string(raw))))
	}
	skip.Summary = title
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}

	startAt, err := parseCorporationDateTime(corporationAttributeValue(corporationStartDateTimePattern.FindStringSubmatch(string(raw))))
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Corporation start time for %q: %w", title, err)
	}
	endAt, err := parseCorporationDateTime(corporationAttributeValue(corporationEndDateTimePattern.FindStringSubmatch(string(raw))))
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Corporation end time for %q: %w", title, err)
	}

	return EventCandidate{
		UID:         detailURL,
		Summary:     title,
		Description: corporationText(corporationDescriptionPattern.FindStringSubmatch(string(raw))),
		Location:    "Corporation",
		URL:         detailURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
		EndAt:       formatTime(endAt),
	}, ParseSkip{}, nil
}

func corporationLocationRaw(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case map[string]any:
		name := yellowArchJSONString(typed["name"])
		address := corporationAddressText(typed["address"])
		switch {
		case name == "":
			return address
		case address == "":
			return name
		default:
			return name + ", " + address
		}
	default:
		return ""
	}
}

func corporationAddressText(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case map[string]any:
		parts := make([]string, 0, 5)
		for _, key := range []string{"streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"} {
			if part := yellowArchJSONString(typed[key]); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, ", ")
	default:
		return ""
	}
}

func corporationDetailURL(base *url.URL, raw string) (string, bool) {
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
	if !corporationEventPathPattern.MatchString(resolved.EscapedPath()) {
		return "", false
	}
	resolved.RawQuery = ""
	resolved.Fragment = ""
	return resolved.String(), true
}

func normalizeCorporationHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func corporationText(match []string) string {
	if len(match) < 2 {
		return ""
	}
	text := html.UnescapeString(match[1])
	text = corporationTagPattern.ReplaceAllString(text, " ")
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func corporationPageTitle(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	head, _, _ := strings.Cut(value, "|")
	return strings.TrimSpace(head)
}

func corporationAttributeValue(match []string) string {
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(html.UnescapeString(match[1]))
}

func parseCorporationDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("missing datetime")
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	} {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime %q", value)
}
