package ingest

import (
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var yellowArchJSONLDPattern = regexp.MustCompile(`(?is)<script\b[^>]*type\s*=\s*["']application/ld\+json["'][^>]*>(.*?)</script>`)

func ParseYellowArchPage(raw []byte) ParseResult {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		return ParseResult{Errors: []string{"no application/ld+json event data found"}}
	}

	var result ParseResult
	foundEventData := false
	for _, match := range matches {
		nodes, found, err := parseYellowArchJSONLDScript(match[1])
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			continue
		}
		if !found {
			continue
		}
		foundEventData = true
		for _, node := range nodes {
			candidate, skip, err := yellowArchCandidateFromNode(node)
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
	}

	if !foundEventData {
		result.Errors = append(result.Errors, "no schema.org Event objects found in application/ld+json")
	}
	return result
}

func ParseYellowArchSourcePage(pageURL string, raw []byte, limit int) ParseResult {
	result := limitParseResult(ParseYellowArchPage(raw), limit)

	baseURL, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("parse Yellow Arch source page URL: %v", err))
		return result
	}
	for i := range result.Candidates {
		resolvedURL, err := resolveYellowArchURL(baseURL, result.Candidates[i].URL)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("resolve Yellow Arch event URL %q: %v", result.Candidates[i].URL, err))
			continue
		}
		result.Candidates[i].URL = resolvedURL
	}
	return result
}

func parseYellowArchJSONLDScript(raw []byte) ([]map[string]any, bool, error) {
	text := strings.TrimSpace(html.UnescapeString(string(raw)))
	if text == "" {
		return nil, false, nil
	}

	var payload any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return nil, false, fmt.Errorf("decode Yellow Arch JSON-LD: %w", err)
	}

	var events []map[string]any
	collectYellowArchEventNodes(payload, &events)
	return events, len(events) > 0, nil
}

func collectYellowArchEventNodes(value any, events *[]map[string]any) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			collectYellowArchEventNodes(item, events)
		}
	case map[string]any:
		if graph, ok := typed["@graph"]; ok {
			collectYellowArchEventNodes(graph, events)
		}
		if yellowArchNodeHasType(typed["@type"], "Event") {
			*events = append(*events, typed)
		}
	}
}

func yellowArchNodeHasType(value any, want string) bool {
	switch typed := value.(type) {
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), want)
	case []any:
		for _, item := range typed {
			if yellowArchNodeHasType(item, want) {
				return true
			}
		}
	}
	return false
}

func yellowArchCandidateFromNode(node map[string]any) (EventCandidate, ParseSkip, error) {
	name := yellowArchJSONString(node["name"])
	rawURL := yellowArchJSONString(node["url"])
	description := yellowArchJSONString(node["description"])
	startText := yellowArchJSONString(node["startDate"])
	endText := yellowArchJSONString(node["endDate"])
	location := yellowArchLocationName(node["location"])

	skip := ParseSkip{Summary: name}
	switch {
	case name == "":
		skip.Reason = "missing event name"
		return EventCandidate{}, skip, nil
	case startText == "":
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	case endText == "":
		skip.Reason = "missing event end time"
		return EventCandidate{}, skip, nil
	case location == "":
		skip.Reason = "missing event location"
		return EventCandidate{}, skip, nil
	}

	startAt, err := parseYellowArchDateTime(startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Yellow Arch start time for %q: %w", name, err)
	}
	endAt, err := parseYellowArchDateTime(endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Yellow Arch end time for %q: %w", name, err)
	}

	return EventCandidate{
		Summary:     name,
		Description: description,
		Location:    location,
		URL:         rawURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
		EndAt:       formatTime(endAt),
	}, ParseSkip{}, nil
}

func yellowArchJSONString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(html.UnescapeString(text))
}

func yellowArchLocationName(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case map[string]any:
		if name := yellowArchJSONString(typed["name"]); name != "" {
			return name
		}
	}
	return ""
}

func parseYellowArchDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
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
	} {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime %q", value)
}

func resolveYellowArchURL(baseURL *url.URL, raw string) (string, error) {
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
