package ingest

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

var leadmillCalendarLinkPattern = regexp.MustCompile(`(?is)<link\b[^>]*>`)
var leadmillAttrPattern = regexp.MustCompile(`(?is)([a-zA-Z_:][a-zA-Z0-9_:\-]*)\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)`)
var leadmillSheffieldPostcodePattern = regexp.MustCompile(`(?i)\bS[0-9]{1,2}\s*[0-9][A-Z]{2}\b`)

func ExtractLeadmillICSLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	matches := leadmillCalendarLinkPattern.FindAll(body, -1)
	seen := make(map[string]bool)
	links := make([]string, 0, min(limit, len(matches)))
	for _, match := range matches {
		attrs := parseHTMLAttributes(string(match))
		if !leadmillCalendarLinkMatches(attrs) {
			continue
		}

		resolved, err := resolveURL(parsedBase, attrs["href"])
		if err != nil || seen[resolved] {
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

func ParseLeadmillICS(raw []byte) ParseResult {
	lines := unfoldICSLines(string(raw))
	var result ParseResult
	var event []icsProperty
	inEvent := false

	for _, line := range lines {
		prop, ok := parseICSProperty(line)
		if !ok {
			continue
		}
		switch prop.name {
		case "BEGIN":
			if strings.EqualFold(prop.value, "VEVENT") {
				if inEvent {
					result.Errors = append(result.Errors, "nested VEVENT")
				}
				inEvent = true
				event = event[:0]
			}
		case "END":
			if strings.EqualFold(prop.value, "VEVENT") {
				if !inEvent {
					result.Errors = append(result.Errors, "END:VEVENT without BEGIN:VEVENT")
					continue
				}
				candidate, skip := parseLeadmillEvent(event)
				if skip.Reason != "" {
					result.Skips = append(result.Skips, skip)
				} else {
					result.Candidates = append(result.Candidates, candidate)
				}
				inEvent = false
				event = nil
			}
		default:
			if inEvent {
				event = append(event, prop)
			}
		}
	}

	if inEvent {
		result.Errors = append(result.Errors, "unterminated VEVENT")
	}
	return result
}

func parseLeadmillEvent(properties []icsProperty) (EventCandidate, ParseSkip) {
	skip := ParseSkip{
		UID:     cleanICSValue(firstValue(properties, "UID")),
		Summary: cleanICSValue(firstValue(properties, "SUMMARY")),
	}
	if !leadmillHasLiveCategory(properties) {
		skip.Reason = "filtered non-Live category"
		return EventCandidate{}, skip
	}

	candidate, parseSkip := parseEvent(properties)
	if parseSkip.Reason != "" {
		return EventCandidate{}, parseSkip
	}

	venueText := leadmillVenueText(candidate.Location)
	if !leadmillIsSheffieldLocation(candidate.Location, venueText) {
		skip.Reason = "filtered non-Sheffield location"
		return EventCandidate{}, skip
	}
	if venueText != "" {
		candidate.Location = venueText
	}
	return candidate, ParseSkip{}
}

func parseHTMLAttributes(tag string) map[string]string {
	matches := leadmillAttrPattern.FindAllStringSubmatch(tag, -1)
	attrs := make(map[string]string, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(match[1]))
		value := strings.TrimSpace(match[2])
		value = strings.Trim(value, `"'`)
		attrs[key] = html.UnescapeString(value)
	}
	return attrs
}

func leadmillCalendarLinkMatches(attrs map[string]string) bool {
	if !strings.EqualFold(strings.TrimSpace(attrs["type"]), "text/calendar") {
		return false
	}
	for _, rel := range strings.Fields(strings.ToLower(attrs["rel"])) {
		if rel == "alternate" {
			return strings.TrimSpace(attrs["href"]) != ""
		}
	}
	return false
}

func leadmillHasLiveCategory(properties []icsProperty) bool {
	for _, property := range properties {
		if property.name != "CATEGORIES" {
			continue
		}
		for _, category := range strings.Split(property.value, ",") {
			if strings.EqualFold(strings.TrimSpace(category), "Live") {
				return true
			}
		}
	}
	return false
}

func leadmillIsSheffieldLocation(location, venueText string) bool {
	location = strings.TrimSpace(location)
	if strings.Contains(strings.ToLower(location), "sheffield") {
		return true
	}
	if leadmillSheffieldPostcodePattern.MatchString(location) {
		return true
	}
	switch VenueSlugFromText(venueText) {
	case "leadmill", "yellow-arch", "hallamshire-hotel", "foundry", "memorial-hall", "sheffield-plate", "steamworks":
		return true
	default:
		return false
	}
}

func leadmillVenueText(location string) string {
	location = strings.TrimSpace(location)
	head, _, _ := strings.Cut(location, ",")
	return strings.Join(strings.Fields(strings.TrimSpace(head)), " ")
}
