package ingest

import (
	"fmt"
	"strings"
	"time"
)

type EventCandidate struct {
	UID         string `json:"uid,omitempty"`
	Summary     string `json:"summary"`
	Description string `json:"description,omitempty"`
	Location    string `json:"location,omitempty"`
	URL         string `json:"url,omitempty"`
	Status      string `json:"status,omitempty"`
	StartAt     string `json:"start_at"`
	EndAt       string `json:"end_at,omitempty"`
}

type ParseSkip struct {
	UID     string `json:"uid,omitempty"`
	Summary string `json:"summary,omitempty"`
	Reason  string `json:"reason"`
}

type ParseResult struct {
	Candidates []EventCandidate `json:"candidates"`
	Skips      []ParseSkip      `json:"skips"`
	Errors     []string         `json:"errors"`
}

type icsProperty struct {
	name   string
	params map[string]string
	value  string
}

func ParseICS(raw []byte) ParseResult {
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
				candidate, skip := parseEvent(event)
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

func unfoldICSLines(raw string) []string {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	physical := strings.Split(normalized, "\n")
	lines := make([]string, 0, len(physical))
	for _, line := range physical {
		if line == "" {
			continue
		}
		if (line[0] == ' ' || line[0] == '\t') && len(lines) > 0 {
			lines[len(lines)-1] += line[1:]
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func parseICSProperty(line string) (icsProperty, bool) {
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		return icsProperty{}, false
	}

	left := line[:colon]
	value := line[colon+1:]
	parts := strings.Split(left, ";")
	name := strings.ToUpper(strings.TrimSpace(parts[0]))
	if name == "" {
		return icsProperty{}, false
	}

	params := make(map[string]string)
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		params[strings.ToUpper(strings.TrimSpace(key))] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	return icsProperty{name: name, params: params, value: value}, true
}

func parseEvent(properties []icsProperty) (EventCandidate, ParseSkip) {
	uid := cleanICSValue(firstValue(properties, "UID"))
	summary := cleanICSValue(firstValue(properties, "SUMMARY"))
	skip := ParseSkip{UID: uid, Summary: summary}

	if summary == "" {
		skip.Reason = "missing summary"
		return EventCandidate{}, skip
	}

	status := cleanICSValue(firstValue(properties, "STATUS"))
	if strings.EqualFold(status, "CANCELLED") {
		skip.Reason = "cancelled"
		return EventCandidate{}, skip
	}

	startProp, ok := firstProperty(properties, "DTSTART")
	if !ok {
		skip.Reason = "missing DTSTART"
		return EventCandidate{}, skip
	}
	startAt, reason, err := parseICSTime(startProp)
	if reason != "" {
		skip.Reason = reason
		return EventCandidate{}, skip
	}
	if err != nil {
		skip.Reason = fmt.Sprintf("malformed DTSTART: %v", err)
		return EventCandidate{}, skip
	}

	var endText string
	if endProp, ok := firstProperty(properties, "DTEND"); ok {
		endAt, reason, err := parseICSTime(endProp)
		if reason != "" {
			skip.Reason = reason
			return EventCandidate{}, skip
		}
		if err != nil {
			skip.Reason = fmt.Sprintf("malformed DTEND: %v", err)
			return EventCandidate{}, skip
		}
		endText = formatTime(endAt)
	}

	return EventCandidate{
		UID:         uid,
		Summary:     summary,
		Description: cleanICSValue(firstValue(properties, "DESCRIPTION")),
		Location:    cleanICSValue(firstValue(properties, "LOCATION")),
		URL:         cleanICSValue(firstValue(properties, "URL")),
		Status:      status,
		StartAt:     formatTime(startAt),
		EndAt:       endText,
	}, ParseSkip{}
}

func firstProperty(properties []icsProperty, name string) (icsProperty, bool) {
	for _, property := range properties {
		if property.name == name {
			return property, true
		}
	}
	return icsProperty{}, false
}

func firstValue(properties []icsProperty, name string) string {
	if property, ok := firstProperty(properties, name); ok {
		return property.value
	}
	return ""
}

func parseICSTime(property icsProperty) (time.Time, string, error) {
	valueType := strings.ToUpper(property.params["VALUE"])
	value := strings.TrimSpace(property.value)
	if valueType == "DATE" || (len(value) == len("20060102") && !strings.Contains(value, "T")) {
		return time.Time{}, "all-day event", nil
	}

	if tzid := property.params["TZID"]; tzid != "" && tzid != "Europe/London" {
		return time.Time{}, "unsupported timezone " + tzid, nil
	}

	if strings.HasSuffix(value, "Z") {
		parsed, err := parseWithLayouts(value, time.UTC, "20060102T150405Z", "20060102T1504Z")
		if err != nil {
			return time.Time{}, "", err
		}
		return parsed.UTC(), "", nil
	}

	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, "", err
	}
	parsed, err := parseWithLayouts(value, loc, "20060102T150405", "20060102T1504")
	if err != nil {
		return time.Time{}, "", err
	}
	return parsed.UTC(), "", nil
}

func parseWithLayouts(value string, loc *time.Location, layouts ...string) (time.Time, error) {
	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func cleanICSValue(value string) string {
	value = strings.TrimSpace(value)
	replacer := strings.NewReplacer(
		`\n`, "\n",
		`\N`, "\n",
		`\,`, ",",
		`\;`, ";",
		`\\`, `\`,
	)
	return replacer.Replace(value)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
