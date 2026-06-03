package ingest

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const hallamshireHotelAllDayFallbackBasis = "Hallamshire Hotel all-day fallback 19:30 Europe/London"

var hallamshireHotelURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>\\]+`)

func hallamshire_hotel_cfg_filestring(baseURL string, body []byte, limit int) ([]string, error) {
	return ExtractHiddenCalendarLinks(baseURL, body, limit)
}

func ParseHallamshireHotelICS(raw []byte) ParseResult {
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
				candidate, skip := parseHallamshireHotelEvent(event)
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

func parseHallamshireHotelEvent(properties []icsProperty) (EventCandidate, ParseSkip) {
	uid := cleanICSValue(firstValue(properties, "UID"))
	occurrenceUID := icsOccurrenceUID(uid, properties)
	summary := cleanICSValue(firstValue(properties, "SUMMARY"))
	rawLocation := strings.TrimSpace(firstValue(properties, "LOCATION"))
	location := cleanICSValue(rawLocation)
	status := cleanICSValue(firstValue(properties, "STATUS"))
	description := cleanICSValue(firstValue(properties, "DESCRIPTION"))
	detailURL := hallamshireHotelFirstTrustedDetailURL(description, cleanICSValue(firstValue(properties, "URL")))

	skip := ParseSkip{UID: occurrenceUID, Summary: summary}
	if summary == "" {
		skip.Reason = "missing summary"
		return EventCandidate{}, skip
	}
	if strings.EqualFold(status, "CANCELLED") {
		skip.Reason = "cancelled"
		return EventCandidate{}, skip
	}

	startProp, ok := firstProperty(properties, "DTSTART")
	if !ok {
		skip.Reason = "missing DTSTART"
		return EventCandidate{}, skip
	}
	startDate, allDay, reason, err := hallamshireHotelAllDayStartDate(properties, startProp)
	if err != nil {
		skip.Reason = reason
		return EventCandidate{}, skip
	}
	if !allDay {
		candidate, parseSkip := parseEvent(properties)
		if parseSkip.Reason != "" {
			return EventCandidate{}, parseSkip
		}
		if detailURL != "" {
			candidate.URL = detailURL
			candidate.SourceURLSourceIdentityDisabled = true
		} else {
			candidate.URL = ""
			candidate.SourceURLSourceIdentityDisabled = false
		}
		return candidate, ParseSkip{}
	}
	if reason != "" {
		skip.Reason = reason
		return EventCandidate{}, skip
	}
	if !hallamshireHotelAllDaySummaryAllowed(summary) {
		skip.Reason = "filtered non-music all-day event"
		return EventCandidate{}, skip
	}

	startAt, err := hallamshireHotelFallbackStartAt(startDate)
	if err != nil {
		skip.Reason = fmt.Sprintf("fallback start time unavailable: %v", err)
		return EventCandidate{}, skip
	}

	return EventCandidate{
		UID:                             occurrenceUID,
		Summary:                         summary,
		Description:                     description,
		Location:                        location,
		LocationRaw:                     rawLocation,
		URL:                             detailURL,
		ImageSourceURL:                  firstICSImageURL(properties),
		ImageAlt:                        summary,
		Status:                          status,
		StartAt:                         formatTime(startAt),
		StartAtInferred:                 true,
		StartAtBasis:                    hallamshireHotelAllDayFallbackBasis,
		SourceURLSourceIdentityDisabled: detailURL != "",
	}, ParseSkip{}
}

func hallamshireHotelAllDayStartDate(properties []icsProperty, startProp icsProperty) (time.Time, bool, string, error) {
	startDate, allDay, err := hallamshireHotelICSDate(startProp)
	if err != nil {
		return time.Time{}, true, fmt.Sprintf("malformed DTSTART: %v", err), err
	}
	if !allDay {
		return time.Time{}, false, "", nil
	}
	if endProp, ok := firstProperty(properties, "DTEND"); ok {
		endDate, endAllDay, err := hallamshireHotelICSDate(endProp)
		if err != nil {
			return time.Time{}, true, fmt.Sprintf("malformed DTEND: %v", err), err
		}
		if !endAllDay {
			return time.Time{}, true, "mixed all-day DTSTART with timed DTEND", nil
		}
		if !endDate.Equal(startDate.AddDate(0, 0, 1)) {
			return time.Time{}, true, "multi-day all-day event", nil
		}
	}
	return startDate, true, "", nil
}

func hallamshireHotelICSDate(property icsProperty) (time.Time, bool, error) {
	valueType := strings.ToUpper(strings.TrimSpace(property.params["VALUE"]))
	value := strings.TrimSpace(property.value)
	if valueType != "DATE" && (len(value) != len("20060102") || strings.Contains(value, "T")) {
		return time.Time{}, false, nil
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, true, err
	}
	parsed, err := time.ParseInLocation("20060102", value, loc)
	if err != nil {
		return time.Time{}, true, err
	}
	return parsed, true, nil
}

func hallamshireHotelFallbackStartAt(date time.Time) (time.Time, error) {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	localDate := date.In(loc)
	return time.Date(localDate.Year(), localDate.Month(), localDate.Day(), 19, 30, 0, 0, loc).UTC(), nil
}

func hallamshireHotelAllDaySummaryAllowed(summary string) bool {
	normalized := strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	switch normalized {
	case "PRIVATE PARTY", "TRAMLINES FRINGE", "FRINGE AT TRAMLINES":
		return false
	}
	for _, prefix := range []string{
		"QUIZ:",
		"FREE ENTRY QUIZ:",
		"FREE ENTY QUIZ:",
		"CLUB NIGHT:",
		"CLUB EVENT:",
		"FREE ENTRY CLUBNIGHT:",
		"PRE/POST:",
	} {
		if strings.HasPrefix(normalized, prefix) {
			return false
		}
	}
	for _, prefix := range []string{"GIG:", "LIVE:", "FREE ENTRY GIG:", "FREE GIG:"} {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
}

func hallamshireHotelFirstTrustedDetailURL(values ...string) string {
	for _, value := range values {
		for _, candidate := range hallamshireHotelURLCandidates(value) {
			if trusted, ok := hallamshireHotelTrustedDetailURL(candidate); ok {
				return trusted
			}
		}
	}
	return ""
}

func hallamshireHotelURLCandidates(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	candidates := make([]string, 0, 2)
	for _, match := range htmlAnchorTagPattern.FindAllString(value, -1) {
		attrs := parseHTMLAttributes(match)
		if href := strings.TrimSpace(attrs["href"]); href != "" {
			candidates = append(candidates, href)
		}
	}
	for _, match := range hallamshireHotelURLPattern.FindAllString(value, -1) {
		candidates = append(candidates, strings.TrimRight(match, ".,);]"))
	}
	if strings.HasPrefix(strings.ToLower(value), "http://") || strings.HasPrefix(strings.ToLower(value), "https://") {
		candidates = append(candidates, strings.TrimRight(value, ".,);]"))
	}
	return candidates
}

func hallamshireHotelTrustedDetailURL(raw string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
	default:
		return "", false
	}
	host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	if host != "fatsoma.com" && host != "leadmill.co.uk" && host != "wegottickets.com" {
		return "", false
	}
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String(), true
}
