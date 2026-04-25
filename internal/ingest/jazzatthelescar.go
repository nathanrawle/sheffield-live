package ingest

import (
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"
)

var (
	jazzAtTheLescarBlockPattern       = regexp.MustCompile(`(?is)<div\s+class=["']art["']>(.*?)</div>\s*<div\s+class=["']ttl["']>(.*?)</div>.*?<div\s+class=["']dsc["']>(.*?)</div>`)
	jazzAtTheLescarTagPattern         = regexp.MustCompile(`(?is)<[^>]+>`)
	jazzAtTheLescarWhitespacePattern  = regexp.MustCompile(`\s+`)
	jazzAtTheLescarDefaultTimePattern = regexp.MustCompile(`(?i)music\s+(\d{1,2}(?::\d{2})?\s*[ap]m)`)
	jazzAtTheLescarInlineTimePattern  = regexp.MustCompile(`(?i)\b(?:music|start)\s*:?\s*(\d{1,2}(?::\d{2})?\s*[ap]m)\b`)
	jazzAtTheLescarUpdatedPattern     = regexp.MustCompile(`(?i)Page\s+last\s+updated:\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})`)
	jazzAtTheLescarMonthPattern       = regexp.MustCompile(`(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December)\b`)
	jazzAtTheLescarDayPattern         = regexp.MustCompile(`(?i)\b(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+)?(\d{1,2})(?:st|nd|rd|th)?\b`)
)

func ParseJazzAtTheLescarSourcePage(_ string, raw []byte, limit int) ParseResult {
	return limitParseResult(ParseJazzAtTheLescarPage(raw), limit)
}

func ParseJazzAtTheLescarPage(raw []byte) ParseResult {
	defaultTime, updatedDate, err := parseJazzAtTheLescarPageMetadata(raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}

	matches := jazzAtTheLescarBlockPattern.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		return ParseResult{Errors: []string{"no Jazz at The Lescar listing blocks found"}}
	}

	var result ParseResult
	for _, match := range matches {
		candidate, skip, err := jazzAtTheLescarCandidateFromBlock(match[1], match[2], match[3], defaultTime, updatedDate)
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

func parseJazzAtTheLescarPageMetadata(raw []byte) (string, time.Time, error) {
	defaultTimeMatch := jazzAtTheLescarDefaultTimePattern.FindSubmatch(raw)
	if len(defaultTimeMatch) < 2 {
		return "", time.Time{}, fmt.Errorf("missing Jazz at The Lescar default music time")
	}
	updatedMatch := jazzAtTheLescarUpdatedPattern.FindSubmatch(raw)
	if len(updatedMatch) < 4 {
		return "", time.Time{}, fmt.Errorf("missing Jazz at The Lescar page updated date")
	}

	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return "", time.Time{}, err
	}
	updatedDate, err := time.ParseInLocation("2 January 2006", string(updatedMatch[1])+" "+string(updatedMatch[2])+" "+string(updatedMatch[3]), loc)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("parse Jazz at The Lescar page updated date: %w", err)
	}
	return strings.ToLower(strings.TrimSpace(string(defaultTimeMatch[1]))), updatedDate, nil
}

func jazzAtTheLescarCandidateFromBlock(rawTitle, rawTTL, rawDescription []byte, defaultTime string, updatedDate time.Time) (EventCandidate, ParseSkip, error) {
	title := jazzAtTheLescarCleanText(string(rawTitle))
	ttl := jazzAtTheLescarCleanText(string(rawTTL))
	description := jazzAtTheLescarDescription(string(rawDescription))
	skip := ParseSkip{Summary: title}

	switch {
	case title == "":
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	case ttl == "":
		skip.Reason = "missing date/venue line"
		return EventCandidate{}, skip, nil
	}

	datePart, venue, err := jazzAtTheLescarDateAndVenue(ttl)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Jazz at The Lescar ttl for %q: %w", title, err)
	}
	if jazzAtTheLescarHasMultipleDates(datePart) {
		skip.Reason = "multiple event dates"
		return EventCandidate{}, skip, nil
	}
	startTime, conflict := jazzAtTheLescarExplicitTime(ttl, description)
	if conflict {
		skip.Reason = "multiple explicit start times"
		return EventCandidate{}, skip, nil
	}
	if startTime == "" {
		startTime = defaultTime
	}
	startAt, err := jazzAtTheLescarStartAt(datePart, startTime, updatedDate)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Jazz at The Lescar date for %q: %w", title, err)
	}

	return EventCandidate{
		Summary:     title,
		Description: description,
		Location:    venue,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
	}, ParseSkip{}, nil
}

func jazzAtTheLescarDateAndVenue(ttl string) (string, string, error) {
	parts := strings.Split(ttl, "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("expected date / venue / price")
	}
	datePart := strings.TrimSpace(parts[0])
	venue := strings.TrimSpace(parts[1])
	if datePart == "" || venue == "" {
		return "", "", fmt.Errorf("missing date or venue")
	}
	return datePart, venue, nil
}

func jazzAtTheLescarHasMultipleDates(datePart string) bool {
	return len(jazzAtTheLescarDayPattern.FindAllStringSubmatch(datePart, -1)) > 1
}

func jazzAtTheLescarExplicitTime(values ...string) (string, bool) {
	seen := make(map[string]struct{})
	for _, value := range values {
		matches := jazzAtTheLescarInlineTimePattern.FindAllStringSubmatch(value, -1)
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			seen[strings.ToLower(strings.TrimSpace(match[1]))] = struct{}{}
		}
	}
	switch len(seen) {
	case 0:
		return "", false
	case 1:
		for value := range seen {
			return value, false
		}
	}
	return "", true
}

func jazzAtTheLescarStartAt(datePart, startTime string, updatedDate time.Time) (time.Time, error) {
	monthMatch := jazzAtTheLescarMonthPattern.FindStringSubmatch(datePart)
	if len(monthMatch) < 2 {
		return time.Time{}, fmt.Errorf("missing month in %q", datePart)
	}
	dayMatch := jazzAtTheLescarDayPattern.FindStringSubmatch(datePart)
	if len(dayMatch) < 2 {
		return time.Time{}, fmt.Errorf("missing day in %q", datePart)
	}

	loc := updatedDate.Location()
	var candidate time.Time
	var err error
	for _, layout := range []string{"2 January 2006 3:04pm", "2 January 2006 3pm"} {
		candidate, err = time.ParseInLocation(layout, dayMatch[1]+" "+monthMatch[1]+" "+fmt.Sprintf("%d", updatedDate.Year())+" "+startTime, loc)
		if err == nil {
			break
		}
	}
	if err != nil {
		return time.Time{}, err
	}
	if candidate.Before(updatedDate) {
		candidate = candidate.AddDate(1, 0, 0)
	}
	return candidate.UTC(), nil
}

func jazzAtTheLescarDescription(raw string) string {
	text := strings.ReplaceAll(raw, "<br>", "\n")
	text = strings.ReplaceAll(text, "<br/>", "\n")
	text = strings.ReplaceAll(text, "<br />", "\n")
	text = html.UnescapeString(text)
	text = jazzAtTheLescarTagPattern.ReplaceAllString(text, " ")
	return jazzAtTheLescarCleanText(text)
}

func jazzAtTheLescarCleanText(value string) string {
	value = html.UnescapeString(value)
	value = jazzAtTheLescarWhitespacePattern.ReplaceAllString(strings.TrimSpace(value), " ")
	return strings.TrimSpace(value)
}
