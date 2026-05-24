package ingest

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const hagglersCornerVenueName = "Hagglers Corner"
const hagglersCornerVenueEvidence = "Hagglers Corner, 586 Queens Road, Sheffield"

var (
	hagglersCornerDetailPathPattern     = regexp.MustCompile(`^/[a-z0-9][a-z0-9-]*/?$`)
	hagglersCornerH1Pattern             = regexp.MustCompile(`(?is)<h1\b[^>]*>(.*?)</h1>`)
	hagglersCornerTitleTagPattern       = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	hagglersCornerAggregateTitlePattern = regexp.MustCompile(`(?i)^\s*what'?s on(?: this)?(?:\s+[a-z]+)?\s*$`)
	hagglersCornerNonMusicTitlePattern  = regexp.MustCompile(`(?i)\b(?:quiz|market|workshop|private(?:\s+hire)?|community(?:\s+event)?|fundraiser|comedy)\b`)
	hagglersCornerMusicSignalPattern    = regexp.MustCompile(`(?i)\b(?:gig|gigs|live music|live performance|performance|dj|djs|band|bands|album launch|launch party|sound system|selector|selectors|reggae|house|disco|club night|club|live set|warm up dj set|roots|dancehall|afrobeats|hip hop|soul|funk|jazz)\b`)
	hagglersCornerDatePattern           = regexp.MustCompile(`(?i)\b(?:\b(?:mon|tues|wednes|thurs|fri|sat|sun)(?:day)?\s+)?(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+(?:20\d{2}|\d{2})|\d{1,2}/\d{1,2}/(?:20\d{2}|\d{2})|\d{4}-\d{2}-\d{2})\b`)
	hagglersCornerTimePattern           = regexp.MustCompile(`(?i)\b(?:from\s+)?(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{2}:\d{2})(?:\s*[–-]\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{2}:\d{2}))?\b`)
)

var hagglersCornerRejectedDetailPaths = map[string]struct{}{
	"bar":          {},
	"book-a-table": {},
	"businesses":   {},
	"contact":      {},
	"events":       {},
	"kitchen":      {},
	"love-yurts":   {},
	"markets":      {},
	"venue-hire":   {},
	"workshops":    {},
}

func hagglers_corner_detail_links(baseURL string, body []byte, limit int) ([]string, error) {
	links, err := ExtractSameHostLinks(baseURL, body, limit, func(u *url.URL) bool {
		path := strings.TrimSuffix(strings.TrimSpace(u.EscapedPath()), "/")
		if !hagglersCornerDetailPathPattern.MatchString(path) {
			return false
		}
		_, rejected := hagglersCornerRejectedDetailPaths[strings.Trim(strings.ToLower(path), "/")]
		return !rejected
	})
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(links))
	stable := make([]string, 0, len(links))
	for _, link := range links {
		parsed, err := url.Parse(link)
		if err != nil {
			return nil, err
		}
		parsed.RawQuery = ""
		parsed.Fragment = ""
		normalized := parsed.String()
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		stable = append(stable, normalized)
	}
	return stable, nil
}

func ParseHagglersCornerDetailPage(pageURL string, raw []byte) ParseResult {
	candidate, skip, err := hagglersCornerDetailCandidate(pageURL, raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}
	if skip.Reason != "" {
		return ParseResult{Skips: []ParseSkip{skip}}
	}
	return ParseResult{Candidates: []EventCandidate{candidate}}
}

func hagglersCornerDetailCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	pageURL = strings.TrimSpace(pageURL)
	title := hagglersCornerDetailTitle(raw)
	skip := ParseSkip{UID: pageURL, Summary: title}
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}
	if hagglersCornerIsAggregatePost(title) {
		skip.Reason = "aggregate monthly post"
		return EventCandidate{}, skip, nil
	}

	lines := hagglersCornerCleanLines(raw)
	if hagglersCornerHasNonMusicSignal(title) {
		skip.Reason = "non-music event"
		return EventCandidate{}, skip, nil
	}

	hasMusicSignal := hagglersCornerHasMusicSignal(title, lines)
	dateText, dateFound := hagglersCornerFindDateText(append([]string{title}, lines...))
	startText, endText, timeFound := hagglersCornerFindTimeText(append([]string{title}, lines...))
	switch {
	case !hasMusicSignal && dateFound:
		skip.Reason = "missing music signal"
		return EventCandidate{}, skip, nil
	case !dateFound && hasMusicSignal:
		skip.Reason = "missing event date"
		return EventCandidate{}, skip, nil
	case !dateFound && !hasMusicSignal:
		skip.Reason = "missing music signal"
		return EventCandidate{}, skip, nil
	case dateFound && !timeFound:
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	}

	startAt, endAt, err := hagglersCornerParseDateTime(dateText, startText, endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Hagglers Corner start time for %q: %w", title, err)
	}

	candidate := EventCandidate{
		UID:         pageURL,
		Summary:     title,
		Location:    hagglersCornerVenueName,
		LocationRaw: hagglersCornerVenueEvidence,
		URL:         pageURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
	}
	if !endAt.IsZero() {
		candidate.EndAt = formatTime(endAt)
	}
	return candidate, ParseSkip{}, nil
}

func hagglersCornerDetailTitle(raw []byte) string {
	if match := hagglersCornerH1Pattern.FindStringSubmatch(string(raw)); len(match) > 1 {
		if title := hagglersCornerCleanTitle(match[1]); title != "" {
			return title
		}
	}
	if match := hagglersCornerTitleTagPattern.FindStringSubmatch(string(raw)); len(match) > 1 {
		if title := hagglersCornerCleanTitle(match[1]); title != "" {
			return title
		}
	}

	lines := hagglersCornerCleanLines(raw)
	if len(lines) > 0 {
		return lines[0]
	}
	return ""
}

func hagglersCornerCleanTitle(value string) string {
	title := normalizeEventTitleSpacing(semanticInlineText(value))
	lower := strings.ToLower(title)
	for _, suffix := range []string{" | hagglers corner", " - hagglers corner", " – hagglers corner", " — hagglers corner"} {
		if strings.HasSuffix(lower, suffix) {
			title = strings.TrimSpace(title[:len(title)-len(suffix)])
			break
		}
	}
	return strings.TrimSpace(title)
}

func hagglersCornerCleanLines(raw []byte) []string {
	text := semanticDescriptionText(string(raw))
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = normalizeEventTitleSpacing(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func hagglersCornerIsAggregatePost(title string) bool {
	return hagglersCornerAggregateTitlePattern.MatchString(strings.TrimSpace(title))
}

func hagglersCornerHasNonMusicSignal(title string) bool {
	return hagglersCornerNonMusicTitlePattern.MatchString(strings.ToLower(strings.TrimSpace(title)))
}

func hagglersCornerHasMusicSignal(title string, lines []string) bool {
	candidate := strings.ToLower(strings.TrimSpace(title))
	if hagglersCornerMusicSignalPattern.MatchString(candidate) {
		return true
	}
	for i := 0; i < len(lines) && i < 6; i++ {
		if hagglersCornerMusicSignalPattern.MatchString(strings.ToLower(lines[i])) {
			return true
		}
	}
	return false
}

func hagglersCornerFindDateText(lines []string) (string, bool) {
	for _, line := range lines {
		if match := hagglersCornerDatePattern.FindStringSubmatch(line); len(match) > 1 {
			return strings.TrimSpace(match[1]), true
		}
	}
	return "", false
}

func hagglersCornerFindTimeText(lines []string) (string, string, bool) {
	for _, line := range lines {
		if match := hagglersCornerTimePattern.FindStringSubmatch(line); len(match) > 1 {
			start := strings.TrimSpace(match[1])
			end := ""
			if len(match) > 2 {
				end = strings.TrimSpace(match[2])
			}
			return start, end, true
		}
	}
	return "", "", false
}

func hagglersCornerParseDateTime(dateText, startText, endText string) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	date, err := hagglersCornerParseDate(dateText, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	startHour, startMinute, err := hagglersCornerParseClock(startText)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startAt := time.Date(date.Year(), date.Month(), date.Day(), startHour, startMinute, 0, 0, loc)

	if strings.TrimSpace(endText) == "" {
		return startAt.UTC(), time.Time{}, nil
	}
	endHour, endMinute, err := hagglersCornerParseClock(endText)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	endAt := time.Date(date.Year(), date.Month(), date.Day(), endHour, endMinute, 0, 0, loc)
	if !endAt.After(startAt) {
		endAt = endAt.Add(24 * time.Hour)
	}

	return startAt.UTC(), endAt.UTC(), nil
}

func hagglersCornerParseDate(value string, loc *time.Location) (time.Time, error) {
	value = strings.TrimSpace(strings.TrimSuffix(value, ","))
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return time.Time{}, fmt.Errorf("missing date")
	}

	fields := strings.Fields(value)
	if len(fields) > 1 {
		switch strings.ToLower(fields[0]) {
		case "mon", "monday", "tues", "tuesday", "wed", "wednesday", "thurs", "thursday", "fri", "friday", "sat", "saturday", "sun", "sunday":
			fields = fields[1:]
		}
	}
	if len(fields) == 0 {
		return time.Time{}, fmt.Errorf("missing date")
	}
	value = strings.Join(fields, " ")
	value = strings.NewReplacer("st ", " ", "nd ", " ", "rd ", " ", "th ", " ").Replace(value)
	value = strings.TrimSpace(strings.NewReplacer("st", "", "nd", "", "rd", "", "th", "").Replace(value))

	layouts := []string{
		"2 January 2006",
		"2 Jan 2006",
		"2 January 06",
		"2 Jan 06",
		"2/1/2006",
		"2/1/06",
		"02/01/2006",
		"02/01/06",
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported date %q", value)
}

func hagglersCornerParseClock(value string) (int, int, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "from ")
	value = strings.ReplaceAll(value, " ", "")
	for _, layout := range []string{"3:04pm", "3pm", "15:04"} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.Hour(), parsed.Minute(), nil
		}
	}
	return 0, 0, fmt.Errorf("unsupported time %q", value)
}
