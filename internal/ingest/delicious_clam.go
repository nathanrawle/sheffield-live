package ingest

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const deliciousClamVenueName = "Delicious Clam"
const deliciousClamVenueSlug = "delicious-clam"

var (
	deliciousClamLegacySeparatorPattern    = regexp.MustCompile(`(?is)\* \* \*`)
	deliciousClamSkiddleEventPathPattern   = regexp.MustCompile(`(?i)^/e/\d+/?$`)
	deliciousClamSkiddleWhatsOnPathPattern = regexp.MustCompile(`(?i)^/whats-on/Sheffield/Delicious-Clam/[^/]+/\d+/?$`)
	deliciousClamLongDatePattern           = regexp.MustCompile(`(?i)(?:\b(?:mon|tues|wednes|thurs|fri|sat|sun)(?:day)?\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(20\d{2})`)
	deliciousClamSlashDatePattern          = regexp.MustCompile(`(?i)\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b`)
	deliciousClamPerformanceStartsPattern  = regexp.MustCompile(`(?i)performance starts\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{2}:\d{2})`)
	deliciousClamFromPattern               = regexp.MustCompile(`(?i)\bfrom\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{2}:\d{2})`)
	deliciousClamTimePattern               = regexp.MustCompile(`(?i)\b(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{2}:\d{2})\b`)
)

func ExtractDeliciousClamTicketLinks(pageURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	if !deliciousClamOfficialEventsPage(pageURL) {
		return nil, nil
	}

	scanBody := body
	if cut := deliciousClamLegacySeparatorPattern.FindIndex(scanBody); cut != nil {
		scanBody = scanBody[:cut[0]]
	}

	baseURL, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return nil, err
	}

	matches := htmlAnchorTagPattern.FindAllIndex(scanBody, -1)
	seen := make(map[string]struct{}, len(matches))
	links := make([]string, 0, min(limit, len(matches)))
	for _, match := range matches {
		attrs := parseHTMLAttributes(string(scanBody[match[0]:match[1]]))
		resolved, ok := deliciousClamResolvedTicketURL(baseURL, attrs["href"])
		if !ok {
			continue
		}
		if _, exists := seen[resolved]; exists {
			continue
		}
		seen[resolved] = struct{}{}
		links = append(links, resolved)
		if len(links) >= limit {
			break
		}
	}
	return links, nil
}

func ParseDeliciousClamTicketPage(pageURL string, raw []byte) ParseResult {
	candidate, skip, err := deliciousClamTicketPageCandidate(pageURL, raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}
	if skip.Reason != "" {
		return ParseResult{Skips: []ParseSkip{skip}}
	}
	return ParseResult{Candidates: []EventCandidate{candidate}}
}

func deliciousClamOfficialEventsPage(pageURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host != "deliciousclam.co.uk" && host != "www.deliciousclam.co.uk" {
		return false
	}
	return strings.Trim(strings.ToLower(strings.TrimSpace(parsed.EscapedPath())), "/") == "events"
}

func deliciousClamResolvedTicketURL(base *url.URL, raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	switch strings.ToLower(strings.TrimSpace(resolved.Hostname())) {
	case "skiddle.com", "www.skiddle.com":
		resolved.Host = "www.skiddle.com"
	default:
		return "", false
	}

	path := strings.TrimSuffix(resolved.EscapedPath(), "/")
	if !deliciousClamSkiddleEventPathPattern.MatchString(path) && !deliciousClamSkiddleWhatsOnPathPattern.MatchString(path) {
		return "", false
	}

	resolved.Fragment = ""
	resolved.RawFragment = ""
	resolved.RawQuery = ""
	if strings.HasSuffix(resolved.Path, "/") && resolved.Path != "/" {
		resolved.Path = strings.TrimSuffix(resolved.Path, "/")
	}
	if resolved.Scheme == "" {
		resolved.Scheme = "https"
	}
	return resolved.String(), true
}

func deliciousClamTicketPageCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	text := deliciousClamCleanText(raw)
	lines := deliciousClamCleanLines(text)
	skip := ParseSkip{UID: strings.TrimSpace(pageURL)}

	title := deliciousClamTitle(lines)
	skip.Summary = title
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}
	if deliciousClamHasNonMusicSignal(title, lines) {
		skip.Reason = "non-music event"
		return EventCandidate{}, skip, nil
	}

	venueText, venueRaw := deliciousClamVenueEvidence(lines)
	if venueText == "" {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, nil
	}

	startAt, _, multiDay, ok := deliciousClamStartAt(lines)
	if !ok {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	}
	if multiDay {
		skip.Reason = "multi-day event"
		return EventCandidate{}, skip, nil
	}

	return EventCandidate{
		UID:         strings.TrimSpace(pageURL),
		Summary:     title,
		Location:    deliciousClamVenueName,
		LocationRaw: venueRaw,
		URL:         strings.TrimSpace(pageURL),
		Status:      "Listed",
		StartAt:     formatTime(startAt),
	}, ParseSkip{}, nil
}

func deliciousClamCleanText(raw []byte) string {
	return semanticDescriptionText(string(raw))
}

func deliciousClamCleanLines(text string) []string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func deliciousClamTitle(lines []string) string {
	for _, line := range lines {
		title := deliciousClamHeadingText(line)
		if title == "" {
			continue
		}
		if deliciousClamIsBoilerplateHeading(title) {
			continue
		}
		return title
	}
	return ""
}

func deliciousClamHeadingText(line string) string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "#") {
		return ""
	}
	title := strings.TrimSpace(strings.TrimLeft(line, "#"))
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	return title
}

func deliciousClamIsBoilerplateHeading(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "back to all events", "tickets here", "venue", "hours", "about", "google calendar", "ics", "earlier event":
		return true
	default:
		return false
	}
}

func deliciousClamHasNonMusicSignal(title string, lines []string) bool {
	candidate := strings.ToLower(title + "\n" + strings.Join(lines, "\n"))
	for _, keyword := range []string{"comedy", "film", "workshop", "market", "talk", "quiz", "theatre", "poetry"} {
		if strings.Contains(candidate, keyword) {
			return true
		}
	}
	return false
}

func deliciousClamVenueEvidence(lines []string) (string, string) {
	for _, line := range lines {
		lower := strings.ToLower(line)
		switch {
		case strings.Contains(lower, "delicious clam"):
			return deliciousClamVenueName, line
		case strings.Contains(lower, "12 exchange street"):
			return deliciousClamVenueName, line
		case strings.Contains(lower, "s2 5ts"):
			return deliciousClamVenueName, line
		}
	}
	return "", ""
}

func deliciousClamStartAt(lines []string) (time.Time, int, bool, bool) {
	var (
		firstDate     time.Time
		firstDateText string
		firstIndex    = -1
		multiDay      bool
	)
	seenDates := make(map[string]struct{})

	for i, line := range lines {
		dateText, ok := deliciousClamDateText(line)
		if !ok {
			continue
		}
		date, err := deliciousClamParseDate(dateText)
		if err != nil {
			return time.Time{}, 0, false, false
		}
		key := date.Format("2006-01-02")
		if firstIndex < 0 {
			firstDate = date
			firstDateText = dateText
			firstIndex = i
		} else if key != firstDate.Format("2006-01-02") {
			multiDay = true
		}
		seenDates[key] = struct{}{}
	}

	if len(seenDates) == 0 || firstIndex < 0 {
		return time.Time{}, 0, false, false
	}
	if multiDay || len(seenDates) > 1 {
		return time.Time{}, firstIndex, true, true
	}

	timeText := deliciousClamTimeText(lines, firstIndex)
	if timeText == "" {
		return time.Time{}, firstIndex, false, false
	}

	startAt, err := deliciousClamParseDateTime(firstDateText, timeText)
	if err != nil {
		return time.Time{}, 0, false, false
	}
	return startAt, firstIndex, false, true
}

func deliciousClamDateText(line string) (string, bool) {
	if match := deliciousClamLongDatePattern.FindStringSubmatch(line); len(match) == 4 {
		return strings.Join([]string{match[1], match[2], match[3]}, " "), true
	}
	if match := deliciousClamSlashDatePattern.FindStringSubmatch(line); len(match) == 4 {
		day := strings.TrimLeft(match[1], "0")
		if day == "" {
			day = "0"
		}
		month := strings.TrimLeft(match[2], "0")
		if month == "" {
			month = "0"
		}
		year := match[3]
		if len(year) == 2 {
			year = "20" + year
		}
		return strings.Join([]string{day, month, year}, "/"), true
	}
	return "", false
}

func deliciousClamTimeText(lines []string, dateIndex int) string {
	for i := dateIndex; i < len(lines) && i < dateIndex+5; i++ {
		if timeText := deliciousClamPreferredTimeText(lines[i]); timeText != "" {
			return timeText
		}
	}
	return ""
}

func deliciousClamPreferredTimeText(line string) string {
	for _, pattern := range []*regexp.Regexp{
		deliciousClamPerformanceStartsPattern,
		deliciousClamFromPattern,
		deliciousClamTimePattern,
	} {
		match := pattern.FindStringSubmatch(line)
		if len(match) < 2 {
			continue
		}
		return deliciousClamNormalizeClockText(match[1])
	}
	return ""
}

func deliciousClamNormalizeClockText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, ".", ":")
	return value
}

func deliciousClamParseDateTime(dateText, timeText string) (time.Time, error) {
	date, err := deliciousClamParseDate(dateText)
	if err != nil {
		return time.Time{}, err
	}
	hour, minute, err := deliciousClamParseClock(timeText)
	if err != nil {
		return time.Time{}, err
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, loc), nil
}

func deliciousClamParseDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ".")
	if strings.HasPrefix(strings.ToLower(value), "on ") {
		value = strings.TrimSpace(value[3:])
	}
	value = strings.TrimSpace(value)

	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}

	if match := deliciousClamSlashDatePattern.FindStringSubmatch(value); len(match) == 4 {
		day, err := strconv.Atoi(strings.TrimLeft(match[1], "0"))
		if err != nil {
			return time.Time{}, err
		}
		month, err := strconv.Atoi(strings.TrimLeft(match[2], "0"))
		if err != nil {
			return time.Time{}, err
		}
		year, err := strconv.Atoi(match[3])
		if err != nil {
			return time.Time{}, err
		}
		if year < 100 {
			year += 2000
		}
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc), nil
	}

	normalized := value
	if match := deliciousClamLongDatePattern.FindStringSubmatch(value); len(match) == 4 {
		normalized = strings.Join([]string{match[1], match[2], match[3]}, " ")
	}
	normalized = strings.TrimSpace(normalized)
	for _, layout := range []string{"2 January 2006", "2 Jan 2006"} {
		parsed, err := time.ParseInLocation(layout, normalized, loc)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Delicious Clam date %q", value)
}

func deliciousClamParseClock(value string) (int, int, error) {
	value = deliciousClamNormalizeClockText(value)
	if value == "" {
		return 0, 0, fmt.Errorf("empty time")
	}

	if strings.HasSuffix(value, "am") || strings.HasSuffix(value, "pm") {
		trimmed := strings.TrimSuffix(strings.TrimSuffix(value, "am"), "pm")
		if trimmed == "" {
			return 0, 0, fmt.Errorf("invalid time %q", value)
		}
		hour, minute, err := deliciousClamParseClockHMM(trimmed)
		if err != nil {
			return 0, 0, err
		}
		if strings.HasSuffix(value, "pm") && hour != 12 {
			hour += 12
		}
		if strings.HasSuffix(value, "am") && hour == 12 {
			hour = 0
		}
		return hour, minute, nil
	}

	return deliciousClamParseClockHMM(value)
}

func deliciousClamParseClockHMM(value string) (int, int, error) {
	parts := strings.Split(value, ":")
	switch len(parts) {
	case 1:
		hour, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, err
		}
		return hour, 0, nil
	case 2:
		hour, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, err
		}
		minute, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, err
		}
		return hour, minute, nil
	default:
		return 0, 0, fmt.Errorf("unsupported time %q", value)
	}
}
