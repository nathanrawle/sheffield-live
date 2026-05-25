package ingest

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const crookesClubVenueName = "Crookes Club"
const crookesClubConcertRoomName = "Concert Room"
const crookesClubLoungeRoomName = "Lounge"

var (
	crookesClubHeadingPattern    = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>`)
	crookesClubDatePattern       = regexp.MustCompile(`(?i)\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)(?:\s+(20\d{2}))?\b`)
	crookesClubTimePattern       = regexp.MustCompile(`(?i)\b(\d{1,2}(?::\d{2})?\s*[ap]m|\d{2}:\d{2})\b`)
	crookesClubYearPattern       = regexp.MustCompile(`\b(20\d{2})\b`)
	crookesClubDateLinePattern   = regexp.MustCompile(`(?i)^(?:mon|tues|wednes|thurs|fri|sat|sun)(?:day)?\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+(?:\s+20\d{2})?(?:\s+\d{1,2}(?::\d{2})?\s*[ap]m)?\s*$|^(?:\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+(?:\s+20\d{2})?)(?:\s+\d{1,2}(?::\d{2})?\s*[ap]m)?\s*$`)
	crookesClubHomepageSkipWords = []string{"england vs", "panama", "comedy", "quiz", "bingo", "snooker", "pool", "bowls", "football"}
	crookesClubLoungeTimeSupport = regexp.MustCompile(`(?i)live music starts at\s*8:45\s*pm`)
)

func ParseCrookesClubSourcePage(pageURL string, raw []byte, limit int) ParseResult {
	result := ParseResult{}
	if crookesClubIsLoungePage(pageURL) {
		result = parseCrookesClubLoungePage(pageURL, raw)
	} else {
		result = parseCrookesClubHomepagePage(pageURL, raw)
	}
	return limitParseResult(result, limit)
}

func ExtractCrookesClubSecondaryPages(pageURL string, raw []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	base, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return nil, err
	}
	loungeURL := base.ResolveReference(&url.URL{Path: "/lounge-live-music"}).String()
	links := []string{loungeURL}
	if limit == 1 {
		return links, nil
	}

	discovered, err := ExtractSameHostLinks(pageURL, raw, limit, func(u *url.URL) bool {
		path := strings.TrimSuffix(strings.TrimSpace(u.EscapedPath()), "/")
		return path == "/lounge-live-music"
	})
	if err != nil {
		return nil, err
	}
	links = append(links, discovered...)

	seen := make(map[string]struct{}, len(links))
	out := make([]string, 0, len(links))
	for _, link := range links {
		if len(out) >= limit {
			break
		}
		parsed, err := url.Parse(link)
		if err != nil {
			return nil, err
		}
		parsed.Fragment = ""
		parsed.RawFragment = ""
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		if parsed.Path == "" {
			parsed.Path = "/"
		}
		normalized := parsed.String()
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func parseCrookesClubHomepagePage(pageURL string, raw []byte) ParseResult {
	section, err := crookesClubSection(raw, "UPCOMING DATES FOR YOUR DIARY", "What We Offer")
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}

	matches := crookesClubHeadingPattern.FindAllSubmatchIndex(section, -1)
	if len(matches) == 0 {
		return ParseResult{Errors: []string{"no Crookes Club homepage event headings found"}}
	}

	var result ParseResult
	seen := make(map[string]struct{}, len(matches))
	for i, match := range matches {
		title := crookesClubCleanText(string(section[match[2]:match[3]]))
		if title == "" {
			continue
		}
		bodyEnd := len(section)
		if i+1 < len(matches) {
			bodyEnd = matches[i+1][0]
		}
		body := section[match[1]:bodyEnd]
		lines := crookesClubCleanLines(body)
		if len(lines) == 0 {
			continue
		}

		candidate, skip, ok, err := crookesClubHomepageCandidate(pageURL, title, lines)
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			continue
		}
		if !ok {
			continue
		}
		if skip.Reason != "" {
			result.Skips = append(result.Skips, skip)
			continue
		}
		if _, exists := seen[candidate.UID]; exists {
			continue
		}
		seen[candidate.UID] = struct{}{}
		result.Candidates = append(result.Candidates, candidate)
	}
	return result
}

func parseCrookesClubLoungePage(pageURL string, raw []byte) ParseResult {
	section, err := crookesClubSection(raw, "Upcoming Artistes", "Copyright")
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}

	lines := crookesClubCleanLines(section)
	if len(lines) == 0 {
		return ParseResult{Errors: []string{"no Crookes Club lounge rows found"}}
	}
	defaultTime, hasDefaultTime := crookesClubLoungeDefaultTime(raw)

	var result ParseResult
	seen := make(map[string]struct{}, len(lines))
	for i := 0; i < len(lines); i++ {
		dateLine := crookesClubCleanText(lines[i])
		if !crookesClubIsLoungeDateLine(dateLine) {
			continue
		}

		artistLine, nextIndex := crookesClubNextNonEmptyLine(lines, i+1)
		if artistLine == "" {
			result.Skips = append(result.Skips, ParseSkip{Reason: "missing artist"})
			continue
		}

		candidate, skip, ok, err := crookesClubLoungeCandidate(pageURL, dateLine, artistLine, defaultTime, hasDefaultTime, lines[i:nextIndex])
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			continue
		}
		if !ok {
			continue
		}
		if skip.Reason != "" {
			result.Skips = append(result.Skips, skip)
			continue
		}
		if _, exists := seen[candidate.UID]; exists {
			continue
		}
		seen[candidate.UID] = struct{}{}
		result.Candidates = append(result.Candidates, candidate)
		i = nextIndex - 1
	}
	return result
}

func crookesClubHomepageCandidate(pageURL, title string, lines []string) (EventCandidate, ParseSkip, bool, error) {
	skip := ParseSkip{Summary: title}
	combined := strings.ToLower(title + "\n" + strings.Join(lines, "\n"))
	for _, word := range crookesClubHomepageSkipWords {
		if strings.Contains(combined, word) {
			if word == "comedy" {
				skip.Reason = "comedy"
			} else {
				skip.Reason = "non-music social event"
			}
			return EventCandidate{}, skip, true, nil
		}
	}

	dateLine, dateIndex := crookesClubFindDateLine(lines)
	if dateLine == "" {
		skip.Reason = "missing event date"
		return EventCandidate{}, skip, true, nil
	}

	yearText := crookesClubFindYear(lines[dateIndex:])
	if yearText == "" {
		yearText = crookesClubFindYear(lines)
	}
	if yearText == "" {
		skip.Reason = "missing deterministic year"
		return EventCandidate{}, skip, true, nil
	}

	timeText := crookesClubFindTime(lines[dateIndex:])
	if timeText == "" {
		timeText = crookesClubFindTime(lines)
	}
	if timeText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, true, nil
	}

	startAt, err := crookesClubStartAt(dateLine, yearText, timeText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse Crookes Club homepage date for %q: %w", title, err)
	}

	candidate := crookesClubBaseCandidate(pageURL, title, crookesClubConcertRoomName, "concert-room", startAt)
	return candidate, ParseSkip{}, true, nil
}

func crookesClubLoungeCandidate(pageURL, dateLine, artistLine, defaultTime string, hasDefaultTime bool, lines []string) (EventCandidate, ParseSkip, bool, error) {
	title := crookesClubCleanText(artistLine)
	skip := ParseSkip{Summary: title}
	if title == "" {
		skip.Reason = "missing artist"
		return EventCandidate{}, skip, true, nil
	}

	yearText := crookesClubFindYear([]string{dateLine})
	if yearText == "" {
		skip.Reason = "missing deterministic year"
		return EventCandidate{}, skip, true, nil
	}

	timeText := crookesClubFindTime(lines)
	if timeText == "" && hasDefaultTime {
		timeText = defaultTime
	}
	if timeText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, true, nil
	}

	startAt, err := crookesClubStartAt(dateLine, yearText, timeText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse Crookes Club lounge date for %q: %w", title, err)
	}

	candidate := crookesClubBaseCandidate(pageURL, title, crookesClubLoungeRoomName, "lounge", startAt)
	return candidate, ParseSkip{}, true, nil
}

func crookesClubBaseCandidate(pageURL, title, roomName, roomSlug string, startAt time.Time) EventCandidate {
	return EventCandidate{
		UID:         crookesClubEventUID(roomSlug, title, startAt),
		Summary:     crookesClubCleanText(title),
		Location:    crookesClubVenueName,
		LocationRaw: crookesClubVenueName,
		RoomText:    roomName,
		Rooms: []RoomCandidate{{
			Slug: roomSlug,
			Name: roomName,
		}},
		URL:     strings.TrimSpace(pageURL),
		Status:  "Listed",
		StartAt: formatTime(startAt),
	}
}

func crookesClubSection(raw []byte, startMarker, endMarker string) ([]byte, error) {
	start, err := crookesClubHeadingBoundary(raw, startMarker, true)
	if err != nil {
		return nil, err
	}
	end, err := crookesClubHeadingBoundary(raw[start:], endMarker, false)
	if err != nil {
		fallbackEnd := crookesClubTextBoundary(raw[start:], endMarker)
		if fallbackEnd < 0 {
			return nil, err
		}
		end = fallbackEnd
	}
	return raw[start : start+end], nil
}

func crookesClubHeadingBoundary(raw []byte, marker string, after bool) (int, error) {
	pattern := regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>\s*` + regexp.QuoteMeta(marker) + `\s*</h[1-6]>`)
	loc := pattern.FindIndex(raw)
	if loc == nil {
		return 0, fmt.Errorf("missing Crookes Club section marker %q", marker)
	}
	if after {
		return loc[1], nil
	}
	return loc[0], nil
}

func crookesClubTextBoundary(raw []byte, marker string) int {
	idx := strings.Index(strings.ToLower(string(raw)), strings.ToLower(strings.TrimSpace(marker)))
	return idx
}

func crookesClubCleanLines(raw []byte) []string {
	lines := strings.Split(labeledFieldText(raw), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = crookesClubCleanText(line)
		if line == "" {
			continue
		}
		switch strings.ToLower(line) {
		case "image":
			continue
		}
		out = append(out, line)
	}
	return out
}

func crookesClubCleanText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func crookesClubIsLoungePage(pageURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return false
	}
	path := strings.TrimSuffix(strings.TrimSpace(parsed.EscapedPath()), "/")
	return path == "/lounge-live-music"
}

func crookesClubIsLoungeDateLine(line string) bool {
	return crookesClubDatePattern.MatchString(strings.TrimSpace(line))
}

func crookesClubFindDateLine(lines []string) (string, int) {
	for i, line := range lines {
		if crookesClubIsLoungeDateLine(line) {
			return crookesClubCleanText(line), i
		}
	}
	return "", -1
}

func crookesClubFindYear(lines []string) string {
	for _, line := range lines {
		if match := crookesClubYearPattern.FindStringSubmatch(line); len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func crookesClubFindTime(lines []string) string {
	for _, line := range lines {
		if match := crookesClubTimePattern.FindStringSubmatch(line); len(match) == 2 {
			return strings.ToLower(crookesClubCleanText(match[1]))
		}
	}
	return ""
}

func crookesClubLoungeDefaultTime(raw []byte) (string, bool) {
	if !crookesClubLoungeTimeSupport.Match(raw) {
		return "", false
	}
	return "20:45", true
}

func crookesClubStartAt(dateLine, yearText, timeText string) (time.Time, error) {
	dateMatch := crookesClubDatePattern.FindStringSubmatch(strings.TrimSpace(dateLine))
	if len(dateMatch) != 4 {
		return time.Time{}, fmt.Errorf("unable to parse date line %q", dateLine)
	}
	day := dateMatch[1]
	month := dateMatch[2]
	year := strings.TrimSpace(dateMatch[3])
	if year == "" {
		year = strings.TrimSpace(yearText)
	}
	if year == "" {
		return time.Time{}, fmt.Errorf("missing year in %q", dateLine)
	}

	tm, err := crookesClubParseClock(timeText)
	if err != nil {
		return time.Time{}, err
	}

	clock := tm.Format("15:04")
	for _, layout := range []string{"2 Jan 2006 15:04", "2 January 2006 15:04"} {
		start, parseErr := time.ParseInLocation(layout, day+" "+month+" "+year+" "+clock, crookesClubLondonLocation())
		if parseErr == nil {
			return start.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("parse date %q with month %q", dateLine, month)
}

func crookesClubNormalizeClock(value string) string {
	value = strings.ToLower(crookesClubCleanText(value))
	value = strings.ReplaceAll(value, " ", "")
	return value
}

func crookesClubParseClock(value string) (time.Time, error) {
	normalized := crookesClubNormalizeClock(value)
	for _, layout := range []string{"3:04pm", "3pm", "15:04"} {
		tm, err := time.ParseInLocation(layout, normalized, crookesClubLondonLocation())
		if err == nil {
			return tm, nil
		}
	}
	return time.Time{}, fmt.Errorf("parse time %q", value)
}

func crookesClubLondonLocation() *time.Location {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.UTC
	}
	return loc
}

func crookesClubEventUID(roomSlug, title string, startAt time.Time) string {
	normalizedTitle := strings.ToLower(crookesClubCleanText(title))
	return strings.Join([]string{
		CrookesClubSource,
		roomSlug,
		normalizedTitle,
		startAt.UTC().Format(time.RFC3339),
	}, "|")
}

func crookesClubNextNonEmptyLine(lines []string, start int) (string, int) {
	for i := start; i < len(lines); i++ {
		if line := crookesClubCleanText(lines[i]); line != "" {
			return line, i + 1
		}
	}
	return "", len(lines)
}
