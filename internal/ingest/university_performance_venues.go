package ingest

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const universityPerformanceVenuesOctagonDisplay = "Octagon Centre"
const universityPerformanceVenuesFirthHallDisplay = "Firth Hall"
const universityPerformanceVenuesDramaStudioDisplay = "Drama Studio"

var (
	universityPerformanceVenuesDetailPathPattern    = regexp.MustCompile(`^/our-events/[^/]+/?$`)
	universityPerformanceVenuesHeadingPattern       = regexp.MustCompile(`(?is)<h1\b[^>]*>(.*?)</h1>`)
	universityPerformanceVenuesTitlePattern         = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	universityPerformanceVenuesAnchorPattern        = regexp.MustCompile(`(?is)<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>`)
	universityPerformanceVenuesYearPattern          = regexp.MustCompile(`\b(20\d{2})\b`)
	universityPerformanceVenuesRangePattern         = regexp.MustCompile(`(?i)\b\d{1,2}(?:st|nd|rd|th)?\s*-\s*\d{1,2}(?:st|nd|rd|th)?\b`)
	universityPerformanceVenuesDateChunkPattern     = regexp.MustCompile(`(?i)^(?:(?:mon(?:day)?|tues(?:day)?|wednes(?:day)?|thurs(?:day)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\s+)?(?:(\d{1,2})(?:st|nd|rd|th)?(?:\s+([A-Za-z]+))?|([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?)(?:,?\s*(20\d{2}))?$`)
	universityPerformanceVenuesTimePattern          = regexp.MustCompile(`(?i)\b(\d{1,2}(?:(?::|\.)\d{2})?\s*(?:am|pm)?|\d{2}:\d{2})\b`)
	universityPerformanceVenuesMusicPositivePattern = regexp.MustCompile(`(?i)\b(?:gigs?|concerts?|music|musical|choir|orchestra|ensemble|recital|recitals|band|bands|trio|duo|quartet|quintet|folk|jazz|rock|pop|indie|metal|electronic|blues|soul|funk|reggae|sevdah|tour|album|live|tribute|gig|symphony|strings?|wind orchestra|chamber choir|pop/jazz|classical)\b`)
	universityPerformanceVenuesNonMusicPattern      = regexp.MustCompile(`(?i)\b(?:film|screening|q\s*&\s*a|theatre|theater|comedy|talk|lecture|workshop|ceremony|award|conference|panel|discussion|poetry|play|homo|fundraiser|quiz|private hire|hire)\b`)
)

func university_performance_venues_detail_links(baseURL string, body []byte, limit int) ([]string, error) {
	links, err := ExtractSameHostLinks(baseURL, body, limit, func(u *url.URL) bool {
		return universityPerformanceVenuesDetailPathPattern.MatchString(strings.TrimSpace(u.EscapedPath()))
	})
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(links))
	stable := make([]string, 0, len(links))
	for _, link := range links {
		parsed, err := url.Parse(link)
		if err != nil {
			return nil, err
		}
		parsed.RawQuery = ""
		parsed.Fragment = ""
		parsed.RawFragment = ""
		normalized := parsed.String()
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		stable = append(stable, normalized)
	}
	return stable, nil
}

func ParseUniversityPerformanceVenuesDetailPage(pageURL string, raw []byte) ParseResult {
	candidates, skip, err := universityPerformanceVenuesDetailCandidates(pageURL, raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}
	if skip.Reason != "" {
		return ParseResult{Skips: []ParseSkip{skip}}
	}
	return ParseResult{Candidates: candidates}
}

func universityPerformanceVenuesDetailCandidates(pageURL string, raw []byte) ([]EventCandidate, ParseSkip, error) {
	pageURL = strings.TrimSpace(pageURL)
	title := universityPerformanceVenuesPageTitle(raw)
	skip := ParseSkip{UID: pageURL, Summary: title}
	if title == "" {
		skip.Reason = "missing event title"
		return nil, skip, nil
	}

	fields := ParseLabeledFields(raw, "Dates", "Venue", "Times", "Cost")
	dates, skipReason, err := universityPerformanceVenuesParseDates(fields["Dates"])
	if err != nil {
		return nil, ParseSkip{}, err
	}
	if skipReason != "" {
		skip.Reason = skipReason
		return nil, skip, nil
	}

	venueValue := strings.TrimSpace(fields["Venue"])
	_, venueDisplay, venueRaw, ok := universityPerformanceVenuesVenueEvidence(venueValue)
	if !ok {
		switch {
		case venueValue == "":
			skip.Reason = "unsupported venue"
		case universityPerformanceVenuesVenueHasMultipleKnownSlugs(venueValue):
			skip.Reason = "ambiguous venue"
		default:
			skip.Reason = "unsupported venue"
		}
		return nil, skip, nil
	}

	if !universityPerformanceVenuesHasMusicSignal(title, raw) {
		skip.Reason = "non-music event"
		return nil, skip, nil
	}

	startHour, startMinute, ok := universityPerformanceVenuesParseClock(fields["Times"])
	if !ok {
		skip.Reason = "missing event start time"
		return nil, skip, nil
	}

	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return nil, ParseSkip{}, err
	}

	ticketURL := universityPerformanceVenuesTicketURL(pageURL, raw)
	candidates := make([]EventCandidate, 0, len(dates))
	for _, date := range dates {
		startAt := time.Date(date.Year(), date.Month(), date.Day(), startHour, startMinute, 0, 0, loc)
		uid := pageURL + "|" + date.Format("2006-01-02")
		if ticketURL != "" {
			if len(dates) == 1 {
				uid = ticketURL
			} else {
				uid = ticketURL + "|" + date.Format("2006-01-02")
			}
		}

		candidate := EventCandidate{
			UID:         uid,
			Summary:     title,
			Location:    venueDisplay,
			LocationRaw: venueRaw,
			URL:         pageURL,
			Status:      "Listed",
			StartAt:     formatTime(startAt),
		}
		candidates = append(candidates, candidate)
	}

	if len(candidates) == 0 {
		skip.Reason = "missing event date"
		return nil, skip, nil
	}

	return candidates, ParseSkip{}, nil
}

func universityPerformanceVenuesPageTitle(raw []byte) string {
	if match := universityPerformanceVenuesHeadingPattern.FindSubmatch(raw); len(match) > 1 {
		if title := normalizeEventTitleSpacing(semanticInlineText(string(match[1]))); title != "" {
			return title
		}
	}
	if match := universityPerformanceVenuesTitlePattern.FindSubmatch(raw); len(match) > 1 {
		if title := normalizeEventTitleSpacing(semanticInlineText(string(match[1]))); title != "" {
			return strings.TrimSpace(strings.TrimSuffix(title, " - Performance Venues"))
		}
	}
	return ""
}

func universityPerformanceVenuesVenueEvidence(value string) (string, string, string, bool) {
	slug, _, ok := universityPerformanceVenuesVenueSlugFromText(value)
	if !ok {
		return "", "", "", false
	}
	return slug, universityPerformanceVenuesVenueDisplayForSlug(slug), value, true
}

func universityPerformanceVenuesVenueHasMultipleKnownSlugs(value string) bool {
	_, multiple, _ := universityPerformanceVenuesVenueSlugFromText(value)
	return multiple
}

func universityPerformanceVenuesVenueSlugFromText(value string) (string, bool, bool) {
	parts := universityPerformanceVenuesVenueParts(value)
	seen := make(map[string]struct{}, len(parts))
	unknown := false
	for _, part := range parts {
		partSlug := universityPerformanceVenuesVenueSlugFromPart(part)
		if partSlug == "" {
			unknown = true
			continue
		}
		seen[partSlug] = struct{}{}
	}
	switch len(seen) {
	case 0:
		return "", false, false
	case 1:
		if unknown {
			return "", false, false
		}
		for slug := range seen {
			return slug, false, true
		}
	default:
		return "", true, false
	}
	return "", false, false
}

func universityPerformanceVenuesVenueSlugFromPart(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "octagon", "octagon centre", "the octagon centre":
		return "octagon-centre"
	case "firth hall":
		return "firth-hall"
	case "drama studio":
		return "drama-studio"
	default:
		return ""
	}
}

func universityPerformanceVenuesVenueDisplayForSlug(slug string) string {
	switch strings.TrimSpace(slug) {
	case "octagon-centre":
		return universityPerformanceVenuesOctagonDisplay
	case "firth-hall":
		return universityPerformanceVenuesFirthHallDisplay
	case "drama-studio":
		return universityPerformanceVenuesDramaStudioDisplay
	default:
		return ""
	}
}

func universityPerformanceVenuesVenueParts(value string) []string {
	value = html.UnescapeString(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	value = strings.ReplaceAll(value, "&", "|")
	parts := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', ';', '/', '|', '\n':
			return true
		default:
			return false
		}
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Join(strings.Fields(strings.TrimSpace(part)), " ")
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func universityPerformanceVenuesHasMusicSignal(title string, raw []byte) bool {
	text := strings.ToLower(title + "\n" + semanticDescriptionText(string(raw)))
	if universityPerformanceVenuesNonMusicPattern.MatchString(text) {
		return false
	}
	return universityPerformanceVenuesMusicPositivePattern.MatchString(text)
}

func universityPerformanceVenuesParseDates(value string) ([]time.Time, string, error) {
	value = strings.Join(strings.Fields(strings.TrimSpace(html.UnescapeString(value))), " ")
	if value == "" {
		return nil, "missing event date", nil
	}
	if universityPerformanceVenuesRangePattern.MatchString(value) {
		return nil, "ambiguous date range", nil
	}

	year := universityPerformanceVenuesYearPattern.FindString(value)
	if year == "" {
		return nil, "missing deterministic year", nil
	}

	chunks := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '&'
	})
	parts := make([]universityPerformanceVenuesDatePart, 0, len(chunks))
	for _, chunk := range chunks {
		part := universityPerformanceVenuesParseDateChunk(chunk)
		if part.ignore {
			continue
		}
		if part.err != nil {
			return nil, "", part.err
		}
		if part.day == 0 {
			continue
		}
		parts = append(parts, part)
	}

	if len(parts) == 0 {
		return nil, "missing event date", nil
	}

	for i := range parts {
		if parts[i].month != 0 {
			continue
		}
		if month := universityPerformanceVenuesNextMonth(parts, i+1); month != 0 {
			parts[i].month = month
			continue
		}
		if month := universityPerformanceVenuesPreviousMonth(parts, i-1); month != 0 {
			parts[i].month = month
			continue
		}
		return nil, "missing event date", nil
	}

	globalYear, err := time.Parse("2006", year)
	if err != nil {
		return nil, "", err
	}

	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return nil, "", err
	}

	dates := make([]time.Time, 0, len(parts))
	for _, part := range parts {
		yearValue := globalYear.Year()
		if part.year != 0 {
			yearValue = part.year
		}
		if part.month == 0 {
			return nil, "missing event date", nil
		}
		date, err := time.ParseInLocation("2 January 2006", fmt.Sprintf("%d %s %d", part.day, part.month.String(), yearValue), loc)
		if err != nil {
			return nil, "", err
		}
		dates = append(dates, date)
	}
	return dates, "", nil
}

type universityPerformanceVenuesDatePart struct {
	day    int
	month  time.Month
	year   int
	ignore bool
	err    error
}

func universityPerformanceVenuesParseDateChunk(chunk string) universityPerformanceVenuesDatePart {
	chunk = strings.Join(strings.Fields(strings.TrimSpace(html.UnescapeString(chunk))), " ")
	if chunk == "" {
		return universityPerformanceVenuesDatePart{ignore: true}
	}
	if universityPerformanceVenuesYearPattern.MatchString(chunk) && !strings.ContainsAny(chunk, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		year, err := time.Parse("2006", universityPerformanceVenuesYearPattern.FindString(chunk))
		if err != nil {
			return universityPerformanceVenuesDatePart{err: err}
		}
		return universityPerformanceVenuesDatePart{year: year.Year(), ignore: true}
	}
	match := universityPerformanceVenuesDateChunkPattern.FindStringSubmatch(chunk)
	if len(match) == 0 {
		return universityPerformanceVenuesDatePart{ignore: true}
	}

	dayText := match[1]
	monthText := match[2]
	if dayText == "" {
		dayText = match[4]
		monthText = match[3]
	}
	day, err := time.Parse("2", dayText)
	if err != nil {
		return universityPerformanceVenuesDatePart{err: err}
	}

	month := time.Month(0)
	if monthText != "" {
		month, err = universityPerformanceVenuesParseMonth(monthText)
		if err != nil {
			return universityPerformanceVenuesDatePart{err: err}
		}
	}

	year := 0
	if match[5] != "" {
		parsed, err := time.Parse("2006", match[5])
		if err != nil {
			return universityPerformanceVenuesDatePart{err: err}
		}
		year = parsed.Year()
	}

	return universityPerformanceVenuesDatePart{
		day:   day.Day(),
		month: month,
		year:  year,
	}
}

func universityPerformanceVenuesParseMonth(value string) (time.Month, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return 0, fmt.Errorf("missing month")
	}
	for _, layout := range []string{"January", "Jan"} {
		parsed, err := time.Parse(layout, strings.Title(value))
		if err == nil {
			return parsed.Month(), nil
		}
	}
	return 0, fmt.Errorf("unsupported month %q", value)
}

func universityPerformanceVenuesNextMonth(parts []universityPerformanceVenuesDatePart, start int) time.Month {
	for i := start; i < len(parts); i++ {
		if parts[i].month != 0 {
			return parts[i].month
		}
	}
	return 0
}

func universityPerformanceVenuesPreviousMonth(parts []universityPerformanceVenuesDatePart, start int) time.Month {
	for i := start; i >= 0; i-- {
		if parts[i].month != 0 {
			return parts[i].month
		}
	}
	return 0
}

func universityPerformanceVenuesParseClock(value string) (int, int, bool) {
	value = strings.ToLower(strings.TrimSpace(html.UnescapeString(value)))
	if value == "" {
		return 0, 0, false
	}
	value = strings.TrimSpace(strings.TrimPrefix(value, "doors "))
	value = strings.TrimSpace(strings.TrimPrefix(value, "from "))
	value = strings.ReplaceAll(value, ".", ":")
	match := universityPerformanceVenuesTimePattern.FindString(value)
	if match == "" {
		return 0, 0, false
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(match), " "))
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return 0, 0, false
	}
	for _, layout := range []string{"3:04 pm", "3:04pm", "3 pm", "3pm", "15:04"} {
		parsed, err := time.ParseInLocation(layout, normalized, loc)
		if err == nil {
			return parsed.Hour(), parsed.Minute(), true
		}
	}
	return 0, 0, false
}

func universityPerformanceVenuesTicketURL(pageURL string, raw []byte) string {
	base, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return ""
	}

	matches := universityPerformanceVenuesAnchorPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		href := strings.TrimSpace(html.UnescapeString(string(match[1])))
		if href == "" {
			continue
		}
		text := strings.ToLower(strings.Join(strings.Fields(universityPerformanceVenuesAnchorText(string(match[2]))), " "))
		if !strings.Contains(text, "book now") && !strings.Contains(text, "buy tickets") && !strings.Contains(text, "tickets") {
			continue
		}
		resolved, err := url.Parse(href)
		if err != nil {
			continue
		}
		if !resolved.IsAbs() {
			resolved = base.ResolveReference(resolved)
		}
		resolved.Fragment = ""
		resolved.RawFragment = ""
		if resolved.String() != "" {
			return resolved.String()
		}
	}
	return ""
}

func universityPerformanceVenuesAnchorText(raw string) string {
	return semanticInlineText(raw)
}
