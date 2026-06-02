package ingest

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const theWashingtonVenueName = "The Washington"
const theWashingtonOfficialCalendarID = "c_u2bs6ittml6rm5k0l5qjt3pn1o@group.calendar.google.com"

var theWashingtonCalendarIDPattern = regexp.MustCompile(`(?i)googleCalendarId\s*:\s*['"]([^'"]+)['"]`)
var theWashingtonCalendarAPIKeyPattern = regexp.MustCompile(`(?i)googleCalendarApiKey\s*:\s*['"]([^'"]+)['"]`)
var theWashingtonVenuePattern = regexp.MustCompile(`(?i)\bwashington\b`)
var theWashingtonMusicPositivePattern = regexp.MustCompile(`(?i)(?:\[(?:dj|live)\]|\blive\s*:|\bdj\b|\bdjs\b|\bgig\b|\bgigs\b|\blive music\b|\bband\b|\bbands\b|\bclub night\b|\bclub nights\b|\bopen mic\b|\balbum launch\b|\btour\b)`)
var theWashingtonMusicNegativePattern = regexp.MustCompile(`(?i)\b(?:quiz|comedy|film|screening|theatre|theater|workshop|market|talk|lecture|private hire|community)\b`)
var theWashingtonNow = time.Now

func the_washington_api_links(pageURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	id := theWashingtonCalendarID(body)
	if id == "" {
		return nil, fmt.Errorf("no Google Calendar ID found on The Washington calendar page")
	}
	if id != theWashingtonOfficialCalendarID {
		return nil, fmt.Errorf("unexpected The Washington Google Calendar ID %q", id)
	}
	key := theWashingtonCalendarAPIKey(body)
	if key == "" {
		return nil, fmt.Errorf("no Google Calendar API key found on The Washington calendar page")
	}

	apiURL := theWashingtonCalendarAPIURL(id, key, limit, theWashingtonNow())
	return []string{apiURL}, nil
}

func ParseTheWashingtonAPIDetailPage(pageURL string, raw []byte) ParseResult {
	var payload theWashingtonAPIFeed
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ParseResult{Errors: []string{fmt.Sprintf("decode The Washington calendar API response: %v", err)}}
	}

	candidateLimit := theWashingtonCalendarURLMaxResults(pageURL)
	requestedTimeMin, hasRequestedTimeMin := theWashingtonCalendarURLTimeMin(pageURL)
	sourceVenueEvidence := theWashingtonHasOfficialCalendarSourceEvidence(pageURL)
	var result ParseResult
	for _, item := range payload.Items {
		candidate, skip, ok, err := theWashingtonCandidateFromAPI(pageURL, item, sourceVenueEvidence)
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
		if hasRequestedTimeMin {
			startAt, err := time.Parse(time.RFC3339, candidate.StartAt)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("parse The Washington normalized start time for %q: %v", candidate.Summary, err))
				continue
			}
			if startAt.Before(requestedTimeMin) {
				result.Skips = append(result.Skips, ParseSkip{UID: candidate.UID, Summary: candidate.Summary, Reason: "event before requested time window"})
				continue
			}
		}
		if candidateLimit > 0 && len(result.Candidates) >= candidateLimit {
			result.Skips = append(result.Skips, ParseSkip{UID: candidate.UID, Summary: candidate.Summary, Reason: "candidate limit reached"})
			continue
		}
		result.Candidates = append(result.Candidates, candidate)
	}
	return result
}

func theWashingtonCalendarID(body []byte) string {
	match := theWashingtonCalendarIDPattern.FindSubmatch(body)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func theWashingtonCalendarAPIKey(body []byte) string {
	match := theWashingtonCalendarAPIKeyPattern.FindSubmatch(body)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func theWashingtonCalendarAPIURL(calendarID, apiKey string, limit int, now time.Time) string {
	if limit <= 0 {
		limit = 1
	}
	return "https://www.googleapis.com/calendar/v3/calendars/" +
		url.QueryEscape(strings.TrimSpace(calendarID)) +
		"/events?key=" + url.QueryEscape(strings.TrimSpace(apiKey)) +
		"&singleEvents=true&orderBy=startTime&maxResults=" + fmt.Sprintf("%d", limit) +
		"&timeMin=" + url.QueryEscape(theWashingtonCalendarTimeMin(now))
}

func theWashingtonCalendarTimeMin(now time.Time) string {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		loc = time.UTC
	}
	local := now.In(loc)
	startOfLocalDay := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
	return startOfLocalDay.UTC().Format(time.RFC3339)
}

func theWashingtonCalendarURLMaxResults(pageURL string) int {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return 0
	}
	maxResults, err := strconv.Atoi(strings.TrimSpace(parsed.Query().Get("maxResults")))
	if err != nil || maxResults <= 0 {
		return 0
	}
	return maxResults
}

func theWashingtonCalendarURLTimeMin(pageURL string) (time.Time, bool) {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return time.Time{}, false
	}
	timeMin := strings.TrimSpace(parsed.Query().Get("timeMin"))
	if timeMin == "" {
		return time.Time{}, false
	}
	parsedTimeMin, err := time.Parse(time.RFC3339, timeMin)
	if err != nil {
		return time.Time{}, false
	}
	return parsedTimeMin, true
}

func theWashingtonHasOfficialCalendarSourceEvidence(pageURL string) bool {
	return theWashingtonGoogleCalendarIDFromAPIURL(pageURL) == theWashingtonOfficialCalendarID
}

func theWashingtonGoogleCalendarIDFromAPIURL(pageURL string) string {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return ""
	}
	if !strings.EqualFold(parsed.Host, "www.googleapis.com") {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 5 {
		return ""
	}
	if parts[0] == "calendar" &&
		parts[1] == "v3" &&
		parts[2] == "calendars" &&
		parts[4] == "events" {
		return strings.TrimSpace(parts[3])
	}
	return ""
}

type theWashingtonAPIFeed struct {
	Items []theWashingtonAPIEvent `json:"items"`
}

type theWashingtonAPIEvent struct {
	ICalUID     string `json:"iCalUID"`
	ID          string `json:"id"`
	HtmlLink    string `json:"htmlLink"`
	Summary     string `json:"summary"`
	Description string `json:"description"`
	Location    string `json:"location"`
	Status      string `json:"status"`
	Start       struct {
		DateTime string `json:"dateTime"`
		Date     string `json:"date"`
	} `json:"start"`
	End struct {
		DateTime string `json:"dateTime"`
		Date     string `json:"date"`
	} `json:"end"`
}

func theWashingtonCandidateFromAPI(pageURL string, item theWashingtonAPIEvent, sourceVenueEvidence bool) (EventCandidate, ParseSkip, bool, error) {
	title := strings.TrimSpace(item.Summary)
	uid := theWashingtonOccurrenceUID(pageURL, item)
	skip := ParseSkip{UID: uid, Summary: title}
	if title == "" {
		skip.Reason = "missing event name"
		return EventCandidate{}, skip, true, nil
	}
	if strings.EqualFold(strings.TrimSpace(item.Status), "cancelled") {
		skip.Reason = "cancelled"
		return EventCandidate{}, skip, true, nil
	}

	startText := strings.TrimSpace(item.Start.DateTime)
	if startText == "" {
		skip.Reason = "all-day event"
		return EventCandidate{}, skip, true, nil
	}
	startAt, err := time.Parse(time.RFC3339, startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse The Washington start time for %q: %w", title, err)
	}

	endText := strings.TrimSpace(item.End.DateTime)
	var endAt string
	if endText != "" {
		parsedEnd, err := time.Parse(time.RFC3339, endText)
		if err != nil {
			return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse The Washington end time for %q: %w", title, err)
		}
		endAt = formatTime(parsedEnd)
	}

	rawLocation := strings.TrimSpace(item.Location)
	if rawLocation == "" {
		if !sourceVenueEvidence {
			skip.Reason = "missing venue evidence"
			return EventCandidate{}, skip, true, nil
		}
	} else if !theWashingtonVenuePattern.MatchString(rawLocation) {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, true, nil
	}

	officialURL := strings.TrimSpace(item.HtmlLink)
	if officialURL == "" {
		skip.Reason = "missing event URL"
		return EventCandidate{}, skip, true, nil
	}

	musicText := strings.Join([]string{title, item.Description}, "\n")
	if !theWashingtonHasMusicSignal(musicText) || theWashingtonHasNonMusicSignal(musicText) {
		skip.Reason = "non-music event"
		return EventCandidate{}, skip, true, nil
	}

	return EventCandidate{
		UID:         uid,
		Summary:     title,
		Description: semanticDescriptionText(item.Description),
		Location:    theWashingtonVenueName,
		LocationRaw: rawLocation,
		URL:         officialURL,
		Status:      "CONFIRMED",
		StartAt:     formatTime(startAt),
		EndAt:       endAt,
	}, ParseSkip{}, true, nil
}

func theWashingtonOccurrenceUID(pageURL string, item theWashingtonAPIEvent) string {
	return GoogleCalendarOccurrenceUID(theWashingtonGoogleCalendarIDFromAPIURL(pageURL), item.ID, item.HtmlLink, item.ICalUID)
}

func theWashingtonVenueSlugFromText(value string) string {
	if theWashingtonVenuePattern.MatchString(value) {
		return TheWashingtonSource
	}
	return VenueSlugFromText(value)
}

func theWashingtonHasMusicSignal(value string) bool {
	return theWashingtonMusicPositivePattern.MatchString(strings.TrimSpace(value))
}

func theWashingtonHasNonMusicSignal(value string) bool {
	return theWashingtonMusicNegativePattern.MatchString(strings.TrimSpace(value))
}
