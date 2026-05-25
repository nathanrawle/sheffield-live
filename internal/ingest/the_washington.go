package ingest

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const theWashingtonVenueName = "The Washington"

var theWashingtonCalendarIDPattern = regexp.MustCompile(`(?i)googleCalendarId\s*:\s*['"]([^'"]+)['"]`)
var theWashingtonCalendarAPIKeyPattern = regexp.MustCompile(`(?i)googleCalendarApiKey\s*:\s*['"]([^'"]+)['"]`)
var theWashingtonVenuePattern = regexp.MustCompile(`(?i)\bwashington\b`)
var theWashingtonMusicPositivePattern = regexp.MustCompile(`(?i)(?:\[(?:dj|live)\]|\blive\s*:|\bdj\b|\bdjs\b|\bgig\b|\bgigs\b|\blive music\b|\bband\b|\bbands\b|\bclub night\b|\bclub nights\b|\bopen mic\b|\balbum launch\b|\btour\b)`)
var theWashingtonMusicNegativePattern = regexp.MustCompile(`(?i)\b(?:quiz|comedy|film|screening|theatre|theater|workshop|market|talk|lecture|private hire|community)\b`)

func the_washington_api_links(pageURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	id := theWashingtonCalendarID(body)
	if id == "" {
		return nil, fmt.Errorf("no Google Calendar ID found on The Washington calendar page")
	}
	key := theWashingtonCalendarAPIKey(body)
	if key == "" {
		return nil, fmt.Errorf("no Google Calendar API key found on The Washington calendar page")
	}

	apiURL := theWashingtonCalendarAPIURL(id, key)
	return []string{apiURL}, nil
}

func ParseTheWashingtonAPIDetailPage(pageURL string, raw []byte) ParseResult {
	var payload theWashingtonAPIFeed
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ParseResult{Errors: []string{fmt.Sprintf("decode The Washington calendar API response: %v", err)}}
	}

	var result ParseResult
	for _, item := range payload.Items {
		candidate, skip, ok, err := theWashingtonCandidateFromAPI(pageURL, item)
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

func theWashingtonCalendarAPIURL(calendarID, apiKey string) string {
	return "https://www.googleapis.com/calendar/v3/calendars/" +
		url.QueryEscape(strings.TrimSpace(calendarID)) +
		"/events?key=" + url.QueryEscape(strings.TrimSpace(apiKey)) +
		"&singleEvents=true&orderBy=startTime&maxResults=2500&timeMin=2026-01-01T00:00:00Z"
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

func theWashingtonCandidateFromAPI(pageURL string, item theWashingtonAPIEvent) (EventCandidate, ParseSkip, bool, error) {
	title := strings.TrimSpace(item.Summary)
	skip := ParseSkip{UID: strings.TrimSpace(firstNonEmpty(item.ICalUID, item.ID)), Summary: title}
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
	if rawLocation != "" && !theWashingtonVenuePattern.MatchString(rawLocation) {
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

	locationText := theWashingtonVenueName
	if rawLocation == "" {
		rawLocation = theWashingtonVenueName
	}

	return EventCandidate{
		UID:         firstNonEmpty(item.ICalUID, item.ID, officialURL),
		Summary:     title,
		Description: semanticDescriptionText(item.Description),
		Location:    locationText,
		LocationRaw: rawLocation,
		URL:         officialURL,
		Status:      "CONFIRMED",
		StartAt:     formatTime(startAt),
		EndAt:       endAt,
	}, ParseSkip{}, true, nil
}

func theWashingtonHasMusicSignal(value string) bool {
	return theWashingtonMusicPositivePattern.MatchString(strings.TrimSpace(value))
}

func theWashingtonHasNonMusicSignal(value string) bool {
	return theWashingtonMusicNegativePattern.MatchString(strings.TrimSpace(value))
}
