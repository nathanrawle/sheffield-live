package ingest

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
)

const networkSheffieldVenueSlug = "network-sheffield"

var networkSheffieldDetailPathPattern = regexp.MustCompile(`^/event/[^/]+/?$`)
var networkSheffieldRoomPattern = regexp.MustCompile(`(?i)\bnetwork\s*([123])\b`)
var networkSheffieldVenuePattern = regexp.MustCompile(`(?i)\bnetwork sheffield\b|\bnetwork\b`)
var networkSheffieldWholeVenuePattern = regexp.MustCompile(`(?i)\b(all\s+rooms|whole\s+venue|[123]\s+stages?|three\s+stages?)\b`)

func network_sheffield_detail_links(baseURL string, body []byte, limit int) ([]string, error) {
	links, err := ExtractSameHostLinks(baseURL, body, limit, func(u *url.URL) bool {
		return networkSheffieldDetailPathPattern.MatchString(strings.TrimSpace(u.EscapedPath()))
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

func ParseNetworkSheffieldDetailPage(pageURL string, raw []byte) ParseResult {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		return ParseResult{Errors: []string{"no application/ld+json event data found"}}
	}

	var result ParseResult
	foundEventData := false
	for _, match := range matches {
		nodes, found, err := parseYellowArchJSONLDScript(match[1])
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			continue
		}
		if !found {
			continue
		}
		foundEventData = true
		for _, node := range nodes {
			candidate, skip, ok, err := networkSheffieldCandidateFromNode(pageURL, node)
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
	}

	if !foundEventData {
		result.Errors = append(result.Errors, "no schema.org Event objects found in application/ld+json")
	}
	return result
}

func networkSheffieldCandidateFromNode(pageURL string, node map[string]any) (EventCandidate, ParseSkip, bool, error) {
	title := yellowArchJSONString(node["name"])
	skip := ParseSkip{UID: strings.TrimSpace(pageURL), Summary: title}
	if title == "" {
		skip.Reason = "missing event name"
		return EventCandidate{}, skip, true, nil
	}

	startText := yellowArchJSONString(node["startDate"])
	if startText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, true, nil
	}
	endText := yellowArchJSONString(node["endDate"])
	if endText == "" {
		skip.Reason = "missing event end time"
		return EventCandidate{}, skip, true, nil
	}

	locationName := networkSheffieldLocationName(node["location"])
	venueText, roomText, rooms, ok := networkSheffieldVenueEvidence(title, locationName)
	if !ok {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, true, nil
	}

	startAt, err := parseYellowArchDateTime(startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse Network Sheffield start time for %q: %w", title, err)
	}
	endAt, err := parseYellowArchDateTime(endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse Network Sheffield end time for %q: %w", title, err)
	}

	officialURL := strings.TrimSpace(pageURL)
	externalURL := yellowArchJSONString(node["url"])
	if externalURL == "" {
		externalURL = officialURL
	}

	return EventCandidate{
		UID:            externalURL,
		Summary:        title,
		Description:    semanticDescriptionText(yellowArchJSONString(node["description"])),
		Location:       venueText,
		LocationRaw:    networkSheffieldLocationRaw(venueText, node["location"]),
		RoomText:       roomText,
		Rooms:          rooms,
		URL:            officialURL,
		ImageSourceURL: jsonLDImageURL(node["image"]),
		ImageAlt:       title,
		Status:         "Listed",
		StartAt:        formatTime(startAt),
		EndAt:          formatTime(endAt),
	}, ParseSkip{}, true, nil
}

func networkSheffieldVenueEvidence(title, locationName string) (string, string, []RoomCandidate, bool) {
	locationName = strings.TrimSpace(locationName)
	title = strings.TrimSpace(title)

	if networkSheffieldRejectedVenueText(locationName) || networkSheffieldRejectedVenueText(title) {
		return "", "", nil, false
	}

	if room := networkSheffieldRoomText(title); room != "" {
		return room, room, []RoomCandidate{{Slug: networkSheffieldRoomSlug(room), Name: room}}, true
	}
	if room := networkSheffieldRoomText(locationName); room != "" {
		return room, room, []RoomCandidate{{Slug: networkSheffieldRoomSlug(room), Name: room}}, true
	}

	if venue := networkSheffieldVenueText(locationName); venue != "" {
		return venue, "", nil, true
	}
	if venue := networkSheffieldVenueText(title); venue != "" {
		return venue, "", nil, true
	}
	return "", "", nil, false
}

func networkSheffieldVenueText(value string) string {
	text := networkSheffieldNormalizedText(value)
	switch {
	case text == "":
		return ""
	case strings.Contains(text, "network sheffield"):
		return "Network Sheffield"
	case networkSheffieldVenuePattern.MatchString(text):
		return "Network"
	default:
		return ""
	}
}

func networkSheffieldVenueSlugFromText(value string) string {
	if networkSheffieldRejectedVenueText(value) {
		return ""
	}
	if networkSheffieldRoomText(value) != "" {
		return networkSheffieldVenueSlug
	}
	if networkSheffieldVenueText(value) != "" {
		return networkSheffieldVenueSlug
	}
	return ""
}

func networkSheffieldRoomText(value string) string {
	text := networkSheffieldNormalizedText(value)
	if networkSheffieldWholeVenuePattern.MatchString(text) {
		return ""
	}
	match := networkSheffieldRoomPattern.FindStringSubmatch(text)
	if len(match) != 2 {
		return ""
	}
	return "Network " + match[1]
}

func networkSheffieldRoomSlug(roomText string) string {
	switch networkSheffieldRoomText(roomText) {
	case "Network 1":
		return "network-1"
	case "Network 2":
		return "network-2"
	case "Network 3":
		return "network-3"
	default:
		return ""
	}
}

func networkSheffieldRejectedVenueText(value string) bool {
	text := networkSheffieldNormalizedText(value)
	switch {
	case text == "":
		return false
	case strings.Contains(text, "earl's yard"):
		return true
	case strings.Contains(text, "earls yard"):
		return true
	case strings.Contains(text, "arundel emporium"):
		return true
	case strings.Contains(text, "record junkee"):
		return true
	default:
		return false
	}
}

func networkSheffieldNormalizedText(value string) string {
	return strings.ToLower(normalizeEventTitleSpacing(html.UnescapeString(strings.TrimSpace(value))))
}

func networkSheffieldLocationName(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case map[string]any:
		return yellowArchJSONString(typed["name"])
	default:
		return ""
	}
}

func networkSheffieldLocationRaw(venueText string, value any) string {
	venueText = strings.TrimSpace(venueText)
	address := networkSheffieldAddressText(value)
	switch {
	case venueText == "":
		return address
	case address == "":
		return venueText
	default:
		return venueText + ", " + address
	}
}

func networkSheffieldAddressText(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case map[string]any:
		if address := networkSheffieldAddressText(typed["address"]); address != "" {
			return address
		}
		if streetAddress := yellowArchJSONString(typed["streetAddress"]); streetAddress != "" {
			return streetAddress
		}
		parts := make([]string, 0, 5)
		for _, key := range []string{"addressLocality", "addressRegion", "postalCode", "addressCountry"} {
			if part := yellowArchJSONString(typed[key]); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, ", ")
	default:
		return ""
	}
}
