package ingest

import (
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const alderVenueName = "Alder"

var (
	alderAnchorPattern          = regexp.MustCompile(`(?is)<a\b[^>]*>`)
	alderFastbootShoeboxPattern = regexp.MustCompile(`(?is)<script\b[^>]*type\s*=\s*["']fastboot/shoebox["'][^>]*>(.*?)</script>`)
	alderTicketpassPagePattern  = regexp.MustCompile(`(?is)<div\b[^>]*id\s*=\s*["']app["'][^>]*data-page\s*=\s*["']([^"']+)["']`)
	alderMusicPositivePattern   = regexp.MustCompile(`(?i)\b(?:live music|gig|gigs|band|bands|festival|festivals|banger|bangers|playing alder|concert|concerts|dj|djs|club night|club nights|album launch|tour|touring)\b`)
	alderMusicNegativePattern   = regexp.MustCompile(`(?i)\b(?:spoken word|folktale|folktales|myth|myths|legend|legends|comedy|film|screening|theatre|theater|workshop|market|markets|quiz|talk|lecture|community|private hire|poetry|storytelling)\b`)
	alderNonAlnumPattern        = regexp.MustCompile(`[^a-z0-9]+`)
)

func alder_listing_links(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	links := make([]string, 0, limit)
	for _, match := range alderAnchorPattern.FindAll(body, -1) {
		attrs := parseHTMLAttributes(string(match))
		resolved, ok := alderListingLinkURL(parsedBase, attrs["href"])
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

func ParseAlderEventDetailPage(pageURL string, raw []byte) ParseResult {
	candidate, skip, err := alderEventCandidate(pageURL, raw)
	if err != nil {
		return ParseResult{Errors: []string{err.Error()}}
	}
	if skip.Reason != "" {
		return ParseResult{Skips: []ParseSkip{skip}}
	}
	return ParseResult{Candidates: []EventCandidate{candidate}}
}

func alderEventCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	parsed, err := url.Parse(strings.TrimSpace(pageURL))
	if err != nil {
		return EventCandidate{}, ParseSkip{}, err
	}

	switch strings.ToLower(strings.TrimPrefix(strings.TrimSpace(parsed.Hostname()), "www.")) {
	case "eventbrite.com", "eventbrite.co.uk":
		return alderEventbriteCandidate(pageURL, raw)
	case "fatsoma.com":
		return alderFatsomaCandidate(pageURL, raw)
	case "ticketpass.org":
		return alderTicketpassCandidate(pageURL, raw)
	default:
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("unsupported Alder detail page host %q", parsed.Hostname())
	}
}

func alderListingLinkURL(base *url.URL, raw string) (string, bool) {
	raw = strings.TrimSpace(html.UnescapeString(raw))
	if raw == "" {
		return "", false
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	resolved.RawQuery = ""
	resolved.Fragment = ""
	resolved.RawFragment = ""

	host := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(resolved.Hostname()), "www."))
	path := strings.TrimSuffix(strings.TrimSpace(resolved.EscapedPath()), "/")
	switch host {
	case "eventbrite.com", "eventbrite.co.uk":
		if !alderEventbritePathPattern.MatchString(path) {
			return "", false
		}
	case "fatsoma.com":
		if !alderFatsomaPathPattern.MatchString(path) {
			return "", false
		}
	case "ticketpass.org":
		if !alderTicketpassPathPattern.MatchString(path) {
			return "", false
		}
	default:
		return "", false
	}

	return resolved.String(), true
}

var (
	alderEventbritePathPattern = regexp.MustCompile(`^/e/[^/?#]+/?$`)
	alderFatsomaPathPattern    = regexp.MustCompile(`^/e/[^/?#]+/[^/?#]+/?$`)
	alderTicketpassPathPattern = regexp.MustCompile(`^/event/[^/?#]+/[^/?#]+/?$`)
)

func alderEventbriteCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	nodes, found, err := alderParseJSONLDScripts(raw)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, err
	}
	if !found {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("no schema.org event data found")
	}

	for _, node := range nodes {
		candidate, skip, ok, err := alderEventbriteCandidateFromNode(pageURL, node)
		if err != nil {
			return EventCandidate{}, ParseSkip{}, err
		}
		if !ok {
			continue
		}
		if skip.Reason != "" {
			return EventCandidate{}, skip, nil
		}
		return candidate, ParseSkip{}, nil
	}

	return EventCandidate{}, ParseSkip{UID: pageURL, Reason: "missing event data"}, nil
}

func alderEventbriteCandidateFromNode(pageURL string, node map[string]any) (EventCandidate, ParseSkip, bool, error) {
	title := alderJSONString(node["name"])
	skip := ParseSkip{UID: pageURL, Summary: title}
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, true, nil
	}

	locationName, locationAddress, ok := alderJSONLDLocation(node["location"])
	if !ok {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, true, nil
	}

	startText := alderJSONString(node["startDate"])
	if startText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, true, nil
	}
	endText := alderJSONString(node["endDate"])

	description := alderNormalizedDescription(alderJSONString(node["description"]))
	performers := alderJSONLDText(node["performer"])
	organizer := alderJSONLDOrganizerText(node["organizer"])
	musicText := strings.Join([]string{title, description, performers, organizer}, "\n")
	if !alderHasMusicSignal(musicText) || alderHasNonMusicSignal(musicText) {
		skip.Reason = "non-music event"
		return EventCandidate{}, skip, true, nil
	}

	startAt, err := alderParseDateTime(startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, fmt.Errorf("parse Alder start time for %q: %w", title, err)
	}
	endAt, err := alderOptionalEndAt(title, endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, false, err
	}

	location, locationRaw, ok := alderVenueEvidence(locationName, locationAddress)
	if !ok {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, true, nil
	}

	return EventCandidate{
		UID:         pageURL,
		Summary:     title,
		Description: description,
		Location:    location,
		LocationRaw: locationRaw,
		URL:         pageURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
		EndAt:       endAt,
	}, ParseSkip{}, true, nil
}

func alderFatsomaCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	payloads, found, err := alderDecodeFastbootShoeboxPayloads(raw)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, err
	}
	if !found {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("no Alder Fatsoma payload found")
	}

	for _, payload := range payloads {
		event, included := alderFatsomaEventAndIncluded(payload)
		if event == nil {
			continue
		}

		attrs, _ := event["attributes"].(map[string]any)
		title := alderJSONString(attrs["name"])
		skip := ParseSkip{UID: pageURL, Summary: title}
		if title == "" {
			skip.Reason = "missing event title"
			return EventCandidate{}, skip, nil
		}

		startText := alderJSONString(attrs["starts-at"])
		if startText == "" {
			skip.Reason = "missing event start time"
			return EventCandidate{}, skip, nil
		}
		endText := alderJSONString(attrs["ends-at"])

		locationName, locationAddress := alderFatsomaLocation(included, event)
		location, locationRaw, ok := alderVenueEvidence(locationName, locationAddress)
		if !ok {
			skip.Reason = "unsupported venue"
			return EventCandidate{}, skip, nil
		}

		description := alderNormalizedDescription(alderJSONString(attrs["description"]))
		announcement := alderNormalizedText(alderJSONString(attrs["announcement-message"]))
		category := alderFatsomaCategoryName(included, event)
		musicText := strings.Join([]string{title, description, announcement, category}, "\n")
		if !alderHasMusicSignal(musicText) || alderHasNonMusicSignal(musicText) {
			skip.Reason = "non-music event"
			return EventCandidate{}, skip, nil
		}

		startAt, err := alderParseDateTime(startText)
		if err != nil {
			return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Alder start time for %q: %w", title, err)
		}
		endAt, err := alderOptionalEndAt(title, endText)
		if err != nil {
			return EventCandidate{}, ParseSkip{}, err
		}

		return EventCandidate{
			UID:         pageURL,
			Summary:     title,
			Description: description,
			Location:    location,
			LocationRaw: locationRaw,
			URL:         pageURL,
			Status:      "Listed",
			StartAt:     formatTime(startAt),
			EndAt:       endAt,
		}, ParseSkip{}, nil
	}

	return EventCandidate{}, ParseSkip{}, fmt.Errorf("no Alder Fatsoma event data found")
}

func alderTicketpassCandidate(pageURL string, raw []byte) (EventCandidate, ParseSkip, error) {
	payload, found, err := alderDecodeTicketpassPayload(raw)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, err
	}
	if !found {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("no Alder Ticketpass payload found")
	}

	props, _ := payload["props"].(map[string]any)
	event, _ := props["event"].(map[string]any)
	if event == nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("no Alder Ticketpass event data found")
	}

	title := alderJSONString(event["name"])
	skip := ParseSkip{UID: pageURL, Summary: title}
	if title == "" {
		skip.Reason = "missing event title"
		return EventCandidate{}, skip, nil
	}

	dates, _ := event["eventDates"].([]any)
	if len(dates) == 0 {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	}
	firstDate, _ := dates[0].(map[string]any)
	startText := alderJSONString(firstDate["startTime"])
	if startText == "" {
		skip.Reason = "missing event start time"
		return EventCandidate{}, skip, nil
	}
	endText := alderJSONString(firstDate["endTime"])

	venue, _ := event["venue"].(map[string]any)
	locationName := alderJSONString(venue["name"])
	locationAddress := alderJSONString(venue["address"])
	location, locationRaw, ok := alderVenueEvidence(locationName, locationAddress)
	if !ok {
		skip.Reason = "unsupported venue"
		return EventCandidate{}, skip, nil
	}

	description := alderNormalizedDescription(alderJSONString(event["description"]))
	musicText := strings.Join([]string{title, description, alderNormalizedText(locationName)}, "\n")
	if !alderHasMusicSignal(musicText) || alderHasNonMusicSignal(musicText) {
		skip.Reason = "non-music event"
		return EventCandidate{}, skip, nil
	}

	startAt, err := alderParseDateTime(startText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, fmt.Errorf("parse Alder start time for %q: %w", title, err)
	}
	endAt, err := alderOptionalEndAt(title, endText)
	if err != nil {
		return EventCandidate{}, ParseSkip{}, err
	}

	return EventCandidate{
		UID:         pageURL,
		Summary:     title,
		Description: description,
		Location:    location,
		LocationRaw: locationRaw,
		URL:         pageURL,
		Status:      "Listed",
		StartAt:     formatTime(startAt),
		EndAt:       endAt,
	}, ParseSkip{}, nil
}

func alderParseJSONLDScripts(raw []byte) ([]map[string]any, bool, error) {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil, false, nil
	}

	var nodes []map[string]any
	found := false
	for _, match := range matches {
		parsed, ok, err := alderParseJSONLDScript(match[1])
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		found = true
		nodes = append(nodes, parsed...)
	}
	return nodes, found, nil
}

func alderParseJSONLDScript(raw []byte) ([]map[string]any, bool, error) {
	text := strings.TrimSpace(html.UnescapeString(string(raw)))
	if text == "" {
		return nil, false, nil
	}

	var payload any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return nil, false, fmt.Errorf("decode Alder JSON-LD: %w", err)
	}

	var nodes []map[string]any
	alderCollectJSONLDEventNodes(payload, &nodes)
	return nodes, len(nodes) > 0, nil
}

func alderCollectJSONLDEventNodes(value any, nodes *[]map[string]any) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			alderCollectJSONLDEventNodes(item, nodes)
		}
	case map[string]any:
		if graph, ok := typed["@graph"]; ok {
			alderCollectJSONLDEventNodes(graph, nodes)
		}
		if alderJSONLDNodeHasEventType(typed["@type"]) {
			*nodes = append(*nodes, typed)
		}
	}
}

func alderJSONLDNodeHasEventType(value any) bool {
	return yellowArchNodeHasType(value, "Event") || yellowArchNodeHasType(value, "Festival")
}

func alderJSONString(value any) string {
	return alderNormalizedText(yellowArchJSONString(value))
}

func alderNormalizedText(value string) string {
	value = html.UnescapeString(strings.TrimSpace(value))
	return strings.Join(strings.Fields(value), " ")
}

func alderNormalizedDescription(value string) string {
	return semanticDescriptionText(value)
}

func alderVenueEvidence(name, address string) (string, string, bool) {
	name = alderNormalizedText(name)
	if name == "" {
		return "", "", false
	}

	if !alderVenueNameEvidence(name) {
		return "", "", false
	}

	address = alderNormalizedText(address)
	if !alderVenueAddressEvidence(address) {
		return "", "", false
	}

	return alderVenueName, alderVenueName + ", " + address, true
}

func alderVenueNameEvidence(name string) bool {
	lower := strings.ToLower(name)
	if strings.ContainsAny(lower, "/|") || strings.Contains(lower, " and ") {
		return false
	}

	switch alderTokenText(name) {
	case "alder", "alder bar", "alder sheffield", "alder bar sheffield":
		return true
	default:
		return false
	}
}

func alderVenueAddressEvidence(address string) bool {
	if address == "" {
		return false
	}

	tokens := alderTokenText(address)
	compact := strings.ReplaceAll(tokens, " ", "")
	hasPercyStreet := strings.Contains(tokens, "percy street") || strings.Contains(tokens, "percy st")
	hasPostcode := strings.Contains(compact, "s38bt")
	hasVenueMarker := strings.Contains(compact, "unit111") ||
		strings.Contains(compact, "unit112") ||
		strings.Contains(compact, "111112") ||
		strings.Contains(compact, "jcalbyn") ||
		strings.Contains(tokens, "neepsend")
	return hasPercyStreet && hasPostcode && hasVenueMarker
}

func alderTokenText(value string) string {
	lower := strings.ToLower(alderNormalizedText(value))
	return strings.Join(strings.Fields(alderNonAlnumPattern.ReplaceAllString(lower, " ")), " ")
}

func alderParseDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
	}

	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime %q", value)
}

func alderOptionalEndAt(title, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	endAt, err := alderParseDateTime(value)
	if err != nil {
		return "", fmt.Errorf("parse Alder end time for %q: %w", title, err)
	}
	return formatTime(endAt), nil
}

func alderHasMusicSignal(value string) bool {
	return alderMusicPositivePattern.MatchString(strings.ToLower(strings.TrimSpace(value)))
}

func alderHasNonMusicSignal(value string) bool {
	return alderMusicNegativePattern.MatchString(strings.ToLower(strings.TrimSpace(value)))
}

func alderDecodeFastbootShoeboxPayloads(raw []byte) ([]map[string]any, bool, error) {
	matches := alderFastbootShoeboxPattern.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil, false, nil
	}

	payloads := make([]map[string]any, 0, len(matches))
	found := false
	for _, match := range matches {
		text := strings.TrimSpace(html.UnescapeString(string(match[1])))
		if text == "" {
			continue
		}

		var encoded string
		if err := json.Unmarshal([]byte(text), &encoded); err == nil {
			text = encoded
		}

		var payload any
		if err := json.Unmarshal([]byte(text), &payload); err != nil {
			continue
		}

		if data, ok := payload.(map[string]any); ok {
			payloads = append(payloads, data)
			found = true
		}
	}

	return payloads, found, nil
}

func alderFatsomaEventAndIncluded(payload map[string]any) (map[string]any, map[string]map[string]any) {
	included := make(map[string]map[string]any)
	if rawIncluded, ok := payload["included"].([]any); ok {
		for _, item := range rawIncluded {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ := alderJSONString(obj["type"])
			id := alderJSONString(obj["id"])
			if typ == "" || id == "" {
				continue
			}
			included[typ+"\x00"+id] = obj
		}
	}

	var event map[string]any
	if data, ok := payload["data"].([]any); ok {
		for _, item := range data {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if alderJSONString(obj["type"]) == "events" {
				event = obj
				break
			}
		}
	}
	return event, included
}

func alderFatsomaCategoryName(included map[string]map[string]any, event map[string]any) string {
	relationships, _ := event["relationships"].(map[string]any)
	categories, _ := relationships["categories"].(map[string]any)
	data, _ := categories["data"].([]any)
	for _, item := range data {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key := alderJSONString(obj["type"]) + "\x00" + alderJSONString(obj["id"])
		if category := included[key]; category != nil {
			if attrs, ok := category["attributes"].(map[string]any); ok {
				if name := alderJSONString(attrs["name"]); name != "" {
					return name
				}
			}
		}
	}
	return ""
}

func alderFatsomaLocation(included map[string]map[string]any, event map[string]any) (string, string) {
	relationships, _ := event["relationships"].(map[string]any)
	location, _ := relationships["location"].(map[string]any)
	data, _ := location["data"].(map[string]any)
	key := alderJSONString(data["type"]) + "\x00" + alderJSONString(data["id"])
	if raw := included[key]; raw != nil {
		if attrs, ok := raw["attributes"].(map[string]any); ok {
			return alderJSONString(attrs["name"]), alderJSONString(attrs["address"])
		}
	}
	return "", ""
}

func alderJSONLDLocation(value any) (string, string, bool) {
	switch typed := value.(type) {
	case string:
		name := alderJSONString(typed)
		if name == "" {
			return "", "", false
		}
		return name, "", true
	case map[string]any:
		name := alderJSONString(typed["name"])
		if name == "" {
			return "", "", false
		}
		return name, alderJSONLDAddress(typed["address"]), true
	default:
		return "", "", false
	}
}

func alderJSONLDAddress(value any) string {
	switch typed := value.(type) {
	case string:
		return alderJSONString(typed)
	case map[string]any:
		parts := make([]string, 0, 5)
		for _, key := range []string{"streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"} {
			if part := alderJSONString(typed[key]); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, ", ")
	default:
		return ""
	}
}

func alderJSONLDText(value any) string {
	switch typed := value.(type) {
	case string:
		return alderJSONString(typed)
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := alderJSONLDText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		parts := make([]string, 0, 2)
		for _, key := range []string{"name", "description"} {
			if text := alderJSONString(typed[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func alderJSONLDOrganizerText(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		parts := make([]string, 0, 2)
		for _, key := range []string{"name", "description"} {
			if text := alderJSONString(typed[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return alderJSONLDText(value)
	}
}

func alderDecodeTicketpassPayload(raw []byte) (map[string]any, bool, error) {
	match := alderTicketpassPagePattern.FindStringSubmatch(string(raw))
	if len(match) < 2 {
		return nil, false, nil
	}

	text := strings.TrimSpace(html.UnescapeString(match[1]))
	if text == "" {
		return nil, false, nil
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return nil, true, fmt.Errorf("decode Alder Ticketpass page: %w", err)
	}
	return payload, true, nil
}
