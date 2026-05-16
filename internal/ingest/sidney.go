package ingest

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

var (
	anchorPattern             = regexp.MustCompile(`(?is)<a\b[^>]*?\bhref\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)[^>]*>(.*?)</a>`)
	sidneyEventArticlePattern = regexp.MustCompile(`(?is)<article\b[^>]*\beventlist-event\b[^>]*>(.*?)</article>`)
	sidneyEventExcerptPattern = regexp.MustCompile(`(?is)<div\b[^>]*\beventlist-excerpt\b[^>]*>(.*?)</div>`)
	sidneyExcerptParagraph    = regexp.MustCompile(`(?is)<p\b[^>]*>(.*?)</p>`)
	tagPattern                = regexp.MustCompile(`(?is)<[^>]+>`)
	spacePattern              = regexp.MustCompile(`\s+`)
)

func ExtractSidneyAndMatildaICSLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	matches := anchorPattern.FindAllSubmatch(body, -1)
	seen := make(map[string]bool)
	links := make([]string, 0, min(limit, len(matches)))
	for _, match := range matches {
		href := strings.TrimSpace(string(match[1]))
		href = strings.Trim(href, `"'`)
		label := anchorLabel(match[2])
		rawHref := html.UnescapeString(href)
		if !isSidneyAndMatildaICSLink(label, rawHref) {
			continue
		}

		resolved, err := resolveURL(parsedBase, rawHref)
		if err != nil || seen[resolved] {
			continue
		}
		seen[resolved] = true
		links = append(links, resolved)
		if len(links) >= limit {
			break
		}
	}
	return links, nil
}

func ExtractSidneyAndMatildaRoomEvidence(baseURL string, body []byte) map[string]sourceRoomEvidence {
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}
	out := make(map[string]sourceRoomEvidence)
	titleEvidence := make(map[string]sourceRoomEvidence)
	titleCounts := make(map[string]int)
	for _, article := range sidneyEventArticlePattern.FindAllSubmatch(body, -1) {
		detailURL, title := sidneyEventArticleIdentity(parsedBase, article[1])
		if detailURL == "" && title == "" {
			continue
		}
		text := sidneyEventArticleRoomText(article[1])
		evidence, ok := sidneyRoomEvidenceFromText(text)
		if !ok {
			continue
		}
		if detailURL != "" {
			out["url:"+strings.TrimRight(detailURL, "/")] = evidence
		}
		if title != "" {
			key := roomEvidenceTitleKey(title)
			titleEvidence[key] = evidence
			titleCounts[key]++
		}
	}
	for key, evidence := range titleEvidence {
		if titleCounts[key] == 1 {
			out[key] = evidence
		}
	}
	return out
}

func sidneyEventArticleIdentity(base *url.URL, raw []byte) (string, string) {
	var detailURL string
	var title string
	for _, match := range anchorPattern.FindAllSubmatch(raw, -1) {
		href := strings.Trim(strings.TrimSpace(string(match[1])), `"'`)
		label := anchorLabel(match[2])
		rawHref := html.UnescapeString(href)
		if !isSidneyAndMatildaEventDetailLink(label, rawHref) {
			continue
		}
		resolved, err := resolveURL(base, rawHref)
		if err != nil {
			continue
		}
		if detailURL == "" {
			detailURL = resolved
		}
		if title == "" && sidneyEventTitleCandidate(label) {
			title = label
		}
		if detailURL != "" && title != "" {
			return detailURL, title
		}
	}
	return detailURL, title
}

func sidneyEventTitleCandidate(label string) bool {
	label = strings.TrimSpace(label)
	if label == "" {
		return false
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(label), " "))
	return normalized != "view event" && !strings.HasPrefix(normalized, "view event ")
}

func sidneyEventArticleRoomText(raw []byte) string {
	match := sidneyEventExcerptPattern.FindSubmatch(raw)
	if len(match) < 2 {
		return ""
	}
	for _, paragraph := range sidneyExcerptParagraph.FindAllSubmatch(match[1], -1) {
		text := anchorLabel(paragraph[1])
		if sidneyRoomLabelCandidate(text) {
			return text
		}
	}
	return ""
}

func sidneyRoomLabelCandidate(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "ticket") ||
		strings.Contains(lower, "free entry") ||
		strings.Contains(lower, "sold out") ||
		strings.Contains(lower, "http") {
		return false
	}
	_, ok := sidneyRoomEvidenceFromText(value)
	return ok
}

func sidneyRoomEvidenceFromText(value string) (sourceRoomEvidence, bool) {
	text := strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if text == "" {
		return sourceRoomEvidence{}, false
	}
	normalized := strings.ToUpper(text)
	normalized = strings.ReplaceAll(normalized, "&", "+")
	normalized = strings.ReplaceAll(normalized, " AND ", "+")
	if strings.Contains(normalized, "WHOLE VENUE") {
		return sourceRoomEvidence{Text: text}, true
	}
	parts := strings.Split(normalized, "+")
	rooms := make([]RoomCandidate, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, " \t\r\n-")
		switch part {
		case "FACTORY":
			rooms = append(rooms, RoomCandidate{Slug: "factory", Name: "Factory"})
		case "BASEMENT":
			rooms = append(rooms, RoomCandidate{Slug: "basement", Name: "Basement"})
		case "GALLERY":
			rooms = append(rooms, RoomCandidate{Slug: "gallery", Name: "Gallery"})
		default:
			if !sidneyUnknownRoomLabel(part) {
				return sourceRoomEvidence{}, false
			}
			rooms = append(rooms, RoomCandidate{Slug: VenueSlugFromText(part), Name: titleRoomName(part)})
		}
	}
	if len(rooms) == 0 {
		return sourceRoomEvidence{}, false
	}
	return sourceRoomEvidence{Text: text, Rooms: rooms}, true
}

func sidneyUnknownRoomLabel(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 40 {
		return false
	}
	for _, r := range value {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ' ' || r == '-' || r == '\'' {
			continue
		}
		return false
	}
	return true
}

func titleRoomName(value string) string {
	words := strings.Fields(strings.ToLower(value))
	for i, word := range words {
		if word == "" {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

func isSidneyAndMatildaICSLink(label, href string) bool {
	if strings.EqualFold(label, "Google Calendar ICS") {
		return true
	}
	if !strings.EqualFold(label, "ICS") {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Query().Get("format"), "ical")
}

func anchorLabel(raw []byte) string {
	label := tagPattern.ReplaceAllString(string(raw), " ")
	label = html.UnescapeString(label)
	label = spacePattern.ReplaceAllString(strings.TrimSpace(label), " ")
	return label
}

func resolveURL(base *url.URL, raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	return base.ResolveReference(parsed).String(), nil
}
