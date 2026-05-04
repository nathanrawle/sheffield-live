package ingest

import (
	"context"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type eventDetailDescription struct {
	URL         string
	Summary     string
	StartAt     string
	Description string
}

type liveDetailDescriptionResult struct {
	Descriptions []eventDetailDescription
	Snapshots    int
}

var (
	htmlHeadingPattern      = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>`)
	cafe9EventInfoPattern   = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>\s*Event\s+information\s*</h[1-6]>(.*?)(?:<h[1-6]\b|<footer\b|</main>|</body>|</html>)`)
	sidneyDetailTextPattern = regexp.MustCompile(`(?is)<body\b[^>]*>(.*?)</body>`)
)

func sourceSupportsDetailDescriptionEnrichment(cfg sourceConfig) bool {
	switch cfg.Key {
	case CafeNo9Source, DefaultSource:
		return true
	default:
		return false
	}
}

func sourceDetailPageSourceName(cfg sourceConfig) string {
	return cfg.Name + " event details"
}

func liveDetailDescriptionsForCandidates(ctx context.Context, st Store, fetcher Fetcher, runID int64, cfg sourceConfig, links []string) liveDetailDescriptionResult {
	if len(links) == 0 || !sourceSupportsDetailDescriptionEnrichment(cfg) {
		return liveDetailDescriptionResult{}
	}

	seen := make(map[string]struct{}, len(links))
	result := liveDetailDescriptionResult{Descriptions: make([]eventDetailDescription, 0, len(links))}
	for _, link := range links {
		link = strings.TrimSpace(link)
		if link == "" {
			continue
		}
		key := detailURLKey(link)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		sourceID, err := st.EnsureSource(ctx, sourceDetailPageSourceName(cfg), link)
		if err != nil {
			continue
		}
		fetchResult, err := fetcher.Fetch(ctx, link)
		if err != nil {
			continue
		}
		if _, err := createSnapshot(ctx, st, runID, sourceID, fetchResult); err != nil {
			continue
		}
		result.Snapshots++
		if fetchResult.Truncated || !statusIsOK(fetchResult.StatusCode) {
			continue
		}
		detail := parseDetailDescriptionForSource(cfg, firstNonEmpty(fetchResult.FinalURL, fetchResult.URL), fetchResult.Body)
		if strings.TrimSpace(detail.Description) == "" {
			continue
		}
		result.Descriptions = append(result.Descriptions, detail)
	}
	return result
}

func replayDetailDescriptionsForSource(decoded []decodedReplaySnapshot, cfg sourceConfig, pageSnapshotID int64) (liveDetailDescriptionResult, error) {
	if !sourceSupportsDetailDescriptionEnrichment(cfg) {
		return liveDetailDescriptionResult{}, nil
	}

	sourceName := sourceDetailPageSourceName(cfg)
	result := liveDetailDescriptionResult{Descriptions: make([]eventDetailDescription, 0)}
	seen := make(map[string]int64)
	for _, snapshot := range decoded {
		if snapshot.snapshot.ID == pageSnapshotID || strings.TrimSpace(snapshot.snapshot.SourceName) != sourceName {
			continue
		}
		result.Snapshots++
		for _, key := range replaySnapshotLookupKeys(snapshot.envelope.Metadata) {
			if key == "" {
				continue
			}
			if existingID, exists := seen[key]; exists {
				return liveDetailDescriptionResult{}, fmt.Errorf("duplicate detail page snapshot lookup key %q for snapshots %d and %d", key, existingID, snapshot.snapshot.ID)
			}
			seen[key] = snapshot.snapshot.ID
		}
		if snapshot.envelope.Truncated || !statusIsOK(snapshot.envelope.Metadata.StatusCode) {
			continue
		}
		detail := parseDetailDescriptionForSource(cfg, firstNonEmpty(snapshot.envelope.Metadata.FinalURL, snapshot.envelope.Metadata.URL), snapshot.body)
		if strings.TrimSpace(detail.Description) == "" {
			continue
		}
		result.Descriptions = append(result.Descriptions, detail)
	}
	return result, nil
}

func detailLinksForSource(cfg sourceConfig, pageURL string, body []byte, candidates []EventCandidate, limit int) []string {
	switch cfg.Key {
	case CafeNo9Source:
		return detailLinksFromCandidateURLs(candidates, limit)
	case DefaultSource:
		links, err := ExtractSidneyAndMatildaEventDetailLinks(pageURL, body, limit)
		if err != nil {
			return nil
		}
		return links
	default:
		return nil
	}
}

func detailLinksFromCandidateURLs(candidates []EventCandidate, limit int) []string {
	links := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.Description) != "" {
			continue
		}
		link := strings.TrimSpace(firstNonEmpty(candidate.URL, candidate.UID))
		if link == "" {
			continue
		}
		key := detailURLKey(link)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		links = append(links, link)
		if limit > 0 && len(links) >= limit {
			break
		}
	}
	return links
}

func mergeDetailDescriptions(candidates []EventCandidate, details []eventDetailDescription) []EventCandidate {
	if len(candidates) == 0 || len(details) == 0 {
		return candidates
	}

	byURL := make(map[string]string, len(details))
	bySummaryStart := make(map[string]string, len(details))
	for _, detail := range details {
		description := strings.TrimSpace(detail.Description)
		if description == "" {
			continue
		}
		if key := detailURLKey(detail.URL); key != "" {
			byURL[key] = description
		}
		if key := detailSummaryStartKey(detail.Summary, detail.StartAt); key != "" {
			bySummaryStart[key] = description
		}
	}
	if len(byURL) == 0 && len(bySummaryStart) == 0 {
		return candidates
	}

	merged := append([]EventCandidate(nil), candidates...)
	for i := range merged {
		if strings.TrimSpace(merged[i].Description) != "" {
			continue
		}
		if description := byURL[detailURLKey(firstNonEmpty(merged[i].URL, merged[i].UID))]; description != "" {
			merged[i].Description = description
			continue
		}
		if description := bySummaryStart[detailSummaryStartKey(merged[i].Summary, merged[i].StartAt)]; description != "" {
			merged[i].Description = description
		}
	}
	return merged
}

func parseDetailDescriptionForSource(cfg sourceConfig, pageURL string, body []byte) eventDetailDescription {
	switch cfg.Key {
	case CafeNo9Source:
		return ParseCafeNo9DetailPage(pageURL, body)
	case DefaultSource:
		return ParseSidneyAndMatildaDetailPage(pageURL, body)
	default:
		return eventDetailDescription{}
	}
}

func ParseCafeNo9DetailPage(pageURL string, raw []byte) eventDetailDescription {
	detail := eventDetailDescription{
		URL:     strings.TrimSpace(pageURL),
		Summary: firstHTMLHeadingText(raw),
	}
	match := cafe9EventInfoPattern.FindSubmatch(raw)
	if len(match) < 2 {
		return detail
	}
	detail.Description = htmlBlockText(match[1])
	return detail
}

func ExtractSidneyAndMatildaEventDetailLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	matches := anchorPattern.FindAllSubmatch(body, -1)
	seen := make(map[string]struct{})
	links := make([]string, 0)
	for _, match := range matches {
		href := strings.Trim(strings.TrimSpace(string(match[1])), `"'`)
		label := anchorLabel(match[2])
		rawHref := html.UnescapeString(href)
		if !isSidneyAndMatildaEventDetailLink(label, rawHref) {
			continue
		}
		resolved, err := resolveURL(parsedBase, rawHref)
		if err != nil {
			continue
		}
		if _, ok := seen[resolved]; ok {
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

func isSidneyAndMatildaEventDetailLink(label, href string) bool {
	if strings.EqualFold(strings.TrimSpace(label), "ICS") || strings.Contains(strings.ToLower(label), "calendar") {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return false
	}
	if strings.EqualFold(parsed.Query().Get("format"), "ical") {
		return false
	}
	path := strings.Trim(strings.ToLower(parsed.Path), "/")
	return strings.HasPrefix(path, "events/") && path != "events"
}

func ParseSidneyAndMatildaDetailPage(pageURL string, raw []byte) eventDetailDescription {
	detail := eventDetailDescription{
		URL:     strings.TrimSpace(pageURL),
		Summary: firstHTMLHeadingText(raw),
	}
	lines := htmlLines(raw)
	detail.StartAt = sidneyDetailStartAt(lines)
	detail.Description = sidneyDetailDescription(lines)
	return detail
}

func sidneyDetailStartAt(lines []string) string {
	for i, line := range lines {
		if !strings.Contains(line, ",") || !strings.Contains(line, "202") {
			continue
		}
		if i+1 >= len(lines) {
			continue
		}
		start := strings.TrimSpace(lines[i+1])
		if fields := strings.Fields(start); len(fields) >= 2 {
			start = fields[0] + " " + fields[1]
		}
		parsed, err := parseSidneyDetailDateTime(line, start)
		if err == nil {
			return formatTime(parsed)
		}
	}
	return ""
}

func sidneyDetailDescription(lines []string) string {
	start := -1
	for i, line := range lines {
		if strings.Contains(strings.ToLower(line), "google calendar") || strings.EqualFold(line, "ICS") {
			start = i + 1
			break
		}
	}
	if start < 0 || start >= len(lines) {
		return ""
	}

	description := make([]string, 0)
	for _, line := range lines[start:] {
		lower := strings.ToLower(strings.TrimSpace(line))
		switch {
		case line == "":
			continue
		case strings.EqualFold(line, "ICS"):
			continue
		case strings.Contains(lower, "buy tickets"):
			continue
		case strings.EqualFold(line, "Back to All Events"):
			continue
		case strings.Contains(lower, "previous previous") || strings.Contains(lower, "next next"):
			return strings.Join(description, "\n")
		case strings.Contains(lower, "rivelin works"):
			return strings.Join(description, "\n")
		case strings.Contains(lower, "sidney & matilda") && len(description) > 0:
			return strings.Join(description, "\n")
		default:
			description = append(description, line)
		}
	}
	return strings.Join(description, "\n")
}

func parseSidneyDetailDateTime(dateText, timeText string) (time.Time, error) {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	value := strings.TrimSpace(dateText + " " + strings.ToLower(timeText))
	for _, layout := range []string{
		"Monday, January 2, 2006 3:04pm",
		"Monday, January 2, 2006 3:04 pm",
		"Monday, January 2, 2006 3pm",
		"Monday, January 2, 2006 3 pm",
	} {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Sidney & Matilda datetime %q", value)
}

func firstHTMLHeadingText(raw []byte) string {
	match := htmlHeadingPattern.FindSubmatch(raw)
	if len(match) < 2 {
		return ""
	}
	return htmlCleanText(string(match[1]))
}

func htmlLines(raw []byte) []string {
	body := raw
	if match := sidneyDetailTextPattern.FindSubmatch(raw); len(match) >= 2 {
		body = match[1]
	}
	text := cafe9LineBreakPattern.ReplaceAllString(string(body), "\n")
	text = cafe9TagPattern.ReplaceAllString(text, "\n")
	text = html.UnescapeString(text)
	parts := strings.Split(text, "\n")
	lines := make([]string, 0, len(parts))
	for _, part := range parts {
		line := htmlCleanText(part)
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func htmlBlockText(raw []byte) string {
	return strings.Join(htmlLines(raw), "\n")
}

func htmlCleanText(value string) string {
	value = html.UnescapeString(value)
	value = cafe9WhitespacePattern.ReplaceAllString(strings.TrimSpace(value), " ")
	return strings.TrimSpace(value)
}

func detailURLKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return strings.TrimRight(value, "/")
	}
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func detailSummaryStartKey(summary, startAt string) string {
	summary = cafe9WhitespacePattern.ReplaceAllString(strings.ToLower(strings.TrimSpace(summary)), " ")
	startAt = strings.TrimSpace(startAt)
	if summary == "" || startAt == "" {
		return ""
	}
	return summary + "\x00" + startAt
}
