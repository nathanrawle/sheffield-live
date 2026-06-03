package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type eventDetailDescription struct {
	URL                      string
	URLAliases               []string
	Summary                  string
	StartAt                  string
	EndAt                    string
	Description              string
	ImageSourceURL           string
	ImageAlt                 string
	ImageWidth               int
	ImageHeight              int
	DisableSourceURLIdentity bool
	ReplaceInferredStart     bool
}

type liveDetailDescriptionResult struct {
	Descriptions []eventDetailDescription
	Snapshots    int
}

var (
	htmlHeadingPattern            = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>`)
	htmlScriptStylePattern        = regexp.MustCompile(`(?is)<(?:script|style)\b[^>]*>.*?</(?:script|style)>`)
	cafe9EventInfoPattern         = regexp.MustCompile(`(?is)<h[1-6]\b[^>]*>\s*Event\s+information\s*</h[1-6]>(.*?)(?:<h[1-6]\b|<footer\b|</main>|</body>|</html>)`)
	htmlCanonicalLinkPattern      = regexp.MustCompile(`(?is)<link\b[^>]*rel\s*=\s*["']canonical["'][^>]*href\s*=\s*["']([^"']+)["'][^>]*>`)
	htmlCanonicalHrefPattern      = regexp.MustCompile(`(?is)<link\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*rel\s*=\s*["']canonical["'][^>]*>`)
	yellowArchContentPattern      = regexp.MustCompile(`(?is)<div\b[^>]*class\s*=\s*["'][^"']*\bevent-single__content\b[^"']*["'][^>]*>(.*?)<div\b[^>]*class\s*=\s*["'][^"']*\bevent-single__venue-address-wrapper\b`)
	sidneyDetailTextPattern       = regexp.MustCompile(`(?is)<body\b[^>]*>(.*?)</body>`)
	sidneyContentContainerPattern = regexp.MustCompile(`(?is)<div\b[^>]*class\s*=\s*["'][^"']*\beventitem-column-content\b[^"']*["'][^>]*>(.*?)<div\b[^>]*class\s*=\s*["'][^"']*\beventitem-content-footer\b`)
	sidneyHTMLContentPattern      = regexp.MustCompile(`(?is)<div\b[^>]*class\s*=\s*["'][^"']*\bsqs-html-content\b[^"']*["'][^>]*>(.*?)</div>`)
	hallamshireOrdinalPattern     = regexp.MustCompile(`(?i)(\d{1,2})(?:st|nd|rd|th)`)
	hallamshireClockPattern       = regexp.MustCompile(`(?i)\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b|\b\d{1,2}:\d{2}\b`)
	hallamshireDetailDatePattern  = regexp.MustCompile(`(?i)\b(?:mon|tues|wednes|thurs|fri|satur|sun)day\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+\s*,?\s*20\d{2}\b|\b\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+\s*,?\s*20\d{2}\b`)
)

func sourceSupportsDetailDescriptionEnrichment(cfg sourceConfig) bool {
	switch cfg.Key {
	case CafeNo9Source, DefaultSource, HallamshireHotelSource, YellowArchSource:
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
		detail.URLAliases = appendDetailURLAliases(detail.URLAliases, fetchResult.URL, fetchResult.FinalURL, detail.URL)
		if !detailHasUsefulEnrichment(detail) {
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
		detail.URLAliases = appendDetailURLAliases(detail.URLAliases, snapshot.envelope.Metadata.URL, snapshot.envelope.Metadata.FinalURL, detail.URL)
		if !detailHasUsefulEnrichment(detail) {
			continue
		}
		result.Descriptions = append(result.Descriptions, detail)
	}
	return result, nil
}

func detailLinksForSource(cfg sourceConfig, pageURL string, body []byte, candidates []EventCandidate, limit int) []string {
	switch cfg.Key {
	case CafeNo9Source, YellowArchSource:
		return detailLinksFromCandidateURLs(candidates, limit)
	case HallamshireHotelSource:
		return hallamshireHotelDetailLinksFromCandidateURLs(candidates, limit)
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

func hallamshireHotelDetailLinksFromCandidateURLs(candidates []EventCandidate, limit int) []string {
	links := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		link, ok := hallamshireHotelTrustedDetailURL(candidate.URL)
		if !ok {
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

func detailHasUsefulEnrichment(detail eventDetailDescription) bool {
	return strings.TrimSpace(detail.Description) != "" ||
		strings.TrimSpace(detail.ImageSourceURL) != "" ||
		strings.TrimSpace(detail.StartAt) != "" ||
		strings.TrimSpace(detail.EndAt) != "" ||
		(detail.DisableSourceURLIdentity && strings.TrimSpace(detail.URL) != "")
}

func mergeDetailDescriptions(candidates []EventCandidate, details []eventDetailDescription) []EventCandidate {
	return mergeDetailDescriptionsWithPreference(candidates, details, true)
}

func mergeDetailDescriptionsWithPreference(candidates []EventCandidate, details []eventDetailDescription, preferDetails bool) []EventCandidate {
	if len(candidates) == 0 || len(details) == 0 {
		return candidates
	}

	byURL := make(map[string]eventDetailDescription, len(details))
	bySummaryStart := make(map[string]eventDetailDescription, len(details))
	for _, detail := range details {
		description := strings.TrimSpace(detail.Description)
		imageURL := strings.TrimSpace(detail.ImageSourceURL)
		startAt := strings.TrimSpace(detail.StartAt)
		endAt := strings.TrimSpace(detail.EndAt)
		if description == "" && imageURL == "" && startAt == "" && endAt == "" && !detail.DisableSourceURLIdentity {
			continue
		}
		detail.Description = description
		detail.ImageSourceURL = imageURL
		detail.StartAt = startAt
		detail.EndAt = endAt
		for _, alias := range appendDetailURLAliases(detail.URLAliases, detail.URL) {
			key := detailURLKey(alias)
			if key == "" {
				continue
			}
			byURL[key] = detail
		}
		if key := detailSummaryStartKey(detail.Summary, detail.StartAt); key != "" {
			bySummaryStart[key] = detail
		}
	}
	if len(byURL) == 0 && len(bySummaryStart) == 0 {
		return candidates
	}

	merged := append([]EventCandidate(nil), candidates...)
	for i := range merged {
		if !preferDetails && strings.TrimSpace(merged[i].Description) != "" {
			continue
		}
		if detail, ok := byURL[detailURLKey(firstNonEmpty(merged[i].URL, merged[i].UID))]; ok {
			mergeDetailIntoCandidate(&merged[i], detail, preferDetails)
			continue
		}
		if detail, ok := bySummaryStart[detailSummaryStartKey(merged[i].Summary, merged[i].StartAt)]; ok {
			mergeDetailIntoCandidate(&merged[i], detail, preferDetails)
		}
	}
	return merged
}

func mergeDetailIntoCandidate(candidate *EventCandidate, detail eventDetailDescription, preferDetails bool) {
	if candidate == nil {
		return
	}
	if strings.TrimSpace(detail.Description) != "" && (preferDetails || strings.TrimSpace(candidate.Description) == "") {
		candidate.Description = detail.Description
	}
	if strings.TrimSpace(detail.ImageSourceURL) != "" && (preferDetails || strings.TrimSpace(candidate.ImageSourceURL) == "") {
		candidate.ImageSourceURL = detail.ImageSourceURL
		candidate.ImageAlt = detail.ImageAlt
		candidate.ImageWidth = detail.ImageWidth
		candidate.ImageHeight = detail.ImageHeight
	}
	if detail.DisableSourceURLIdentity && strings.TrimSpace(detail.URL) != "" {
		candidate.URL = strings.TrimSpace(detail.URL)
		candidate.SourceURLSourceIdentityDisabled = true
	}
	if detail.ReplaceInferredStart && candidate.StartAtInferred && hallamshireDetailStartMatchesCandidateDate(candidate.StartAt, detail.StartAt) {
		candidate.StartAt = strings.TrimSpace(detail.StartAt)
		candidate.StartAtInferred = false
		candidate.StartAtBasis = ""
		if strings.TrimSpace(detail.EndAt) != "" {
			candidate.EndAt = strings.TrimSpace(detail.EndAt)
		}
	}
}

func hallamshireDetailStartMatchesCandidateDate(candidateStart, detailStart string) bool {
	candidateTime, err := time.Parse(time.RFC3339, strings.TrimSpace(candidateStart))
	if err != nil {
		return false
	}
	detailTime, err := time.Parse(time.RFC3339, strings.TrimSpace(detailStart))
	if err != nil {
		return false
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return false
	}
	candidateLocal := candidateTime.In(loc)
	detailLocal := detailTime.In(loc)
	return candidateLocal.Year() == detailLocal.Year() &&
		candidateLocal.Month() == detailLocal.Month() &&
		candidateLocal.Day() == detailLocal.Day()
}

func parseDetailDescriptionForSource(cfg sourceConfig, pageURL string, body []byte) eventDetailDescription {
	switch cfg.Key {
	case CafeNo9Source:
		return ParseCafeNo9DetailPage(pageURL, body)
	case HallamshireHotelSource:
		return ParseHallamshireHotelDetailPage(pageURL, body)
	case DefaultSource:
		return ParseSidneyAndMatildaDetailPage(pageURL, body)
	case YellowArchSource:
		return ParseYellowArchDetailPage(pageURL, body)
	default:
		return eventDetailDescription{}
	}
}

func ParseCafeNo9DetailPage(pageURL string, raw []byte) eventDetailDescription {
	detail := eventDetailDescription{
		URL:            strings.TrimSpace(pageURL),
		Summary:        firstHTMLHeadingText(raw),
		ImageSourceURL: firstDocumentImageURL(pageURL, raw),
	}
	if canonicalURL := firstCanonicalLink(pageURL, raw); canonicalURL != "" {
		detail.URLAliases = appendDetailURLAliases(detail.URLAliases, canonicalURL)
	}
	match := cafe9EventInfoPattern.FindSubmatch(raw)
	if len(match) < 2 {
		return detail
	}
	detail.Description = htmlParagraphText(match[1])
	return detail
}

func ParseHallamshireHotelDetailPage(pageURL string, raw []byte) eventDetailDescription {
	detail := hallamshireHotelBaseDetail(pageURL, raw)
	lines := htmlLines(raw)
	visibleStartAt := hallamshireHotelVisibleDetailStartAt(lines)
	visibleDescription := ""
	if match := cafe9EventInfoPattern.FindSubmatch(raw); len(match) >= 2 {
		visibleDescription = htmlParagraphText(match[1])
	}
	if structured := parseHallamshireHotelStructuredDetail(pageURL, raw); detailHasUsefulEnrichment(structured) {
		structured.URL = firstNonEmpty(structured.URL, detail.URL)
		structured.URLAliases = appendDetailURLAliases(detail.URLAliases, structured.URLAliases...)
		structured.Summary = firstNonEmpty(structured.Summary, detail.Summary)
		structured.StartAt = firstNonEmpty(structured.StartAt, visibleStartAt)
		structured.Description = firstNonEmpty(structured.Description, visibleDescription)
		structured.ImageSourceURL = firstNonEmpty(structured.ImageSourceURL, detail.ImageSourceURL)
		structured.ImageAlt = firstNonEmpty(structured.ImageAlt, detail.ImageAlt)
		structured.DisableSourceURLIdentity = structured.DisableSourceURLIdentity || detail.DisableSourceURLIdentity
		structured.ReplaceInferredStart = true
		return structured
	}

	detail.StartAt = visibleStartAt
	detail.Description = visibleDescription
	detail.ReplaceInferredStart = true
	return detail
}

func hallamshireHotelBaseDetail(pageURL string, raw []byte) eventDetailDescription {
	pageURL = strings.TrimSpace(pageURL)
	detailURL := ""
	disableSourceURLIdentity := false
	aliases := appendDetailURLAliases(nil, pageURL)
	if canonicalURL := firstCanonicalLink(pageURL, raw); canonicalURL != "" {
		aliases = appendDetailURLAliases(aliases, canonicalURL)
		if trusted, ok := hallamshireHotelTrustedDetailURL(canonicalURL); ok {
			detailURL = trusted
			disableSourceURLIdentity = true
		}
	}
	if detailURL == "" {
		detailURL = pageURL
	}
	if trusted, ok := hallamshireHotelTrustedDetailURL(detailURL); ok {
		detailURL = trusted
		disableSourceURLIdentity = true
	} else {
		detailURL = ""
	}
	summary := firstHTMLHeadingText(raw)
	return eventDetailDescription{
		URL:                      detailURL,
		URLAliases:               aliases,
		Summary:                  summary,
		ImageSourceURL:           firstDocumentImageURL(pageURL, raw),
		ImageAlt:                 summary,
		DisableSourceURLIdentity: disableSourceURLIdentity,
	}
}

func parseHallamshireHotelStructuredDetail(pageURL string, raw []byte) eventDetailDescription {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		nodes, found, err := parseYellowArchJSONLDScript(match[1])
		if err != nil || !found {
			continue
		}
		for _, node := range nodes {
			detail := hallamshireHotelStructuredDetailFromNode(pageURL, node)
			if detailHasUsefulEnrichment(detail) {
				return detail
			}
		}
	}
	return eventDetailDescription{}
}

func hallamshireHotelStructuredDetailFromNode(pageURL string, node map[string]any) eventDetailDescription {
	name := yellowArchJSONString(node["name"])
	description := cleanStructuredDescription(yellowArchJSONString(node["description"]))
	if !descriptionLooksClean(description) {
		description = ""
	}
	startAt := ""
	if startText := yellowArchJSONString(node["startDate"]); startText != "" {
		if parsed, err := parseHallamshireHotelStructuredDateTime(startText); err == nil {
			startAt = formatTime(parsed)
		}
	}
	endAt := ""
	if endText := yellowArchJSONString(node["endDate"]); endText != "" {
		if parsed, err := parseHallamshireHotelStructuredDateTime(endText); err == nil {
			endAt = formatTime(parsed)
		}
	}

	rawURL := yellowArchJSONString(node["url"])
	detailURL := hallamshireHotelFirstTrustedDetailURL(rawURL, pageURL)
	return eventDetailDescription{
		URL:                      detailURL,
		URLAliases:               appendDetailURLAliases(nil, rawURL, pageURL),
		Summary:                  name,
		StartAt:                  startAt,
		EndAt:                    endAt,
		Description:              description,
		ImageSourceURL:           resolveImageSourceURL(pageURL, jsonLDImageURL(node["image"])),
		ImageAlt:                 name,
		DisableSourceURLIdentity: detailURL != "",
		ReplaceInferredStart:     true,
	}
}

func parseHallamshireHotelStructuredDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04Z07:00", value); err == nil {
		return parsed.UTC(), nil
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
	} {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Hallamshire detail datetime %q", value)
}

func hallamshireHotelVisibleDetailStartAt(lines []string) string {
	for i, line := range lines {
		if !strings.Contains(line, "20") {
			continue
		}
		for j := i; j < len(lines) && j <= i+3; j++ {
			timeText := hallamshireClockPattern.FindString(lines[j])
			if timeText == "" {
				continue
			}
			if parsed, err := parseHallamshireHotelDetailDateTime(line, timeText); err == nil {
				return formatTime(parsed)
			}
		}
	}
	text := strings.Join(lines, "\n")
	for _, match := range hallamshireDetailDatePattern.FindAllStringIndex(text, -1) {
		dateText := text[match[0]:match[1]]
		end := match[1] + 80
		if end > len(text) {
			end = len(text)
		}
		if timeText := hallamshireClockPattern.FindString(text[match[1]:end]); timeText != "" {
			if parsed, err := parseHallamshireHotelDetailDateTime(dateText, timeText); err == nil {
				return formatTime(parsed)
			}
		}
	}
	return ""
}

func parseHallamshireHotelDetailDateTime(dateText, timeText string) (time.Time, error) {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.Time{}, err
	}
	dateText = hallamshireOrdinalPattern.ReplaceAllString(strings.TrimSpace(dateText), "$1")
	dateText = strings.ReplaceAll(dateText, ",", "")
	value := strings.Join(strings.Fields(dateText+" "+timeText), " ")
	for _, layout := range []string{
		"Monday 2 January 2006 3:04pm",
		"Monday 2 January 2006 3pm",
		"Monday 2 January 2006 15:04",
		"2 January 2006 3:04pm",
		"2 January 2006 3pm",
		"2 January 2006 15:04",
	} {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Hallamshire detail datetime %q", value)
}

func ParseYellowArchDetailPage(pageURL string, raw []byte) eventDetailDescription {
	detail := eventDetailDescription{
		URL:            strings.TrimSpace(pageURL),
		Summary:        firstHTMLHeadingText(raw),
		ImageSourceURL: firstDocumentImageURL(pageURL, raw),
	}
	if canonicalURL := firstCanonicalLink(pageURL, raw); canonicalURL != "" {
		detail.URLAliases = appendDetailURLAliases(detail.URLAliases, canonicalURL)
	}

	matches := yellowArchContentPattern.FindAllSubmatch(raw, -1)
	if len(matches) != 1 || len(matches[0]) < 2 {
		return detail
	}
	detail.Description = htmlParagraphText(matches[0][1])
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
		URL:            strings.TrimSpace(pageURL),
		Summary:        firstHTMLHeadingText(raw),
		ImageSourceURL: firstDocumentImageURL(pageURL, raw),
	}
	if canonicalURL := firstCanonicalLink(pageURL, raw); canonicalURL != "" {
		detail.URLAliases = appendDetailURLAliases(detail.URLAliases, canonicalURL)
	}
	if structured := parseSidneyAndMatildaStructuredDetail(pageURL, raw); strings.TrimSpace(structured.Description) != "" {
		structured.URL = firstNonEmpty(detail.URL, structured.URL)
		structured.URLAliases = appendDetailURLAliases(detail.URLAliases, structured.URLAliases...)
		structured.Summary = firstNonEmpty(structured.Summary, detail.Summary)
		structured.ImageSourceURL = firstNonEmpty(structured.ImageSourceURL, detail.ImageSourceURL)
		return structured
	}
	lines := htmlLines(raw)
	detail.StartAt = sidneyDetailStartAt(lines)
	detail.Description = sidneyDetailDescription(raw)
	return detail
}

func parseSidneyAndMatildaJSONLDScript(raw []byte) ([]map[string]any, bool, error) {
	if !sidneyJSONLDHasSchemaContext(raw) {
		return nil, false, nil
	}
	return parseYellowArchJSONLDScript(raw)
}

func sidneyJSONLDHasSchemaContext(raw []byte) bool {
	text := strings.TrimSpace(html.UnescapeString(string(raw)))
	if text == "" {
		return false
	}

	var payload any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return false
	}
	return sidneyJSONLDContainsSchemaContext(payload)
}

func sidneyJSONLDContainsSchemaContext(value any) bool {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if sidneyJSONLDContainsSchemaContext(item) {
				return true
			}
		}
	case map[string]any:
		if context, ok := typed["@context"]; ok && sidneyJSONLDContextValueHasSchema(context) {
			return true
		}
		for key, item := range typed {
			if key == "@context" {
				continue
			}
			if sidneyJSONLDContainsSchemaContext(item) {
				return true
			}
		}
	}
	return false
}

func sidneyJSONLDContextValueHasSchema(value any) bool {
	switch typed := value.(type) {
	case string:
		return sidneyIsSchemaOrgContext(typed)
	case []any:
		for _, item := range typed {
			if sidneyJSONLDContextValueHasSchema(item) {
				return true
			}
		}
	case map[string]any:
		if vocab, ok := typed["@vocab"]; ok && sidneyJSONLDContextValueHasSchema(vocab) {
			return true
		}
	}
	return false
}

func sidneyIsSchemaOrgContext(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	if !strings.EqualFold(parsed.Host, "schema.org") {
		return false
	}
	return strings.Trim(parsed.Path, "/") == ""
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

func sidneyDetailDescription(raw []byte) string {
	body := sidneyDetailContent(raw)
	if len(body) == 0 {
		return ""
	}

	paragraphs := htmlParagraphs(body)
	for i, paragraph := range paragraphs {
		lower := strings.ToLower(paragraph)
		if strings.Contains(lower, "google calendar") || strings.EqualFold(strings.TrimSpace(paragraph), "ICS") {
			paragraphs = paragraphs[i+1:]
			break
		}
	}
	description := make([]string, 0, len(paragraphs))
	for _, paragraph := range paragraphs {
		if sidneyIsIgnorableDescriptionParagraph(paragraph) {
			continue
		}
		description = append(description, paragraph)
	}
	return strings.Join(description, "\n\n")
}

func sidneyDetailContent(raw []byte) []byte {
	body := raw
	if match := sidneyDetailTextPattern.FindSubmatch(raw); len(match) >= 2 {
		body = match[1]
	}
	if match := sidneyContentContainerPattern.FindSubmatch(body); len(match) >= 2 {
		body = match[1]
	}
	if match := sidneyHTMLContentPattern.FindSubmatch(body); len(match) >= 2 {
		return match[1]
	}
	return nil
}

func sidneyIsIgnorableDescriptionParagraph(paragraph string) bool {
	lower := strings.ToLower(strings.TrimSpace(paragraph))
	switch {
	case paragraph == "":
		return true
	case strings.EqualFold(paragraph, "ICS"):
		return true
	case strings.Contains(lower, "buy tickets"):
		return true
	case strings.EqualFold(paragraph, "Back to All Events"):
		return true
	case strings.Contains(lower, "previous previous") || strings.Contains(lower, "next next"):
		return true
	case strings.Contains(lower, "rivelin works"):
		return true
	case strings.Contains(lower, "#block-") || strings.Contains(lower, "--tweak-") || strings.Contains(lower, "@media screen"):
		return true
	default:
		return false
	}
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

func firstCanonicalLink(pageURL string, raw []byte) string {
	match := htmlCanonicalLinkPattern.FindSubmatch(raw)
	if len(match) < 2 {
		match = htmlCanonicalHrefPattern.FindSubmatch(raw)
	}
	if len(match) < 2 {
		return ""
	}
	canonicalURL := strings.TrimSpace(html.UnescapeString(string(match[1])))
	if canonicalURL == "" {
		return ""
	}
	if baseURL, err := url.Parse(strings.TrimSpace(pageURL)); err == nil {
		if resolved, err := resolveURL(baseURL, canonicalURL); err == nil {
			return resolved
		}
	}
	return canonicalURL
}

func parseSidneyAndMatildaStructuredDetail(pageURL string, raw []byte) eventDetailDescription {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		nodes, found, err := parseSidneyAndMatildaJSONLDScript(match[1])
		if err != nil || !found {
			continue
		}
		for _, node := range nodes {
			name := yellowArchJSONString(node["name"])
			description := cleanStructuredDescription(yellowArchJSONString(node["description"]))
			if !descriptionLooksClean(description) {
				continue
			}
			startAt := ""
			if startText := yellowArchJSONString(node["startDate"]); startText != "" {
				if parsed, err := parseYellowArchDateTime(startText); err == nil {
					startAt = formatTime(parsed)
				}
			}
			rawURL := yellowArchJSONString(node["url"])
			return eventDetailDescription{
				URL:            rawURL,
				URLAliases:     appendDetailURLAliases(nil, rawURL),
				Summary:        strings.TrimSuffix(name, " — Sidney&Matilda"),
				StartAt:        startAt,
				Description:    description,
				ImageSourceURL: resolveImageSourceURL(pageURL, jsonLDImageURL(node["image"])),
				ImageAlt:       name,
			}
		}
	}
	return eventDetailDescription{}
}

func cleanStructuredDescription(value string) string {
	return semanticDescriptionText(value)
}

func htmlLines(raw []byte) []string {
	body := raw
	if match := sidneyDetailTextPattern.FindSubmatch(raw); len(match) >= 2 {
		body = match[1]
	}
	body = stripNonContentHTML(body)
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

func htmlParagraphText(raw []byte) string {
	return semanticDescriptionText(string(raw))
}

func htmlParagraphs(raw []byte) []string {
	text := semanticDescriptionText(string(raw))
	parts := strings.Split(text, "\n\n")
	paragraphs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		paragraphs = append(paragraphs, part)
	}
	return paragraphs
}

func stripNonContentHTML(raw []byte) []byte {
	return htmlScriptStylePattern.ReplaceAll(raw, nil)
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
	parsed.Host = strings.TrimPrefix(strings.ToLower(parsed.Host), "www.")
	return strings.TrimRight(parsed.String(), "/")
}

func appendDetailURLAliases(dst []string, values ...string) []string {
	seen := make(map[string]struct{}, len(dst)+len(values))
	out := make([]string, 0, len(dst)+len(values))
	for _, value := range append(dst, values...) {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := detailURLKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func detailSummaryStartKey(summary, startAt string) string {
	summary = cafe9WhitespacePattern.ReplaceAllString(strings.ToLower(strings.TrimSpace(summary)), " ")
	startAt = strings.TrimSpace(startAt)
	if summary == "" || startAt == "" {
		return ""
	}
	return summary + "\x00" + startAt
}

func descriptionLooksClean(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	switch {
	case strings.Contains(lower, "#block-"):
		return false
	case strings.Contains(lower, "--tweak-"):
		return false
	case strings.Contains(lower, "@media screen"):
		return false
	case strings.Contains(lower, "<script") || strings.Contains(lower, "<style"):
		return false
	case strings.EqualFold(value, "buy tickets"):
		return false
	case strings.EqualFold(value, "basement buy tickets"):
		return false
	case len([]rune(value)) < 40 && !strings.ContainsAny(value, ".!?"):
		return false
	default:
		return true
	}
}
