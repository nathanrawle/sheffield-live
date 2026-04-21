package ingest

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

var (
	anchorPattern = regexp.MustCompile(`(?is)<a\b[^>]*?\bhref\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)[^>]*>(.*?)</a>`)
	tagPattern    = regexp.MustCompile(`(?is)<[^>]+>`)
	spacePattern  = regexp.MustCompile(`\s+`)
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
