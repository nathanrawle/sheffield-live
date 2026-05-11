package ingest

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

var htmlMetaTagPattern = regexp.MustCompile(`(?is)<meta\b[^>]*>`)

func resolveImageSourceURL(baseURL, raw string) string {
	raw = strings.TrimSpace(html.UnescapeString(raw))
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return raw
	}
	resolved, err := resolveURL(parsed, raw)
	if err != nil {
		return raw
	}
	return resolved
}

func firstDocumentImageURL(pageURL string, raw []byte) string {
	for _, match := range htmlMetaTagPattern.FindAll(raw, -1) {
		attrs := parseHTMLAttributes(string(match))
		key := strings.ToLower(strings.TrimSpace(firstNonEmpty(attrs["property"], attrs["name"])))
		switch key {
		case "og:image", "twitter:image":
			if resolved := resolveImageSourceURL(pageURL, attrs["content"]); resolved != "" {
				return resolved
			}
		}
	}
	if imageURL := firstJSONLDImageURL(pageURL, raw); imageURL != "" {
		return imageURL
	}
	return ""
}

func firstJSONLDImageURL(pageURL string, raw []byte) string {
	matches := yellowArchJSONLDPattern.FindAllSubmatch(raw, -1)
	for _, match := range matches {
		nodes, found, err := parseYellowArchJSONLDScript(match[1])
		if err != nil || !found {
			continue
		}
		for _, node := range nodes {
			if imageURL := resolveImageSourceURL(pageURL, jsonLDImageURL(node["image"])); imageURL != "" {
				return imageURL
			}
		}
	}
	return ""
}

func firstImageSrc(pageURL string, raw []byte) string {
	img := regexp.MustCompile(`(?is)<img\b[^>]*>`).Find(raw)
	if len(img) == 0 {
		return ""
	}
	attrs := parseHTMLAttributes(string(img))
	return resolveImageSourceURL(pageURL, attrs["src"])
}

func jsonLDImageURL(value any) string {
	switch typed := value.(type) {
	case string:
		return yellowArchJSONString(typed)
	case []any:
		for _, item := range typed {
			if imageURL := jsonLDImageURL(item); imageURL != "" {
				return imageURL
			}
		}
	case map[string]any:
		for _, key := range []string{"url", "contentUrl"} {
			if imageURL := yellowArchJSONString(typed[key]); imageURL != "" {
				return imageURL
			}
		}
	}
	return ""
}
