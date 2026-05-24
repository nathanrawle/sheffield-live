package ingest

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

var (
	htmlAnchorTagPattern     = regexp.MustCompile(`(?is)<a\b[^>]*>`)
	hiddenCalendarURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>\\]+`)
	htmlLineBreakPattern     = regexp.MustCompile(`(?is)<\s*/?\s*(br|p|div|li|tr|td|th|h[1-6])\b[^>]*>`)
	labeledFieldTagPattern   = regexp.MustCompile(`(?is)<[^>]+>`)
)

func ExtractSameHostLinks(baseURL string, body []byte, limit int, predicate func(*url.URL) bool) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	links := make([]string, 0, limit)
	for _, match := range htmlAnchorTagPattern.FindAll(body, -1) {
		attrs := parseHTMLAttributes(string(match))
		resolved, ok := sameHostLinkURL(parsedBase, attrs["href"], predicate)
		if !ok || seen[resolved] {
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

func sameHostLinkURL(base *url.URL, raw string, predicate func(*url.URL) bool) (string, bool) {
	raw = strings.TrimSpace(html.UnescapeString(raw))
	if raw == "" {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	if sameHostKey(resolved.Host) != sameHostKey(base.Host) {
		return "", false
	}
	resolved.Fragment = ""
	resolved.RawFragment = ""
	if predicate != nil && !predicate(resolved) {
		return "", false
	}
	return resolved.String(), true
}

func sameHostKey(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	return strings.TrimPrefix(host, "www.")
}

func ExtractHiddenCalendarLinks(baseURL string, body []byte, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	links := make([]string, 0, limit)
	for _, candidate := range hiddenCalendarCandidates(body) {
		resolved, ok := hiddenCalendarURL(parsedBase, candidate)
		key := hiddenCalendarDedupKey(resolved)
		if !ok || seen[key] {
			continue
		}
		seen[key] = true
		links = append(links, resolved)
		if len(links) >= limit {
			break
		}
	}
	return links, nil
}

func hiddenCalendarCandidates(body []byte) []string {
	var out []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
		if IsCalendarURL(value) {
			return
		}
		if decoded, err := url.QueryUnescape(value); err == nil && decoded != value {
			decoded = strings.TrimSpace(decoded)
			if decoded != "" {
				out = append(out, decoded)
			}
		}
	}

	for _, tagPattern := range []*regexp.Regexp{htmlAnchorTagPattern, leadmillCalendarLinkPattern} {
		for _, match := range tagPattern.FindAll(body, -1) {
			attrs := parseHTMLAttributes(string(match))
			for _, key := range []string{"href", "src", "data-href", "data-url"} {
				add(attrs[key])
			}
		}
	}

	texts := []string{string(body), html.UnescapeString(string(body))}
	if decoded, err := url.QueryUnescape(texts[1]); err == nil {
		texts = append(texts, decoded)
	}
	for _, text := range texts {
		for _, match := range hiddenCalendarURLPattern.FindAllString(text, -1) {
			add(strings.TrimRight(match, ".,);]"))
		}
	}
	return out
}

func hiddenCalendarURL(base *url.URL, raw string) (string, bool) {
	raw = strings.TrimSpace(html.UnescapeString(raw))
	if raw == "" {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	resolved.Fragment = ""
	resolved.RawFragment = ""
	if !IsCalendarURL(resolved.String()) {
		return "", false
	}
	return resolved.String(), true
}

func hiddenCalendarDedupKey(value string) string {
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func ParseLabeledFields(raw []byte, labels ...string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	text := labeledFieldText(raw)
	lines := strings.Split(text, "\n")
	labelKeys := make(map[string]string, len(labels))
	for _, label := range labels {
		key := labeledFieldKey(label)
		if key != "" {
			labelKeys[key] = strings.TrimSpace(label)
		}
	}

	out := make(map[string]string, len(labels))
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		label, value, ok := splitLabeledFieldLine(line, labelKeys)
		if !ok {
			continue
		}
		if value == "" && i+1 < len(lines) {
			next := strings.TrimSpace(lines[i+1])
			if _, _, isLabel := splitLabeledFieldLine(next, labelKeys); !isLabel {
				value = next
				i++
			}
		}
		if value != "" {
			out[label] = value
		}
	}
	return out
}

func labeledFieldText(raw []byte) string {
	text := htmlLineBreakPattern.ReplaceAllString(string(raw), "\n")
	text = labeledFieldTagPattern.ReplaceAllString(text, " ")
	text = html.UnescapeString(text)
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		if line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func splitLabeledFieldLine(line string, labels map[string]string) (string, string, bool) {
	before, after, found := strings.Cut(line, ":")
	if !found {
		if label, ok := labels[labeledFieldKey(line)]; ok {
			return label, "", true
		}
		return "", "", false
	}
	label, ok := labels[labeledFieldKey(before)]
	if !ok {
		return "", "", false
	}
	return label, strings.TrimSpace(after), true
}

func labeledFieldKey(value string) string {
	value = strings.TrimSuffix(strings.TrimSpace(value), ":")
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}
