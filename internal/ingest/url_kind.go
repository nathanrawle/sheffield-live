package ingest

import (
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

func IsCalendarURL(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	query := parsed.Query()
	if strings.EqualFold(query.Get("format"), "ical") {
		return true
	}
	for _, key := range []string{"ical", "ics"} {
		if query.Has(key) {
			raw := strings.TrimSpace(query.Get(key))
			return raw == "" || raw == "1" || strings.EqualFold(raw, "true")
		}
	}

	ext := strings.ToLower(path.Ext(parsed.EscapedPath()))
	if ext == "" && parsed.Opaque != "" {
		ext = strings.ToLower(path.Ext(parsed.Opaque))
	}
	if ext == ".ics" || ext == ".ical" {
		return true
	}
	lower := strings.ToLower(value)
	return strings.Contains(lower, "/ical/") ||
		strings.Contains(lower, "/ics/") ||
		strings.Contains(lower, "calendar.ics")
}

func URLWithTextFragment(rawURL, text string) string {
	rawURL = strings.TrimSpace(rawURL)
	text = strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	if rawURL == "" || text == "" {
		return rawURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Fragment != "" {
		return rawURL
	}
	parsed.Fragment = ":~:text=" + text
	parsed.RawFragment = ":~:text=" + escapeTextFragment(text)
	return parsed.String()
}

func escapeTextFragment(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for _, r := range value {
		if isTextFragmentPlainByte(r) {
			builder.WriteRune(r)
			continue
		}
		var buf [utf8.UTFMax]byte
		n := utf8.EncodeRune(buf[:], r)
		for _, c := range buf[:n] {
			builder.WriteByte('%')
			builder.WriteByte(upperHex[c>>4])
			builder.WriteByte(upperHex[c&0x0f])
		}
	}
	return builder.String()
}

func isTextFragmentPlainByte(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}

const upperHex = "0123456789ABCDEF"
