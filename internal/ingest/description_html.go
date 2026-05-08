package ingest

import (
	"html"
	"regexp"
	"strings"
	"unicode"
)

var (
	descriptionScriptStylePattern = regexp.MustCompile(`(?is)<(?:script|style)\b[^>]*>.*?</(?:script|style)>`)
	descriptionHeadingPattern     = regexp.MustCompile(`(?is)<h([1-6])\b[^>]*>(.*?)</h[1-6]>`)
	descriptionHRPattern          = regexp.MustCompile(`(?is)<hr\b[^>]*>`)
	descriptionDoubleBRPattern    = regexp.MustCompile(`(?is)(?:<br\s*/?>\s*){2,}`)
	descriptionSingleBRPattern    = regexp.MustCompile(`(?is)<br\s*/?>`)
	descriptionParagraphOpen      = regexp.MustCompile(`(?is)<p\b[^>]*>`)
	descriptionParagraphClose     = regexp.MustCompile(`(?is)</p\s*>`)
	descriptionBlockClose         = regexp.MustCompile(`(?is)</(?:div|section|article|main)\s*>`)
	descriptionLineClose          = regexp.MustCompile(`(?is)</(?:li)\s*>`)
	descriptionTagPattern         = regexp.MustCompile(`(?is)<[^>]+>`)
	descriptionWhitespacePattern  = regexp.MustCompile(`[ \t\f\v]+`)
)

func semanticDescriptionText(raw string) string {
	text := strings.ReplaceAll(raw, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	text = descriptionScriptStylePattern.ReplaceAllString(text, "")
	text = replaceSemanticInlineTags(text, `em|i`, "_")
	text = replaceSemanticInlineTags(text, `strong|b`, "**")
	text = replaceSemanticHeadingTags(text)
	text = descriptionHRPattern.ReplaceAllString(text, "\n\n---\n\n")
	text = descriptionDoubleBRPattern.ReplaceAllString(text, "\n\n")
	text = descriptionSingleBRPattern.ReplaceAllString(text, "\n")
	text = descriptionParagraphOpen.ReplaceAllString(text, "\n\n")
	text = descriptionParagraphClose.ReplaceAllString(text, "\n\n")
	text = descriptionBlockClose.ReplaceAllString(text, "\n\n")
	text = descriptionLineClose.ReplaceAllString(text, "\n")
	text = descriptionTagPattern.ReplaceAllString(text, " ")
	text = html.UnescapeString(text)
	return normalizeDescriptionLines(text)
}

func replaceSemanticHeadingTags(value string) string {
	return descriptionHeadingPattern.ReplaceAllStringFunc(value, func(match string) string {
		parts := descriptionHeadingPattern.FindStringSubmatch(match)
		if len(parts) < 3 {
			return "\n\n"
		}
		level := int(parts[1][0] - '0')
		inner := semanticInlineText(parts[2])
		if inner == "" {
			return "\n\n"
		}
		return "\n\n" + strings.Repeat("#", level) + " " + inner + "\n\n"
	})
}

func replaceSemanticInlineTags(value, names, marker string) string {
	pattern := regexp.MustCompile(`(?is)<(?:` + names + `)\b[^>]*>(.*?)</(?:` + names + `)>`)
	for {
		changed := false
		next := pattern.ReplaceAllStringFunc(value, func(match string) string {
			parts := pattern.FindStringSubmatch(match)
			if len(parts) < 2 {
				return ""
			}
			inner := semanticInlineText(parts[1])
			if inner == "" {
				return ""
			}
			changed = true
			prefix, suffix := semanticInlineBoundarySpaces(parts[1])
			return prefix + marker + inner + marker + suffix
		})
		value = next
		if !changed {
			return value
		}
	}
}

func semanticInlineBoundarySpaces(value string) (string, string) {
	value = descriptionScriptStylePattern.ReplaceAllString(value, "")
	value = descriptionTagPattern.ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	prefix := ""
	if first, ok := firstRune(value); ok && unicode.IsSpace(first) {
		prefix = " "
	}
	suffix := ""
	if last, ok := lastRune(value); ok && unicode.IsSpace(last) {
		suffix = " "
	}
	return prefix, suffix
}

func firstRune(value string) (rune, bool) {
	for _, r := range value {
		return r, true
	}
	return 0, false
}

func lastRune(value string) (rune, bool) {
	var last rune
	ok := false
	for _, r := range value {
		last = r
		ok = true
	}
	return last, ok
}

func semanticInlineText(value string) string {
	value = descriptionScriptStylePattern.ReplaceAllString(value, "")
	value = descriptionTagPattern.ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	value = descriptionWhitespacePattern.ReplaceAllString(strings.TrimSpace(value), " ")
	return strings.TrimSpace(value)
}

func normalizeDescriptionLines(value string) string {
	lines := strings.Split(value, "\n")
	out := make([]string, 0, len(lines))
	blank := true
	for _, line := range lines {
		line = descriptionWhitespacePattern.ReplaceAllString(strings.TrimSpace(line), " ")
		if line == "" {
			if !blank {
				out = append(out, "")
				blank = true
			}
			continue
		}
		out = append(out, line)
		blank = false
	}
	for len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return strings.Join(out, "\n")
}
