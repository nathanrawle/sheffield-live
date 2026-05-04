package web

import (
	"html/template"
	"strings"
	"unicode/utf8"
)

func descriptionHTML(value string) template.HTML {
	paragraphs := splitDescriptionParagraphs(value)
	if len(paragraphs) == 0 {
		return ""
	}

	var out strings.Builder
	for _, paragraph := range paragraphs {
		switch {
		case paragraph == "---":
			out.WriteString("<hr>")
		case descriptionHeadingLevel(paragraph) > 0:
			level := descriptionHeadingLevel(paragraph)
			text := strings.TrimSpace(paragraph[level+1:])
			tagLevel := level
			if tagLevel < 2 {
				tagLevel = 2
			}
			out.WriteString("<h")
			out.WriteByte(byte('0' + tagLevel))
			out.WriteString(">")
			out.WriteString(renderDescriptionInline(text))
			out.WriteString("</h")
			out.WriteByte(byte('0' + tagLevel))
			out.WriteString(">")
		default:
			lines := strings.Split(paragraph, "\n")
			out.WriteString("<p>")
			for i, line := range lines {
				if i > 0 {
					out.WriteString("<br>")
				}
				out.WriteString(renderDescriptionInline(line))
			}
			out.WriteString("</p>")
		}
	}
	return template.HTML(out.String())
}

func splitDescriptionParagraphs(value string) []string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")

	var paragraphs []string
	var lines []string
	flush := func() {
		if len(lines) == 0 {
			return
		}
		paragraphs = append(paragraphs, strings.Join(lines, "\n"))
		lines = nil
	}
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			flush()
			continue
		}
		lines = append(lines, line)
	}
	flush()
	return paragraphs
}

func descriptionHeadingLevel(value string) int {
	level := 0
	for level < len(value) && level < 6 && value[level] == '#' {
		level++
	}
	if level == 0 || len(value) <= level || value[level] != ' ' {
		return 0
	}
	return level
}

func renderDescriptionInline(value string) string {
	var out strings.Builder
	for value != "" {
		if strings.HasPrefix(value, "**") {
			if inner, rest, ok := markedDescriptionSpan(value[2:], "**"); ok {
				out.WriteString("<strong>")
				out.WriteString(renderDescriptionInline(inner))
				out.WriteString("</strong>")
				value = rest
				continue
			}
		}
		if strings.HasPrefix(value, "_") {
			if inner, rest, ok := markedDescriptionSpan(value[1:], "_"); ok {
				out.WriteString("<em>")
				out.WriteString(renderDescriptionInline(inner))
				out.WriteString("</em>")
				value = rest
				continue
			}
		}

		r, size := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && size == 0 {
			break
		}
		out.WriteString(template.HTMLEscapeString(value[:size]))
		value = value[size:]
	}
	return out.String()
}

func markedDescriptionSpan(value, marker string) (string, string, bool) {
	end := strings.Index(value, marker)
	if end < 0 {
		return "", "", false
	}
	inner := value[:end]
	if strings.TrimSpace(inner) == "" {
		return "", "", false
	}
	return inner, value[end+len(marker):], true
}
