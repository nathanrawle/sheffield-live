package ingest

import "strings"

func VenueSlugFromText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	if strings.Contains(value, "sidney") && strings.Contains(value, "matilda") {
		return "sidney-and-matilda"
	}
	if strings.Contains(value, "yellow") && strings.Contains(value, "arch") {
		return "yellow-arch"
	}
	if strings.Contains(value, "cafe") && strings.Contains(value, "9") {
		return "cafe-no-9"
	}
	if strings.Contains(value, "leadmill") {
		return "leadmill"
	}

	var out strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		case !lastDash:
			out.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(out.String(), "-")
}
