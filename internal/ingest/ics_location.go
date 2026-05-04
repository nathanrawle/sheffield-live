package ingest

import "strings"

// HasICSEscapeSequences reports whether a raw ICS text field still contains
// escape sequences whose decoding affects venue/location parsing.
func HasICSEscapeSequences(value string) bool {
	value = strings.TrimSpace(value)
	for i := 0; i+1 < len(value); i++ {
		if value[i] != '\\' {
			continue
		}
		switch value[i+1] {
		case '\\', ',', ';', 'n', 'N':
			return true
		}
	}
	return false
}

func VenueLocationEvidenceHead(value string) string {
	parts := VenueLocationEvidenceParts(value)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func VenueLocationEvidenceParts(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	if !HasICSEscapeSequences(value) {
		return cleanVenueLocationParts(value)
	}

	rawParts := ICSVenueLocationParts(value)
	cleanedParts := cleanVenueLocationParts(cleanICSValue(value))
	if shouldUseRawVenueLocationParts(rawParts, cleanedParts) {
		return rawParts
	}
	return cleanedParts
}

func ICSVenueLocationParts(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	var (
		parts  []string
		token  strings.Builder
		escape bool
	)

	flush := func() {
		appendICSLocationTokenParts(&parts, token.String())
		token.Reset()
	}

	for i := 0; i < len(value); i++ {
		ch := value[i]
		if escape {
			token.WriteByte(ch)
			escape = false
			continue
		}

		switch ch {
		case '\\':
			token.WriteByte(ch)
			escape = true
		case '\r', '\n', ',':
			flush()
		default:
			token.WriteByte(ch)
		}
	}
	flush()

	return parts
}

func cleanVenueLocationParts(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	lines := strings.Split(value, "\n")
	parts := make([]string, 0, len(lines))
	for _, line := range lines {
		for _, part := range strings.Split(line, ",") {
			part = strings.Join(strings.Fields(strings.TrimSpace(part)), " ")
			if part != "" {
				parts = append(parts, part)
			}
		}
	}
	return parts
}

func shouldUseRawVenueLocationParts(rawParts, cleanedParts []string) bool {
	switch {
	case len(rawParts) == 0:
		return false
	case len(rawParts) > 1:
		return true
	case len(cleanedParts) <= 1:
		return true
	case len(cleanedParts) == 2:
		return !looksAddressStructured(cleanedParts[1])
	default:
		return false
	}
}

func looksAddressStructured(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	if strings.Contains(value, "sheffield") {
		return true
	}
	for _, r := range value {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

func appendICSLocationTokenParts(parts *[]string, token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}

	decoded := cleanICSValue(token)
	decoded = strings.ReplaceAll(decoded, "\r\n", "\n")
	decoded = strings.ReplaceAll(decoded, "\r", "\n")
	for _, line := range strings.Split(decoded, "\n") {
		line = strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		if line != "" {
			*parts = append(*parts, line)
		}
	}
}
