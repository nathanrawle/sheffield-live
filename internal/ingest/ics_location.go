package ingest

import "strings"

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
	return cleanVenueLocationParts(cleanICSValue(value))
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
