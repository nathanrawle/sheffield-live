package eventidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const ExactKeyVersion = 1

func NormalizeCleanTitle(title string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(title)), " "))
}

func MaterialText(version int, venueSlug string, start time.Time, cleanTitle string) string {
	var builder strings.Builder
	writeMaterialPart(&builder, fmt.Sprintf("exact:v%d", version))
	writeMaterialPart(&builder, strings.TrimSpace(venueSlug))
	writeMaterialPart(&builder, start.UTC().Format(time.RFC3339))
	writeMaterialPart(&builder, NormalizeCleanTitle(cleanTitle))
	return builder.String()
}

func BuildKey(version int, venueSlug string, start time.Time, cleanTitle string) string {
	material := MaterialText(version, venueSlug, start, cleanTitle)
	sum := sha256.Sum256([]byte(material))
	return fmt.Sprintf("exact:v%d:%s", version, hex.EncodeToString(sum[:]))
}

func writeMaterialPart(w interface {
	Write([]byte) (int, error)
}, value string) {
	_, _ = fmt.Fprintf(w, "%d:%s\x00", len(value), value)
}
