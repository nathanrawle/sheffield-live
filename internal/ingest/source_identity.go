package ingest

import (
	"net"
	"net/url"
	"sort"
	"strings"
)

const (
	sourceIdentityKindUID = "uid"
	sourceIdentityKindURL = "url"
)

type SourceIdentity struct {
	Key        string
	Kind       string
	Raw        string
	Normalized string
}

type SourceIdentityInput struct {
	ExternalID  string
	SourceURL   string
	CalendarURL string
}

type SourceIdentitySet struct {
	identities []SourceIdentity
}

func (s SourceIdentitySet) Keys() []string {
	keys := make([]string, 0, len(s.identities))
	for _, identity := range s.identities {
		keys = append(keys, identity.Key)
	}
	return keys
}

func (s SourceIdentitySet) LookupKeys() []string {
	return SourceIdentityLookupKeys(s.Keys())
}

func (s SourceIdentitySet) PrimaryKey() string {
	if len(s.identities) == 0 {
		return ""
	}
	return s.identities[0].Key
}

func SourceIdentities(input SourceIdentityInput) SourceIdentitySet {
	identities := make([]SourceIdentity, 0, 3)
	seen := make(map[string]struct{}, 3)

	add := func(kind, raw, normalized string) {
		raw = strings.TrimSpace(raw)
		normalized = strings.TrimSpace(normalized)
		if raw == "" || normalized == "" {
			return
		}
		key := kind + ":" + normalized
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		identities = append(identities, SourceIdentity{
			Key:        key,
			Kind:       kind,
			Raw:        raw,
			Normalized: normalized,
		})
	}

	if raw := strings.TrimSpace(input.ExternalID); raw != "" {
		if normalized, ok := normalizeSourceIdentityURL(raw); ok {
			add(sourceIdentityKindURL, raw, normalized)
		} else if !isAbsoluteHTTPURL(raw) {
			add(sourceIdentityKindUID, raw, raw)
		}
	}

	if raw := strings.TrimSpace(input.SourceURL); raw != "" {
		if normalized, ok := NormalizeEventIdentityURL(raw); ok {
			add(sourceIdentityKindURL, raw, normalized)
		}
	}

	if raw := strings.TrimSpace(input.CalendarURL); raw != "" {
		if normalized, ok := normalizeAllowedCalendarIdentityURL(raw); ok {
			add(sourceIdentityKindURL, raw, normalized)
		}
	}

	return SourceIdentitySet{identities: identities}
}

func SourceIdentitiesFromKeys(keys []string) SourceIdentitySet {
	identities := make([]SourceIdentity, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		identity := SourceIdentity{
			Key:        key,
			Raw:        key,
			Normalized: key,
		}
		if prefix, _, ok := strings.Cut(key, ":"); ok {
			switch prefix {
			case sourceIdentityKindUID, sourceIdentityKindURL:
				identity.Kind = prefix
			}
		}
		identities = append(identities, identity)
	}

	return SourceIdentitySet{identities: identities}
}

func SourceIdentityLookupKeys(keys []string) []string {
	lookupKeys := make([]string, 0, len(keys)*2)
	seen := make(map[string]struct{}, len(keys)*2)

	add := func(key string) {
		key = strings.TrimSpace(key)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		lookupKeys = append(lookupKeys, key)
	}

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		add(key)
		prefix, value, ok := strings.Cut(key, ":")
		if !ok {
			continue
		}
		switch prefix {
		case sourceIdentityKindUID, sourceIdentityKindURL:
			add(value)
		}
	}

	return lookupKeys
}

func NormalizeDetailURL(raw string) (string, bool) {
	return normalizeSourceURL(raw, false)
}

func NormalizeEventIdentityURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() {
		return "", false
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return "", false
	}
	if strings.Trim(strings.ToLower(strings.TrimSpace(parsed.EscapedPath())), "/") == "" {
		return "", false
	}
	if IsCalendarURL(raw) {
		return "", false
	}
	if looksLikeFallbackSourceURL(parsed) {
		return "", false
	}

	return normalizeSourceURL(raw, false)
}

func normalizeSourceIdentityURL(raw string) (string, bool) {
	if normalized, ok := NormalizeEventIdentityURL(raw); ok {
		return normalized, true
	}
	if normalized, ok := normalizeAllowedCalendarIdentityURL(raw); ok {
		return normalized, true
	}
	return "", false
}

func SourceIdentityKey(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	if normalized, ok := normalizeSourceIdentityURL(raw); ok {
		return sourceIdentityKindURL + ":" + normalized, true
	}
	if isAbsoluteHTTPURL(raw) {
		return "", false
	}
	return sourceIdentityKindUID + ":" + raw, true
}

func normalizeAllowedCalendarIdentityURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() {
		return "", false
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return "", false
	}
	if !isSidneyAndMatildaCalendarURL(parsed) {
		return "", false
	}
	return normalizeSourceURL(raw, false)
}

func isSidneyAndMatildaCalendarURL(parsed *url.URL) bool {
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host != "sidneyandmatilda.com" && host != "www.sidneyandmatilda.com" {
		return false
	}

	path := strings.Trim(strings.ToLower(strings.TrimSpace(parsed.EscapedPath())), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] != "events" || parts[1] == "" {
		return false
	}
	if isSidneyAndMatildaFeedShapedEventPathSegment(parts[1]) {
		return false
	}

	query := parsed.Query()
	if len(query) != 1 {
		return false
	}
	return strings.EqualFold(query.Get("format"), "ical")
}

func isSidneyAndMatildaFeedShapedEventPathSegment(segment string) bool {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "ical.ics", "ical.ical":
		return true
	default:
		return false
	}
}

func normalizeSourceURL(raw string, allowFallback bool) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() {
		return "", false
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return "", false
	}
	if !allowFallback && looksLikeFallbackSourceURL(parsed) {
		return "", false
	}

	normalized := &url.URL{
		Scheme: strings.ToLower(parsed.Scheme),
		User:   parsed.User,
		Path:   normalizeSourceURLPath(parsed.Path),
	}

	host := strings.ToLower(parsed.Hostname())
	if port := parsed.Port(); port != "" {
		if !isDefaultHTTPPort(normalized.Scheme, port) {
			host = net.JoinHostPort(host, port)
		}
	}
	normalized.Host = host
	normalized.RawQuery = normalizeSourceURLQuery(parsed.Query())

	return normalized.String(), true
}

func normalizeSourceURLPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	path = strings.TrimRight(path, "/")
	if path == "" {
		return "/"
	}
	return path
}

func normalizeSourceURLQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	filtered := make(url.Values, len(values))
	for key, items := range values {
		if isTrackingQueryParam(key) {
			continue
		}
		filtered[key] = append([]string(nil), items...)
		sort.Strings(filtered[key])
	}
	if len(filtered) == 0 {
		return ""
	}
	return filtered.Encode()
}

func isTrackingQueryParam(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return false
	}
	if strings.HasPrefix(key, "utm_") {
		return true
	}
	switch key {
	case "fbclid", "gclid", "gbraid", "wbraid", "mc_cid", "mc_eid":
		return true
	default:
		return false
	}
}

func isDefaultHTTPPort(scheme, port string) bool {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "http":
		return port == "80"
	case "https":
		return port == "443"
	default:
		return false
	}
}

func isAbsoluteHTTPURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !parsed.IsAbs() {
		return false
	}
	return strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")
}

func looksLikeFallbackSourceURL(parsed *url.URL) bool {
	path := strings.Trim(strings.ToLower(strings.TrimSpace(parsed.EscapedPath())), "/")
	if path == "" {
		return false
	}
	if strings.HasSuffix(path, ".ics") || strings.HasSuffix(path, ".ical") {
		return true
	}

	segments := strings.Split(path, "/")
	switch segments[0] {
	case "calendar", "calendars", "feed", "feeds", "listing", "listings", "live":
		return len(segments) == 1
	case "events":
		return len(segments) == 1
	case "event":
		return len(segments) == 1
	default:
		return false
	}
}
