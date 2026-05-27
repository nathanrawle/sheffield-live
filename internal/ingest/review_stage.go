package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/eventidentity"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

const reviewStageDefaultSourceName = "Sidney & Matilda manual ingest"
const reviewStageStagingKeyVersion = 1

type ReviewStageClusterInput struct {
	ImportRunID                 int64
	Title                       string
	SourceName                  string
	SourceURL                   string
	Notes                       string
	StagingKey                  string
	StagingKeyVersion           int
	AuthoritativeSourceName     string
	AuthoritativeSourceURL      string
	AuthoritativeSourceEventKey string
	Candidates                  []review.CandidateInput
}

type reviewStagePreparedCandidate struct {
	Calendar  CalendarReport
	Candidate EventCandidate
}

type reviewStageUIDSafety struct {
	unsafe map[string]struct{}
}

func ReviewClustersFromReport(report Report) []ReviewStageClusterInput {
	catalog, err := DefaultCatalog()
	if err != nil {
		return nil
	}
	return ReviewClustersFromReportWithCatalog(catalog, report)
}

func ReviewClustersFromReportWithCatalog(catalog *Catalog, report Report) []ReviewStageClusterInput {
	if catalog == nil {
		return nil
	}
	if report.Status != importStatusSucceeded {
		return nil
	}

	clusters := make(map[string]*ReviewStageClusterInput)
	var order []string
	preparedCandidates := reviewStagePreparedCandidates(catalog, report)
	uidSafety := newReviewStageUIDSafety(preparedCandidates)

	for _, prepared := range preparedCandidates {
		calendar := prepared.Calendar
		candidate := prepared.Candidate
		key, ok := reviewStageKey(catalog, report.Source, candidate, uidSafety)
		if !ok {
			continue
		}

		cluster, exists := clusters[key]
		if !exists {
			cluster = &ReviewStageClusterInput{
				SourceName: reviewStageSourceName(catalog, report),
				SourceURL:  reviewStageFirstNonEmpty(calendar.URL, report.SourceURL),
				Notes:      reviewStageNotes(report),
			}
			clusters[key] = cluster
			order = append(order, key)
		}

		cluster.Candidates = append(cluster.Candidates, reviewStageCandidateInput(catalog, report, calendar, candidate, uidSafety))
	}

	result := make([]ReviewStageClusterInput, 0, len(order))
	for _, key := range order {
		cluster := clusters[key]
		cluster.ImportRunID = report.ImportRunID
		cluster.Title = reviewStageTitle(cluster.Candidates)
		cluster.StagingKey = reviewStageStagingKeyFromCandidates(cluster.Candidates)
		cluster.StagingKeyVersion = reviewStageStagingKeyVersion
		cluster.AuthoritativeSourceName, cluster.AuthoritativeSourceURL, cluster.AuthoritativeSourceEventKey = reviewStageAuthoritativeSource(catalog, report.Source, cluster.SourceName, cluster.SourceURL, cluster.Candidates)
		result = append(result, *cluster)
	}
	return result
}

func reviewStagePreparedCandidates(catalog *Catalog, report Report) []reviewStagePreparedCandidate {
	var out []reviewStagePreparedCandidate
	for _, calendar := range report.Calendars {
		for _, candidate := range calendar.Candidates {
			candidate.Summary = cleanEventCandidateSummaryForCatalog(catalog, report.Source, candidate)
			out = append(out, reviewStagePreparedCandidate{
				Calendar:  calendar,
				Candidate: candidate,
			})
		}
	}
	return out
}

func newReviewStageUIDSafety(candidates []reviewStagePreparedCandidate) reviewStageUIDSafety {
	startsByUID := make(map[string]map[string]struct{})
	for _, prepared := range candidates {
		uid := strings.TrimSpace(prepared.Candidate.UID)
		if uid == "" {
			continue
		}
		startAt, ok := reviewStageNormalizedStartAt(prepared.Candidate.StartAt)
		if !ok {
			continue
		}
		if startsByUID[uid] == nil {
			startsByUID[uid] = make(map[string]struct{})
		}
		startsByUID[uid][startAt] = struct{}{}
	}

	unsafe := make(map[string]struct{})
	for uid, starts := range startsByUID {
		if len(starts) > 1 {
			unsafe[uid] = struct{}{}
		}
	}
	return reviewStageUIDSafety{unsafe: unsafe}
}

func (s reviewStageUIDSafety) isUnsafe(uid string) bool {
	uid = strings.TrimSpace(uid)
	if uid == "" || len(s.unsafe) == 0 {
		return false
	}
	_, ok := s.unsafe[uid]
	return ok
}

func reviewStageNormalizedStartAt(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", false
	}
	return parsed.UTC().Format(time.RFC3339), true
}

func reviewStageAuthoritativeSource(catalog *Catalog, source, baseSourceName, baseSourceURL string, candidates []review.CandidateInput) (string, string, string) {
	ownedVenueSlugs := catalog.OwnedVenueSlugsForSource(source)
	if len(ownedVenueSlugs) == 0 || len(candidates) == 0 {
		return "", "", ""
	}
	ownedVenueSet := make(map[string]struct{}, len(ownedVenueSlugs))
	for _, slug := range ownedVenueSlugs {
		slug = strings.TrimSpace(slug)
		if slug == "" {
			continue
		}
		ownedVenueSet[slug] = struct{}{}
	}
	if len(ownedVenueSet) == 0 {
		return "", "", ""
	}

	var authoritativeSourceName string
	var authoritativeSourceURL string
	var authoritativeSourceEventKey string
	var authoritativeVenueSlug string
	for _, candidate := range candidates {
		venueSlug := strings.TrimSpace(candidate.VenueSlug)
		if _, ok := ownedVenueSet[venueSlug]; !ok {
			return "", "", ""
		}
		if authoritativeVenueSlug == "" {
			authoritativeVenueSlug = venueSlug
		} else if venueSlug != authoritativeVenueSlug {
			return "", "", ""
		}
		candidateEventKey := reviewStageAuthoritativeSourceEventKey(candidate)
		if candidateEventKey == "" {
			return "", "", ""
		}
		candidateSourceName := reviewStageFirstNonEmpty(candidate.SourceName, baseSourceName, authoritativeSourceName)
		candidateSourceURL := reviewStageAuthoritativeSourceURL(candidate, baseSourceURL, authoritativeSourceURL)
		if candidateSourceName == "" || candidateSourceURL == "" {
			return "", "", ""
		}
		if authoritativeSourceEventKey == "" {
			authoritativeSourceName = candidateSourceName
			authoritativeSourceURL = candidateSourceURL
			authoritativeSourceEventKey = candidateEventKey
			continue
		}
		if candidateSourceName != authoritativeSourceName || candidateSourceURL != authoritativeSourceURL || candidateEventKey != authoritativeSourceEventKey {
			return "", "", ""
		}
	}
	return authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey
}

func reviewStageAuthoritativeSourceURL(candidate review.CandidateInput, baseSourceURL, authoritativeSourceURL string) string {
	var values []string
	if !candidate.CalendarURLSourceIdentityDisabled {
		values = append(values, candidate.CalendarURL)
	}
	if !candidate.SourceURLSourceIdentityDisabled {
		values = append(values, candidate.SourceURL)
	}
	values = append(values, baseSourceURL, authoritativeSourceURL)
	return reviewStageFirstNonEmpty(values...)
}

func reviewStageAuthoritativeSourceEventKey(candidate review.CandidateInput) string {
	if !candidate.ExternalIDSourceIdentityDisabled {
		if key, ok := SourceIdentityKey(candidate.ExternalID); ok {
			return key
		}
	}
	if !candidate.SourceURLSourceIdentityDisabled && (candidate.ExternalIDSourceIdentityDisabled || reviewStageHasRealSourceURL(candidate.Provenance)) {
		if normalized, ok := NormalizeEventIdentityURL(candidate.SourceURL); ok {
			return "url:" + normalized
		}
	}
	if !candidate.CalendarURLSourceIdentityDisabled {
		if key, ok := SourceIdentityKey(candidate.CalendarURL); ok {
			return key
		}
	}
	return ""
}

func reviewStageCandidateSourceIdentityInput(candidate review.CandidateInput) SourceIdentityInput {
	input := SourceIdentityInput{
		ExternalID:  strings.TrimSpace(candidate.ExternalID),
		SourceURL:   strings.TrimSpace(candidate.SourceURL),
		CalendarURL: strings.TrimSpace(candidate.CalendarURL),
	}
	if candidate.ExternalIDSourceIdentityDisabled {
		input.ExternalID = ""
	}
	if candidate.SourceURLSourceIdentityDisabled {
		input.SourceURL = ""
	}
	if candidate.CalendarURLSourceIdentityDisabled {
		input.CalendarURL = ""
	}
	return input
}

func reviewStageCandidateSourceIdentities(candidate review.CandidateInput) SourceIdentitySet {
	return SourceIdentities(reviewStageCandidateSourceIdentityInput(candidate))
}

func reviewStageCandidateSourceIdentityKey(candidate review.CandidateInput) (string, bool) {
	identities := reviewStageCandidateSourceIdentities(candidate)
	if key := identities.PrimaryKey(); key != "" {
		return key, true
	}
	return "", false
}

func reviewStageHasRealSourceURL(provenance string) bool {
	for _, part := range strings.Split(strings.TrimSpace(provenance), ";") {
		if strings.HasPrefix(strings.TrimSpace(part), "URL ") {
			return true
		}
	}
	return false
}

func reviewStageStagingKey(cluster ReviewStageClusterInput) string {
	return reviewStageStagingKeyFromCandidates(cluster.Candidates)
}

func reviewStageStagingKeyFromCandidates(candidates []review.CandidateInput) string {
	candidateFingerprints := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidateFingerprints = append(candidateFingerprints, reviewStageCandidateFingerprint(candidate))
	}
	sort.Strings(candidateFingerprints)

	sum := sha256.New()
	writeReviewStageHashPart(sum, "review-stage-group:v2")
	for _, fingerprint := range candidateFingerprints {
		writeReviewStageHashPart(sum, fingerprint)
	}
	return "v1:" + hex.EncodeToString(sum.Sum(nil))
}

func reviewStageCandidateFingerprint(candidate review.CandidateInput) string {
	sum := sha256.New()
	writeReviewStageHashPart(sum, "review-stage-candidate:v2")
	writeReviewStageHashPart(sum, candidate.ExternalID)
	writeReviewStageHashPart(sum, candidate.Name)
	writeReviewStageHashPart(sum, candidate.VenueSlug)
	writeReviewStageHashPart(sum, candidate.StartAt)
	writeReviewStageHashPart(sum, candidate.EndAt)
	writeReviewStageHashPart(sum, candidate.Genre)
	writeReviewStageHashPart(sum, candidate.Status)
	writeReviewStageHashPart(sum, candidate.Description)
	return hex.EncodeToString(sum.Sum(nil))
}

func writeReviewStageHashPart(sum interface{ Write([]byte) (int, error) }, value string) {
	_, _ = fmt.Fprintf(sum, "%d:%s\x00", len(value), value)
}

func reviewStageKey(catalog *Catalog, source string, candidate EventCandidate, uidSafety reviewStageUIDSafety) (string, bool) {
	if uid := strings.TrimSpace(candidate.UID); uid != "" {
		if uidSafety.isUnsafe(uid) {
			if startAt, ok := reviewStageNormalizedStartAt(candidate.StartAt); ok {
				return strings.Join([]string{
					"unsafe_uid_occurrence",
					uid,
					startAt,
					reviewStageVenueSlug(catalog, source, candidate),
				}, "\x00"), true
			}
		} else {
			return "uid\x00" + uid, true
		}
	}

	summary := normalizeReviewStageText(candidate.Summary)
	startAt := strings.TrimSpace(candidate.StartAt)
	if summary == "" || startAt == "" {
		return "", false
	}

	return strings.Join([]string{
		"fallback",
		summary,
		startAt,
		reviewStageVenueSlug(catalog, source, candidate),
	}, "\x00"), true
}

func reviewStageCandidateInput(catalog *Catalog, report Report, calendar CalendarReport, candidate EventCandidate, uidSafety reviewStageUIDSafety) review.CandidateInput {
	sourceURL := reviewStageOfficialSourceURL(report, calendar, candidate)
	calendarURL := reviewStageCalendarURL(calendar, candidate)
	externalIDDisabled, sourceURLDisabled, calendarURLDisabled := reviewStageCandidateIdentityDisabledFlags(candidate, sourceURL, calendarURL, uidSafety)
	return review.CandidateInput{
		ExternalID:                        strings.TrimSpace(candidate.UID),
		ExternalIDSourceIdentityDisabled:  externalIDDisabled,
		Name:                              strings.TrimSpace(candidate.Summary),
		VenueSlug:                         reviewStageVenueSlug(catalog, report.Source, candidate),
		VenueText:                         strings.TrimSpace(candidate.Location),
		VenueLocationRaw:                  candidate.LocationRaw,
		RoomText:                          strings.TrimSpace(candidate.RoomText),
		Rooms:                             reviewStageRooms(catalog, report.Source, candidate),
		StartAt:                           strings.TrimSpace(candidate.StartAt),
		EndAt:                             strings.TrimSpace(candidate.EndAt),
		Genre:                             "",
		Status:                            reviewStageStatus(candidate.Status),
		Description:                       strings.TrimSpace(candidate.Description),
		ImageURL:                          strings.TrimSpace(candidate.ImageURL),
		ImageSourceURL:                    strings.TrimSpace(candidate.ImageSourceURL),
		ImageAlt:                          strings.TrimSpace(candidate.ImageAlt),
		ImageWidth:                        candidate.ImageWidth,
		ImageHeight:                       candidate.ImageHeight,
		ImageFocusX:                       candidate.ImageFocusX,
		ImageFocusY:                       candidate.ImageFocusY,
		SourceName:                        reviewStageSourceName(catalog, report),
		SourceURL:                         sourceURL,
		SourceURLSourceIdentityDisabled:   sourceURLDisabled,
		CalendarURL:                       calendarURL,
		CalendarURLSourceIdentityDisabled: calendarURLDisabled,
		Provenance:                        reviewStageProvenance(report, calendar, candidate),
	}
}

func reviewStageCandidateIdentityDisabledFlags(candidate EventCandidate, sourceURL, calendarURL string, uidSafety reviewStageUIDSafety) (bool, bool, bool) {
	if !uidSafety.isUnsafe(candidate.UID) {
		return false, false, false
	}

	sourceURLDisabled := true
	candidateURL := strings.TrimSpace(candidate.URL)
	if candidateURL != "" && strings.TrimSpace(sourceURL) == candidateURL {
		if _, ok := NormalizeEventIdentityURL(candidateURL); ok {
			sourceURLDisabled = false
		}
	}

	calendarURLDisabled := true
	if _, ok := normalizeAllowedCalendarIdentityURL(calendarURL); ok {
		calendarURLDisabled = false
	}

	return true, sourceURLDisabled, calendarURLDisabled
}

func reviewStageRooms(catalog *Catalog, source string, candidate EventCandidate) []domain.VenueRoom {
	venueSlug := reviewStageVenueSlug(catalog, source, candidate)
	rooms := make([]domain.VenueRoom, 0, len(candidate.Rooms))
	for _, room := range candidate.Rooms {
		slug := strings.TrimSpace(room.Slug)
		if slug == "" {
			slug = VenueSlugFromText(room.Name)
		}
		name := strings.TrimSpace(room.Name)
		if name == "" {
			name = slug
		}
		if venueSlug == "" || slug == "" || name == "" {
			continue
		}
		rooms = append(rooms, domain.VenueRoom{
			VenueSlug: venueSlug,
			Slug:      slug,
			Name:      name,
		})
	}
	return rooms
}

func reviewStageOfficialSourceURL(report Report, calendar CalendarReport, candidate EventCandidate) string {
	if candidateURL := strings.TrimSpace(candidate.URL); candidateURL != "" && !IsCalendarURL(candidateURL) {
		return candidateURL
	}
	if calendarURL := strings.TrimSpace(calendar.URL); calendarURL != "" && !IsCalendarURL(calendarURL) {
		return URLWithTextFragment(calendarURL, candidate.Summary)
	}
	sourceURL := strings.TrimSpace(report.SourceURL)
	if sourceURL == "" || IsCalendarURL(sourceURL) {
		return sourceURL
	}
	return URLWithTextFragment(sourceURL, candidate.Summary)
}

func reviewStageCalendarURL(calendar CalendarReport, candidate EventCandidate) string {
	candidateURL := strings.TrimSpace(candidate.URL)
	if _, ok := normalizeAllowedCalendarIdentityURL(candidateURL); ok {
		return candidateURL
	}
	for _, value := range []string{calendar.URL, candidate.URL} {
		value = strings.TrimSpace(value)
		if IsCalendarURL(value) {
			return value
		}
	}
	return ""
}

func reviewStageTitle(candidates []review.CandidateInput) string {
	prefix := "Duplicate review"
	if len(candidates) == 1 {
		prefix = "New listing review"
	}

	for _, candidate := range candidates {
		if name := normalizeReviewStageDisplay(candidate.Name); name != "" {
			return prefix + ": " + name
		}
	}
	return prefix
}

func reviewStageSourceName(catalog *Catalog, report Report) string {
	return catalog.ReviewStageSourceNameForSource(report.Source)
}

func reviewStageNotes(report Report) string {
	if report.ImportRunID == 0 {
		return "Created from manual ingest review staging."
	}
	return fmt.Sprintf("Created from manual ingest run %d review staging.", report.ImportRunID)
}

func reviewStageStatus(status string) string {
	status = strings.TrimSpace(status)
	if strings.EqualFold(status, "CONFIRMED") {
		return "Listed"
	}
	return status
}

func reviewStageProvenance(report Report, calendar CalendarReport, candidate EventCandidate) string {
	var parts []string
	if report.ImportRunID != 0 {
		parts = append(parts, fmt.Sprintf("import run %d", report.ImportRunID))
	}
	if calendar.URL != "" {
		parts = append(parts, "calendar "+calendar.URL)
	}
	if candidate.UID != "" {
		parts = append(parts, "UID "+candidate.UID)
	} else if candidate.URL != "" {
		parts = append(parts, "URL "+candidate.URL)
	}
	if len(parts) == 0 {
		return "manual ingest"
	}
	return strings.Join(parts, "; ")
}

func normalizeReviewStageText(value string) string {
	return strings.ToLower(normalizeReviewStageDisplay(value))
}

func normalizeReviewStageDisplay(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func reviewStageFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func reviewStageVenueSlug(catalog *Catalog, source string, candidate EventCandidate) string {
	value := reviewStageVenueSlugValue(candidate)
	if catalog == nil {
		return VenueSlugFromText(value)
	}
	return catalog.VenueSlugForSourceLocation(source, value)
}

func reviewStageVenueSlugValue(candidate EventCandidate) string {
	if head := VenueLocationEvidenceHead(candidate.LocationRaw); head != "" {
		return head
	}
	return strings.TrimSpace(candidate.Location)
}

type reviewStageEventReviewEvidencePayload struct {
	GroupTitle                                 string                    `json:"group_title"`
	GroupSourceName                            string                    `json:"group_source_name"`
	GroupSourceURL                             string                    `json:"group_source_url"`
	SourceAuthority                            string                    `json:"source_authority"`
	GroupAuthoritativeSourceName               string                    `json:"group_authoritative_source_name,omitempty"`
	GroupAuthoritativeSourceURL                string                    `json:"group_authoritative_source_url,omitempty"`
	GroupAuthoritativeSourceEventKey           string                    `json:"group_authoritative_source_event_key,omitempty"`
	GroupNotes                                 string                    `json:"group_notes,omitempty"`
	SourceName                                 string                    `json:"source_name"`
	SourceURL                                  string                    `json:"source_url,omitempty"`
	CalendarURL                                string                    `json:"calendar_url,omitempty"`
	Provenance                                 string                    `json:"provenance,omitempty"`
	CandidateExternalID                        string                    `json:"candidate_external_id,omitempty"`
	CandidateExternalIDSourceIdentityDisabled  bool                      `json:"candidate_external_id_source_identity_disabled,omitempty"`
	CandidateTitle                             string                    `json:"candidate_title,omitempty"`
	CandidateVenueSlug                         string                    `json:"candidate_venue_slug,omitempty"`
	CandidateVenueText                         string                    `json:"candidate_venue_text,omitempty"`
	CandidateVenueLocationRaw                  string                    `json:"candidate_venue_location_raw,omitempty"`
	CandidateRoomText                          string                    `json:"candidate_room_text,omitempty"`
	CandidateRooms                             []reviewStageEvidenceRoom `json:"candidate_rooms,omitempty"`
	CandidateStartAt                           string                    `json:"candidate_start_at,omitempty"`
	CandidateEndAt                             string                    `json:"candidate_end_at,omitempty"`
	CandidateGenre                             string                    `json:"candidate_genre,omitempty"`
	CandidateStatus                            string                    `json:"candidate_status,omitempty"`
	CandidateDescription                       string                    `json:"candidate_description,omitempty"`
	CandidateImageURL                          string                    `json:"candidate_image_url,omitempty"`
	CandidateImageSourceURL                    string                    `json:"candidate_image_source_url,omitempty"`
	CandidateImageAlt                          string                    `json:"candidate_image_alt,omitempty"`
	CandidateImageWidth                        int                       `json:"candidate_image_width,omitempty"`
	CandidateImageHeight                       int                       `json:"candidate_image_height,omitempty"`
	CandidateImageFocusX                       int                       `json:"candidate_image_focus_x,omitempty"`
	CandidateImageFocusY                       int                       `json:"candidate_image_focus_y,omitempty"`
	CandidateSourceURLSourceIdentityDisabled   bool                      `json:"candidate_source_url_source_identity_disabled,omitempty"`
	CandidateCalendarURLSourceIdentityDisabled bool                      `json:"candidate_calendar_url_source_identity_disabled,omitempty"`
}

type reviewStageEvidenceRoom struct {
	VenueSlug string `json:"venue_slug"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
}

func ReviewStageClusterEventReviewEvidenceInputs(cluster ReviewStageClusterInput) []seedstore.StageEventReviewEvidenceInput {
	inputs := make([]seedstore.StageEventReviewEvidenceInput, 0, len(cluster.Candidates))
	for _, candidate := range cluster.Candidates {
		inputs = append(inputs, reviewStageClusterEventReviewEvidenceInput(cluster, candidate))
	}
	return inputs
}

func reviewStageClusterEventReviewEvidenceInput(cluster ReviewStageClusterInput, candidate review.CandidateInput) seedstore.StageEventReviewEvidenceInput {
	sourceName, sourceURL, sourceAuthority := reviewStageClusterEvidenceSourceRef(cluster, candidate)
	payloadSourceName := reviewStageFirstNonEmpty(candidate.SourceName, cluster.SourceName)
	payloadSourceURL := reviewStageFirstNonEmpty(candidate.SourceURL, candidate.CalendarURL, cluster.SourceURL)

	payload := reviewStageEventReviewEvidencePayload{
		GroupTitle:                       strings.TrimSpace(cluster.Title),
		GroupSourceName:                  strings.TrimSpace(cluster.SourceName),
		GroupSourceURL:                   strings.TrimSpace(cluster.SourceURL),
		SourceAuthority:                  string(sourceAuthority),
		GroupAuthoritativeSourceName:     strings.TrimSpace(cluster.AuthoritativeSourceName),
		GroupAuthoritativeSourceURL:      strings.TrimSpace(cluster.AuthoritativeSourceURL),
		GroupAuthoritativeSourceEventKey: strings.TrimSpace(cluster.AuthoritativeSourceEventKey),
		GroupNotes:                       strings.TrimSpace(cluster.Notes),
		SourceName:                       payloadSourceName,
		SourceURL:                        payloadSourceURL,
		CalendarURL:                      strings.TrimSpace(candidate.CalendarURL),
		Provenance:                       strings.TrimSpace(candidate.Provenance),
		CandidateExternalID:              strings.TrimSpace(candidate.ExternalID),
		CandidateExternalIDSourceIdentityDisabled: candidate.ExternalIDSourceIdentityDisabled,
		CandidateTitle:                             strings.TrimSpace(candidate.Name),
		CandidateVenueSlug:                         strings.TrimSpace(candidate.VenueSlug),
		CandidateVenueText:                         strings.TrimSpace(candidate.VenueText),
		CandidateVenueLocationRaw:                  strings.TrimSpace(candidate.VenueLocationRaw),
		CandidateRoomText:                          strings.TrimSpace(candidate.RoomText),
		CandidateRooms:                             reviewStageEvidenceRooms(candidate.Rooms),
		CandidateStartAt:                           strings.TrimSpace(candidate.StartAt),
		CandidateEndAt:                             strings.TrimSpace(candidate.EndAt),
		CandidateGenre:                             strings.TrimSpace(candidate.Genre),
		CandidateStatus:                            strings.TrimSpace(candidate.Status),
		CandidateDescription:                       strings.TrimSpace(candidate.Description),
		CandidateImageURL:                          strings.TrimSpace(candidate.ImageURL),
		CandidateImageSourceURL:                    strings.TrimSpace(candidate.ImageSourceURL),
		CandidateImageAlt:                          strings.TrimSpace(candidate.ImageAlt),
		CandidateImageWidth:                        candidate.ImageWidth,
		CandidateImageHeight:                       candidate.ImageHeight,
		CandidateImageFocusX:                       candidate.ImageFocusX,
		CandidateImageFocusY:                       candidate.ImageFocusY,
		CandidateSourceURLSourceIdentityDisabled:   candidate.SourceURLSourceIdentityDisabled,
		CandidateCalendarURLSourceIdentityDisabled: candidate.CalendarURLSourceIdentityDisabled,
	}
	payloadBytes, _ := json.Marshal(payload)

	sourceIdentityKeys, exactIdentityKeys, weakEvidence, weakReason := reviewStageClusterEvidenceIdentityKeys(cluster, candidate)
	fingerprintMaterialBytes, _ := json.Marshal(reviewStageEventReviewEvidenceFingerprintMaterial{
		SourceAuthority:     string(sourceAuthority),
		SourceURL:           sourceURL,
		CalendarURL:         strings.TrimSpace(candidate.CalendarURL),
		Provenance:          strings.TrimSpace(candidate.Provenance),
		CandidateExternalID: strings.TrimSpace(candidate.ExternalID),
		CandidateExternalIDSourceIdentityDisabled: candidate.ExternalIDSourceIdentityDisabled,
		CandidateTitle:                             normalizeReviewStageDisplay(candidate.Name),
		CandidateVenueSlug:                         strings.TrimSpace(candidate.VenueSlug),
		CandidateVenueText:                         strings.TrimSpace(candidate.VenueText),
		CandidateVenueLocationRaw:                  strings.TrimSpace(candidate.VenueLocationRaw),
		CandidateRoomText:                          strings.TrimSpace(candidate.RoomText),
		CandidateRooms:                             reviewStageEvidenceRooms(candidate.Rooms),
		CandidateStartAt:                           strings.TrimSpace(candidate.StartAt),
		CandidateEndAt:                             strings.TrimSpace(candidate.EndAt),
		CandidateGenre:                             strings.TrimSpace(candidate.Genre),
		CandidateStatus:                            strings.TrimSpace(candidate.Status),
		CandidateDescription:                       strings.TrimSpace(candidate.Description),
		CandidateImageURL:                          strings.TrimSpace(candidate.ImageURL),
		CandidateImageSourceURL:                    strings.TrimSpace(candidate.ImageSourceURL),
		CandidateImageAlt:                          strings.TrimSpace(candidate.ImageAlt),
		CandidateImageWidth:                        candidate.ImageWidth,
		CandidateImageHeight:                       candidate.ImageHeight,
		CandidateImageFocusX:                       candidate.ImageFocusX,
		CandidateImageFocusY:                       candidate.ImageFocusY,
		CandidateSourceURLSourceIdentityDisabled:   candidate.SourceURLSourceIdentityDisabled,
		CandidateCalendarURLSourceIdentityDisabled: candidate.CalendarURLSourceIdentityDisabled,
		SourceIdentityKeys:                         reviewStageUniqueSortedKeys(sourceIdentityKeys),
		ExactIdentityKeys:                          reviewStageUniqueSortedKeys(exactIdentityKeys),
	})

	return seedstore.StageEventReviewEvidenceInput{
		SourceName:          sourceName,
		SourceURL:           sourceURL,
		SourceAuthority:     sourceAuthority,
		StagingKey:          strings.TrimSpace(cluster.StagingKey),
		StagingKeyVersion:   cluster.StagingKeyVersion,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		Payload:             string(payloadBytes),
		EvidenceFingerprint: reviewStageEventReviewEvidenceFingerprint(fingerprintMaterialBytes),
		SourceIdentityKeys:  sourceIdentityKeys,
		ExactIdentityKeys:   exactIdentityKeys,
		WeakEvidence:        weakEvidence,
		WeakEvidenceReason:  weakReason,
	}
}

func reviewStageClusterEvidenceSourceRef(cluster ReviewStageClusterInput, candidate review.CandidateInput) (string, string, seedstore.SourceAuthority) {
	if authoritativeName := strings.TrimSpace(cluster.AuthoritativeSourceName); authoritativeName != "" {
		authoritativeURL := strings.TrimSpace(cluster.AuthoritativeSourceURL)
		authoritativeEventKey := strings.TrimSpace(cluster.AuthoritativeSourceEventKey)
		if authoritativeURL != "" && authoritativeEventKey != "" {
			return authoritativeName, authoritativeURL, seedstore.SourceAuthorityAuthoritative
		}
	}
	return reviewStageFirstNonEmpty(candidate.SourceName, cluster.SourceName), reviewStageFirstNonEmpty(candidate.SourceURL, candidate.CalendarURL, cluster.SourceURL), seedstore.SourceAuthoritySupporting
}

func reviewStageEvidenceRooms(rooms []domain.VenueRoom) []reviewStageEvidenceRoom {
	if len(rooms) == 0 {
		return nil
	}
	out := make([]reviewStageEvidenceRoom, 0, len(rooms))
	for _, room := range rooms {
		out = append(out, reviewStageEvidenceRoom{
			VenueSlug: strings.TrimSpace(room.VenueSlug),
			Slug:      strings.TrimSpace(room.Slug),
			Name:      strings.TrimSpace(room.Name),
		})
	}
	return out
}

func reviewStageClusterEvidenceIdentityKeys(cluster ReviewStageClusterInput, candidate review.CandidateInput) ([]string, []string, bool, string) {
	sourceIdentityKeys := reviewStageEventReviewSourceIdentityKeys(cluster, candidate)
	exactIdentityKeys := reviewStageExactIdentityKeys(candidate)
	if len(sourceIdentityKeys) == 0 && len(exactIdentityKeys) == 0 {
		return nil, nil, true, "missing source and exact identity keys"
	}
	return sourceIdentityKeys, exactIdentityKeys, false, ""
}

func reviewStageEventReviewSourceIdentityKeys(cluster ReviewStageClusterInput, candidate review.CandidateInput) []string {
	identities := reviewStageCandidateSourceIdentities(candidate)
	keys := append([]string(nil), identities.Keys()...)
	if key := strings.TrimSpace(cluster.AuthoritativeSourceEventKey); key != "" {
		keys = append(keys, key)
	}
	return reviewStageUniqueSortedKeys(keys)
}

func reviewStageExactIdentityKeys(candidate review.CandidateInput) []string {
	startAt := strings.TrimSpace(candidate.StartAt)
	venueSlug := strings.TrimSpace(candidate.VenueSlug)
	if startAt == "" || venueSlug == "" || strings.TrimSpace(candidate.Name) == "" {
		return nil
	}
	start, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		return nil
	}
	cleanTitle := strings.TrimSpace(CleanEventTitleForVenue(candidate.Name, venueSlug))
	if cleanTitle == "" {
		return nil
	}
	return []string{eventidentity.BuildKey(eventidentity.ExactKeyVersion, venueSlug, start.UTC(), cleanTitle)}
}

type reviewStageEventReviewEvidenceFingerprintMaterial struct {
	SourceAuthority                            string                    `json:"source_authority,omitempty"`
	SourceURL                                  string                    `json:"source_url,omitempty"`
	CalendarURL                                string                    `json:"calendar_url,omitempty"`
	Provenance                                 string                    `json:"provenance,omitempty"`
	CandidateExternalID                        string                    `json:"candidate_external_id,omitempty"`
	CandidateExternalIDSourceIdentityDisabled  bool                      `json:"candidate_external_id_source_identity_disabled,omitempty"`
	CandidateTitle                             string                    `json:"candidate_title,omitempty"`
	CandidateVenueSlug                         string                    `json:"candidate_venue_slug,omitempty"`
	CandidateVenueText                         string                    `json:"candidate_venue_text,omitempty"`
	CandidateVenueLocationRaw                  string                    `json:"candidate_venue_location_raw,omitempty"`
	CandidateRoomText                          string                    `json:"candidate_room_text,omitempty"`
	CandidateRooms                             []reviewStageEvidenceRoom `json:"candidate_rooms,omitempty"`
	CandidateStartAt                           string                    `json:"candidate_start_at,omitempty"`
	CandidateEndAt                             string                    `json:"candidate_end_at,omitempty"`
	CandidateGenre                             string                    `json:"candidate_genre,omitempty"`
	CandidateStatus                            string                    `json:"candidate_status,omitempty"`
	CandidateDescription                       string                    `json:"candidate_description,omitempty"`
	CandidateImageURL                          string                    `json:"candidate_image_url,omitempty"`
	CandidateImageSourceURL                    string                    `json:"candidate_image_source_url,omitempty"`
	CandidateImageAlt                          string                    `json:"candidate_image_alt,omitempty"`
	CandidateImageWidth                        int                       `json:"candidate_image_width,omitempty"`
	CandidateImageHeight                       int                       `json:"candidate_image_height,omitempty"`
	CandidateImageFocusX                       int                       `json:"candidate_image_focus_x,omitempty"`
	CandidateImageFocusY                       int                       `json:"candidate_image_focus_y,omitempty"`
	CandidateSourceURLSourceIdentityDisabled   bool                      `json:"candidate_source_url_source_identity_disabled,omitempty"`
	CandidateCalendarURLSourceIdentityDisabled bool                      `json:"candidate_calendar_url_source_identity_disabled,omitempty"`
	SourceIdentityKeys                         []string                  `json:"source_identity_keys,omitempty"`
	ExactIdentityKeys                          []string                  `json:"exact_identity_keys,omitempty"`
}

func reviewStageEventReviewEvidenceFingerprint(material []byte) string {
	sum := sha256.New()
	writeReviewStageHashPart(sum, "event-review-evidence:v1")
	writeReviewStageHashPart(sum, string(material))
	return "v1:" + hex.EncodeToString(sum.Sum(nil))
}

func reviewStageUniqueSortedKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
