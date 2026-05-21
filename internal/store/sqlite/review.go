package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func (s *Store) decorateEventForPublish(event domain.Event) domain.Event {
	return decorateEventForPublish(event, s.sourceMetadata)
}

func decorateEventForPublish(event domain.Event, sourceMetadata ingest.SourceMetadataLookup) domain.Event {
	links := decorateEventLinkValues(event.Name, event.SourceName, event.SourceURL, event.OfficialListingURL, event.CalendarURL, sourceMetadata)
	event.OfficialListingURL = links.officialListingURL
	event.CalendarURL = links.calendarURL
	return event
}

func recordEventObservationsForSourceIdentityContextTx(ctx context.Context, tx interface {
	execer
	queryer
}, scope seedstore.ObservationRunScope, sourceID int64, sourceCtx reviewSourceIdentityContext, authority seedstore.SourceAuthority, target eventRecord, incoming domain.Event) error {
	if scope == "" || sourceCtx.PrimaryObservationKey == "" {
		return nil
	}
	return recordEventObservationsTx(ctx, tx, scope, sourceID, sourceCtx.PrimaryObservationKey, authority, target, incoming)
}

func ensureEventSourceLinkForSourceIdentityContextTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID, sourceID int64, sourceCtx reviewSourceIdentityContext, authority sourceLinkAuthority, policy sourceLinkConflictPolicy, now time.Time) (sourceLinkWriteResult, error) {
	if len(sourceCtx.Identities.Keys()) == 0 {
		return sourceLinkWriteResult{TargetEventID: eventID, Reason: "no stable source identities"}, nil
	}
	return ensureEventSourceLinkTx(ctx, tx, eventID, sourceID, sourceCtx.Identities, authority, policy, now)
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return formatRFC3339UTC(value)
}

func writeStagedReviewCandidateFingerprintPart(sum interface{ Write([]byte) (int, error) }, value string) {
	_, _ = fmt.Fprintf(sum, "%d:%s\x00", len(value), value)
}

func reviewConsensusFields() []review.Field {
	return []review.Field{
		review.FieldName,
		review.FieldVenueSlug,
		review.FieldRoomSlugs,
		review.FieldStartAt,
		review.FieldEndAt,
		review.FieldGenre,
		review.FieldStatus,
		review.FieldDescription,
		review.FieldImageURL,
	}
}

func markEventReviewedTx(ctx context.Context, tx execer, eventID int64) error {
	if eventID <= 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE events
		SET publication_state = ?
		WHERE id = ?
	`, string(domain.PublicationStateReviewed), eventID)
	return err
}

func reviewCandidatesMatchingEvent(candidates []review.Candidate, event domain.Event) []review.Candidate {
	matching := make([]review.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if reviewCandidateMatchesEvent(candidate, event) {
			matching = append(matching, candidate)
		}
	}
	return matching
}

func reviewCandidateMatchesEvent(candidate review.Candidate, event domain.Event) bool {
	if normalizedReviewEventName(candidate.Name) != normalizedReviewEventName(event.Name) {
		return false
	}
	if strings.TrimSpace(candidate.VenueSlug) != strings.TrimSpace(event.VenueSlug) {
		return false
	}
	if roomEvidenceConflicts(candidate.RoomText, candidate.Rooms, event.RoomText, event.Rooms) {
		return false
	}
	start, err := parseRFC3339UTC(strings.TrimSpace(candidate.StartAt))
	if err != nil {
		return false
	}
	return start.UTC().Equal(event.Start.UTC())
}

func normalizedReviewEventName(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func unanimousStagedDuplicate(candidates []review.Candidate) bool {
	if len(candidates) < 2 {
		return false
	}
	for _, field := range reviewConsensusFields() {
		value := review.CandidateValue(candidates[0], field)
		for _, candidate := range candidates[1:] {
			if review.CandidateValue(candidate, field) != value {
				return false
			}
		}
	}
	return true
}

func observationRoomText(roomText string, rooms []domain.VenueRoom) string {
	roomText = strings.TrimSpace(roomText)
	if roomText != "" {
		return roomText
	}
	if len(rooms) == 0 {
		return ""
	}
	parts := make([]string, 0, len(rooms))
	for _, room := range rooms {
		slug := strings.TrimSpace(room.Slug)
		name := strings.TrimSpace(room.Name)
		switch {
		case slug != "" && name != "":
			parts = append(parts, slug+":"+name)
		case slug != "":
			parts = append(parts, slug)
		case name != "":
			parts = append(parts, name)
		}
	}
	return strings.Join(parts, ", ")
}

func observationNameNormalized(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

type eventObservationField struct {
	name                string
	incomingRaw         string
	incomingNormalized  string
	canonicalBeforeRaw  string
	canonicalBeforeNorm string
}

func eventObservationFields(target eventRecord, incoming domain.Event) []eventObservationField {
	return []eventObservationField{
		{name: "name", incomingRaw: incoming.Name, incomingNormalized: observationNameNormalized(incoming.Name), canonicalBeforeRaw: target.Event.Name, canonicalBeforeNorm: observationNameNormalized(target.Event.Name)},
		{name: "venue_slug", incomingRaw: strings.TrimSpace(incoming.VenueSlug), incomingNormalized: strings.TrimSpace(incoming.VenueSlug), canonicalBeforeRaw: strings.TrimSpace(target.Event.VenueSlug), canonicalBeforeNorm: strings.TrimSpace(target.Event.VenueSlug)},
		{name: "room_text", incomingRaw: observationRoomText(incoming.RoomText, incoming.Rooms), incomingNormalized: observationRoomText(incoming.RoomText, incoming.Rooms), canonicalBeforeRaw: observationRoomText(target.Event.RoomText, target.Event.Rooms), canonicalBeforeNorm: observationRoomText(target.Event.RoomText, target.Event.Rooms)},
		{name: "start_at", incomingRaw: observationTimeText(incoming.Start), incomingNormalized: observationTimeText(incoming.Start), canonicalBeforeRaw: observationTimeText(target.Event.Start), canonicalBeforeNorm: observationTimeText(target.Event.Start)},
		{name: "end_at", incomingRaw: observationTimeText(incoming.End), incomingNormalized: observationTimeText(incoming.End), canonicalBeforeRaw: observationTimeText(target.Event.End), canonicalBeforeNorm: observationTimeText(target.Event.End)},
		{name: "status", incomingRaw: strings.TrimSpace(incoming.Status), incomingNormalized: strings.TrimSpace(incoming.Status), canonicalBeforeRaw: strings.TrimSpace(target.Event.Status), canonicalBeforeNorm: strings.TrimSpace(target.Event.Status)},
		{name: "genre", incomingRaw: strings.TrimSpace(incoming.Genre), incomingNormalized: strings.TrimSpace(incoming.Genre), canonicalBeforeRaw: strings.TrimSpace(target.Event.Genre), canonicalBeforeNorm: strings.TrimSpace(target.Event.Genre)},
		{name: "description", incomingRaw: strings.TrimSpace(incoming.Description), incomingNormalized: strings.TrimSpace(incoming.Description), canonicalBeforeRaw: strings.TrimSpace(target.Event.Description), canonicalBeforeNorm: strings.TrimSpace(target.Event.Description)},
		{name: "official_listing_url", incomingRaw: strings.TrimSpace(incoming.OfficialListingURL), incomingNormalized: strings.TrimSpace(incoming.OfficialListingURL), canonicalBeforeRaw: strings.TrimSpace(target.Event.OfficialListingURL), canonicalBeforeNorm: strings.TrimSpace(target.Event.OfficialListingURL)},
		{name: "calendar_url", incomingRaw: strings.TrimSpace(incoming.CalendarURL), incomingNormalized: strings.TrimSpace(incoming.CalendarURL), canonicalBeforeRaw: strings.TrimSpace(target.Event.CalendarURL), canonicalBeforeNorm: strings.TrimSpace(target.Event.CalendarURL)},
	}
}

func writeSourceAttributeObservationTx(ctx context.Context, tx interface {
	execer
	queryer
}, scope seedstore.ObservationRunScope, sourceID int64, sourceIdentityKey string, authority seedstore.SourceAuthority, targetKind seedstore.ObservationTargetKind, eventID, reviewGroupID int64, fieldName, incomingRaw, incomingNormalized, canonicalBeforeRaw, canonicalBeforeNormalized string, outcome seedstore.ObservationOutcome, isConflict bool) error {
	input := seedstore.SourceAttributeObservationInput{
		RunScope:                  scope,
		SourceID:                  sourceID,
		SourceIdentityKey:         sourceIdentityKey,
		SourceAuthority:           authority,
		TargetKind:                targetKind,
		FieldName:                 fieldName,
		IncomingRaw:               incomingRaw,
		IncomingNormalized:        incomingNormalized,
		CanonicalBeforeRaw:        canonicalBeforeRaw,
		CanonicalBeforeNormalized: canonicalBeforeNormalized,
		Outcome:                   string(outcome),
		IsConflict:                isConflict,
	}
	switch targetKind {
	case seedstore.ObservationTargetKindEvent:
		input.EventID = int64Ptr(eventID)
	case seedstore.ObservationTargetKindReviewGroup:
		input.ReviewGroupID = int64Ptr(reviewGroupID)
	default:
		return fmt.Errorf("unsupported observation target kind %q", targetKind)
	}
	return upsertSourceAttributeObservationTx(ctx, tx, input)
}

func (s *Store) recordStagedConflictEventObservationsAfterRollbackTx(ctx context.Context, scope seedstore.ObservationRunScope, sourceCtx reviewSourceIdentityContext, target eventRecord, incoming domain.Event) error {
	if scope == "" {
		return nil
	}
	if sourceCtx.PrimaryObservationKey == "" {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	sourceID, err := ensureSourceTx(ctx, tx, sourceCtx.SourceName, sourceCtx.SourceURL)
	if err != nil {
		return err
	}
	if err := recordStagedConflictEventObservationsTx(ctx, tx, scope, sourceID, sourceCtx.PrimaryObservationKey, seedstore.SourceAuthoritySupporting, target, incoming); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func recordEventObservationsTx(ctx context.Context, tx interface {
	execer
	queryer
}, scope seedstore.ObservationRunScope, sourceID int64, sourceIdentityKey string, authority seedstore.SourceAuthority, target eventRecord, incoming domain.Event) error {
	if scope == "" {
		return nil
	}
	for _, field := range eventObservationFields(target, incoming) {
		if strings.TrimSpace(field.incomingRaw) == "" {
			continue
		}
		outcome := seedstore.ObservationOutcomeApplied
		isConflict := false
		switch {
		case strings.TrimSpace(field.canonicalBeforeRaw) == "":
			outcome = seedstore.ObservationOutcomeFilledBlank
		case strings.TrimSpace(field.incomingNormalized) != strings.TrimSpace(field.canonicalBeforeNorm):
			outcome = seedstore.ObservationOutcomeConflictObserved
			isConflict = true
		}
		if err := writeSourceAttributeObservationTx(ctx, tx, scope, sourceID, sourceIdentityKey, authority, seedstore.ObservationTargetKindEvent, target.ID, 0, field.name, field.incomingRaw, field.incomingNormalized, field.canonicalBeforeRaw, field.canonicalBeforeNorm, outcome, isConflict); err != nil {
			return err
		}
	}
	return nil
}

func recordStagedConflictEventObservationsTx(ctx context.Context, tx interface {
	execer
	queryer
}, scope seedstore.ObservationRunScope, sourceID int64, sourceIdentityKey string, authority seedstore.SourceAuthority, target eventRecord, incoming domain.Event) error {
	if scope == "" {
		return nil
	}
	for _, field := range eventObservationFields(target, incoming) {
		if strings.TrimSpace(field.incomingRaw) == "" {
			continue
		}
		isConflict := strings.TrimSpace(field.canonicalBeforeRaw) != "" && strings.TrimSpace(field.incomingNormalized) != strings.TrimSpace(field.canonicalBeforeNorm)
		if err := writeSourceAttributeObservationTx(ctx, tx, scope, sourceID, sourceIdentityKey, authority, seedstore.ObservationTargetKindEvent, target.ID, 0, field.name, field.incomingRaw, field.incomingNormalized, field.canonicalBeforeRaw, field.canonicalBeforeNorm, seedstore.ObservationOutcomeStagedConflict, isConflict); err != nil {
			return err
		}
	}
	return nil
}

func recordEventReviewClusterObservationsForStageInputTx(ctx context.Context, tx interface {
	execer
	queryer
}, scope seedstore.ObservationRunScope, stageInput seedstore.StageEventReviewEvidenceInput, clusterID int64, now time.Time) error {
	if scope == "" || clusterID <= 0 {
		return nil
	}
	if stageInput.RunRef.Kind != seedstore.EventReviewRunKindImport || stageInput.RunRef.ID <= 0 {
		return nil
	}
	if stageInput.ConflictType != seedstore.EventReviewConflictTypeImportReview || stageInput.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return nil
	}

	authority := seedstore.SourceAuthority(strings.TrimSpace(string(stageInput.SourceAuthority)))
	switch authority {
	case seedstore.SourceAuthorityAuthoritative, seedstore.SourceAuthoritySupporting:
	default:
		return nil
	}

	parsed, err := parseImportReviewCandidatePayload(stageInput.Payload)
	if err != nil {
		return nil
	}

	candidate := review.CandidateInput{
		ExternalID:  strings.TrimSpace(parsed.ExternalID),
		Name:        strings.TrimSpace(parsed.Title),
		VenueSlug:   strings.TrimSpace(parsed.VenueSlug),
		VenueText:   strings.TrimSpace(parsed.VenueText),
		RoomText:    strings.TrimSpace(parsed.RoomText),
		Rooms:       parsedRoomsFromImportReviewPayload(parsed.Rooms),
		StartAt:     strings.TrimSpace(parsed.StartAt),
		EndAt:       strings.TrimSpace(parsed.EndAt),
		Genre:       strings.TrimSpace(parsed.Genre),
		Status:      strings.TrimSpace(parsed.Status),
		Description: strings.TrimSpace(parsed.Description),
		SourceName:  firstNonEmptyImportReviewText(parsed.SourceName, stageInput.SourceName),
		SourceURL:   firstNonEmptyImportReviewText(parsed.SourceURL, stageInput.SourceURL),
		CalendarURL: strings.TrimSpace(parsed.CalendarURL),
		Provenance:  strings.TrimSpace(parsed.Provenance),
	}
	sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentitySupporting, stageInput.SourceName, stageInput.SourceURL, "", "", "", candidate, "event_review_import_staging")
	if sourceCtx.PrimaryObservationKey == "" {
		return nil
	}

	for _, field := range reviewCandidateObservationFields(candidate) {
		if field.value == "" {
			continue
		}
		if err := upsertSourceAttributeObservationRowTx(ctx, tx, seedstore.SourceAttributeObservationInput{
			RunScope:                  scope,
			SourceID:                  stageInput.SourceID,
			SourceIdentityKey:         sourceCtx.PrimaryObservationKey,
			SourceAuthority:           authority,
			TargetKind:                seedstore.ObservationTargetKindEventReviewCluster,
			EventReviewClusterID:      int64Ptr(clusterID),
			FieldName:                 field.name,
			IncomingRaw:               field.value,
			IncomingNormalized:        field.normalized,
			CanonicalBeforeRaw:        "",
			CanonicalBeforeNormalized: "",
			Outcome:                   string(seedstore.ObservationOutcomeStagedForReview),
			IsConflict:                false,
		}, now, now); err != nil {
			return err
		}
	}
	return nil
}

type reviewObservationField struct {
	name       string
	value      string
	normalized string
}

func reviewCandidateObservationFields(candidate review.CandidateInput) []reviewObservationField {
	return []reviewObservationField{
		{name: "name", value: strings.TrimSpace(candidate.Name), normalized: observationNameNormalized(candidate.Name)},
		{name: "venue_slug", value: strings.TrimSpace(candidate.VenueSlug), normalized: strings.TrimSpace(candidate.VenueSlug)},
		{name: "room_text", value: observationRoomText(candidate.RoomText, candidate.Rooms), normalized: observationRoomText(candidate.RoomText, candidate.Rooms)},
		{name: "start_at", value: strings.TrimSpace(candidate.StartAt), normalized: strings.TrimSpace(candidate.StartAt)},
		{name: "end_at", value: strings.TrimSpace(candidate.EndAt), normalized: strings.TrimSpace(candidate.EndAt)},
		{name: "status", value: strings.TrimSpace(candidate.Status), normalized: strings.TrimSpace(candidate.Status)},
		{name: "genre", value: strings.TrimSpace(candidate.Genre), normalized: strings.TrimSpace(candidate.Genre)},
		{name: "description", value: strings.TrimSpace(candidate.Description), normalized: strings.TrimSpace(candidate.Description)},
		{name: "official_listing_url", value: strings.TrimSpace(candidate.SourceURL), normalized: strings.TrimSpace(candidate.SourceURL)},
		{name: "calendar_url", value: strings.TrimSpace(candidate.CalendarURL), normalized: strings.TrimSpace(candidate.CalendarURL)},
	}
}

func uniqueLiveEventMatchForEventTx(ctx context.Context, q queryer, event domain.Event) (eventRecord, bool, bool, error) {
	if exactRecords, ok, err := matchLiveEventsByExactIdentityTx(ctx, q, event); err != nil {
		return eventRecord{}, false, false, err
	} else if ok {
		switch len(exactRecords) {
		case 0:
		case 1:
			return exactRecords[0], true, false, nil
		default:
			return eventRecord{}, false, true, nil
		}
	}
	result, err := matchLiveEventsByIdentityTx(ctx, q, event.Slug, event.Name, event.VenueSlug, event.Start)
	if err != nil {
		return eventRecord{}, false, false, err
	}
	switch len(result) {
	case 0:
		return eventRecord{}, false, false, nil
	case 1:
		return result[0], true, false, nil
	default:
		return eventRecord{}, false, true, nil
	}
}

func guardedNearLiveEventMatchForEventTx(ctx context.Context, q queryer, event domain.Event, sourceMetadata ingest.SourceMetadataLookup) ([]eventRecord, bool, error) {
	records, ok, err := matchLiveEventsByGuardedNearIdentityTx(ctx, q, event, guardedNearMatchWindowForEventSource(sourceMetadata, event.SourceName))
	if err != nil || !ok {
		return nil, false, err
	}
	return records, guardedNearMatchEnabledForEventSource(sourceMetadata, event.SourceName), nil
}

func guardedNearMatchEnabledForEventSource(sourceMetadata ingest.SourceMetadataLookup, sourceName string) bool {
	if sourceMetadata == nil {
		return true
	}
	return !sourceMetadata.GuardedNearMatchDisabledForReviewStageSourceName(sourceName)
}

func guardedNearMatchWindowForEventSource(sourceMetadata ingest.SourceMetadataLookup, sourceName string) time.Duration {
	if sourceMetadata == nil {
		return 75 * time.Minute
	}
	window := sourceMetadata.GuardedNearMatchWindowForReviewStageSourceName(sourceName)
	if window <= 0 {
		return 75 * time.Minute
	}
	return window
}

func supportingEventConflict(existing, incoming domain.Event) bool {
	if normalizedReviewEventName(existing.Name) != normalizedReviewEventName(incoming.Name) {
		return true
	}
	if nonEmptyStringConflict(existing.VenueSlug, incoming.VenueSlug) {
		return true
	}
	if roomEvidenceConflicts(existing.RoomText, existing.Rooms, incoming.RoomText, incoming.Rooms) {
		return true
	}
	if !existing.Start.IsZero() && !incoming.Start.IsZero() && !existing.Start.UTC().Equal(incoming.Start.UTC()) {
		return true
	}
	if existing.HasEnd() && incoming.HasEnd() && !existing.End.UTC().Equal(incoming.End.UTC()) {
		return true
	}
	if nonEmptyStringConflict(existing.Status, incoming.Status) {
		return true
	}
	if nonEmptyStringConflict(existing.Description, incoming.Description) {
		return true
	}
	if nonEmptyStringConflict(existing.OfficialListingURL, incoming.OfficialListingURL) {
		return true
	}
	if nonEmptyStringConflict(existing.CalendarURL, incoming.CalendarURL) {
		return true
	}
	return false
}

func observationTimeText(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return formatRFC3339UTC(value)
}

func nonEmptyStringConflict(existing, incoming string) bool {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	return existing != "" && incoming != "" && existing != incoming
}

func updateSupportingMatchedEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, existing eventRecord, incoming domain.Event) error {
	updated := existing.Event
	if !updated.HasEnd() && incoming.HasEnd() {
		updated.End = incoming.End
	}
	if strings.TrimSpace(updated.Status) == "" && strings.TrimSpace(incoming.Status) != "" {
		updated.Status = incoming.Status
	}
	if strings.TrimSpace(updated.Genre) == "" && strings.TrimSpace(incoming.Genre) != "" {
		updated.Genre = incoming.Genre
	}
	if strings.TrimSpace(updated.Description) == "" && strings.TrimSpace(incoming.Description) != "" {
		updated.Description = incoming.Description
	}
	if len(updated.Rooms) == 0 && len(incoming.Rooms) > 0 {
		updated.Rooms = append([]domain.VenueRoom(nil), incoming.Rooms...)
	}
	if strings.TrimSpace(updated.RoomText) == "" && strings.TrimSpace(incoming.RoomText) != "" {
		updated.RoomText = incoming.RoomText
	}
	if strings.TrimSpace(updated.OfficialListingURL) == "" && strings.TrimSpace(incoming.OfficialListingURL) != "" {
		updated.OfficialListingURL = incoming.OfficialListingURL
	}
	if strings.TrimSpace(updated.CalendarURL) == "" && strings.TrimSpace(incoming.CalendarURL) != "" {
		updated.CalendarURL = incoming.CalendarURL
	}
	if strings.TrimSpace(updated.ImageURL) == "" && strings.TrimSpace(incoming.ImageURL) != "" {
		updated.ImageURL = incoming.ImageURL
		updated.ImageSourceURL = incoming.ImageSourceURL
		updated.ImageAlt = incoming.ImageAlt
		updated.ImageWidth = incoming.ImageWidth
		updated.ImageHeight = incoming.ImageHeight
		updated.ImageFocusX = incoming.ImageFocusX
		updated.ImageFocusY = incoming.ImageFocusY
	}
	updated.LastChecked = incoming.LastChecked.UTC()

	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET end_at = ?,
			genre = ?,
			status = ?,
			description = ?,
			image_url = ?,
			image_source_url = ?,
			image_alt = ?,
			image_width = ?,
			image_height = ?,
			image_focus_x = ?,
			image_focus_y = ?,
			official_listing_url = ?,
			calendar_url = ?,
			last_checked_at = ?
		WHERE id = ?
	`, nullableRFC3339UTC(updated.End), updated.Genre, updated.Status, updated.Description, updated.ImageURL, updated.ImageSourceURL, updated.ImageAlt, updated.ImageWidth, updated.ImageHeight, normalizedImageFocusValue(updated.ImageFocusX), normalizedImageFocusValue(updated.ImageFocusY), updated.OfficialListingURL, updated.CalendarURL, formatRFC3339UTC(updated.LastChecked), existing.ID); err != nil {
		return err
	}
	if err := replaceEventRoomsTx(ctx, tx, existing.ID, updated); err != nil {
		return err
	}
	if err := refreshEventGenresTx(ctx, tx, existing.ID, updated.Description, nil, incoming.LastChecked); err != nil {
		return err
	}
	if strings.TrimSpace(updated.Genre) != "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET genre = ?
			WHERE id = ?
		`, updated.Genre, existing.ID); err != nil {
			return err
		}
	}
	return nil
}

type DescriptionRepairReport struct {
	Repaired       int      `json:"description_repaired"`
	Unchanged      int      `json:"description_unchanged"`
	Skipped        int      `json:"description_skipped"`
	RepairedSlugs  []string `json:"repaired_event_slugs"`
	UnchangedSlugs []string `json:"unchanged_event_slugs"`
	SkippedTitles  []string `json:"skipped_titles"`
}

func (s *Store) RepairEventDescriptionsFromReport(ctx context.Context, catalog *ingest.Catalog, report ingest.Report) (DescriptionRepairReport, error) {
	repair := DescriptionRepairReport{
		RepairedSlugs:  []string{},
		UnchangedSlugs: []string{},
		SkippedTitles:  []string{},
	}
	if s == nil || s.db == nil {
		return repair, errors.New("sqlite store is not open")
	}
	if catalog == nil {
		return repair, errors.New("catalog is nil")
	}
	if !strings.EqualFold(strings.TrimSpace(report.Status), "succeeded") {
		return repair, nil
	}

	clusters := ingest.ReviewClustersFromReportWithCatalog(catalog, report)
	for _, cluster := range clusters {
		if strings.TrimSpace(cluster.AuthoritativeSourceName) == "" ||
			strings.TrimSpace(cluster.AuthoritativeSourceURL) == "" ||
			strings.TrimSpace(cluster.AuthoritativeSourceEventKey) == "" ||
			len(cluster.Candidates) != 1 {
			repair.Skipped++
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(cluster.Title))
			continue
		}

		event, err := singletonResolvedEventFromReviewStageClusterInput(cluster, time.Now().UTC())
		if err != nil {
			repair.Skipped++
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(cluster.Title))
			continue
		}
		result, err := s.repairAuthoritativeEventDescription(ctx, event, cluster.AuthoritativeSourceEventKey)
		if err != nil {
			return repair, err
		}
		switch result.Status {
		case descriptionRepairRepaired:
			repair.Repaired++
			repair.RepairedSlugs = append(repair.RepairedSlugs, result.EventSlug)
		case descriptionRepairUnchanged:
			repair.Unchanged++
			repair.UnchangedSlugs = append(repair.UnchangedSlugs, result.EventSlug)
		default:
			repair.Skipped++
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(cluster.Title))
		}
	}
	return repair, nil
}

type descriptionRepairStatus string

const (
	descriptionRepairRepaired  descriptionRepairStatus = "repaired"
	descriptionRepairUnchanged descriptionRepairStatus = "unchanged"
	descriptionRepairSkipped   descriptionRepairStatus = "skipped"
)

type descriptionRepairResult struct {
	Status    descriptionRepairStatus
	EventSlug string
}

func (s *Store) repairAuthoritativeEventDescription(ctx context.Context, incoming domain.Event, sourceEventKey string) (descriptionRepairResult, error) {
	incoming.Description = strings.TrimSpace(incoming.Description)
	if !shouldReplaceDescription("", incoming.Description) {
		return descriptionRepairResult{Status: descriptionRepairSkipped}, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return descriptionRepairResult{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	record, found, err := findExistingAuthoritativeEventForDescriptionRepairTx(ctx, tx, incoming, sourceEventKey)
	if err != nil {
		return descriptionRepairResult{}, err
	}
	if !found {
		return descriptionRepairResult{Status: descriptionRepairSkipped}, nil
	}
	if !shouldReplaceDescription(record.Event.Description, incoming.Description) {
		return descriptionRepairResult{Status: descriptionRepairUnchanged, EventSlug: record.Event.Slug}, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET description = ?
		WHERE id = ?
	`, incoming.Description, record.ID); err != nil {
		return descriptionRepairResult{}, err
	}
	if err := refreshEventGenresTx(ctx, tx, record.ID, incoming.Description, nil, time.Now().UTC()); err != nil {
		return descriptionRepairResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return descriptionRepairResult{}, err
	}
	return descriptionRepairResult{Status: descriptionRepairRepaired, EventSlug: record.Event.Slug}, nil
}

func findExistingAuthoritativeEventForDescriptionRepairTx(ctx context.Context, tx queryer, incoming domain.Event, sourceEventKey string) (eventRecord, bool, error) {
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, tx, incoming.SourceName, incoming.SourceURL)
	if err != nil {
		return eventRecord{}, false, err
	}
	if !ok {
		return eventRecord{}, false, nil
	}

	if record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, tx, sourceID, reviewGroupAuthoritativeSourceIdentities(reviewGroupAuthoritativeLink{
		SourceURL:      incoming.SourceURL,
		SourceEventKey: sourceEventKey,
	})); err != nil {
		return eventRecord{}, false, err
	} else if ambiguous {
		return eventRecord{}, false, nil
	} else if ok {
		return record, true, nil
	}

	if legacy, ok, err := loadEventRecordBySlugAndSourceTx(ctx, tx, incoming.Slug, sourceID); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		return legacy, true, nil
	}
	if matched, found, ambiguous, err := uniqueLiveEventMatchForEventSourceTx(ctx, tx, incoming, sourceID); err != nil {
		return eventRecord{}, false, err
	} else if found && !ambiguous {
		return matched, true, nil
	}
	return eventRecord{}, false, nil
}

func uniqueLiveEventMatchForEventSourceTx(ctx context.Context, q queryer, event domain.Event, sourceID int64) (eventRecord, bool, bool, error) {
	result, err := matchLiveEventsBySourceIdentityTx(ctx, q, sourceID, event.Slug, event.Name, event.VenueSlug, event.Start)
	if err != nil {
		return eventRecord{}, false, false, err
	}
	switch len(result) {
	case 0:
		return eventRecord{}, false, false, nil
	case 1:
		return result[0], true, false, nil
	default:
		return eventRecord{}, false, true, nil
	}
}

func matchLiveEventsByIdentityTx(ctx context.Context, q queryer, slug, name, venueSlug string, start time.Time) ([]eventRecord, error) {
	matched := make(map[int64]eventRecord)

	if strings.TrimSpace(slug) != "" {
		record, ok, err := loadLiveEventRecordBySlugTx(ctx, q, slug)
		if err != nil {
			return nil, err
		}
		if ok {
			matched[record.ID] = record
		}
	}

	records, err := loadLiveEventRecordsByFingerprintTx(ctx, q, name, venueSlug, start)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		matched[record.ID] = record
	}

	out := make([]eventRecord, 0, len(matched))
	for _, record := range matched {
		out = append(out, record)
	}
	return out, nil
}

func matchLiveEventsBySourceIdentityTx(ctx context.Context, q queryer, sourceID int64, slug, name, venueSlug string, start time.Time) ([]eventRecord, error) {
	matched := make(map[int64]eventRecord)

	if strings.TrimSpace(slug) != "" {
		record, ok, err := loadLiveEventRecordBySlugAndSourceTx(ctx, q, slug, sourceID)
		if err != nil {
			return nil, err
		}
		if ok {
			matched[record.ID] = record
		}
	}

	records, err := loadLiveEventRecordsByFingerprintAndSourceTx(ctx, q, sourceID, name, venueSlug, start)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		matched[record.ID] = record
	}

	out := make([]eventRecord, 0, len(matched))
	for _, record := range matched {
		out = append(out, record)
	}
	return out, nil
}

type eventRecord struct {
	ID    int64
	Event domain.Event
}

func applyAuthoritativeEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event, sourceCtx reviewSourceIdentityContext, now time.Time, scope seedstore.ObservationRunScope, sourceMetadata ingest.SourceMetadataLookup) (eventRecord, bool, error) {
	event.PublicationState = domain.PublicationStateReviewed
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return eventRecord{}, false, err
	}
	if !ok {
		return eventRecord{}, false, nil
	}
	sourceID, err := ensureSourceTx(ctx, tx, sourceCtx.SourceName, sourceCtx.SourceURL)
	if err != nil {
		return eventRecord{}, false, err
	}
	sourceLinkIdentities := sourceCtx.Identities
	if linked, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, tx, sourceID, sourceLinkIdentities); err != nil {
		return eventRecord{}, false, err
	} else if ambiguous {
		return eventRecord{}, false, nil
	} else if ok {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, linked, event, venueID, sourceID, now, false)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureActiveExactIdentityWithConflictTransferTx(ctx, tx, linked.ID, updated, 0, now); err != nil {
			return eventRecord{}, false, err
		}
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, linked.ID, sourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if writeResult.Ambiguous {
			return eventRecord{}, false, nil
		}
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthorityAuthoritative, linked, event); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, linked.ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: linked.ID, Event: updated}, true, nil
	}

	if matched, found, ambiguous, err := uniqueLiveEventMatchForEventTx(ctx, tx, event); err != nil {
		return eventRecord{}, false, err
	} else if ambiguous {
		return eventRecord{}, false, nil
	} else if found {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, matched, event, venueID, sourceID, now, true)
		if err != nil {
			return eventRecord{}, false, err
		}
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, matched.ID, sourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if writeResult.Ambiguous {
			return eventRecord{}, false, nil
		}
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthorityAuthoritative, matched, event); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, matched.ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: matched.ID, Event: updated}, true, nil
	}

	if near, _, err := guardedNearLiveEventMatchForEventTx(ctx, tx, event, sourceMetadata); err != nil {
		return eventRecord{}, false, err
	} else if len(near) > 0 {
		if len(near) > 1 {
			return eventRecord{}, false, nil
		}
		updated, err := updateEventAuthoritativelyTx(ctx, tx, near[0], event, venueID, sourceID, now, true)
		if err != nil {
			return eventRecord{}, false, err
		}
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, near[0].ID, sourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if writeResult.Ambiguous {
			return eventRecord{}, false, nil
		}
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthorityAuthoritative, near[0], event); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, near[0].ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: near[0].ID, Event: updated}, true, nil
	}

	if _, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		return eventRecord{}, false, nil
	}

	eventID, err := insertEventTx(ctx, tx, event, venueID, sourceID, now)
	if err != nil {
		return eventRecord{}, false, err
	}
	writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, eventID, sourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
	if err != nil {
		return eventRecord{}, false, err
	}
	if writeResult.Ambiguous {
		return eventRecord{}, false, nil
	}
	if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthorityAuthoritative, eventRecord{ID: eventID, Event: domain.Event{}}, event); err != nil {
		return eventRecord{}, false, err
	}
	if err := refreshEventGenresTx(ctx, tx, eventID, event.Description, nil, now); err != nil {
		return eventRecord{}, false, err
	}
	return eventRecord{ID: eventID, Event: event}, true, nil
}

func insertEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event, venueID, sourceID int64, now time.Time) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			official_listing_url,
			calendar_url,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		event.ImageURL, event.ImageSourceURL, event.ImageAlt, event.ImageWidth, event.ImageHeight,
		normalizedImageFocusValue(event.ImageFocusX),
		normalizedImageFocusValue(event.ImageFocusY),
		event.OfficialListingURL,
		event.CalendarURL,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin),
		string(normalizedPublicationState(event.PublicationState)))
	if err != nil {
		return 0, err
	}
	eventID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := replaceEventRoomsTx(ctx, tx, eventID, event); err != nil {
		return 0, err
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, eventID, event, 0, now); err != nil {
		return 0, err
	}
	return eventID, nil
}

func updateEventAuthoritativelyTx(ctx context.Context, tx interface {
	execer
	queryer
}, existing eventRecord, authoritative domain.Event, venueID, sourceID int64, now time.Time, syncExactIdentity bool) (domain.Event, error) {
	updated := existing.Event
	updated.Name = authoritative.Name
	updated.VenueSlug = authoritative.VenueSlug
	updated.Rooms = append([]domain.VenueRoom(nil), authoritative.Rooms...)
	updated.RoomText = strings.TrimSpace(authoritative.RoomText)
	updated.Start = authoritative.Start
	updated.End = authoritative.End
	if authoritative.Genre != "" {
		updated.Genre = authoritative.Genre
	}
	if authoritative.Status != "" {
		updated.Status = authoritative.Status
	}
	if authoritativeDescriptionUsable(authoritative.Description) {
		updated.Description = authoritative.Description
	}
	if strings.TrimSpace(authoritative.ImageURL) != "" {
		updated.ImageURL = authoritative.ImageURL
		updated.ImageSourceURL = authoritative.ImageSourceURL
		updated.ImageAlt = authoritative.ImageAlt
		updated.ImageWidth = authoritative.ImageWidth
		updated.ImageHeight = authoritative.ImageHeight
		updated.ImageFocusX = authoritative.ImageFocusX
		updated.ImageFocusY = authoritative.ImageFocusY
	}
	updated.SourceName = authoritative.SourceName
	updated.SourceURL = authoritative.SourceURL
	if strings.TrimSpace(authoritative.OfficialListingURL) != "" {
		updated.OfficialListingURL = authoritative.OfficialListingURL
	}
	if strings.TrimSpace(authoritative.CalendarURL) != "" {
		updated.CalendarURL = authoritative.CalendarURL
	}
	updated.LastChecked = authoritative.LastChecked.UTC()
	updated.Origin = authoritative.Origin
	updated.PublicationState = normalizedPublicationState(authoritative.PublicationState)

	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET venue_id = ?,
			source_id = ?,
			name = ?,
			start_at = ?,
			end_at = ?,
			genre = ?,
			status = ?,
			description = ?,
			image_url = ?,
			image_source_url = ?,
			image_alt = ?,
			image_width = ?,
			image_height = ?,
			image_focus_x = ?,
			image_focus_y = ?,
			official_listing_url = ?,
			calendar_url = ?,
			last_checked_at = ?,
			origin = ?,
			publication_state = ?
		WHERE id = ?
	`, venueID, sourceID, updated.Name, formatRFC3339UTC(updated.Start), nullableRFC3339UTC(updated.End), updated.Genre, updated.Status, updated.Description, updated.ImageURL, updated.ImageSourceURL, updated.ImageAlt, updated.ImageWidth, updated.ImageHeight, normalizedImageFocusValue(updated.ImageFocusX), normalizedImageFocusValue(updated.ImageFocusY), updated.OfficialListingURL, updated.CalendarURL, formatRFC3339UTC(updated.LastChecked), string(updated.Origin), string(updated.PublicationState), existing.ID); err != nil {
		return domain.Event{}, err
	}
	if err := replaceEventRoomsTx(ctx, tx, existing.ID, updated); err != nil {
		return domain.Event{}, err
	}
	if syncExactIdentity {
		if err := ensureActiveExactIdentityTx(ctx, tx, existing.ID, updated, 0, now); err != nil {
			return domain.Event{}, err
		}
	}
	return updated, nil
}

func shouldReplaceDescription(existing, incoming string) bool {
	incoming = strings.TrimSpace(incoming)
	if incoming == "" || !descriptionIsClean(incoming) {
		return false
	}
	existing = strings.TrimSpace(existing)
	return existing == "" || descriptionIsGeneratedMarkup(existing)
}

func authoritativeDescriptionUsable(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if descriptionIsGeneratedMarkup(value) {
		return false
	}
	switch {
	case strings.EqualFold(value, "buy tickets"):
		return false
	case strings.EqualFold(value, "basement buy tickets"):
		return false
	case strings.EqualFold(value, "tickets"):
		return false
	case strings.EqualFold(value, "book tickets"):
		return false
	case strings.EqualFold(value, "read more"):
		return false
	case strings.EqualFold(value, "find out more"):
		return false
	case strings.EqualFold(value, "more info"):
		return false
	case strings.EqualFold(value, "event details"):
		return false
	case strings.EqualFold(value, "click here"):
		return false
	case strings.EqualFold(value, "back to events"):
		return false
	}
	return true
}

func descriptionIsClean(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	switch {
	case strings.Contains(lower, "#block-"):
		return false
	case strings.Contains(lower, "--tweak-"):
		return false
	case strings.Contains(lower, "@media screen"):
		return false
	case strings.Contains(lower, "<script") || strings.Contains(lower, "<style"):
		return false
	case strings.EqualFold(value, "buy tickets"):
		return false
	case strings.EqualFold(value, "basement buy tickets"):
		return false
	case len([]rune(value)) < 40 && !strings.ContainsAny(value, ".!?"):
		return false
	default:
		return true
	}
}

func descriptionIsGeneratedMarkup(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	return strings.Contains(lower, "#block-") ||
		strings.Contains(lower, "--tweak-") ||
		strings.Contains(lower, "@media screen") ||
		strings.Contains(lower, "<script") ||
		strings.Contains(lower, "<style")
}

type sourceLinkAuthority int

const (
	sourceLinkAuthoritySupporting sourceLinkAuthority = iota
	sourceLinkAuthorityAuthoritative
)

type sourceLinkConflictPolicy int

const (
	sourceLinkConflictPolicyNoMove sourceLinkConflictPolicy = iota
)

type sourceLinkWriteResult struct {
	Applied         bool
	Ambiguous       bool
	Reason          string
	ExistingEventID int64
	TargetEventID   int64
}

type sourceLinkRow struct {
	eventID          int64
	isAuthoritative  bool
	origin           string
	publicationState string
	canonicalEventID sql.NullInt64
	canonicalOrigin  sql.NullString
	canonicalState   sql.NullString
	resolvedEventID  int64
}

func sourceIdentityInputForKey(raw string) ingest.SourceIdentityInput {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ingest.SourceIdentityInput{}
	}
	if prefix, value, ok := strings.Cut(raw, ":"); ok {
		switch prefix {
		case "uid":
			return ingest.SourceIdentityInput{ExternalID: strings.TrimSpace(value)}
		case "url":
			value = strings.TrimSpace(value)
			if len(ingest.SourceIdentities(ingest.SourceIdentityInput{CalendarURL: value}).Keys()) > 0 {
				return ingest.SourceIdentityInput{CalendarURL: value}
			}
			return ingest.SourceIdentityInput{SourceURL: value}
		}
	}
	return ingest.SourceIdentityInput{ExternalID: raw}
}

func authoritativeSourceCandidateForApply(selected map[review.Field]review.Candidate, canonical review.Candidate, staged []review.Candidate) review.Candidate {
	if candidate, ok := selected[review.FieldSourceURL]; ok {
		return candidate
	}
	if candidate, ok := selected[review.FieldSourceName]; ok {
		return candidate
	}
	if len(staged) > 0 {
		return staged[0]
	}
	return canonical
}

func reviewCandidateSourceIdentities(candidate review.CandidateInput) ingest.SourceIdentitySet {
	return ingest.SourceIdentities(sourceIdentityInputFromCandidateValues(candidate.ExternalID, candidate.SourceURL, candidate.CalendarURL))
}

func reviewStoredCandidateSourceIdentities(candidate review.Candidate) ingest.SourceIdentitySet {
	return ingest.SourceIdentities(sourceIdentityInputFromCandidateValues(candidate.ExternalID, candidate.SourceURL, candidate.CalendarURL))
}

func sourceIdentityInputFromCandidateValues(externalID, sourceURL, calendarURL string) ingest.SourceIdentityInput {
	input := ingest.SourceIdentityInput{
		ExternalID:  strings.TrimSpace(externalID),
		SourceURL:   strings.TrimSpace(sourceURL),
		CalendarURL: strings.TrimSpace(calendarURL),
	}
	if input.ExternalID == "" {
		return input
	}
	if prefix, value, ok := strings.Cut(input.ExternalID, ":"); ok {
		switch prefix {
		case "uid", "url":
			input.ExternalID = strings.TrimSpace(value)
		}
	}
	return input
}

func reviewGroupAuthoritativeSourceIdentities(link reviewGroupAuthoritativeLink) ingest.SourceIdentitySet {
	input := sourceIdentityInputForKey(link.SourceEventKey)
	input.SourceURL = link.SourceURL
	return ingest.SourceIdentities(input)
}

func ensureEventSourceLinkTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID, sourceID int64, identities ingest.SourceIdentitySet, authority sourceLinkAuthority, policy sourceLinkConflictPolicy, now time.Time) (sourceLinkWriteResult, error) {
	if eventID <= 0 {
		return sourceLinkWriteResult{}, errors.New("event source link event ID is required")
	}
	if sourceID <= 0 {
		return sourceLinkWriteResult{}, errors.New("event source link source ID is required")
	}
	keys := identities.Keys()
	if len(keys) == 0 {
		return sourceLinkWriteResult{TargetEventID: eventID, Reason: "no stable source identities"}, nil
	}

	resolvedTargetID, found, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, tx, sourceID, identities)
	if err != nil {
		return sourceLinkWriteResult{}, err
	}
	if ambiguous {
		return sourceLinkWriteResult{
			Ambiguous:       true,
			Reason:          "source identities resolve ambiguously",
			ExistingEventID: resolvedTargetID,
			TargetEventID:   eventID,
		}, nil
	}
	if found && resolvedTargetID != eventID {
		return sourceLinkWriteResult{
			Ambiguous:       true,
			Reason:          "source identity already linked to a different live event",
			ExistingEventID: resolvedTargetID,
			TargetEventID:   eventID,
		}, nil
	}

	desiredAuthoritative := authority == sourceLinkAuthorityAuthoritative
	wrote := false
	nowText := formatRFC3339UTC(now)
	for _, sourceEventKey := range keys {
		row, found, ambiguous, err := loadSourceLinkRowByKeyTx(ctx, tx, sourceID, sourceEventKey)
		if err != nil {
			return sourceLinkWriteResult{}, err
		}
		if ambiguous {
			return sourceLinkWriteResult{
				Ambiguous:       true,
				Reason:          "source identity has no usable live target",
				TargetEventID:   eventID,
				ExistingEventID: resolvedTargetID,
			}, nil
		}
		switch {
		case !found:
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO event_source_links (
					event_id,
					source_id,
					source_event_key,
					is_authoritative,
					created_at,
					updated_at
				) VALUES (?, ?, ?, ?, ?, ?)
			`, eventID, sourceID, sourceEventKey, boolInt(desiredAuthoritative), nowText, nowText); err != nil {
				return sourceLinkWriteResult{}, err
			}
			wrote = true
		case row.resolvedEventID != eventID:
			if policy == sourceLinkConflictPolicyNoMove {
				return sourceLinkWriteResult{
					Ambiguous:       true,
					Reason:          "source identity already linked to a different live event",
					ExistingEventID: row.resolvedEventID,
					TargetEventID:   eventID,
				}, nil
			}
			return sourceLinkWriteResult{}, errors.New("source link conflict policy is not implemented")
		default:
			if row.eventID != eventID || (desiredAuthoritative && !row.isAuthoritative) {
				_, err := tx.ExecContext(ctx, `
					UPDATE event_source_links
					SET event_id = ?,
						is_authoritative = CASE
							WHEN ? = 1 THEN 1
							WHEN is_authoritative = 1 THEN 1
							ELSE 0
						END,
						updated_at = ?
					WHERE source_id = ?
					  AND source_event_key = ?
				`, eventID, boolInt(desiredAuthoritative), nowText, sourceID, sourceEventKey)
				if err != nil {
					return sourceLinkWriteResult{}, err
				}
				wrote = true
			}
		}
	}
	return sourceLinkWriteResult{
		Applied:         wrote,
		ExistingEventID: resolvedTargetID,
		TargetEventID:   eventID,
	}, nil
}

func loadSourceLinkRowByKeyTx(ctx context.Context, q queryer, sourceID int64, sourceEventKey string) (sourceLinkRow, bool, bool, error) {
	sourceEventKey = strings.TrimSpace(sourceEventKey)
	if sourceEventKey == "" {
		return sourceLinkRow{}, false, false, nil
	}

	row := sourceLinkRow{}
	err := q.QueryRowContext(ctx, `
		SELECT
			l.event_id,
			l.is_authoritative,
			e.origin,
			e.publication_state,
			COALESCE(e.canonical_event_id, 0),
			COALESCE(c.origin, ''),
			COALESCE(c.publication_state, '')
		FROM event_source_links l
		JOIN events e ON e.id = l.event_id
		LEFT JOIN events c ON c.id = e.canonical_event_id
		WHERE l.source_id = ? AND l.source_event_key = ?
		LIMIT 1
	`, sourceID, sourceEventKey).Scan(
		&row.eventID,
		&row.isAuthoritative,
		&row.origin,
		&row.publicationState,
		&row.canonicalEventID,
		&row.canonicalOrigin,
		&row.canonicalState,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return sourceLinkRow{}, false, false, nil
	case err != nil:
		return sourceLinkRow{}, false, false, err
	default:
		resolvedID, ok, ambiguous := resolvedLiveEventIDForSourceLinkRow(row)
		if ambiguous {
			return sourceLinkRow{}, false, true, nil
		}
		if !ok {
			return sourceLinkRow{}, false, true, nil
		}
		row.resolvedEventID = resolvedID
		return row, true, false, nil
	}
}

func resolvedLiveEventIDForSourceLinkRow(row sourceLinkRow) (int64, bool, bool) {
	if isLiveNonWithheldEventRow(row.origin, row.publicationState) {
		return row.eventID, true, false
	}
	if strings.EqualFold(strings.TrimSpace(row.publicationState), string(domain.PublicationStateWithheld)) {
		if row.canonicalEventID.Valid && row.canonicalEventID.Int64 > 0 &&
			row.canonicalEventID.Int64 != row.eventID &&
			isLiveNonWithheldEventRow(row.canonicalOrigin.String, row.canonicalState.String) {
			return row.canonicalEventID.Int64, true, false
		}
	}
	return 0, false, true
}

func isLiveNonWithheldEventRow(origin, publicationState string) bool {
	return strings.EqualFold(strings.TrimSpace(origin), string(domain.OriginLive)) &&
		!strings.EqualFold(strings.TrimSpace(publicationState), string(domain.PublicationStateWithheld))
}

func resolveLiveEventIDBySourceIdentitiesTx(ctx context.Context, q queryer, sourceID int64, identities ingest.SourceIdentitySet) (int64, bool, bool, error) {
	keys := identities.LookupKeys()
	if len(keys) == 0 {
		return 0, false, false, nil
	}

	var (
		targetID int64
		found    bool
	)
	for _, sourceEventKey := range keys {
		row, ok, ambiguous, err := loadSourceLinkRowByKeyTx(ctx, q, sourceID, sourceEventKey)
		if err != nil {
			return 0, false, false, err
		}
		if ambiguous {
			return 0, false, true, nil
		}
		if !ok {
			continue
		}
		if !found {
			targetID = row.resolvedEventID
			found = true
			continue
		}
		if targetID != row.resolvedEventID {
			return 0, false, true, nil
		}
	}

	return targetID, found, false, nil
}

func resolveLiveEventRecordBySourceIdentitiesTx(ctx context.Context, q queryer, sourceID int64, identities ingest.SourceIdentitySet) (eventRecord, bool, bool, error) {
	eventID, found, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, q, sourceID, identities)
	if err != nil || !found || ambiguous {
		return eventRecord{}, found, ambiguous, err
	}
	record, ok, err := loadEventRecordByIDTx(ctx, q, eventID)
	if err != nil || !ok {
		return eventRecord{}, false, false, err
	}
	return record, true, false, nil
}

func authoritativeLinkedEventIDTx(ctx context.Context, q queryer, authoritative reviewGroupAuthoritativeLink) (int64, bool, bool, error) {
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, q, authoritative.SourceName, authoritative.SourceURL)
	if err != nil || !ok {
		return 0, ok, false, err
	}
	record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, q, sourceID, reviewGroupAuthoritativeSourceIdentities(authoritative))
	if err != nil {
		return 0, false, false, err
	}
	if ambiguous {
		return 0, false, true, nil
	}
	if !ok {
		return 0, false, false, nil
	}
	return record.ID, true, false, nil
}

func loadSourceIDByNameURLTx(ctx context.Context, q queryer, sourceName, sourceURL string) (int64, bool, error) {
	var sourceID int64
	err := q.QueryRowContext(ctx, `
		SELECT id
		FROM sources
		WHERE name = ? AND url = ?
		LIMIT 1
	`, sourceName, sourceURL).Scan(&sourceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return sourceID, true, nil
}

func loadEventRecordBySourceLinkTx(ctx context.Context, q queryer, sourceID int64, sourceEventKey string) (eventRecord, bool, error) {
	record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, q, sourceID, ingest.SourceIdentities(sourceIdentityInputForKey(sourceEventKey)))
	if err != nil || ambiguous || !ok {
		return eventRecord{}, false, err
	}
	return record, true, nil
}

func loadEventRecordBySlugTx(ctx context.Context, q queryer, slug string) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.slug = ?
		LIMIT 1
	`, slug)
}

func loadEventRecordBySlugAndSourceTx(ctx context.Context, q queryer, slug string, sourceID int64) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.slug = ? AND e.source_id = ?
		LIMIT 1
	`, slug, sourceID)
}

func loadLiveEventRecordBySlugTx(ctx context.Context, q queryer, slug string) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.slug = ? AND e.origin = ?
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
		LIMIT 1
	`, slug, string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func loadLiveEventRecordBySlugAndSourceTx(ctx context.Context, q queryer, slug string, sourceID int64) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.slug = ? AND e.source_id = ? AND e.origin = ?
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
		LIMIT 1
	`, slug, sourceID, string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func loadLiveEventRecordsByFingerprintTx(ctx context.Context, q queryer, name, venueSlug string, start time.Time) ([]eventRecord, error) {
	return loadEventRecords(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.name = ?
		  AND v.slug = ?
		  AND e.start_at = ?
		  AND e.origin = ?
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
	`, strings.TrimSpace(name), strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func loadLiveEventRecordsByFingerprintAndSourceTx(ctx context.Context, q queryer, sourceID int64, name, venueSlug string, start time.Time) ([]eventRecord, error) {
	return loadEventRecords(ctx, q, `
		SELECT
			e.id,
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.source_id = ?
		  AND e.name = ?
		  AND v.slug = ?
		  AND e.start_at = ?
		  AND e.origin = ?
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
	`, sourceID, strings.TrimSpace(name), strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func loadEventRecords(ctx context.Context, q queryer, query string, args ...any) ([]eventRecord, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []eventRecord
	for rows.Next() {
		var record eventRecord
		var origin string
		var publicationState string
		var startText string
		var endText sql.NullString
		var lastCheckedText string
		if err := rows.Scan(
			&record.ID,
			&record.Event.Slug,
			&record.Event.Name,
			&record.Event.VenueSlug,
			&startText,
			&endText,
			&record.Event.Genre,
			&record.Event.Status,
			&record.Event.Description,
			&record.Event.ImageURL,
			&record.Event.ImageSourceURL,
			&record.Event.ImageAlt,
			&record.Event.ImageWidth,
			&record.Event.ImageHeight,
			&record.Event.ImageFocusX,
			&record.Event.ImageFocusY,
			&record.Event.SourceName,
			&record.Event.SourceURL,
			&record.Event.OfficialListingURL,
			&record.Event.CalendarURL,
			&lastCheckedText,
			&origin,
			&publicationState,
		); err != nil {
			return nil, err
		}
		startAt, err := parseRFC3339UTC(startText)
		if err != nil {
			return nil, fmt.Errorf("parse event %q start time: %w", record.Event.Slug, err)
		}
		endAt, err := parseNullableRFC3339UTC(endText)
		if err != nil {
			return nil, fmt.Errorf("parse event %q end time: %w", record.Event.Slug, err)
		}
		lastChecked, err := parseRFC3339UTC(lastCheckedText)
		if err != nil {
			return nil, fmt.Errorf("parse event %q last checked time: %w", record.Event.Slug, err)
		}
		record.Event.Start = startAt
		record.Event.End = endAt
		record.Event.LastChecked = lastChecked
		focus := normalizedImageFocus(record.Event.ImageFocusX, record.Event.ImageFocusY)
		record.Event.ImageFocusX = focus.X
		record.Event.ImageFocusY = focus.Y
		record.Event.Origin = domain.Origin(origin)
		record.Event.PublicationState = normalizedPublicationState(domain.PublicationState(publicationState))
		if err := record.Event.ValidateCanonical(); err != nil {
			return nil, fmt.Errorf("event %q %w", record.Event.Slug, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := hydrateEventRecordRooms(ctx, q, records); err != nil {
		return nil, err
	}
	return records, nil
}

func loadEventRecord(ctx context.Context, q queryer, query string, args ...any) (eventRecord, bool, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return eventRecord{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{}, false, nil
	}

	var record eventRecord
	var origin string
	var publicationState string
	var startText string
	var endText sql.NullString
	var lastCheckedText string
	if err := rows.Scan(
		&record.ID,
		&record.Event.Slug,
		&record.Event.Name,
		&record.Event.VenueSlug,
		&startText,
		&endText,
		&record.Event.Genre,
		&record.Event.Status,
		&record.Event.Description,
		&record.Event.ImageURL,
		&record.Event.ImageSourceURL,
		&record.Event.ImageAlt,
		&record.Event.ImageWidth,
		&record.Event.ImageHeight,
		&record.Event.ImageFocusX,
		&record.Event.ImageFocusY,
		&record.Event.SourceName,
		&record.Event.SourceURL,
		&record.Event.OfficialListingURL,
		&record.Event.CalendarURL,
		&lastCheckedText,
		&origin,
		&publicationState,
	); err != nil {
		return eventRecord{}, false, err
	}
	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q start time: %w", record.Event.Slug, err)
	}
	end, err := parseNullableRFC3339UTC(endText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q end time: %w", record.Event.Slug, err)
	}
	lastChecked, err := parseRFC3339UTC(lastCheckedText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q last checked time: %w", record.Event.Slug, err)
	}
	record.Event.Start = start
	record.Event.End = end
	record.Event.LastChecked = lastChecked
	focus := normalizedImageFocus(record.Event.ImageFocusX, record.Event.ImageFocusY)
	record.Event.ImageFocusX = focus.X
	record.Event.ImageFocusY = focus.Y
	record.Event.Origin = domain.Origin(origin)
	record.Event.PublicationState = normalizedPublicationState(domain.PublicationState(publicationState))
	if err := record.Event.ValidateCanonical(); err != nil {
		return eventRecord{}, false, fmt.Errorf("event %q %w", record.Event.Slug, err)
	}
	if err := rows.Err(); err != nil {
		return eventRecord{}, false, err
	}
	records := []eventRecord{record}
	if err := hydrateEventRecordRooms(ctx, q, records); err != nil {
		return eventRecord{}, false, err
	}
	record = records[0]
	return record, true, nil
}

func firstNonEmptyReviewText(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

type reviewGroupAuthoritativeLink struct {
	SourceName     string
	SourceURL      string
	SourceEventKey string
}

func replaceEventSecondarySourceInfoTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if err := deleteCompatibilityEventSourceObservationsForEventTx(ctx, tx, eventID); err != nil {
		return err
	}
	if err := deleteEventSecondarySourceInfoForEventTx(ctx, tx, eventID); err != nil {
		return err
	}
	return upsertEventSecondarySourceInfoTx(ctx, tx, eventID, primary, candidates, now)
}

func upsertEventSecondarySourceInfoTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if eventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	rows := make([]eventSecondarySourceInfoRow, 0, len(candidates)*2)
	for _, candidate := range candidates {
		sourceName := strings.TrimSpace(candidate.SourceName)
		sourceURL := strings.TrimSpace(candidate.SourceURL)
		calendarURL := strings.TrimSpace(candidate.CalendarURL)
		authoritativeMatchURL := sourceURL
		if calendarURL != "" {
			authoritativeMatchURL = calendarURL
		}
		if sourceName == "" || sourceURL == "" {
			sourceURL = calendarURL
		}
		if sourceName == "" || sourceURL == "" {
			continue
		}
		if sourceName == primary.SourceName && authoritativeMatchURL == primary.SourceURL {
			continue
		}

		sourceID, err := ensureSourceTx(ctx, tx, sourceName, sourceURL)
		if err != nil {
			return err
		}
		compatibilityKey := reviewStoredCandidateSourceIdentities(candidate).PrimaryKey()
		for _, item := range []struct {
			infoType string
			value    string
		}{
			{infoType: "genre", value: strings.TrimSpace(candidate.Genre)},
			{infoType: "description", value: strings.TrimSpace(candidate.Description)},
		} {
			if item.value == "" {
				continue
			}
			rows = append(rows, eventSecondarySourceInfoRow{
				EventID:    eventID,
				SourceID:   sourceID,
				VenueSlug:  strings.TrimSpace(candidate.VenueSlug),
				EventName:  strings.TrimSpace(candidate.Name),
				StartAt:    strings.TrimSpace(candidate.StartAt),
				InfoType:   item.infoType,
				Value:      item.value,
				RecordedAt: now,
			})
			if err := upsertCompatibilitySecondarySourceObservationTx(ctx, tx, eventID, sourceID, compatibilityKey, item.infoType, item.value, now); err != nil {
				return err
			}
		}
	}
	for _, row := range rows {
		if err := upsertEventSecondarySourceInfoRowTx(ctx, tx, row); err != nil {
			return err
		}
	}
	return nil
}

func primarySourceIdentity(event domain.Event) reviewGroupAuthoritativeLink {
	return reviewGroupAuthoritativeLink{
		SourceName: strings.TrimSpace(event.SourceName),
		SourceURL:  strings.TrimSpace(event.SourceURL),
	}
}

func refreshEventGenresFromStoredDescriptionsTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, canonicalDescription string, now time.Time) error {
	secondaryDescriptions, err := loadSecondaryDescriptionsForEventIDTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	return refreshEventGenresTx(ctx, tx, eventID, canonicalDescription, secondaryDescriptions, now)
}

type eventSecondarySourceInfoRow struct {
	EventID    int64
	SourceID   int64
	VenueSlug  string
	EventName  string
	StartAt    string
	InfoType   string
	Value      string
	RecordedAt time.Time
}

func upsertEventSecondarySourceInfoRowTx(ctx context.Context, tx execer, row eventSecondarySourceInfoRow) error {
	if row.EventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	if row.SourceID <= 0 {
		return errors.New("secondary source info source ID is required")
	}
	if row.VenueSlug == "" || row.EventName == "" || row.StartAt == "" || row.InfoType == "" || row.Value == "" {
		return errors.New("secondary source info row is incomplete")
	}
	recordedAt := formatRFC3339UTC(row.RecordedAt)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO event_secondary_source_info (
			event_id,
			source_id,
			venue_slug,
			event_name,
			start_at,
			info_type,
			value,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_id, venue_slug, event_name, start_at, info_type) DO UPDATE SET
			event_id = excluded.event_id,
			value = excluded.value,
			updated_at = excluded.updated_at
	`, row.EventID, row.SourceID, row.VenueSlug, row.EventName, row.StartAt, row.InfoType, row.Value, recordedAt, recordedAt)
	return err
}

func deleteEventSecondarySourceInfoForEventTx(ctx context.Context, tx execer, eventID int64) error {
	if eventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_secondary_source_info
		WHERE event_id = ?
	`, eventID)
	return err
}

func buildResolvedEvent(group review.Group, selected map[review.Field]review.Candidate, publishedAt time.Time) (domain.Event, error) {
	name := strings.TrimSpace(review.CandidateValue(selected[review.FieldName], review.FieldName))
	venueSlug := strings.TrimSpace(review.CandidateValue(selected[review.FieldVenueSlug], review.FieldVenueSlug))
	roomCandidate := selected[review.FieldRoomSlugs]
	startText := strings.TrimSpace(review.CandidateValue(selected[review.FieldStartAt], review.FieldStartAt))
	endText := strings.TrimSpace(review.CandidateValue(selected[review.FieldEndAt], review.FieldEndAt))
	genre := strings.TrimSpace(review.CandidateValue(selected[review.FieldGenre], review.FieldGenre))
	status := strings.TrimSpace(review.CandidateValue(selected[review.FieldStatus], review.FieldStatus))
	description := strings.TrimSpace(review.CandidateValue(selected[review.FieldDescription], review.FieldDescription))
	imageCandidate := selected[review.FieldImageURL]
	imageURL := strings.TrimSpace(review.CandidateValue(imageCandidate, review.FieldImageURL))
	sourceName := strings.TrimSpace(review.CandidateValue(selected[review.FieldSourceName], review.FieldSourceName))
	if sourceName == "" {
		sourceName = strings.TrimSpace(group.SourceName)
	}
	sourceURL := strings.TrimSpace(review.CandidateValue(selected[review.FieldSourceURL], review.FieldSourceURL))
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(group.SourceURL)
	}
	calendarURL := ""
	if sourceCandidate, ok := selected[review.FieldSourceURL]; ok {
		calendarURL = strings.TrimSpace(sourceCandidate.CalendarURL)
	}
	if calendarURL == "" && ingest.IsCalendarURL(sourceURL) {
		calendarURL = sourceURL
	}
	officialListingURL := ""
	if !ingest.IsCalendarURL(sourceURL) {
		officialListingURL = sourceURL
	}

	if name == "" {
		return domain.Event{}, errors.New("review event name is required")
	}
	if venueSlug == "" {
		return domain.Event{}, errors.New("review event venue slug is required")
	}
	if err := validateRoomChoiceVenue(roomCandidate, venueSlug); err != nil {
		return domain.Event{}, err
	}
	if startText == "" {
		return domain.Event{}, errors.New("review event start time is required")
	}
	if sourceName == "" {
		return domain.Event{}, errors.New("review event source name is required")
	}
	if sourceURL == "" {
		return domain.Event{}, errors.New("review event source URL is required")
	}

	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse review event start time: %w", err)
	}
	end := time.Time{}
	if endText != "" {
		end, err = parseRFC3339UTC(endText)
		if err != nil {
			return domain.Event{}, fmt.Errorf("parse review event end time: %w", err)
		}
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		return domain.Event{}, err
	}

	event := domain.Event{
		Slug:               slug,
		Name:               name,
		VenueSlug:          venueSlug,
		Rooms:              normalizeRoomsForVenue(venueSlug, roomCandidate.Rooms),
		RoomText:           strings.TrimSpace(roomCandidate.RoomText),
		Start:              start,
		End:                end,
		Genre:              genre,
		Status:             status,
		Description:        description,
		ImageURL:           imageURL,
		ImageSourceURL:     strings.TrimSpace(imageCandidate.ImageSourceURL),
		ImageAlt:           strings.TrimSpace(imageCandidate.ImageAlt),
		ImageWidth:         imageCandidate.ImageWidth,
		ImageHeight:        imageCandidate.ImageHeight,
		ImageFocusX:        imageCandidate.ImageFocusX,
		ImageFocusY:        imageCandidate.ImageFocusY,
		SourceName:         sourceName,
		SourceURL:          sourceURL,
		OfficialListingURL: officialListingURL,
		CalendarURL:        calendarURL,
		LastChecked:        publishedAt.UTC(),
		Origin:             domain.OriginLive,
		PublicationState:   domain.PublicationStateReviewed,
	}
	if err := event.ValidateCanonical(); err != nil {
		return domain.Event{}, fmt.Errorf("review event %w", err)
	}
	return event, nil
}

func validateRoomChoiceVenue(roomCandidate review.Candidate, venueSlug string) error {
	if len(roomCandidate.Rooms) == 0 {
		return nil
	}
	venueSlug = strings.TrimSpace(venueSlug)
	roomVenueSlug := strings.TrimSpace(roomCandidate.VenueSlug)
	if roomVenueSlug == "" {
		return errors.New("review room choice has rooms but no venue slug")
	}
	if roomVenueSlug != venueSlug {
		return fmt.Errorf("review room choice venue %q does not match selected venue %q", roomVenueSlug, venueSlug)
	}
	for _, room := range roomCandidate.Rooms {
		if roomVenueSlug := strings.TrimSpace(room.VenueSlug); roomVenueSlug != "" && roomVenueSlug != venueSlug {
			return fmt.Errorf("review room %q belongs to venue %q, not selected venue %q", strings.TrimSpace(room.Slug), roomVenueSlug, venueSlug)
		}
	}
	return nil
}

func buildLiveEventSlug(name, venueSlug string, start time.Time) (string, error) {
	nameSlug := slugFromText(name)
	venueSlugPart := slugFromText(venueSlug)
	if nameSlug == "" {
		return "", errors.New("review event name cannot produce a slug")
	}
	if venueSlugPart == "" {
		return "", errors.New("review event venue slug cannot produce a slug")
	}
	return fmt.Sprintf("live-%s-%s-%s", nameSlug, venueSlugPart, start.UTC().Format("20060102150405")), nil
}

func slugFromText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	wroteDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			wroteDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			wroteDash = false
		default:
			if builder.Len() > 0 && !wroteDash {
				builder.WriteByte('-')
				wroteDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}

func upsertEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event, now time.Time) (eventRecord, error) {
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return eventRecord{}, err
	}
	if !ok {
		return eventRecord{}, fmt.Errorf("venue %q not found", event.VenueSlug)
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return eventRecord{}, err
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			official_listing_url,
			calendar_url,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(slug) DO UPDATE SET
			venue_id = excluded.venue_id,
			source_id = excluded.source_id,
			name = excluded.name,
			start_at = excluded.start_at,
			end_at = excluded.end_at,
			genre = excluded.genre,
			status = excluded.status,
			description = excluded.description,
			image_url = CASE WHEN excluded.image_url = '' THEN image_url ELSE excluded.image_url END,
			image_source_url = CASE WHEN excluded.image_url = '' THEN image_source_url ELSE excluded.image_source_url END,
			image_alt = CASE WHEN excluded.image_url = '' THEN image_alt ELSE excluded.image_alt END,
			image_width = CASE WHEN excluded.image_url = '' THEN image_width ELSE excluded.image_width END,
			image_height = CASE WHEN excluded.image_url = '' THEN image_height ELSE excluded.image_height END,
			image_focus_x = CASE WHEN excluded.image_url = '' THEN image_focus_x ELSE excluded.image_focus_x END,
			image_focus_y = CASE WHEN excluded.image_url = '' THEN image_focus_y ELSE excluded.image_focus_y END,
			official_listing_url = excluded.official_listing_url,
			calendar_url = excluded.calendar_url,
			last_checked_at = excluded.last_checked_at,
			origin = excluded.origin,
			publication_state = excluded.publication_state
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		event.ImageURL, event.ImageSourceURL, event.ImageAlt, event.ImageWidth, event.ImageHeight,
		normalizedImageFocusValue(event.ImageFocusX),
		normalizedImageFocusValue(event.ImageFocusY),
		event.OfficialListingURL,
		event.CalendarURL,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin),
		string(normalizedPublicationState(event.PublicationState))); err != nil {
		return eventRecord{}, err
	}
	record, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug)
	if err != nil {
		return eventRecord{}, err
	}
	if !ok {
		return eventRecord{}, fmt.Errorf("event %q not found after upsert", event.Slug)
	}
	if err := replaceEventRoomsTx(ctx, tx, record.ID, event); err != nil {
		return eventRecord{}, err
	}
	if err := ensureActiveExactIdentityTx(ctx, tx, record.ID, record.Event, 0, now); err != nil {
		return eventRecord{}, err
	}
	record.Event.Rooms = append([]domain.VenueRoom(nil), event.Rooms...)
	record.Event.RoomText = strings.TrimSpace(event.RoomText)
	return record, nil
}

func updateCanonicalMatchedEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event, now time.Time) (int64, error) {
	if eventID <= 0 {
		return 0, errors.New("canonical event ID is required")
	}
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("venue %q not found", event.VenueSlug)
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return 0, err
	}
	if conflict, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return 0, err
	} else if ok && conflict.ID != eventID {
		if !eventRecordHasResolvedIdentity(conflict, event) {
			return 0, fmt.Errorf("review event slug %q already belongs to a different event", event.Slug)
		}
		if err := updateEventRecordFieldsTx(ctx, tx, conflict.ID, venueID, sourceID, event, now); err != nil {
			return 0, err
		}
		if err := mergeDuplicateEventRecordTx(ctx, tx, eventID, conflict.ID, now); err != nil {
			return 0, err
		}
		return conflict.ID, nil
	}
	if err := updateEventRecordFieldsTx(ctx, tx, eventID, venueID, sourceID, event, now); err != nil {
		return 0, err
	}
	return eventID, nil
}

func eventRecordHasResolvedIdentity(record eventRecord, event domain.Event) bool {
	return strings.TrimSpace(record.Event.Slug) == strings.TrimSpace(event.Slug) &&
		strings.TrimSpace(record.Event.VenueSlug) == strings.TrimSpace(event.VenueSlug) &&
		record.Event.Start.Equal(event.Start) &&
		record.Event.Origin == domain.OriginLive
}

func updateEventRecordFieldsTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID, venueID, sourceID int64, event domain.Event, now time.Time) error {
	incomingImageURL := strings.TrimSpace(event.ImageURL)
	res, err := tx.ExecContext(ctx, `
		UPDATE events
		SET slug = ?,
			venue_id = ?,
			source_id = ?,
			name = ?,
			start_at = ?,
			end_at = ?,
			genre = ?,
			status = ?,
			description = ?,
			image_url = CASE WHEN ? = '' THEN image_url ELSE ? END,
			image_source_url = CASE WHEN ? = '' THEN image_source_url ELSE ? END,
			image_alt = CASE WHEN ? = '' THEN image_alt ELSE ? END,
			image_width = CASE WHEN ? = '' THEN image_width ELSE ? END,
			image_height = CASE WHEN ? = '' THEN image_height ELSE ? END,
			image_focus_x = CASE WHEN ? = '' THEN image_focus_x ELSE ? END,
			image_focus_y = CASE WHEN ? = '' THEN image_focus_y ELSE ? END,
			official_listing_url = ?,
			calendar_url = ?,
			last_checked_at = ?,
			origin = ?,
			publication_state = ?
		WHERE id = ?
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		incomingImageURL, incomingImageURL,
		incomingImageURL, strings.TrimSpace(event.ImageSourceURL),
		incomingImageURL, strings.TrimSpace(event.ImageAlt),
		incomingImageURL, event.ImageWidth,
		incomingImageURL, event.ImageHeight,
		incomingImageURL, normalizedImageFocusValue(event.ImageFocusX),
		incomingImageURL, normalizedImageFocusValue(event.ImageFocusY),
		event.OfficialListingURL,
		event.CalendarURL,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin),
		string(normalizedPublicationState(event.PublicationState)),
		eventID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return nil
	}
	if err := replaceEventRoomsTx(ctx, tx, eventID, event); err != nil {
		return err
	}
	return ensureActiveExactIdentityTx(ctx, tx, eventID, event, 0, now)
}

func mergeDuplicateEventRecordTx(ctx context.Context, tx interface {
	execer
	queryer
}, duplicateEventID, targetEventID int64, now time.Time) error {
	if duplicateEventID <= 0 {
		return errors.New("duplicate event ID is required")
	}
	if targetEventID <= 0 {
		return errors.New("target event ID is required")
	}
	if duplicateEventID == targetEventID {
		return nil
	}
	nowText := formatRFC3339UTC(now)
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_source_links
		SET event_id = ?,
			updated_at = ?
		WHERE event_id = ?
	`, targetEventID, nowText, duplicateEventID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_secondary_source_info
		SET event_id = ?,
			updated_at = ?
		WHERE event_id = ?
	`, targetEventID, nowText, duplicateEventID); err != nil {
		return err
	}
	if err := rehomeEventSourceAttributeObservationsForDuplicateEventTx(ctx, tx, duplicateEventID, targetEventID, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_candidates
		SET canonical_event_id = ?
		WHERE canonical_event_id = ?
	`, targetEventID, duplicateEventID); err != nil {
		return err
	}
	if err := deactivateActiveExactIdentitiesForEventTx(ctx, tx, duplicateEventID, fmt.Sprintf("merged into event %d", targetEventID), 0, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_exact_identities
		SET event_id = ?,
			updated_at = ?
		WHERE event_id = ?
	`, targetEventID, formatRFC3339UTC(now), duplicateEventID); err != nil {
		return err
	}
	targetRecord, ok, err := loadEventRecordByIDTx(ctx, tx, targetEventID)
	if err != nil {
		return err
	}
	if ok {
		if err := ensureActiveExactIdentityTx(ctx, tx, targetRecord.ID, targetRecord.Event, 0, now); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `
		DELETE FROM events
		WHERE id = ?
	`, duplicateEventID)
	return err
}

func loadVenueIDBySlugTx(ctx context.Context, q queryer, slug string) (int64, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT id
		FROM venues
		WHERE slug = ?
		LIMIT 1
	`, slug)
	var id int64
	switch err := row.Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return id, true, nil
}
