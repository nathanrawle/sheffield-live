package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

type eventReviewClusterAutoResolutionCandidate struct {
	EvidenceID      int64
	SourceID        int64
	Candidate       review.Candidate
	CandidateInput  review.CandidateInput
	SourceAuthority seedstore.SourceAuthority
	SourceName      string
	SourceURL       string
}

type eventReviewClusterAutoResolutionResult struct {
	Result             string
	CanonicalEventID   int64
	CanonicalEventSlug string
	ClusterStatus      seedstore.EventReviewClusterStatus
	Version            int
	Applied            eventReviewResolutionAppliedAutoResolutionSnapshot
}

type eventReviewClusterEvidencePayload struct {
	SourceAuthority           string                           `json:"source_authority,omitempty"`
	SourceName                string                           `json:"source_name,omitempty"`
	SourceURL                 string                           `json:"source_url,omitempty"`
	CalendarURL               string                           `json:"calendar_url,omitempty"`
	Provenance                string                           `json:"provenance,omitempty"`
	CandidateExternalID       string                           `json:"candidate_external_id,omitempty"`
	CandidateTitle            string                           `json:"candidate_title,omitempty"`
	CandidateVenueSlug        string                           `json:"candidate_venue_slug,omitempty"`
	CandidateVenueText        string                           `json:"candidate_venue_text,omitempty"`
	CandidateVenueLocationRaw string                           `json:"candidate_venue_location_raw,omitempty"`
	CandidateRoomText         string                           `json:"candidate_room_text,omitempty"`
	CandidateRooms            []eventReviewClusterEvidenceRoom `json:"candidate_rooms,omitempty"`
	CandidateStartAt          string                           `json:"candidate_start_at,omitempty"`
	CandidateEndAt            string                           `json:"candidate_end_at,omitempty"`
	CandidateGenre            string                           `json:"candidate_genre,omitempty"`
	CandidateStatus           string                           `json:"candidate_status,omitempty"`
	CandidateDescription      string                           `json:"candidate_description,omitempty"`
	CandidateImageURL         string                           `json:"candidate_image_url,omitempty"`
	CandidateImageSourceURL   string                           `json:"candidate_image_source_url,omitempty"`
	CandidateImageAlt         string                           `json:"candidate_image_alt,omitempty"`
	CandidateImageWidth       int                              `json:"candidate_image_width,omitempty"`
	CandidateImageHeight      int                              `json:"candidate_image_height,omitempty"`
	CandidateImageFocusX      int                              `json:"candidate_image_focus_x,omitempty"`
	CandidateImageFocusY      int                              `json:"candidate_image_focus_y,omitempty"`
}

type eventReviewClusterEvidenceRoom struct {
	VenueSlug string `json:"venue_slug,omitempty"`
	Slug      string `json:"slug,omitempty"`
	Name      string `json:"name,omitempty"`
}

func maybeAutoResolveEventReviewClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, cluster seedstore.EventReviewCluster, scope seedstore.ObservationRunScope, sourceMetadata ingest.SourceMetadataLookup, now time.Time) (*eventReviewClusterAutoResolutionResult, error) {
	evidence, err := loadEventReviewClusterEvidenceSummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return nil, err
	}
	parsed, err := parseEventReviewClusterAutoResolutionCandidates(evidence)
	if err != nil || len(parsed) == 0 {
		return nil, nil
	}

	hasCanonical := cluster.CanonicalEventID != nil && *cluster.CanonicalEventID > 0
	if hasCanonical {
		canonical, ok, err := loadEventRecordByIDTx(ctx, tx, *cluster.CanonicalEventID)
		if err != nil {
			return nil, err
		}
		if ok && eventReviewClusterCandidatesMatchCanonicalEvent(parsed, canonical.Event) {
			return autoResolveEventReviewClusterCanonicalExactMatchTx(ctx, tx, cluster, parsed, scope, now)
		}
	}
	if !hasCanonical && len(parsed) >= 2 && unanimousStagedDuplicate(eventReviewClusterCandidates(parsed)) {
		return autoResolveEventReviewClusterUnanimousDuplicateTx(ctx, tx, cluster, parsed, scope, sourceMetadata, now)
	}
	return nil, nil
}

func eventReviewClusterCandidatesMatchCanonicalEvent(candidates []eventReviewClusterAutoResolutionCandidate, event domain.Event) bool {
	if len(candidates) == 0 {
		return false
	}
	for _, candidate := range candidates {
		candidateEvent, ok, err := eventReviewClusterAutoResolutionEvent(candidate, event.LastChecked)
		if err != nil || !ok {
			return false
		}
		if !eventReviewAutoResolutionEventsExactMatch(candidateEvent, event) {
			return false
		}
	}
	return true
}

func eventReviewAutoResolutionEventsExactMatch(candidate, canonical domain.Event) bool {
	if normalizedReviewEventName(candidate.Name) != normalizedReviewEventName(canonical.Name) {
		return false
	}
	if strings.TrimSpace(candidate.VenueSlug) != strings.TrimSpace(canonical.VenueSlug) {
		return false
	}
	if strings.TrimSpace(candidate.RoomText) != strings.TrimSpace(canonical.RoomText) {
		return false
	}
	if !eventReviewVenueRoomsExactMatch(candidate.Rooms, canonical.Rooms) {
		return false
	}
	if !candidate.Start.UTC().Equal(canonical.Start.UTC()) {
		return false
	}
	if !candidate.End.UTC().Equal(canonical.End.UTC()) {
		return false
	}
	return strings.TrimSpace(candidate.Genre) == strings.TrimSpace(canonical.Genre) &&
		strings.TrimSpace(candidate.Status) == strings.TrimSpace(canonical.Status) &&
		strings.TrimSpace(candidate.Description) == strings.TrimSpace(canonical.Description) &&
		eventReviewImageFieldsExactMatch(candidate, canonical)
}

func eventReviewImageFieldsExactMatch(candidate, canonical domain.Event) bool {
	candidateImageURL := strings.TrimSpace(candidate.ImageURL)
	canonicalImageURL := strings.TrimSpace(canonical.ImageURL)
	if candidateImageURL == "" && canonicalImageURL == "" {
		return strings.TrimSpace(candidate.ImageSourceURL) == "" &&
			strings.TrimSpace(canonical.ImageSourceURL) == "" &&
			strings.TrimSpace(candidate.ImageAlt) == "" &&
			strings.TrimSpace(canonical.ImageAlt) == "" &&
			candidate.ImageWidth == 0 &&
			canonical.ImageWidth == 0 &&
			candidate.ImageHeight == 0 &&
			canonical.ImageHeight == 0
	}
	return candidateImageURL == canonicalImageURL &&
		strings.TrimSpace(candidate.ImageSourceURL) == strings.TrimSpace(canonical.ImageSourceURL) &&
		strings.TrimSpace(candidate.ImageAlt) == strings.TrimSpace(canonical.ImageAlt) &&
		candidate.ImageWidth == canonical.ImageWidth &&
		candidate.ImageHeight == canonical.ImageHeight &&
		candidate.ImageFocusX == canonical.ImageFocusX &&
		candidate.ImageFocusY == canonical.ImageFocusY
}

func eventReviewVenueRoomsExactMatch(a, b []domain.VenueRoom) bool {
	a = normalizeRoomsForVenue("", a)
	b = normalizeRoomsForVenue("", b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.TrimSpace(a[i].VenueSlug) != strings.TrimSpace(b[i].VenueSlug) ||
			strings.TrimSpace(a[i].Slug) != strings.TrimSpace(b[i].Slug) ||
			strings.TrimSpace(a[i].Name) != strings.TrimSpace(b[i].Name) {
			return false
		}
	}
	return true
}

func eventReviewClusterAutoResolutionEvent(candidate eventReviewClusterAutoResolutionCandidate, now time.Time) (domain.Event, bool, error) {
	event, err := buildResolvedEvent(review.Group{
		SourceName: candidate.SourceName,
		SourceURL:  candidate.SourceURL,
	}, reviewFieldsForAutoResolutionCandidate(candidate.Candidate), now)
	if err != nil {
		return domain.Event{}, false, nil
	}
	return event, true, nil
}

func parseEventReviewClusterAutoResolutionCandidates(evidence []seedstore.EventReviewClusterEvidenceSummary) ([]eventReviewClusterAutoResolutionCandidate, error) {
	candidates := make([]eventReviewClusterAutoResolutionCandidate, 0, len(evidence))
	for _, row := range evidence {
		candidate, err := parseEventReviewClusterAutoResolutionCandidate(row)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func parseEventReviewClusterAutoResolutionCandidate(row seedstore.EventReviewClusterEvidenceSummary) (eventReviewClusterAutoResolutionCandidate, error) {
	var payload eventReviewClusterEvidencePayload
	if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
		return eventReviewClusterAutoResolutionCandidate{}, err
	}

	sourceName := firstNonEmptyReviewString(payload.SourceName, row.SourceName)
	sourceURL := firstNonEmptyReviewString(payload.SourceURL, row.SourceURL)
	candidateInput := review.CandidateInput{
		ExternalID:       strings.TrimSpace(payload.CandidateExternalID),
		Name:             strings.TrimSpace(payload.CandidateTitle),
		VenueSlug:        strings.TrimSpace(payload.CandidateVenueSlug),
		VenueText:        strings.TrimSpace(payload.CandidateVenueText),
		VenueLocationRaw: strings.TrimSpace(payload.CandidateVenueLocationRaw),
		RoomText:         strings.TrimSpace(payload.CandidateRoomText),
		Rooms:            eventReviewClusterEvidenceRoomsFromPayload(payload.CandidateRooms),
		StartAt:          strings.TrimSpace(payload.CandidateStartAt),
		EndAt:            strings.TrimSpace(payload.CandidateEndAt),
		Genre:            strings.TrimSpace(payload.CandidateGenre),
		Status:           strings.TrimSpace(payload.CandidateStatus),
		Description:      strings.TrimSpace(payload.CandidateDescription),
		ImageURL:         strings.TrimSpace(payload.CandidateImageURL),
		ImageSourceURL:   strings.TrimSpace(payload.CandidateImageSourceURL),
		ImageAlt:         strings.TrimSpace(payload.CandidateImageAlt),
		ImageWidth:       payload.CandidateImageWidth,
		ImageHeight:      payload.CandidateImageHeight,
		ImageFocusX:      payload.CandidateImageFocusX,
		ImageFocusY:      payload.CandidateImageFocusY,
		SourceName:       sourceName,
		SourceURL:        sourceURL,
		CalendarURL:      strings.TrimSpace(payload.CalendarURL),
		Provenance:       strings.TrimSpace(payload.Provenance),
	}

	return eventReviewClusterAutoResolutionCandidate{
		EvidenceID: row.EvidenceID,
		SourceID:   row.SourceID,
		Candidate: review.Candidate{
			ExternalID:       candidateInput.ExternalID,
			Name:             candidateInput.Name,
			VenueSlug:        candidateInput.VenueSlug,
			VenueText:        candidateInput.VenueText,
			VenueLocationRaw: candidateInput.VenueLocationRaw,
			RoomText:         candidateInput.RoomText,
			Rooms:            append([]domain.VenueRoom(nil), candidateInput.Rooms...),
			StartAt:          candidateInput.StartAt,
			EndAt:            candidateInput.EndAt,
			Genre:            candidateInput.Genre,
			Status:           candidateInput.Status,
			Description:      candidateInput.Description,
			ImageURL:         candidateInput.ImageURL,
			ImageSourceURL:   candidateInput.ImageSourceURL,
			ImageAlt:         candidateInput.ImageAlt,
			ImageWidth:       candidateInput.ImageWidth,
			ImageHeight:      candidateInput.ImageHeight,
			ImageFocusX:      candidateInput.ImageFocusX,
			ImageFocusY:      candidateInput.ImageFocusY,
			SourceName:       candidateInput.SourceName,
			SourceURL:        candidateInput.SourceURL,
			CalendarURL:      candidateInput.CalendarURL,
			Provenance:       candidateInput.Provenance,
		},
		CandidateInput:  candidateInput,
		SourceAuthority: seedstore.SourceAuthority(strings.TrimSpace(payload.SourceAuthority)),
		SourceName:      sourceName,
		SourceURL:       sourceURL,
	}, nil
}

func eventReviewClusterEvidenceRoomsFromPayload(rooms []eventReviewClusterEvidenceRoom) []domain.VenueRoom {
	if len(rooms) == 0 {
		return nil
	}
	out := make([]domain.VenueRoom, 0, len(rooms))
	for _, room := range rooms {
		out = append(out, domain.VenueRoom{
			VenueSlug: strings.TrimSpace(room.VenueSlug),
			Slug:      strings.TrimSpace(room.Slug),
			Name:      strings.TrimSpace(room.Name),
		})
	}
	return out
}

func eventReviewClusterCandidates(candidates []eventReviewClusterAutoResolutionCandidate) []review.Candidate {
	out := make([]review.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.Candidate)
	}
	return out
}

func eventReviewClusterSelectedAutoResolutionCandidate(candidates []eventReviewClusterAutoResolutionCandidate) eventReviewClusterAutoResolutionCandidate {
	selected := candidates[0]
	for _, candidate := range candidates {
		if candidate.SourceAuthority == seedstore.SourceAuthorityAuthoritative {
			return candidate
		}
	}
	return selected
}

func autoResolveEventReviewClusterCanonicalExactMatchTx(ctx context.Context, tx interface {
	execer
	queryer
}, cluster seedstore.EventReviewCluster, candidates []eventReviewClusterAutoResolutionCandidate, scope seedstore.ObservationRunScope, now time.Time) (*eventReviewClusterAutoResolutionResult, error) {
	selected := eventReviewClusterSelectedAutoResolutionCandidate(candidates)
	event, ok, err := eventReviewClusterAutoResolutionEvent(selected, now)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	mode := reviewSourceIdentitySupporting
	authority := seedstore.SourceAuthoritySupporting
	if selected.SourceAuthority == seedstore.SourceAuthorityAuthoritative {
		mode = reviewSourceIdentityAuthoritative
		authority = seedstore.SourceAuthorityAuthoritative
	}
	sourceCtx := reviewSourceIdentityContextForCandidateInput(mode, selected.SourceName, selected.SourceURL, "", "", "", selected.CandidateInput, "event_review_canonical_exact_match")

	targetRecord, ok, err := loadEventRecordByIDTx(ctx, tx, *cluster.CanonicalEventID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("event %d not found", *cluster.CanonicalEventID)
	}
	compatibleEvidenceEventIDs, err := canonicalExactAutoResolutionEvidenceEventIDsCompatibleTx(ctx, tx, targetRecord, candidates, now)
	if err != nil {
		return nil, err
	}
	if !compatibleEvidenceEventIDs {
		return nil, nil
	}

	if selected.SourceAuthority == seedstore.SourceAuthorityAuthoritative {
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, *cluster.CanonicalEventID, selected.SourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return nil, err
		}
		if writeResult.Ambiguous {
			return nil, nil
		}

		incoming := event
		incoming.SourceName = sourceCtx.SourceName
		incoming.SourceURL = sourceCtx.SourceURL
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, selected.SourceID, sourceCtx, authority, targetRecord, incoming); err != nil {
			return nil, err
		}
	}

	recordSupportingProvenance := selected.SourceAuthority != seedstore.SourceAuthorityAuthoritative
	targetRecord, ok, err = applyCanonicalExactAutoResolutionProvenanceTx(ctx, tx, targetRecord, candidates, scope, now, recordSupportingProvenance)
	if err != nil || !ok {
		return nil, err
	}
	if err := markEventReviewedTx(ctx, tx, targetRecord.ID); err != nil {
		return nil, err
	}
	targetRecord, ok, err = loadEventRecordByIDTx(ctx, tx, targetRecord.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("event %d not found", *cluster.CanonicalEventID)
	}
	applied := eventReviewResolutionAppliedAutoResolutionSnapshot{
		EventID:       targetRecord.ID,
		EventSlug:     targetRecord.Event.Slug,
		Result:        "canonical_exact_match",
		SourceID:      selected.SourceID,
		SourceName:    sourceCtx.SourceName,
		SourceURL:     sourceCtx.SourceURL,
		EvidenceCount: len(candidates),
	}
	snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusResolved, "", nil, nil, &applied, nil, nil, nil, nil, nil, nil, now)
	if err != nil {
		return nil, err
	}
	if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusResolved, snapshot, "", now); err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET status = ?, canonical_event_id = ?, version = version + 1, updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, string(seedstore.EventReviewClusterStatusResolved), targetRecord.ID, formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rowsAffected != 1 {
		return nil, fmt.Errorf("event review cluster %d update was rejected", cluster.ID)
	}
	return &eventReviewClusterAutoResolutionResult{
		Result:             "canonical_exact_match",
		CanonicalEventID:   targetRecord.ID,
		CanonicalEventSlug: targetRecord.Event.Slug,
		ClusterStatus:      seedstore.EventReviewClusterStatusResolved,
		Version:            cluster.Version + 1,
		Applied:            applied,
	}, nil
}

func applyCanonicalExactAutoResolutionProvenanceTx(ctx context.Context, tx interface {
	execer
	queryer
}, targetRecord eventRecord, candidates []eventReviewClusterAutoResolutionCandidate, scope seedstore.ObservationRunScope, now time.Time, recordSupportingProvenance bool) (eventRecord, bool, error) {
	type supportingProvenanceCandidate struct {
		candidate eventReviewClusterAutoResolutionCandidate
		incoming  domain.Event
		sourceCtx reviewSourceIdentityContext
	}
	matchingCandidates := make([]review.Candidate, 0, len(candidates))
	supportingProvenanceCandidates := make([]supportingProvenanceCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		incoming, ok, err := eventReviewClusterAutoResolutionEvent(candidate, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if !ok || !eventReviewAutoResolutionEventsExactMatch(incoming, targetRecord.Event) {
			continue
		}
		compatibleEvidenceEventID, err := eventReviewEvidenceEventIDCompatibleWithCanonicalTx(ctx, tx, candidate.EvidenceID, targetRecord.ID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if !compatibleEvidenceEventID {
			return eventRecord{}, false, nil
		}
		if !recordSupportingProvenance || candidate.SourceAuthority != seedstore.SourceAuthoritySupporting {
			continue
		}
		matchingCandidates = append(matchingCandidates, candidate.Candidate)
		sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentitySupporting, candidate.SourceName, candidate.SourceURL, "", "", "", candidate.CandidateInput, "event_review_canonical_exact_match")
		resolvedID, found, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, tx, candidate.SourceID, sourceCtx.Identities)
		if err != nil {
			return eventRecord{}, false, err
		}
		if ambiguous || (found && resolvedID != targetRecord.ID) {
			return eventRecord{}, false, nil
		}
		supportingProvenanceCandidates = append(supportingProvenanceCandidates, supportingProvenanceCandidate{
			candidate: candidate,
			incoming:  incoming,
			sourceCtx: sourceCtx,
		})
	}

	for _, candidate := range candidates {
		incoming, ok, err := eventReviewClusterAutoResolutionEvent(candidate, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if !ok || !eventReviewAutoResolutionEventsExactMatch(incoming, targetRecord.Event) {
			continue
		}
		targetEventID := targetRecord.ID
		if err := fillEventReviewEvidenceEventIDTx(ctx, tx, candidate.EvidenceID, &targetEventID, now); err != nil {
			return eventRecord{}, false, err
		}
	}

	for _, item := range supportingProvenanceCandidates {
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, targetRecord.ID, item.candidate.SourceID, item.sourceCtx, sourceLinkAuthoritySupporting, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return eventRecord{}, false, err
		}
		if writeResult.Ambiguous {
			return eventRecord{}, false, fmt.Errorf("canonical exact supporting source identity is ambiguous: %s", writeResult.Reason)
		}
		incoming := item.incoming
		incoming.SourceName = item.sourceCtx.SourceName
		incoming.SourceURL = item.sourceCtx.SourceURL
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, item.candidate.SourceID, item.sourceCtx, seedstore.SourceAuthoritySupporting, targetRecord, incoming); err != nil {
			return eventRecord{}, false, err
		}
	}

	updatedRecord, ok, err := loadEventRecordByIDTx(ctx, tx, targetRecord.ID)
	if err != nil || !ok {
		return eventRecord{}, ok, err
	}
	if err := upsertEventSecondarySourceInfoTx(ctx, tx, updatedRecord.ID, primarySourceIdentity(updatedRecord.Event), reviewCandidatesMatchingEvent(matchingCandidates, updatedRecord.Event), now); err != nil {
		return eventRecord{}, false, err
	}
	if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, updatedRecord.ID, updatedRecord.Event.Description, now); err != nil {
		return eventRecord{}, false, err
	}
	updatedRecord, ok, err = loadEventRecordByIDTx(ctx, tx, targetRecord.ID)
	if err != nil || !ok {
		return eventRecord{}, ok, err
	}
	return updatedRecord, true, nil
}

func eventReviewEvidenceEventIDCompatibleWithCanonicalTx(ctx context.Context, q queryer, evidenceID, canonicalEventID int64) (bool, error) {
	var existingEventID sql.NullInt64
	switch err := q.QueryRowContext(ctx, `
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, evidenceID).Scan(&existingEventID); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return !existingEventID.Valid || existingEventID.Int64 == canonicalEventID, nil
	}
}

func canonicalExactAutoResolutionEvidenceEventIDsCompatibleTx(ctx context.Context, q queryer, targetRecord eventRecord, candidates []eventReviewClusterAutoResolutionCandidate, now time.Time) (bool, error) {
	for _, candidate := range candidates {
		incoming, ok, err := eventReviewClusterAutoResolutionEvent(candidate, now)
		if err != nil {
			return false, err
		}
		if !ok || !eventReviewAutoResolutionEventsExactMatch(incoming, targetRecord.Event) {
			continue
		}
		compatible, err := eventReviewEvidenceEventIDCompatibleWithCanonicalTx(ctx, q, candidate.EvidenceID, targetRecord.ID)
		if err != nil || !compatible {
			return compatible, err
		}
	}
	return true, nil
}

func autoResolveEventReviewClusterUnanimousDuplicateTx(ctx context.Context, tx interface {
	execer
	queryer
}, cluster seedstore.EventReviewCluster, candidates []eventReviewClusterAutoResolutionCandidate, scope seedstore.ObservationRunScope, sourceMetadata ingest.SourceMetadataLookup, now time.Time) (*eventReviewClusterAutoResolutionResult, error) {
	selected := eventReviewClusterSelectedAutoResolutionCandidate(candidates)
	event, ok, err := eventReviewClusterAutoResolutionEvent(selected, now)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	mode := reviewSourceIdentitySupporting
	authority := seedstore.SourceAuthoritySupporting
	if selected.SourceAuthority == seedstore.SourceAuthorityAuthoritative {
		mode = reviewSourceIdentityAuthoritative
		authority = seedstore.SourceAuthorityAuthoritative
	}
	sourceCtx := reviewSourceIdentityContextForCandidateInput(mode, selected.SourceName, selected.SourceURL, "", "", "", selected.CandidateInput, "event_review_unanimous_duplicate")

	var record eventRecord
	var applied bool
	if selected.SourceAuthority == seedstore.SourceAuthorityAuthoritative {
		record, applied, err = applyAuthoritativeEventTx(ctx, tx, event, sourceCtx, now, scope, sourceMetadata)
		if err != nil {
			return nil, err
		}
		if !applied {
			return nil, nil
		}
		primary := reviewGroupAuthoritativeLink{
			SourceName: sourceCtx.SourceName,
			SourceURL:  sourceCtx.SourceURL,
		}
		if err := replaceEventSecondarySourceInfoTx(ctx, tx, record.ID, primary, reviewCandidatesMatchingEvent(eventReviewClusterCandidates(candidates), record.Event), now); err != nil {
			return nil, err
		}
		if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, record.ID, record.Event.Description, now); err != nil {
			return nil, err
		}
	} else {
		record, err = upsertEventTx(ctx, tx, event, now)
		if err != nil {
			return nil, err
		}
		if scope != "" {
			if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, selected.SourceID, sourceCtx, authority, record, event); err != nil {
				return nil, err
			}
		}
		if err := upsertEventSecondarySourceInfoTx(ctx, tx, record.ID, primarySourceIdentity(record.Event), reviewCandidatesMatchingEvent(eventReviewClusterCandidates(candidates), record.Event), now); err != nil {
			return nil, err
		}
		if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, record.ID, record.Event.Description, now); err != nil {
			return nil, err
		}
	}

	appliedSnapshot := eventReviewResolutionAppliedAutoResolutionSnapshot{
		EventID:       record.ID,
		EventSlug:     record.Event.Slug,
		Result:        "unanimous_duplicate",
		SourceID:      selected.SourceID,
		SourceName:    sourceCtx.SourceName,
		SourceURL:     sourceCtx.SourceURL,
		EvidenceCount: len(candidates),
	}
	snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusResolved, "", nil, nil, &appliedSnapshot, nil, nil, nil, nil, nil, nil, now)
	if err != nil {
		return nil, err
	}
	if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusResolved, snapshot, "", now); err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET status = ?, canonical_event_id = ?, version = version + 1, updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, string(seedstore.EventReviewClusterStatusResolved), record.ID, formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rowsAffected != 1 {
		return nil, fmt.Errorf("event review cluster %d update was rejected", cluster.ID)
	}
	return &eventReviewClusterAutoResolutionResult{
		Result:             "unanimous_duplicate",
		CanonicalEventID:   record.ID,
		CanonicalEventSlug: record.Event.Slug,
		ClusterStatus:      seedstore.EventReviewClusterStatusResolved,
		Version:            cluster.Version + 1,
		Applied:            appliedSnapshot,
	}, nil
}

func reviewFieldsForAutoResolutionCandidate(candidate review.Candidate) map[review.Field]review.Candidate {
	selected := make(map[review.Field]review.Candidate, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		selected[field] = candidate
	}
	return selected
}
