package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

func (s *Store) CreateReviewGroup(ctx context.Context, input review.GroupInput) (int64, error) {
	return s.createReviewGroup(ctx, input, "")
}

func (s *Store) StageReviewGroup(ctx context.Context, input review.GroupInput) (review.StageGroupResult, error) {
	if s == nil || s.db == nil {
		return review.StageGroupResult{}, errors.New("sqlite store is not open")
	}
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		groupID, err := s.createReviewGroup(ctx, input, "")
		if err != nil {
			return review.StageGroupResult{}, err
		}
		return review.StageGroupResult{ID: groupID, Created: true}, nil
	}
	input.Title = strings.TrimSpace(input.Title)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.Title == "" {
		input.Title = "Review group"
	}
	if input.SourceName == "" {
		return review.StageGroupResult{}, errors.New("review source name is required")
	}
	if input.SourceURL == "" {
		return review.StageGroupResult{}, errors.New("review source URL is required")
	}
	if len(input.Candidates) == 0 {
		return review.StageGroupResult{}, errors.New("at least one review candidate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return review.StageGroupResult{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO review_groups (
			title,
			source_name,
			source_url,
			authoritative_source_name,
			authoritative_source_url,
			authoritative_source_event_key,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL,
		nullableReviewText(input.AuthoritativeSourceName),
		nullableReviewText(input.AuthoritativeSourceURL),
		nullableReviewText(input.AuthoritativeSourceEventKey),
		stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return review.StageGroupResult{}, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return review.StageGroupResult{}, err
	}

	if rowsAffected == 1 {
		groupID, err := res.LastInsertId()
		if err != nil {
			return review.StageGroupResult{}, err
		}
		if err := replaceReviewCandidatesTx(ctx, tx, groupID, input.Candidates, input.SourceName, input.SourceURL); err != nil {
			return review.StageGroupResult{}, err
		}
		if err := ensureProvisionalVenuesForCandidateInputsTx(ctx, tx, input.Candidates); err != nil {
			return review.StageGroupResult{}, err
		}
		if err := ensureProvisionalRoomsForCandidateInputsTx(ctx, tx, input.Candidates); err != nil {
			return review.StageGroupResult{}, err
		}
		if _, err := refreshCanonicalSnapshotAndDefaultsTx(ctx, tx, groupID, input, now); err != nil {
			return review.StageGroupResult{}, err
		}
		autoResolved, outcome, canonicalSlug, err := maybeAutoResolveDuplicateReviewGroupTx(ctx, tx, groupID, now, s.sourceMetadata)
		if err != nil {
			return review.StageGroupResult{}, err
		}
		if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
			return review.StageGroupResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return review.StageGroupResult{}, err
		}
		return review.StageGroupResult{
			ID:                 groupID,
			Created:            true,
			AutoResolved:       autoResolved,
			AutoResolvedResult: outcome,
			CanonicalEventSlug: canonicalSlug,
		}, nil
	}

	group, ok, err := loadReviewGroupByStagingKey(ctx, tx, stagingKey)
	if err != nil {
		return review.StageGroupResult{}, err
	}
	if !ok {
		return review.StageGroupResult{}, errors.New("staged review group not found after ignore")
	}
	if group.Status == review.StatusOpen {
		if err := refreshReviewGroupAuthoritativeLinkTx(ctx, tx, group.ID, reviewGroupAuthoritativeLinkInput{
			SourceName:     input.AuthoritativeSourceName,
			SourceURL:      input.AuthoritativeSourceURL,
			SourceEventKey: input.AuthoritativeSourceEventKey,
		}, now); err != nil {
			return review.StageGroupResult{}, err
		}
		if _, err := refreshCanonicalSnapshotAndDefaultsTx(ctx, tx, group.ID, input, now); err != nil {
			return review.StageGroupResult{}, err
		}
		backfillCandidates, err := refreshStagedReviewCandidateVenueEvidenceTx(ctx, tx, group.ID, input.Candidates)
		if err != nil {
			return review.StageGroupResult{}, err
		}
		if err := ensureProvisionalVenuesForReviewCandidatesTx(ctx, tx, backfillCandidates); err != nil {
			return review.StageGroupResult{}, err
		}
		if err := ensureProvisionalRoomsForCandidateInputsTx(ctx, tx, input.Candidates); err != nil {
			return review.StageGroupResult{}, err
		}
	}
	if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, group.ID, now); err != nil {
		return review.StageGroupResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return review.StageGroupResult{}, err
	}
	return review.StageGroupResult{ID: group.ID, Created: false}, nil
}

func (s *Store) PromoteSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, errors.New("sqlite store is not open")
	}
	now := time.Now().UTC()

	eventSlug, applied, err := s.promoteAuthoritativeSingletonReviewGroupIfMissing(ctx, input, now)
	if err != nil || applied {
		return eventSlug, applied, err
	}
	return s.promoteNonAuthoritativeSingletonReviewGroupIfMissing(ctx, input, now)
}

func (s *Store) decorateEventForPublish(event domain.Event) domain.Event {
	return decorateEventForPublish(event, s.sourceMetadata)
}

func decorateEventForPublish(event domain.Event, sourceMetadata ingest.SourceMetadataLookup) domain.Event {
	links := decorateEventLinkValues(event.Name, event.SourceName, event.SourceURL, event.OfficialListingURL, event.CalendarURL, sourceMetadata)
	event.OfficialListingURL = links.officialListingURL
	event.CalendarURL = links.calendarURL
	return event
}

func (s *Store) promoteAuthoritativeSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput, now time.Time) (string, bool, error) {
	sourceEventKey := authoritativeSingletonSourceEventKey(s.sourceMetadata, input)
	if sourceEventKey == "" {
		return "", false, nil
	}

	event, err := singletonResolvedEventFromGroupInput(input, now)
	if err != nil {
		return "", false, nil
	}
	if authoritative, ok := reviewGroupInputAuthoritativeSource(input); ok {
		event.SourceName = authoritative.SourceName
		event.SourceURL = authoritative.SourceURL
	}
	event = s.decorateEventForPublish(event)
	event.PublicationState = domain.PublicationStateReviewed

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	appliedEvent, applied, err := applyAuthoritativeEventTx(ctx, tx, event, sourceEventKey, now)
	if err != nil {
		return "", false, err
	}
	if applied {
		if err := resolveMatchingOpenReviewGroupsTx(ctx, tx, input, now); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	if !applied {
		return "", false, nil
	}
	return appliedEvent.Event.Slug, true, nil
}

func (s *Store) promoteNonAuthoritativeSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput, now time.Time) (string, bool, error) {
	event, err := singletonResolvedEventFromGroupInput(input, now)
	if err != nil {
		return "", false, nil
	}
	event = s.decorateEventForPublish(event)
	event.PublicationState = domain.PublicationStateProvisional

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	matcher, err := loadVenueMatcher(ctx, tx)
	if err != nil {
		return "", false, err
	}
	venueMatch, err := ensureProvisionalVenueForCandidateTx(ctx, tx, &matcher, reviewCandidateFromInput(input.Candidates[0]))
	if err != nil {
		return "", false, err
	}
	switch venueMatch.status {
	case venueMatchResolved:
		event.VenueSlug = venueMatch.slug
		event.Slug, err = buildLiveEventSlug(event.Name, event.VenueSlug, event.Start)
		if err != nil {
			return "", false, err
		}
	case venueMatchAmbiguous, venueMatchNoMatch:
		return "", false, nil
	}

	record, found, ambiguous, err := uniqueLiveEventMatchForEventTx(ctx, tx, event)
	if err != nil {
		return "", false, err
	}
	if ambiguous {
		return "", false, nil
	}
	if found {
		if supportingEventConflict(record.Event, event) {
			return "", false, nil
		}
		if err := updateSupportingMatchedEventTx(ctx, tx, record, event); err != nil {
			return "", false, err
		}
		if err := resolveMatchingOpenNonAuthoritativeSingletonReviewGroupsTx(ctx, tx, input, now); err != nil {
			return "", false, err
		}
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		return record.Event.Slug, true, nil
	}

	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return "", false, err
	}
	eventID, err := insertEventTx(ctx, tx, event, venueID, sourceID)
	if err != nil {
		return "", false, err
	}
	if err := refreshEventGenresTx(ctx, tx, eventID, event.Description, nil, now); err != nil {
		return "", false, err
	}
	if err := resolveMatchingOpenNonAuthoritativeSingletonReviewGroupsTx(ctx, tx, input, now); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return event.Slug, true, nil
}

func ensureProvisionalVenuesForCandidateInputsTx(ctx context.Context, tx interface {
	execer
	queryer
}, inputs []review.CandidateInput) error {
	candidates := make([]review.Candidate, 0, len(inputs))
	for _, input := range inputs {
		if input.CanonicalEventID != 0 {
			continue
		}
		candidates = append(candidates, reviewCandidateFromInput(input))
	}
	return ensureProvisionalVenuesForReviewCandidatesTx(ctx, tx, candidates)
}

func ensureProvisionalVenuesForReviewCandidatesTx(ctx context.Context, tx interface {
	execer
	queryer
}, candidates []review.Candidate) error {
	matcher, err := loadVenueMatcher(ctx, tx)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.CanonicalEventID != 0 {
			continue
		}
		if _, err := ensureProvisionalVenueForCandidateTx(ctx, tx, &matcher, candidate); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) createReviewGroup(ctx context.Context, input review.GroupInput, stagingKey string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("sqlite store is not open")
	}
	input.Title = strings.TrimSpace(input.Title)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.Title == "" {
		input.Title = "Review group"
	}
	if input.SourceName == "" {
		return 0, errors.New("review source name is required")
	}
	if input.SourceURL == "" {
		return 0, errors.New("review source URL is required")
	}
	if len(input.Candidates) == 0 {
		return 0, errors.New("at least one review candidate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			authoritative_source_name,
			authoritative_source_url,
			authoritative_source_event_key,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL,
		nullableReviewText(input.AuthoritativeSourceName),
		nullableReviewText(input.AuthoritativeSourceURL),
		nullableReviewText(input.AuthoritativeSourceEventKey),
		stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, err
	}
	groupID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	if err := insertReviewCandidatesTx(ctx, tx, groupID, input.Candidates, input.SourceName, input.SourceURL); err != nil {
		return 0, err
	}
	if err := ensureProvisionalVenuesForCandidateInputsTx(ctx, tx, input.Candidates); err != nil {
		return 0, err
	}
	if err := ensureProvisionalRoomsForCandidateInputsTx(ctx, tx, input.Candidates); err != nil {
		return 0, err
	}
	if err := recomputeReviewFieldDefaultsTx(ctx, tx, groupID, now); err != nil {
		return 0, err
	}
	if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return groupID, nil
}

func replaceReviewCandidatesTx(ctx context.Context, tx execer, groupID int64, candidates []review.CandidateInput, defaultSourceName, defaultSourceURL string) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM review_candidates
		WHERE group_id = ? AND canonical_event_id IS NULL
	`, groupID); err != nil {
		return err
	}
	for i, candidate := range candidates {
		candidate.CanonicalEventID = 0
		if err := insertReviewCandidate(ctx, tx, groupID, i+1, candidate, defaultSourceName, defaultSourceURL); err != nil {
			return err
		}
	}
	return nil
}

func refreshStagedReviewCandidateVenueEvidenceTx(ctx context.Context, tx interface {
	execer
	queryer
}, groupID int64, incoming []review.CandidateInput) ([]review.Candidate, error) {
	existing, err := loadReviewCandidates(ctx, tx, groupID)
	if err != nil {
		return nil, err
	}
	var backfillCandidates []review.Candidate

	incomingByFingerprint := make(map[string][]review.CandidateInput)
	for _, candidate := range incoming {
		if candidate.CanonicalEventID != 0 {
			continue
		}
		fingerprint := stagedReviewCandidateFingerprint(candidate.ExternalID, candidate.Name, candidate.StartAt, candidate.EndAt, candidate.Genre, candidate.Status, candidate.Description)
		incomingByFingerprint[fingerprint] = append(incomingByFingerprint[fingerprint], candidate)
	}

	existingByFingerprint := make(map[string][]review.Candidate)
	for _, candidate := range existing {
		if candidate.CanonicalEventID != 0 {
			continue
		}
		fingerprint := stagedReviewCandidateFingerprint(candidate.ExternalID, candidate.Name, candidate.StartAt, candidate.EndAt, candidate.Genre, candidate.Status, candidate.Description)
		existingByFingerprint[fingerprint] = append(existingByFingerprint[fingerprint], candidate)
	}

	for fingerprint, incomingBucket := range incomingByFingerprint {
		existingBucket := existingByFingerprint[fingerprint]
		if len(existingBucket) != len(incomingBucket) || len(existingBucket) != 1 {
			continue
		}
		incomingCandidate := incomingBucket[0]
		existingCandidate := existingBucket[0]
		incomingVenueText := strings.TrimSpace(incomingCandidate.VenueText)
		incomingVenueLocationRaw := strings.TrimSpace(incomingCandidate.VenueLocationRaw)
		incomingRoomText := strings.TrimSpace(incomingCandidate.RoomText)
		if strings.TrimSpace(incomingCandidate.ImageURL) != "" {
			if _, err := tx.ExecContext(ctx, `
					UPDATE review_candidates
					SET venue_text = ?,
						venue_location_raw = ?,
						room_text = ?,
						image_url = ?,
						image_source_url = ?,
						image_alt = ?,
						image_width = ?,
						image_height = ?,
						image_focus_x = ?,
						image_focus_y = ?
					WHERE id = ? AND group_id = ? AND canonical_event_id IS NULL
				`, incomingVenueText, incomingVenueLocationRaw, incomingRoomText,
				strings.TrimSpace(incomingCandidate.ImageURL),
				strings.TrimSpace(incomingCandidate.ImageSourceURL),
				strings.TrimSpace(incomingCandidate.ImageAlt),
				incomingCandidate.ImageWidth,
				incomingCandidate.ImageHeight,
				normalizedImageFocusValue(incomingCandidate.ImageFocusX),
				normalizedImageFocusValue(incomingCandidate.ImageFocusY),
				existingCandidate.ID, groupID); err != nil {
				return nil, err
			}
		} else {
			if _, err := tx.ExecContext(ctx, `
					UPDATE review_candidates
					SET venue_text = ?,
						venue_location_raw = ?,
						room_text = ?
					WHERE id = ? AND group_id = ? AND canonical_event_id IS NULL
				`, incomingVenueText, incomingVenueLocationRaw, incomingRoomText, existingCandidate.ID, groupID); err != nil {
				return nil, err
			}
		}
		if err := replaceReviewCandidateRoomsTx(ctx, tx, existingCandidate.ID, incomingCandidate.Rooms); err != nil {
			return nil, err
		}
		if reviewCandidateNeedsProvisionalVenueBackfill(existingCandidate, incomingVenueText, incomingVenueLocationRaw) {
			existingCandidate.VenueSlug = strings.TrimSpace(incomingCandidate.VenueSlug)
			existingCandidate.VenueText = incomingVenueText
			existingCandidate.VenueLocationRaw = incomingVenueLocationRaw
			backfillCandidates = append(backfillCandidates, existingCandidate)
		}
	}

	return backfillCandidates, nil
}

func reviewCandidateNeedsProvisionalVenueBackfill(existing review.Candidate, incomingVenueText, incomingVenueLocationRaw string) bool {
	if existing.CanonicalEventID != 0 {
		return false
	}
	if strings.TrimSpace(existing.VenueText) != "" || strings.TrimSpace(existing.VenueLocationRaw) != "" {
		return false
	}
	return incomingVenueLocationRaw != ""
}

func insertReviewCandidatesTx(ctx context.Context, tx execer, groupID int64, candidates []review.CandidateInput, defaultSourceName, defaultSourceURL string) error {
	for i, candidate := range candidates {
		if err := insertReviewCandidate(ctx, tx, groupID, i+1, candidate, defaultSourceName, defaultSourceURL); err != nil {
			return err
		}
	}
	return nil
}

func refreshCanonicalSnapshotAndDefaultsTx(ctx context.Context, tx interface {
	execer
	queryer
}, groupID int64, input review.GroupInput, now time.Time) (*eventRecord, error) {
	canonical, err := attachCanonicalSnapshotTx(ctx, tx, groupID, input)
	if err != nil {
		return nil, err
	}
	if err := recomputeReviewFieldDefaultsTx(ctx, tx, groupID, now); err != nil {
		return nil, err
	}
	return canonical, nil
}

func attachCanonicalSnapshotTx(ctx context.Context, tx interface {
	execer
	queryer
}, groupID int64, input review.GroupInput) (*eventRecord, error) {
	record, err := canonicalMatchForGroupInputTx(ctx, tx, input)
	if err != nil {
		return nil, err
	}
	if record == nil {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM review_candidates
			WHERE group_id = ? AND canonical_event_id IS NOT NULL
		`, groupID); err != nil {
			return nil, err
		}
		return nil, nil
	}

	position := len(input.Candidates) + 1
	candidate := review.CandidateInput{
		CanonicalEventID: record.ID,
		ExternalID:       "",
		Name:             record.Event.Name,
		VenueSlug:        record.Event.VenueSlug,
		VenueText:        "",
		VenueLocationRaw: "",
		RoomText:         record.Event.RoomText,
		Rooms:            append([]domain.VenueRoom(nil), record.Event.Rooms...),
		StartAt:          formatRFC3339UTC(record.Event.Start),
		EndAt:            formatOptionalTime(record.Event.End),
		Genre:            record.Event.Genre,
		Status:           record.Event.Status,
		Description:      record.Event.Description,
		ImageURL:         record.Event.ImageURL,
		ImageSourceURL:   record.Event.ImageSourceURL,
		ImageAlt:         record.Event.ImageAlt,
		ImageWidth:       record.Event.ImageWidth,
		ImageHeight:      record.Event.ImageHeight,
		ImageFocusX:      record.Event.ImageFocusX,
		ImageFocusY:      record.Event.ImageFocusY,
		SourceName:       record.Event.SourceName,
		SourceURL:        firstNonEmptyReviewText(record.Event.OfficialListingURL, record.Event.SourceURL),
		CalendarURL:      record.Event.CalendarURL,
		Provenance:       "Canonical live event snapshot",
	}

	existing, ok, err := loadCanonicalSnapshotCandidate(ctx, tx, groupID)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := tx.ExecContext(ctx, `
			UPDATE review_candidates
			SET position = ?,
				canonical_event_id = ?,
				external_id = ?,
				name = ?,
				venue_slug = ?,
				venue_text = ?,
				venue_location_raw = ?,
				room_text = ?,
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
				source_name = ?,
				source_url = ?,
				calendar_url = ?,
				provenance = ?
			WHERE id = ? AND group_id = ?
		`, position, record.ID, "", candidate.Name, candidate.VenueSlug, candidate.VenueText, candidate.VenueLocationRaw, candidate.RoomText, candidate.StartAt, candidate.EndAt, candidate.Genre, candidate.Status, candidate.Description, candidate.ImageURL, candidate.ImageSourceURL, candidate.ImageAlt, candidate.ImageWidth, candidate.ImageHeight, normalizedImageFocusValue(candidate.ImageFocusX), normalizedImageFocusValue(candidate.ImageFocusY), candidate.SourceName, candidate.SourceURL, candidate.CalendarURL, candidate.Provenance, existing.ID, groupID); err != nil {
			return nil, err
		}
		if err := replaceReviewCandidateRoomsTx(ctx, tx, existing.ID, candidate.Rooms); err != nil {
			return nil, err
		}
		return record, nil
	}
	if err := insertReviewCandidate(ctx, tx, groupID, position, candidate, input.SourceName, input.SourceURL); err != nil {
		return nil, err
	}
	return record, nil
}

func stagedReviewCandidateFingerprint(values ...string) string {
	sum := sha256.New()
	writeStagedReviewCandidateFingerprintPart(sum, "review-stage-candidate:v1")
	for _, value := range values {
		writeStagedReviewCandidateFingerprintPart(sum, value)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

func writeStagedReviewCandidateFingerprintPart(sum interface{ Write([]byte) (int, error) }, value string) {
	_, _ = fmt.Fprintf(sum, "%d:%s\x00", len(value), value)
}

func canonicalMatchForGroupInputTx(ctx context.Context, q queryer, input review.GroupInput) (*eventRecord, error) {
	matched := make(map[int64]eventRecord)
	if authoritative, ok := reviewGroupInputAuthoritativeSource(input); ok {
		sourceID, found, err := loadSourceIDByNameURLTx(ctx, q, authoritative.SourceName, authoritative.SourceURL)
		if err != nil {
			return nil, err
		}
		if found {
			record, ok, err := loadEventRecordBySourceLinkTx(ctx, q, sourceID, authoritative.SourceEventKey)
			if err != nil {
				return nil, err
			}
			if ok {
				matched[record.ID] = record
			}
		}
	}

	derivedAny := false
	for _, candidate := range input.Candidates {
		records, ok, err := candidateLiveEventIdentityMatchesTx(ctx, q, candidate)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		derivedAny = true
		for _, record := range records {
			matched[record.ID] = record
		}
	}
	if !derivedAny || len(matched) != 1 {
		return nil, nil
	}
	for _, record := range matched {
		copy := record
		return &copy, nil
	}
	return nil, nil
}

func derivedCandidateLiveSlug(candidate review.CandidateInput) (string, bool) {
	name := strings.TrimSpace(candidate.Name)
	venueSlug := strings.TrimSpace(candidate.VenueSlug)
	startText := strings.TrimSpace(candidate.StartAt)
	if name == "" || venueSlug == "" || startText == "" {
		return "", false
	}
	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return "", false
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		return "", false
	}
	return slug, true
}

func candidateLiveEventIdentityMatchesTx(ctx context.Context, q queryer, candidate review.CandidateInput) ([]eventRecord, bool, error) {
	slug, ok := derivedCandidateLiveSlug(candidate)
	if !ok {
		return nil, false, nil
	}

	start, err := parseRFC3339UTC(strings.TrimSpace(candidate.StartAt))
	if err != nil {
		return nil, false, nil
	}
	records, err := matchLiveEventsByIdentityTx(ctx, q, slug, candidate.Name, candidate.VenueSlug, start)
	if err != nil {
		return nil, false, err
	}
	return records, true, nil
}

func reviewGroupInputAuthoritativeSource(input review.GroupInput) (reviewGroupAuthoritativeLink, bool) {
	if strings.TrimSpace(input.AuthoritativeSourceName) == "" || strings.TrimSpace(input.AuthoritativeSourceURL) == "" || strings.TrimSpace(input.AuthoritativeSourceEventKey) == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     strings.TrimSpace(input.AuthoritativeSourceName),
		SourceURL:      strings.TrimSpace(input.AuthoritativeSourceURL),
		SourceEventKey: strings.TrimSpace(input.AuthoritativeSourceEventKey),
	}, true
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return formatRFC3339UTC(value)
}

func recomputeReviewFieldDefaultsTx(ctx context.Context, tx interface {
	execer
	queryer
}, groupID int64, now time.Time) error {
	candidates, err := loadReviewCandidates(ctx, tx, groupID)
	if err != nil {
		return err
	}
	defaults := computeReviewFieldDefaults(candidates, now)
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM review_field_defaults
		WHERE group_id = ?
	`, groupID); err != nil {
		return err
	}
	for _, choice := range defaults {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_field_defaults (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
		`, groupID, string(choice.Field), choice.CandidateID, choice.Value, formatRFC3339UTC(choice.UpdatedAt)); err != nil {
			return err
		}
	}
	return nil
}

func computeReviewFieldDefaults(candidates []review.Candidate, now time.Time) map[review.Field]review.DraftChoice {
	defaults := make(map[review.Field]review.DraftChoice)
	for _, field := range reviewConsensusFields() {
		type tally struct {
			count       int
			candidateID int64
		}
		counts := make(map[string]tally)
		for _, candidate := range candidates {
			value := review.CandidateValue(candidate, field)
			entry := counts[value]
			entry.count++
			if entry.candidateID == 0 {
				entry.candidateID = candidate.ID
			}
			counts[value] = entry
		}
		threshold := len(candidates) / 2
		for value, entry := range counts {
			if entry.count > threshold {
				defaults[field] = review.DraftChoice{
					Field:       field,
					CandidateID: entry.candidateID,
					Value:       value,
					UpdatedAt:   now,
				}
				break
			}
		}
	}
	return defaults
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

func maybeAutoResolveDuplicateReviewGroupTx(ctx context.Context, tx interface {
	execer
	queryer
}, groupID int64, now time.Time, sourceMetadata ingest.SourceMetadataLookup) (bool, string, string, error) {
	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return false, "", "", err
	}
	if !ok {
		return false, "", "", fmt.Errorf("review group %d not found", groupID)
	}
	candidates, err := loadReviewCandidates(ctx, tx, groupID)
	if err != nil {
		return false, "", "", err
	}
	group.Candidates = candidates
	defaults, err := loadReviewDefaultChoices(ctx, tx, groupID)
	if err != nil {
		return false, "", "", err
	}
	group.DefaultChoices = defaults

	canonical, hasCanonical := canonicalSnapshotCandidate(candidates)
	if hasCanonical && exactCanonicalDuplicate(candidates) {
		if authoritative, ok := reviewGroupAuthoritativeSource(group); ok {
			targetID, ok, err := authoritativeLinkedEventIDTx(ctx, tx, authoritative)
			if err != nil {
				return false, "", "", err
			}
			if ok && targetID != canonical.CanonicalEventID {
				return false, "", "", nil
			}
		}
		if err := persistResolvedChoiceSetTx(ctx, tx, groupID, chooseAllFieldsFromCandidate(canonical, now), now); err != nil {
			return false, "", "", err
		}
		if err := markEventReviewedTx(ctx, tx, canonical.CanonicalEventID); err != nil {
			return false, "", "", err
		}
		if err := markReviewGroupResolvedTx(ctx, tx, groupID, now); err != nil {
			return false, "", "", err
		}
		return true, "canonical_exact_match", candidateDerivedSlug(canonical), nil
	}

	staged := stagedReviewCandidates(candidates)
	if !hasCanonical && len(staged) >= 2 && unanimousStagedDuplicate(staged) {
		winner := staged[0]
		for _, candidate := range staged[1:] {
			if candidate.Position < winner.Position || (candidate.Position == winner.Position && candidate.ID < winner.ID) {
				winner = candidate
			}
		}
		matcher, err := loadVenueMatcher(ctx, tx)
		if err != nil {
			return false, "", "", err
		}
		venueMatch, err := ensureProvisionalVenueForCandidateTx(ctx, tx, &matcher, winner)
		if err != nil {
			return false, "", "", err
		}
		if venueMatch.status != venueMatchResolved {
			return false, "", "", nil
		}
		winner.VenueSlug = venueMatch.slug
		if err := persistResolvedChoiceSetTx(ctx, tx, groupID, chooseAllFieldsFromCandidate(winner, now), now); err != nil {
			return false, "", "", err
		}
		event, err := buildResolvedEvent(group, choiceMapFromChoices(winner), now)
		if err != nil {
			return false, "", "", err
		}
		event = decorateEventForPublish(event, sourceMetadata)
		if authoritative, ok := reviewGroupAuthoritativeSource(group); ok {
			event.SourceName = authoritative.SourceName
			event.SourceURL = authoritative.SourceURL
			event = decorateEventForPublish(event, sourceMetadata)
			record, applied, err := applyAuthoritativeEventTx(ctx, tx, event, authoritative.SourceEventKey, now)
			if err != nil {
				return false, "", "", err
			}
			if !applied {
				return false, "", "", fmt.Errorf("venue %q not found", event.VenueSlug)
			}
			matchingStaged := reviewCandidatesMatchingEvent(staged, record.Event)
			if err := replaceEventSecondarySourceInfoTx(ctx, tx, record.ID, authoritative, matchingStaged, now); err != nil {
				return false, "", "", err
			}
			if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, record.ID, record.Event.Description, now); err != nil {
				return false, "", "", err
			}
		} else {
			record, err := upsertEventTx(ctx, tx, event)
			if err != nil {
				return false, "", "", err
			}
			matchingStaged := reviewCandidatesMatchingEvent(staged, record.Event)
			if err := upsertEventSecondarySourceInfoTx(ctx, tx, record.ID, primarySourceIdentity(record.Event), matchingStaged, now); err != nil {
				return false, "", "", err
			}
			if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, record.ID, record.Event.Description, now); err != nil {
				return false, "", "", err
			}
		}
		if err := markReviewGroupResolvedTx(ctx, tx, groupID, now); err != nil {
			return false, "", "", err
		}
		return true, "unanimous_duplicate", "", nil
	}

	return false, "", "", nil
}

func candidateDerivedSlug(candidate review.Candidate) string {
	start, err := parseRFC3339UTC(strings.TrimSpace(candidate.StartAt))
	if err != nil {
		return ""
	}
	slug, err := buildLiveEventSlug(candidate.Name, candidate.VenueSlug, start)
	if err != nil {
		return ""
	}
	return slug
}

func choiceMapFromChoices(candidate review.Candidate) map[review.Field]review.Candidate {
	selected := make(map[review.Field]review.Candidate, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		selected[field] = candidate
	}
	return selected
}

func chooseAllFieldsFromCandidate(candidate review.Candidate, now time.Time) []review.DraftChoice {
	choices := make([]review.DraftChoice, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		choices = append(choices, review.DraftChoice{
			Field:       field,
			CandidateID: candidate.ID,
			Value:       review.CandidateValue(candidate, field),
			UpdatedAt:   now,
		})
	}
	return choices
}

func persistResolvedChoiceSetTx(ctx context.Context, tx execer, groupID int64, choices []review.DraftChoice, now time.Time) error {
	for _, choice := range choices {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_draft_choices (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, field) DO UPDATE SET
				candidate_id = excluded.candidate_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, groupID, string(choice.Field), choice.CandidateID, choice.Value, formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	return nil
}

func markReviewGroupResolvedTx(ctx context.Context, tx execer, groupID int64, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE id = ?
	`, review.StatusResolved, formatRFC3339UTC(now), groupID)
	return err
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

func canonicalSnapshotCandidate(candidates []review.Candidate) (review.Candidate, bool) {
	for _, candidate := range candidates {
		if candidate.IsCanonicalSnapshot() {
			return candidate, true
		}
	}
	return review.Candidate{}, false
}

func stagedReviewCandidates(candidates []review.Candidate) []review.Candidate {
	staged := make([]review.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.IsCanonicalSnapshot() {
			staged = append(staged, candidate)
		}
	}
	return staged
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
	if roomSetsConflict(candidate.Rooms, event.Rooms) {
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

func exactCanonicalDuplicate(candidates []review.Candidate) bool {
	if len(candidates) == 0 {
		return false
	}
	for _, field := range reviewConsensusFields() {
		var value string
		first := true
		for _, candidate := range candidates {
			candidateValue := review.CandidateValue(candidate, field)
			if first {
				value = candidateValue
				first = false
				continue
			}
			if candidateValue != value {
				return false
			}
		}
	}
	return true
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

func linkReviewGroupInputToImportRunTx(ctx context.Context, tx interface {
	execer
	queryer
}, input review.GroupInput, groupID int64, linkedAt time.Time) error {
	importRunID := input.ImportRunID
	if importRunID <= 0 {
		importRunID, _ = review.ParseOriginImportRunID(input.Notes)
	}
	return linkReviewGroupToImportRunTx(ctx, tx, importRunID, groupID, linkedAt)
}

func singletonResolvedEventFromGroupInput(input review.GroupInput, publishedAt time.Time) (domain.Event, error) {
	if len(input.Candidates) != 1 {
		return domain.Event{}, errors.New("singleton review group promotion requires exactly one candidate")
	}

	candidate := input.Candidates[0]
	group := review.Group{
		Title:      strings.TrimSpace(input.Title),
		SourceName: strings.TrimSpace(input.SourceName),
		SourceURL:  strings.TrimSpace(input.SourceURL),
	}
	selectedCandidate := review.Candidate{
		ID:             1,
		ExternalID:     strings.TrimSpace(candidate.ExternalID),
		Name:           strings.TrimSpace(candidate.Name),
		VenueSlug:      strings.TrimSpace(candidate.VenueSlug),
		RoomText:       strings.TrimSpace(candidate.RoomText),
		Rooms:          append([]domain.VenueRoom(nil), candidate.Rooms...),
		StartAt:        strings.TrimSpace(candidate.StartAt),
		EndAt:          strings.TrimSpace(candidate.EndAt),
		Genre:          strings.TrimSpace(candidate.Genre),
		Status:         strings.TrimSpace(candidate.Status),
		Description:    strings.TrimSpace(candidate.Description),
		ImageURL:       strings.TrimSpace(candidate.ImageURL),
		ImageSourceURL: strings.TrimSpace(candidate.ImageSourceURL),
		ImageAlt:       strings.TrimSpace(candidate.ImageAlt),
		ImageWidth:     candidate.ImageWidth,
		ImageHeight:    candidate.ImageHeight,
		ImageFocusX:    candidate.ImageFocusX,
		ImageFocusY:    candidate.ImageFocusY,
		SourceName:     strings.TrimSpace(candidate.SourceName),
		SourceURL:      strings.TrimSpace(candidate.SourceURL),
		CalendarURL:    strings.TrimSpace(candidate.CalendarURL),
		Provenance:     strings.TrimSpace(candidate.Provenance),
	}
	selected := make(map[review.Field]review.Candidate, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		selected[field] = selectedCandidate
	}
	return buildResolvedEvent(group, selected, publishedAt)
}

func authoritativeSourceEventKey(input review.GroupInput) string {
	if len(input.Candidates) != 1 {
		return ""
	}
	candidate := input.Candidates[0]
	for _, value := range []string{candidate.ExternalID, candidate.SourceURL} {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func authoritativeSingletonSourceEventKey(sourceMetadata ingest.SourceMetadataLookup, input review.GroupInput) string {
	sourceEventKey := authoritativeSourceEventKey(input)
	if sourceEventKey == "" {
		return ""
	}

	sourceName := strings.TrimSpace(input.SourceName)
	ownedVenueSlug := strings.TrimSpace(sourceMetadata.OwnedVenueSlugForReviewStageSourceName(sourceName))
	if ownedVenueSlug == "" {
		return ""
	}
	if strings.TrimSpace(input.Candidates[0].VenueSlug) != ownedVenueSlug {
		return ""
	}
	return sourceEventKey
}

func nonAuthoritativeSingletonVenueSlug(sourceMetadata ingest.SourceMetadataLookup, input review.GroupInput) string {
	if len(input.Candidates) != 1 {
		return ""
	}
	expectedVenueSlug := strings.TrimSpace(sourceMetadata.NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(input.SourceName))
	if expectedVenueSlug == "" {
		return ""
	}
	if strings.TrimSpace(input.Candidates[0].VenueSlug) != expectedVenueSlug {
		return ""
	}
	return expectedVenueSlug
}

func uniqueLiveEventMatchForEventTx(ctx context.Context, q queryer, event domain.Event) (eventRecord, bool, bool, error) {
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

func supportingEventConflict(existing, incoming domain.Event) bool {
	if strings.TrimSpace(existing.Name) != strings.TrimSpace(incoming.Name) {
		return true
	}
	if strings.TrimSpace(existing.VenueSlug) != strings.TrimSpace(incoming.VenueSlug) {
		return true
	}
	if roomSetsConflict(existing.Rooms, incoming.Rooms) {
		return true
	}
	if !existing.Start.UTC().Equal(incoming.Start.UTC()) {
		return true
	}
	if existing.HasEnd() && incoming.HasEnd() && !existing.End.UTC().Equal(incoming.End.UTC()) {
		return true
	}
	if strings.TrimSpace(existing.Status) != "" && strings.TrimSpace(incoming.Status) != "" && strings.TrimSpace(existing.Status) != strings.TrimSpace(incoming.Status) {
		return true
	}
	if strings.TrimSpace(existing.Description) != "" && strings.TrimSpace(incoming.Description) != "" && strings.TrimSpace(existing.Description) != strings.TrimSpace(incoming.Description) {
		return true
	}
	return false
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
	return refreshEventGenresTx(ctx, tx, existing.ID, updated.Description, nil, incoming.LastChecked)
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

	groups := ingest.ReviewGroupsFromReportWithCatalog(catalog, report)
	for _, group := range groups {
		if strings.TrimSpace(group.AuthoritativeSourceName) == "" ||
			strings.TrimSpace(group.AuthoritativeSourceURL) == "" ||
			strings.TrimSpace(group.AuthoritativeSourceEventKey) == "" ||
			len(group.Candidates) != 1 {
			repair.Skipped++
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(group.Title))
			continue
		}

		event, err := singletonResolvedEventFromGroupInput(group, time.Now().UTC())
		if err != nil {
			repair.Skipped++
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(group.Title))
			continue
		}
		result, err := s.repairAuthoritativeEventDescription(ctx, event, group.AuthoritativeSourceEventKey)
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
			repair.SkippedTitles = append(repair.SkippedTitles, strings.TrimSpace(group.Title))
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

	if linked, ok, err := loadEventRecordBySourceLinkTx(ctx, tx, sourceID, sourceEventKey); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		return linked, true, nil
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

func resolveMatchingOpenReviewGroupsTx(ctx context.Context, tx execer, input review.GroupInput, now time.Time) error {
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		return nil
	}

	args := []any{review.StatusResolved, formatRFC3339UTC(now), review.StatusOpen, stagingKey}
	query := `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE status = ?
		  AND staging_key = ?
	`
	if sourceEventKey := strings.TrimSpace(input.AuthoritativeSourceEventKey); sourceEventKey != "" {
		query += ` AND authoritative_source_event_key = ?`
		args = append(args, sourceEventKey)
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func resolveMatchingOpenNonAuthoritativeSingletonReviewGroupsTx(ctx context.Context, tx interface {
	execer
	queryer
}, input review.GroupInput, now time.Time) error {
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		return nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT g.id
		FROM review_groups g
		JOIN review_candidates c ON c.group_id = g.id
		WHERE g.status = ?
		  AND g.staging_key = ?
		GROUP BY g.id
		HAVING COUNT(c.id) = 1
	`, review.StatusOpen, stagingKey)
	if err != nil {
		return err
	}
	defer rows.Close()

	var groupIDs []int64
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, groupID := range groupIDs {
		if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE review_groups
			SET status = ?, updated_at = ?
			WHERE id = ?
		`, review.StatusResolved, formatRFC3339UTC(now), groupID); err != nil {
			return err
		}
	}
	return nil
}

type eventRecord struct {
	ID    int64
	Event domain.Event
}

func applyAuthoritativeEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event, sourceEventKey string, now time.Time) (eventRecord, bool, error) {
	event.PublicationState = domain.PublicationStateReviewed
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return eventRecord{}, false, err
	}
	if !ok {
		return eventRecord{}, false, nil
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return eventRecord{}, false, err
	}

	if linked, ok, err := loadEventRecordBySourceLinkTx(ctx, tx, sourceID, sourceEventKey); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, linked, event, venueID, sourceID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureEventSourceLinkTx(ctx, tx, linked.ID, sourceID, sourceEventKey, now); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, linked.ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: linked.ID, Event: updated}, true, nil
	}

	if legacy, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, legacy, event, venueID, sourceID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureEventSourceLinkTx(ctx, tx, legacy.ID, sourceID, sourceEventKey, now); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, legacy.ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: legacy.ID, Event: updated}, true, nil
	}

	if matched, found, ambiguous, err := uniqueLiveEventMatchForEventTx(ctx, tx, event); err != nil {
		return eventRecord{}, false, err
	} else if ambiguous {
		return eventRecord{}, false, nil
	} else if found {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, matched, event, venueID, sourceID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureEventSourceLinkTx(ctx, tx, matched.ID, sourceID, sourceEventKey, now); err != nil {
			return eventRecord{}, false, err
		}
		if err := refreshEventGenresTx(ctx, tx, matched.ID, updated.Description, nil, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: matched.ID, Event: updated}, true, nil
	}

	eventID, err := insertEventTx(ctx, tx, event, venueID, sourceID)
	if err != nil {
		return eventRecord{}, false, err
	}
	if err := ensureEventSourceLinkTx(ctx, tx, eventID, sourceID, sourceEventKey, now); err != nil {
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
}, event domain.Event, venueID, sourceID int64) (int64, error) {
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
	return eventID, nil
}

func updateEventAuthoritativelyTx(ctx context.Context, tx interface {
	execer
	queryer
}, existing eventRecord, authoritative domain.Event, venueID, sourceID int64) (domain.Event, error) {
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

func ensureEventSourceLinkTx(ctx context.Context, tx execer, eventID, sourceID int64, sourceEventKey string, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event source link event ID is required")
	}
	if sourceID <= 0 {
		return errors.New("event source link source ID is required")
	}
	sourceEventKey = strings.TrimSpace(sourceEventKey)
	if sourceEventKey == "" {
		return errors.New("event source link key is required")
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(source_id, source_event_key) DO UPDATE SET
			event_id = excluded.event_id,
			is_authoritative = excluded.is_authoritative,
			updated_at = excluded.updated_at
	`, eventID, sourceID, sourceEventKey, formatRFC3339UTC(now), formatRFC3339UTC(now))
	return err
}

func authoritativeLinkedEventIDTx(ctx context.Context, q queryer, authoritative reviewGroupAuthoritativeLink) (int64, bool, error) {
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, q, authoritative.SourceName, authoritative.SourceURL)
	if err != nil || !ok {
		return 0, ok, err
	}

	var eventID int64
	err = q.QueryRowContext(ctx, `
		SELECT event_id
		FROM event_source_links
		WHERE source_id = ? AND source_event_key = ?
		LIMIT 1
	`, sourceID, authoritative.SourceEventKey).Scan(&eventID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	default:
		return eventID, true, nil
	}
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
		FROM event_source_links l
		JOIN events e ON e.id = l.event_id
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE l.source_id = ? AND l.source_event_key = ?
		LIMIT 1
	`, sourceID, sourceEventKey)
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
		LIMIT 1
	`, slug, string(domain.OriginLive))
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
		LIMIT 1
	`, slug, sourceID, string(domain.OriginLive))
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
	`, strings.TrimSpace(name), strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive))
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
	`, sourceID, strings.TrimSpace(name), strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive))
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

func stagingKeyValue(value string) any {
	return nullableReviewText(value)
}

func nullableReviewText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
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

func nullableCanonicalEventID(id int64) any {
	if id <= 0 {
		return nil
	}
	return id
}

func (s *Store) ListOpenReviewGroups(ctx context.Context) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN c.id END),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END)
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN COALESCE(v.name, '') END)
				ELSE ''
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status = ?
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
	`, review.StatusOpen)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	matcher, err := loadVenueMatcher(ctx, s.db)
	if err != nil {
		return nil, err
	}
	for i := range groups {
		sharedVenue, err := loadReviewGroupSharedVenue(ctx, s.db, matcher, groups[i].ID)
		if err != nil {
			return nil, err
		}
		if sharedVenue.status == venueMatchResolved {
			groups[i].SharedVenueSlug = sharedVenue.slug
			groups[i].SharedVenueName = sharedVenue.name
		} else {
			groups[i].SharedVenueSlug = ""
			groups[i].SharedVenueName = ""
		}
	}
	return groups, nil
}

func (s *Store) ListClosedReviewGroups(ctx context.Context, limit int) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if limit <= 0 {
		return nil, errors.New("review group limit must be positive")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN c.id END),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END)
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN COALESCE(v.name, '') END)
				ELSE ''
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status IN (?, ?)
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
		LIMIT ?
	`, review.StatusResolved, review.StatusRejected, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	matcher, err := loadVenueMatcher(ctx, s.db)
	if err != nil {
		return nil, err
	}
	for i := range groups {
		sharedVenue, err := loadReviewGroupSharedVenue(ctx, s.db, matcher, groups[i].ID)
		if err != nil {
			return nil, err
		}
		if sharedVenue.status == venueMatchResolved {
			groups[i].SharedVenueSlug = sharedVenue.slug
			groups[i].SharedVenueName = sharedVenue.name
		} else {
			groups[i].SharedVenueSlug = ""
			groups[i].SharedVenueName = ""
		}
	}
	return groups, nil
}

func (s *Store) ListReviewGroupsForImportRun(ctx context.Context, importRunID int64) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if importRunID <= 0 {
		return nil, errors.New("import run ID is required")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN c.id END),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END)
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN COALESCE(v.name, '') END)
				ELSE ''
			END
		FROM review_groups g
		JOIN import_run_review_groups l
			ON l.review_group_id = g.id
			AND l.import_run_id = ?
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status IN (?, ?, ?)
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
	`, importRunID, review.StatusOpen, review.StatusResolved, review.StatusRejected)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	matcher, err := loadVenueMatcher(ctx, s.db)
	if err != nil {
		return nil, err
	}
	for i := range groups {
		sharedVenue, err := loadReviewGroupSharedVenue(ctx, s.db, matcher, groups[i].ID)
		if err != nil {
			return nil, err
		}
		if sharedVenue.status == venueMatchResolved {
			groups[i].SharedVenueSlug = sharedVenue.slug
			groups[i].SharedVenueName = sharedVenue.name
		} else {
			groups[i].SharedVenueSlug = ""
			groups[i].SharedVenueName = ""
		}
	}
	return groups, nil
}

func (s *Store) LoadReviewGroup(ctx context.Context, id int64) (review.Group, bool, error) {
	if s == nil || s.db == nil {
		return review.Group{}, false, errors.New("sqlite store is not open")
	}
	if id <= 0 {
		return review.Group{}, false, nil
	}

	group, ok, err := loadReviewGroup(ctx, s.db, id)
	if err != nil || !ok {
		return review.Group{}, ok, err
	}
	candidates, err := loadReviewCandidates(ctx, s.db, id)
	if err != nil {
		return review.Group{}, false, err
	}
	choices, err := loadReviewDraftChoices(ctx, s.db, id)
	if err != nil {
		return review.Group{}, false, err
	}
	defaults, err := loadReviewDefaultChoices(ctx, s.db, id)
	if err != nil {
		return review.Group{}, false, err
	}
	group.Candidates = candidates
	group.DraftChoices = choices
	group.DefaultChoices = defaults
	matcher, err := loadVenueMatcher(ctx, s.db)
	if err != nil {
		return review.Group{}, false, err
	}
	sharedVenue := matcher.matchSharedVenue(candidates)
	if sharedVenue.status == venueMatchResolved {
		group.SharedVenueSlug = sharedVenue.slug
		group.SharedVenueName = sharedVenue.name
	} else {
		group.SharedVenueSlug = ""
		group.SharedVenueName = ""
	}
	return group, true, nil
}

func (s *Store) SaveReviewDraftChoices(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	if len(choices) == 0 {
		return errors.New("at least one review choice is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}

	now := time.Now().UTC()
	for _, choice := range choices {
		if !choice.Field.Valid() {
			return fmt.Errorf("invalid review field %q", choice.Field)
		}
		if choice.CandidateID <= 0 {
			return fmt.Errorf("candidate ID is required for %s", choice.Field.Label())
		}
		candidate, ok, err := loadReviewCandidate(ctx, tx, groupID, choice.CandidateID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("review candidate %d not found in group %d", choice.CandidateID, groupID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_draft_choices (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, field) DO UPDATE SET
				candidate_id = excluded.candidate_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, groupID, string(choice.Field), choice.CandidateID, review.CandidateValue(candidate, choice.Field), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Store) ResolveReviewGroup(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}
	candidates, err := loadReviewCandidates(ctx, tx, groupID)
	if err != nil {
		return err
	}
	group.Candidates = candidates
	choices = completeOptionalReviewRoomChoice(choices, candidates)
	if len(choices) != len(review.CanonicalFields) {
		return fmt.Errorf("all review fields must be selected before resolving")
	}
	matcher, err := loadVenueMatcher(ctx, tx)
	if err != nil {
		return err
	}

	seen := make(map[review.Field]struct{}, len(choices))
	selectedCandidates := make(map[review.Field]review.Candidate, len(choices))
	now := time.Now().UTC()
	for _, choice := range choices {
		if !choice.Field.Valid() {
			return fmt.Errorf("invalid review field %q", choice.Field)
		}
		if _, exists := seen[choice.Field]; exists {
			return fmt.Errorf("duplicate review field %q", choice.Field)
		}
		seen[choice.Field] = struct{}{}
		if choice.CandidateID <= 0 {
			return fmt.Errorf("candidate ID is required for %s", choice.Field.Label())
		}
		candidate, ok, err := loadReviewCandidate(ctx, tx, groupID, choice.CandidateID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("review candidate %d not found in group %d", choice.CandidateID, groupID)
		}
		selectedCandidates[choice.Field] = candidate
	}

	if venueCandidate, ok := selectedCandidates[review.FieldVenueSlug]; ok {
		resolvedSlug, err := resolveReviewVenueTx(ctx, tx, &matcher, venueCandidate)
		if err != nil {
			return err
		}
		venueCandidate.VenueSlug = resolvedSlug
		selectedCandidates[review.FieldVenueSlug] = venueCandidate
	}

	for _, choice := range choices {
		candidate := selectedCandidates[choice.Field]
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_draft_choices (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, field) DO UPDATE SET
				candidate_id = excluded.candidate_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, groupID, string(choice.Field), choice.CandidateID, review.CandidateValue(candidate, choice.Field), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	event, err := buildResolvedEvent(group, selectedCandidates, now)
	if err != nil {
		return err
	}
	event = s.decorateEventForPublish(event)
	canonicalCandidate, hasCanonicalCandidate := canonicalSnapshotCandidate(candidates)
	staged := stagedReviewCandidates(candidates)
	if authoritative, ok := reviewGroupAuthoritativeSource(group); ok {
		event.SourceName = authoritative.SourceName
		event.SourceURL = authoritative.SourceURL
		event = s.decorateEventForPublish(event)
		canonicalRecord, applied, err := applyAuthoritativeEventTx(ctx, tx, event, authoritative.SourceEventKey, now)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("venue %q not found", event.VenueSlug)
		}
		matchingStaged := reviewCandidatesMatchingEvent(staged, canonicalRecord.Event)
		if err := replaceEventSecondarySourceInfoTx(ctx, tx, canonicalRecord.ID, authoritative, matchingStaged, now); err != nil {
			return err
		}
		if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, canonicalRecord.ID, canonicalRecord.Event.Description, now); err != nil {
			return err
		}
		if hasCanonicalCandidate && canonicalCandidate.CanonicalEventID > 0 && canonicalCandidate.CanonicalEventID != canonicalRecord.ID {
			// The authoritative target wins when it disagrees with the canonical slug match.
		}
	} else if hasCanonicalCandidate {
		if err := updateCanonicalMatchedEventTx(ctx, tx, canonicalCandidate.CanonicalEventID, event); err != nil {
			return err
		}
		matchingStaged := reviewCandidatesMatchingEvent(staged, event)
		if err := upsertEventSecondarySourceInfoTx(ctx, tx, canonicalCandidate.CanonicalEventID, primarySourceIdentity(event), matchingStaged, now); err != nil {
			return err
		}
		if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, canonicalCandidate.CanonicalEventID, event.Description, now); err != nil {
			return err
		}
	} else {
		record, err := upsertEventTx(ctx, tx, event)
		if err != nil {
			return err
		}
		matchingStaged := reviewCandidatesMatchingEvent(staged, record.Event)
		if err := upsertEventSecondarySourceInfoTx(ctx, tx, record.ID, primarySourceIdentity(record.Event), matchingStaged, now); err != nil {
			return err
		}
		if err := refreshEventGenresFromStoredDescriptionsTx(ctx, tx, record.ID, record.Event.Description, now); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE id = ?
	`, review.StatusResolved, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func completeOptionalReviewRoomChoice(choices []review.DraftChoiceInput, candidates []review.Candidate) []review.DraftChoiceInput {
	for _, choice := range choices {
		if choice.Field == review.FieldRoomSlugs {
			return choices
		}
	}
	if len(candidates) == 0 || candidates[0].ID <= 0 {
		return choices
	}
	completed := make([]review.DraftChoiceInput, 0, len(choices)+1)
	completed = append(completed, choices...)
	completed = append(completed, review.DraftChoiceInput{
		Field:       review.FieldRoomSlugs,
		CandidateID: candidates[0].ID,
	})
	return completed
}

func (s *Store) UpdateReviewGroupStatus(ctx context.Context, groupID int64, status string) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	if status != review.StatusRejected {
		return fmt.Errorf("invalid review status %q", status)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE id = ?
	`, status, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func insertReviewCandidate(ctx context.Context, tx execer, groupID int64, position int, input review.CandidateInput, defaultSourceName, defaultSourceURL string) error {
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.CalendarURL = strings.TrimSpace(input.CalendarURL)
	if input.SourceName == "" {
		input.SourceName = defaultSourceName
	}
	if input.SourceURL == "" {
		input.SourceURL = defaultSourceURL
	}
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		return fmt.Errorf("review candidate %d name is required", position)
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO review_candidates (
			group_id,
			position,
			canonical_event_id,
			external_id,
			name,
			venue_slug,
			venue_text,
			venue_location_raw,
			room_text,
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
			source_name,
			source_url,
			calendar_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, position, nullableCanonicalEventID(input.CanonicalEventID), strings.TrimSpace(input.ExternalID), input.Name,
		strings.TrimSpace(input.VenueSlug),
		strings.TrimSpace(input.VenueText),
		input.VenueLocationRaw,
		strings.TrimSpace(input.RoomText),
		strings.TrimSpace(input.StartAt),
		strings.TrimSpace(input.EndAt),
		strings.TrimSpace(input.Genre),
		strings.TrimSpace(input.Status),
		strings.TrimSpace(input.Description),
		strings.TrimSpace(input.ImageURL),
		strings.TrimSpace(input.ImageSourceURL),
		strings.TrimSpace(input.ImageAlt),
		input.ImageWidth,
		input.ImageHeight,
		normalizedImageFocusValue(input.ImageFocusX),
		normalizedImageFocusValue(input.ImageFocusY),
		input.SourceName,
		input.SourceURL,
		input.CalendarURL,
		strings.TrimSpace(input.Provenance))
	if err != nil {
		return err
	}
	candidateID, err := res.LastInsertId()
	if err != nil {
		return err
	}
	return replaceReviewCandidateRoomsTx(ctx, tx, candidateID, input.Rooms)
}

func loadReviewGroup(ctx context.Context, q queryer, id int64) (review.Group, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.authoritative_source_name,
			g.authoritative_source_url,
			g.authoritative_source_event_key,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			COUNT(CASE WHEN c.canonical_event_id IS NULL THEN 1 END),
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN COALESCE(v.name, '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END)
				ELSE NULL
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.id = ?
		GROUP BY g.id
		LIMIT 1
	`, id)
	return scanReviewGroupRow(row, id)
}

func loadReviewGroupByStagingKey(ctx context.Context, q queryer, stagingKey string) (review.Group, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.authoritative_source_name,
			g.authoritative_source_url,
			g.authoritative_source_event_key,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			COUNT(CASE WHEN c.canonical_event_id IS NULL THEN 1 END),
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.venue_slug), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN COALESCE(v.name, '') END)
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END) = 1
				THEN MIN(CASE WHEN c.canonical_event_id IS NULL THEN NULLIF(TRIM(c.start_at), '') END)
				ELSE NULL
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.staging_key = ?
		GROUP BY g.id
		LIMIT 1
	`, stagingKey)
	return scanReviewGroupRow(row, 0)
}

func scanReviewGroupRow(scanner interface {
	Scan(...any) error
}, fallbackID int64) (review.Group, bool, error) {
	var group review.Group
	var authoritativeSourceName sql.NullString
	var authoritativeSourceURL sql.NullString
	var authoritativeSourceEventKey sql.NullString
	var createdAt string
	var updatedAt string
	var sharedVenueSlug string
	var sharedVenueName string
	var sharedStartAt sql.NullString
	switch err := scanner.Scan(
		&group.ID,
		&group.Title,
		&group.SourceName,
		&group.SourceURL,
		&authoritativeSourceName,
		&authoritativeSourceURL,
		&authoritativeSourceEventKey,
		&group.Status,
		&group.Notes,
		&createdAt,
		&updatedAt,
		&group.LatestImportRunID,
		&group.StagedCandidateCount,
		&sharedVenueSlug,
		&sharedVenueName,
		&sharedStartAt,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return review.Group{}, false, nil
	case err != nil:
		return review.Group{}, false, err
	}
	group.AuthoritativeSourceName = strings.TrimSpace(authoritativeSourceName.String)
	group.AuthoritativeSourceURL = strings.TrimSpace(authoritativeSourceURL.String)
	group.AuthoritativeSourceEventKey = strings.TrimSpace(authoritativeSourceEventKey.String)
	group.SharedVenueSlug = strings.TrimSpace(sharedVenueSlug)
	group.SharedVenueName = strings.TrimSpace(sharedVenueName)
	if sharedStartAt.Valid && strings.TrimSpace(sharedStartAt.String) != "" {
		parsedSharedStartAt, err := parseRFC3339UTC(sharedStartAt.String)
		if err != nil {
			if fallbackID == 0 {
				fallbackID = group.ID
			}
			return review.Group{}, false, fmt.Errorf("parse review group %d shared_start_at: %w", fallbackID, err)
		}
		group.SharedStartAt = &parsedSharedStartAt
	}
	parsedCreatedAt, err := parseRFC3339UTC(createdAt)
	if err != nil {
		if fallbackID == 0 {
			fallbackID = group.ID
		}
		return review.Group{}, false, fmt.Errorf("parse review group %d created_at: %w", fallbackID, err)
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		if fallbackID == 0 {
			fallbackID = group.ID
		}
		return review.Group{}, false, fmt.Errorf("parse review group %d updated_at: %w", fallbackID, err)
	}
	group.CreatedAt = parsedCreatedAt
	group.UpdatedAt = parsedUpdatedAt
	return group, true, nil
}

func scanReviewGroupSummaryRow(scanner interface {
	Scan(...any) error
}) (review.GroupSummary, error) {
	var group review.GroupSummary
	var createdAt string
	var updatedAt string
	var authoritative int
	var sharedStartAt sql.NullString
	if err := scanner.Scan(
		&group.ID,
		&group.Title,
		&group.SourceName,
		&group.SourceURL,
		&group.Status,
		&group.Notes,
		&createdAt,
		&updatedAt,
		&group.CandidateCount,
		&group.DraftCount,
		&group.LatestImportRunID,
		&authoritative,
		&sharedStartAt,
		&group.SharedVenueSlug,
		&group.SharedVenueName,
	); err != nil {
		return review.GroupSummary{}, err
	}
	parsedCreatedAt, err := parseRFC3339UTC(createdAt)
	if err != nil {
		return review.GroupSummary{}, fmt.Errorf("parse review group %d created_at: %w", group.ID, err)
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return review.GroupSummary{}, fmt.Errorf("parse review group %d updated_at: %w", group.ID, err)
	}
	group.CreatedAt = parsedCreatedAt
	group.UpdatedAt = parsedUpdatedAt
	group.Authoritative = authoritative == 1
	group.SharedVenueSlug = strings.TrimSpace(group.SharedVenueSlug)
	group.SharedVenueName = strings.TrimSpace(group.SharedVenueName)
	if sharedStartAt.Valid && strings.TrimSpace(sharedStartAt.String) != "" {
		parsedSharedStartAt, err := parseRFC3339UTC(sharedStartAt.String)
		if err != nil {
			return review.GroupSummary{}, fmt.Errorf("parse review group %d shared_start_at: %w", group.ID, err)
		}
		group.SharedStartAt = &parsedSharedStartAt
	}
	return group, nil
}

type reviewGroupAuthoritativeLink struct {
	SourceName     string
	SourceURL      string
	SourceEventKey string
}

type reviewGroupAuthoritativeLinkInput struct {
	SourceName     string
	SourceURL      string
	SourceEventKey string
}

func reviewGroupAuthoritativeSource(group review.Group) (reviewGroupAuthoritativeLink, bool) {
	sourceName := strings.TrimSpace(group.AuthoritativeSourceName)
	sourceURL := strings.TrimSpace(group.AuthoritativeSourceURL)
	sourceEventKey := strings.TrimSpace(group.AuthoritativeSourceEventKey)
	if sourceName == "" || sourceURL == "" || sourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceEventKey,
	}, true
}

func backfillOpenReviewGroupsAuthoritativeLinks(ctx context.Context, tx interface {
	execer
	queryer
}, sourceMetadata ingest.SourceMetadataLookup) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM review_groups
		WHERE status = ?
			AND (
				authoritative_source_name IS NULL
				OR authoritative_source_name = ''
				OR authoritative_source_url IS NULL
				OR authoritative_source_url = ''
				OR authoritative_source_event_key IS NULL
				OR authoritative_source_event_key = ''
			)
	`, review.StatusOpen)
	if err != nil {
		return err
	}
	defer rows.Close()

	var groupIDs []int64
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, groupID := range groupIDs {
		group, ok, err := loadReviewGroup(ctx, tx, groupID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		candidates, err := loadReviewCandidates(ctx, tx, groupID)
		if err != nil {
			return err
		}
		link, ok := deriveReviewGroupAuthoritativeLink(sourceMetadata, group, candidates)
		if !ok {
			continue
		}
		if err := refreshReviewGroupAuthoritativeLinkTx(ctx, tx, groupID, reviewGroupAuthoritativeLinkInput(link), now); err != nil {
			return err
		}
	}
	return nil
}

func backfillReviewFieldDefaults(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM review_groups
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var groupIDs []int64
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, groupID := range groupIDs {
		if err := recomputeReviewFieldDefaultsTx(ctx, tx, groupID, now); err != nil {
			return err
		}
	}
	return nil
}

func refreshReviewGroupAuthoritativeLinkTx(ctx context.Context, tx execer, groupID int64, input reviewGroupAuthoritativeLinkInput, now time.Time) error {
	link, ok := normalizeReviewGroupAuthoritativeLinkInput(input)
	if !ok {
		_, err := tx.ExecContext(ctx, `
			UPDATE review_groups
			SET authoritative_source_name = NULL,
				authoritative_source_url = NULL,
				authoritative_source_event_key = NULL,
				updated_at = CASE
					WHEN COALESCE(authoritative_source_name, '') <> ''
						OR COALESCE(authoritative_source_url, '') <> ''
						OR COALESCE(authoritative_source_event_key, '') <> ''
					THEN ?
					ELSE updated_at
				END
			WHERE id = ?
		`, formatRFC3339UTC(now), groupID)
		return err
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET authoritative_source_name = ?,
			authoritative_source_url = ?,
			authoritative_source_event_key = ?,
			updated_at = CASE
				WHEN COALESCE(authoritative_source_name, '') <> ?
					OR COALESCE(authoritative_source_url, '') <> ?
					OR COALESCE(authoritative_source_event_key, '') <> ?
				THEN ?
				ELSE updated_at
			END
		WHERE id = ?
	`, link.SourceName, link.SourceURL, link.SourceEventKey,
		link.SourceName, link.SourceURL, link.SourceEventKey,
		formatRFC3339UTC(now), groupID)
	return err
}

func normalizeReviewGroupAuthoritativeLinkInput(input reviewGroupAuthoritativeLinkInput) (reviewGroupAuthoritativeLink, bool) {
	link := reviewGroupAuthoritativeLink{
		SourceName:     strings.TrimSpace(input.SourceName),
		SourceURL:      strings.TrimSpace(input.SourceURL),
		SourceEventKey: strings.TrimSpace(input.SourceEventKey),
	}
	if link.SourceName == "" || link.SourceURL == "" || link.SourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return link, true
}

func deriveReviewGroupAuthoritativeLink(sourceMetadata ingest.SourceMetadataLookup, group review.Group, candidates []review.Candidate) (reviewGroupAuthoritativeLink, bool) {
	candidates = stagedReviewCandidates(candidates)
	if len(candidates) == 0 {
		return reviewGroupAuthoritativeLink{}, false
	}

	var venueSlug string
	var link reviewGroupAuthoritativeLink
	for _, candidate := range candidates {
		candidateVenueSlug := strings.TrimSpace(candidate.VenueSlug)
		if candidateVenueSlug == "" {
			return reviewGroupAuthoritativeLink{}, false
		}
		if venueSlug == "" {
			venueSlug = candidateVenueSlug
		} else if candidateVenueSlug != venueSlug {
			return reviewGroupAuthoritativeLink{}, false
		}

		candidateLink, ok := authoritativeLinkFromStoredReviewCandidate(group, candidate)
		if !ok {
			return reviewGroupAuthoritativeLink{}, false
		}
		if link.SourceEventKey == "" {
			link = candidateLink
			continue
		}
		if candidateLink != link {
			return reviewGroupAuthoritativeLink{}, false
		}
	}
	if sourceMetadata.OwnedVenueSlugForReviewStageSourceName(link.SourceName) != venueSlug {
		return reviewGroupAuthoritativeLink{}, false
	}
	return link, true
}

func authoritativeLinkFromStoredReviewCandidate(group review.Group, candidate review.Candidate) (reviewGroupAuthoritativeLink, bool) {
	sourceName := strings.TrimSpace(candidate.SourceName)
	if sourceName == "" {
		sourceName = strings.TrimSpace(group.SourceName)
	}
	sourceURL := strings.TrimSpace(candidate.CalendarURL)
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(candidate.SourceURL)
	}
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(group.SourceURL)
	}
	sourceEventKey := strings.TrimSpace(candidate.ExternalID)
	if sourceEventKey == "" {
		sourceEventKey = strings.TrimSpace(candidate.SourceURL)
	}
	if sourceEventKey == "" {
		sourceEventKey = sourceURL
	}
	if sourceName == "" || sourceURL == "" || sourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceEventKey,
	}, true
}

func replaceEventSecondarySourceInfoTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
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
}, event domain.Event) (eventRecord, error) {
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
	record.Event.Rooms = append([]domain.VenueRoom(nil), event.Rooms...)
	record.Event.RoomText = strings.TrimSpace(event.RoomText)
	return record, nil
}

func updateCanonicalMatchedEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event) error {
	if eventID <= 0 {
		return errors.New("canonical event ID is required")
	}
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("venue %q not found", event.VenueSlug)
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return err
	}
	if conflict, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return err
	} else if ok && conflict.ID != eventID {
		return fmt.Errorf("review event slug %q already belongs to a different event", event.Slug)
	}
	incomingImageURL := strings.TrimSpace(event.ImageURL)
	_, err = tx.ExecContext(ctx, `
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
	return replaceEventRoomsTx(ctx, tx, eventID, event)
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

func reviewGroupExists(ctx context.Context, q queryer, id int64) (bool, error) {
	_, ok, err := loadReviewGroup(ctx, q, id)
	return ok, err
}

func loadReviewCandidates(ctx context.Context, q queryer, groupID int64) ([]review.Candidate, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
			group_id,
			position,
			COALESCE(canonical_event_id, 0),
			external_id,
			name,
			venue_slug,
			venue_text,
			venue_location_raw,
			room_text,
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
			source_name,
			source_url,
			calendar_url,
			provenance
		FROM review_candidates
		WHERE group_id = ?
		ORDER BY CASE WHEN canonical_event_id IS NULL THEN 0 ELSE 1 END, position, id
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []review.Candidate
	for rows.Next() {
		candidate, err := scanReviewCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := hydrateReviewCandidateRooms(ctx, q, candidates); err != nil {
		return nil, err
	}
	return candidates, nil
}

func loadReviewCandidate(ctx context.Context, q queryer, groupID, candidateID int64) (review.Candidate, bool, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
			group_id,
			position,
			COALESCE(canonical_event_id, 0),
			external_id,
			name,
			venue_slug,
			venue_text,
			venue_location_raw,
			room_text,
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
			source_name,
			source_url,
			calendar_url,
			provenance
		FROM review_candidates
		WHERE group_id = ? AND id = ?
		LIMIT 1
	`, groupID, candidateID)
	if err != nil {
		return review.Candidate{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return review.Candidate{}, false, err
		}
		return review.Candidate{}, false, nil
	}
	candidate, err := scanReviewCandidate(rows)
	if err != nil {
		return review.Candidate{}, false, err
	}
	if err := rows.Err(); err != nil {
		return review.Candidate{}, false, err
	}
	candidates := []review.Candidate{candidate}
	if err := hydrateReviewCandidateRooms(ctx, q, candidates); err != nil {
		return review.Candidate{}, false, err
	}
	candidate = candidates[0]
	return candidate, true, nil
}

func loadCanonicalSnapshotCandidate(ctx context.Context, q queryer, groupID int64) (review.Candidate, bool, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
			group_id,
			position,
			COALESCE(canonical_event_id, 0),
			external_id,
			name,
			venue_slug,
			venue_text,
			venue_location_raw,
			room_text,
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
			source_name,
			source_url,
			calendar_url,
			provenance
		FROM review_candidates
		WHERE group_id = ? AND canonical_event_id IS NOT NULL
		ORDER BY id
		LIMIT 1
	`, groupID)
	if err != nil {
		return review.Candidate{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return review.Candidate{}, false, err
		}
		return review.Candidate{}, false, nil
	}
	candidate, err := scanReviewCandidate(rows)
	if err != nil {
		return review.Candidate{}, false, err
	}
	if err := rows.Err(); err != nil {
		return review.Candidate{}, false, err
	}
	candidates := []review.Candidate{candidate}
	if err := hydrateReviewCandidateRooms(ctx, q, candidates); err != nil {
		return review.Candidate{}, false, err
	}
	candidate = candidates[0]
	return candidate, true, nil
}

func scanReviewCandidate(rows *sql.Rows) (review.Candidate, error) {
	var candidate review.Candidate
	if err := rows.Scan(
		&candidate.ID,
		&candidate.GroupID,
		&candidate.Position,
		&candidate.CanonicalEventID,
		&candidate.ExternalID,
		&candidate.Name,
		&candidate.VenueSlug,
		&candidate.VenueText,
		&candidate.VenueLocationRaw,
		&candidate.RoomText,
		&candidate.StartAt,
		&candidate.EndAt,
		&candidate.Genre,
		&candidate.Status,
		&candidate.Description,
		&candidate.ImageURL,
		&candidate.ImageSourceURL,
		&candidate.ImageAlt,
		&candidate.ImageWidth,
		&candidate.ImageHeight,
		&candidate.ImageFocusX,
		&candidate.ImageFocusY,
		&candidate.SourceName,
		&candidate.SourceURL,
		&candidate.CalendarURL,
		&candidate.Provenance,
	); err != nil {
		return review.Candidate{}, err
	}
	focus := normalizedImageFocus(candidate.ImageFocusX, candidate.ImageFocusY)
	candidate.ImageFocusX = focus.X
	candidate.ImageFocusY = focus.Y
	return candidate, nil
}

func loadReviewDraftChoices(ctx context.Context, q queryer, groupID int64) (map[review.Field]review.DraftChoice, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT field, candidate_id, value, updated_at
		FROM review_draft_choices
		WHERE group_id = ?
		ORDER BY field
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	choices := make(map[review.Field]review.DraftChoice)
	for rows.Next() {
		var choice review.DraftChoice
		var field string
		var updatedAt string
		if err := rows.Scan(&field, &choice.CandidateID, &choice.Value, &updatedAt); err != nil {
			return nil, err
		}
		parsedField, ok := review.ParseField(field)
		if !ok {
			return nil, fmt.Errorf("invalid stored review field %q", field)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review choice %q updated_at: %w", field, err)
		}
		choice.Field = parsedField
		choice.UpdatedAt = parsedUpdatedAt
		choices[parsedField] = choice
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return choices, nil
}

func loadReviewDefaultChoices(ctx context.Context, q queryer, groupID int64) (map[review.Field]review.DraftChoice, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT field, candidate_id, value, updated_at
		FROM review_field_defaults
		WHERE group_id = ?
		ORDER BY field
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	choices := make(map[review.Field]review.DraftChoice)
	for rows.Next() {
		var choice review.DraftChoice
		var field string
		var updatedAt string
		if err := rows.Scan(&field, &choice.CandidateID, &choice.Value, &updatedAt); err != nil {
			return nil, err
		}
		parsedField, ok := review.ParseField(field)
		if !ok {
			return nil, fmt.Errorf("invalid stored review default field %q", field)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review default %q updated_at: %w", field, err)
		}
		choice.Field = parsedField
		choice.UpdatedAt = parsedUpdatedAt
		choices[parsedField] = choice
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return choices, nil
}
