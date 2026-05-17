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

type EventTitleRepairReport struct {
	DryRun              bool                     `json:"dry_run"`
	Applied             bool                     `json:"applied"`
	Repaired            int                      `json:"repaired"`
	ReviewGroupsCreated int                      `json:"review_groups_created"`
	ReviewGroupsReused  int                      `json:"review_groups_reused"`
	Unchanged           int                      `json:"unchanged"`
	Skipped             int                      `json:"skipped"`
	Changes             []EventTitleRepairChange `json:"changes"`
}

type EventTitleRepairChange struct {
	Result        string `json:"result"`
	Reason        string `json:"reason,omitempty"`
	MatchKind     string `json:"match_kind,omitempty"`
	EventID       int64  `json:"event_id,omitempty"`
	ReviewGroupID int64  `json:"review_group_id,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	OldSlug       string `json:"old_slug,omitempty"`
	NewSlug       string `json:"new_slug,omitempty"`
	OldTitle      string `json:"old_title,omitempty"`
	NewTitle      string `json:"new_title,omitempty"`
}

const (
	eventTitleRepairCleanedProvenance              = "Cleaned title from title repair"
	eventTitleRepairAuthoritativeCleanedProvenance = "Authoritative cleaned title from title repair"
	eventTitleRepairExistingCleanProvenance        = "Existing live event with cleaned title"
	eventTitleRepairCanonicalProvenance            = "Canonical live event snapshot"
)

func (s *Store) RepairEventTitlesFromReport(ctx context.Context, catalog *ingest.Catalog, report ingest.Report, apply bool) (EventTitleRepairReport, error) {
	repair := EventTitleRepairReport{
		DryRun:  !apply,
		Applied: apply,
		Changes: []EventTitleRepairChange{},
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

	now := time.Now().UTC()
	groups := ingest.ReviewGroupsFromReportWithCatalog(catalog, report)
	for _, group := range groups {
		for _, candidate := range group.Candidates {
			single := singleCandidateTitleRepairGroup(group, candidate)
			event, err := singletonResolvedEventFromGroupInput(single, now)
			if err != nil {
				repair.addTitleRepairChange(EventTitleRepairChange{
					Result:     "skipped",
					Reason:     err.Error(),
					SourceName: strings.TrimSpace(candidate.SourceName),
					SourceURL:  strings.TrimSpace(candidate.SourceURL),
					NewTitle:   strings.TrimSpace(candidate.Name),
				})
				continue
			}
			event = s.decorateEventForPublish(event)

			var change EventTitleRepairChange
			if strings.TrimSpace(single.AuthoritativeSourceName) != "" &&
				strings.TrimSpace(single.AuthoritativeSourceURL) != "" &&
				strings.TrimSpace(single.AuthoritativeSourceEventKey) != "" {
				change, err = s.repairAuthoritativeEventTitle(ctx, single, event, apply, now)
			} else {
				change, err = s.stageSupportingEventTitleRepair(ctx, single, event, apply, now)
			}
			if err != nil {
				return repair, err
			}
			repair.addTitleRepairChange(change)
		}
	}
	return repair, nil
}

func (r *EventTitleRepairReport) addTitleRepairChange(change EventTitleRepairChange) {
	r.Changes = append(r.Changes, change)
	switch change.Result {
	case "repaired", "would_repair":
		r.Repaired++
	case "review_created", "would_create_review":
		r.ReviewGroupsCreated++
	case "review_reused":
		r.ReviewGroupsReused++
	case "unchanged":
		r.Unchanged++
	default:
		r.Skipped++
	}
}

func singleCandidateTitleRepairGroup(group review.GroupInput, candidate review.CandidateInput) review.GroupInput {
	group.Candidates = []review.CandidateInput{candidate}
	return group
}

func (s *Store) repairAuthoritativeEventTitle(ctx context.Context, group review.GroupInput, incoming domain.Event, apply bool, now time.Time) (EventTitleRepairChange, error) {
	change := titleRepairChangeForIncoming(incoming)
	change.MatchKind = "authoritative"
	authoritative, ok := reviewGroupInputAuthoritativeSource(group)
	if !ok {
		authoritative = reviewGroupAuthoritativeLink{
			SourceName:     strings.TrimSpace(incoming.SourceName),
			SourceURL:      strings.TrimSpace(incoming.SourceURL),
			SourceEventKey: strings.TrimSpace(authoritativeSourceEventKey(group)),
		}
	}
	change.SourceName = authoritative.SourceName
	change.SourceURL = authoritative.SourceURL

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, tx, authoritative.SourceName, authoritative.SourceURL)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if !ok {
		change.Result = "skipped"
		change.Reason = "source not found"
		return change, nil
	}

	record, matchKind, found, ambiguous, err := findAuthoritativeEventForTitleRepairTx(ctx, tx, sourceID, incoming, authoritative.SourceEventKey)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if ambiguous {
		change.Result = "skipped"
		change.Reason = "ambiguous authoritative match"
		change.MatchKind = matchKind
		return change, nil
	}
	if !found {
		change.Result = "skipped"
		change.Reason = "no authoritative match"
		change.MatchKind = matchKind
		return change, nil
	}

	change.EventID = record.ID
	change.MatchKind = matchKind
	change.OldSlug = record.Event.Slug
	change.OldTitle = record.Event.Name
	if titleRepairUnchanged(record.Event, incoming) {
		change.Result = "unchanged"
		return change, nil
	}
	if conflict, ok, err := loadEventRecordBySlugTx(ctx, tx, incoming.Slug); err != nil {
		return EventTitleRepairChange{}, err
	} else if ok && conflict.ID != record.ID {
		if !eventRecordHasResolvedIdentity(conflict, incoming) {
			change.Result = "skipped"
			change.Reason = "target slug already belongs to another event"
			return change, nil
		}
		if !apply {
			change.Result = "would_create_review"
			return change, nil
		}
		groupID, created, err := stageEventTitleRepairReviewGroupTx(ctx, tx, group, record, incoming, now)
		if err != nil {
			return EventTitleRepairChange{}, err
		}
		if err := tx.Commit(); err != nil {
			return EventTitleRepairChange{}, err
		}
		change.ReviewGroupID = groupID
		if created {
			change.Result = "review_created"
		} else {
			change.Result = "review_reused"
		}
		return change, nil
	}
	if !apply {
		change.Result = "would_repair"
		return change, nil
	}

	if err := updateEventTitleTx(ctx, tx, record.ID, incoming.Slug, incoming.Name, now); err != nil {
		return EventTitleRepairChange{}, err
	}
	if sourceEventKey := strings.TrimSpace(authoritative.SourceEventKey); sourceEventKey != "" {
		if err := ensureEventSourceLinkTx(ctx, tx, record.ID, sourceID, sourceEventKey, now); err != nil {
			return EventTitleRepairChange{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return EventTitleRepairChange{}, err
	}
	change.Result = "repaired"
	return change, nil
}

func (s *Store) stageSupportingEventTitleRepair(ctx context.Context, group review.GroupInput, incoming domain.Event, apply bool, now time.Time) (EventTitleRepairChange, error) {
	change := titleRepairChangeForIncoming(incoming)
	change.MatchKind = "supporting_clean_title"

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	records, err := matchingLiveEventRecordsByCleanTitleTx(ctx, tx, incoming)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	record, found, ambiguous := selectTitleRepairRecord(records, incoming)
	if ambiguous {
		change.Result = "skipped"
		change.Reason = "ambiguous supporting match"
		return change, nil
	}
	if !found {
		change.Result = "skipped"
		change.Reason = "no supporting match"
		return change, nil
	}

	change.EventID = record.ID
	change.OldSlug = record.Event.Slug
	change.OldTitle = record.Event.Name
	if titleRepairUnchanged(record.Event, incoming) {
		change.Result = "unchanged"
		return change, nil
	}
	hasAuthoritative, err := eventHasAuthoritativeSourceLinkTx(ctx, tx, record.ID)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if hasAuthoritative {
		change.Result = "skipped"
		change.Reason = "matched event has authoritative source link"
		return change, nil
	}
	if !apply {
		change.Result = "would_create_review"
		return change, nil
	}

	groupID, created, err := stageEventTitleRepairReviewGroupTx(ctx, tx, group, record, incoming, now)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if err := tx.Commit(); err != nil {
		return EventTitleRepairChange{}, err
	}
	change.ReviewGroupID = groupID
	if created {
		change.Result = "review_created"
	} else {
		change.Result = "review_reused"
	}
	return change, nil
}

func selectTitleRepairRecord(records []eventRecord, incoming domain.Event) (eventRecord, bool, bool) {
	if len(records) == 0 {
		return eventRecord{}, false, false
	}
	var repairable []eventRecord
	var unchanged []eventRecord
	for _, record := range records {
		if titleRepairUnchanged(record.Event, incoming) {
			unchanged = append(unchanged, record)
			continue
		}
		repairable = append(repairable, record)
	}
	switch {
	case len(repairable) == 1:
		return repairable[0], true, false
	case len(repairable) > 1:
		return eventRecord{}, false, true
	case len(unchanged) == 1:
		return unchanged[0], true, false
	default:
		return eventRecord{}, false, true
	}
}

func titleRepairChangeForIncoming(incoming domain.Event) EventTitleRepairChange {
	return EventTitleRepairChange{
		SourceName: strings.TrimSpace(incoming.SourceName),
		SourceURL:  strings.TrimSpace(incoming.SourceURL),
		NewSlug:    strings.TrimSpace(incoming.Slug),
		NewTitle:   strings.TrimSpace(incoming.Name),
	}
}

func titleRepairUnchanged(existing, incoming domain.Event) bool {
	return strings.TrimSpace(existing.Name) == strings.TrimSpace(incoming.Name) &&
		strings.TrimSpace(existing.Slug) == strings.TrimSpace(incoming.Slug)
}

func findAuthoritativeEventForTitleRepairTx(ctx context.Context, q queryer, sourceID int64, incoming domain.Event, sourceEventKey string) (eventRecord, string, bool, bool, error) {
	if sourceEventKey = strings.TrimSpace(sourceEventKey); sourceEventKey != "" {
		record, ok, err := loadEventRecordBySourceLinkTx(ctx, q, sourceID, sourceEventKey)
		if err != nil || ok {
			return record, "authoritative_link", ok, false, err
		}
	}

	records, err := matchingLiveEventRecordsByCleanTitleAndSourceTx(ctx, q, sourceID, incoming)
	if err != nil {
		return eventRecord{}, "same_source_clean_title", false, false, err
	}
	record, found, ambiguous := selectTitleRepairRecord(records, incoming)
	if ambiguous {
		return eventRecord{}, "same_source_clean_title", false, true, nil
	}
	if !found {
		return eventRecord{}, "same_source_clean_title", false, false, nil
	}
	return record, "same_source_clean_title", true, false, nil
}

func matchingLiveEventRecordsByCleanTitleAndSourceTx(ctx context.Context, q queryer, sourceID int64, incoming domain.Event) ([]eventRecord, error) {
	records, err := loadLiveEventRecordsByVenueStartAndSourceTx(ctx, q, sourceID, incoming.VenueSlug, incoming.Start)
	if err != nil {
		return nil, err
	}
	return filterTitleRepairMatches(records, incoming), nil
}

func matchingLiveEventRecordsByCleanTitleTx(ctx context.Context, q queryer, incoming domain.Event) ([]eventRecord, error) {
	records, err := loadLiveEventRecordsByVenueStartTx(ctx, q, incoming.VenueSlug, incoming.Start)
	if err != nil {
		return nil, err
	}
	return filterTitleRepairMatches(records, incoming), nil
}

func filterTitleRepairMatches(records []eventRecord, incoming domain.Event) []eventRecord {
	want := normalizedReviewEventName(incoming.Name)
	matches := make([]eventRecord, 0, len(records))
	for _, record := range records {
		cleaned := ingest.CleanEventTitleForVenue(record.Event.Name, record.Event.VenueSlug)
		if normalizedReviewEventName(cleaned) == want {
			matches = append(matches, record)
		}
	}
	return matches
}

func loadLiveEventRecordsByVenueStartTx(ctx context.Context, q queryer, venueSlug string, start time.Time) ([]eventRecord, error) {
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
		WHERE v.slug = ?
		  AND e.start_at = ?
		  AND e.origin = ?
	`, strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive))
}

func loadLiveEventRecordsByVenueStartAndSourceTx(ctx context.Context, q queryer, sourceID int64, venueSlug string, start time.Time) ([]eventRecord, error) {
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
		  AND v.slug = ?
		  AND e.start_at = ?
		  AND e.origin = ?
	`, sourceID, strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive))
}

func updateEventTitleTx(ctx context.Context, tx execer, eventID int64, slug, name string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE events
		SET slug = ?,
			name = ?,
			last_checked_at = ?
		WHERE id = ?
	`, strings.TrimSpace(slug), strings.TrimSpace(name), formatRFC3339UTC(now), eventID)
	return err
}

func eventHasAuthoritativeSourceLinkTx(ctx context.Context, q queryer, eventID int64) (bool, error) {
	var id int64
	err := q.QueryRowContext(ctx, `
		SELECT id
		FROM event_source_links
		WHERE event_id = ? AND is_authoritative = 1
		LIMIT 1
	`, eventID).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func stageEventTitleRepairReviewGroupTx(ctx context.Context, tx interface {
	execer
	queryer
}, input review.GroupInput, record eventRecord, incoming domain.Event, now time.Time) (int64, bool, error) {
	stagingKey := eventTitleRepairStagingKey(record.ID, incoming.Name, incoming.Slug)
	title := "Event title repair: " + strings.TrimSpace(record.Event.Name) + " -> " + strings.TrimSpace(incoming.Name)
	notes := "Created from event title repair."
	if input.ImportRunID > 0 {
		notes = fmt.Sprintf("Created from import run %d event title repair.", input.ImportRunID)
	}
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
		) VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?)
	`, title, incoming.SourceName, incoming.SourceURL, stagingKey, review.StatusOpen, notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if rowsAffected == 0 {
		group, ok, err := loadReviewGroupByStagingKey(ctx, tx, stagingKey)
		if err != nil || !ok {
			return 0, false, err
		}
		if err := linkTitleRepairImportRunTx(ctx, tx, input.ImportRunID, group.ID, now); err != nil {
			return 0, false, err
		}
		return group.ID, false, nil
	}

	groupID, err := res.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	candidates, err := eventTitleRepairReviewCandidatesTx(ctx, tx, input, record, incoming)
	if err != nil {
		return 0, false, err
	}
	if err := insertReviewCandidatesTx(ctx, tx, groupID, candidates, incoming.SourceName, incoming.SourceURL); err != nil {
		return 0, false, err
	}
	if err := recomputeReviewFieldDefaultsTx(ctx, tx, groupID, now); err != nil {
		return 0, false, err
	}
	if err := linkTitleRepairImportRunTx(ctx, tx, input.ImportRunID, groupID, now); err != nil {
		return 0, false, err
	}
	return groupID, true, nil
}

func eventTitleRepairReviewCandidatesTx(ctx context.Context, q queryer, input review.GroupInput, record eventRecord, incoming domain.Event) ([]review.CandidateInput, error) {
	externalID := ""
	if len(input.Candidates) > 0 {
		externalID = strings.TrimSpace(input.Candidates[0].ExternalID)
	}
	provenance := eventTitleRepairCleanedProvenance
	if authoritative, ok := reviewGroupInputAuthoritativeSource(input); ok {
		externalID = authoritative.SourceEventKey
		provenance = eventTitleRepairAuthoritativeCleanedProvenance
	}
	cleaned := reviewCandidateInputFromEvent(incoming, externalID, provenance)
	if authoritative, ok := reviewGroupInputAuthoritativeSource(input); ok {
		cleaned.SourceName = authoritative.SourceName
		if ingest.IsCalendarURL(authoritative.SourceURL) {
			cleaned.CalendarURL = authoritative.SourceURL
		} else {
			cleaned.SourceURL = authoritative.SourceURL
		}
	}
	candidates := []review.CandidateInput{
		cleaned,
	}
	if conflict, ok, err := loadEventRecordBySlugTx(ctx, q, incoming.Slug); err != nil {
		return nil, err
	} else if ok && conflict.ID != record.ID && eventRecordHasResolvedIdentity(conflict, incoming) {
		candidates = append(candidates, reviewCandidateInputFromEvent(conflict.Event, "", eventTitleRepairExistingCleanProvenance))
	}
	canonical := reviewCandidateInputFromEvent(record.Event, "", eventTitleRepairCanonicalProvenance)
	canonical.CanonicalEventID = record.ID
	candidates = append(candidates, canonical)
	return candidates, nil
}

func reviewCandidateInputFromEvent(event domain.Event, externalID, provenance string) review.CandidateInput {
	return review.CandidateInput{
		ExternalID:     strings.TrimSpace(externalID),
		Name:           strings.TrimSpace(event.Name),
		VenueSlug:      strings.TrimSpace(event.VenueSlug),
		StartAt:        formatRFC3339UTC(event.Start),
		EndAt:          formatOptionalTime(event.End),
		Genre:          strings.TrimSpace(event.Genre),
		Status:         strings.TrimSpace(event.Status),
		Description:    strings.TrimSpace(event.Description),
		ImageURL:       strings.TrimSpace(event.ImageURL),
		ImageSourceURL: strings.TrimSpace(event.ImageSourceURL),
		ImageAlt:       strings.TrimSpace(event.ImageAlt),
		ImageWidth:     event.ImageWidth,
		ImageHeight:    event.ImageHeight,
		ImageFocusX:    event.ImageFocusX,
		ImageFocusY:    event.ImageFocusY,
		SourceName:     strings.TrimSpace(event.SourceName),
		SourceURL:      firstNonEmptyReviewText(event.OfficialListingURL, event.SourceURL),
		CalendarURL:    strings.TrimSpace(event.CalendarURL),
		Provenance:     provenance,
	}
}

func eventTitleRepairStagingKey(eventID int64, name, slug string) string {
	sum := sha256.New()
	writeStagedReviewCandidateFingerprintPart(sum, "event-title-repair:v1")
	writeStagedReviewCandidateFingerprintPart(sum, fmt.Sprintf("%d", eventID))
	writeStagedReviewCandidateFingerprintPart(sum, strings.TrimSpace(name))
	writeStagedReviewCandidateFingerprintPart(sum, strings.TrimSpace(slug))
	return "title-repair:" + hex.EncodeToString(sum.Sum(nil))
}

func linkTitleRepairImportRunTx(ctx context.Context, tx interface {
	execer
	queryer
}, importRunID, groupID int64, now time.Time) error {
	if importRunID <= 0 || groupID <= 0 {
		return nil
	}
	return linkReviewGroupToImportRunTx(ctx, tx, importRunID, groupID, now)
}
