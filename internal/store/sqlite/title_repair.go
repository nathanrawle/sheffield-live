package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

type EventTitleRepairReport struct {
	DryRun                            bool                     `json:"dry_run"`
	Applied                           bool                     `json:"applied"`
	Repaired                          int                      `json:"repaired"`
	EventReviewClustersCreated        int                      `json:"event_review_clusters_created"`
	EventReviewClustersReused         int                      `json:"event_review_clusters_reused"`
	EventReviewClustersTerminalReused int                      `json:"event_review_clusters_terminal_reused"`
	Unchanged                         int                      `json:"unchanged"`
	Skipped                           int                      `json:"skipped"`
	RepairRunID                       int64                    `json:"repair_run_id,omitempty"`
	Changes                           []EventTitleRepairChange `json:"changes"`
}

type EventTitleRepairChange struct {
	Result                   string `json:"result"`
	Reason                   string `json:"reason,omitempty"`
	MatchKind                string `json:"match_kind,omitempty"`
	EventID                  int64  `json:"event_id,omitempty"`
	EventReviewClusterID     int64  `json:"event_review_cluster_id,omitempty"`
	EventReviewClusterStatus string `json:"event_review_cluster_status,omitempty"`
	SourceName               string `json:"source_name,omitempty"`
	SourceURL                string `json:"source_url,omitempty"`
	OldSlug                  string `json:"old_slug,omitempty"`
	NewSlug                  string `json:"new_slug,omitempty"`
	OldTitle                 string `json:"old_title,omitempty"`
	NewTitle                 string `json:"new_title,omitempty"`
}

const (
	eventTitleRepairStagingKeyVersion                       = 1
	eventTitleRepairConflictType                            = "title_repair"
	eventTitleRepairConflictReasonSupportingCleanTitle      = "supporting_clean_title"
	eventTitleRepairConflictReasonAuthoritativeSlugConflict = "authoritative_slug_conflict"
	eventTitleRepairRunStatusRunning                        = historicalDuplicateRepairRunStatusRunning
	eventTitleRepairRunStatusSucceeded                      = historicalDuplicateRepairRunStatusSucceeded
	eventTitleRepairRunStatusCompletedWithErrors            = historicalDuplicateRepairRunStatusCompletedWithErrors
	eventTitleRepairCleanedProvenance                       = "Cleaned title from title repair"
	eventTitleRepairAuthoritativeCleanedProvenance          = "Authoritative cleaned title from title repair"
	eventTitleRepairExistingCleanProvenance                 = "Existing live event with cleaned title"
	eventTitleRepairCanonicalProvenance                     = "Canonical live event snapshot"
)

type titleRepairRunEnsurer func() (int64, error)

func (s *Store) RepairEventTitlesFromReport(ctx context.Context, catalog *ingest.Catalog, report ingest.Report, apply bool) (repair EventTitleRepairReport, err error) {
	repair = EventTitleRepairReport{
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
	var repairRunID int64
	ensureRepairRun := func() (int64, error) {
		if !apply {
			return 0, nil
		}
		if repairRunID > 0 {
			return repairRunID, nil
		}
		startedAt := time.Now().UTC()
		runID, createErr := s.createHistoricalDuplicateRepairRun(ctx, startedAt, eventTitleRepairRunStartNotes())
		if createErr != nil {
			return 0, createErr
		}
		repairRunID = runID
		return repairRunID, nil
	}
	defer func() {
		repair.RepairRunID = repairRunID
		if repairRunID <= 0 {
			return
		}
		status := eventTitleRepairRunStatusSucceeded
		if err != nil {
			status = eventTitleRepairRunStatusCompletedWithErrors
		}
		if finishErr := s.finishHistoricalDuplicateRepairRun(ctx, repairRunID, status, eventTitleRepairRunFinishNotes(status)); finishErr != nil && err == nil {
			err = finishErr
		}
	}()

	clusters := ingest.ReviewClustersFromReportWithCatalog(catalog, report)
	for _, cluster := range clusters {
		for _, candidate := range cluster.Candidates {
			single := singleCandidateTitleRepairCluster(cluster, candidate)
			event, err := singletonResolvedEventFromReviewStageClusterInput(single, now)
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
				change, err = s.repairAuthoritativeEventTitle(ctx, single, event, apply, now, ensureRepairRun)
			} else {
				change, err = s.stageSupportingEventTitleRepair(ctx, single, event, apply, now, ensureRepairRun)
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
	case "event_review_created", "would_create_event_review":
		r.EventReviewClustersCreated++
	case "event_review_reused":
		r.EventReviewClustersReused++
	case "event_review_terminal_reused":
		r.EventReviewClustersTerminalReused++
	case "unchanged":
		r.Unchanged++
	default:
		r.Skipped++
	}
}

func singleCandidateTitleRepairCluster(cluster ingest.ReviewStageClusterInput, candidate review.CandidateInput) ingest.ReviewStageClusterInput {
	cluster.Candidates = []review.CandidateInput{candidate}
	return cluster
}

func (s *Store) repairAuthoritativeEventTitle(ctx context.Context, cluster ingest.ReviewStageClusterInput, incoming domain.Event, apply bool, now time.Time, ensureRepairRun titleRepairRunEnsurer) (EventTitleRepairChange, error) {
	change := titleRepairChangeForIncoming(incoming)
	change.MatchKind = "authoritative"
	authoritative, ok := reviewStageClusterAuthoritativeSource(cluster)
	if !ok {
		authoritative = reviewGroupAuthoritativeLink{
			SourceName:     strings.TrimSpace(incoming.SourceName),
			SourceURL:      strings.TrimSpace(incoming.SourceURL),
			SourceEventKey: strings.TrimSpace(authoritativeSourceEventKeyFromClusterInput(cluster)),
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
		separated, err := hasActiveEventReviewSeparationBetweenKeysTx(ctx, tx, seedstore.EventReviewSeparationEventEndpointKey(record.ID), seedstore.EventReviewSeparationEventEndpointKey(conflict.ID))
		if err != nil {
			return EventTitleRepairChange{}, err
		}
		if separated {
			change.Result = "skipped"
			change.Reason = "target slug conflict is already marked separate"
			return change, nil
		}
		if !apply {
			change.Result = "would_create_event_review"
			return change, nil
		}
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return EventTitleRepairChange{}, err
		}
		if _, err := ensureRepairRun(); err != nil {
			return EventTitleRepairChange{}, err
		}
		stageInput, err := buildEventTitleRepairClusterInput(ctx, s.db, cluster, record, incoming, sourceID, authoritative, eventTitleRepairConflictReasonAuthoritativeSlugConflict, &conflict, now)
		if err != nil {
			return EventTitleRepairChange{}, err
		}
		result, err := s.stageEventTitleRepairCluster(ctx, ensureRepairRun, stageInput)
		if err != nil {
			return EventTitleRepairChange{}, err
		}
		change.EventReviewClusterID = result.ClusterID
		change.EventReviewClusterStatus = string(result.Status)
		if result.TerminalReused {
			change.Result = "event_review_terminal_reused"
		} else if result.Created {
			change.Result = "event_review_created"
		} else {
			change.Result = "event_review_reused"
		}
		return change, nil
	}
	if !apply {
		change.Result = "would_repair"
		return change, nil
	}

	sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentityAuthoritative, incoming.SourceName, incoming.SourceURL, authoritative.SourceName, authoritative.SourceURL, authoritative.SourceEventKey, cluster.Candidates[0], "title_repair_authoritative_apply")
	writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, record.ID, sourceID, sourceCtx, sourceLinkAuthorityAuthoritative, sourceLinkConflictPolicyNoMove, now)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if writeResult.Ambiguous {
		change.Result = "skipped"
		change.Reason = "ambiguous authoritative source link"
		return change, nil
	}
	if err := updateEventTitleTx(ctx, tx, record.ID, incoming.Slug, incoming.Name, now); err != nil {
		return EventTitleRepairChange{}, err
	}
	if err := tx.Commit(); err != nil {
		return EventTitleRepairChange{}, err
	}
	change.Result = "repaired"
	return change, nil
}

func (s *Store) stageSupportingEventTitleRepair(ctx context.Context, cluster ingest.ReviewStageClusterInput, incoming domain.Event, apply bool, now time.Time, ensureRepairRun titleRepairRunEnsurer) (EventTitleRepairChange, error) {
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
	var conflict *eventRecord
	if conflictRecord, ok, err := loadEventRecordBySlugTx(ctx, tx, incoming.Slug); err != nil {
		return EventTitleRepairChange{}, err
	} else if ok && conflictRecord.ID != record.ID && eventRecordHasResolvedIdentity(conflictRecord, incoming) {
		separated, err := hasActiveEventReviewSeparationBetweenKeysTx(ctx, tx, seedstore.EventReviewSeparationEventEndpointKey(record.ID), seedstore.EventReviewSeparationEventEndpointKey(conflictRecord.ID))
		if err != nil {
			return EventTitleRepairChange{}, err
		}
		if separated {
			change.Result = "skipped"
			change.Reason = "target slug conflict is already marked separate"
			return change, nil
		}
		conflict = &conflictRecord
	}
	if !apply {
		change.Result = "would_create_event_review"
		return change, nil
	}

	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return EventTitleRepairChange{}, err
	}
	if _, err := ensureRepairRun(); err != nil {
		return EventTitleRepairChange{}, err
	}
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, s.db, cluster.SourceName, cluster.SourceURL)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	if !ok {
		return EventTitleRepairChange{}, errors.New("event title repair source not found")
	}

	stageInput, err := buildEventTitleRepairClusterInput(ctx, s.db, cluster, record, incoming, sourceID, reviewGroupAuthoritativeLink{}, eventTitleRepairConflictReasonSupportingCleanTitle, conflict, now)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	result, err := s.stageEventTitleRepairCluster(ctx, ensureRepairRun, stageInput)
	if err != nil {
		return EventTitleRepairChange{}, err
	}
	change.EventReviewClusterID = result.ClusterID
	change.EventReviewClusterStatus = string(result.Status)
	if result.TerminalReused {
		change.Result = "event_review_terminal_reused"
	} else if result.Created {
		change.Result = "event_review_created"
	} else {
		change.Result = "event_review_reused"
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
	sourceLinkIdentities := reviewGroupAuthoritativeSourceIdentities(reviewGroupAuthoritativeLink{
		SourceURL:      incoming.SourceURL,
		SourceEventKey: sourceEventKey,
	})
	if record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, q, sourceID, sourceLinkIdentities); err != nil {
		return eventRecord{}, "authoritative_link", false, false, err
	} else if ambiguous {
		return eventRecord{}, "authoritative_link", false, true, nil
	} else if ok {
		return record, "authoritative_link", true, false, nil
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

func authoritativeSourceEventKeyLookupCandidates(sourceEventKey string) []string {
	sourceEventKey = strings.TrimSpace(sourceEventKey)
	if sourceEventKey == "" {
		return nil
	}

	candidates := []string{sourceEventKey}
	switch {
	case strings.HasPrefix(sourceEventKey, "uid:"):
		if legacy := strings.TrimSpace(strings.TrimPrefix(sourceEventKey, "uid:")); legacy != "" {
			candidates = append(candidates, legacy)
		}
	case strings.HasPrefix(sourceEventKey, "url:"):
		if legacy := strings.TrimSpace(strings.TrimPrefix(sourceEventKey, "url:")); legacy != "" {
			candidates = append(candidates, legacy)
		}
	}
	return candidates
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
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
	`, strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive), string(domain.PublicationStateWithheld))
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
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
	`, sourceID, strings.TrimSpace(venueSlug), formatRFC3339UTC(start), string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func updateEventTitleTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, slug, name string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE events
		SET slug = ?,
			name = ?,
			last_checked_at = ?
		WHERE id = ?
	`, strings.TrimSpace(slug), strings.TrimSpace(name), formatRFC3339UTC(now), eventID)
	if err != nil {
		return err
	}
	record, ok, err := loadEventRecordByIDTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("event %d not found after title update", eventID)
	}
	return replaceActiveExactIdentityForLiveEventTx(ctx, tx, eventID, record.Event, "title repaired", 0, now)
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

func eventTitleRepairRunStartNotes() string {
	return "event title repair"
}

func eventTitleRepairRunFinishNotes(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "event title repair"
	}
	return "event title repair " + strings.ReplaceAll(status, "_", " ")
}

func (s *Store) stageEventTitleRepairCluster(ctx context.Context, ensureRepairRun titleRepairRunEnsurer, input seedstore.StageRepairEventReviewClusterInput) (seedstore.StageRepairEventReviewClusterResult, error) {
	if ensureRepairRun == nil {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("repair run creator is required")
	}
	repairRunID, err := ensureRepairRun()
	if err != nil {
		return seedstore.StageRepairEventReviewClusterResult{}, err
	}
	if repairRunID <= 0 {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("repair run ID is required")
	}
	input.RunRef = seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: repairRunID}
	return s.StageRepairEventReviewCluster(ctx, input)
}

func buildEventTitleRepairClusterInput(ctx context.Context, q queryer, cluster ingest.ReviewStageClusterInput, record eventRecord, incoming domain.Event, sourceID int64, authoritative reviewGroupAuthoritativeLink, conflictReason string, conflict *eventRecord, now time.Time) (seedstore.StageRepairEventReviewClusterInput, error) {
	if len(cluster.Candidates) == 0 {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("event title repair cluster requires a candidate")
	}
	candidate := cluster.Candidates[0]
	sourceName := strings.TrimSpace(cluster.SourceName)
	sourceURL := strings.TrimSpace(cluster.SourceURL)
	sourceAuthority := seedstore.SourceAuthoritySupporting
	sourceIdentityKeys := reviewCandidateSourceIdentities(candidate).Keys()
	sourceEventKey := strings.TrimSpace(candidate.ExternalID)
	provenance := eventTitleRepairCleanedProvenance
	if authoritative.SourceName != "" && authoritative.SourceURL != "" && authoritative.SourceEventKey != "" {
		sourceName = strings.TrimSpace(authoritative.SourceName)
		sourceURL = strings.TrimSpace(authoritative.SourceURL)
		sourceAuthority = seedstore.SourceAuthorityAuthoritative
		sourceIdentityKeys = reviewGroupAuthoritativeSourceIdentities(authoritative).Keys()
		sourceEventKey = strings.TrimSpace(authoritative.SourceEventKey)
		provenance = eventTitleRepairAuthoritativeCleanedProvenance
	}
	if sourceName == "" || sourceURL == "" {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("event title repair source is required")
	}
	if sourceID <= 0 {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("event title repair source ID is required")
	}

	canonicalSourceID, err := loadTitleRepairEvidenceSourceID(ctx, q, sourceID, record.Event.SourceName, record.Event.SourceURL)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterInput{}, err
	}
	canonicalExactKey, err := titleRepairExactIdentityKey(record.Event)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterInput{}, err
	}
	proposedExactKey := titleRepairProposedExactIdentityKey(record.Event, incoming)

	evidence := make([]seedstore.StageRepairEventReviewEvidenceInput, 0, 3)
	incomingProposedEvent := eventTitleRepairPayloadEventFromDomainEvent(record.ID, incoming)
	incomingPayload := eventTitleRepairEvidencePayload{
		Role:               "incoming_cleaned_title",
		Provenance:         provenance,
		Source:             eventTitleRepairPayloadSource{Name: sourceName, URL: sourceURL, Authority: string(sourceAuthority), EventKey: sourceEventKey},
		SourceIdentityKeys: append([]string(nil), sourceIdentityKeys...),
		ExactIdentityKey:   proposedExactKey,
		OldTitle:           strings.TrimSpace(record.Event.Name),
		NewTitle:           strings.TrimSpace(incoming.Name),
		OldSlug:            strings.TrimSpace(record.Event.Slug),
		NewSlug:            strings.TrimSpace(incoming.Slug),
		ProposedEvent:      &incomingProposedEvent,
	}
	incomingEvidence, err := buildEventTitleRepairEvidenceInput(sourceID, sourceAuthority, incomingPayload, incoming, proposedExactKey)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterInput{}, err
	}
	incomingEvidence.SourceIdentityKeys = append([]string(nil), sourceIdentityKeys...)
	incomingEvidence.ExactIdentityKeys = []string{proposedExactKey}
	evidence = append(evidence, incomingEvidence)

	canonicalPayload := eventTitleRepairEvidencePayload{
		Role:               "canonical_existing_event",
		Provenance:         eventTitleRepairCanonicalProvenance,
		Source:             eventTitleRepairPayloadSource{Name: strings.TrimSpace(record.Event.SourceName), URL: strings.TrimSpace(record.Event.SourceURL), Authority: string(seedstore.SourceAuthorityAuthoritative)},
		SourceIdentityKeys: nil,
		ExactIdentityKey:   canonicalExactKey,
		OldTitle:           strings.TrimSpace(record.Event.Name),
		NewTitle:           strings.TrimSpace(incoming.Name),
		OldSlug:            strings.TrimSpace(record.Event.Slug),
		NewSlug:            strings.TrimSpace(incoming.Slug),
	}
	canonicalCurrentEvent := eventTitleRepairPayloadEventFromDomainEvent(record.ID, record.Event)
	canonicalProposedEvent := eventTitleRepairPayloadEventFromDomainEvent(record.ID, incoming)
	canonicalPayload.CurrentEvent = &canonicalCurrentEvent
	canonicalPayload.ProposedEvent = &canonicalProposedEvent
	canonicalPayload.SourceIdentityKeys = reviewCandidateSourceIdentities(reviewCandidateInputFromEvent(record.Event, "", eventTitleRepairCanonicalProvenance)).Keys()
	canonicalSourceID, err = loadTitleRepairEvidenceSourceID(ctx, q, canonicalSourceID, record.Event.SourceName, record.Event.SourceURL)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterInput{}, err
	}
	canonicalEvidence, err := buildEventTitleRepairEvidenceInput(canonicalSourceID, seedstore.SourceAuthorityAuthoritative, canonicalPayload, record.Event, canonicalExactKey)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterInput{}, err
	}
	canonicalEvidence.EventID = int64Ptr(record.ID)
	canonicalEvidence.SourceIdentityKeys = canonicalPayload.SourceIdentityKeys
	canonicalEvidence.ExactIdentityKeys = []string{canonicalExactKey}
	evidence = append(evidence, canonicalEvidence)

	if conflict != nil {
		conflictExactKey, err := titleRepairExactIdentityKey(conflict.Event)
		if err != nil {
			return seedstore.StageRepairEventReviewClusterInput{}, err
		}
		conflictSourceID, err := loadTitleRepairEvidenceSourceID(ctx, q, sourceID, conflict.Event.SourceName, conflict.Event.SourceURL)
		if err != nil {
			return seedstore.StageRepairEventReviewClusterInput{}, err
		}
		conflictPayload := eventTitleRepairEvidencePayload{
			Role:               "existing_clean_duplicate",
			Provenance:         eventTitleRepairExistingCleanProvenance,
			Source:             eventTitleRepairPayloadSource{Name: strings.TrimSpace(conflict.Event.SourceName), URL: strings.TrimSpace(conflict.Event.SourceURL), Authority: string(seedstore.SourceAuthoritySupporting)},
			SourceIdentityKeys: reviewCandidateSourceIdentities(reviewCandidateInputFromEvent(conflict.Event, "", eventTitleRepairExistingCleanProvenance)).Keys(),
			ExactIdentityKey:   conflictExactKey,
			OldTitle:           strings.TrimSpace(conflict.Event.Name),
			NewTitle:           strings.TrimSpace(incoming.Name),
			OldSlug:            strings.TrimSpace(conflict.Event.Slug),
			NewSlug:            strings.TrimSpace(incoming.Slug),
		}
		conflictCurrentEvent := eventTitleRepairPayloadEventFromDomainEvent(conflict.ID, conflict.Event)
		conflictProposedEvent := eventTitleRepairPayloadEventFromDomainEvent(record.ID, incoming)
		conflictPayload.CurrentEvent = &conflictCurrentEvent
		conflictPayload.ProposedEvent = &conflictProposedEvent
		conflictEvidence, err := buildEventTitleRepairEvidenceInput(conflictSourceID, seedstore.SourceAuthoritySupporting, conflictPayload, conflict.Event, conflictExactKey)
		if err != nil {
			return seedstore.StageRepairEventReviewClusterInput{}, err
		}
		conflictEvidence.EventID = int64Ptr(conflict.ID)
		conflictEvidence.SourceIdentityKeys = conflictPayload.SourceIdentityKeys
		conflictEvidence.ExactIdentityKeys = []string{conflictExactKey}
		evidence = append(evidence, conflictEvidence)
	}

	return seedstore.StageRepairEventReviewClusterInput{
		StagingKey:        eventTitleRepairStagingKey(record.ID, incoming.Name, incoming.Slug),
		StagingKeyVersion: eventTitleRepairStagingKeyVersion,
		ConflictType:      eventTitleRepairConflictType,
		ConflictReason:    strings.TrimSpace(conflictReason),
		CanonicalEventID:  int64Ptr(record.ID),
		Evidence:          evidence,
		CanonicalChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "name", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(record.ID), Value: strings.TrimSpace(record.Event.Name)},
			{FieldName: "venue_slug", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(record.ID), Value: strings.TrimSpace(record.Event.VenueSlug)},
			{FieldName: "start_at", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(record.ID), Value: formatRFC3339UTC(record.Event.Start)},
		},
		DraftChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "name", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: strings.TrimSpace(incoming.Name)},
			{FieldName: "slug", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: strings.TrimSpace(incoming.Slug)},
		},
		LiveActions: []seedstore.EventReviewLiveActionInput{},
	}, nil
}

func buildEventTitleRepairEvidenceInput(sourceID int64, sourceAuthority seedstore.SourceAuthority, payload eventTitleRepairEvidencePayload, event domain.Event, exactIdentityKey string) (seedstore.StageRepairEventReviewEvidenceInput, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return seedstore.StageRepairEventReviewEvidenceInput{}, err
	}
	return seedstore.StageRepairEventReviewEvidenceInput{
		SourceID:            sourceID,
		SourceName:          strings.TrimSpace(payload.Source.Name),
		SourceURL:           strings.TrimSpace(payload.Source.URL),
		SourceAuthority:     sourceAuthority,
		EventID:             nil,
		EvidenceFingerprint: eventTitleRepairEvidenceFingerprint(payload.Role, encoded),
		Payload:             string(encoded),
		WeakEvidence:        false,
		SourceIdentityKeys:  append([]string(nil), payload.SourceIdentityKeys...),
		ExactIdentityKeys:   []string{exactIdentityKey},
	}, nil
}

func eventTitleRepairEvidenceFingerprint(role string, payload []byte) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"event-title-repair-evidence:v1",
		strings.TrimSpace(role),
		string(payload),
	}, "\x1f")))
	return "event-title-repair-evidence:v1:" + hex.EncodeToString(sum[:])
}

func titleRepairExactIdentityKey(event domain.Event) (string, error) {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("event %q cannot produce exact identity material", event.Slug)
	}
	return buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle), nil
}

func titleRepairProposedExactIdentityKey(record domain.Event, incoming domain.Event) string {
	cleanTitle := normalizeExactIdentityCleanTitle(ingest.CleanEventTitleForVenue(incoming.Name, record.VenueSlug))
	return buildExactIdentityKey(exactIdentityKeyVersion, strings.TrimSpace(record.VenueSlug), record.Start.UTC(), cleanTitle)
}

func loadTitleRepairEvidenceSourceID(ctx context.Context, q queryer, fallback int64, sourceName, sourceURL string) (int64, error) {
	sourceName = strings.TrimSpace(sourceName)
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceName == "" || sourceURL == "" {
		return fallback, nil
	}
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, q, sourceName, sourceURL)
	if err != nil {
		return 0, err
	}
	if ok {
		return sourceID, nil
	}
	return fallback, nil
}

type eventTitleRepairEvidencePayload struct {
	Role               string                        `json:"role"`
	Provenance         string                        `json:"provenance"`
	Source             eventTitleRepairPayloadSource `json:"source"`
	SourceIdentityKeys []string                      `json:"source_identity_keys,omitempty"`
	ExactIdentityKey   string                        `json:"exact_identity_key,omitempty"`
	OldTitle           string                        `json:"old_title,omitempty"`
	NewTitle           string                        `json:"new_title,omitempty"`
	OldSlug            string                        `json:"old_slug,omitempty"`
	NewSlug            string                        `json:"new_slug,omitempty"`
	CurrentEvent       *eventTitleRepairPayloadEvent `json:"current_event,omitempty"`
	ProposedEvent      *eventTitleRepairPayloadEvent `json:"proposed_event,omitempty"`
	ConflictEvent      *eventTitleRepairPayloadEvent `json:"conflict_event,omitempty"`
}

type eventTitleRepairPayloadSource struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Authority string `json:"authority"`
	EventKey  string `json:"event_key,omitempty"`
}

type eventTitleRepairPayloadEvent struct {
	ID        int64  `json:"id,omitempty"`
	Slug      string `json:"slug,omitempty"`
	Name      string `json:"name,omitempty"`
	VenueSlug string `json:"venue_slug,omitempty"`
	StartAt   string `json:"start_at,omitempty"`
	EndAt     string `json:"end_at,omitempty"`
}

func eventTitleRepairPayloadEventFromDomainEvent(id int64, event domain.Event) eventTitleRepairPayloadEvent {
	return eventTitleRepairPayloadEvent{
		ID:        id,
		Slug:      strings.TrimSpace(event.Slug),
		Name:      strings.TrimSpace(event.Name),
		VenueSlug: strings.TrimSpace(event.VenueSlug),
		StartAt:   formatRFC3339UTC(event.Start),
		EndAt:     formatOptionalTime(event.End),
	}
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
