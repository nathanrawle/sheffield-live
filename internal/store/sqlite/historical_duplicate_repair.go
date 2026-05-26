package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

const historicalDuplicateRepairMaxWindow = 75 * time.Minute

const (
	historicalDuplicateRepairSourceName        = "Historical duplicate repair"
	historicalDuplicateRepairSourceURL         = "sqlite:historical-duplicate-repair"
	historicalDuplicateRepairNotes             = "historical duplicate repair"
	historicalDuplicateRepairKeyPrefix         = "historical-duplicate:v1:"
	historicalDuplicateRepairStagingKeyVersion = 1
	historicalDuplicateRepairConflictType      = "historical_duplicate"
)

const (
	historicalDuplicateRepairRunStatusRunning             = "running"
	historicalDuplicateRepairRunStatusSucceeded           = "succeeded"
	historicalDuplicateRepairRunStatusCompletedWithErrors = "completed_with_errors"
)

const (
	historicalDuplicateRepairCanonicalBasisSingleReviewedTarget          = "single_reviewed_target"
	historicalDuplicateRepairCanonicalBasisUniqueAuthoritativeSourceLink = "unique_authoritative_source_link"
)

var historicalDuplicateRepairRepeatMarkerPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\brepeat\b(?:[-_/.\s]+)?\bperformance\b(?:[-_/.\s]+)?\bmarker\b`),
	regexp.MustCompile(`\brepeat\b(?:[-_/.\s]+)?\bperformance\b`),
	regexp.MustCompile(`\bearly\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\bearly\b`),
	regexp.MustCompile(`\blate\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\blate\b`),
	regexp.MustCompile(`\bmatinee\b`),
	regexp.MustCompile(`\bafternoon\b`),
	regexp.MustCompile(`\bevening\b`),
	regexp.MustCompile(`\bsecond\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\bfirst\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\badditional\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\bextra\b(?:[-_/.\s]+)?\bshow\b`),
	regexp.MustCompile(`\bextra\b(?:[-_/.\s]+)?\bperformance\b`),
	regexp.MustCompile(`\bnight\b(?:[-_/.\s]+)?\d+\b`),
	regexp.MustCompile(`\bday\b(?:[-_/.\s]+)?\d+\b`),
	regexp.MustCompile(`\bshow\b(?:[-_/.\s]+)?\d+\b`),
}

type HistoricalDuplicateRepairOptions struct {
	Apply      bool
	NearWindow time.Duration
}

type HistoricalDuplicateRepairReport struct {
	DryRun                            bool                              `json:"dry_run"`
	Applied                           bool                              `json:"applied"`
	WindowMinutes                     int                               `json:"window_minutes"`
	Clusters                          int                               `json:"clusters"`
	WouldNormalizeWithheldState       int                               `json:"would_normalize_withheld_state"`
	NormalizedWithheldState           int                               `json:"normalized_withheld_state"`
	WouldWithhold                     int                               `json:"would_withhold"`
	AutoWithheld                      int                               `json:"auto_withheld"`
	AlreadyWithheld                   int                               `json:"already_withheld"`
	EventReviewClustersCreated        int                               `json:"event_review_clusters_created"`
	EventReviewClustersReused         int                               `json:"event_review_clusters_reused"`
	EventReviewClustersTerminalReused int                               `json:"event_review_clusters_terminal_reused"`
	RepairRunID                       int64                             `json:"repair_run_id,omitempty"`
	Skipped                           int                               `json:"skipped"`
	Failed                            int                               `json:"failed"`
	Changes                           []HistoricalDuplicateRepairChange `json:"changes"`
}

type HistoricalDuplicateRepairChange struct {
	Result                   string                               `json:"result"`
	Reason                   string                               `json:"reason,omitempty"`
	Failure                  string                               `json:"failure,omitempty"`
	StagingKey               string                               `json:"staging_key,omitempty"`
	CanonicalEventID         int64                                `json:"canonical_event_id,omitempty"`
	LoserEventIDs            []int64                              `json:"loser_event_ids,omitempty"`
	EvidenceTiers            []string                             `json:"evidence_tiers,omitempty"`
	EventReviewClusterID     int64                                `json:"event_review_cluster_id,omitempty"`
	EventReviewClusterStatus string                               `json:"event_review_cluster_status,omitempty"`
	Candidates               []HistoricalDuplicateRepairCandidate `json:"candidates,omitempty"`
}

type HistoricalDuplicateRepairCandidate struct {
	EventID         int64  `json:"event_id,omitempty"`
	ExistingEventID int64  `json:"existing_event_id,omitempty"`
	Slug            string `json:"slug,omitempty"`
	Title           string `json:"title,omitempty"`
	VenueSlug       string `json:"venue_slug,omitempty"`
	StartAt         string `json:"start_at,omitempty"`
	EvidenceTier    string `json:"evidence_tier,omitempty"`
}

type historicalDuplicateRepairEvent struct {
	record          eventRecord
	exactKey        string
	cleanTitle      string
	titleVariantKey string
	headlinerKey    string
	reviewState     historicalDuplicateRepairState
}

type historicalDuplicateRepairState string

const (
	historicalDuplicateRepairStateReviewed    historicalDuplicateRepairState = "reviewed"
	historicalDuplicateRepairStateProvisional historicalDuplicateRepairState = "provisional"
	historicalDuplicateRepairStateOther       historicalDuplicateRepairState = "other"
)

type historicalDuplicateCluster struct {
	events []historicalDuplicateRepairEvent
}

type historicalDuplicateRepairNormalizationCandidate struct {
	record           eventRecord
	canonicalEventID int64
}

type historicalDuplicateClusterDecision struct {
	change         HistoricalDuplicateRepairChange
	mode           string
	canonicalIndex int
	canonicalBasis string
}

type historicalDuplicateWithholdOptions struct {
	AllowReviewed          bool
	DetachLoserSourceLinks bool
}

func (s *Store) RepairHistoricalDuplicateEvents(ctx context.Context, opts HistoricalDuplicateRepairOptions) (HistoricalDuplicateRepairReport, error) {
	report := HistoricalDuplicateRepairReport{
		DryRun:  !opts.Apply,
		Applied: opts.Apply,
		Changes: []HistoricalDuplicateRepairChange{},
	}
	if s == nil || s.db == nil {
		return report, errors.New("sqlite store is not open")
	}
	window := opts.NearWindow
	if window <= 0 {
		window = historicalDuplicateRepairMaxWindow
	}
	if window > historicalDuplicateRepairMaxWindow {
		return report, fmt.Errorf("historical duplicate near window must not exceed %s", historicalDuplicateRepairMaxWindow)
	}
	report.WindowMinutes = int(window / time.Minute)

	var err error
	var repairRunID int64
	var repairRunNotes string
	if opts.Apply {
		startedAt := time.Now().UTC()
		repairRunNotes = historicalDuplicateRepairRunNotes(startedAt, window)
		repairRunID, err = s.createHistoricalDuplicateRepairRun(ctx, startedAt, repairRunNotes)
		if err != nil {
			return report, err
		}
		report.RepairRunID = repairRunID
	}

	normalizedIDs, err := s.normalizeHistoricalDuplicateRepairWithheldState(ctx, opts.Apply, repairRunID, &report)
	if err != nil {
		return report, err
	}

	events, err := loadHistoricalDuplicateRepairEventsTx(ctx, s.db)
	if err != nil {
		return report, err
	}
	events = filterHistoricalDuplicateRepairEventsByID(events, normalizedIDs)
	clusters := buildHistoricalDuplicateRepairClusters(events, window)
	report.Clusters = len(clusters)

	for _, cluster := range clusters {
		decision, err := s.analyzeHistoricalDuplicateCluster(ctx, cluster, window)
		if err != nil {
			report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
				Result:  "failed",
				Failure: err.Error(),
			})
			continue
		}
		if decision.mode == "skip" {
			report.addHistoricalDuplicateRepairChange(decision.change)
			continue
		}

		if !opts.Apply {
			if decision.mode == "auto" {
				decision.change.Result = "would_withhold"
			} else {
				existing, ok, loadErr := loadHistoricalDuplicateReviewClusterByStagingKeyVersion(ctx, s.db, decision.change.StagingKey)
				if loadErr != nil {
					report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
						Result:  "failed",
						Failure: loadErr.Error(),
					})
					continue
				}
				if ok {
					decision.change.EventReviewClusterID = existing.ID
					decision.change.EventReviewClusterStatus = string(existing.Status)
					if existing.Status == seedstore.EventReviewClusterStatusOpen {
						decision.change.Result = "would_reuse_event_review"
					} else {
						decision.change.Result = "would_reuse_terminal_event_review"
					}
				} else {
					decision.change.Result = "would_create_event_review"
				}
			}
			report.addHistoricalDuplicateRepairChange(decision.change)
			continue
		}

		switch decision.mode {
		case "auto":
			clusterResult, err := s.withholdHistoricalDuplicateCluster(ctx, cluster, decision.canonicalIndex, repairRunID, historicalDuplicateWithholdOptions{
				AllowReviewed:          decision.canonicalBasis == historicalDuplicateRepairCanonicalBasisUniqueAuthoritativeSourceLink,
				DetachLoserSourceLinks: false,
			})
			if err != nil {
				report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
					Result:  "failed",
					Failure: err.Error(),
				})
				continue
			}
			decision.change.Result = clusterResult
			report.addHistoricalDuplicateRepairChange(decision.change)
		case "review":
			stageResult, err := s.stageHistoricalDuplicateEventReviewCluster(ctx, cluster, decision, window, repairRunID)
			if err != nil {
				report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
					Result:  "failed",
					Failure: err.Error(),
				})
				continue
			}
			if stageResult.Created {
				decision.change.Result = "event_review_created"
			} else if stageResult.TerminalReused {
				decision.change.Result = "event_review_terminal_reused"
			} else {
				decision.change.Result = "event_review_reused"
			}
			decision.change.EventReviewClusterID = stageResult.ClusterID
			decision.change.EventReviewClusterStatus = string(stageResult.Status)
			report.addHistoricalDuplicateRepairChange(decision.change)
		default:
			decision.change.Result = "skipped"
			report.addHistoricalDuplicateRepairChange(decision.change)
		}
	}

	if opts.Apply {
		status := historicalDuplicateRepairRunStatusSucceeded
		if report.Failed > 0 {
			status = historicalDuplicateRepairRunStatusCompletedWithErrors
		}
		if err := s.finishHistoricalDuplicateRepairRun(ctx, repairRunID, status, repairRunNotes); err != nil {
			return report, err
		}
	}

	return report, nil
}

func (r *HistoricalDuplicateRepairReport) addHistoricalDuplicateRepairChange(change HistoricalDuplicateRepairChange) {
	r.Changes = append(r.Changes, change)
	switch change.Result {
	case "would_normalize_withheld_state":
		r.WouldNormalizeWithheldState++
	case "normalized_withheld_state":
		r.NormalizedWithheldState++
	case "would_withhold":
		r.WouldWithhold++
	case "auto_withheld":
		r.AutoWithheld++
	case "already_withheld":
		r.AlreadyWithheld++
	case "would_create_event_review":
		r.EventReviewClustersCreated++
	case "would_reuse_event_review":
		r.EventReviewClustersReused++
	case "would_reuse_terminal_event_review":
		r.EventReviewClustersTerminalReused++
	case "event_review_created":
		r.EventReviewClustersCreated++
	case "event_review_reused":
		r.EventReviewClustersReused++
	case "event_review_terminal_reused":
		r.EventReviewClustersTerminalReused++
	case "skipped":
		r.Skipped++
	case "failed":
		r.Failed++
	}
}

func (s *Store) normalizeHistoricalDuplicateRepairWithheldState(ctx context.Context, apply bool, repairRunID int64, report *HistoricalDuplicateRepairReport) (map[int64]struct{}, error) {
	candidates, err := loadHistoricalDuplicateRepairNormalizationCandidatesTx(ctx, s.db)
	if err != nil {
		return nil, err
	}
	excluded := make(map[int64]struct{}, len(candidates))
	for _, candidate := range candidates {
		excluded[candidate.record.ID] = struct{}{}
		finalCanonicalID, err := historicalDuplicateRepairResolveCanonicalChainTx(ctx, s.db, candidate.record.ID, candidate.canonicalEventID)
		if err != nil {
			report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
				Result:  "failed",
				Failure: fmt.Sprintf("event %q: %v", candidate.record.Event.Slug, err),
			})
			continue
		}
		change := HistoricalDuplicateRepairChange{
			CanonicalEventID: finalCanonicalID,
			LoserEventIDs:    []int64{candidate.record.ID},
			Reason:           "historical duplicate canonical chain normalization",
		}
		if !apply {
			change.Result = "would_normalize_withheld_state"
			report.addHistoricalDuplicateRepairChange(change)
			continue
		}
		if err := s.normalizeHistoricalDuplicateRepairWithheldStateTx(ctx, candidate.record.ID, candidate.record.Event.Slug, finalCanonicalID, repairRunID); err != nil {
			report.addHistoricalDuplicateRepairChange(HistoricalDuplicateRepairChange{
				Result:  "failed",
				Failure: fmt.Sprintf("event %q: %v", candidate.record.Event.Slug, err),
			})
			continue
		}
		change.Result = "normalized_withheld_state"
		report.addHistoricalDuplicateRepairChange(change)
	}
	return excluded, nil
}

func loadHistoricalDuplicateRepairNormalizationCandidatesTx(ctx context.Context, q queryer) ([]historicalDuplicateRepairNormalizationCandidate, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			e.id,
			e.slug,
			e.canonical_event_id
		FROM events e
		WHERE e.origin = ?
			AND e.canonical_event_id IS NOT NULL
			AND TRIM(COALESCE(e.publication_state, '')) <> ?
		ORDER BY e.id
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]historicalDuplicateRepairNormalizationCandidate, 0)
	for rows.Next() {
		var candidate historicalDuplicateRepairNormalizationCandidate
		if err := rows.Scan(&candidate.record.ID, &candidate.record.Event.Slug, &candidate.canonicalEventID); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.record.ID)
	}
	records, err := loadHistoricalDuplicateRepairEventRecordsByIDTx(ctx, q, ids)
	if err != nil {
		return nil, err
	}
	recordsByID := make(map[int64]eventRecord, len(records))
	for _, record := range records {
		recordsByID[record.ID] = record
	}
	for i := range candidates {
		record, ok := recordsByID[candidates[i].record.ID]
		if !ok {
			return nil, fmt.Errorf("normalization candidate %d not found", candidates[i].record.ID)
		}
		candidates[i].record = record
	}
	return candidates, nil
}

func loadHistoricalDuplicateRepairEventRecordsByIDTx(ctx context.Context, q queryer, eventIDs []int64) ([]eventRecord, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, 0, len(eventIDs))
	args := make([]any, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		if eventID <= 0 {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, eventID)
	}
	if len(placeholders) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
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
		WHERE e.id IN (%s)
	`, strings.Join(placeholders, ","))
	return loadEventRecords(ctx, q, query, args...)
}

func (s *Store) normalizeHistoricalDuplicateRepairWithheldStateTx(ctx context.Context, eventID int64, slug string, canonicalID, repairRunID int64) error {
	if eventID <= 0 {
		return errors.New("event ID is required")
	}
	if canonicalID <= 0 {
		return errors.New("canonical event ID is required")
	}
	if repairRunID <= 0 {
		return errors.New("repair run ID is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE events
		SET publication_state = ?,
			withheld_reason = ?,
			canonical_event_id = ?,
			withheld_repair_run_id = ?
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), "historical duplicate listing", canonicalID, nullableRepairRunID(repairRunID), eventID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event %d not found", eventID)
	}
	if err := historicalDuplicateRepairEnsureExactIdentitiesInactive(ctx, tx, eventID, repairRunID, now); err != nil {
		return err
	}
	if err := historicalDuplicateRepairEnsureSlugAliasTx(ctx, tx, slug, canonicalID, repairRunID, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

type historicalDuplicateRepairCanonicalChainRow struct {
	ID               int64
	Slug             string
	Origin           string
	PublicationState string
	CanonicalEventID sql.NullInt64
}

func loadHistoricalDuplicateRepairCanonicalChainRowTx(ctx context.Context, q queryer, eventID int64) (historicalDuplicateRepairCanonicalChainRow, bool, error) {
	if eventID <= 0 {
		return historicalDuplicateRepairCanonicalChainRow{}, false, errors.New("event ID is required")
	}
	row := q.QueryRowContext(ctx, `
		SELECT
			e.id,
			e.slug,
			e.origin,
			e.publication_state,
			e.canonical_event_id
		FROM events e
		WHERE e.id = ?
	`, eventID)
	var result historicalDuplicateRepairCanonicalChainRow
	switch err := row.Scan(&result.ID, &result.Slug, &result.Origin, &result.PublicationState, &result.CanonicalEventID); {
	case errors.Is(err, sql.ErrNoRows):
		return historicalDuplicateRepairCanonicalChainRow{}, false, nil
	case err != nil:
		return historicalDuplicateRepairCanonicalChainRow{}, false, err
	default:
		return result, true, nil
	}
}

func historicalDuplicateRepairResolveCanonicalChainTx(ctx context.Context, q queryer, eventID, canonicalEventID int64) (int64, error) {
	if eventID <= 0 {
		return 0, errors.New("event ID is required")
	}
	if canonicalEventID <= 0 {
		return 0, fmt.Errorf("event %d is missing a canonical event", eventID)
	}
	if canonicalEventID == eventID {
		return 0, fmt.Errorf("event %d references itself as canonical event", eventID)
	}

	visited := map[int64]struct{}{
		eventID: struct{}{},
	}
	currentID := canonicalEventID
	for hops := 0; hops < 128; hops++ {
		row, ok, err := loadHistoricalDuplicateRepairCanonicalChainRowTx(ctx, q, currentID)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("event %d references missing canonical event %d", eventID, currentID)
		}
		if row.ID == eventID {
			return 0, fmt.Errorf("event %d references itself as canonical event", eventID)
		}
		if !strings.EqualFold(strings.TrimSpace(row.Origin), string(domain.OriginLive)) {
			return 0, fmt.Errorf("canonical event %d is not live", row.ID)
		}
		if row.CanonicalEventID.Valid && row.CanonicalEventID.Int64 > 0 {
			nextID := row.CanonicalEventID.Int64
			if nextID == row.ID {
				return 0, fmt.Errorf("canonical event %d references itself as canonical event", row.ID)
			}
			if _, seen := visited[nextID]; seen {
				return 0, fmt.Errorf("event %d has an ambiguous canonical chain", eventID)
			}
			visited[row.ID] = struct{}{}
			currentID = nextID
			continue
		}
		if !isLiveNonWithheldEventRow(row.Origin, row.PublicationState) {
			return 0, fmt.Errorf("canonical event %d is not a live public event", row.ID)
		}
		return row.ID, nil
	}
	return 0, fmt.Errorf("event %d has an excessively deep canonical chain", eventID)
}

func filterHistoricalDuplicateRepairEventsByID(events []historicalDuplicateRepairEvent, excluded map[int64]struct{}) []historicalDuplicateRepairEvent {
	if len(events) == 0 || len(excluded) == 0 {
		return events
	}
	filtered := make([]historicalDuplicateRepairEvent, 0, len(events))
	for _, event := range events {
		if _, ok := excluded[event.record.ID]; ok {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func historicalDuplicateRepairEventHasAnyAuthoritativeSourceLinkTx(ctx context.Context, q queryer, eventID int64) (bool, error) {
	if eventID <= 0 {
		return false, errors.New("event ID is required")
	}
	row := q.QueryRowContext(ctx, `
		SELECT 1
		FROM event_source_links
		WHERE event_id = ?
			AND is_authoritative = 1
		LIMIT 1
	`, eventID)
	var dummy int
	switch err := row.Scan(&dummy); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func historicalDuplicateRepairReviewedEventHasUniqueAuthoritativeSourceLinkTx(ctx context.Context, q queryer, eventID int64) (bool, string, error) {
	if eventID <= 0 {
		return false, "", errors.New("event ID is required")
	}
	rows, err := q.QueryContext(ctx, `
		SELECT source_id, source_event_key
		FROM event_source_links
		WHERE event_id = ?
			AND is_authoritative = 1
		ORDER BY id
	`, eventID)
	if err != nil {
		return false, "", err
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		found = true
		var sourceID int64
		var sourceEventKey string
		if err := rows.Scan(&sourceID, &sourceEventKey); err != nil {
			return false, "", err
		}
		resolvedID, resolvedFound, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, q, sourceID, ingest.SourceIdentities(sourceIdentityInputForKey(sourceEventKey)))
		if err != nil {
			return false, "", err
		}
		if ambiguous {
			return false, "authoritative source identity is ambiguous", nil
		}
		if !resolvedFound {
			return false, "authoritative source identity is missing or unresolvable", nil
		}
		if resolvedID != eventID {
			return false, "authoritative source identity resolves to another live event", nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	if !found {
		return false, "", nil
	}
	return true, "", nil
}

func loadHistoricalDuplicateRepairEventsTx(ctx context.Context, q queryer) ([]historicalDuplicateRepairEvent, error) {
	records, err := loadEventRecords(ctx, q, `
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
		WHERE e.origin = ?
			AND TRIM(COALESCE(e.publication_state, '')) <> ?
		ORDER BY v.slug, e.start_at, e.id
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return nil, err
	}

	events := make([]historicalDuplicateRepairEvent, 0, len(records))
	for _, record := range records {
		material, ok, err := exactIdentityMaterialForEvent(record.Event)
		if err != nil {
			return nil, err
		}
		event := historicalDuplicateRepairEvent{record: record}
		if ok {
			event.exactKey = buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle)
		}
		event.cleanTitle = normalizeHistoricalDuplicateCleanTitle(ingest.CleanEventTitleForVenue(record.Event.Name, record.Event.VenueSlug))
		event.titleVariantKey = historicalDuplicateRepairTitleVariantKey(event.cleanTitle)
		event.headlinerKey = historicalDuplicateRepairHeadlinerKey(event.cleanTitle)
		switch normalizedPublicationState(record.Event.PublicationState) {
		case domain.PublicationStateReviewed:
			event.reviewState = historicalDuplicateRepairStateReviewed
		case domain.PublicationStateProvisional:
			event.reviewState = historicalDuplicateRepairStateProvisional
		default:
			event.reviewState = historicalDuplicateRepairStateOther
		}
		events = append(events, event)
	}
	return events, nil
}

func buildHistoricalDuplicateRepairClusters(events []historicalDuplicateRepairEvent, window time.Duration) []historicalDuplicateCluster {
	if len(events) < 2 {
		return nil
	}
	parents := newHistoricalDuplicateRepairUnionFind(len(events))
	byVenue := make(map[string][]int)
	for i, event := range events {
		byVenue[event.record.Event.VenueSlug] = append(byVenue[event.record.Event.VenueSlug], i)
	}

	for _, indexes := range byVenue {
		for i := 0; i < len(indexes); i++ {
			left := events[indexes[i]]
			for j := i + 1; j < len(indexes); j++ {
				right := events[indexes[j]]
				if diff := right.record.Event.Start.Sub(left.record.Event.Start); diff > window {
					break
				}
				if tier, ok := historicalDuplicateEvidenceTier(left, right, window); ok {
					parents.union(indexes[i], indexes[j])
					_ = tier
				}
			}
		}
	}

	componentIndexes := make(map[int][]int)
	for i := range events {
		root := parents.find(i)
		componentIndexes[root] = append(componentIndexes[root], i)
	}

	clusters := make([]historicalDuplicateCluster, 0, len(componentIndexes))
	for _, indexes := range componentIndexes {
		if len(indexes) < 2 {
			continue
		}
		sorted := make([]int, len(indexes))
		copy(sorted, indexes)
		sort.Slice(sorted, func(i, j int) bool {
			left := events[sorted[i]]
			right := events[sorted[j]]
			if left.record.Event.Start.Equal(right.record.Event.Start) {
				return left.record.ID < right.record.ID
			}
			return left.record.Event.Start.Before(right.record.Event.Start)
		})
		clusterEvents := make([]historicalDuplicateRepairEvent, 0, len(sorted))
		for _, index := range sorted {
			clusterEvents = append(clusterEvents, events[index])
		}
		clusters = append(clusters, historicalDuplicateCluster{events: clusterEvents})
	}

	sort.Slice(clusters, func(i, j int) bool {
		left := clusters[i].events[0]
		right := clusters[j].events[0]
		if left.record.Event.VenueSlug == right.record.Event.VenueSlug {
			if left.record.Event.Start.Equal(right.record.Event.Start) {
				return left.record.ID < right.record.ID
			}
			return left.record.Event.Start.Before(right.record.Event.Start)
		}
		return left.record.Event.VenueSlug < right.record.Event.VenueSlug
	})
	return clusters
}

func (s *Store) analyzeHistoricalDuplicateCluster(ctx context.Context, cluster historicalDuplicateCluster, window time.Duration) (historicalDuplicateClusterDecision, error) {
	decision := historicalDuplicateClusterDecision{
		change: HistoricalDuplicateRepairChange{
			LoserEventIDs: []int64{},
			EvidenceTiers: []string{},
			Candidates:    []HistoricalDuplicateRepairCandidate{},
		},
	}
	if len(cluster.events) < 2 {
		decision.mode = "skip"
		decision.change.Result = "skipped"
		decision.change.Reason = "cluster contains fewer than two events"
		return decision, nil
	}
	allSeparated, err := historicalDuplicateRepairClusterAllEventPairsSeparatedTx(ctx, s.db, cluster)
	if err != nil {
		return historicalDuplicateClusterDecision{}, err
	}
	if allSeparated {
		decision.mode = "skip"
		decision.change.Result = "skipped"
		decision.change.Reason = "events already marked separate"
		decision.change.Candidates = historicalDuplicateRepairCandidates(cluster, -1, window)
		return decision, nil
	}

	tiers := historicalDuplicateRepairClusterEvidenceTiers(cluster, window)
	decision.change.EvidenceTiers = tiers
	canonicalIndex, canonicalBasis, selectionReason, err := s.historicalDuplicateRepairCanonicalSelection(ctx, cluster)
	if err != nil {
		return historicalDuplicateClusterDecision{}, err
	}
	decision.canonicalIndex = canonicalIndex
	decision.canonicalBasis = canonicalBasis
	if canonicalIndex < 0 {
		decision.mode = "review"
		decision.change.Reason = selectionReason
	} else if historicalDuplicateRepairClusterHasRepeatMarker(cluster) {
		decision.mode = "review"
		decision.change.Reason = "repeat performance marker"
	} else {
		eligible, reason, err := historicalDuplicateRepairAutoEligibleCluster(ctx, s.db, cluster, canonicalIndex, canonicalBasis, window)
		if err != nil {
			return historicalDuplicateClusterDecision{}, err
		}
		if eligible {
			decision.mode = "auto"
			decision.change.Result = "would_withhold"
		} else {
			decision.mode = "review"
			decision.change.Reason = reason
		}
	}

	decision.change.CanonicalEventID = historicalDuplicateRepairCanonicalEventID(cluster, decision.canonicalIndex)
	decision.change.LoserEventIDs = historicalDuplicateRepairLoserEventIDs(cluster, decision.canonicalIndex)
	decision.change.Candidates = historicalDuplicateRepairCandidates(cluster, decision.canonicalIndex, window)
	if decision.mode == "review" {
		decision.change.StagingKey = historicalDuplicateRepairStagingKey(cluster)
	}
	return decision, nil
}

func (s *Store) historicalDuplicateRepairCanonicalSelection(ctx context.Context, cluster historicalDuplicateCluster) (int, string, string, error) {
	reviewedIndexes := make([]int, 0, len(cluster.events))
	authoritativeIndexes := make([]int, 0, 1)
	authoritativeFailureReason := ""
	for i, event := range cluster.events {
		if event.reviewState != historicalDuplicateRepairStateReviewed {
			continue
		}
		reviewedIndexes = append(reviewedIndexes, i)
		hasAuthoritative, reason, err := historicalDuplicateRepairReviewedEventHasUniqueAuthoritativeSourceLinkTx(ctx, s.db, event.record.ID)
		if err != nil {
			return -1, "", "", err
		}
		if hasAuthoritative {
			authoritativeIndexes = append(authoritativeIndexes, i)
			continue
		}
		if reason != "" && authoritativeFailureReason == "" {
			authoritativeFailureReason = reason
		}
	}
	switch len(authoritativeIndexes) {
	case 1:
		return authoritativeIndexes[0], historicalDuplicateRepairCanonicalBasisUniqueAuthoritativeSourceLink, "", nil
	case 0:
		if authoritativeFailureReason != "" {
			return -1, "", authoritativeFailureReason, nil
		}
		if len(reviewedIndexes) == 1 {
			return reviewedIndexes[0], historicalDuplicateRepairCanonicalBasisSingleReviewedTarget, "", nil
		}
		if len(reviewedIndexes) == 0 {
			return -1, "", "no reviewed target", nil
		}
		return -1, "", "multiple reviewed targets", nil
	default:
		return -1, "", "multiple authoritative targets", nil
	}
}

func historicalDuplicateRepairReviewedTarget(cluster historicalDuplicateCluster) (int, int) {
	reviewedIndex := -1
	reviewedCount := 0
	for i, event := range cluster.events {
		if event.reviewState != historicalDuplicateRepairStateReviewed {
			continue
		}
		reviewedIndex = i
		reviewedCount++
	}
	return reviewedIndex, reviewedCount
}

func historicalDuplicateRepairCanonicalEventID(cluster historicalDuplicateCluster, targetIndex int) int64 {
	if targetIndex < 0 || targetIndex >= len(cluster.events) {
		return 0
	}
	return cluster.events[targetIndex].record.ID
}

func historicalDuplicateRepairLoserEventIDs(cluster historicalDuplicateCluster, targetIndex int) []int64 {
	losers := make([]int64, 0, len(cluster.events))
	for i, event := range cluster.events {
		if i == targetIndex {
			continue
		}
		losers = append(losers, event.record.ID)
	}
	return losers
}

func historicalDuplicateRepairCandidates(cluster historicalDuplicateCluster, targetIndex int, window time.Duration) []HistoricalDuplicateRepairCandidate {
	candidates := make([]HistoricalDuplicateRepairCandidate, 0, len(cluster.events))
	for i, event := range cluster.events {
		candidate := HistoricalDuplicateRepairCandidate{
			EventID:         event.record.ID,
			ExistingEventID: event.record.ID,
			Slug:            event.record.Event.Slug,
			Title:           event.record.Event.Name,
			VenueSlug:       event.record.Event.VenueSlug,
			StartAt:         formatRFC3339UTC(event.record.Event.Start),
		}
		if i == targetIndex {
			candidate.EvidenceTier = "canonical"
		} else if targetIndex >= 0 && targetIndex < len(cluster.events) {
			candidate.EvidenceTier = historicalDuplicateRepairPairEvidenceTier(cluster.events[targetIndex], event, window)
		} else {
			candidate.EvidenceTier = historicalDuplicateRepairBestClusterEvidenceTier(event, cluster.events, window)
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func historicalDuplicateRepairPairEvidenceTier(left, right historicalDuplicateRepairEvent, window time.Duration) string {
	if tier, ok := historicalDuplicateEvidenceTier(left, right, window); ok {
		return tier
	}
	return ""
}

func historicalDuplicateRepairBestClusterEvidenceTier(event historicalDuplicateRepairEvent, cluster []historicalDuplicateRepairEvent, window time.Duration) string {
	bestTier := ""
	bestRank := -1
	for _, other := range cluster {
		if other.record.ID == event.record.ID {
			continue
		}
		tier, ok := historicalDuplicateEvidenceTier(event, other, window)
		if !ok {
			continue
		}
		rank := historicalDuplicateEvidenceTierRank(tier)
		if rank > bestRank {
			bestRank = rank
			bestTier = tier
		}
	}
	return bestTier
}

func historicalDuplicateRepairClusterEvidenceTiers(cluster historicalDuplicateCluster, window time.Duration) []string {
	seen := map[string]struct{}{}
	for i := 0; i < len(cluster.events); i++ {
		for j := i + 1; j < len(cluster.events); j++ {
			tier, ok := historicalDuplicateEvidenceTier(cluster.events[i], cluster.events[j], window)
			if !ok {
				continue
			}
			seen[tier] = struct{}{}
		}
	}
	tiers := make([]string, 0, len(seen))
	for tier := range seen {
		tiers = append(tiers, tier)
	}
	sort.Slice(tiers, func(i, j int) bool {
		return historicalDuplicateEvidenceTierRank(tiers[i]) < historicalDuplicateEvidenceTierRank(tiers[j])
	})
	return tiers
}

func historicalDuplicateEvidenceTier(left, right historicalDuplicateRepairEvent, window time.Duration) (string, bool) {
	if left.record.Event.VenueSlug != right.record.Event.VenueSlug {
		return "", false
	}
	diff := right.record.Event.Start.Sub(left.record.Event.Start)
	if diff < 0 {
		diff = -diff
	}
	if diff > window {
		return "", false
	}
	if left.exactKey != "" && left.exactKey == right.exactKey {
		return "exact_identity", true
	}
	if left.cleanTitle != "" && left.cleanTitle == right.cleanTitle {
		return "clean_title_near", true
	}
	if left.titleVariantKey != "" && left.titleVariantKey == right.titleVariantKey {
		return "title_variant_near", true
	}
	if left.headlinerKey != "" && left.headlinerKey == right.headlinerKey {
		return "headliner_near", true
	}
	return "", false
}

func historicalDuplicateEvidenceTierRank(tier string) int {
	switch tier {
	case "exact_identity":
		return 0
	case "clean_title_near":
		return 1
	case "title_variant_near":
		return 2
	case "headliner_near":
		return 3
	default:
		return 4
	}
}

func historicalDuplicateRepairClusterHasRepeatMarker(cluster historicalDuplicateCluster) bool {
	for _, event := range cluster.events {
		text := strings.ToLower(strings.Join([]string{
			event.record.Event.Slug,
			event.record.Event.Name,
			event.record.Event.Description,
		}, " "))
		for _, pattern := range historicalDuplicateRepairRepeatMarkerPatterns {
			if pattern.MatchString(text) {
				return true
			}
		}
	}
	return false
}

func historicalDuplicateRepairRunNotes(startedAt time.Time, window time.Duration) string {
	return fmt.Sprintf("%s started_at=%s window=%dm", historicalDuplicateRepairNotes, formatRFC3339UTC(startedAt), int(window/time.Minute))
}

func (s *Store) createHistoricalDuplicateRepairRun(ctx context.Context, startedAt time.Time, notes string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("sqlite store is not open")
	}
	if startedAt.IsZero() {
		return 0, errors.New("repair run started time is required")
	}
	notes = strings.TrimSpace(notes)
	if notes == "" {
		return 0, errors.New("repair run notes are required")
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO repair_runs (
			started_at,
			status,
			notes
		) VALUES (?, ?, ?)
	`, formatRFC3339UTC(startedAt), historicalDuplicateRepairRunStatusRunning, notes)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) finishHistoricalDuplicateRepairRun(ctx context.Context, repairRunID int64, status, notes string) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if repairRunID <= 0 {
		return errors.New("repair run ID is required")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("repair run status is required")
	}
	notes = strings.TrimSpace(notes)
	if notes == "" {
		return errors.New("repair run notes are required")
	}

	finishedAt := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE repair_runs
		SET finished_at = ?,
			status = ?,
			notes = ?
		WHERE id = ?
	`, formatRFC3339UTC(finishedAt), status, notes, repairRunID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("repair run %d not found", repairRunID)
	}
	return nil
}

func finishHistoricalDuplicateRepairRunTx(ctx context.Context, tx execer, repairRunID int64, status, notes string) error {
	if repairRunID <= 0 {
		return errors.New("repair run ID is required")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("repair run status is required")
	}
	notes = strings.TrimSpace(notes)
	if notes == "" {
		return errors.New("repair run notes are required")
	}

	finishedAt := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE repair_runs
		SET finished_at = ?,
			status = ?,
			notes = ?
		WHERE id = ?
	`, formatRFC3339UTC(finishedAt), status, notes, repairRunID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("repair run %d not found", repairRunID)
	}
	return nil
}

func historicalDuplicateRepairAutoEligibleCluster(ctx context.Context, q queryer, cluster historicalDuplicateCluster, canonicalIndex int, canonicalBasis string, window time.Duration) (bool, string, error) {
	if canonicalIndex < 0 || canonicalIndex >= len(cluster.events) {
		return false, "no reviewed target", nil
	}
	canonical := cluster.events[canonicalIndex]
	if canonical.reviewState != historicalDuplicateRepairStateReviewed {
		return false, "canonical target is not reviewed", nil
	}
	for i, loser := range cluster.events {
		if i == canonicalIndex {
			continue
		}
		switch canonicalBasis {
		case historicalDuplicateRepairCanonicalBasisSingleReviewedTarget:
			if loser.reviewState != historicalDuplicateRepairStateProvisional {
				return false, "loser is not provisional", nil
			}
		case historicalDuplicateRepairCanonicalBasisUniqueAuthoritativeSourceLink:
			if loser.reviewState != historicalDuplicateRepairStateProvisional && loser.reviewState != historicalDuplicateRepairStateReviewed {
				return false, "loser is not provisional", nil
			}
			if loser.reviewState == historicalDuplicateRepairStateReviewed {
				hasAuthoritative, err := historicalDuplicateRepairEventHasAnyAuthoritativeSourceLinkTx(ctx, q, loser.record.ID)
				if err != nil {
					return false, "", err
				}
				if hasAuthoritative {
					return false, "reviewed loser has authoritative source link", nil
				}
			}
		default:
			return false, "historical duplicate cluster has no canonical target", nil
		}
		tier := historicalDuplicateRepairPairEvidenceTier(canonical, loser, window)
		if tier != "exact_identity" && tier != "clean_title_near" && tier != "title_variant_near" {
			return false, "loser has review-only evidence", nil
		}
		if reason, reviewNeeded, err := historicalDuplicateRepairSourceLinkGuardTx(ctx, q, loser.record.ID, canonical.record.ID); err != nil {
			return false, "", err
		} else if reviewNeeded {
			return false, reason, nil
		}
		if reason, reviewNeeded, err := historicalDuplicateRepairAliasConflictTx(ctx, q, loser.record.Event.Slug, canonical.record.ID); err != nil {
			return false, "", err
		} else if reviewNeeded {
			return false, reason, nil
		}
	}
	return true, "", nil
}

func historicalDuplicateRepairSourceLinkGuardTx(ctx context.Context, q queryer, loserID, canonicalID int64) (string, bool, error) {
	if loserID <= 0 {
		return "", false, errors.New("loser event ID is required")
	}
	if canonicalID <= 0 {
		return "", false, errors.New("canonical event ID is required")
	}
	var rows *sql.Rows
	var err error
	rows, err = q.QueryContext(ctx, `
		SELECT source_id, source_event_key, is_authoritative
		FROM event_source_links
		WHERE event_id = ?
		ORDER BY id
	`, loserID)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()

	for rows.Next() {
		var sourceID int64
		var sourceEventKey string
		var authoritative int
		if err := rows.Scan(&sourceID, &sourceEventKey, &authoritative); err != nil {
			return "", false, err
		}
		resolvedID, found, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, q, sourceID, ingest.SourceIdentities(sourceIdentityInputForKey(sourceEventKey)))
		if err != nil {
			return "", false, err
		}
		if ambiguous || !found {
			return "source identity resolves to another live event", true, nil
		}
		if authoritative == 1 {
			if resolvedID != canonicalID {
				return "authoritative source identity does not resolve to canonical", true, nil
			}
			continue
		}
		if resolvedID != loserID && resolvedID != canonicalID {
			return "source identity resolves to another live event", true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

func (s *Store) withholdHistoricalDuplicateCluster(ctx context.Context, cluster historicalDuplicateCluster, canonicalIndex int, repairRunID int64, opts historicalDuplicateWithholdOptions) (string, error) {
	if len(cluster.events) < 2 {
		return "skipped", nil
	}
	if repairRunID <= 0 {
		return "", errors.New("repair run ID is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()

	if canonicalIndex < 0 || canonicalIndex >= len(cluster.events) {
		return "", fmt.Errorf("auto-withhold requires a canonical target")
	}
	target := cluster.events[canonicalIndex]
	if target.reviewState != historicalDuplicateRepairStateReviewed {
		return "", fmt.Errorf("auto-withhold requires a reviewed canonical target")
	}

	for i, loser := range cluster.events {
		if i == canonicalIndex {
			continue
		}
		result, err := withholdHistoricalDuplicateEventTx(ctx, tx, loser.record.ID, target.record.ID, repairRunID, now, opts)
		if err != nil {
			return "", err
		}
		if result == "already_withheld" {
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}
	return "auto_withheld", nil
}

func withholdHistoricalDuplicateEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, loserID, canonicalID, repairRunID int64, now time.Time, opts historicalDuplicateWithholdOptions) (string, error) {
	if loserID <= 0 {
		return "", errors.New("loser event ID is required")
	}
	if canonicalID <= 0 {
		return "", errors.New("canonical event ID is required")
	}
	loserRecord, ok, err := loadEventRecordByIDTx(ctx, tx, loserID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("loser event %d not found", loserID)
	}
	targetRecord, ok, err := loadEventRecordByIDTx(ctx, tx, canonicalID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("canonical event %d not found", canonicalID)
	}
	if !isLiveNonWithheldEventRow(string(targetRecord.Event.Origin), string(targetRecord.Event.PublicationState)) {
		return "", fmt.Errorf("canonical event %d is not a reviewed live event", canonicalID)
	}
	if normalizedPublicationState(targetRecord.Event.PublicationState) != domain.PublicationStateReviewed {
		return "", fmt.Errorf("canonical event %d is not reviewed", canonicalID)
	}

	currentPublicationState := normalizedPublicationState(loserRecord.Event.PublicationState)
	currentCanonicalID := historicalDuplicateRepairCanonicalEventIDFromDB(ctx, tx, loserID)
	if currentPublicationState == domain.PublicationStateWithheld {
		if currentCanonicalID != canonicalID {
			return "", fmt.Errorf("loser event %d is already withheld to event %d", loserID, currentCanonicalID)
		}
		if opts.DetachLoserSourceLinks {
			if err := detachHistoricalDuplicateLoserSourceLinksTx(ctx, tx, loserID); err != nil {
				return "", err
			}
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET publication_state = ?,
				withheld_reason = ?,
				canonical_event_id = ?,
				withheld_repair_run_id = ?
			WHERE id = ?
		`, string(domain.PublicationStateWithheld), "historical duplicate listing", canonicalID, nullableRepairRunID(repairRunID), loserID); err != nil {
			return "", err
		}
		if err := historicalDuplicateRepairEnsureExactIdentitiesInactive(ctx, tx, loserID, repairRunID, now); err != nil {
			return "", err
		}
		if err := historicalDuplicateRepairEnsureSlugAliasTx(ctx, tx, loserRecord.Event.Slug, canonicalID, repairRunID, now); err != nil {
			return "", err
		}
		return "already_withheld", nil
	}
	if currentPublicationState == domain.PublicationStateReviewed && !opts.AllowReviewed {
		return "", fmt.Errorf("loser event %d is not provisional", loserID)
	}
	if currentPublicationState != domain.PublicationStateProvisional && currentPublicationState != domain.PublicationStateReviewed {
		return "", fmt.Errorf("loser event %d is not provisional", loserID)
	}
	if reason, reviewNeeded, err := historicalDuplicateRepairSourceLinkGuardTx(ctx, tx, loserID, canonicalID); err != nil {
		return "", err
	} else if reviewNeeded {
		return "", errors.New(reason)
	}
	if reason, reviewNeeded, err := historicalDuplicateRepairAliasConflictTx(ctx, tx, loserRecord.Event.Slug, canonicalID); err != nil {
		return "", err
	} else if reviewNeeded {
		return "", errors.New(reason)
	}
	if opts.DetachLoserSourceLinks {
		if err := detachHistoricalDuplicateLoserSourceLinksTx(ctx, tx, loserID); err != nil {
			return "", err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET publication_state = ?,
			withheld_reason = ?,
			canonical_event_id = ?,
			withheld_repair_run_id = ?
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), "historical duplicate listing", canonicalID, nullableRepairRunID(repairRunID), loserID); err != nil {
		return "", err
	}
	if err := historicalDuplicateRepairEnsureExactIdentitiesInactive(ctx, tx, loserID, repairRunID, now); err != nil {
		return "", err
	}
	if err := historicalDuplicateRepairEnsureSlugAliasTx(ctx, tx, loserRecord.Event.Slug, canonicalID, repairRunID, now); err != nil {
		return "", err
	}
	return "auto_withheld", nil
}

func detachHistoricalDuplicateLoserSourceLinksTx(ctx context.Context, tx execer, loserID int64) error {
	if loserID <= 0 {
		return errors.New("loser event ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_links
		WHERE event_id = ?
	`, loserID)
	return err
}

func createHistoricalDuplicateRepairRunTx(ctx context.Context, tx execer, startedAt time.Time, notes string) (int64, error) {
	if startedAt.IsZero() {
		return 0, errors.New("repair run started time is required")
	}
	notes = strings.TrimSpace(notes)
	if notes == "" {
		return 0, errors.New("repair run notes are required")
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO repair_runs (
			started_at,
			status,
			notes
		) VALUES (?, ?, ?)
	`, formatRFC3339UTC(startedAt), historicalDuplicateRepairRunStatusRunning, notes)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func historicalDuplicateRepairCanonicalEventIDFromDB(ctx context.Context, q queryer, loserID int64) int64 {
	var canonicalID sql.NullInt64
	if err := q.QueryRowContext(ctx, `
		SELECT canonical_event_id
		FROM events
		WHERE id = ?
	`, loserID).Scan(&canonicalID); err != nil {
		return 0
	}
	if canonicalID.Valid {
		return canonicalID.Int64
	}
	return 0
}

func historicalDuplicateRepairEnsureExactIdentitiesInactive(ctx context.Context, tx interface {
	execer
	queryer
}, loserID int64, repairRunID int64, now time.Time) error {
	rows, err := loadActiveExactIdentityRowsByEventTx(ctx, tx, loserID)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return deactivateActiveExactIdentitiesForEventTx(ctx, tx, loserID, "historical duplicate listing", repairRunID, now)
}

func historicalDuplicateRepairEnsureSlugAliasTx(ctx context.Context, tx interface {
	execer
	queryer
}, aliasSlug string, canonicalID int64, repairRunID int64, now time.Time) error {
	aliasSlug = strings.TrimSpace(aliasSlug)
	if aliasSlug == "" {
		return errors.New("alias slug is required")
	}
	if canonicalID <= 0 {
		return errors.New("canonical event ID is required")
	}

	var existingTargetID int64
	err := tx.QueryRowContext(ctx, `
		SELECT target_event_id
		FROM slug_aliases
		WHERE alias_slug = ?
			AND target_kind = ?
		LIMIT 1
	`, aliasSlug, string(seedstore.SlugAliasTargetKindEvent)).Scan(&existingTargetID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `
			INSERT INTO slug_aliases (
				alias_slug,
				target_kind,
				target_event_id,
				target_venue_id,
				repair_run_id,
				reason,
				created_at,
				updated_at
			) VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
		`, aliasSlug, string(seedstore.SlugAliasTargetKindEvent), canonicalID, nullableRepairRunID(repairRunID), "historical duplicate listing", formatRFC3339UTC(now), formatRFC3339UTC(now))
		return err
	case err != nil:
		return err
	case existingTargetID != canonicalID:
		return fmt.Errorf("slug alias %q already points to event %d", aliasSlug, existingTargetID)
	default:
		_, err = tx.ExecContext(ctx, `
			UPDATE slug_aliases
			SET target_event_id = ?,
				repair_run_id = CASE WHEN ? > 0 THEN ? ELSE repair_run_id END,
				reason = ?,
				updated_at = ?
			WHERE alias_slug = ?
				AND target_kind = ?
		`, canonicalID, repairRunID, repairRunID, "historical duplicate listing", formatRFC3339UTC(now), aliasSlug, string(seedstore.SlugAliasTargetKindEvent))
		return err
	}
}

func historicalDuplicateRepairAliasConflictTx(ctx context.Context, tx queryer, aliasSlug string, canonicalID int64) (string, bool, error) {
	aliasSlug = strings.TrimSpace(aliasSlug)
	if aliasSlug == "" {
		return "", false, nil
	}
	var targetEventID int64
	err := tx.QueryRowContext(ctx, `
		SELECT target_event_id
		FROM slug_aliases
		WHERE alias_slug = ?
			AND target_kind = ?
		LIMIT 1
	`, aliasSlug, string(seedstore.SlugAliasTargetKindEvent)).Scan(&targetEventID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	case targetEventID != canonicalID:
		return "slug alias already points to a different event", true, nil
	default:
		return "", false, nil
	}
}

func historicalDuplicateRepairClusterAllEventPairsSeparatedTx(ctx context.Context, q queryer, cluster historicalDuplicateCluster) (bool, error) {
	if len(cluster.events) < 2 {
		return false, nil
	}
	for i := 0; i < len(cluster.events); i++ {
		for j := i + 1; j < len(cluster.events); j++ {
			separated, err := hasActiveEventReviewSeparationBetweenKeysTx(ctx, q,
				seedstore.EventReviewSeparationEventEndpointKey(cluster.events[i].record.ID),
				seedstore.EventReviewSeparationEventEndpointKey(cluster.events[j].record.ID),
			)
			if err != nil {
				return false, err
			}
			if !separated {
				return false, nil
			}
		}
	}
	return true, nil
}

func historicalDuplicateRepairStagingKey(cluster historicalDuplicateCluster) string {
	ids := make([]int64, 0, len(cluster.events))
	for _, event := range cluster.events {
		ids = append(ids, event.record.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var builder strings.Builder
	builder.WriteString(historicalDuplicateRepairKeyPrefix)
	for i, id := range ids {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(fmt.Sprintf("%d", id))
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return historicalDuplicateRepairKeyPrefix + hex.EncodeToString(sum[:])
}

func loadHistoricalDuplicateReviewClusterByStagingKeyVersion(ctx context.Context, q queryer, stagingKey string) (seedstore.EventReviewCluster, bool, error) {
	return loadEventReviewClusterByStagingKeyVersionTx(ctx, q, stagingKey, historicalDuplicateRepairStagingKeyVersion)
}

func (s *Store) stageHistoricalDuplicateEventReviewCluster(ctx context.Context, cluster historicalDuplicateCluster, decision historicalDuplicateClusterDecision, window time.Duration, repairRunID int64) (seedstore.StageRepairEventReviewClusterResult, error) {
	if repairRunID <= 0 {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("repair run ID is required")
	}
	input, err := s.buildHistoricalDuplicateRepairEventReviewClusterInput(ctx, s.db, cluster, decision, window)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterResult{}, err
	}
	input.RunRef = seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: repairRunID}
	return s.StageRepairEventReviewCluster(ctx, input)
}

func (s *Store) buildHistoricalDuplicateRepairEventReviewClusterInput(ctx context.Context, q queryer, cluster historicalDuplicateCluster, decision historicalDuplicateClusterDecision, window time.Duration) (seedstore.StageRepairEventReviewClusterInput, error) {
	if len(cluster.events) == 0 {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("historical duplicate cluster is empty")
	}
	stagingKey := historicalDuplicateRepairStagingKey(cluster)
	canonicalIndex := decision.canonicalIndex
	var canonicalEventID *int64
	if canonicalIndex >= 0 {
		canonicalEventID = int64Ptr(cluster.events[canonicalIndex].record.ID)
	}

	evidence := make([]seedstore.StageRepairEventReviewEvidenceInput, 0, len(cluster.events))
	for i, event := range cluster.events {
		authority := seedstore.SourceAuthoritySupporting
		if i == canonicalIndex {
			authority = seedstore.SourceAuthorityAuthoritative
		}
		sourceID, sourceName, sourceURL, resolvedAuthority, err := s.historicalDuplicateRepairResolveSource(ctx, q, event, authority)
		if err != nil {
			return seedstore.StageRepairEventReviewClusterInput{}, err
		}
		exactKeys := historicalDuplicateRepairExactIdentityKeys(event.record.Event)
		sourceKeys := historicalDuplicateRepairSourceIdentityKeys(event.record.Event)
		role := "loser"
		if i == canonicalIndex {
			role = "canonical"
		}
		payload := historicalDuplicateRepairEvidencePayload{
			Role:               role,
			EvidenceTier:       historicalDuplicateRepairEvidenceTierForEvent(cluster, i, canonicalIndex, window),
			Reason:             decision.change.Reason,
			Source:             historicalDuplicateRepairEvidenceSource{Name: sourceName, URL: sourceURL, Authority: string(resolvedAuthority)},
			Event:              historicalDuplicateRepairEvidenceSnapshotFromEvent(event.record.ID, event.record.Event),
			SourceIdentityKeys: append([]string(nil), sourceKeys...),
			ExactIdentityKeys:  append([]string(nil), exactKeys...),
			ClusterReason:      decision.change.Reason,
			ReviewState:        string(event.reviewState),
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return seedstore.StageRepairEventReviewClusterInput{}, err
		}
		evidence = append(evidence, seedstore.StageRepairEventReviewEvidenceInput{
			SourceID:            sourceID,
			SourceName:          sourceName,
			SourceURL:           sourceURL,
			SourceAuthority:     resolvedAuthority,
			EventID:             int64Ptr(event.record.ID),
			EvidenceFingerprint: fmt.Sprintf("historical-duplicate-event:%d", event.record.ID),
			Payload:             string(encoded),
			SourceIdentityKeys:  sourceKeys,
			ExactIdentityKeys:   exactKeys,
			WeakEvidence:        false,
		})
	}

	canonicalChoices := []seedstore.EventReviewChoiceInput{}
	liveActions := []seedstore.EventReviewLiveActionInput{}
	if canonicalIndex >= 0 {
		canonical := cluster.events[canonicalIndex]
		canonicalChoices = []seedstore.EventReviewChoiceInput{
			{FieldName: "name", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(canonical.record.ID), Value: strings.TrimSpace(canonical.record.Event.Name)},
			{FieldName: "venue_slug", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(canonical.record.ID), Value: strings.TrimSpace(canonical.record.Event.VenueSlug)},
			{FieldName: "start_at", ChoiceKind: seedstore.EventReviewChoiceKindEvent, EventID: int64Ptr(canonical.record.ID), Value: formatRFC3339UTC(canonical.record.Event.Start)},
		}
		liveActions = append(liveActions, seedstore.EventReviewLiveActionInput{
			EventID: canonical.record.ID,
			Action:  seedstore.EventReviewLiveActionKindKeepSeparate,
			Reason:  "historical duplicate repair keep separate",
		})
		for i, loser := range cluster.events {
			if i == canonicalIndex {
				continue
			}
			liveActions = append(liveActions, seedstore.EventReviewLiveActionInput{
				EventID: loser.record.ID,
				Action:  seedstore.EventReviewLiveActionKindWithholdDuplicate,
				Reason:  "historical duplicate repair withhold duplicate",
			})
		}
	}

	return seedstore.StageRepairEventReviewClusterInput{
		StagingKey:        stagingKey,
		StagingKeyVersion: historicalDuplicateRepairStagingKeyVersion,
		ConflictType:      historicalDuplicateRepairConflictType,
		ConflictReason:    strings.TrimSpace(decision.change.Reason),
		CanonicalEventID:  canonicalEventID,
		Evidence:          evidence,
		CanonicalChoices:  canonicalChoices,
		DraftChoices:      []seedstore.EventReviewChoiceInput{},
		LiveActions:       liveActions,
	}, nil
}

func (s *Store) historicalDuplicateRepairResolveSource(ctx context.Context, q queryer, event historicalDuplicateRepairEvent, authority seedstore.SourceAuthority) (int64, string, string, seedstore.SourceAuthority, error) {
	sourceName := strings.TrimSpace(event.record.Event.SourceName)
	sourceURL := strings.TrimSpace(event.record.Event.SourceURL)
	if sourceName == "" || sourceURL == "" {
		return 0, "", "", seedstore.SourceAuthority(""), errors.New("historical duplicate repair requires existing event source name and url")
	}
	sourceID, ok, err := loadSourceIDByNameURLTx(ctx, q, sourceName, sourceURL)
	if err != nil {
		return 0, "", "", seedstore.SourceAuthority(""), err
	}
	if !ok {
		return 0, "", "", seedstore.SourceAuthority(""), fmt.Errorf("historical duplicate repair source not found for %q %q", sourceName, sourceURL)
	}
	return sourceID, sourceName, sourceURL, authority, nil
}

func historicalDuplicateRepairExactIdentityKeys(event domain.Event) []string {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil || !ok {
		return nil
	}
	return []string{buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle)}
}

func historicalDuplicateRepairSourceIdentityKeys(event domain.Event) []string {
	return reviewCandidateSourceIdentities(reviewCandidateInputFromEvent(event, "", historicalDuplicateRepairNotes)).Keys()
}

type historicalDuplicateRepairEvidenceSource struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Authority string `json:"authority"`
}

type historicalDuplicateRepairEvidenceSnapshot struct {
	ID               int64  `json:"id"`
	Slug             string `json:"slug"`
	Name             string `json:"name"`
	VenueSlug        string `json:"venue_slug"`
	StartAt          string `json:"start_at"`
	EndAt            string `json:"end_at,omitempty"`
	SourceName       string `json:"source_name,omitempty"`
	SourceURL        string `json:"source_url,omitempty"`
	CalendarURL      string `json:"calendar_url,omitempty"`
	Description      string `json:"description,omitempty"`
	PublicationState string `json:"publication_state,omitempty"`
}

type historicalDuplicateRepairEvidencePayload struct {
	Role               string                                    `json:"role"`
	EvidenceTier       string                                    `json:"evidence_tier,omitempty"`
	Reason             string                                    `json:"reason,omitempty"`
	Source             historicalDuplicateRepairEvidenceSource   `json:"source"`
	Event              historicalDuplicateRepairEvidenceSnapshot `json:"event"`
	SourceIdentityKeys []string                                  `json:"source_identity_keys,omitempty"`
	ExactIdentityKeys  []string                                  `json:"exact_identity_keys,omitempty"`
	ClusterReason      string                                    `json:"cluster_reason,omitempty"`
	ReviewState        string                                    `json:"review_state,omitempty"`
}

func historicalDuplicateRepairEvidenceSnapshotFromEvent(eventID int64, event domain.Event) historicalDuplicateRepairEvidenceSnapshot {
	return historicalDuplicateRepairEvidenceSnapshot{
		ID:               eventID,
		Slug:             strings.TrimSpace(event.Slug),
		Name:             strings.TrimSpace(event.Name),
		VenueSlug:        strings.TrimSpace(event.VenueSlug),
		StartAt:          formatRFC3339UTC(event.Start),
		EndAt:            formatOptionalTime(event.End),
		SourceName:       strings.TrimSpace(event.SourceName),
		SourceURL:        strings.TrimSpace(event.SourceURL),
		CalendarURL:      strings.TrimSpace(event.CalendarURL),
		Description:      strings.TrimSpace(event.Description),
		PublicationState: string(normalizedPublicationState(event.PublicationState)),
	}
}

func historicalDuplicateRepairEvidenceTierForEvent(cluster historicalDuplicateCluster, index, canonicalIndex int, window time.Duration) string {
	if canonicalIndex >= 0 && index == canonicalIndex {
		return "canonical"
	}
	if canonicalIndex >= 0 && canonicalIndex < len(cluster.events) && index >= 0 && index < len(cluster.events) {
		return historicalDuplicateRepairPairEvidenceTier(cluster.events[canonicalIndex], cluster.events[index], window)
	}
	if index >= 0 && index < len(cluster.events) {
		return historicalDuplicateRepairBestClusterEvidenceTier(cluster.events[index], cluster.events, window)
	}
	return ""
}

func historicalDuplicateRepairCleanTitle(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(historicalDuplicateRepairNormalizeTitleForMatching(value))), " "))
}

func normalizeHistoricalDuplicateCleanTitle(title string) string {
	return historicalDuplicateRepairCleanTitle(title)
}

func historicalDuplicateRepairTitleVariantKey(title string) string {
	title = normalizeHistoricalDuplicateCleanTitle(title)
	if title == "" {
		return ""
	}
	for _, marker := range []string{" (", " [", " - ", " : "} {
		if idx := strings.Index(title, marker); idx >= 0 {
			title = strings.TrimSpace(title[:idx])
			break
		}
	}
	return title
}

func historicalDuplicateRepairHeadlinerKey(title string) string {
	title = normalizeHistoricalDuplicateCleanTitle(title)
	if title == "" {
		return ""
	}
	for _, marker := range []string{" with ", " featuring ", " feat ", " ft ", " vs ", " / ", " plus ", " & ", " + "} {
		if idx := strings.Index(title, marker); idx >= 0 {
			title = strings.TrimSpace(title[:idx])
			break
		}
	}
	return title
}

func historicalDuplicateRepairNormalizeTitleForMatching(title string) string {
	runes := []rune(strings.TrimSpace(title))
	if len(runes) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(title) + len(title)/8)
	for i, r := range runes {
		prev := historicalDuplicateRepairTitleRuneAt(runes, i-1)
		next := historicalDuplicateRepairTitleRuneAt(runes, i+1)
		switch {
		case historicalDuplicateRepairIsDashSeparatorRune(r) && (!historicalDuplicateRepairIsTitleWordRune(prev) || !historicalDuplicateRepairIsTitleWordRune(next)):
			builder.WriteString(" - ")
		case r == '+' && prev != '+' && next != '+' && (!historicalDuplicateRepairIsTitleWordRune(prev) || !historicalDuplicateRepairIsTitleWordRune(next)):
			builder.WriteString(" + ")
		default:
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func historicalDuplicateRepairTitleRuneAt(runes []rune, index int) rune {
	if index < 0 || index >= len(runes) {
		return 0
	}
	return runes[index]
}

func historicalDuplicateRepairIsDashSeparatorRune(r rune) bool {
	switch r {
	case '-', '‐', '‑', '‒', '–', '—', '―', '−':
		return true
	default:
		return false
	}
}

func historicalDuplicateRepairIsTitleWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

func newHistoricalDuplicateRepairUnionFind(size int) *historicalDuplicateRepairUnionFind {
	parent := make([]int, size)
	rank := make([]int, size)
	for i := range parent {
		parent[i] = i
	}
	return &historicalDuplicateRepairUnionFind{parent: parent, rank: rank}
}

type historicalDuplicateRepairUnionFind struct {
	parent []int
	rank   []int
}

func (u *historicalDuplicateRepairUnionFind) find(x int) int {
	if u.parent[x] != x {
		u.parent[x] = u.find(u.parent[x])
	}
	return u.parent[x]
}

func (u *historicalDuplicateRepairUnionFind) union(a, b int) {
	rootA := u.find(a)
	rootB := u.find(b)
	if rootA == rootB {
		return
	}
	if u.rank[rootA] < u.rank[rootB] {
		u.parent[rootA] = rootB
		return
	}
	if u.rank[rootA] > u.rank[rootB] {
		u.parent[rootB] = rootA
		return
	}
	u.parent[rootB] = rootA
	u.rank[rootA]++
}
