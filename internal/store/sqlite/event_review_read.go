package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

func (s *Store) ListOpenEventReviewClusters(ctx context.Context) ([]seedstore.EventReviewClusterSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.conflict_type,
			c.conflict_reason,
			c.canonical_event_id,
			COALESCE(ec.slug, ''),
			COALESCE(display_ev.name, ''),
			COALESCE(display_v.slug, ''),
			COALESCE(display_v.name, ''),
			display_ev.start_at,
			COUNT(ce.id),
			c.updated_at,
			(
				SELECT l.import_run_id
				FROM import_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			),
			(
				SELECT l.repair_run_id
				FROM repair_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.repair_run_id DESC
				LIMIT 1
			)
		FROM event_review_clusters c
		LEFT JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id AND ce.active = 1
		LEFT JOIN events ec ON ec.id = c.canonical_event_id
		LEFT JOIN events display_ev ON display_ev.id = COALESCE(c.canonical_event_id, (
			SELECT e2.id
			FROM event_review_cluster_evidence ce2
			JOIN event_review_evidence ev2 ON ev2.id = ce2.evidence_id
			JOIN events e2 ON e2.id = ev2.event_id
			WHERE ce2.cluster_id = c.id
				AND ce2.active = 1
				AND ev2.event_id IS NOT NULL
			ORDER BY e2.id ASC
			LIMIT 1
		))
		LEFT JOIN venues display_v ON display_v.id = display_ev.venue_id
		WHERE c.status = ?
		GROUP BY c.id
		ORDER BY c.updated_at DESC, c.id DESC
	`, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clusters []seedstore.EventReviewClusterSummary
	for rows.Next() {
		cluster, ok, err := scanEventReviewClusterSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		clusters = append(clusters, cluster)
	}
	return clusters, rows.Err()
}

func (s *Store) ListClosedEventReviewClusters(ctx context.Context, limit int) ([]seedstore.EventReviewClusterHistorySummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.conflict_type,
			c.conflict_reason,
			c.canonical_event_id,
			COALESCE(ec.slug, ''),
			COALESCE(display_ev.name, ''),
			COALESCE(display_v.slug, ''),
			COALESCE(display_v.name, ''),
			display_ev.start_at,
			COUNT(ce.id),
			c.updated_at,
			(
				SELECT l.import_run_id
				FROM import_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			),
			(
				SELECT l.repair_run_id
				FROM repair_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.repair_run_id DESC
				LIMIT 1
			),
			r.id,
			r.status,
			COALESCE(r.discard_reason, ''),
			r.created_at,
			r.updated_at,
			c.superseded_by_cluster_id
		FROM event_review_clusters c
		LEFT JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id AND ce.active = 1
		LEFT JOIN events ec ON ec.id = c.canonical_event_id
		LEFT JOIN events display_ev ON display_ev.id = COALESCE(c.canonical_event_id, (
			SELECT e2.id
			FROM event_review_cluster_evidence ce2
			JOIN event_review_evidence ev2 ON ev2.id = ce2.evidence_id
			JOIN events e2 ON e2.id = ev2.event_id
			WHERE ce2.cluster_id = c.id
				AND ce2.active = 1
				AND ev2.event_id IS NOT NULL
			ORDER BY e2.id ASC
			LIMIT 1
		))
		LEFT JOIN venues display_v ON display_v.id = display_ev.venue_id
		JOIN event_review_resolutions r ON r.cluster_id = c.id
		WHERE c.status IN (?, ?, ?)
		GROUP BY c.id
		ORDER BY r.created_at DESC, c.id DESC
		LIMIT ?
	`, string(seedstore.EventReviewClusterStatusResolved), string(seedstore.EventReviewClusterStatusDiscarded), string(seedstore.EventReviewClusterStatusSuperseded), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clusters []seedstore.EventReviewClusterHistorySummary
	for rows.Next() {
		cluster, ok, err := scanEventReviewClusterHistorySummaryRow(rows)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		clusters = append(clusters, cluster)
	}
	return clusters, rows.Err()
}

func (s *Store) ListEventReviewClustersForImportRun(ctx context.Context, importRunID int64) ([]seedstore.EventReviewClusterSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if importRunID <= 0 {
		return nil, errors.New("import run id must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.conflict_type,
			c.conflict_reason,
			c.canonical_event_id,
			COALESCE(ec.slug, ''),
			COALESCE(display_ev.name, ''),
			COALESCE(display_v.slug, ''),
			COALESCE(display_v.name, ''),
			display_ev.start_at,
			COUNT(ce.id),
			c.updated_at,
			(
				SELECT l.import_run_id
				FROM import_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			),
			(
				SELECT l.repair_run_id
				FROM repair_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.repair_run_id DESC
				LIMIT 1
			)
		FROM event_review_clusters c
		JOIN import_run_event_review_clusters l ON l.cluster_id = c.id AND l.import_run_id = ?
		LEFT JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id AND ce.active = 1
		LEFT JOIN events ec ON ec.id = c.canonical_event_id
		LEFT JOIN events display_ev ON display_ev.id = COALESCE(c.canonical_event_id, (
			SELECT e2.id
			FROM event_review_cluster_evidence ce2
			JOIN event_review_evidence ev2 ON ev2.id = ce2.evidence_id
			JOIN events e2 ON e2.id = ev2.event_id
			WHERE ce2.cluster_id = c.id
				AND ce2.active = 1
				AND ev2.event_id IS NOT NULL
			ORDER BY e2.id ASC
			LIMIT 1
		))
		LEFT JOIN venues display_v ON display_v.id = display_ev.venue_id
		GROUP BY c.id
		ORDER BY l.linked_at DESC, c.id DESC
	`, importRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clusters []seedstore.EventReviewClusterSummary
	for rows.Next() {
		cluster, ok, err := scanEventReviewClusterSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		clusters = append(clusters, cluster)
	}
	return clusters, rows.Err()
}

func (s *Store) LoadEventReviewCluster(ctx context.Context, id int64) (seedstore.EventReviewClusterDetail, bool, error) {
	if s == nil || s.db == nil {
		return seedstore.EventReviewClusterDetail{}, false, errors.New("sqlite store is not open")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	summary, ok, err := loadEventReviewClusterSummaryByIDTx(ctx, tx, id)
	if err != nil || !ok {
		return seedstore.EventReviewClusterDetail{}, ok, err
	}
	evidence, err := loadEventReviewClusterEvidenceSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	clusterIdentityKeys, err := loadEventReviewClusterIdentityKeySummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	evidenceIdentityKeys, err := loadEventReviewEvidenceIdentityKeySummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	sourceIdentityLinks, err := loadEventReviewClusterSourceIdentityLinkSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	sourceIdentityChoices, err := loadEventReviewClusterSourceIdentityChoiceSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	exactIdentityMatches, err := loadEventReviewClusterExactIdentityMatchSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	observations, err := loadEventReviewClusterObservationSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	separations, err := loadEventReviewClusterSeparationSummariesTx(ctx, tx, eventReviewClusterSeparationEndpointKeys(summary, evidence, clusterIdentityKeys, evidenceIdentityKeys))
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	var resolution *seedstore.EventReviewResolutionSummary
	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		clusterMeta, ok, err := loadEventReviewClusterTx(ctx, tx, id)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		if ok {
			resolution, err = loadEventReviewClusterResolutionTx(ctx, tx, id, clusterMeta.SupersededByClusterID)
		} else {
			resolution, err = loadEventReviewClusterResolutionTx(ctx, tx, id, nil)
		}
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
	}
	importReadiness := loadEventReviewImportReadinessTx(summary, evidence, evidenceIdentityKeys, exactIdentityMatches, sourceIdentityLinks, sourceIdentityChoices)
	if importReadiness != nil {
		structuralTargets, err := loadEventReviewImportStructuralTargetsTx(ctx, tx, s, summary, evidence)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		importReadiness.ExistingEventTargets = normalizeEventReviewImportExistingEventTargets(append(importReadiness.ExistingEventTargets, structuralTargets...))
		importReadiness.ExistingEventTargets = applyEventReviewImportUnresolvedSourceLinkBlockers(importReadiness.ExistingEventTargets, importReadiness.CandidateIdentityStatuses)
		nearTargets, err := loadEventReviewImportNearTitleTargetsTx(ctx, tx, s, summary, evidence, importReadiness.ExistingEventTargets)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		importReadiness.ExistingEventTargets = normalizeEventReviewImportExistingEventTargets(append(importReadiness.ExistingEventTargets, nearTargets...))
		importReadiness.ExistingEventTargets = applyEventReviewImportUnresolvedSourceLinkBlockers(importReadiness.ExistingEventTargets, importReadiness.CandidateIdentityStatuses)
		importReadiness.ExistingEventTargets = applyEventReviewImportSourceChoiceBlockers(importReadiness.ExistingEventTargets, importReadiness.CandidateIdentityStatuses)
		importReadiness.ExistingEventTargets, err = applyEventReviewImportExistingTargetBlockersTx(ctx, tx, s, summary, evidence, importReadiness.ExistingEventTargets)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		importReadiness.ExistingEventTargets, err = applyEventReviewImportNearTitleHardTargetBlockersTx(ctx, tx, s, summary, evidence, importReadiness.ExistingEventTargets)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		importReadiness.ExistingEventTargets = normalizeEventReviewImportExistingEventTargets(importReadiness.ExistingEventTargets)
		authoritativeTargets, err := loadEventReviewImportAuthoritativeTargetsTx(ctx, tx, s, summary, evidence)
		if err != nil {
			return seedstore.EventReviewClusterDetail{}, false, err
		}
		importReadiness.AuthoritativeTargets = authoritativeTargets
		assignSelectedCandidateExistingEventTargets(importReadiness)
		finalizeEventReviewImportNewListingScope(importReadiness)
	}
	canonicalChoices, err := loadEventReviewClusterChoiceSummariesTx(ctx, tx, id, "event_review_canonical_choices")
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	draftChoices, err := loadEventReviewClusterChoiceSummariesTx(ctx, tx, id, "event_review_draft_choices")
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	titleRepairReadiness, err := loadEventReviewTitleRepairReadinessTx(ctx, tx, summary, draftChoices)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	liveActions, err := loadEventReviewClusterLiveActionSummariesTx(ctx, tx, id)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}
	historicalDuplicateReadiness, err := loadEventReviewHistoricalDuplicateReadinessTx(ctx, tx, summary, evidence, liveActions)
	if err != nil {
		return seedstore.EventReviewClusterDetail{}, false, err
	}

	return seedstore.EventReviewClusterDetail{
		Summary:                      summary,
		Resolution:                   resolution,
		ImportReadiness:              importReadiness,
		TitleRepairReadiness:         titleRepairReadiness,
		HistoricalDuplicateReadiness: historicalDuplicateReadiness,
		Evidence:                     evidence,
		ClusterIdentityKeys:          clusterIdentityKeys,
		EvidenceIdentityKeys:         evidenceIdentityKeys,
		SourceIdentityLinks:          sourceIdentityLinks,
		ExactIdentityMatches:         exactIdentityMatches,
		Observations:                 observations,
		Separations:                  separations,
		CanonicalChoices:             canonicalChoices,
		DraftChoices:                 draftChoices,
		LiveActions:                  liveActions,
	}, true, nil
}

func loadEventReviewClusterSummaryByIDTx(ctx context.Context, q queryer, id int64) (seedstore.EventReviewClusterSummary, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.conflict_type,
			c.conflict_reason,
			c.canonical_event_id,
			COALESCE(ec.slug, ''),
			COALESCE(display_ev.name, ''),
			COALESCE(display_v.slug, ''),
			COALESCE(display_v.name, ''),
			display_ev.start_at,
			COUNT(ce.id),
			c.updated_at,
			(
				SELECT l.import_run_id
				FROM import_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			),
			(
				SELECT l.repair_run_id
				FROM repair_run_event_review_clusters l
				WHERE l.cluster_id = c.id
				ORDER BY l.linked_at DESC, l.repair_run_id DESC
				LIMIT 1
			)
		FROM event_review_clusters c
		LEFT JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id AND ce.active = 1
		LEFT JOIN events ec ON ec.id = c.canonical_event_id
		LEFT JOIN events display_ev ON display_ev.id = COALESCE(c.canonical_event_id, (
			SELECT e2.id
			FROM event_review_cluster_evidence ce2
			JOIN event_review_evidence ev2 ON ev2.id = ce2.evidence_id
			JOIN events e2 ON e2.id = ev2.event_id
			WHERE ce2.cluster_id = c.id
				AND ce2.active = 1
				AND ev2.event_id IS NOT NULL
			ORDER BY e2.id ASC
			LIMIT 1
		))
		LEFT JOIN venues display_v ON display_v.id = display_ev.venue_id
		WHERE c.id = ?
		GROUP BY c.id
		LIMIT 1
	`, id)
	cluster, ok, err := scanEventReviewClusterSummaryRow(row)
	if err != nil || !ok {
		return seedstore.EventReviewClusterSummary{}, ok, err
	}
	return cluster, true, nil
}

func loadEventReviewClusterResolutionTx(ctx context.Context, q queryer, clusterID int64, supersededByClusterID *int64) (*seedstore.EventReviewResolutionSummary, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			id,
			cluster_id,
			status,
			snapshot,
			COALESCE(discard_reason, ''),
			created_at,
			updated_at
		FROM event_review_resolutions
		WHERE cluster_id = ?
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, clusterID)

	var (
		summary   seedstore.EventReviewResolutionSummary
		status    string
		snapshot  string
		createdAt string
		updatedAt string
	)
	switch err := row.Scan(
		&summary.ID,
		&summary.ClusterID,
		&status,
		&snapshot,
		&summary.DiscardReason,
		&createdAt,
		&updatedAt,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}

	summary.Status = seedstore.EventReviewResolutionStatus(status)
	summary.SnapshotRaw = snapshot
	if supersededByClusterID != nil {
		summary.SupersededByClusterID = supersededByClusterID
	}

	var parsed eventReviewResolutionSnapshotParsed
	if err := json.Unmarshal([]byte(snapshot), &parsed); err != nil {
		summary.SnapshotParseWarning = err.Error()
	} else {
		if parsed.RepairRunID != nil {
			summary.RepairRunID = parsed.RepairRunID
		}
		if parsed.CanonicalEventID != nil {
			summary.CanonicalEventID = parsed.CanonicalEventID
		}
		if summary.SupersededByClusterID == nil && parsed.SupersededByClusterID != nil {
			summary.SupersededByClusterID = parsed.SupersededByClusterID
		}
		if parsed.AppliedAutoResolution != nil {
			summary.AppliedAutoResolution = &seedstore.EventReviewResolutionAppliedAutoResolutionSummary{
				EventID:       parsed.AppliedAutoResolution.EventID,
				EventSlug:     parsed.AppliedAutoResolution.EventSlug,
				Result:        parsed.AppliedAutoResolution.Result,
				SourceID:      parsed.AppliedAutoResolution.SourceID,
				SourceName:    parsed.AppliedAutoResolution.SourceName,
				SourceURL:     parsed.AppliedAutoResolution.SourceURL,
				EvidenceCount: parsed.AppliedAutoResolution.EvidenceCount,
			}
		}
		if parsed.AppliedImportListing != nil {
			appliedImportListing := &seedstore.EventReviewResolutionAppliedImportListingSummary{
				EventID:    parsed.AppliedImportListing.EventID,
				EventSlug:  parsed.AppliedImportListing.EventSlug,
				Title:      parsed.AppliedImportListing.Title,
				VenueSlug:  parsed.AppliedImportListing.VenueSlug,
				VenueName:  parsed.AppliedImportListing.VenueName,
				SourceID:   parsed.AppliedImportListing.SourceID,
				SourceName: parsed.AppliedImportListing.SourceName,
				SourceURL:  parsed.AppliedImportListing.SourceURL,
				EvidenceID: parsed.AppliedImportListing.EvidenceID,
			}
			if parsed.AppliedImportListing.StartAt != "" {
				startAt, err := parseRFC3339UTC(parsed.AppliedImportListing.StartAt)
				if err != nil {
					summary.SnapshotParseWarning = err.Error()
				} else {
					appliedImportListing.StartAt = startAt
				}
			}
			summary.AppliedImportListing = appliedImportListing
		}
		if parsed.AppliedSupportingSource != nil {
			summary.AppliedSupportingSource = &seedstore.EventReviewResolutionAppliedSupportingSourceSummary{
				EventID:        parsed.AppliedSupportingSource.EventID,
				EventSlug:      parsed.AppliedSupportingSource.EventSlug,
				Title:          parsed.AppliedSupportingSource.Title,
				SourceID:       parsed.AppliedSupportingSource.SourceID,
				SourceName:     parsed.AppliedSupportingSource.SourceName,
				SourceURL:      parsed.AppliedSupportingSource.SourceURL,
				EvidenceID:     parsed.AppliedSupportingSource.EvidenceID,
				TargetBasis:    parsed.AppliedSupportingSource.TargetBasis,
				PromotedReview: parsed.AppliedSupportingSource.PromotedReview,
			}
		}
		if parsed.AppliedAuthoritativeImport != nil {
			summary.AppliedAuthoritativeImport = &seedstore.EventReviewResolutionAppliedAuthoritativeImportSummary{
				EventID:    parsed.AppliedAuthoritativeImport.EventID,
				EventSlug:  parsed.AppliedAuthoritativeImport.EventSlug,
				Title:      parsed.AppliedAuthoritativeImport.Title,
				SourceID:   parsed.AppliedAuthoritativeImport.SourceID,
				SourceName: parsed.AppliedAuthoritativeImport.SourceName,
				SourceURL:  parsed.AppliedAuthoritativeImport.SourceURL,
				EvidenceID: parsed.AppliedAuthoritativeImport.EvidenceID,
				Result:     parsed.AppliedAuthoritativeImport.Result,
			}
		}
		summary.AppliedSeparations = make([]seedstore.EventReviewResolutionAppliedSeparationSummary, 0, len(parsed.AppliedSeparations))
		for _, separation := range parsed.AppliedSeparations {
			summary.AppliedSeparations = append(summary.AppliedSeparations, seedstore.EventReviewResolutionAppliedSeparationSummary{
				SeparationID: separation.SeparationID,
				EndpointAKey: strings.TrimSpace(separation.EndpointAKey),
				EndpointBKey: strings.TrimSpace(separation.EndpointBKey),
				Reason:       strings.TrimSpace(separation.Reason),
			})
		}
		if parsed.AppliedTitleRepair != nil {
			summary.AppliedTitleRepair = &seedstore.EventReviewResolutionAppliedTitleRepairSummary{
				EventID:  parsed.AppliedTitleRepair.EventID,
				OldTitle: parsed.AppliedTitleRepair.OldTitle,
				NewTitle: parsed.AppliedTitleRepair.NewTitle,
				OldSlug:  parsed.AppliedTitleRepair.OldSlug,
				NewSlug:  parsed.AppliedTitleRepair.NewSlug,
			}
		}
		if parsed.AppliedTitleSlugConflict != nil {
			summary.AppliedTitleSlugConflict = &seedstore.EventReviewResolutionAppliedTitleSlugConflictSummary{
				Mode:                parsed.AppliedTitleSlugConflict.Mode,
				OldCanonicalEventID: parsed.AppliedTitleSlugConflict.OldCanonicalEventID,
				SlugConflictEventID: parsed.AppliedTitleSlugConflict.SlugConflictEventID,
				SurvivingEventID:    parsed.AppliedTitleSlugConflict.SurvivingEventID,
				OldTitle:            parsed.AppliedTitleSlugConflict.OldTitle,
				NewTitle:            parsed.AppliedTitleSlugConflict.NewTitle,
				OldSlug:             parsed.AppliedTitleSlugConflict.OldSlug,
				NewSlug:             parsed.AppliedTitleSlugConflict.NewSlug,
			}
		}
		if parsed.AppliedHistoricalKeepSeparate != nil {
			summary.AppliedHistoricalKeepSeparate = &seedstore.EventReviewResolutionAppliedHistoricalKeepSeparateSummary{
				KeptEvents: make([]seedstore.EventReviewResolutionKeptHistoricalDuplicateEventSummary, 0, len(parsed.AppliedHistoricalKeepSeparate.KeptEvents)),
			}
			for _, event := range parsed.AppliedHistoricalKeepSeparate.KeptEvents {
				summary.AppliedHistoricalKeepSeparate.KeptEvents = append(summary.AppliedHistoricalKeepSeparate.KeptEvents, seedstore.EventReviewResolutionKeptHistoricalDuplicateEventSummary{
					EventID:   event.EventID,
					EventSlug: event.EventSlug,
				})
			}
		}
		summary.AppliedLiveActions = make([]seedstore.EventReviewResolutionAppliedLiveActionSummary, 0, len(parsed.AppliedLiveActions))
		for _, action := range parsed.AppliedLiveActions {
			summary.AppliedLiveActions = append(summary.AppliedLiveActions, seedstore.EventReviewResolutionAppliedLiveActionSummary{
				EventID:   action.EventID,
				EventSlug: action.EventSlug,
				Action:    action.Action,
				Reason:    action.Reason,
			})
		}
	}

	created, err := parseRFC3339UTC(createdAt)
	if err != nil {
		return nil, err
	}
	summary.CreatedAt = created
	updated, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return nil, err
	}
	summary.UpdatedAt = updated
	return &summary, nil
}

type eventReviewResolutionSnapshotParsed struct {
	RepairRunID                   *int64                                                          `json:"repair_run_id,omitempty"`
	SupersededByClusterID         *int64                                                          `json:"superseded_by_cluster_id,omitempty"`
	CanonicalEventID              *int64                                                          `json:"canonical_event_id,omitempty"`
	AppliedAutoResolution         *eventReviewResolutionAppliedAutoResolutionSnapshotView         `json:"applied_auto_resolution,omitempty"`
	AppliedImportListing          *eventReviewResolutionAppliedImportListingSnapshotView          `json:"applied_import_listing,omitempty"`
	AppliedSupportingSource       *eventReviewResolutionAppliedSupportingSourceSnapshotView       `json:"applied_supporting_source,omitempty"`
	AppliedAuthoritativeImport    *eventReviewResolutionAppliedAuthoritativeImportSnapshotView    `json:"applied_authoritative_import,omitempty"`
	AppliedSeparations            []eventReviewResolutionAppliedSeparationSnapshotView            `json:"applied_separations,omitempty"`
	AppliedTitleRepair            *eventReviewResolutionAppliedTitleRepairSnapshotView            `json:"applied_title_repair,omitempty"`
	AppliedTitleSlugConflict      *eventReviewResolutionAppliedTitleSlugConflictSnapshotView      `json:"applied_title_slug_conflict,omitempty"`
	AppliedHistoricalKeepSeparate *eventReviewResolutionAppliedHistoricalKeepSeparateSnapshotView `json:"applied_historical_keep_separate,omitempty"`
	AppliedLiveActions            []eventReviewResolutionAppliedLiveActionSnapshotView            `json:"applied_live_actions,omitempty"`
}

type eventReviewResolutionAppliedAutoResolutionSnapshotView struct {
	EventID       int64  `json:"event_id"`
	EventSlug     string `json:"event_slug,omitempty"`
	Result        string `json:"result,omitempty"`
	SourceID      int64  `json:"source_id,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	EvidenceCount int    `json:"evidence_count,omitempty"`
}

type eventReviewResolutionAppliedImportListingSnapshotView struct {
	EventID    int64  `json:"event_id"`
	EventSlug  string `json:"event_slug,omitempty"`
	Title      string `json:"title,omitempty"`
	VenueSlug  string `json:"venue_slug,omitempty"`
	VenueName  string `json:"venue_name,omitempty"`
	StartAt    string `json:"start_at,omitempty"`
	SourceID   int64  `json:"source_id,omitempty"`
	SourceName string `json:"source_name,omitempty"`
	SourceURL  string `json:"source_url,omitempty"`
	EvidenceID int64  `json:"evidence_id,omitempty"`
}

type eventReviewResolutionAppliedSupportingSourceSnapshotView struct {
	EventID        int64                                  `json:"event_id"`
	EventSlug      string                                 `json:"event_slug,omitempty"`
	Title          string                                 `json:"title,omitempty"`
	SourceID       int64                                  `json:"source_id,omitempty"`
	SourceName     string                                 `json:"source_name,omitempty"`
	SourceURL      string                                 `json:"source_url,omitempty"`
	EvidenceID     int64                                  `json:"evidence_id,omitempty"`
	TargetBasis    seedstore.EventReviewImportTargetBasis `json:"target_basis,omitempty"`
	PromotedReview bool                                   `json:"promoted_review,omitempty"`
}

type eventReviewResolutionAppliedSeparationSnapshotView struct {
	SeparationID int64  `json:"separation_id,omitempty"`
	EndpointAKey string `json:"endpoint_a_key,omitempty"`
	EndpointBKey string `json:"endpoint_b_key,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

type eventReviewResolutionAppliedAuthoritativeImportSnapshotView struct {
	EventID    int64  `json:"event_id"`
	EventSlug  string `json:"event_slug,omitempty"`
	Title      string `json:"title,omitempty"`
	SourceID   int64  `json:"source_id,omitempty"`
	SourceName string `json:"source_name,omitempty"`
	SourceURL  string `json:"source_url,omitempty"`
	EvidenceID int64  `json:"evidence_id,omitempty"`
	Result     string `json:"result,omitempty"`
}

type eventReviewResolutionAppliedLiveActionSnapshotView struct {
	EventID   int64                               `json:"event_id"`
	EventSlug string                              `json:"event_slug,omitempty"`
	Action    seedstore.EventReviewLiveActionKind `json:"action"`
	Reason    string                              `json:"reason,omitempty"`
}

type eventReviewResolutionAppliedHistoricalKeepSeparateSnapshotView struct {
	KeptEvents []eventReviewResolutionKeptHistoricalDuplicateEventSnapshotView `json:"kept_events,omitempty"`
}

type eventReviewResolutionKeptHistoricalDuplicateEventSnapshotView struct {
	EventID   int64  `json:"event_id"`
	EventSlug string `json:"event_slug,omitempty"`
}

type eventReviewResolutionAppliedTitleRepairSnapshotView struct {
	EventID  int64  `json:"event_id"`
	OldTitle string `json:"old_title,omitempty"`
	NewTitle string `json:"new_title,omitempty"`
	OldSlug  string `json:"old_slug,omitempty"`
	NewSlug  string `json:"new_slug,omitempty"`
}

type eventReviewResolutionAppliedTitleSlugConflictSnapshotView struct {
	Mode                seedstore.EventReviewTitleRepairSlugConflictMode `json:"mode"`
	OldCanonicalEventID int64                                            `json:"old_canonical_event_id,omitempty"`
	SlugConflictEventID int64                                            `json:"slug_conflict_event_id,omitempty"`
	SurvivingEventID    int64                                            `json:"surviving_event_id,omitempty"`
	OldTitle            string                                           `json:"old_title,omitempty"`
	NewTitle            string                                           `json:"new_title,omitempty"`
	OldSlug             string                                           `json:"old_slug,omitempty"`
	NewSlug             string                                           `json:"new_slug,omitempty"`
}

func scanEventReviewClusterSummaryRow(scanner interface {
	Scan(dest ...any) error
}) (seedstore.EventReviewClusterSummary, bool, error) {
	var (
		cluster        seedstore.EventReviewClusterSummary
		status         string
		stagingKey     sql.NullString
		canonicalEvent sql.NullInt64
		displayStartAt sql.NullString
		updatedAt      string
		importRunID    sql.NullInt64
		repairRunID    sql.NullInt64
	)
	switch err := scanner.Scan(
		&cluster.ID,
		&status,
		&cluster.Version,
		&stagingKey,
		&cluster.StagingKeyVersion,
		&cluster.ConflictType,
		&cluster.ConflictReason,
		&canonicalEvent,
		&cluster.CanonicalEventSlug,
		&cluster.DisplayTitle,
		&cluster.DisplayVenueSlug,
		&cluster.DisplayVenueName,
		&displayStartAt,
		&cluster.EvidenceCount,
		&updatedAt,
		&importRunID,
		&repairRunID,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return seedstore.EventReviewClusterSummary{}, false, nil
	case err != nil:
		return seedstore.EventReviewClusterSummary{}, false, err
	}

	cluster.Status = seedstore.EventReviewClusterStatus(status)
	if stagingKey.Valid {
		cluster.StagingKey = &stagingKey.String
	}
	if canonicalEvent.Valid {
		cluster.CanonicalEventID = &canonicalEvent.Int64
	}
	if displayStartAt.Valid {
		parsedDisplayStartAt, err := parseRFC3339UTC(displayStartAt.String)
		if err != nil {
			return seedstore.EventReviewClusterSummary{}, false, err
		}
		cluster.DisplayStartAt = &parsedDisplayStartAt
	}
	if importRunID.Valid {
		cluster.LatestImportRunID = &importRunID.Int64
	}
	if repairRunID.Valid {
		cluster.LatestRepairRunID = &repairRunID.Int64
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return seedstore.EventReviewClusterSummary{}, false, err
	}
	cluster.UpdatedAt = parsedUpdatedAt
	return cluster, true, nil
}

func scanEventReviewClusterHistorySummaryRow(scanner interface {
	Scan(dest ...any) error
}) (seedstore.EventReviewClusterHistorySummary, bool, error) {
	var (
		cluster               seedstore.EventReviewClusterHistorySummary
		status                string
		stagingKey            sql.NullString
		canonicalEvent        sql.NullInt64
		displayStartAt        sql.NullString
		updatedAt             string
		importRunID           sql.NullInt64
		repairRunID           sql.NullInt64
		supersededByClusterID sql.NullInt64
		resolutionStatus      string
		resolutionAt          string
		resolutionUpdatedAt   string
	)
	switch err := scanner.Scan(
		&cluster.ID,
		&status,
		&cluster.Version,
		&stagingKey,
		&cluster.StagingKeyVersion,
		&cluster.ConflictType,
		&cluster.ConflictReason,
		&canonicalEvent,
		&cluster.CanonicalEventSlug,
		&cluster.DisplayTitle,
		&cluster.DisplayVenueSlug,
		&cluster.DisplayVenueName,
		&displayStartAt,
		&cluster.EvidenceCount,
		&updatedAt,
		&importRunID,
		&repairRunID,
		&cluster.ResolutionID,
		&resolutionStatus,
		&cluster.DiscardReason,
		&resolutionAt,
		&resolutionUpdatedAt,
		&supersededByClusterID,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return seedstore.EventReviewClusterHistorySummary{}, false, nil
	case err != nil:
		return seedstore.EventReviewClusterHistorySummary{}, false, err
	}

	cluster.Status = seedstore.EventReviewClusterStatus(status)
	cluster.ResolutionStatus = seedstore.EventReviewResolutionStatus(resolutionStatus)
	if stagingKey.Valid {
		cluster.StagingKey = &stagingKey.String
	}
	if canonicalEvent.Valid {
		cluster.CanonicalEventID = &canonicalEvent.Int64
	}
	if displayStartAt.Valid {
		parsedDisplayStartAt, err := parseRFC3339UTC(displayStartAt.String)
		if err != nil {
			return seedstore.EventReviewClusterHistorySummary{}, false, err
		}
		cluster.DisplayStartAt = &parsedDisplayStartAt
	}
	if importRunID.Valid {
		cluster.LatestImportRunID = &importRunID.Int64
	}
	if repairRunID.Valid {
		cluster.LatestRepairRunID = &repairRunID.Int64
	}
	if supersededByClusterID.Valid {
		cluster.SupersededByClusterID = &supersededByClusterID.Int64
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return seedstore.EventReviewClusterHistorySummary{}, false, err
	}
	cluster.UpdatedAt = parsedUpdatedAt
	parsedResolvedAt, err := parseRFC3339UTC(resolutionAt)
	if err != nil {
		return seedstore.EventReviewClusterHistorySummary{}, false, err
	}
	cluster.ResolutionCreatedAt = parsedResolvedAt
	parsedResolutionUpdatedAt, err := parseRFC3339UTC(resolutionUpdatedAt)
	if err != nil {
		return seedstore.EventReviewClusterHistorySummary{}, false, err
	}
	cluster.ResolvedAt = parsedResolutionUpdatedAt
	return cluster, true, nil
}

func loadEventReviewClusterEvidenceSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterEvidenceSummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			ce.id,
			ce.evidence_id,
			e.source_id,
			COALESCE(s.name, ''),
			COALESCE(s.url, ''),
			e.event_id,
			COALESCE(ev.slug, ''),
			e.evidence_fingerprint,
			e.payload,
			ce.linked_at,
			ce.link_reason
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		LEFT JOIN sources s ON s.id = e.source_id
		LEFT JOIN events ev ON ev.id = e.event_id
		WHERE ce.cluster_id = ?
			AND ce.active = 1
		ORDER BY ce.linked_at DESC, ce.id DESC
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var evidence []seedstore.EventReviewClusterEvidenceSummary
	for rows.Next() {
		var row seedstore.EventReviewClusterEvidenceSummary
		var eventID sql.NullInt64
		var linkedAt string
		if err := rows.Scan(
			&row.ID,
			&row.EvidenceID,
			&row.SourceID,
			&row.SourceName,
			&row.SourceURL,
			&eventID,
			&row.EventSlug,
			&row.EvidenceFingerprint,
			&row.Payload,
			&linkedAt,
			&row.LinkReason,
		); err != nil {
			return nil, err
		}
		if eventID.Valid {
			row.EventID = &eventID.Int64
		}
		row.LinkedAt, err = parseRFC3339UTC(linkedAt)
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, row)
	}
	return evidence, rows.Err()
}

func loadEventReviewClusterIdentityKeySummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterIdentityKeySummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			cik.id,
			cik.identity_key_id,
			i.identity_key_hash,
			i.key_kind,
			i.key_version,
			i.normalized_key,
			cik.linked_at
		FROM event_review_cluster_identity_keys cik
		JOIN event_review_identity_keys i ON i.id = cik.identity_key_id
		WHERE cik.cluster_id = ?
			AND cik.active = 1
		ORDER BY i.key_kind, i.normalized_key, i.id, cik.id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var identityKeys []seedstore.EventReviewClusterIdentityKeySummary
	for rows.Next() {
		var row seedstore.EventReviewClusterIdentityKeySummary
		var linkedAt string
		if err := rows.Scan(
			&row.ID,
			&row.IdentityKeyID,
			&row.IdentityKeyHash,
			&row.KeyKind,
			&row.KeyVersion,
			&row.NormalizedKey,
			&linkedAt,
		); err != nil {
			return nil, err
		}
		row.LinkedAt, err = parseRFC3339UTC(linkedAt)
		if err != nil {
			return nil, err
		}
		identityKeys = append(identityKeys, row)
	}
	return identityKeys, rows.Err()
}

func loadEventReviewEvidenceIdentityKeySummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewEvidenceIdentityKeySummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			eki.id,
			e.id,
			e.evidence_fingerprint,
			eki.identity_key_id,
			i.identity_key_hash,
			i.key_kind,
			i.key_version,
			i.normalized_key,
			eki.source_id,
			eki.role
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		JOIN event_review_evidence_identity_keys eki ON eki.evidence_id = e.id
		JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
		WHERE ce.cluster_id = ?
			AND ce.active = 1
		ORDER BY e.id, eki.role, i.key_kind, i.normalized_key, i.id, eki.id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var identityKeys []seedstore.EventReviewEvidenceIdentityKeySummary
	for rows.Next() {
		var row seedstore.EventReviewEvidenceIdentityKeySummary
		var sourceID sql.NullInt64
		if err := rows.Scan(
			&row.ID,
			&row.EvidenceID,
			&row.EvidenceFingerprint,
			&row.IdentityKeyID,
			&row.IdentityKeyHash,
			&row.KeyKind,
			&row.KeyVersion,
			&row.NormalizedKey,
			&sourceID,
			&row.Role,
		); err != nil {
			return nil, err
		}
		if sourceID.Valid {
			row.SourceID = &sourceID.Int64
		}
		identityKeys = append(identityKeys, row)
	}
	return identityKeys, rows.Err()
}

func eventReviewClusterSeparationEndpointKeys(summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, clusterIdentityKeys []seedstore.EventReviewClusterIdentityKeySummary, evidenceIdentityKeys []seedstore.EventReviewEvidenceIdentityKeySummary) []string {
	keys := make(map[string]struct{})
	add := func(key string) {
		key = strings.TrimSpace(key)
		if key != "" {
			keys[key] = struct{}{}
		}
	}

	if summary.CanonicalEventID != nil {
		add(seedstore.EventReviewSeparationEventEndpointKey(*summary.CanonicalEventID))
	}
	for _, row := range evidence {
		add(eventReviewSeparationEndpointKeyEvidence(row.EvidenceFingerprint))
		if row.EventID != nil {
			add(seedstore.EventReviewSeparationEventEndpointKey(*row.EventID))
		}
	}
	for _, row := range clusterIdentityKeys {
		add(EventReviewSeparationEndpointKeyIdentity(row.IdentityKeyHash))
	}
	for _, row := range evidenceIdentityKeys {
		add(EventReviewSeparationEndpointKeyIdentity(row.IdentityKeyHash))
	}

	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

type eventReviewClusterSeparationRow struct {
	id                     int64
	endpointAKind          string
	endpointAKey           string
	endpointAEventID       sql.NullInt64
	endpointAEvidenceID    sql.NullInt64
	endpointAIdentityKeyID sql.NullInt64
	endpointACanonicalID   sql.NullInt64
	endpointAEventSlug     string
	endpointAEvidenceFP    string
	endpointAIdentityHash  string
	endpointAIdentityKind  string
	endpointANormalizedKey string
	endpointACanonicalSlug string
	endpointBKind          string
	endpointBKey           string
	endpointBEventID       sql.NullInt64
	endpointBEvidenceID    sql.NullInt64
	endpointBIdentityKeyID sql.NullInt64
	endpointBCanonicalID   sql.NullInt64
	endpointBEventSlug     string
	endpointBEvidenceFP    string
	endpointBIdentityHash  string
	endpointBIdentityKind  string
	endpointBNormalizedKey string
	endpointBCanonicalSlug string
	reason                 string
	createdAt              string
	updatedAt              string
}

type eventReviewClusterObservationSummaryRow struct {
	id                        int64
	runScope                  string
	sourceID                  int64
	sourceName                string
	sourceURL                 string
	sourceIdentityKey         string
	sourceAuthority           string
	fieldName                 string
	incomingRaw               string
	incomingNormalized        string
	canonicalBeforeRaw        string
	canonicalBeforeNormalized string
	outcome                   string
	isConflict                int
	createdAt                 string
	updatedAt                 string
}

type eventReviewClusterSourceIdentityLinkRow struct {
	sourceID                  int64
	sourceName                string
	sourceURL                 string
	sourceIdentityKey         string
	evidenceCount             int64
	linkedEventID             sql.NullInt64
	linkedEventSlug           string
	linkedEventTitle          string
	rawLinkedEventID          sql.NullInt64
	rawLinkedEventSlug        string
	rawLinkedPublicationState string
	resolvedFromWithheld      int
	authoritative             int
	linkUpdatedAt             sql.NullString
}

type eventReviewClusterExactIdentityMatchRow struct {
	identityKeyID        int64
	identityKeyHash      string
	keyVersion           int
	normalizedKey        string
	evidenceCount        int
	linkedEventID        sql.NullInt64
	linkedEventSlug      string
	linkedEventTitle     string
	linkedEventVenueSlug string
	linkedEventStartAt   sql.NullString
}

func loadEventReviewClusterSeparationSummariesTx(ctx context.Context, q queryer, endpointKeys []string) ([]seedstore.EventReviewClusterSeparationSummary, error) {
	if len(endpointKeys) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(endpointKeys)*2)
	for _, key := range endpointKeys {
		args = append(args, key)
	}
	for _, key := range endpointKeys {
		args = append(args, key)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			s.id,
			s.endpoint_a_kind,
			s.endpoint_a_key,
			s.endpoint_a_event_id,
			s.endpoint_a_evidence_id,
			s.endpoint_a_identity_key_id,
			s.endpoint_a_canonical_event_id,
			COALESCE(aev.slug, ''),
			COALESCE(aee.evidence_fingerprint, ''),
			COALESCE(aik.identity_key_hash, ''),
			COALESCE(aik.key_kind, ''),
			COALESCE(aik.normalized_key, ''),
			COALESCE(aec.slug, ''),
			s.endpoint_b_kind,
			s.endpoint_b_key,
			s.endpoint_b_event_id,
			s.endpoint_b_evidence_id,
			s.endpoint_b_identity_key_id,
			s.endpoint_b_canonical_event_id,
			COALESCE(bev.slug, ''),
			COALESCE(bee.evidence_fingerprint, ''),
			COALESCE(bik.identity_key_hash, ''),
			COALESCE(bik.key_kind, ''),
			COALESCE(bik.normalized_key, ''),
			COALESCE(bec.slug, ''),
			s.reason,
			s.created_at,
			s.updated_at
		FROM event_review_separations s
		LEFT JOIN events aev ON aev.id = s.endpoint_a_event_id
		LEFT JOIN event_review_evidence aee ON aee.id = s.endpoint_a_evidence_id
		LEFT JOIN event_review_identity_keys aik ON aik.id = s.endpoint_a_identity_key_id
		LEFT JOIN events aec ON aec.id = s.endpoint_a_canonical_event_id
		LEFT JOIN events bev ON bev.id = s.endpoint_b_event_id
		LEFT JOIN event_review_evidence bee ON bee.id = s.endpoint_b_evidence_id
		LEFT JOIN event_review_identity_keys bik ON bik.id = s.endpoint_b_identity_key_id
		LEFT JOIN events bec ON bec.id = s.endpoint_b_canonical_event_id
		WHERE s.active = 1
			AND (
				s.endpoint_a_key IN (`+placeholders(len(endpointKeys))+`)
				OR s.endpoint_b_key IN (`+placeholders(len(endpointKeys))+`)
			)
		ORDER BY s.updated_at DESC, s.id DESC
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var separations []seedstore.EventReviewClusterSeparationSummary
	for rows.Next() {
		var row eventReviewClusterSeparationRow
		if err := rows.Scan(
			&row.id,
			&row.endpointAKind,
			&row.endpointAKey,
			&row.endpointAEventID,
			&row.endpointAEvidenceID,
			&row.endpointAIdentityKeyID,
			&row.endpointACanonicalID,
			&row.endpointAEventSlug,
			&row.endpointAEvidenceFP,
			&row.endpointAIdentityHash,
			&row.endpointAIdentityKind,
			&row.endpointANormalizedKey,
			&row.endpointACanonicalSlug,
			&row.endpointBKind,
			&row.endpointBKey,
			&row.endpointBEventID,
			&row.endpointBEvidenceID,
			&row.endpointBIdentityKeyID,
			&row.endpointBCanonicalID,
			&row.endpointBEventSlug,
			&row.endpointBEvidenceFP,
			&row.endpointBIdentityHash,
			&row.endpointBIdentityKind,
			&row.endpointBNormalizedKey,
			&row.endpointBCanonicalSlug,
			&row.reason,
			&row.createdAt,
			&row.updatedAt,
		); err != nil {
			return nil, err
		}
		separations = append(separations, row.toSummary())
	}
	return separations, rows.Err()
}

func loadEventReviewClusterObservationSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterObservationSummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			o.id,
			o.run_scope,
			o.source_id,
			COALESCE(s.name, ''),
			COALESCE(s.url, ''),
			o.source_identity_key,
			o.source_authority,
			o.field_name,
			o.incoming_raw,
			o.incoming_normalized,
			o.canonical_before_raw,
			o.canonical_before_normalized,
			o.outcome,
			o.is_conflict,
			o.created_at,
			o.updated_at
		FROM event_source_attribute_observations o
		LEFT JOIN sources s ON s.id = o.source_id
		WHERE o.target_kind = ?
			AND o.event_review_cluster_id = ?
		ORDER BY o.updated_at DESC, o.source_id ASC, o.source_identity_key ASC, o.field_name ASC, o.id ASC
	`, string(seedstore.ObservationTargetKindEventReviewCluster), clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var observations []seedstore.EventReviewClusterObservationSummary
	for rows.Next() {
		var row eventReviewClusterObservationSummaryRow
		if err := rows.Scan(
			&row.id,
			&row.runScope,
			&row.sourceID,
			&row.sourceName,
			&row.sourceURL,
			&row.sourceIdentityKey,
			&row.sourceAuthority,
			&row.fieldName,
			&row.incomingRaw,
			&row.incomingNormalized,
			&row.canonicalBeforeRaw,
			&row.canonicalBeforeNormalized,
			&row.outcome,
			&row.isConflict,
			&row.createdAt,
			&row.updatedAt,
		); err != nil {
			return nil, err
		}
		observations = append(observations, row.toSummary())
	}
	return observations, rows.Err()
}

func loadEventReviewClusterSourceIdentityLinkSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterSourceIdentityLinkSummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			eik.source_id,
			COALESCE(s.name, ''),
			COALESCE(s.url, ''),
			i.normalized_key,
			COUNT(DISTINCT e.id),
			ev.id,
			COALESCE(ev.slug, ''),
			COALESCE(ev.name, ''),
			linked_ev.id,
			COALESCE(linked_ev.slug, ''),
			COALESCE(linked_ev.publication_state, ''),
			CASE
				WHEN linked_ev.id IS NOT NULL
					AND ev.id IS NOT NULL
					AND linked_ev.id <> ev.id THEN 1
				ELSE 0
			END,
			COALESCE(l.is_authoritative, 0),
			l.updated_at
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		JOIN event_review_evidence_identity_keys eik ON eik.evidence_id = e.id
		JOIN event_review_identity_keys i ON i.id = eik.identity_key_id
		LEFT JOIN sources s ON s.id = eik.source_id
		LEFT JOIN event_source_links l ON l.source_id = eik.source_id
			AND l.source_event_key = i.normalized_key
		LEFT JOIN events linked_ev ON linked_ev.id = l.event_id
		LEFT JOIN events canonical_ev ON canonical_ev.id = linked_ev.canonical_event_id
		LEFT JOIN events ev ON ev.id = CASE
			WHEN linked_ev.origin = ? AND TRIM(COALESCE(linked_ev.publication_state, '')) <> ? THEN linked_ev.id
			WHEN canonical_ev.id IS NOT NULL
				AND canonical_ev.origin = ?
				AND TRIM(COALESCE(canonical_ev.publication_state, '')) <> ? THEN canonical_ev.id
			ELSE NULL
		END
		WHERE ce.cluster_id = ?
			AND ce.active = 1
			AND eik.source_id IS NOT NULL
			AND i.key_kind = ?
		GROUP BY
			eik.source_id,
			COALESCE(s.name, ''),
			COALESCE(s.url, ''),
			i.normalized_key,
			ev.id,
			ev.slug,
			ev.name,
			linked_ev.id,
			linked_ev.slug,
			linked_ev.publication_state,
			l.is_authoritative,
			l.updated_at
		ORDER BY COALESCE(s.name, ''), eik.source_id, i.normalized_key, COALESCE(ev.slug, ''), COALESCE(ev.name, ''), COALESCE(l.event_id, 0)
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld), string(domain.OriginLive), string(domain.PublicationStateWithheld), clusterID, string(seedstore.EventReviewIdentityKeyKindSource))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []seedstore.EventReviewClusterSourceIdentityLinkSummary
	for rows.Next() {
		var row eventReviewClusterSourceIdentityLinkRow
		if err := rows.Scan(
			&row.sourceID,
			&row.sourceName,
			&row.sourceURL,
			&row.sourceIdentityKey,
			&row.evidenceCount,
			&row.linkedEventID,
			&row.linkedEventSlug,
			&row.linkedEventTitle,
			&row.rawLinkedEventID,
			&row.rawLinkedEventSlug,
			&row.rawLinkedPublicationState,
			&row.resolvedFromWithheld,
			&row.authoritative,
			&row.linkUpdatedAt,
		); err != nil {
			return nil, err
		}
		links = append(links, row.toSummary())
	}
	return links, rows.Err()
}

func loadEventReviewClusterSourceIdentityChoiceSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewSourceIdentityChoice, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
			cluster_id,
			source_id,
			source_identity_key,
			selected,
			selection_reason,
			updated_at
		FROM event_review_source_identity_choices
		WHERE cluster_id = ?
		ORDER BY source_id, source_identity_key, id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var choices []seedstore.EventReviewSourceIdentityChoice
	for rows.Next() {
		var row seedstore.EventReviewSourceIdentityChoice
		var selected int
		var updatedAt string
		if err := rows.Scan(
			&row.ID,
			&row.ClusterID,
			&row.SourceID,
			&row.SourceIdentityKey,
			&selected,
			&row.SelectionReason,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		row.Selected = selected != 0
		var err error
		row.UpdatedAt, err = parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, err
		}
		choices = append(choices, row)
	}
	return choices, rows.Err()
}

func loadEventReviewClusterExactIdentityMatchSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterExactIdentityMatchSummary, error) {
	rows, err := q.QueryContext(ctx, `
		WITH exact_key_rows AS (
			SELECT
				ik.id AS identity_key_id,
				ik.identity_key_hash,
				ik.key_version,
				ik.normalized_key,
				NULL AS evidence_id
			FROM event_review_cluster_identity_keys cik
			JOIN event_review_identity_keys ik ON ik.id = cik.identity_key_id
			WHERE cik.cluster_id = ?
				AND cik.active = 1
				AND ik.key_kind = ?
			UNION ALL
			SELECT
				ik.id AS identity_key_id,
				ik.identity_key_hash,
				ik.key_version,
				ik.normalized_key,
				ce.evidence_id
			FROM event_review_cluster_evidence ce
			JOIN event_review_evidence_identity_keys eik ON eik.evidence_id = ce.evidence_id
			JOIN event_review_identity_keys ik ON ik.id = eik.identity_key_id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
				AND ik.key_kind = ?
		)
		SELECT
			k.identity_key_id,
			k.identity_key_hash,
			k.key_version,
			k.normalized_key,
			COUNT(DISTINCT k.evidence_id),
			ev.id,
			COALESCE(ev.slug, ''),
			COALESCE(ev.name, ''),
			COALESCE(v.slug, ''),
			ev.start_at
		FROM exact_key_rows k
		LEFT JOIN event_exact_identities ex ON ex.identity_key = k.normalized_key
			AND ex.active = 1
		LEFT JOIN events ev ON ev.id = ex.event_id
			AND ev.origin = ?
			AND TRIM(COALESCE(ev.publication_state, '')) <> ?
		LEFT JOIN venues v ON v.id = ev.venue_id
		GROUP BY
			k.identity_key_id,
			k.identity_key_hash,
			k.key_version,
			k.normalized_key,
			ev.id,
			ev.slug,
			ev.name,
			v.slug,
			ev.start_at
		ORDER BY k.normalized_key, k.identity_key_id, COALESCE(ev.id, 0)
	`, clusterID, string(seedstore.EventReviewIdentityKeyKindExact), clusterID, string(seedstore.EventReviewIdentityKeyKindExact), string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matches []seedstore.EventReviewClusterExactIdentityMatchSummary
	for rows.Next() {
		var row eventReviewClusterExactIdentityMatchRow
		if err := rows.Scan(
			&row.identityKeyID,
			&row.identityKeyHash,
			&row.keyVersion,
			&row.normalizedKey,
			&row.evidenceCount,
			&row.linkedEventID,
			&row.linkedEventSlug,
			&row.linkedEventTitle,
			&row.linkedEventVenueSlug,
			&row.linkedEventStartAt,
		); err != nil {
			return nil, err
		}
		matches = append(matches, row.toSummary())
	}
	return matches, rows.Err()
}

func (r eventReviewClusterObservationSummaryRow) toSummary() seedstore.EventReviewClusterObservationSummary {
	return seedstore.EventReviewClusterObservationSummary{
		ID:                        r.id,
		RunScope:                  strings.TrimSpace(r.runScope),
		SourceID:                  r.sourceID,
		SourceName:                strings.TrimSpace(r.sourceName),
		SourceURL:                 strings.TrimSpace(r.sourceURL),
		SourceIdentityKey:         strings.TrimSpace(r.sourceIdentityKey),
		SourceAuthority:           seedstore.SourceAuthority(strings.TrimSpace(r.sourceAuthority)),
		FieldName:                 strings.TrimSpace(r.fieldName),
		IncomingRaw:               strings.TrimSpace(r.incomingRaw),
		IncomingNormalized:        strings.TrimSpace(r.incomingNormalized),
		CanonicalBeforeRaw:        strings.TrimSpace(r.canonicalBeforeRaw),
		CanonicalBeforeNormalized: strings.TrimSpace(r.canonicalBeforeNormalized),
		Outcome:                   strings.TrimSpace(r.outcome),
		IsConflict:                r.isConflict != 0,
		CreatedAt:                 mustParseSeparationTimestamp(r.createdAt),
		UpdatedAt:                 mustParseSeparationTimestamp(r.updatedAt),
	}
}

func (r eventReviewClusterSourceIdentityLinkRow) toSummary() seedstore.EventReviewClusterSourceIdentityLinkSummary {
	summary := seedstore.EventReviewClusterSourceIdentityLinkSummary{
		SourceID:                  r.sourceID,
		SourceName:                strings.TrimSpace(r.sourceName),
		SourceURL:                 strings.TrimSpace(r.sourceURL),
		SourceIdentityKey:         strings.TrimSpace(r.sourceIdentityKey),
		EvidenceCount:             int(r.evidenceCount),
		LinkedEventSlug:           strings.TrimSpace(r.linkedEventSlug),
		LinkedEventTitle:          strings.TrimSpace(r.linkedEventTitle),
		RawLinkedEventSlug:        strings.TrimSpace(r.rawLinkedEventSlug),
		RawLinkedPublicationState: strings.TrimSpace(r.rawLinkedPublicationState),
		ResolvedFromWithheld:      r.resolvedFromWithheld != 0,
		Authoritative:             r.authoritative != 0,
	}
	if r.linkedEventID.Valid {
		summary.LinkedEventID = &r.linkedEventID.Int64
	}
	if r.rawLinkedEventID.Valid {
		summary.RawLinkedEventID = &r.rawLinkedEventID.Int64
	}
	if r.linkUpdatedAt.Valid {
		if parsed, err := parseRFC3339UTC(r.linkUpdatedAt.String); err == nil {
			summary.LinkUpdatedAt = &parsed
		}
	}
	return summary
}

func (r eventReviewClusterExactIdentityMatchRow) toSummary() seedstore.EventReviewClusterExactIdentityMatchSummary {
	summary := seedstore.EventReviewClusterExactIdentityMatchSummary{
		IdentityKeyID:        r.identityKeyID,
		IdentityKeyHash:      strings.TrimSpace(r.identityKeyHash),
		KeyVersion:           r.keyVersion,
		NormalizedKey:        strings.TrimSpace(r.normalizedKey),
		EvidenceCount:        r.evidenceCount,
		LinkedEventSlug:      strings.TrimSpace(r.linkedEventSlug),
		LinkedEventTitle:     strings.TrimSpace(r.linkedEventTitle),
		LinkedEventVenueSlug: strings.TrimSpace(r.linkedEventVenueSlug),
	}
	if r.linkedEventID.Valid {
		summary.LinkedEventID = &r.linkedEventID.Int64
	}
	if r.linkedEventStartAt.Valid {
		if parsed, err := parseRFC3339UTC(r.linkedEventStartAt.String); err == nil {
			summary.LinkedEventStartAt = &parsed
		}
	}
	return summary
}

func (r eventReviewClusterSeparationRow) toSummary() seedstore.EventReviewClusterSeparationSummary {
	return seedstore.EventReviewClusterSeparationSummary{
		ID: r.id,
		EndpointA: r.endpointSummary(
			r.endpointAKind,
			r.endpointAKey,
			r.endpointAEventID,
			r.endpointAEventSlug,
			r.endpointAEvidenceID,
			r.endpointAEvidenceFP,
			r.endpointAIdentityKeyID,
			r.endpointAIdentityHash,
			r.endpointAIdentityKind,
			r.endpointANormalizedKey,
			r.endpointACanonicalID,
			r.endpointACanonicalSlug,
		),
		EndpointB: r.endpointSummary(
			r.endpointBKind,
			r.endpointBKey,
			r.endpointBEventID,
			r.endpointBEventSlug,
			r.endpointBEvidenceID,
			r.endpointBEvidenceFP,
			r.endpointBIdentityKeyID,
			r.endpointBIdentityHash,
			r.endpointBIdentityKind,
			r.endpointBNormalizedKey,
			r.endpointBCanonicalID,
			r.endpointBCanonicalSlug,
		),
		Reason:    r.reason,
		CreatedAt: mustParseSeparationTimestamp(r.createdAt),
		UpdatedAt: mustParseSeparationTimestamp(r.updatedAt),
	}
}

func (r eventReviewClusterSeparationRow) endpointSummary(kindText, key string, eventID sql.NullInt64, eventSlug string, evidenceID sql.NullInt64, evidenceFingerprint string, identityKeyID sql.NullInt64, identityKeyHash, identityKeyKind, normalizedKey string, canonicalEventID sql.NullInt64, canonicalEventSlug string) seedstore.EventReviewSeparationEndpointSummary {
	summary := seedstore.EventReviewSeparationEndpointSummary{
		Kind:                seedstore.EventReviewSeparationEndpointKind(kindText),
		Key:                 key,
		EventSlug:           strings.TrimSpace(eventSlug),
		EvidenceFingerprint: strings.TrimSpace(evidenceFingerprint),
		IdentityKeyHash:     strings.TrimSpace(identityKeyHash),
		IdentityKeyKind:     seedstore.EventReviewIdentityKeyKind(strings.TrimSpace(identityKeyKind)),
		NormalizedKey:       strings.TrimSpace(normalizedKey),
		CanonicalEventSlug:  strings.TrimSpace(canonicalEventSlug),
	}
	if eventID.Valid {
		summary.EventID = &eventID.Int64
	}
	if evidenceID.Valid {
		summary.EvidenceID = &evidenceID.Int64
	}
	if identityKeyID.Valid {
		summary.IdentityKeyID = &identityKeyID.Int64
	}
	if canonicalEventID.Valid {
		summary.CanonicalEventID = &canonicalEventID.Int64
	}
	return summary
}

func mustParseSeparationTimestamp(value string) time.Time {
	parsed, err := parseRFC3339UTC(value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func loadEventReviewTitleRepairReadinessTx(ctx context.Context, q queryer, summary seedstore.EventReviewClusterSummary, draftChoices []seedstore.EventReviewClusterChoiceSummary) (*seedstore.EventReviewTitleRepairReadiness, error) {
	if summary.ConflictType != eventTitleRepairConflictType {
		return nil, nil
	}

	readiness := &seedstore.EventReviewTitleRepairReadiness{
		CanonicalEventID: 0,
	}
	if summary.CanonicalEventID != nil {
		readiness.CanonicalEventID = *summary.CanonicalEventID
	}
	if summary.CanonicalEventID == nil || *summary.CanonicalEventID <= 0 {
		readiness.BlockingReasons = append(readiness.BlockingReasons, "canonical event is missing")
	} else {
		record, ok, err := loadEventReviewTitleRepairEventByIDTx(ctx, q, *summary.CanonicalEventID)
		if err != nil {
			return nil, err
		}
		if !ok {
			readiness.BlockingReasons = append(readiness.BlockingReasons, "canonical event is missing")
		} else {
			readiness.CurrentTitle = strings.TrimSpace(record.name)
			readiness.CurrentSlug = strings.TrimSpace(record.slug)
			readiness.CurrentEventLive = isLiveNonWithheldEventRow(record.origin, record.publicationState)
			if !readiness.CurrentEventLive {
				readiness.BlockingReasons = append(readiness.BlockingReasons, "current event is not live/non-withheld")
			}
		}
	}

	readiness.DraftTitle = strings.TrimSpace(eventReviewChoiceValueByFieldName(draftChoices, "name"))
	readiness.DraftSlug = strings.TrimSpace(eventReviewChoiceValueByFieldName(draftChoices, "slug"))
	if readiness.DraftTitle == "" {
		readiness.BlockingReasons = append(readiness.BlockingReasons, "draft title is required")
	}
	if readiness.DraftSlug == "" {
		readiness.BlockingReasons = append(readiness.BlockingReasons, "draft slug is required")
	}

	supportedConflictReason := false
	switch summary.ConflictReason {
	case eventTitleRepairConflictReasonSupportingCleanTitle:
		supportedConflictReason = true
	case eventTitleRepairConflictReasonAuthoritativeSlugConflict:
		supportedConflictReason = true
	case "":
		readiness.BlockingReasons = append(readiness.BlockingReasons, "unsupported title repair conflict reason")
	default:
		readiness.BlockingReasons = append(readiness.BlockingReasons, "unsupported title repair conflict reason: "+summary.ConflictReason)
	}

	baseBlockingReasons := len(readiness.BlockingReasons)
	if readiness.DraftSlug != "" && readiness.CurrentSlug != "" && readiness.DraftSlug != readiness.CurrentSlug {
		conflict, ok, err := loadEventReviewTitleRepairEventBySlugTx(ctx, q, readiness.DraftSlug)
		if err != nil {
			return nil, err
		}
		if ok && isLiveNonWithheldEventRow(conflict.origin, conflict.publicationState) && (summary.CanonicalEventID == nil || conflict.id != *summary.CanonicalEventID) {
			readiness.SlugConflictEventID = &conflict.id
			readiness.SlugConflictEventSlug = strings.TrimSpace(conflict.slug)
			readiness.SlugConflictEventTitle = strings.TrimSpace(conflict.name)
			separated := false
			if summary.CanonicalEventID != nil && *summary.CanonicalEventID > 0 {
				separated, err = hasActiveEventReviewSeparationBetweenKeysTx(ctx, q, seedstore.EventReviewSeparationEventEndpointKey(*summary.CanonicalEventID), seedstore.EventReviewSeparationEventEndpointKey(conflict.id))
				if err != nil {
					return nil, err
				}
			}
			if separated {
				reason := "target slug conflict is already marked separate"
				readiness.BlockingReasons = append(readiness.BlockingReasons, reason)
				readiness.SlugConflictBlockingReasons = append(readiness.SlugConflictBlockingReasons, reason)
			} else {
				readiness.BlockingReasons = append(readiness.BlockingReasons, "target slug already belongs to another live event")
				if supportedConflictReason && baseBlockingReasons == 0 {
					readiness.SlugConflictResolutionAvailable = true
				}
			}
		}
	}

	if len(readiness.BlockingReasons) == 0 {
		readiness.Eligible = true
	}
	return readiness, nil
}

func loadEventReviewHistoricalDuplicateReadinessTx(ctx context.Context, q queryer, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, liveActions []seedstore.EventReviewClusterLiveActionSummary) (*seedstore.EventReviewHistoricalDuplicateReadiness, error) {
	if summary.ConflictType != historicalDuplicateRepairConflictType {
		return nil, nil
	}

	readiness := &seedstore.EventReviewHistoricalDuplicateReadiness{}
	actionByEventID := make(map[int64]seedstore.EventReviewClusterLiveActionSummary, len(liveActions))
	eventIDs := make([]int64, 0, len(evidence)+len(liveActions)+1)
	if summary.CanonicalEventID != nil {
		eventIDs = append(eventIDs, *summary.CanonicalEventID)
	}
	for _, row := range evidence {
		if row.EventID != nil {
			eventIDs = append(eventIDs, *row.EventID)
		}
	}
	for _, action := range liveActions {
		eventIDs = append(eventIDs, action.EventID)
		if action.EventID > 0 {
			actionByEventID[action.EventID] = action
		}
	}
	eventIDs = uniqueInt64s(eventIDs)

	for _, eventID := range eventIDs {
		eventReadiness := seedstore.EventReviewHistoricalDuplicateEventReadiness{EventID: eventID}
		if summary.CanonicalEventID != nil && *summary.CanonicalEventID == eventID {
			eventReadiness.Canonical = true
		}
		if action, ok := actionByEventID[eventID]; ok {
			eventReadiness.Action = action.Action
			eventReadiness.EventSlug = strings.TrimSpace(action.EventSlug)
		}
		record, ok, err := loadEventRecordByIDTx(ctx, q, eventID)
		if err != nil {
			return nil, err
		}
		if !ok {
			appendUniqueImportReadinessReason(&eventReadiness.BlockingReasons, "event is missing")
			appendUniqueImportReadinessReason(&readiness.KeepSeparateBlockingReasons, "all events must exist")
			readiness.Events = append(readiness.Events, eventReadiness)
			continue
		}
		if eventReadiness.EventSlug == "" {
			eventReadiness.EventSlug = strings.TrimSpace(record.Event.Slug)
		}
		state := normalizedPublicationState(record.Event.PublicationState)
		eventReadiness.PublicationState = string(state)
		eventReadiness.Live = isLiveNonWithheldEventRow(string(record.Event.Origin), string(record.Event.PublicationState))
		eventReadiness.CanonicalEligible = eventReadiness.Live && state == domain.PublicationStateReviewed
		if !eventReadiness.Live {
			appendUniqueImportReadinessReason(&eventReadiness.BlockingReasons, "event is not live/non-withheld")
		}
		if state != domain.PublicationStateReviewed && state != domain.PublicationStateProvisional {
			appendUniqueImportReadinessReason(&eventReadiness.BlockingReasons, "event is not reviewed or provisional")
		}
		eventReadiness.KeepEligible = len(eventReadiness.BlockingReasons) == 0
		if !eventReadiness.KeepEligible {
			appendUniqueImportReadinessReason(&readiness.KeepSeparateBlockingReasons, "all events must be live reviewed/provisional")
		}
		readiness.Events = append(readiness.Events, eventReadiness)
	}

	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		appendUniqueImportReadinessReason(&readiness.LiveActionBlockingReasons, "cluster is not open")
		appendUniqueImportReadinessReason(&readiness.KeepSeparateBlockingReasons, "cluster is not open")
	}
	if len(eventIDs) < 2 {
		appendUniqueImportReadinessReason(&readiness.KeepSeparateBlockingReasons, "at least two historical duplicate events are required")
	}
	liveActionBlockers, err := historicalDuplicateLiveActionBlockingReasonsTx(ctx, q, summary, liveActions)
	if err != nil {
		return nil, err
	}
	for _, reason := range liveActionBlockers {
		appendUniqueImportReadinessReason(&readiness.LiveActionBlockingReasons, reason)
	}
	readiness.CanResolveLiveActions = len(readiness.LiveActionBlockingReasons) == 0
	readiness.CanKeepAllSeparate = len(readiness.KeepSeparateBlockingReasons) == 0
	return readiness, nil
}

func historicalDuplicateLiveActionBlockingReasonsTx(ctx context.Context, q queryer, summary seedstore.EventReviewClusterSummary, liveActions []seedstore.EventReviewClusterLiveActionSummary) ([]string, error) {
	var blockers []string
	appendBlocker := func(reason string) {
		appendUniqueImportReadinessReason(&blockers, reason)
	}
	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		appendBlocker("cluster is not open")
	}
	if summary.CanonicalEventID == nil || *summary.CanonicalEventID <= 0 {
		appendBlocker("canonical event is missing")
	}
	if len(liveActions) == 0 {
		appendBlocker("no stored live actions")
	}
	if summary.CanonicalEventID == nil || *summary.CanonicalEventID <= 0 || len(liveActions) == 0 {
		return blockers, nil
	}

	canonicalID := *summary.CanonicalEventID
	canonicalRecord, ok, err := loadEventRecordByIDTx(ctx, q, canonicalID)
	if err != nil {
		return nil, err
	}
	if !ok {
		appendBlocker("canonical event is missing")
	} else {
		if !isLiveNonWithheldEventRow(string(canonicalRecord.Event.Origin), string(canonicalRecord.Event.PublicationState)) {
			appendBlocker("canonical event is not live/non-withheld")
		}
		if normalizedPublicationState(canonicalRecord.Event.PublicationState) != domain.PublicationStateReviewed {
			appendBlocker("canonical event is not reviewed")
		}
	}

	seenEventIDs := make(map[int64]struct{}, len(liveActions))
	keepCount := 0
	withholdCount := 0
	for _, action := range liveActions {
		if action.EventID <= 0 {
			appendBlocker("live action event is missing")
			continue
		}
		if _, ok := seenEventIDs[action.EventID]; ok {
			appendBlocker("duplicate live action event")
			continue
		}
		seenEventIDs[action.EventID] = struct{}{}
		if !action.Action.Valid() {
			appendBlocker("unsupported live action")
			continue
		}
		switch action.Action {
		case seedstore.EventReviewLiveActionKindKeepSeparate:
			keepCount++
			if action.EventID != canonicalID {
				appendBlocker("keep_separate action must target canonical event")
			}
		case seedstore.EventReviewLiveActionKindWithholdDuplicate:
			withholdCount++
			if action.EventID == canonicalID {
				appendBlocker("withhold_duplicate action must target a non-canonical event")
				continue
			}
			record, ok, err := loadEventRecordByIDTx(ctx, q, action.EventID)
			if err != nil {
				return nil, err
			}
			if !ok {
				appendBlocker("withhold event is missing")
				continue
			}
			state := normalizedPublicationState(record.Event.PublicationState)
			if state == domain.PublicationStateWithheld {
				if historicalDuplicateRepairCanonicalEventIDFromDB(ctx, q, action.EventID) != canonicalID {
					appendBlocker("withhold event is already withheld to another canonical event")
				}
				continue
			}
			if record.Event.Origin != domain.OriginLive || (state != domain.PublicationStateProvisional && state != domain.PublicationStateReviewed) {
				appendBlocker("withhold event is not provisional or reviewed live")
				continue
			}
			if reason, reviewNeeded, err := historicalDuplicateRepairSourceLinkGuardTx(ctx, q, action.EventID, canonicalID); err != nil {
				return nil, err
			} else if reviewNeeded {
				appendBlocker(reason)
			}
			if reason, reviewNeeded, err := historicalDuplicateRepairAliasConflictTx(ctx, q, record.Event.Slug, canonicalID); err != nil {
				return nil, err
			} else if reviewNeeded {
				appendBlocker(reason)
			}
		}
	}
	if keepCount != 1 {
		appendBlocker("exactly one keep_separate action for canonical event is required")
	}
	if withholdCount == 0 {
		appendBlocker("at least one withhold_duplicate action is required")
	}
	return blockers, nil
}

func historicalDuplicateReadinessEventIDs(readiness *seedstore.EventReviewHistoricalDuplicateReadiness) []int64 {
	if readiness == nil {
		return nil
	}
	ids := make([]int64, 0, len(readiness.Events))
	for _, event := range readiness.Events {
		ids = append(ids, event.EventID)
	}
	return uniqueInt64s(ids)
}

type eventReviewImportCandidatePayload struct {
	SourceAuthority string                                      `json:"source_authority"`
	SourceName      string                                      `json:"source_name,omitempty"`
	SourceURL       string                                      `json:"source_url,omitempty"`
	ExternalID      string                                      `json:"candidate_external_id"`
	Title           string                                      `json:"candidate_title"`
	VenueSlug       string                                      `json:"candidate_venue_slug"`
	VenueText       string                                      `json:"candidate_venue_text"`
	RoomText        string                                      `json:"candidate_room_text,omitempty"`
	Rooms           []eventReviewImportReviewListingRoomPayload `json:"candidate_rooms,omitempty"`
	StartAt         string                                      `json:"candidate_start_at"`
	EndAt           string                                      `json:"candidate_end_at"`
	Genre           string                                      `json:"candidate_genre,omitempty"`
	Status          string                                      `json:"candidate_status,omitempty"`
	Description     string                                      `json:"candidate_description,omitempty"`
	ImageURL        string                                      `json:"candidate_image_url,omitempty"`
	ImageSourceURL  string                                      `json:"candidate_image_source_url,omitempty"`
	ImageAlt        string                                      `json:"candidate_image_alt,omitempty"`
	ImageWidth      int                                         `json:"candidate_image_width,omitempty"`
	ImageHeight     int                                         `json:"candidate_image_height,omitempty"`
	ImageFocusX     int                                         `json:"candidate_image_focus_x,omitempty"`
	ImageFocusY     int                                         `json:"candidate_image_focus_y,omitempty"`
	CalendarURL     string                                      `json:"calendar_url"`
	Provenance      string                                      `json:"provenance,omitempty"`
}

type eventReviewImportComparisonCandidate struct {
	EvidenceID          int64
	EvidenceFingerprint string
	SourceID            int64
	SourceName          string
	SourceURL           string
	EventID             *int64
	EventSlug           string
	RawSourceName       string
	RawSourceURL        string
	RawExternalID       string
	RawCalendarURL      string
	RawTitle            string
	RawVenueSlug        string
	RawVenueText        string
	RawStartAt          string
	NormalizedTitle     string
	NormalizedVenueSlug string
	NormalizedDate      string
	NormalizedStart     string
	ExactIdentity       string
	TitleWarning        string
	VenueWarning        string
	StartWarning        string
	ExactWarning        string
	ParseWarning        string
}

func newEventReviewImportComparisonCandidate(row seedstore.EventReviewClusterEvidenceSummary, payload eventReviewImportCandidatePayload, candidate seedstore.EventReviewImportCandidateSummary) eventReviewImportComparisonCandidate {
	analysis := eventReviewImportComparisonCandidate{
		EvidenceID:          row.EvidenceID,
		EvidenceFingerprint: row.EvidenceFingerprint,
		SourceID:            row.SourceID,
		SourceName:          row.SourceName,
		SourceURL:           row.SourceURL,
		EventID:             row.EventID,
		EventSlug:           row.EventSlug,
		RawSourceName:       row.SourceName,
		RawSourceURL:        row.SourceURL,
		RawExternalID:       strings.TrimSpace(payload.ExternalID),
		RawCalendarURL:      strings.TrimSpace(payload.CalendarURL),
		RawTitle:            strings.TrimSpace(payload.Title),
		RawVenueSlug:        strings.TrimSpace(payload.VenueSlug),
		RawVenueText:        strings.TrimSpace(payload.VenueText),
		RawStartAt:          strings.TrimSpace(payload.StartAt),
	}

	venueSlug := analysis.RawVenueSlug
	if venueSlug == "" && analysis.RawVenueText != "" {
		venueSlug = ingest.VenueSlugFromText(analysis.RawVenueText)
		if venueSlug != "" {
			analysis.VenueWarning = "venue normalized from raw text"
		}
	}
	if venueSlug != "" {
		analysis.NormalizedVenueSlug = venueSlug
	}
	if analysis.RawTitle != "" && analysis.NormalizedVenueSlug != "" {
		analysis.NormalizedTitle = normalizeExactIdentityCleanTitle(ingest.CleanEventTitleForVenue(analysis.RawTitle, analysis.NormalizedVenueSlug))
	}
	if candidate.StartAt != nil {
		start := candidate.StartAt.UTC()
		analysis.NormalizedDate = start.Format("2006-01-02")
		analysis.NormalizedStart = formatRFC3339UTC(start)
	}
	if analysis.NormalizedTitle != "" && analysis.NormalizedVenueSlug != "" && analysis.NormalizedStart != "" {
		if start, err := parseRFC3339UTC(analysis.NormalizedStart); err == nil {
			analysis.ExactIdentity = buildExactIdentityKey(exactIdentityKeyVersion, analysis.NormalizedVenueSlug, start.UTC(), analysis.NormalizedTitle)
		}
	}
	if analysis.NormalizedTitle == "" {
		analysis.TitleWarning = "candidate title is required"
	}
	if analysis.NormalizedVenueSlug == "" {
		analysis.VenueWarning = "candidate venue is required"
	}
	if analysis.NormalizedStart == "" {
		analysis.StartWarning = "candidate start is required"
	}
	if analysis.ExactIdentity == "" {
		analysis.ExactWarning = "exact identity could not be derived"
	}
	return analysis
}

func buildEventReviewImportComparisonReadiness(summary seedstore.EventReviewClusterSummary, candidates []seedstore.EventReviewImportCandidateSummary, analyses []eventReviewImportComparisonCandidate) ([]seedstore.EventReviewImportIdentityRow, []seedstore.EventReviewImportComparisonRow, []string, bool) {
	var blockers []string
	appendComparisonBlocker := func(reason string) {
		appendUniqueImportReadinessReason(&blockers, reason)
	}

	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		appendComparisonBlocker("cluster is not open")
	}
	if summary.CanonicalEventID != nil {
		appendComparisonBlocker("canonical event is already set")
	}
	if len(candidates) < 2 {
		appendComparisonBlocker("comparison requires at least two active evidence rows")
	}
	for _, candidate := range candidates {
		if candidate.EventID != nil {
			appendComparisonBlocker("evidence already references existing event")
			break
		}
	}
	if len(analyses) == 0 {
		appendComparisonBlocker("payload could not be parsed")
	}
	for _, analysis := range analyses {
		if analysis.ParseWarning != "" {
			appendComparisonBlocker("payload could not be parsed")
		}
		if analysis.NormalizedTitle == "" {
			appendComparisonBlocker("candidate title is required")
		}
		if analysis.NormalizedVenueSlug == "" {
			appendComparisonBlocker("candidate venue is required")
		}
		if analysis.NormalizedStart == "" {
			appendComparisonBlocker("candidate start is required")
		}
	}

	identityRows := []seedstore.EventReviewImportIdentityRow{
		{FieldName: "clean_title", Label: "Clean title", Values: make([]seedstore.EventReviewImportIdentityValue, 0, len(analyses))},
		{FieldName: "venue_slug", Label: "Venue slug", Values: make([]seedstore.EventReviewImportIdentityValue, 0, len(analyses))},
		{FieldName: "date", Label: "Date", Values: make([]seedstore.EventReviewImportIdentityValue, 0, len(analyses))},
		{FieldName: "start_time", Label: "Start time", Values: make([]seedstore.EventReviewImportIdentityValue, 0, len(analyses))},
		{FieldName: "exact_identity", Label: "Exact identity", Values: make([]seedstore.EventReviewImportIdentityValue, 0, len(analyses))},
	}
	rawRows := []seedstore.EventReviewImportComparisonRow{
		{FieldName: "source", Label: "Source", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
		{FieldName: "source_url", Label: "Source URL", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
		{FieldName: "external_id", Label: "External ID", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
		{FieldName: "calendar_url", Label: "Calendar URL", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
		{FieldName: "raw_title", Label: "Raw title", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
		{FieldName: "raw_venue_text", Label: "Raw venue text", Values: make([]seedstore.EventReviewImportComparisonValue, 0, len(analyses))},
	}
	for _, analysis := range analyses {
		identityRows[0].Values = append(identityRows[0].Values, seedstore.EventReviewImportIdentityValue{
			EvidenceID: analysis.EvidenceID,
			Normalized: analysis.NormalizedTitle,
			Raw:        analysis.RawTitle,
			Warning:    analysis.TitleWarning,
		})
		identityRows[1].Values = append(identityRows[1].Values, seedstore.EventReviewImportIdentityValue{
			EvidenceID: analysis.EvidenceID,
			Normalized: analysis.NormalizedVenueSlug,
			Raw:        firstNonEmptyImportReviewText(analysis.RawVenueSlug, analysis.RawVenueText),
			Warning:    analysis.VenueWarning,
		})
		identityRows[2].Values = append(identityRows[2].Values, seedstore.EventReviewImportIdentityValue{
			EvidenceID: analysis.EvidenceID,
			Normalized: analysis.NormalizedDate,
			Raw:        analysis.RawStartAt,
			Warning:    analysis.StartWarning,
		})
		identityRows[3].Values = append(identityRows[3].Values, seedstore.EventReviewImportIdentityValue{
			EvidenceID: analysis.EvidenceID,
			Normalized: analysis.NormalizedStart,
			Raw:        analysis.RawStartAt,
			Warning:    analysis.StartWarning,
		})
		identityRows[4].Values = append(identityRows[4].Values, seedstore.EventReviewImportIdentityValue{
			EvidenceID: analysis.EvidenceID,
			Normalized: analysis.ExactIdentity,
			Raw:        exactIdentityRawValue(analysis),
			Warning:    analysis.ExactWarning,
		})

		rawRows[0].Values = append(rawRows[0].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawSourceName})
		rawRows[1].Values = append(rawRows[1].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawSourceURL})
		rawRows[2].Values = append(rawRows[2].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawExternalID})
		rawRows[3].Values = append(rawRows[3].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawCalendarURL})
		rawRows[4].Values = append(rawRows[4].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawTitle})
		rawRows[5].Values = append(rawRows[5].Values, seedstore.EventReviewImportComparisonValue{EvidenceID: analysis.EvidenceID, Value: analysis.RawVenueText})
	}

	for i := range identityRows {
		identityRows[i].Consensus = consensusForImportIdentityValues(identityRows[i].Values)
	}
	for i := range rawRows {
		rawRows[i].Consensus = consensusForImportComparisonValues(rawRows[i].Values)
	}

	return identityRows, rawRows, blockers, len(blockers) == 0
}

func consensusForImportIdentityValues(values []seedstore.EventReviewImportIdentityValue) bool {
	if len(values) == 0 {
		return false
	}
	first := values[0].Normalized
	if first == "" {
		return false
	}
	for _, value := range values[1:] {
		if value.Normalized != first {
			return false
		}
	}
	return true
}

func consensusForImportComparisonValues(values []seedstore.EventReviewImportComparisonValue) bool {
	if len(values) == 0 {
		return false
	}
	first := values[0].Value
	for _, value := range values[1:] {
		if value.Value != first {
			return false
		}
	}
	return true
}

func appendUniqueImportReadinessReason(reasons *[]string, reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	for _, existing := range *reasons {
		if existing == reason {
			return
		}
	}
	*reasons = append(*reasons, reason)
}

func exactIdentityRawValue(candidate eventReviewImportComparisonCandidate) string {
	parts := make([]string, 0, 3)
	if candidate.NormalizedVenueSlug != "" {
		parts = append(parts, "venue="+candidate.NormalizedVenueSlug)
	}
	if candidate.NormalizedStart != "" {
		parts = append(parts, "start="+candidate.NormalizedStart)
	}
	if candidate.NormalizedTitle != "" {
		parts = append(parts, "title="+candidate.NormalizedTitle)
	}
	return strings.Join(parts, " ")
}

func loadEventReviewImportReadinessTx(summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, evidenceIdentityKeys []seedstore.EventReviewEvidenceIdentityKeySummary, exactIdentityMatches []seedstore.EventReviewClusterExactIdentityMatchSummary, sourceIdentityLinks []seedstore.EventReviewClusterSourceIdentityLinkSummary, sourceIdentityChoices []seedstore.EventReviewSourceIdentityChoice) *seedstore.EventReviewImportReadiness {
	if summary.ConflictType != seedstore.EventReviewConflictTypeImportReview || summary.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return nil
	}

	readiness := &seedstore.EventReviewImportReadiness{
		CandidateCount: len(evidence),
	}
	exactMatchByKey := make(map[string]seedstore.EventReviewClusterExactIdentityMatchSummary, len(exactIdentityMatches))
	for _, match := range exactIdentityMatches {
		if key := strings.TrimSpace(match.NormalizedKey); key != "" {
			exactMatchByKey[key] = match
		}
	}
	sourceLinkByKey := make(map[string]seedstore.EventReviewClusterSourceIdentityLinkSummary, len(sourceIdentityLinks))
	for _, link := range sourceIdentityLinks {
		if link.SourceID > 0 && strings.TrimSpace(link.SourceIdentityKey) != "" {
			sourceLinkByKey[importCandidateSourceIdentityKey(link.SourceID, link.SourceIdentityKey)] = link
		}
	}
	sourceChoiceByKey := make(map[string]seedstore.EventReviewSourceIdentityChoice, len(sourceIdentityChoices))
	for _, choice := range sourceIdentityChoices {
		if choice.SourceID > 0 && strings.TrimSpace(choice.SourceIdentityKey) != "" {
			sourceChoiceByKey[importCandidateSourceIdentityKey(choice.SourceID, choice.SourceIdentityKey)] = choice
		}
	}
	candidateStatuses := make([]seedstore.EventReviewImportCandidateIdentityStatus, 0, len(evidence))
	candidateStatusIndex := make(map[int64]int, len(evidence))
	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "cluster is not open")
	}
	if summary.CanonicalEventID != nil {
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "canonical event is already set")
	}
	if len(evidence) != 1 {
		if len(evidence) == 0 {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "exactly one active evidence row is required")
		} else {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "multiple active evidence rows are present")
		}
	}

	var comparisonCandidates []eventReviewImportComparisonCandidate
	for _, row := range evidence {
		candidate := seedstore.EventReviewImportCandidateSummary{
			EvidenceID:          row.EvidenceID,
			EvidenceFingerprint: row.EvidenceFingerprint,
			SourceID:            row.SourceID,
			SourceName:          row.SourceName,
			SourceURL:           row.SourceURL,
			EventID:             row.EventID,
			EventSlug:           row.EventSlug,
			CalendarURL:         "",
		}
		status := seedstore.EventReviewImportCandidateIdentityStatus{
			EvidenceID:          row.EvidenceID,
			EvidenceFingerprint: row.EvidenceFingerprint,
			SourceID:            row.SourceID,
			SourceName:          row.SourceName,
		}

		if row.EventID != nil {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "evidence already references existing event")
		}

		var payload eventReviewImportCandidatePayload
		if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "payload could not be parsed")
			readiness.PayloadWarnings = append(readiness.PayloadWarnings, "evidence #"+strconv.FormatInt(row.EvidenceID, 10)+": "+err.Error())
			readiness.Candidates = append(readiness.Candidates, candidate)
			status.ParseWarning = "payload could not be parsed"
			candidateStatuses = append(candidateStatuses, status)
			candidateStatusIndex[row.EvidenceID] = len(candidateStatuses) - 1
			comparisonCandidates = append(comparisonCandidates, eventReviewImportComparisonCandidate{
				EvidenceID:          row.EvidenceID,
				EvidenceFingerprint: row.EvidenceFingerprint,
				SourceID:            row.SourceID,
				SourceName:          row.SourceName,
				SourceURL:           row.SourceURL,
				EventID:             row.EventID,
				EventSlug:           row.EventSlug,
				RawSourceName:       row.SourceName,
				RawSourceURL:        row.SourceURL,
				ParseWarning:        "payload could not be parsed",
			})
			continue
		}

		candidate.SourceAuthority = seedstore.SourceAuthority(strings.TrimSpace(payload.SourceAuthority))
		candidate.ExternalID = strings.TrimSpace(payload.ExternalID)
		candidate.Title = strings.TrimSpace(payload.Title)
		candidate.VenueSlug = strings.TrimSpace(payload.VenueSlug)
		candidate.VenueText = strings.TrimSpace(payload.VenueText)
		candidate.CalendarURL = strings.TrimSpace(payload.CalendarURL)
		status.Title = candidate.Title
		if candidate.VenueSlug != "" {
			status.VenueSlug = candidate.VenueSlug
		} else if candidate.VenueText != "" {
			status.VenueSlug = ingest.VenueSlugFromText(candidate.VenueText)
		}
		startValid := false
		startBlockAdded := false
		if startText := strings.TrimSpace(payload.StartAt); startText != "" {
			startAt, err := parseRFC3339UTC(startText)
			if err != nil {
				readiness.BlockingReasons = append(readiness.BlockingReasons, "candidate start is required")
				startBlockAdded = true
			} else {
				candidate.StartAt = &startAt
				status.StartAt = &startAt
				startValid = true
			}
		}
		if endText := strings.TrimSpace(payload.EndAt); endText != "" {
			endAt, err := parseRFC3339UTC(endText)
			if err == nil {
				candidate.EndAt = &endAt
			}
		}
		if candidate.Title == "" {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate title is required")
		}
		if !startValid && !startBlockAdded {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate start is required")
		}
		if candidate.VenueSlug == "" && candidate.VenueText == "" {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate venue is required")
		}
		readiness.Candidates = append(readiness.Candidates, candidate)
		candidateStatuses = append(candidateStatuses, status)
		candidateStatusIndex[row.EvidenceID] = len(candidateStatuses) - 1
		comparisonCandidates = append(comparisonCandidates, newEventReviewImportComparisonCandidate(row, payload, candidate))
	}

	if len(readiness.BlockingReasons) == 0 {
		readiness.NewListingScope = true
	}
	readiness.IdentityRows, readiness.RawRows, readiness.ComparisonBlockingReasons, readiness.CandidateComparisonScope = buildEventReviewImportComparisonReadiness(summary, readiness.Candidates, comparisonCandidates)
	readiness.CandidateIdentityStatuses = buildEventReviewCandidateIdentityStatuses(candidateStatuses, candidateStatusIndex, evidenceIdentityKeys, exactMatchByKey, sourceLinkByKey, sourceChoiceByKey)
	readiness.ExistingEventTargets = normalizeEventReviewImportExistingEventTargets(buildEventReviewImportExistingEventTargets(summary, readiness.Candidates, readiness.CandidateIdentityStatuses))
	readiness.SelectedCandidateReadiness = buildEventReviewSelectedCandidateReadiness(summary, readiness.Candidates, readiness.CandidateIdentityStatuses)
	return readiness
}

func buildEventReviewImportExistingEventTargets(summary seedstore.EventReviewClusterSummary, candidates []seedstore.EventReviewImportCandidateSummary, statuses []seedstore.EventReviewImportCandidateIdentityStatus) []seedstore.EventReviewImportExistingEventTarget {
	type targetKey struct {
		evidenceID int64
		eventID    int64
		basis      seedstore.EventReviewImportTargetBasis
	}
	candidateAuthorityByEvidenceID := make(map[int64]seedstore.SourceAuthority, len(candidates))
	for _, candidate := range candidates {
		candidateAuthorityByEvidenceID[candidate.EvidenceID] = candidate.SourceAuthority
	}
	targets := make(map[targetKey]*seedstore.EventReviewImportExistingEventTarget)
	addTarget := func(target seedstore.EventReviewImportExistingEventTarget) {
		if target.EvidenceID <= 0 || target.EventID <= 0 || !target.TargetBasis.Valid() {
			return
		}
		key := targetKey{evidenceID: target.EvidenceID, eventID: target.EventID, basis: target.TargetBasis}
		existing := targets[key]
		if existing == nil {
			target.SourceIdentityKeys = normalizedImportReadinessStrings(target.SourceIdentityKeys)
			target.ExactIdentityKeys = normalizedImportReadinessStrings(target.ExactIdentityKeys)
			target.BlockingReasons = normalizedImportReadinessStrings(target.BlockingReasons)
			targets[key] = &target
			return
		}
		existing.SourceIdentityKeys = normalizedImportReadinessStrings(append(existing.SourceIdentityKeys, target.SourceIdentityKeys...))
		existing.ExactIdentityKeys = normalizedImportReadinessStrings(append(existing.ExactIdentityKeys, target.ExactIdentityKeys...))
		existing.BlockingReasons = normalizedImportReadinessStrings(append(existing.BlockingReasons, target.BlockingReasons...))
		if existing.EventSlug == "" {
			existing.EventSlug = target.EventSlug
		}
		if existing.EventTitle == "" {
			existing.EventTitle = target.EventTitle
		}
	}

	for _, candidate := range candidates {
		if candidate.EventID != nil {
			addTarget(seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:          candidate.EvidenceID,
				EvidenceFingerprint: candidate.EvidenceFingerprint,
				EventID:             *candidate.EventID,
				EventSlug:           candidate.EventSlug,
				TargetBasis:         seedstore.EventReviewImportTargetBasisEvidenceEvent,
			})
		}
		if summary.CanonicalEventID != nil {
			addTarget(seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:          candidate.EvidenceID,
				EvidenceFingerprint: candidate.EvidenceFingerprint,
				EventID:             *summary.CanonicalEventID,
				EventSlug:           summary.CanonicalEventSlug,
				EventTitle:          summary.DisplayTitle,
				TargetBasis:         seedstore.EventReviewImportTargetBasisCanonicalEvent,
			})
		}
	}

	for _, status := range statuses {
		sourceChoicesPresent := eventReviewImportSourceChoicesPresent(status.SourceKeys)
		for _, exactKey := range status.ExactKeys {
			if exactKey.LinkedEventID == nil {
				continue
			}
			addTarget(seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:          status.EvidenceID,
				EvidenceFingerprint: status.EvidenceFingerprint,
				EventID:             *exactKey.LinkedEventID,
				EventSlug:           exactKey.LinkedEventSlug,
				EventTitle:          exactKey.LinkedEventTitle,
				TargetBasis:         seedstore.EventReviewImportTargetBasisExactIdentity,
				ExactIdentityKeys:   []string{exactKey.NormalizedKey},
			})
		}
		for _, sourceKey := range status.SourceKeys {
			if sourceKey.LinkedEventID == nil {
				continue
			}
			var blockingReasons []string
			if sourceChoicesPresent && !sourceKey.ChoiceSelected {
				blockingReasons = append(blockingReasons, "source identity is not selected")
			}
			addTarget(seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:                status.EvidenceID,
				EvidenceFingerprint:       status.EvidenceFingerprint,
				EventID:                   *sourceKey.LinkedEventID,
				EventSlug:                 sourceKey.LinkedEventSlug,
				EventTitle:                sourceKey.LinkedEventTitle,
				TargetBasis:               seedstore.EventReviewImportTargetBasisSourceIdentity,
				SourceIdentityKeys:        []string{sourceKey.SourceIdentityKey},
				RawLinkedEventID:          sourceKey.RawLinkedEventID,
				RawLinkedEventSlug:        sourceKey.RawLinkedEventSlug,
				RawLinkedPublicationState: sourceKey.RawLinkedPublicationState,
				ResolvedFromWithheld:      sourceKey.ResolvedFromWithheld,
				BlockingReasons:           blockingReasons,
			})
		}
	}

	out := make([]seedstore.EventReviewImportExistingEventTarget, 0, len(targets))
	for _, target := range targets {
		if candidateAuthorityByEvidenceID[target.EvidenceID] != "" &&
			candidateAuthorityByEvidenceID[target.EvidenceID] != seedstore.SourceAuthoritySupporting {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "candidate is not a supporting source")
		}
		out = append(out, *target)
	}
	return applyEventReviewImportUnresolvedSourceLinkBlockers(out, statuses)
}

func eventReviewImportSourceChoicesPresent(sourceKeys []seedstore.EventReviewImportCandidateSourceIdentityStatus) bool {
	for _, sourceKey := range sourceKeys {
		if sourceKey.ChoiceUpdatedAt != nil {
			return true
		}
	}
	return false
}

func applyEventReviewImportExistingTargetBlockersTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, targets []seedstore.EventReviewImportExistingEventTarget) ([]seedstore.EventReviewImportExistingEventTarget, error) {
	if len(targets) == 0 {
		return targets, nil
	}
	cluster := seedstore.EventReviewCluster{
		ID:               summary.ID,
		Status:           summary.Status,
		Version:          summary.Version,
		CanonicalEventID: summary.CanonicalEventID,
		ConflictType:     summary.ConflictType,
		ConflictReason:   summary.ConflictReason,
	}
	evidenceByID := make(map[int64]seedstore.EventReviewClusterEvidenceSummary, len(evidence))
	for _, row := range evidence {
		evidenceByID[row.EvidenceID] = row
	}
	type materialResult struct {
		material importReviewCandidateMaterial
		err      error
		loaded   bool
	}
	materialByEvidenceID := make(map[int64]materialResult, len(evidence))
	out := make([]seedstore.EventReviewImportExistingEventTarget, len(targets))
	copy(out, targets)
	now := time.Now().UTC()
	for i := range out {
		target := &out[i]
		record, ok, err := loadEventRecordByIDTx(ctx, tx, target.EventID)
		if err != nil {
			return nil, err
		}
		if !ok {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "target event is missing")
		} else {
			if target.EventSlug == "" {
				target.EventSlug = record.Event.Slug
			}
			if target.EventTitle == "" {
				target.EventTitle = record.Event.Name
			}
			if target.PublicationState == "" {
				target.PublicationState = string(normalizedPublicationState(record.Event.PublicationState))
			}
			targetState := normalizedPublicationState(record.Event.PublicationState)
			if record.Event.Origin != domain.OriginLive || targetState == domain.PublicationStateWithheld {
				appendUniqueImportReadinessReason(&target.BlockingReasons, "target event is not live/non-withheld")
			} else if targetState != domain.PublicationStateReviewed && targetState != domain.PublicationStateProvisional {
				appendUniqueImportReadinessReason(&target.BlockingReasons, "target event is not reviewed or provisional")
			}
		}

		row, ok := evidenceByID[target.EvidenceID]
		if !ok {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "evidence is not active")
			continue
		}
		result := materialByEvidenceID[target.EvidenceID]
		if !result.loaded {
			result.loaded = true
			result.material, result.err = buildImportReviewCandidateMaterialTx(ctx, tx, s, cluster, row, nil, "import_review_existing_target_readiness", reviewSourceIdentitySupporting, now)
			materialByEvidenceID[target.EvidenceID] = result
		}
		if result.err != nil {
			if isImportReviewCandidateMaterializationError(result.err) {
				appendUniqueImportReadinessReason(&target.BlockingReasons, "candidate payload could not be materialized")
				continue
			}
			return nil, result.err
		}
		if result.material.SourceAuthority != seedstore.SourceAuthoritySupporting {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "candidate is not a supporting source")
		}
		if len(target.SourceIdentityKeys) == 0 && len(result.material.SourceCtx.Identities.Keys()) == 0 {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "candidate has no stable source identity keys")
		}
	}
	return out, nil
}

func normalizeEventReviewImportExistingEventTargets(targets []seedstore.EventReviewImportExistingEventTarget) []seedstore.EventReviewImportExistingEventTarget {
	type targetKey struct {
		evidenceID int64
		eventID    int64
		basis      seedstore.EventReviewImportTargetBasis
	}
	merged := make(map[targetKey]*seedstore.EventReviewImportExistingEventTarget, len(targets))
	for _, target := range targets {
		if target.EvidenceID <= 0 || target.EventID <= 0 || !target.TargetBasis.Valid() {
			continue
		}
		key := targetKey{evidenceID: target.EvidenceID, eventID: target.EventID, basis: target.TargetBasis}
		existing := merged[key]
		if existing == nil {
			target.SourceIdentityKeys = normalizedImportReadinessStrings(target.SourceIdentityKeys)
			target.ExactIdentityKeys = normalizedImportReadinessStrings(target.ExactIdentityKeys)
			target.BlockingReasons = normalizedImportReadinessStrings(target.BlockingReasons)
			merged[key] = &target
			continue
		}
		existing.SourceIdentityKeys = normalizedImportReadinessStrings(append(existing.SourceIdentityKeys, target.SourceIdentityKeys...))
		existing.ExactIdentityKeys = normalizedImportReadinessStrings(append(existing.ExactIdentityKeys, target.ExactIdentityKeys...))
		existing.BlockingReasons = normalizedImportReadinessStrings(append(existing.BlockingReasons, target.BlockingReasons...))
		if existing.EventSlug == "" {
			existing.EventSlug = target.EventSlug
		}
		if existing.EventTitle == "" {
			existing.EventTitle = target.EventTitle
		}
		if existing.PublicationState == "" {
			existing.PublicationState = target.PublicationState
		}
		if existing.RawLinkedEventID == nil {
			existing.RawLinkedEventID = target.RawLinkedEventID
		}
		if existing.RawLinkedEventSlug == "" {
			existing.RawLinkedEventSlug = target.RawLinkedEventSlug
		}
		if existing.RawLinkedPublicationState == "" {
			existing.RawLinkedPublicationState = target.RawLinkedPublicationState
		}
		if target.ResolvedFromWithheld {
			existing.ResolvedFromWithheld = true
		}
	}

	hardEventIDsByEvidence := make(map[int64]map[int64]struct{})
	for _, target := range merged {
		if target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
			continue
		}
		if hardEventIDsByEvidence[target.EvidenceID] == nil {
			hardEventIDsByEvidence[target.EvidenceID] = make(map[int64]struct{})
		}
		hardEventIDsByEvidence[target.EvidenceID][target.EventID] = struct{}{}
	}
	for _, target := range merged {
		if target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
			continue
		}
		if len(hardEventIDsByEvidence[target.EvidenceID]) > 1 {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "hard target signals disagree")
		}
	}

	out := make([]seedstore.EventReviewImportExistingEventTarget, 0, len(merged))
	for _, target := range merged {
		out = append(out, *target)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EvidenceID != out[j].EvidenceID {
			return out[i].EvidenceID < out[j].EvidenceID
		}
		if len(out[i].BlockingReasons) == 0 && len(out[j].BlockingReasons) > 0 {
			return true
		}
		if len(out[i].BlockingReasons) > 0 && len(out[j].BlockingReasons) == 0 {
			return false
		}
		if out[i].EventID != out[j].EventID {
			return out[i].EventID < out[j].EventID
		}
		return out[i].TargetBasis < out[j].TargetBasis
	})
	return out
}

const eventReviewImportUnresolvedSourceLinkBlocker = "source identity links to non-live or withheld event without live canonical"

func applyEventReviewImportUnresolvedSourceLinkBlockers(targets []seedstore.EventReviewImportExistingEventTarget, statuses []seedstore.EventReviewImportCandidateIdentityStatus) []seedstore.EventReviewImportExistingEventTarget {
	if len(targets) == 0 || len(statuses) == 0 {
		return targets
	}
	blockedEvidenceIDs := make(map[int64]struct{})
	for _, status := range statuses {
		for _, sourceKey := range status.SourceKeys {
			if sourceKey.LinkedEventID == nil && sourceKey.RawLinkedEventID != nil {
				blockedEvidenceIDs[status.EvidenceID] = struct{}{}
				break
			}
		}
	}
	if len(blockedEvidenceIDs) == 0 {
		return targets
	}
	out := make([]seedstore.EventReviewImportExistingEventTarget, len(targets))
	copy(out, targets)
	for i := range out {
		if _, ok := blockedEvidenceIDs[out[i].EvidenceID]; !ok {
			continue
		}
		appendUniqueImportReadinessReason(&out[i].BlockingReasons, eventReviewImportUnresolvedSourceLinkBlocker)
	}
	return out
}

func applyEventReviewImportSourceChoiceBlockers(targets []seedstore.EventReviewImportExistingEventTarget, statuses []seedstore.EventReviewImportCandidateIdentityStatus) []seedstore.EventReviewImportExistingEventTarget {
	if len(targets) == 0 || len(statuses) == 0 {
		return targets
	}
	blockedEvidenceIDs := make(map[int64]struct{})
	for _, status := range statuses {
		if !eventReviewImportSourceChoicesPresent(status.SourceKeys) {
			continue
		}
		selected := false
		for _, sourceKey := range status.SourceKeys {
			if sourceKey.ChoiceSelected {
				selected = true
				break
			}
		}
		if !selected {
			blockedEvidenceIDs[status.EvidenceID] = struct{}{}
		}
	}
	if len(blockedEvidenceIDs) == 0 {
		return targets
	}
	out := make([]seedstore.EventReviewImportExistingEventTarget, len(targets))
	copy(out, targets)
	for i := range out {
		if _, ok := blockedEvidenceIDs[out[i].EvidenceID]; !ok {
			continue
		}
		appendUniqueImportReadinessReason(&out[i].BlockingReasons, "no selected source identity choices")
	}
	return out
}

func applyEventReviewImportNearTitleHardTargetBlockersTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, targets []seedstore.EventReviewImportExistingEventTarget) ([]seedstore.EventReviewImportExistingEventTarget, error) {
	if len(targets) == 0 ||
		summary.Status != seedstore.EventReviewClusterStatusOpen ||
		summary.ConflictType != seedstore.EventReviewConflictTypeImportReview ||
		summary.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return targets, nil
	}
	hardTargetEvidenceIDs := make(map[int64]struct{})
	for _, target := range targets {
		if target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
			continue
		}
		hardTargetEvidenceIDs[target.EvidenceID] = struct{}{}
	}
	if len(hardTargetEvidenceIDs) == 0 {
		return targets, nil
	}

	evidenceByID := make(map[int64]seedstore.EventReviewClusterEvidenceSummary, len(evidence))
	for _, row := range evidence {
		evidenceByID[row.EvidenceID] = row
	}
	cluster := seedstore.EventReviewCluster{
		ID:               summary.ID,
		Status:           summary.Status,
		Version:          summary.Version,
		CanonicalEventID: summary.CanonicalEventID,
		ConflictType:     summary.ConflictType,
		ConflictReason:   summary.ConflictReason,
	}
	type materialResult struct {
		material importReviewCandidateMaterial
		err      error
		loaded   bool
	}
	materialByEvidenceID := make(map[int64]materialResult, len(hardTargetEvidenceIDs))
	out := make([]seedstore.EventReviewImportExistingEventTarget, len(targets))
	copy(out, targets)
	now := time.Now().UTC()
	for evidenceID := range hardTargetEvidenceIDs {
		row, ok := evidenceByID[evidenceID]
		if !ok {
			continue
		}
		result := materialByEvidenceID[evidenceID]
		if !result.loaded {
			result.loaded = true
			result.material, result.err = buildImportReviewCandidateMaterialTx(ctx, tx, s, cluster, row, nil, "import_review_near_title_hard_target_readiness", reviewSourceIdentitySupporting, now)
			materialByEvidenceID[evidenceID] = result
		}
		if result.err != nil {
			if isImportReviewCandidateMaterializationError(result.err) {
				continue
			}
			return nil, result.err
		}
		nearMatches, _, err := supportingNearTitleGuardMatchesForEvidenceTx(ctx, tx, result.material.Event, row, s.sourceMetadata)
		if err != nil {
			return nil, err
		}
		if len(nearMatches) == 0 {
			continue
		}
		reason := "near-title target is ambiguous"
		var nearTargetID int64
		if len(nearMatches) == 1 {
			reason = "near-title target disagrees with hard target"
			nearTargetID = nearMatches[0].record.ID
		}
		for i := range out {
			if out[i].EvidenceID != evidenceID || out[i].TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
				continue
			}
			if nearTargetID > 0 && out[i].EventID == nearTargetID {
				continue
			}
			appendUniqueImportReadinessReason(&out[i].BlockingReasons, reason)
		}
	}
	return out, nil
}

func assignSelectedCandidateExistingEventTargets(readiness *seedstore.EventReviewImportReadiness) {
	if readiness == nil || readiness.SelectedCandidateReadiness == nil {
		return
	}
	readiness.SelectedCandidateReadiness.ExistingEventTargets = nil
	for _, target := range readiness.ExistingEventTargets {
		if target.EvidenceID == readiness.SelectedCandidateReadiness.EvidenceID {
			readiness.SelectedCandidateReadiness.ExistingEventTargets = append(readiness.SelectedCandidateReadiness.ExistingEventTargets, target)
		}
	}
}

func finalizeEventReviewImportNewListingScope(readiness *seedstore.EventReviewImportReadiness) {
	if readiness == nil || !readiness.NewListingScope {
		return
	}
	targetEvidenceID := int64(0)
	if len(readiness.Candidates) == 1 {
		targetEvidenceID = readiness.Candidates[0].EvidenceID
	}
	for _, target := range readiness.ExistingEventTargets {
		if targetEvidenceID > 0 && target.EvidenceID != targetEvidenceID {
			continue
		}
		readiness.NewListingScope = false
		if target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate matches existing live event by near title")
			return
		}
		if len(target.BlockingReasons) > 0 {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate existing-event target requires review")
			return
		}
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "candidate resolves to existing live event")
		return
	}
}

func loadEventReviewImportStructuralTargetsTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary) ([]seedstore.EventReviewImportExistingEventTarget, error) {
	if summary.Status != seedstore.EventReviewClusterStatusOpen ||
		summary.ConflictType != seedstore.EventReviewConflictTypeImportReview ||
		summary.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return nil, nil
	}
	now := time.Now().UTC()
	cluster := seedstore.EventReviewCluster{
		ID:               summary.ID,
		Status:           summary.Status,
		Version:          summary.Version,
		CanonicalEventID: summary.CanonicalEventID,
		ConflictType:     summary.ConflictType,
		ConflictReason:   summary.ConflictReason,
	}
	targets := make([]seedstore.EventReviewImportExistingEventTarget, 0)
	for _, row := range evidence {
		material, err := buildImportReviewCandidateMaterialTx(ctx, tx, s, cluster, row, nil, "import_review_structural_readiness", reviewSourceIdentitySupporting, now)
		if err != nil {
			continue
		}
		blockingReasons := []string(nil)
		if material.SourceAuthority != seedstore.SourceAuthoritySupporting {
			blockingReasons = append(blockingReasons, "candidate is not a supporting source")
		}
		if record, ok, err := loadLiveEventRecordBySlugTx(ctx, tx, material.Event.Slug); err != nil {
			return nil, err
		} else if ok {
			targets = append(targets, seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:          row.EvidenceID,
				EvidenceFingerprint: row.EvidenceFingerprint,
				EventID:             record.ID,
				EventSlug:           record.Event.Slug,
				EventTitle:          record.Event.Name,
				PublicationState:    string(record.Event.PublicationState),
				TargetBasis:         seedstore.EventReviewImportTargetBasisSlug,
				BlockingReasons:     append([]string(nil), blockingReasons...),
			})
		}
		if records, err := loadLiveEventRecordsByFingerprintTx(ctx, tx, material.Event.Name, material.Event.VenueSlug, material.Event.Start); err != nil {
			return nil, err
		} else if len(records) == 1 {
			record := records[0]
			targets = append(targets, seedstore.EventReviewImportExistingEventTarget{
				EvidenceID:          row.EvidenceID,
				EvidenceFingerprint: row.EvidenceFingerprint,
				EventID:             record.ID,
				EventSlug:           record.Event.Slug,
				EventTitle:          record.Event.Name,
				PublicationState:    string(record.Event.PublicationState),
				TargetBasis:         seedstore.EventReviewImportTargetBasisExactTitleVenueStart,
				BlockingReasons:     append([]string(nil), blockingReasons...),
			})
		}
	}
	return targets, nil
}

func loadEventReviewImportAuthoritativeTargetsTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary) ([]seedstore.EventReviewImportAuthoritativeTarget, error) {
	if summary.Status != seedstore.EventReviewClusterStatusOpen ||
		summary.ConflictType != seedstore.EventReviewConflictTypeImportReview ||
		summary.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return nil, nil
	}
	now := time.Now().UTC()
	cluster := seedstore.EventReviewCluster{
		ID:               summary.ID,
		Status:           summary.Status,
		Version:          summary.Version,
		CanonicalEventID: summary.CanonicalEventID,
		ConflictType:     summary.ConflictType,
		ConflictReason:   summary.ConflictReason,
	}
	targets := make([]seedstore.EventReviewImportAuthoritativeTarget, 0)
	for _, row := range evidence {
		material, err := buildImportReviewCandidateMaterialTx(ctx, tx, s, cluster, row, nil, "import_review_authoritative_readiness", reviewSourceIdentityAuthoritative, now)
		if err != nil {
			continue
		}
		if material.SourceAuthority != seedstore.SourceAuthorityAuthoritative {
			continue
		}
		target := seedstore.EventReviewImportAuthoritativeTarget{
			EvidenceID:          row.EvidenceID,
			EvidenceFingerprint: row.EvidenceFingerprint,
			SourceIdentityKeys:  normalizedImportReadinessStrings(material.SourceCtx.Identities.Keys()),
		}
		applySourceCtx := material.SourceCtx
		applySourceCtx.SourceName = firstNonEmptyImportReviewText(row.SourceName, material.SourceCtx.SourceName)
		applySourceCtx.SourceURL = firstNonEmptyImportReviewText(row.SourceURL, material.SourceCtx.SourceURL)
		sourceID := row.SourceID
		if resolvedSourceID, ok, err := loadSourceIDByNameURLTx(ctx, tx, applySourceCtx.SourceName, applySourceCtx.SourceURL); err != nil {
			return nil, err
		} else if ok {
			sourceID = resolvedSourceID
		}
		if record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, tx, sourceID, applySourceCtx.Identities); err != nil {
			return nil, err
		} else if ambiguous {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "authoritative source identities resolve ambiguously")
			targets = append(targets, target)
			continue
		} else if ok {
			setAuthoritativeReadinessUpdateTarget(&target, record)
			targets = append(targets, target)
			continue
		}
		if record, ok, ambiguous, err := uniqueLiveEventMatchForEventTx(ctx, tx, material.Event); err != nil {
			return nil, err
		} else if ambiguous {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "authoritative identity target is ambiguous")
			targets = append(targets, target)
			continue
		} else if ok {
			setAuthoritativeReadinessUpdateTarget(&target, record)
			targets = append(targets, target)
			continue
		}
		if near, _, err := guardedNearLiveEventMatchForEventTx(ctx, tx, material.Event, s.sourceMetadata); err != nil {
			return nil, err
		} else if len(near) > 1 {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "authoritative near-title target is ambiguous")
			targets = append(targets, target)
			continue
		} else if len(near) == 1 {
			setAuthoritativeReadinessUpdateTarget(&target, near[0])
			targets = append(targets, target)
			continue
		}
		if _, ok, err := loadEventRecordBySlugTx(ctx, tx, material.Event.Slug); err != nil {
			return nil, err
		} else if ok {
			appendUniqueImportReadinessReason(&target.BlockingReasons, "authoritative event slug already exists but is not eligible")
			targets = append(targets, target)
			continue
		}
		target.Result = "inserted"
		targets = append(targets, target)
	}
	sort.SliceStable(targets, func(i, j int) bool {
		if targets[i].EvidenceID != targets[j].EvidenceID {
			return targets[i].EvidenceID < targets[j].EvidenceID
		}
		return targets[i].Result < targets[j].Result
	})
	return targets, nil
}

func setAuthoritativeReadinessUpdateTarget(target *seedstore.EventReviewImportAuthoritativeTarget, record eventRecord) {
	target.Result = "updated"
	eventID := record.ID
	target.EventID = &eventID
	target.EventSlug = record.Event.Slug
	target.EventTitle = record.Event.Name
}

func loadEventReviewImportNearTitleTargetsTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, summary seedstore.EventReviewClusterSummary, evidence []seedstore.EventReviewClusterEvidenceSummary, hardTargets []seedstore.EventReviewImportExistingEventTarget) ([]seedstore.EventReviewImportExistingEventTarget, error) {
	if summary.Status != seedstore.EventReviewClusterStatusOpen ||
		summary.ConflictType != seedstore.EventReviewConflictTypeImportReview ||
		summary.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return nil, nil
	}
	hardTargetByEvidenceID := make(map[int64]struct{}, len(hardTargets))
	for _, target := range hardTargets {
		if target.TargetBasis != seedstore.EventReviewImportTargetBasisNearTitle {
			hardTargetByEvidenceID[target.EvidenceID] = struct{}{}
		}
	}
	now := time.Now().UTC()
	targets := make([]seedstore.EventReviewImportExistingEventTarget, 0)
	for _, row := range evidence {
		if _, ok := hardTargetByEvidenceID[row.EvidenceID]; ok {
			continue
		}
		if row.EventID != nil {
			continue
		}
		material, err := buildImportReviewCandidateMaterialTx(ctx, tx, s, seedstore.EventReviewCluster{
			ID:               summary.ID,
			Status:           summary.Status,
			Version:          summary.Version,
			CanonicalEventID: summary.CanonicalEventID,
			ConflictType:     summary.ConflictType,
			ConflictReason:   summary.ConflictReason,
		}, row, nil, "import_review_near_title_readiness", reviewSourceIdentitySupporting, now)
		if err != nil {
			continue
		}
		nearMatches, _, err := supportingNearTitleGuardMatchesForEvidenceTx(ctx, tx, material.Event, row, s.sourceMetadata)
		if err != nil {
			return nil, err
		}
		if len(nearMatches) != 1 {
			continue
		}
		target := nearMatches[0].record
		targets = append(targets, seedstore.EventReviewImportExistingEventTarget{
			EvidenceID:          row.EvidenceID,
			EvidenceFingerprint: row.EvidenceFingerprint,
			EventID:             target.ID,
			EventSlug:           target.Event.Slug,
			EventTitle:          target.Event.Name,
			PublicationState:    string(target.Event.PublicationState),
			TargetBasis:         seedstore.EventReviewImportTargetBasisNearTitle,
		})
	}
	return targets, nil
}

func normalizedImportReadinessStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func buildEventReviewCandidateIdentityStatuses(statuses []seedstore.EventReviewImportCandidateIdentityStatus, indexByEvidenceID map[int64]int, evidenceIdentityKeys []seedstore.EventReviewEvidenceIdentityKeySummary, exactMatchByKey map[string]seedstore.EventReviewClusterExactIdentityMatchSummary, sourceLinkByKey map[string]seedstore.EventReviewClusterSourceIdentityLinkSummary, sourceChoiceByKey map[string]seedstore.EventReviewSourceIdentityChoice) []seedstore.EventReviewImportCandidateIdentityStatus {
	if len(statuses) == 0 {
		return nil
	}
	exactSeen := make(map[int64]map[string]struct{}, len(statuses))
	sourceSeen := make(map[int64]map[string]struct{}, len(statuses))
	for _, row := range evidenceIdentityKeys {
		idx, ok := indexByEvidenceID[row.EvidenceID]
		if !ok || idx < 0 || idx >= len(statuses) {
			continue
		}
		status := &statuses[idx]
		switch row.KeyKind {
		case seedstore.EventReviewIdentityKeyKindExact:
			key := strings.TrimSpace(row.NormalizedKey)
			if key == "" {
				continue
			}
			if exactSeen[row.EvidenceID] == nil {
				exactSeen[row.EvidenceID] = make(map[string]struct{})
			}
			if _, ok := exactSeen[row.EvidenceID][key]; ok {
				continue
			}
			exactSeen[row.EvidenceID][key] = struct{}{}
			match := exactMatchByKey[key]
			rowStatus := seedstore.EventReviewImportCandidateExactIdentityStatus{
				NormalizedKey:   key,
				IdentityKeyHash: strings.TrimSpace(row.IdentityKeyHash),
			}
			if match.LinkedEventID != nil {
				rowStatus.LinkedEventID = match.LinkedEventID
				rowStatus.LinkedEventSlug = match.LinkedEventSlug
				rowStatus.LinkedEventTitle = match.LinkedEventTitle
			}
			status.ExactKeys = append(status.ExactKeys, rowStatus)
		case seedstore.EventReviewIdentityKeyKindSource:
			if row.SourceID == nil {
				continue
			}
			key := importCandidateSourceIdentityKey(*row.SourceID, row.NormalizedKey)
			if key == "" {
				continue
			}
			if sourceSeen[row.EvidenceID] == nil {
				sourceSeen[row.EvidenceID] = make(map[string]struct{})
			}
			if _, ok := sourceSeen[row.EvidenceID][key]; ok {
				continue
			}
			sourceSeen[row.EvidenceID][key] = struct{}{}
			link, ok := sourceLinkByKey[key]
			rowStatus := seedstore.EventReviewImportCandidateSourceIdentityStatus{
				SourceID:          *row.SourceID,
				SourceName:        status.SourceName,
				SourceIdentityKey: strings.TrimSpace(row.NormalizedKey),
			}
			if ok {
				rowStatus.SourceName = link.SourceName
				rowStatus.LinkedEventID = link.LinkedEventID
				rowStatus.LinkedEventSlug = link.LinkedEventSlug
				rowStatus.LinkedEventTitle = link.LinkedEventTitle
				rowStatus.RawLinkedEventID = link.RawLinkedEventID
				rowStatus.RawLinkedEventSlug = link.RawLinkedEventSlug
				rowStatus.RawLinkedPublicationState = link.RawLinkedPublicationState
				rowStatus.ResolvedFromWithheld = link.ResolvedFromWithheld
				rowStatus.Authoritative = link.Authoritative
			}
			if choice, ok := sourceChoiceByKey[key]; ok {
				rowStatus.ChoiceSelected = choice.Selected
				rowStatus.ChoiceReason = choice.SelectionReason
				updatedAt := choice.UpdatedAt
				rowStatus.ChoiceUpdatedAt = &updatedAt
			}
			status.SourceKeys = append(status.SourceKeys, rowStatus)
		}
	}
	for i := range statuses {
		sort.SliceStable(statuses[i].ExactKeys, func(a, b int) bool {
			if statuses[i].ExactKeys[a].NormalizedKey != statuses[i].ExactKeys[b].NormalizedKey {
				return statuses[i].ExactKeys[a].NormalizedKey < statuses[i].ExactKeys[b].NormalizedKey
			}
			return statuses[i].ExactKeys[a].IdentityKeyHash < statuses[i].ExactKeys[b].IdentityKeyHash
		})
		sort.SliceStable(statuses[i].SourceKeys, func(a, b int) bool {
			if statuses[i].SourceKeys[a].SourceIdentityKey != statuses[i].SourceKeys[b].SourceIdentityKey {
				return statuses[i].SourceKeys[a].SourceIdentityKey < statuses[i].SourceKeys[b].SourceIdentityKey
			}
			return statuses[i].SourceKeys[a].SourceID < statuses[i].SourceKeys[b].SourceID
		})
	}
	return statuses
}

func buildEventReviewSelectedCandidateReadiness(summary seedstore.EventReviewClusterSummary, candidates []seedstore.EventReviewImportCandidateSummary, statuses []seedstore.EventReviewImportCandidateIdentityStatus) *seedstore.EventReviewImportSelectedCandidateReadiness {
	readiness := &seedstore.EventReviewImportSelectedCandidateReadiness{}
	if summary.Status != seedstore.EventReviewClusterStatusOpen {
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "cluster is not open")
	}
	if summary.CanonicalEventID != nil {
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "canonical event is already set")
	}

	candidateByEvidenceID := make(map[int64]seedstore.EventReviewImportCandidateSummary, len(candidates))
	for _, candidate := range candidates {
		candidateByEvidenceID[candidate.EvidenceID] = candidate
	}
	statusByEvidenceID := make(map[int64]seedstore.EventReviewImportCandidateIdentityStatus, len(statuses))
	selectedEvidenceIDs := make([]int64, 0, len(statuses))
	selectedEvidenceSeen := make(map[int64]struct{}, len(statuses))
	for _, status := range statuses {
		statusByEvidenceID[status.EvidenceID] = status
		for _, sourceKey := range status.SourceKeys {
			if !sourceKey.ChoiceSelected {
				continue
			}
			readiness.SelectedSourceKeys = append(readiness.SelectedSourceKeys, sourceKey)
			if _, ok := selectedEvidenceSeen[status.EvidenceID]; ok {
				continue
			}
			selectedEvidenceSeen[status.EvidenceID] = struct{}{}
			selectedEvidenceIDs = append(selectedEvidenceIDs, status.EvidenceID)
		}
	}

	switch len(selectedEvidenceIDs) {
	case 0:
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "no selected source identity choices")
	case 1:
		selectedEvidenceID := selectedEvidenceIDs[0]
		candidate, ok := candidateByEvidenceID[selectedEvidenceID]
		if ok {
			readiness.EvidenceID = candidate.EvidenceID
			readiness.EvidenceFingerprint = candidate.EvidenceFingerprint
			readiness.EventID = candidate.EventID
			readiness.EventSlug = candidate.EventSlug
			readiness.Title = candidate.Title
			readiness.VenueSlug = candidate.VenueSlug
			readiness.VenueText = candidate.VenueText
			readiness.StartAt = candidate.StartAt
		} else {
			readiness.EvidenceID = selectedEvidenceID
		}

		status := statusByEvidenceID[selectedEvidenceID]
		readiness.ExactKeys = append(readiness.ExactKeys, status.ExactKeys...)
		readiness.SourceKeys = append(readiness.SourceKeys, status.SourceKeys...)

		if status.ParseWarning != "" {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate payload could not be parsed")
		}
		if readiness.EventID != nil {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected evidence already references existing event")
		}
		if readiness.Title == "" {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate title is required")
		}
		if readiness.StartAt == nil {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate start is required")
		}
		if readiness.VenueSlug == "" && readiness.VenueText == "" {
			appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate venue is required")
		}
		for _, exactKey := range status.ExactKeys {
			if exactKey.LinkedEventID != nil {
				appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate exact identity already links to live event")
				break
			}
		}
		for _, sourceKey := range status.SourceKeys {
			if !sourceKey.ChoiceSelected {
				continue
			}
			if sourceKey.LinkedEventID != nil {
				appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected candidate source identity already links to live event")
				break
			}
		}
	default:
		appendUniqueImportReadinessReason(&readiness.BlockingReasons, "selected source identity choices span multiple candidates")
	}

	readiness.Eligible = len(readiness.BlockingReasons) == 0
	return readiness
}

func importCandidateSourceIdentityKey(sourceID int64, sourceIdentityKey string) string {
	sourceIdentityKey = strings.TrimSpace(sourceIdentityKey)
	if sourceID <= 0 || sourceIdentityKey == "" {
		return ""
	}
	return strconv.FormatInt(sourceID, 10) + "\x00" + sourceIdentityKey
}

type eventReviewTitleRepairEventRow struct {
	id               int64
	slug             string
	name             string
	origin           string
	publicationState string
}

func loadEventReviewTitleRepairEventByIDTx(ctx context.Context, q queryer, eventID int64) (eventReviewTitleRepairEventRow, bool, error) {
	if eventID <= 0 {
		return eventReviewTitleRepairEventRow{}, false, errors.New("event ID is required")
	}
	return loadEventReviewTitleRepairEventRow(ctx, q, `
		SELECT
			id,
			slug,
			name,
			origin,
			publication_state
		FROM events
		WHERE id = ?
		LIMIT 1
	`, eventID)
}

func loadEventReviewTitleRepairEventBySlugTx(ctx context.Context, q queryer, slug string) (eventReviewTitleRepairEventRow, bool, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return eventReviewTitleRepairEventRow{}, false, nil
	}
	return loadEventReviewTitleRepairEventRow(ctx, q, `
		SELECT
			id,
			slug,
			name,
			origin,
			publication_state
		FROM events
		WHERE slug = ?
		LIMIT 1
	`, slug)
}

func loadEventReviewTitleRepairEventRow(ctx context.Context, q queryer, query string, args ...any) (eventReviewTitleRepairEventRow, bool, error) {
	var row eventReviewTitleRepairEventRow
	switch err := q.QueryRowContext(ctx, query, args...).Scan(
		&row.id,
		&row.slug,
		&row.name,
		&row.origin,
		&row.publicationState,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return eventReviewTitleRepairEventRow{}, false, nil
	case err != nil:
		return eventReviewTitleRepairEventRow{}, false, err
	default:
		return row, true, nil
	}
}

func eventReviewChoiceValueByFieldName(choices []seedstore.EventReviewClusterChoiceSummary, fieldName string) string {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return ""
	}
	var value string
	for _, choice := range choices {
		if strings.EqualFold(strings.TrimSpace(choice.FieldName), fieldName) {
			value = choice.Value
		}
	}
	return value
}

func loadEventReviewClusterChoiceSummariesTx(ctx context.Context, q queryer, clusterID int64, table string) ([]seedstore.EventReviewClusterChoiceSummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			c.id,
			c.field_name,
			c.choice_kind,
			c.event_id,
			COALESCE(ev.slug, ''),
			c.evidence_id,
			COALESCE(e.evidence_fingerprint, ''),
			c.value,
			c.updated_at
		FROM `+table+` c
		LEFT JOIN events ev ON ev.id = c.event_id
		LEFT JOIN event_review_evidence e ON e.id = c.evidence_id
		WHERE c.cluster_id = ?
		ORDER BY c.field_name, c.id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var choices []seedstore.EventReviewClusterChoiceSummary
	for rows.Next() {
		var row seedstore.EventReviewClusterChoiceSummary
		var eventID sql.NullInt64
		var evidenceID sql.NullInt64
		var updatedAt string
		if err := rows.Scan(
			&row.ID,
			&row.FieldName,
			&row.ChoiceKind,
			&eventID,
			&row.EventSlug,
			&evidenceID,
			&row.EvidenceFingerprint,
			&row.Value,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		if eventID.Valid {
			row.EventID = &eventID.Int64
		}
		if evidenceID.Valid {
			row.EvidenceID = &evidenceID.Int64
		}
		row.UpdatedAt, err = parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, err
		}
		choices = append(choices, row)
	}
	return choices, rows.Err()
}

func loadEventReviewClusterLiveActionSummariesTx(ctx context.Context, q queryer, clusterID int64) ([]seedstore.EventReviewClusterLiveActionSummary, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			l.id,
			l.event_id,
			COALESCE(ev.slug, ''),
			l.action,
			l.reason,
			l.created_at,
			l.updated_at
		FROM event_review_live_actions l
		LEFT JOIN events ev ON ev.id = l.event_id
		WHERE l.cluster_id = ?
		ORDER BY l.event_id, l.id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var actions []seedstore.EventReviewClusterLiveActionSummary
	for rows.Next() {
		var row seedstore.EventReviewClusterLiveActionSummary
		var createdAt string
		var updatedAt string
		if err := rows.Scan(
			&row.ID,
			&row.EventID,
			&row.EventSlug,
			&row.Action,
			&row.Reason,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		var err error
		row.CreatedAt, err = parseRFC3339UTC(createdAt)
		if err != nil {
			return nil, err
		}
		row.UpdatedAt, err = parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, err
		}
		actions = append(actions, row)
	}
	return actions, rows.Err()
}
