package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func backfillUnit2Schema(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	if err := backfillSecondarySourceObservationsCompatibilityTx(ctx, tx); err != nil {
		return err
	}
	return backfillInvalidExactIdentityTargetsTx(ctx, tx)
}

func validateUnit2Schema(ctx context.Context, q queryer) error {
	if err := validateWithheldEventRows(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingEventCanonicalRefs(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingEventWithheldRepairRunRefs(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingSlugAliasRows(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingEventExactIdentityRows(ctx, q); err != nil {
		return err
	}
	if err := validateActiveEventExactIdentityTargets(ctx, q); err != nil {
		return err
	}
	if err := validateEventExactIdentityLifecycleRows(ctx, q); err != nil {
		return err
	}
	if err := validateObservationRunScopeRows(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingObservationRows(ctx, q); err != nil {
		return err
	}
	return nil
}

func validateWithheldEventRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT slug
		FROM events
		WHERE publication_state = ?
			AND TRIM(COALESCE(withheld_reason, '')) = ''
		ORDER BY id
		LIMIT 1
	`, string(domain.PublicationStateWithheld))
	var slug string
	switch err := row.Scan(&slug); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event %q is withheld without a reason", slug)
}

func validateDanglingEventCanonicalRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT e.slug, e.canonical_event_id, COALESCE(c.slug, '')
		FROM events e
		LEFT JOIN events c ON c.id = e.canonical_event_id
		WHERE e.canonical_event_id IS NOT NULL
			AND (c.id IS NULL OR c.id = e.id)
		ORDER BY e.id
		LIMIT 1
	`)
	var slug string
	var canonicalEventID int64
	var canonicalSlug string
	switch err := row.Scan(&slug, &canonicalEventID, &canonicalSlug); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if canonicalSlug == "" {
		return fmt.Errorf("event %q references missing canonical event %d", slug, canonicalEventID)
	}
	return fmt.Errorf("event %q references itself as canonical event", slug)
}

func validateDanglingEventWithheldRepairRunRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT e.slug, e.withheld_repair_run_id
		FROM events e
		LEFT JOIN repair_runs r ON r.id = e.withheld_repair_run_id
		WHERE e.withheld_repair_run_id IS NOT NULL
			AND r.id IS NULL
		ORDER BY e.id
		LIMIT 1
	`)
	var slug string
	var repairRunID int64
	switch err := row.Scan(&slug, &repairRunID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event %q references missing withheld repair run %d", slug, repairRunID)
}

func validateDanglingSlugAliasRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT a.id
		FROM slug_aliases a
		LEFT JOIN events e ON e.id = a.target_event_id
		LEFT JOIN venues v ON v.id = a.target_venue_id
		LEFT JOIN repair_runs r ON r.id = a.repair_run_id
		WHERE (
				a.target_kind = ?
				AND (
					a.target_event_id IS NULL
					OR a.target_venue_id IS NOT NULL
					OR e.id IS NULL
				)
			)
			OR (
				a.target_kind = ?
				AND (
					a.target_venue_id IS NULL
					OR a.target_event_id IS NOT NULL
					OR v.id IS NULL
				)
			)
			OR a.target_kind NOT IN (?, ?)
			OR (a.repair_run_id IS NOT NULL AND r.id IS NULL)
		ORDER BY a.id
		LIMIT 1
	`, string(seedstore.SlugAliasTargetKindEvent), string(seedstore.SlugAliasTargetKindVenue), string(seedstore.SlugAliasTargetKindEvent), string(seedstore.SlugAliasTargetKindVenue))
	var aliasID int64
	switch err := row.Scan(&aliasID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("slug alias %d has invalid target or repair run reference", aliasID)
}

func validateDanglingEventExactIdentityRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT i.id
		FROM event_exact_identities i
		LEFT JOIN events e ON e.id = i.event_id
		LEFT JOIN repair_runs r ON r.id = i.repair_run_id
		WHERE e.id IS NULL
			OR (i.repair_run_id IS NOT NULL AND r.id IS NULL)
		ORDER BY i.id
		LIMIT 1
	`)
	var identityID int64
	switch err := row.Scan(&identityID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event exact identity %d references missing event or repair run", identityID)
}

func backfillInvalidExactIdentityTargetsTx(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.id, e.origin, e.publication_state
		FROM event_exact_identities i
		JOIN events e ON e.id = i.event_id
		WHERE i.active = 1
			AND (
				TRIM(COALESCE(e.origin, '')) <> ?
				OR TRIM(COALESCE(e.publication_state, '')) = ?
			)
		ORDER BY e.id
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return err
	}
	defer rows.Close()

	now := time.Now().UTC()
	for rows.Next() {
		var eventID int64
		var origin string
		var publicationState string
		if err := rows.Scan(&eventID, &origin, &publicationState); err != nil {
			return err
		}
		reason := exactIdentityDeactivationReasonForEvent(domain.Event{
			Origin:           domain.Origin(origin),
			PublicationState: domain.PublicationState(publicationState),
		})
		if reason == "" {
			continue
		}
		if err := deactivateActiveExactIdentitiesForEventTx(ctx, tx, eventID, reason, 0, now); err != nil {
			return err
		}
	}
	return rows.Err()
}

func validateActiveEventExactIdentityTargets(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT i.id, e.slug, e.origin, e.publication_state
		FROM event_exact_identities i
		JOIN events e ON e.id = i.event_id
		WHERE i.active = 1
			AND (
				TRIM(COALESCE(e.origin, '')) <> ?
				OR TRIM(COALESCE(e.publication_state, '')) = ?
			)
		ORDER BY i.id
		LIMIT 1
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	var identityID int64
	var slug string
	var origin string
	var publicationState string
	switch err := row.Scan(&identityID, &slug, &origin, &publicationState); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if strings.EqualFold(strings.TrimSpace(publicationState), string(domain.PublicationStateWithheld)) {
		return fmt.Errorf("event exact identity %d is active for withheld event %q", identityID, slug)
	}
	return fmt.Errorf("event exact identity %d is active for event %q with origin %q", identityID, slug, origin)
}

func validateEventExactIdentityLifecycleRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT id, active, COALESCE(deactivated_at, ''), COALESCE(deactivated_reason, '')
		FROM event_exact_identities
		WHERE (
				active = 1
				AND (
					deactivated_at IS NOT NULL
					OR TRIM(COALESCE(deactivated_reason, '')) <> ''
				)
			)
			OR (
				active = 0
				AND (
					deactivated_at IS NULL
					OR TRIM(COALESCE(deactivated_reason, '')) = ''
				)
			)
		ORDER BY id
		LIMIT 1
	`)
	var identityID int64
	var active int
	var deactivatedAt string
	var deactivatedReason string
	switch err := row.Scan(&identityID, &active, &deactivatedAt, &deactivatedReason); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if active == 1 {
		return fmt.Errorf("event exact identity %d is active but has deactivation metadata (%s, %q)", identityID, deactivatedAt, deactivatedReason)
	}
	return fmt.Errorf("event exact identity %d is inactive but lacks deactivation metadata (%s, %q)", identityID, deactivatedAt, deactivatedReason)
}

func validateObservationRunScopeRows(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `
		SELECT id, run_scope
		FROM event_source_attribute_observations
		ORDER BY id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var observationID int64
		var runScope string
		if err := rows.Scan(&observationID, &runScope); err != nil {
			return err
		}
		kind, runID, err := seedstore.ParseObservationRunScope(seedstore.ObservationRunScope(runScope))
		if err != nil {
			return fmt.Errorf("event source attribute observation %d has invalid run_scope %q: %w", observationID, runScope, err)
		}
		switch kind {
		case seedstore.ObservationRunScopeKindImport:
			exists, err := rowExists(ctx, q, "import_runs", runID)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("event source attribute observation %d references missing import run %d", observationID, runID)
			}
		case seedstore.ObservationRunScopeKindRepair:
			exists, err := rowExists(ctx, q, "repair_runs", runID)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("event source attribute observation %d references missing repair run %d", observationID, runID)
			}
		default:
			return fmt.Errorf("event source attribute observation %d has unsupported run_scope %q", observationID, runScope)
		}
	}
	return rows.Err()
}

func validateDanglingObservationRows(ctx context.Context, q queryer) error {
	hasClusterTarget, err := columnExists(ctx, q, "event_source_attribute_observations", "event_review_cluster_id")
	if err != nil {
		return err
	}

	var query string
	var args []any
	if hasClusterTarget {
		query = `
			SELECT o.id
			FROM event_source_attribute_observations o
			LEFT JOIN sources s ON s.id = o.source_id
			LEFT JOIN events e ON e.id = o.event_id
			LEFT JOIN event_review_clusters c ON c.id = o.event_review_cluster_id
			WHERE s.id IS NULL
				OR (
					o.target_kind = ?
					AND (
						o.event_id IS NULL
						OR o.review_group_id IS NOT NULL
						OR o.event_review_cluster_id IS NOT NULL
						OR e.id IS NULL
					)
				)
				OR (
					o.target_kind = ?
					AND (
						o.review_group_id IS NULL
						OR o.event_id IS NOT NULL
						OR o.event_review_cluster_id IS NOT NULL
					)
				)
				OR (
					o.target_kind = ?
					AND (
						o.event_review_cluster_id IS NULL
						OR o.event_id IS NOT NULL
						OR o.review_group_id IS NOT NULL
						OR c.id IS NULL
					)
				)
				OR o.target_kind NOT IN (?, ?, ?)
			ORDER BY o.id
			LIMIT 1
		`
		args = []any{
			string(seedstore.ObservationTargetKindEvent),
			string(seedstore.ObservationTargetKindReviewGroup),
			string(seedstore.ObservationTargetKindEventReviewCluster),
			string(seedstore.ObservationTargetKindEvent),
			string(seedstore.ObservationTargetKindReviewGroup),
			string(seedstore.ObservationTargetKindEventReviewCluster),
		}
	} else {
		query = `
			SELECT o.id
			FROM event_source_attribute_observations o
			LEFT JOIN sources s ON s.id = o.source_id
			LEFT JOIN events e ON e.id = o.event_id
			WHERE s.id IS NULL
				OR (
					o.target_kind = ?
					AND (
						o.event_id IS NULL
						OR o.review_group_id IS NOT NULL
						OR e.id IS NULL
					)
				)
				OR (
					o.target_kind = ?
					AND (
						o.review_group_id IS NULL
						OR o.event_id IS NOT NULL
					)
				)
				OR o.target_kind NOT IN (?, ?)
			ORDER BY o.id
			LIMIT 1
		`
		args = []any{
			string(seedstore.ObservationTargetKindEvent),
			string(seedstore.ObservationTargetKindReviewGroup),
			string(seedstore.ObservationTargetKindEvent),
			string(seedstore.ObservationTargetKindReviewGroup),
		}
	}
	row := q.QueryRowContext(ctx, query, args...)
	var observationID int64
	switch err := row.Scan(&observationID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event source attribute observation %d has invalid target or source reference", observationID)
}

func rowExists(ctx context.Context, q queryer, table string, id int64) (bool, error) {
	row := q.QueryRowContext(ctx, "SELECT 1 FROM "+table+" WHERE id = ? LIMIT 1", id)
	var found int
	switch err := row.Scan(&found); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
