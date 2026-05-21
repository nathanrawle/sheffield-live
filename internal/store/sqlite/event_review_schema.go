package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func validateEventReviewSchema(ctx context.Context, q queryer) error {
	if err := validateEventReviewTerminalClusterResolutions(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewOpenClusterResolutions(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewSupersedeMetadata(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewSelfReferences(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewClusterCanonicalEventRows(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewActiveClusterEvidenceRows(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewActiveClusterIdentityRows(ctx, q); err != nil {
		return err
	}
	if err := validateEventReviewActiveSeparationRows(ctx, q); err != nil {
		return err
	}
	return nil
}

func validateEventReviewTerminalClusterResolutions(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT c.id, c.status, COALESCE(r.status, '')
		FROM event_review_clusters c
		LEFT JOIN event_review_resolutions r ON r.cluster_id = c.id
		WHERE c.status IN (?, ?, ?)
			AND (
				r.id IS NULL
				OR TRIM(COALESCE(r.status, '')) <> c.status
			)
		ORDER BY c.id
		LIMIT 1
	`, string(seedstore.EventReviewClusterStatusResolved), string(seedstore.EventReviewClusterStatusDiscarded), string(seedstore.EventReviewClusterStatusSuperseded))

	var clusterID int64
	var status string
	var resolutionStatus string
	switch err := row.Scan(&clusterID, &status, &resolutionStatus); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if resolutionStatus == "" {
		return fmt.Errorf("event review cluster %d with status %q is missing a resolution row", clusterID, status)
	}
	return fmt.Errorf("event review cluster %d resolution status %q does not match cluster status %q", clusterID, resolutionStatus, status)
}

func validateEventReviewOpenClusterResolutions(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT c.id
		FROM event_review_clusters c
		JOIN event_review_resolutions r ON r.cluster_id = c.id
		WHERE c.status = ?
		ORDER BY c.id
		LIMIT 1
	`, string(seedstore.EventReviewClusterStatusOpen))
	var clusterID int64
	switch err := row.Scan(&clusterID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event review cluster %d is open with a resolution row", clusterID)
}

func validateEventReviewSupersedeMetadata(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT id, status, COALESCE(superseded_by_cluster_id, 0)
		FROM event_review_clusters
		WHERE (
				status = ?
				AND superseded_by_cluster_id IS NULL
			)
			OR (
				status <> ?
				AND superseded_by_cluster_id IS NOT NULL
			)
		ORDER BY id
		LIMIT 1
	`, string(seedstore.EventReviewClusterStatusSuperseded), string(seedstore.EventReviewClusterStatusSuperseded))
	var clusterID int64
	var status string
	var supersededBy int64
	switch err := row.Scan(&clusterID, &status, &supersededBy); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if status == string(seedstore.EventReviewClusterStatusSuperseded) {
		return fmt.Errorf("event review cluster %d is superseded without superseded_by_cluster_id", clusterID)
	}
	return fmt.Errorf("event review cluster %d with status %q must not carry superseded_by_cluster_id %d", clusterID, status, supersededBy)
}

func validateEventReviewSelfReferences(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT id
		FROM event_review_clusters
		WHERE previous_cluster_id = id
			OR superseded_by_cluster_id = id
		ORDER BY id
		LIMIT 1
	`)
	var clusterID int64
	switch err := row.Scan(&clusterID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event review cluster %d has a self reference in previous or superseded_by metadata", clusterID)
}

func validateEventReviewClusterCanonicalEventRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT c.id, c.canonical_event_id, COALESCE(e.slug, ''), COALESCE(e.origin, ''), COALESCE(e.publication_state, '')
		FROM event_review_clusters c
		LEFT JOIN events e ON e.id = c.canonical_event_id
		WHERE c.canonical_event_id IS NOT NULL
			AND (
				e.id IS NULL
				OR TRIM(COALESCE(e.origin, '')) <> ?
				OR TRIM(COALESCE(e.publication_state, '')) = ?
			)
		ORDER BY c.id
		LIMIT 1
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	var clusterID int64
	var canonicalEventID int64
	var canonicalSlug string
	var canonicalOrigin string
	var canonicalPublicationState string
	switch err := row.Scan(&clusterID, &canonicalEventID, &canonicalSlug, &canonicalOrigin, &canonicalPublicationState); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if canonicalSlug == "" {
		return fmt.Errorf("event review cluster %d references missing canonical event %d", clusterID, canonicalEventID)
	}
	if strings.EqualFold(strings.TrimSpace(canonicalPublicationState), string(domain.PublicationStateWithheld)) {
		return fmt.Errorf("event review cluster %d references withheld canonical event %d", clusterID, canonicalEventID)
	}
	return fmt.Errorf("event review cluster %d references non-live canonical event %d with origin %q", clusterID, canonicalEventID, canonicalOrigin)
}

func validateEventReviewActiveClusterEvidenceRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT evidence_id, COUNT(*)
		FROM event_review_cluster_evidence
		WHERE active = 1
		GROUP BY evidence_id
		HAVING COUNT(*) > 1
		ORDER BY evidence_id
		LIMIT 1
	`)
	var evidenceID int64
	var count int
	switch err := row.Scan(&evidenceID, &count); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("evidence %d has %d active cluster attachments", evidenceID, count)
}

func validateEventReviewActiveClusterIdentityRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT i.cluster_id, COUNT(*)
		FROM event_review_cluster_identity_keys i
		JOIN event_review_clusters c ON c.id = i.cluster_id
		WHERE i.active = 1
			AND c.status = ?
		GROUP BY i.cluster_id
		ORDER BY i.cluster_id
		LIMIT 1
	`, string(seedstore.EventReviewClusterStatusSuperseded))
	var clusterID int64
	var count int
	switch err := row.Scan(&clusterID, &count); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("superseded event review cluster %d retains %d active identity-key links", clusterID, count)
}

func validateEventReviewActiveSeparationRows(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT
			s.id,
			s.endpoint_a_kind,
			s.endpoint_a_key,
			s.endpoint_a_event_id,
			s.endpoint_a_evidence_id,
			s.endpoint_a_identity_key_id,
			s.endpoint_a_canonical_event_id,
			s.endpoint_b_kind,
			s.endpoint_b_key,
			s.endpoint_b_event_id,
			s.endpoint_b_evidence_id,
			s.endpoint_b_identity_key_id,
			s.endpoint_b_canonical_event_id,
			COALESCE(ea.slug, ''),
			COALESCE(eb.slug, ''),
			COALESCE(epa.evidence_fingerprint, ''),
			COALESCE(epb.evidence_fingerprint, ''),
			COALESCE(ia.identity_key_hash, ''),
			COALESCE(ib.identity_key_hash, ''),
			COALESCE(ca.slug, ''),
			COALESCE(cb.slug, '')
		FROM event_review_separations s
		LEFT JOIN events ea ON ea.id = s.endpoint_a_event_id
		LEFT JOIN events eb ON eb.id = s.endpoint_b_event_id
		LEFT JOIN event_review_evidence epa ON epa.id = s.endpoint_a_evidence_id
		LEFT JOIN event_review_evidence epb ON epb.id = s.endpoint_b_evidence_id
		LEFT JOIN event_review_identity_keys ia ON ia.id = s.endpoint_a_identity_key_id
		LEFT JOIN event_review_identity_keys ib ON ib.id = s.endpoint_b_identity_key_id
		LEFT JOIN events ca ON ca.id = s.endpoint_a_canonical_event_id
		LEFT JOIN events cb ON cb.id = s.endpoint_b_canonical_event_id
		WHERE s.active = 1
			AND (
				s.endpoint_a_kind NOT IN ('event', 'evidence', 'identity_key')
				OR s.endpoint_b_kind NOT IN ('event', 'evidence', 'identity_key')
				OR TRIM(s.endpoint_a_key) <> s.endpoint_a_key
				OR TRIM(s.endpoint_b_key) <> s.endpoint_b_key
				OR s.endpoint_a_key >= s.endpoint_b_key
				OR (
					s.endpoint_a_kind = 'event'
					AND (
						s.endpoint_a_event_id IS NULL
						OR s.endpoint_a_evidence_id IS NOT NULL
						OR s.endpoint_a_identity_key_id IS NOT NULL
						OR ea.id IS NULL
						OR s.endpoint_a_key <> ('event:' || s.endpoint_a_event_id)
					)
				)
				OR (
					s.endpoint_a_kind = 'evidence'
					AND (
						s.endpoint_a_event_id IS NOT NULL
						OR s.endpoint_a_evidence_id IS NULL
						OR s.endpoint_a_identity_key_id IS NOT NULL
						OR epa.id IS NULL
						OR s.endpoint_a_key <> ('evidence:' || epa.evidence_fingerprint)
					)
				)
				OR (
					s.endpoint_a_kind = 'identity_key'
					AND (
						s.endpoint_a_event_id IS NOT NULL
						OR s.endpoint_a_evidence_id IS NOT NULL
						OR s.endpoint_a_identity_key_id IS NULL
						OR ia.id IS NULL
						OR s.endpoint_a_key <> ('identity:' || ia.identity_key_hash)
					)
				)
				OR (
					s.endpoint_b_kind = 'event'
					AND (
						s.endpoint_b_event_id IS NULL
						OR s.endpoint_b_evidence_id IS NOT NULL
						OR s.endpoint_b_identity_key_id IS NOT NULL
						OR eb.id IS NULL
						OR s.endpoint_b_key <> ('event:' || s.endpoint_b_event_id)
					)
				)
				OR (
					s.endpoint_b_kind = 'evidence'
					AND (
						s.endpoint_b_event_id IS NOT NULL
						OR s.endpoint_b_evidence_id IS NULL
						OR s.endpoint_b_identity_key_id IS NOT NULL
						OR epb.id IS NULL
						OR s.endpoint_b_key <> ('evidence:' || epb.evidence_fingerprint)
					)
				)
				OR (
					s.endpoint_b_kind = 'identity_key'
					AND (
						s.endpoint_b_event_id IS NOT NULL
						OR s.endpoint_b_evidence_id IS NOT NULL
						OR s.endpoint_b_identity_key_id IS NULL
						OR ib.id IS NULL
						OR s.endpoint_b_key <> ('identity:' || ib.identity_key_hash)
					)
				)
				OR (
					s.endpoint_a_canonical_event_id IS NOT NULL
					AND ca.id IS NULL
				)
				OR (
					s.endpoint_b_canonical_event_id IS NOT NULL
					AND cb.id IS NULL
				)
			)
		ORDER BY s.id
		LIMIT 1
	`)

	var (
		separationID                 int64
		endpointAKind                string
		endpointAKey                 string
		endpointAEventID             sql.NullInt64
		endpointAEvidenceID          sql.NullInt64
		endpointAIdentityKeyID       sql.NullInt64
		endpointACanonicalEventID    sql.NullInt64
		endpointBKind                string
		endpointBKey                 string
		endpointBEventID             sql.NullInt64
		endpointBEvidenceID          sql.NullInt64
		endpointBIdentityKeyID       sql.NullInt64
		endpointBCanonicalEventID    sql.NullInt64
		endpointASlug                string
		endpointBSlug                string
		endpointAEvidenceFingerprint string
		endpointBEvidenceFingerprint string
		endpointAIdentityHash        string
		endpointBIdentityHash        string
		canonicalASlug               string
		canonicalBSlug               string
	)
	switch err := row.Scan(
		&separationID,
		&endpointAKind,
		&endpointAKey,
		&endpointAEventID,
		&endpointAEvidenceID,
		&endpointAIdentityKeyID,
		&endpointACanonicalEventID,
		&endpointBKind,
		&endpointBKey,
		&endpointBEventID,
		&endpointBEvidenceID,
		&endpointBIdentityKeyID,
		&endpointBCanonicalEventID,
		&endpointASlug,
		&endpointBSlug,
		&endpointAEvidenceFingerprint,
		&endpointBEvidenceFingerprint,
		&endpointAIdentityHash,
		&endpointBIdentityHash,
		&canonicalASlug,
		&canonicalBSlug,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}

	if endpointAKind == "" || endpointBKind == "" {
		return fmt.Errorf("event review separation %d has an empty endpoint kind", separationID)
	}
	if endpointASlug == "" && endpointAEvidenceFingerprint == "" && endpointAIdentityHash == "" && endpointAEventID.Valid == false {
		return fmt.Errorf("event review separation %d has an invalid endpoint A shape", separationID)
	}
	if endpointBSlug == "" && endpointBEvidenceFingerprint == "" && endpointBIdentityHash == "" && endpointBEventID.Valid == false {
		return fmt.Errorf("event review separation %d has an invalid endpoint B shape", separationID)
	}
	if canonicalASlug == "" && endpointACanonicalEventID.Valid {
		return fmt.Errorf("event review separation %d has a dangling endpoint_a_canonical_event_id %d", separationID, endpointACanonicalEventID.Int64)
	}
	if canonicalBSlug == "" && endpointBCanonicalEventID.Valid {
		return fmt.Errorf("event review separation %d has a dangling endpoint_b_canonical_event_id %d", separationID, endpointBCanonicalEventID.Int64)
	}
	return fmt.Errorf("event review separation %d has non-normalized or invalid endpoints %s:%s and %s:%s", separationID, endpointAKind, endpointAKey, endpointBKind, endpointBKey)
}
