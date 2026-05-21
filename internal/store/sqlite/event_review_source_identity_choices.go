package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	seedstore "sheffield-live/internal/store"
)

func (s *Store) SetEventReviewSourceIdentityChoices(ctx context.Context, input seedstore.SetEventReviewSourceIdentityChoicesInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if len(input.Choices) == 0 {
		return errors.New("at least one source identity choice is required")
	}

	cluster, tx, err := beginOpenEventReviewClusterTx(ctx, s.db, seedstore.EventReviewResolutionInput{
		ClusterID:       input.ClusterID,
		ExpectedVersion: input.ExpectedVersion,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if cluster.ConflictType != seedstore.EventReviewConflictTypeImportReview {
		return fmt.Errorf("event review cluster %d conflict type %q is not supported", cluster.ID, cluster.ConflictType)
	}
	if cluster.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		return fmt.Errorf("event review cluster %d conflict reason %q is not supported", cluster.ID, cluster.ConflictReason)
	}

	activeSourceIdentityKeys, err := loadEventReviewClusterActiveSourceIdentityKeySetTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	seen := make(map[string]struct{}, len(input.Choices))
	for _, choice := range input.Choices {
		sourceIdentityKey := strings.TrimSpace(choice.SourceIdentityKey)
		if choice.SourceID <= 0 {
			return errors.New("source id is required")
		}
		if sourceIdentityKey == "" {
			return errors.New("source identity key is required")
		}
		pairKey := importCandidateSourceIdentityKey(choice.SourceID, sourceIdentityKey)
		if _, ok := seen[pairKey]; ok {
			return fmt.Errorf("duplicate source identity choice %d:%q", choice.SourceID, sourceIdentityKey)
		}
		seen[pairKey] = struct{}{}
		if _, ok := activeSourceIdentityKeys[pairKey]; !ok {
			return fmt.Errorf("unknown source identity choice %d:%q", choice.SourceID, sourceIdentityKey)
		}

		selected := 0
		if choice.Selected {
			selected = 1
		}
		selectionReason := strings.TrimSpace(choice.SelectionReason)
		_, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_source_identity_choices (
				cluster_id,
				source_id,
				source_identity_key,
				selected,
				selection_reason,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(cluster_id, source_id, source_identity_key) DO UPDATE SET
				selected = excluded.selected,
				selection_reason = excluded.selection_reason,
				updated_at = excluded.updated_at
		`, cluster.ID, choice.SourceID, sourceIdentityKey, selected, selectionReason, formatRFC3339UTC(now))
		if err != nil {
			return err
		}
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET version = version + 1,
			updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event review cluster %d update was rejected", cluster.ID)
	}
	return tx.Commit()
}

func loadEventReviewClusterActiveSourceIdentityKeySetTx(ctx context.Context, q queryer, clusterID int64) (map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT DISTINCT
			eik.source_id,
			ik.normalized_key
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		JOIN event_review_evidence_identity_keys eik ON eik.evidence_id = e.id
		JOIN event_review_identity_keys ik ON ik.id = eik.identity_key_id
		WHERE ce.cluster_id = ?
			AND ce.active = 1
			AND eik.source_id IS NOT NULL
			AND ik.key_kind = ?
	`, clusterID, string(seedstore.EventReviewIdentityKeyKindSource))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allowed := make(map[string]struct{})
	for rows.Next() {
		var sourceID int64
		var sourceIdentityKey string
		if err := rows.Scan(&sourceID, &sourceIdentityKey); err != nil {
			return nil, err
		}
		key := importCandidateSourceIdentityKey(sourceID, sourceIdentityKey)
		if key == "" {
			continue
		}
		allowed[key] = struct{}{}
	}
	return allowed, rows.Err()
}
