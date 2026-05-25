package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	seedstore "sheffield-live/internal/store"
)

const (
	eventReviewEvidenceRevisionFingerprintPrefix  = "event-review-evidence-revision"
	eventReviewEvidenceProvenanceReasonExact      = "terminal exact replay"
	eventReviewEvidenceProvenanceReasonCompatible = "terminal compatible reuse"
	eventReviewEvidenceProvenanceReasonRevised    = "terminal revised evidence"
	eventReviewEvidenceProvenanceReasonStaged     = "staged evidence"
	eventReviewEvidenceProvenanceReasonRepair     = "staged repair evidence"
)

type eventReviewEvidenceMaterial struct {
	EvidenceID          int64
	EvidenceFingerprint string
	SourceID            int64
	EventID             *int64
	Payload             string
	SourceIdentityKeys  []string
	ExactIdentityKeys   []string
}

func recordEventReviewRunEvidenceProvenanceTx(ctx context.Context, tx execer, runRef seedstore.EventReviewRunRef, clusterID, evidenceID int64, reason string, now time.Time) error {
	if clusterID <= 0 || evidenceID <= 0 || !runRef.Valid() {
		return errors.New("event review run evidence provenance requires a valid run ref, cluster ID, and evidence ID")
	}
	switch runRef.Kind {
	case seedstore.EventReviewRunKindImport:
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO import_run_event_review_evidence (
				import_run_id,
				cluster_id,
				evidence_id,
				linked_at,
				link_reason
			) VALUES (?, ?, ?, ?, ?)
		`, runRef.ID, clusterID, evidenceID, formatRFC3339UTC(now), strings.TrimSpace(reason))
		return err
	case seedstore.EventReviewRunKindRepair:
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO repair_run_event_review_evidence (
				repair_run_id,
				cluster_id,
				evidence_id,
				linked_at,
				link_reason
			) VALUES (?, ?, ?, ?, ?)
		`, runRef.ID, clusterID, evidenceID, formatRFC3339UTC(now), strings.TrimSpace(reason))
		return err
	default:
		return fmt.Errorf("unsupported event review run kind %q", runRef.Kind)
	}
}

func loadTerminalEventReviewClusterForEvidenceTx(ctx context.Context, q queryer, evidenceID int64) (seedstore.EventReviewCluster, bool, error) {
	if evidenceID <= 0 {
		return seedstore.EventReviewCluster{}, false, nil
	}
	row := q.QueryRowContext(ctx, `
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.superseded_by_cluster_id,
			c.previous_cluster_id,
			c.canonical_event_id,
			c.conflict_type,
			c.conflict_reason,
			c.created_at,
			c.updated_at
		FROM (
			SELECT cluster_id, linked_at
			FROM event_review_cluster_evidence
			WHERE evidence_id = ?
				AND active = 1
			UNION ALL
			SELECT cluster_id, linked_at
			FROM import_run_event_review_evidence
			WHERE evidence_id = ?
			UNION ALL
			SELECT cluster_id, linked_at
			FROM repair_run_event_review_evidence
			WHERE evidence_id = ?
		) l
		JOIN event_review_clusters c ON c.id = l.cluster_id
		WHERE c.status IN (?, ?, ?)
		ORDER BY l.linked_at DESC, c.id DESC
		LIMIT 1
	`, evidenceID, evidenceID, evidenceID,
		string(seedstore.EventReviewClusterStatusResolved),
		string(seedstore.EventReviewClusterStatusDiscarded),
		string(seedstore.EventReviewClusterStatusSuperseded),
	)
	return scanEventReviewClusterRow(row)
}

func eventReviewEvidenceIsTerminalLinkedTx(ctx context.Context, q queryer, evidenceID int64) (bool, error) {
	if evidenceID <= 0 {
		return false, nil
	}
	var exists int
	err := q.QueryRowContext(ctx, `
		SELECT 1
		FROM (
			SELECT cluster_id
			FROM event_review_cluster_evidence
			WHERE evidence_id = ?
				AND active = 1
			UNION ALL
			SELECT cluster_id
			FROM import_run_event_review_evidence
			WHERE evidence_id = ?
			UNION ALL
			SELECT cluster_id
			FROM repair_run_event_review_evidence
			WHERE evidence_id = ?
		) l
		JOIN event_review_clusters c ON c.id = l.cluster_id
		WHERE c.status IN (?, ?, ?)
		LIMIT 1
	`, evidenceID, evidenceID, evidenceID,
		string(seedstore.EventReviewClusterStatusResolved),
		string(seedstore.EventReviewClusterStatusDiscarded),
		string(seedstore.EventReviewClusterStatusSuperseded),
	).Scan(&exists)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func loadTerminalEventReviewClusterByIdentityHashesTx(ctx context.Context, q queryer, identityKeyHashes []string) (seedstore.EventReviewCluster, bool, error) {
	identityKeyHashes = uniqueNonEmptyStrings(identityKeyHashes)
	if len(identityKeyHashes) == 0 {
		return seedstore.EventReviewCluster{}, false, nil
	}
	statusArgs := []any{
		string(seedstore.EventReviewClusterStatusResolved),
		string(seedstore.EventReviewClusterStatusDiscarded),
		string(seedstore.EventReviewClusterStatusSuperseded),
	}
	statusClause := "?, ?, ?"
	hashClause := placeholders(len(identityKeyHashes))
	query := fmt.Sprintf(`
		SELECT
			c.id,
			c.status,
			c.version,
			c.staging_key,
			c.staging_key_version,
			c.superseded_by_cluster_id,
			c.previous_cluster_id,
			c.canonical_event_id,
			c.conflict_type,
			c.conflict_reason,
			c.created_at,
			c.updated_at
		FROM (
			SELECT ce.cluster_id, ce.linked_at
			FROM event_review_cluster_evidence ce
			JOIN event_review_evidence_identity_keys eki ON eki.evidence_id = ce.evidence_id
			JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
			JOIN event_review_clusters c ON c.id = ce.cluster_id
			WHERE ce.active = 1
				AND c.status IN (%s)
				AND i.identity_key_hash IN (%s)
			UNION ALL
			SELECT re.cluster_id, re.linked_at
			FROM import_run_event_review_evidence re
			JOIN event_review_evidence_identity_keys eki ON eki.evidence_id = re.evidence_id
			JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
			JOIN event_review_clusters c ON c.id = re.cluster_id
			WHERE c.status IN (%s)
				AND i.identity_key_hash IN (%s)
			UNION ALL
			SELECT rr.cluster_id, rr.linked_at
			FROM repair_run_event_review_evidence rr
			JOIN event_review_evidence_identity_keys eki ON eki.evidence_id = rr.evidence_id
			JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
			JOIN event_review_clusters c ON c.id = rr.cluster_id
			WHERE c.status IN (%s)
				AND i.identity_key_hash IN (%s)
		) l
		JOIN event_review_clusters c ON c.id = l.cluster_id
		ORDER BY l.linked_at DESC, c.id DESC
		LIMIT 1
	`, statusClause, hashClause, statusClause, hashClause, statusClause, hashClause)
	args := make([]any, 0, len(identityKeyHashes)*4+len(statusArgs)*4)
	for i := 0; i < 3; i++ {
		args = append(args, statusArgs...)
		for _, hash := range identityKeyHashes {
			args = append(args, hash)
		}
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return seedstore.EventReviewCluster{}, false, err
		}
		return seedstore.EventReviewCluster{}, false, nil
	}
	cluster, err := scanEventReviewClusterRows(rows)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	return cluster, true, nil
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
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

func terminalEvidenceIdentityHashesForInput(input seedstore.StageEventReviewEvidenceInput) []string {
	hashes := make([]string, 0, len(input.SourceIdentityKeys)+len(input.ExactIdentityKeys))
	for _, key := range input.SourceIdentityKeys {
		hashes = append(hashes, buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindSource, eventReviewIdentityKeyVersion, key))
	}
	for _, key := range input.ExactIdentityKeys {
		hashes = append(hashes, buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, key))
	}
	return uniqueNonEmptyStrings(hashes)
}

func terminalEvidenceOutcomeMatchesInputTx(ctx context.Context, q queryer, cluster seedstore.EventReviewCluster, resolution *seedstore.EventReviewResolutionSummary, input seedstore.StageEventReviewEvidenceInput) (bool, error) {
	if input.EventID == nil {
		return true, nil
	}
	if cluster.CanonicalEventID != nil && *cluster.CanonicalEventID == *input.EventID {
		return true, nil
	}
	if resolution != nil {
		if resolution.AppliedAutoResolution != nil && resolution.AppliedAutoResolution.EventID == *input.EventID {
			return true, nil
		}
		if resolution.AppliedImportListing != nil && resolution.AppliedImportListing.EventID == *input.EventID {
			return true, nil
		}
		if resolution.AppliedSupportingSource != nil && resolution.AppliedSupportingSource.EventID == *input.EventID {
			return true, nil
		}
		if resolution.AppliedTitleRepair != nil && resolution.AppliedTitleRepair.EventID == *input.EventID {
			return true, nil
		}
		inputEventKey := seedstore.EventReviewSeparationEventEndpointKey(*input.EventID)
		for _, separation := range resolution.AppliedSeparations {
			if separation.EndpointAKey == inputEventKey || separation.EndpointBKey == inputEventKey {
				return true, nil
			}
		}
		for _, action := range resolution.AppliedLiveActions {
			if action.EventID == *input.EventID {
				return true, nil
			}
		}
	}
	canonicalChoices, err := loadEventReviewClusterChoiceSummariesTx(ctx, q, cluster.ID, "event_review_canonical_choices")
	if err != nil {
		return false, err
	}
	draftChoices, err := loadEventReviewClusterChoiceSummariesTx(ctx, q, cluster.ID, "event_review_draft_choices")
	if err != nil {
		return false, err
	}
	liveActions, err := loadEventReviewClusterLiveActionSummariesTx(ctx, q, cluster.ID)
	if err != nil {
		return false, err
	}
	for _, choice := range canonicalChoices {
		if choice.EventID != nil && *choice.EventID == *input.EventID {
			return true, nil
		}
	}
	for _, choice := range draftChoices {
		if choice.EventID != nil && *choice.EventID == *input.EventID {
			return true, nil
		}
	}
	for _, action := range liveActions {
		if action.EventID == *input.EventID {
			return true, nil
		}
	}
	return false, nil
}

func terminalSuccessorStagingKey(terminalClusterID int64, previousClusterID *int64, identityHashes []string, conflictType, conflictReason string) string {
	identityHashes = uniqueNonEmptyStrings(identityHashes)
	components := []string{
		fmt.Sprintf("%d", terminalClusterID),
		eventReviewOptionalInt64String(previousClusterID),
		strings.TrimSpace(conflictType),
		strings.TrimSpace(conflictReason),
		strings.Join(identityHashes, ","),
	}
	sum := sha256.Sum256([]byte(strings.Join(components, "\x1f")))
	return fmt.Sprintf("terminal-successor:v1:%s", hex.EncodeToString(sum[:]))
}

func loadOrCreateTerminalSuccessorClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, stagingKey string, terminalCluster seedstore.EventReviewCluster, now time.Time) (seedstore.EventReviewCluster, bool, error) {
	cluster, ok, err := loadEventReviewClusterByStagingKeyVersionTx(ctx, tx, stagingKey, 1)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	if ok {
		if cluster.Status != seedstore.EventReviewClusterStatusOpen {
			return seedstore.EventReviewCluster{}, false, fmt.Errorf("event review terminal successor cluster %q is not open", stagingKey)
		}
		return cluster, false, nil
	}

	previousClusterID := terminalCluster.ID
	newClusterID, err := createEventReviewClusterTx(ctx, tx, seedstore.EventReviewClusterStatusOpen, &stagingKey, 1, &previousClusterID, terminalCluster.CanonicalEventID, terminalCluster.ConflictType, terminalCluster.ConflictReason, now)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	cluster, ok, err = loadEventReviewClusterTx(ctx, tx, newClusterID)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	if !ok {
		return seedstore.EventReviewCluster{}, false, fmt.Errorf("event review terminal successor cluster %d not found after create", newClusterID)
	}
	return cluster, true, nil
}

func loadEventReviewClusterEvidenceMaterialsTx(ctx context.Context, q queryer, clusterID int64) ([]eventReviewEvidenceMaterial, error) {
	if clusterID <= 0 {
		return nil, nil
	}
	evidenceIDs, err := loadEventReviewClusterEvidenceIDsTx(ctx, q, clusterID)
	if err != nil {
		return nil, err
	}
	if len(evidenceIDs) == 0 {
		return nil, nil
	}
	out := make([]eventReviewEvidenceMaterial, 0, len(evidenceIDs))
	for _, evidenceID := range evidenceIDs {
		material, err := loadEventReviewEvidenceMaterialTx(ctx, q, evidenceID)
		if err != nil {
			return nil, err
		}
		out = append(out, material)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EvidenceID < out[j].EvidenceID })
	return out, nil
}

func loadEventReviewClusterEvidenceIDsTx(ctx context.Context, q queryer, clusterID int64) ([]int64, error) {
	if clusterID <= 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `
		SELECT evidence_id, linked_at
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
		UNION ALL
		SELECT evidence_id, linked_at
		FROM import_run_event_review_evidence
		WHERE cluster_id = ?
		UNION ALL
		SELECT evidence_id, linked_at
		FROM repair_run_event_review_evidence
		WHERE cluster_id = ?
		ORDER BY linked_at DESC, evidence_id DESC
	`, clusterID, clusterID, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := make(map[int64]struct{})
	out := make([]int64, 0)
	for rows.Next() {
		var evidenceID int64
		var linkedAt string
		if err := rows.Scan(&evidenceID, &linkedAt); err != nil {
			return nil, err
		}
		if _, ok := seen[evidenceID]; ok {
			continue
		}
		seen[evidenceID] = struct{}{}
		out = append(out, evidenceID)
	}
	return out, rows.Err()
}

func loadEventReviewEvidenceMaterialTx(ctx context.Context, q queryer, evidenceID int64) (eventReviewEvidenceMaterial, error) {
	var (
		material eventReviewEvidenceMaterial
		eventID  sql.NullInt64
	)
	material.EvidenceID = evidenceID
	material.SourceIdentityKeys = nil
	material.ExactIdentityKeys = nil

	rows, err := q.QueryContext(ctx, `
		SELECT
			e.id,
			e.evidence_fingerprint,
			e.source_id,
			e.event_id,
			e.payload,
			i.key_kind,
			i.normalized_key
		FROM event_review_evidence e
		LEFT JOIN event_review_evidence_identity_keys eki ON eki.evidence_id = e.id
		LEFT JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
		WHERE e.id = ?
		ORDER BY eki.id
	`, evidenceID)
	if err != nil {
		return eventReviewEvidenceMaterial{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rowID       int64
			fingerprint string
			sourceID    int64
			keyKind     sql.NullString
			normalized  sql.NullString
			payload     string
		)
		if err := rows.Scan(&rowID, &fingerprint, &sourceID, &eventID, &payload, &keyKind, &normalized); err != nil {
			return eventReviewEvidenceMaterial{}, err
		}
		material.EvidenceID = rowID
		material.EvidenceFingerprint = fingerprint
		material.SourceID = sourceID
		material.Payload = payload
		if eventID.Valid {
			if material.EventID == nil {
				value := eventID.Int64
				material.EventID = &value
			}
		}
		if !keyKind.Valid || !normalized.Valid {
			continue
		}
		switch seedstore.EventReviewIdentityKeyKind(keyKind.String) {
		case seedstore.EventReviewIdentityKeyKindSource:
			material.SourceIdentityKeys = append(material.SourceIdentityKeys, normalized.String)
		case seedstore.EventReviewIdentityKeyKindExact:
			material.ExactIdentityKeys = append(material.ExactIdentityKeys, normalized.String)
		}
	}
	if err := rows.Err(); err != nil {
		return eventReviewEvidenceMaterial{}, err
	}
	sort.Strings(material.SourceIdentityKeys)
	sort.Strings(material.ExactIdentityKeys)
	return material, nil
}

func eventReviewEvidenceMaterialMatchesInput(material eventReviewEvidenceMaterial, input seedstore.StageEventReviewEvidenceInput) bool {
	if material.SourceID != input.SourceID {
		return false
	}
	if !eventReviewOptionalInt64Equal(material.EventID, input.EventID) {
		return false
	}
	if material.Payload != input.Payload {
		return false
	}
	if !stringSlicesEqual(material.SourceIdentityKeys, input.SourceIdentityKeys) {
		return false
	}
	if !stringSlicesEqual(material.ExactIdentityKeys, input.ExactIdentityKeys) {
		return false
	}
	return true
}

func eventReviewOptionalInt64Equal(a, b *int64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func eventReviewEvidenceRevisionFingerprint(originalFingerprint string, input seedstore.StageEventReviewEvidenceInput) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(originalFingerprint),
		fmt.Sprintf("%d", input.SourceID),
		eventReviewOptionalInt64String(input.EventID),
		strings.TrimSpace(input.Payload),
		strings.Join(input.SourceIdentityKeys, ","),
		strings.Join(input.ExactIdentityKeys, ","),
	}, "\x1f")))
	return fmt.Sprintf("%s:v%d:%s:%s",
		eventReviewEvidenceRevisionFingerprintPrefix,
		seedstore.EventReviewEvidenceRevisionAlgorithmVersion,
		strings.TrimSpace(originalFingerprint),
		hex.EncodeToString(sum[:]),
	)
}

func eventReviewOptionalInt64String(value *int64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%d", *value)
}
