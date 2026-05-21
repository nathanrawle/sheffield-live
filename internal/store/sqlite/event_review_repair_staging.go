package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	seedstore "sheffield-live/internal/store"
)

func (s *Store) StageRepairEventReviewCluster(ctx context.Context, input seedstore.StageRepairEventReviewClusterInput) (seedstore.StageRepairEventReviewClusterResult, error) {
	if s == nil || s.db == nil {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("sqlite store is not open")
	}
	if !input.RunRef.Valid() {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("event review run ref is required")
	}
	if input.RunRef.Kind != seedstore.EventReviewRunKindRepair {
		return seedstore.StageRepairEventReviewClusterResult{}, errors.New("repair event review cluster requires a repair run ref")
	}
	normalizedInput, err := normalizeStageRepairEventReviewClusterInput(input)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return seedstore.StageRepairEventReviewClusterResult{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	result := seedstore.StageRepairEventReviewClusterResult{}

	cluster, ok, err := loadEventReviewClusterByStagingKeyVersionTx(ctx, tx, normalizedInput.StagingKey, normalizedInput.StagingKeyVersion)
	if err != nil {
		return result, err
	}
	if ok && cluster.Status != seedstore.EventReviewClusterStatusOpen {
		evidenceIDs := make([]int64, 0, len(normalizedInput.Evidence))
		seenEvidenceIDs := make(map[int64]struct{}, len(normalizedInput.Evidence))
		effectiveClusterID := cluster.ID
		effectiveClusterStatus := cluster.Status
		effectiveClusterVersion := cluster.Version
		for _, evidenceInput := range normalizedInput.Evidence {
			stageInput, err := normalizeStageRepairEventReviewEvidenceInput(evidenceInput)
			if err != nil {
				return result, err
			}
			if stageInput.SourceID <= 0 {
				return result, errors.New("source ID is required")
			}
			if strings.TrimSpace(stageInput.EvidenceFingerprint) == "" {
				return result, errors.New("evidence fingerprint is required")
			}
			if stageInput.EventID != nil && *stageInput.EventID <= 0 {
				return result, errors.New("event ID must be positive")
			}
			legacyInput := repairEvidenceInputToStageEventReviewEvidenceInput(normalizedInput.RunRef, normalizedInput.StagingKey, normalizedInput.StagingKeyVersion, stageInput)
			evidenceResult, err := stageTerminalEventReviewEvidenceTx(ctx, tx, normalizedInput.RunRef, cluster, legacyInput, now)
			if err != nil {
				return result, err
			}
			if evidenceResult.ClusterID != 0 {
				effectiveClusterID = evidenceResult.ClusterID
				effectiveClusterStatus = evidenceResult.ClusterStatus
				effectiveClusterVersion = evidenceResult.Version
			}
			if _, ok := seenEvidenceIDs[evidenceResult.EvidenceID]; !ok {
				seenEvidenceIDs[evidenceResult.EvidenceID] = struct{}{}
				evidenceIDs = append(evidenceIDs, evidenceResult.EvidenceID)
			}
		}
		if err := linkEventReviewRunToClusterTx(ctx, tx, normalizedInput.RunRef, effectiveClusterID, now); err != nil {
			return result, err
		}
		result.ClusterID = effectiveClusterID
		result.Version = effectiveClusterVersion
		result.Status = effectiveClusterStatus
		result.TerminalReused = true
		result.EvidenceIDs = evidenceIDs
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return result, nil
	}

	terminalEvidenceIDs := make([]int64, 0, len(normalizedInput.Evidence))
	terminalEvidenceFingerprints := make(map[string]struct{}, len(normalizedInput.Evidence))
	terminalClusterID := int64(0)
	terminalClusterStatus := seedstore.EventReviewClusterStatusOpen
	terminalClusterVersion := 0
	allTerminalEvidence := len(normalizedInput.Evidence) > 0
	for _, evidenceInput := range normalizedInput.Evidence {
		stageInput, err := normalizeStageRepairEventReviewEvidenceInput(evidenceInput)
		if err != nil {
			return result, err
		}
		if stageInput.SourceID <= 0 {
			return result, errors.New("source ID is required")
		}
		if strings.TrimSpace(stageInput.EvidenceFingerprint) == "" {
			return result, errors.New("evidence fingerprint is required")
		}
		if stageInput.EventID != nil && *stageInput.EventID <= 0 {
			return result, errors.New("event ID must be positive")
		}

		legacyInput := repairEvidenceInputToStageEventReviewEvidenceInput(normalizedInput.RunRef, normalizedInput.StagingKey, normalizedInput.StagingKeyVersion, stageInput)
		existingEvidence, existingFound, err := loadEventReviewEvidenceByFingerprintTx(ctx, tx, legacyInput.EvidenceFingerprint)
		if err != nil {
			return result, err
		}
		if terminalEvidenceCluster, ok, err := loadTerminalEventReviewClusterForStageInputTx(ctx, tx, legacyInput, existingEvidence, existingFound); err != nil {
			return result, err
		} else if ok {
			evidenceResult, err := stageTerminalEventReviewEvidenceTx(ctx, tx, normalizedInput.RunRef, terminalEvidenceCluster, legacyInput, now)
			if err != nil {
				return result, err
			}
			if _, seen := terminalEvidenceFingerprints[legacyInput.EvidenceFingerprint]; !seen {
				terminalEvidenceFingerprints[legacyInput.EvidenceFingerprint] = struct{}{}
				terminalEvidenceIDs = append(terminalEvidenceIDs, evidenceResult.EvidenceID)
			}
			if evidenceResult.ClusterID != 0 {
				terminalClusterID = evidenceResult.ClusterID
				terminalClusterStatus = evidenceResult.ClusterStatus
				terminalClusterVersion = evidenceResult.Version
			}
			continue
		}
		allTerminalEvidence = false
	}

	if allTerminalEvidence {
		if terminalClusterID == 0 {
			return result, errors.New("terminal repair evidence did not resolve to a cluster")
		}
		if err := linkEventReviewRunToClusterTx(ctx, tx, normalizedInput.RunRef, terminalClusterID, now); err != nil {
			return result, err
		}
		result.ClusterID = terminalClusterID
		result.Version = terminalClusterVersion
		result.Status = terminalClusterStatus
		result.TerminalReused = true
		result.EvidenceIDs = terminalEvidenceIDs
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return result, nil
	}

	if !ok {
		clusterID, err := createRepairEventReviewClusterTx(ctx, tx, normalizedInput, now)
		if err != nil {
			return result, err
		}
		cluster, ok, err = loadEventReviewClusterByStagingKeyVersionTx(ctx, tx, normalizedInput.StagingKey, normalizedInput.StagingKeyVersion)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, fmt.Errorf("event review cluster %d not found after create", clusterID)
		}
		result.Created = true
	} else {
		if err := updateRepairEventReviewClusterMetadataTx(ctx, tx, cluster.ID, normalizedInput, now); err != nil {
			return result, err
		}
		result.Reused = true
	}

	evidenceIDs := make([]int64, 0, len(normalizedInput.Evidence))
	seenEvidenceIDs := make(map[int64]struct{}, len(normalizedInput.Evidence))
	seenIdentityKeyIDs := make(map[int64]struct{})
	for _, evidenceInput := range normalizedInput.Evidence {
		stageInput, err := normalizeStageRepairEventReviewEvidenceInput(evidenceInput)
		if err != nil {
			return result, err
		}
		if stageInput.SourceID <= 0 {
			return result, errors.New("source ID is required")
		}
		if strings.TrimSpace(stageInput.EvidenceFingerprint) == "" {
			return result, errors.New("evidence fingerprint is required")
		}
		if stageInput.EventID != nil && *stageInput.EventID <= 0 {
			return result, errors.New("event ID must be positive")
		}

		legacyInput := repairEvidenceInputToStageEventReviewEvidenceInput(normalizedInput.RunRef, normalizedInput.StagingKey, normalizedInput.StagingKeyVersion, stageInput)
		if _, ok := terminalEvidenceFingerprints[legacyInput.EvidenceFingerprint]; ok {
			continue
		}
		existingEvidence, existingFound, err := loadEventReviewEvidenceByFingerprintTx(ctx, tx, legacyInput.EvidenceFingerprint)
		if err != nil {
			return result, err
		}
		if terminalEvidenceCluster, ok, err := loadTerminalEventReviewClusterForStageInputTx(ctx, tx, legacyInput, existingEvidence, existingFound); err != nil {
			return result, err
		} else if ok {
			evidenceResult, err := stageTerminalEventReviewEvidenceTx(ctx, tx, normalizedInput.RunRef, terminalEvidenceCluster, legacyInput, now)
			if err != nil {
				return result, err
			}
			if _, ok := seenEvidenceIDs[evidenceResult.EvidenceID]; !ok {
				seenEvidenceIDs[evidenceResult.EvidenceID] = struct{}{}
				evidenceIDs = append(evidenceIDs, evidenceResult.EvidenceID)
			}
			continue
		}

		evidenceID, _, err := upsertEventReviewEvidenceTx(ctx, tx, legacyInput, now)
		if err != nil {
			return result, err
		}
		if _, ok := seenEvidenceIDs[evidenceID]; !ok {
			seenEvidenceIDs[evidenceID] = struct{}{}
			evidenceIDs = append(evidenceIDs, evidenceID)
		}

		identityRows, _, err := upsertEventReviewIdentityKeysTx(ctx, tx, evidenceID, legacyInput, now)
		if err != nil {
			return result, err
		}
		if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, evidenceID, legacyInput.SourceID, identityRows); err != nil {
			return result, err
		}
		if err := fillEventReviewEvidenceEventIDTx(ctx, tx, evidenceID, legacyInput.EventID, now); err != nil {
			return result, err
		}
		if err := linkEventReviewEvidenceToClusterTx(ctx, tx, cluster.ID, evidenceID, "staged repair evidence", now); err != nil {
			return result, err
		}
		if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, normalizedInput.RunRef, cluster.ID, evidenceID, eventReviewEvidenceProvenanceReasonRepair, now); err != nil {
			return result, err
		}
		if err := linkEventReviewIdentityKeysToClusterTx(ctx, tx, cluster.ID, identityRows, now); err != nil {
			return result, err
		}
		for _, row := range identityRows.rows {
			seenIdentityKeyIDs[row.id] = struct{}{}
		}
	}

	if err := upsertEventReviewChoicesTx(ctx, tx, cluster.ID, normalizedInput.CanonicalChoices, eventReviewCanonicalChoiceTable, now); err != nil {
		return result, err
	}
	if err := upsertEventReviewChoicesTx(ctx, tx, cluster.ID, normalizedInput.DraftChoices, eventReviewDraftChoiceTable, now); err != nil {
		return result, err
	}
	if err := upsertEventReviewLiveActionsTx(ctx, tx, cluster.ID, normalizedInput.LiveActions, now); err != nil {
		return result, err
	}
	if err := deactivateMissingEventReviewClusterEvidenceTx(ctx, tx, cluster.ID, evidenceIDs, now); err != nil {
		return result, err
	}
	if err := deactivateMissingEventReviewClusterIdentityKeysTx(ctx, tx, cluster.ID, seenIdentityKeyIDs, now); err != nil {
		return result, err
	}
	if err := deleteMissingEventReviewChoicesTx(ctx, tx, cluster.ID, eventReviewCanonicalChoiceTable, choiceFieldNames(normalizedInput.CanonicalChoices)); err != nil {
		return result, err
	}
	if err := deleteMissingEventReviewChoicesTx(ctx, tx, cluster.ID, eventReviewDraftChoiceTable, choiceFieldNames(normalizedInput.DraftChoices)); err != nil {
		return result, err
	}
	if err := deleteMissingEventReviewLiveActionsTx(ctx, tx, cluster.ID, liveActionEventIDs(normalizedInput.LiveActions)); err != nil {
		return result, err
	}
	if err := linkEventReviewRunToClusterTx(ctx, tx, normalizedInput.RunRef, cluster.ID, now); err != nil {
		return result, err
	}

	result.ClusterID = cluster.ID
	result.Version = cluster.Version
	result.Status = cluster.Status
	result.EvidenceIDs = evidenceIDs
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

const (
	eventReviewCanonicalChoiceTable = "event_review_canonical_choices"
	eventReviewDraftChoiceTable     = "event_review_draft_choices"
)

func normalizeStageRepairEventReviewClusterInput(input seedstore.StageRepairEventReviewClusterInput) (seedstore.StageRepairEventReviewClusterInput, error) {
	input.StagingKey = strings.TrimSpace(input.StagingKey)
	input.ConflictType = strings.TrimSpace(input.ConflictType)
	input.ConflictReason = strings.TrimSpace(input.ConflictReason)
	if input.StagingKey == "" {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("staging key is required")
	}
	if input.StagingKeyVersion <= 0 {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("staging key version is required")
	}
	if input.CanonicalEventID != nil && *input.CanonicalEventID <= 0 {
		return seedstore.StageRepairEventReviewClusterInput{}, errors.New("canonical event ID must be positive")
	}
	for i := range input.Evidence {
		input.Evidence[i].SourceName = strings.TrimSpace(input.Evidence[i].SourceName)
		input.Evidence[i].SourceURL = strings.TrimSpace(input.Evidence[i].SourceURL)
		input.Evidence[i].EvidenceFingerprint = strings.TrimSpace(input.Evidence[i].EvidenceFingerprint)
		input.Evidence[i].WeakEvidenceReason = strings.TrimSpace(input.Evidence[i].WeakEvidenceReason)
		input.Evidence[i].SourceIdentityKeys = normalizeStageEventReviewIdentityKeys(input.Evidence[i].SourceIdentityKeys)
		input.Evidence[i].ExactIdentityKeys = normalizeStageEventReviewIdentityKeys(input.Evidence[i].ExactIdentityKeys)
	}
	for i := range input.CanonicalChoices {
		input.CanonicalChoices[i].FieldName = strings.TrimSpace(input.CanonicalChoices[i].FieldName)
		input.CanonicalChoices[i].Value = strings.TrimSpace(input.CanonicalChoices[i].Value)
	}
	for i := range input.DraftChoices {
		input.DraftChoices[i].FieldName = strings.TrimSpace(input.DraftChoices[i].FieldName)
		input.DraftChoices[i].Value = strings.TrimSpace(input.DraftChoices[i].Value)
	}
	for i := range input.LiveActions {
		input.LiveActions[i].Reason = strings.TrimSpace(input.LiveActions[i].Reason)
	}
	return input, nil
}

func normalizeStageRepairEventReviewEvidenceInput(input seedstore.StageRepairEventReviewEvidenceInput) (seedstore.StageRepairEventReviewEvidenceInput, error) {
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.EvidenceFingerprint = strings.TrimSpace(input.EvidenceFingerprint)
	input.WeakEvidenceReason = strings.TrimSpace(input.WeakEvidenceReason)
	input.SourceIdentityKeys = normalizeStageEventReviewIdentityKeys(input.SourceIdentityKeys)
	input.ExactIdentityKeys = normalizeStageEventReviewIdentityKeys(input.ExactIdentityKeys)
	return input, nil
}

func repairEvidenceInputToStageEventReviewEvidenceInput(runRef seedstore.EventReviewRunRef, stagingKey string, stagingKeyVersion int, input seedstore.StageRepairEventReviewEvidenceInput) seedstore.StageEventReviewEvidenceInput {
	return seedstore.StageEventReviewEvidenceInput{
		RunRef:              runRef,
		SourceID:            input.SourceID,
		SourceName:          input.SourceName,
		SourceURL:           input.SourceURL,
		SourceAuthority:     input.SourceAuthority,
		StagingKey:          strings.TrimSpace(stagingKey),
		StagingKeyVersion:   stagingKeyVersion,
		EventID:             input.EventID,
		EvidenceFingerprint: repairEventReviewEvidenceFingerprint(stagingKey, stagingKeyVersion, input),
		Payload:             input.Payload,
		SourceIdentityKeys:  input.SourceIdentityKeys,
		ExactIdentityKeys:   input.ExactIdentityKeys,
		WeakEvidence:        input.WeakEvidence,
		WeakEvidenceReason:  input.WeakEvidenceReason,
	}
}

func repairEventReviewEvidenceFingerprint(stagingKey string, stagingKeyVersion int, input seedstore.StageRepairEventReviewEvidenceInput) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"repair-event-review-evidence:v1",
		strings.TrimSpace(stagingKey),
		fmt.Sprintf("%d", stagingKeyVersion),
		fmt.Sprintf("%d", input.SourceID),
		string(input.SourceAuthority),
		strings.TrimSpace(input.SourceName),
		strings.TrimSpace(input.SourceURL),
		eventReviewRepairOptionalInt64(input.EventID),
		strings.TrimSpace(input.EvidenceFingerprint),
	}, "\x1f")))
	return "repair-event-review-evidence:v1:" + hex.EncodeToString(sum[:])
}

func eventReviewRepairOptionalInt64(value *int64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%d", *value)
}

func createRepairEventReviewClusterTx(ctx context.Context, tx execer, input seedstore.StageRepairEventReviewClusterInput, now time.Time) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_review_clusters (
			status,
			version,
			staging_key,
			staging_key_version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		) VALUES (?, 1, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)
	`, string(seedstore.EventReviewClusterStatusOpen), input.StagingKey, input.StagingKeyVersion, input.CanonicalEventID, input.ConflictType, input.ConflictReason, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func loadEventReviewClusterByStagingKeyVersionTx(ctx context.Context, q queryer, stagingKey string, stagingKeyVersion int) (seedstore.EventReviewCluster, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			id,
			status,
			version,
			staging_key,
			staging_key_version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		FROM event_review_clusters
		WHERE staging_key = ?
			AND staging_key_version = ?
		LIMIT 1
	`, stagingKey, stagingKeyVersion)
	return scanEventReviewClusterRow(row)
}

func updateRepairEventReviewClusterMetadataTx(ctx context.Context, tx execer, clusterID int64, input seedstore.StageRepairEventReviewClusterInput, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET canonical_event_id = ?,
			conflict_type = ?,
			conflict_reason = ?,
			updated_at = ?
		WHERE id = ?
			AND status = ?
	`, input.CanonicalEventID, input.ConflictType, input.ConflictReason, formatRFC3339UTC(now), clusterID, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event review cluster %d is not open", clusterID)
	}
	return nil
}

func upsertEventReviewChoicesTx(ctx context.Context, tx execer, clusterID int64, choices []seedstore.EventReviewChoiceInput, table string, now time.Time) error {
	for _, choice := range choices {
		if err := validateEventReviewChoiceInput(choice); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO `+table+` (
				cluster_id,
				field_name,
				choice_kind,
				event_id,
				evidence_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(cluster_id, field_name) DO UPDATE SET
				choice_kind = excluded.choice_kind,
				event_id = excluded.event_id,
				evidence_id = excluded.evidence_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, clusterID, choice.FieldName, string(choice.ChoiceKind), choice.EventID, choice.EvidenceID, choice.Value, formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	return nil
}

func validateEventReviewChoiceInput(choice seedstore.EventReviewChoiceInput) error {
	choice.FieldName = strings.TrimSpace(choice.FieldName)
	if choice.FieldName == "" {
		return errors.New("event review choice field name is required")
	}
	if !choice.ChoiceKind.Valid() {
		return fmt.Errorf("invalid event review choice kind %q", choice.ChoiceKind)
	}
	switch choice.ChoiceKind {
	case seedstore.EventReviewChoiceKindEvent:
		if choice.EventID == nil || *choice.EventID <= 0 {
			return errors.New("event review event choice requires a positive event ID")
		}
		if choice.EvidenceID != nil {
			return errors.New("event review event choice must not set evidence ID")
		}
	case seedstore.EventReviewChoiceKindEvidence:
		if choice.EvidenceID == nil || *choice.EvidenceID <= 0 {
			return errors.New("event review evidence choice requires a positive evidence ID")
		}
		if choice.EventID != nil {
			return errors.New("event review evidence choice must not set event ID")
		}
	case seedstore.EventReviewChoiceKindManual:
		if choice.EventID != nil || choice.EvidenceID != nil {
			return errors.New("event review manual choice must not set event or evidence IDs")
		}
	default:
		return fmt.Errorf("invalid event review choice kind %q", choice.ChoiceKind)
	}
	return nil
}

func upsertEventReviewLiveActionsTx(ctx context.Context, tx execer, clusterID int64, actions []seedstore.EventReviewLiveActionInput, now time.Time) error {
	for _, action := range actions {
		if err := validateEventReviewLiveActionInput(action); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_live_actions (
				cluster_id,
				event_id,
				action,
				reason,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(cluster_id, event_id) DO UPDATE SET
				action = excluded.action,
				reason = excluded.reason,
				updated_at = excluded.updated_at
		`, clusterID, action.EventID, string(action.Action), action.Reason, formatRFC3339UTC(now), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	return nil
}

func validateEventReviewLiveActionInput(action seedstore.EventReviewLiveActionInput) error {
	if action.EventID <= 0 {
		return errors.New("event review live action requires a positive event ID")
	}
	if !action.Action.Valid() {
		return fmt.Errorf("invalid event review live action kind %q", action.Action)
	}
	return nil
}

func deactivateMissingEventReviewClusterEvidenceTx(ctx context.Context, tx execer, clusterID int64, keepEvidenceIDs []int64, now time.Time) error {
	if len(keepEvidenceIDs) == 0 {
		_, err := tx.ExecContext(ctx, `
			UPDATE event_review_cluster_evidence
			SET active = 0,
				unlinked_at = ?
			WHERE cluster_id = ?
				AND active = 1
		`, formatRFC3339UTC(now), clusterID)
		return err
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE event_review_cluster_evidence
		SET active = 0,
			unlinked_at = ?
		WHERE cluster_id = ?
			AND active = 1
			AND evidence_id NOT IN (`+placeholders(len(keepEvidenceIDs))+`)
	`, append([]any{formatRFC3339UTC(now), clusterID}, int64SliceToAny(keepEvidenceIDs)...)...)
	return err
}

func deactivateMissingEventReviewClusterIdentityKeysTx(ctx context.Context, tx execer, clusterID int64, keepIdentityKeyIDs map[int64]struct{}, now time.Time) error {
	if len(keepIdentityKeyIDs) == 0 {
		_, err := tx.ExecContext(ctx, `
			UPDATE event_review_cluster_identity_keys
			SET active = 0,
				unlinked_at = ?
			WHERE cluster_id = ?
				AND active = 1
		`, formatRFC3339UTC(now), clusterID)
		return err
	}
	keep := sortedInt64Keys(keepIdentityKeyIDs)
	_, err := tx.ExecContext(ctx, `
		UPDATE event_review_cluster_identity_keys
		SET active = 0,
			unlinked_at = ?
		WHERE cluster_id = ?
			AND active = 1
			AND identity_key_id NOT IN (`+placeholders(len(keep))+`)
	`, append([]any{formatRFC3339UTC(now), clusterID}, int64SliceToAny(keep)...)...)
	return err
}

func deleteMissingEventReviewChoicesTx(ctx context.Context, tx execer, clusterID int64, table string, keepFieldNames map[string]struct{}) error {
	if len(keepFieldNames) == 0 {
		_, err := tx.ExecContext(ctx, `
			DELETE FROM `+table+`
			WHERE cluster_id = ?
		`, clusterID)
		return err
	}
	keep := sortedStringKeys(keepFieldNames)
	_, err := tx.ExecContext(ctx, `
		DELETE FROM `+table+`
		WHERE cluster_id = ?
			AND field_name NOT IN (`+placeholders(len(keep))+`)
	`, append([]any{clusterID}, stringSliceToAny(keep)...)...)
	return err
}

func deleteMissingEventReviewLiveActionsTx(ctx context.Context, tx execer, clusterID int64, keepEventIDs map[int64]struct{}) error {
	if len(keepEventIDs) == 0 {
		_, err := tx.ExecContext(ctx, `
			DELETE FROM event_review_live_actions
			WHERE cluster_id = ?
		`, clusterID)
		return err
	}
	keep := sortedInt64Keys(keepEventIDs)
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_review_live_actions
		WHERE cluster_id = ?
			AND event_id NOT IN (`+placeholders(len(keep))+`)
	`, append([]any{clusterID}, int64SliceToAny(keep)...)...)
	return err
}

func choiceFieldNames(choices []seedstore.EventReviewChoiceInput) map[string]struct{} {
	out := make(map[string]struct{}, len(choices))
	for _, choice := range choices {
		out[strings.TrimSpace(choice.FieldName)] = struct{}{}
	}
	return out
}

func liveActionEventIDs(actions []seedstore.EventReviewLiveActionInput) map[int64]struct{} {
	out := make(map[int64]struct{}, len(actions))
	for _, action := range actions {
		out[action.EventID] = struct{}{}
	}
	return out
}

func int64SliceToAny(ids []int64) []any {
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, id)
	}
	return out
}

func stringSliceToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func sortedStringKeys(keys map[string]struct{}) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
