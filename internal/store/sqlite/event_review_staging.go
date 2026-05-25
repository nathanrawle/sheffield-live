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

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

const eventReviewIdentityKeyVersion = 1

type eventReviewClusterEndpointSet map[string]struct{}

type eventReviewClusterEndpoints struct {
	keys eventReviewClusterEndpointSet
}

type eventReviewEvidenceLinkRow struct {
	id         int64
	evidenceID int64
	linkedAt   string
	reason     string
}

type eventReviewIdentityLinkRow struct {
	id            int64
	identityKeyID int64
	linkedAt      string
}

func (s *Store) StageEventReviewEvidence(ctx context.Context, input seedstore.StageEventReviewEvidenceInput) (seedstore.StageEventReviewEvidenceResult, error) {
	if s == nil || s.db == nil {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("sqlite store is not open")
	}
	if !input.RunRef.Valid() {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("event review run ref is required")
	}
	if input.SourceID <= 0 {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("source ID is required")
	}
	if strings.TrimSpace(input.EvidenceFingerprint) == "" {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("evidence fingerprint is required")
	}
	if input.EventID != nil && *input.EventID <= 0 {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("event ID must be positive")
	}

	stageInput := normalizeStageEventReviewEvidenceInput(input)
	if stageInput.StagingKey == "" {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("staging key is required")
	}
	if stageInput.StagingKeyVersion <= 0 {
		return seedstore.StageEventReviewEvidenceResult{}, errors.New("staging key version is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return seedstore.StageEventReviewEvidenceResult{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	scope := eventReviewObservationScopeForRunRef(stageInput.RunRef)
	result := seedstore.StageEventReviewEvidenceResult{ClusterStatus: seedstore.EventReviewClusterStatusOpen}
	var evidenceID int64
	var evidenceCreated bool
	var stagingCluster *seedstore.EventReviewCluster
	if stageInput.StagingKey != "" {
		cluster, ok, err := loadEventReviewClusterByStagingKeyVersionTx(ctx, tx, stageInput.StagingKey, stageInput.StagingKeyVersion)
		if err != nil {
			return result, err
		}
		if ok {
			stagingCluster = &cluster
		}
	}

	existingEvidence, existingEvidenceFound, err := loadEventReviewEvidenceByFingerprintTx(ctx, tx, stageInput.EvidenceFingerprint)
	if err != nil {
		return result, err
	}

	var terminalCluster *seedstore.EventReviewCluster

	if existingEvidenceFound {
		if cluster, ok, err := loadTerminalEventReviewClusterForEvidenceTx(ctx, tx, existingEvidence.EvidenceID); err != nil {
			return result, err
		} else if ok {
			terminalEvidenceResult, err := stageTerminalEventReviewEvidenceTx(ctx, tx, stageInput.RunRef, cluster, stageInput, now)
			if err != nil {
				if isRetryableEventReviewStagingConflict(err) {
					result.RetryableConflict = true
				}
				return result, err
			}
			result.EvidenceID = terminalEvidenceResult.EvidenceID
			result.Created = terminalEvidenceResult.Created
			result.Reused = terminalEvidenceResult.Reused
			result.ClusterID = terminalEvidenceResult.ClusterID
			result.ClusterStatus = terminalEvidenceResult.ClusterStatus
			result.Version = terminalEvidenceResult.Version
			result.ClusterCreated = terminalEvidenceResult.ClusterCreated
			result.ClusterReused = terminalEvidenceResult.ClusterReused
			result.Attached = terminalEvidenceResult.Attached
			result.PreviousClusterID = terminalEvidenceResult.PreviousClusterID
			result.CanonicalEventID = terminalEvidenceResult.CanonicalEventID
			result.ConflictType = terminalEvidenceResult.ConflictType
			result.ConflictReason = terminalEvidenceResult.ConflictReason
			result.AutoResolved = terminalEvidenceResult.AutoResolved
			result.AutoResolvedResult = terminalEvidenceResult.AutoResolvedResult
			result.CanonicalEventSlug = terminalEvidenceResult.CanonicalEventSlug
			if err := linkEventReviewRunToClusterTx(ctx, tx, stageInput.RunRef, result.ClusterID, now); err != nil {
				return result, err
			}
			if err := tx.Commit(); err != nil {
				return result, err
			}
			return result, nil
		}
	}

	identityRows, identityHashes, err := upsertEventReviewIdentityKeysTx(ctx, tx, 0, stageInput, now)
	if err != nil {
		return result, err
	}
	var activeCluster seedstore.EventReviewCluster
	var hasActiveCluster bool
	if existingEvidenceFound {
		activeCluster, hasActiveCluster, err = loadActiveEventReviewClusterForEvidenceTx(ctx, tx, existingEvidence.EvidenceID)
		if err != nil {
			return result, err
		}
	}

	openClusterIDs, err := loadOpenEventReviewClusterIDsByIdentityIDsTx(ctx, tx, identityRows.identityKeyIDs())
	if err != nil {
		return result, err
	}
	if hasActiveCluster && activeCluster.Status == seedstore.EventReviewClusterStatusOpen {
		openClusterIDs[activeCluster.ID] = struct{}{}
	}

	var resolvedCluster *seedstore.EventReviewCluster

	evidenceEndpoints := eventReviewEvidenceEndpointsForInput(stageInput, identityHashes)
	if conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, evidenceEndpoints); err != nil {
		return result, err
	} else if conflict {
		result.RetryableConflict = true
		return result, fmt.Errorf("event review evidence has conflicting proposed endpoints")
	}

	if stagingCluster != nil && stagingCluster.Status == seedstore.EventReviewClusterStatusOpen {
		openClusterIDs[stagingCluster.ID] = struct{}{}
	}
	var openClusters []seedstore.EventReviewCluster
	if len(openClusterIDs) > 0 {
		ids := sortedInt64Keys(openClusterIDs)
		openClusters, err = loadEventReviewClustersByIDsTx(ctx, tx, ids)
		if err != nil {
			return result, err
		}
	}

	if resolvedCluster == nil {
		if candidate, ok, err := loadResolvedEventReviewClusterByIdentityIDsTx(ctx, tx, identityRows.identityKeyIDs()); err != nil {
			return result, err
		} else if ok {
			resolvedCluster = &candidate
		}
	}

	var survivor *seedstore.EventReviewCluster
	var survivorEndpoints eventReviewClusterEndpointSet
	var skipped []int64
	var activeOpenClusterEndpoints eventReviewClusterEndpointSet
	if hasActiveCluster && activeCluster.Status == seedstore.EventReviewClusterStatusOpen {
		activeOpenClusterEndpoints, err = loadEventReviewClusterEndpointsTx(ctx, tx, activeCluster.ID)
		if err != nil {
			return result, err
		}
	}

	if survivor == nil && len(openClusters) > 0 {
		sort.Slice(openClusters, func(i, j int) bool { return openClusters[i].ID < openClusters[j].ID })
		for i := range openClusters {
			endpoints, err := loadEventReviewClusterEndpointsTx(ctx, tx, openClusters[i].ID)
			if err != nil {
				return result, err
			}
			if conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, unionEventReviewEndpointSets(evidenceEndpoints, endpoints)); err != nil {
				return result, err
			} else if conflict {
				skipped = append(skipped, openClusters[i].ID)
				continue
			}
			if hasActiveCluster && activeCluster.Status == seedstore.EventReviewClusterStatusOpen && openClusters[i].ID != activeCluster.ID {
				if conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, unionEventReviewEndpointSets(evidenceEndpoints, endpoints, activeOpenClusterEndpoints)); err != nil {
					return result, err
				} else if conflict {
					skipped = append(skipped, openClusters[i].ID)
					continue
				}
			}
			survivor = &openClusters[i]
			survivorEndpoints = endpoints
			break
		}
	}

	if survivor != nil {
		movedActiveCluster := false
		evidenceID := int64(0)
		if survivor.StagingKey != nil && strings.HasPrefix(*survivor.StagingKey, "terminal-successor:") {
			terminalSurvivorMaterials, err := loadEventReviewClusterEvidenceMaterialsTx(ctx, tx, survivor.ID)
			if err != nil {
				return result, err
			}
			for _, material := range terminalSurvivorMaterials {
				if eventReviewEvidenceMaterialMatchesInput(material, stageInput) {
					evidenceID = material.EvidenceID
					break
				}
			}
		}
		if evidenceID == 0 {
			var err error
			evidenceID, evidenceCreated, err = upsertEventReviewEvidenceTx(ctx, tx, stageInput, now)
			if err != nil {
				return result, err
			}
		}
		result.EvidenceID = evidenceID
		result.Created = evidenceCreated
		result.Reused = !evidenceCreated
		if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, evidenceID, stageInput.SourceID, identityRows); err != nil {
			return result, err
		}
		if err := fillEventReviewEvidenceEventIDTx(ctx, tx, evidenceID, stageInput.EventID, now); err != nil {
			return result, err
		}
		if resolvedCluster != nil {
			if err := setOpenEventReviewClusterLineageTx(ctx, tx, survivor.ID, survivor.Version, *resolvedCluster, now); err != nil {
				if isRetryableEventReviewStagingConflict(err) {
					result.RetryableConflict = true
				}
				return result, err
			}
			if survivor.PreviousClusterID == nil {
				previousClusterID := resolvedCluster.ID
				survivor.PreviousClusterID = &previousClusterID
			}
			if survivor.CanonicalEventID == nil && resolvedCluster.CanonicalEventID != nil {
				canonicalEventID := *resolvedCluster.CanonicalEventID
				survivor.CanonicalEventID = &canonicalEventID
			}
			if strings.TrimSpace(survivor.ConflictType) == "" {
				survivor.ConflictType = resolvedCluster.ConflictType
			}
			if strings.TrimSpace(survivor.ConflictReason) == "" {
				survivor.ConflictReason = resolvedCluster.ConflictReason
			}
		}
		if conflictType := strings.TrimSpace(stageInput.ConflictType); conflictType != "" || strings.TrimSpace(stageInput.ConflictReason) != "" {
			conflictReason := strings.TrimSpace(stageInput.ConflictReason)
			if err := backfillOpenEventReviewClusterConflictMetadataTx(ctx, tx, survivor.ID, survivor.Version, conflictType, conflictReason, now); err != nil {
				if isRetryableEventReviewStagingConflict(err) {
					result.RetryableConflict = true
				}
				return result, err
			}
			if strings.TrimSpace(survivor.ConflictType) == "" {
				survivor.ConflictType = conflictType
			}
			if strings.TrimSpace(survivor.ConflictReason) == "" {
				survivor.ConflictReason = conflictReason
			}
		}
		needsAttach, err := clusterNeedsEventReviewAttachmentTx(ctx, tx, survivor.ID, evidenceID, identityRows)
		if err != nil {
			return result, err
		}
		if hasActiveCluster && activeCluster.ID != survivor.ID && needsAttach {
			if err := moveActiveEventReviewEvidenceLinksTx(ctx, tx, activeCluster.ID, survivor.ID, now); err != nil {
				return result, err
			}
			if err := moveActiveEventReviewIdentityLinksTx(ctx, tx, activeCluster.ID, survivor.ID, now); err != nil {
				return result, err
			}
			if activeCluster.Status == seedstore.EventReviewClusterStatusOpen {
				survivorEndpoints = unionEventReviewEndpointSets(survivorEndpoints, activeOpenClusterEndpoints)
			}
			movedActiveCluster = true
		}
		if err := linkEventReviewEvidenceToClusterTx(ctx, tx, survivor.ID, evidenceID, "staged evidence", now); err != nil {
			return result, err
		}
		if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, stageInput.RunRef, survivor.ID, evidenceID, eventReviewEvidenceProvenanceReasonStaged, now); err != nil {
			return result, err
		}
		if err := linkEventReviewIdentityKeysToClusterTx(ctx, tx, survivor.ID, identityRows, now); err != nil {
			return result, err
		}
		merged, mergedIDs, superseded, skippedIDs, err := mergeEventReviewClustersIfNeededTx(ctx, tx, survivor, survivorEndpoints, openClusters, evidenceEndpoints, now)
		if err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				result.RetryableConflict = true
			}
			return result, err
		}
		result.Attached = needsAttach || merged || movedActiveCluster
		result.ClusterID = survivor.ID
		result.Version = survivor.Version
		result.ClusterReused = true
		result.PreviousClusterID = survivor.PreviousClusterID
		result.CanonicalEventID = survivor.CanonicalEventID
		result.ConflictType = survivor.ConflictType
		result.ConflictReason = survivor.ConflictReason
		result.MergedClusterIDs = mergedIDs
		result.SupersededClusterIDs = superseded
		result.SkippedClusterIDs = append(result.SkippedClusterIDs, skipped...)
		result.SkippedClusterIDs = append(result.SkippedClusterIDs, skippedIDs...)
		if err := linkEventReviewRunToClusterTx(ctx, tx, stageInput.RunRef, survivor.ID, now); err != nil {
			return result, err
		}
		if err := recordEventReviewClusterObservationsForStageInputTx(ctx, tx, scope, stageInput, survivor.ID, now); err != nil {
			return result, err
		}
		if err := touchOpenEventReviewClusterTx(ctx, tx, survivor.ID, survivor.Version, now); err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				result.RetryableConflict = true
			}
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return result, nil
	}

	if survivor == nil && hasActiveCluster && activeCluster.Status == seedstore.EventReviewClusterStatusOpen {
		if len(skipped) > 0 {
			result.RetryableConflict = true
			return result, fmt.Errorf("event review evidence has conflicting proposed endpoints")
		}
		result.RetryableConflict = true
		return result, fmt.Errorf("event review evidence %d has no safe open cluster candidate", evidenceID)
	}

	if terminalCluster == nil {
		if stagingCluster != nil && stagingCluster.Status != seedstore.EventReviewClusterStatusOpen {
			terminalCluster = stagingCluster
		} else if cluster, ok, err := loadTerminalEventReviewClusterForStageInputTx(ctx, tx, stageInput, existingEvidence, existingEvidenceFound); err != nil {
			return result, err
		} else if ok {
			terminalCluster = &cluster
		}
	}
	if terminalCluster != nil {
		terminalEvidenceResult, err := stageTerminalEventReviewEvidenceTx(ctx, tx, stageInput.RunRef, *terminalCluster, stageInput, now)
		if err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				result.RetryableConflict = true
			}
			return result, err
		}
		result.EvidenceID = terminalEvidenceResult.EvidenceID
		result.Created = terminalEvidenceResult.Created
		result.Reused = terminalEvidenceResult.Reused
		result.ClusterID = terminalEvidenceResult.ClusterID
		result.ClusterStatus = terminalEvidenceResult.ClusterStatus
		result.Version = terminalEvidenceResult.Version
		result.ClusterCreated = terminalEvidenceResult.ClusterCreated
		result.ClusterReused = terminalEvidenceResult.ClusterReused
		result.Attached = terminalEvidenceResult.Attached
		result.PreviousClusterID = terminalEvidenceResult.PreviousClusterID
		result.CanonicalEventID = terminalEvidenceResult.CanonicalEventID
		result.ConflictType = terminalEvidenceResult.ConflictType
		result.ConflictReason = terminalEvidenceResult.ConflictReason
		result.AutoResolved = terminalEvidenceResult.AutoResolved
		result.AutoResolvedResult = terminalEvidenceResult.AutoResolvedResult
		result.CanonicalEventSlug = terminalEvidenceResult.CanonicalEventSlug
		terminalResolution, err := loadEventReviewClusterResolutionTx(ctx, tx, result.ClusterID, terminalCluster.SupersededByClusterID)
		if err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				result.RetryableConflict = true
			}
			return result, err
		}
		if terminalResolution != nil && terminalResolution.AppliedAutoResolution != nil {
			result.AutoResolved = true
			result.AutoResolvedResult = terminalResolution.AppliedAutoResolution.Result
			result.CanonicalEventSlug = terminalResolution.AppliedAutoResolution.EventSlug
		}
		if err := linkEventReviewRunToClusterTx(ctx, tx, stageInput.RunRef, result.ClusterID, now); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return result, nil
	}

	if resolvedCluster != nil {
		previousClusterID := resolvedCluster.ID
		newClusterID, err := createEventReviewClusterTx(ctx, tx, seedstore.EventReviewClusterStatusOpen, stagingKeyPtr(stageInput.StagingKey), stageInput.StagingKeyVersion, &previousClusterID, resolvedCluster.CanonicalEventID, resolvedCluster.ConflictType, resolvedCluster.ConflictReason, now)
		if err != nil {
			return result, err
		}
		cluster, ok, err := loadEventReviewClusterTx(ctx, tx, newClusterID)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, fmt.Errorf("event review cluster %d not found after create", newClusterID)
		}
		if evidenceID == 0 {
			evidenceID, evidenceCreated, err = upsertEventReviewEvidenceTx(ctx, tx, stageInput, now)
			if err != nil {
				return result, err
			}
			result.EvidenceID = evidenceID
			result.Created = evidenceCreated
			result.Reused = !evidenceCreated
		}
		if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, evidenceID, stageInput.SourceID, identityRows); err != nil {
			return result, err
		}
		if err := fillEventReviewEvidenceEventIDTx(ctx, tx, evidenceID, stageInput.EventID, now); err != nil {
			return result, err
		}
		if err := linkEventReviewEvidenceToClusterTx(ctx, tx, cluster.ID, evidenceID, "staged from resolved lineage", now); err != nil {
			return result, err
		}
		if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, stageInput.RunRef, cluster.ID, evidenceID, eventReviewEvidenceProvenanceReasonStaged, now); err != nil {
			return result, err
		}
		if err := linkEventReviewIdentityKeysToClusterTx(ctx, tx, cluster.ID, identityRows, now); err != nil {
			return result, err
		}
		if err := linkEventReviewRunToClusterTx(ctx, tx, stageInput.RunRef, cluster.ID, now); err != nil {
			return result, err
		}
		if err := recordEventReviewClusterObservationsForStageInputTx(ctx, tx, scope, stageInput, cluster.ID, now); err != nil {
			return result, err
		}
		if err := touchOpenEventReviewClusterTx(ctx, tx, cluster.ID, cluster.Version, now); err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				result.RetryableConflict = true
			}
			return result, err
		}
		result.ClusterID = cluster.ID
		result.Version = cluster.Version
		result.ClusterCreated = true
		result.Attached = true
		result.PreviousClusterID = cluster.PreviousClusterID
		result.CanonicalEventID = cluster.CanonicalEventID
		result.ConflictType = cluster.ConflictType
		result.ConflictReason = cluster.ConflictReason
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return result, nil
	}

	clusterID, err := createEventReviewClusterTx(ctx, tx, seedstore.EventReviewClusterStatusOpen, stagingKeyPtr(stageInput.StagingKey), stageInput.StagingKeyVersion, nil, nil, stageInput.ConflictType, stageInput.ConflictReason, now)
	if err != nil {
		return result, err
	}
	cluster, ok, err := loadEventReviewClusterTx(ctx, tx, clusterID)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, fmt.Errorf("event review cluster %d not found after create", clusterID)
	}
	if evidenceID == 0 {
		evidenceID, evidenceCreated, err = upsertEventReviewEvidenceTx(ctx, tx, stageInput, now)
		if err != nil {
			return result, err
		}
		result.EvidenceID = evidenceID
		result.Created = evidenceCreated
		result.Reused = !evidenceCreated
	}
	if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, evidenceID, stageInput.SourceID, identityRows); err != nil {
		return result, err
	}
	if err := fillEventReviewEvidenceEventIDTx(ctx, tx, evidenceID, stageInput.EventID, now); err != nil {
		return result, err
	}
	if err := linkEventReviewEvidenceToClusterTx(ctx, tx, cluster.ID, evidenceID, "staged evidence", now); err != nil {
		return result, err
	}
	if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, stageInput.RunRef, cluster.ID, evidenceID, eventReviewEvidenceProvenanceReasonStaged, now); err != nil {
		return result, err
	}
	if err := linkEventReviewIdentityKeysToClusterTx(ctx, tx, cluster.ID, identityRows, now); err != nil {
		return result, err
	}
	if err := linkEventReviewRunToClusterTx(ctx, tx, stageInput.RunRef, cluster.ID, now); err != nil {
		return result, err
	}
	if err := recordEventReviewClusterObservationsForStageInputTx(ctx, tx, scope, stageInput, cluster.ID, now); err != nil {
		return result, err
	}
	if err := touchOpenEventReviewClusterTx(ctx, tx, cluster.ID, cluster.Version, now); err != nil {
		if isRetryableEventReviewStagingConflict(err) {
			result.RetryableConflict = true
		}
		return result, err
	}
	result.ClusterID = cluster.ID
	result.Version = cluster.Version
	result.ClusterCreated = true
	result.Attached = true
	result.ConflictType = cluster.ConflictType
	result.ConflictReason = cluster.ConflictReason
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) PromoteSingletonReviewClusterIfMissing(ctx context.Context, input ingest.ReviewStageClusterInput) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, errors.New("sqlite store is not open")
	}
	now := time.Now().UTC()
	authoritativeSourceKey := authoritativeSingletonSourceEventKeyForClusterInput(s.sourceMetadata, input)

	eventSlug, applied, err := s.promoteAuthoritativeSingletonReviewClusterIfMissing(ctx, input, now)
	if err != nil || applied || authoritativeSourceKey != "" {
		return eventSlug, applied, err
	}
	eventSlug, applied, err = s.promoteNonAuthoritativeSingletonReviewClusterIfMissing(ctx, input, now)
	if err != nil || !applied {
		return eventSlug, applied, err
	}
	return eventSlug, true, nil
}

func (s *Store) promoteAuthoritativeSingletonReviewClusterIfMissing(ctx context.Context, input ingest.ReviewStageClusterInput, now time.Time) (string, bool, error) {
	if authoritativeSingletonSourceEventKeyForClusterInput(s.sourceMetadata, input) == "" {
		return "", false, nil
	}

	event, err := singletonResolvedEventFromReviewStageClusterInput(input, now)
	if err != nil {
		return "", false, nil
	}
	if authoritative, ok := reviewStageClusterAuthoritativeSource(input); ok {
		event.SourceName = authoritative.SourceName
		event.SourceURL = authoritative.SourceURL
	}
	event = s.decorateEventForPublish(event)
	event.PublicationState = domain.PublicationStateReviewed

	scope, _ := reviewStageClusterObservationScope(input)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentityAuthoritative, input.SourceName, input.SourceURL, input.AuthoritativeSourceName, input.AuthoritativeSourceURL, input.AuthoritativeSourceEventKey, input.Candidates[0], "authoritative_singleton_autopromotion")
	event.SourceName = sourceCtx.SourceName
	event.SourceURL = sourceCtx.SourceURL
	appliedEvent, applied, err := applyAuthoritativeEventTx(ctx, tx, event, sourceCtx, now, scope, s.sourceMetadata)
	if err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	committed = true
	if !applied {
		return "", false, nil
	}
	return appliedEvent.Event.Slug, true, nil
}

func (s *Store) promoteNonAuthoritativeSingletonReviewClusterIfMissing(ctx context.Context, input ingest.ReviewStageClusterInput, now time.Time) (string, bool, error) {
	event, err := singletonResolvedEventFromReviewStageClusterInput(input, now)
	if err != nil {
		return "", false, nil
	}
	event = s.decorateEventForPublish(event)
	event.PublicationState = domain.PublicationStateProvisional

	scope, _ := reviewStageClusterObservationScope(input)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
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

	sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentitySupporting, input.SourceName, input.SourceURL, "", "", "", input.Candidates[0], "supporting_singleton_autopromotion")
	event.SourceName = sourceCtx.SourceName
	event.SourceURL = sourceCtx.SourceURL
	sourceID, err := ensureSourceTx(ctx, tx, sourceCtx.SourceName, sourceCtx.SourceURL)
	if err != nil {
		return "", false, err
	}
	sourceRecord, sourceFound, sourceAmbiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, tx, sourceID, sourceCtx.Identities)
	if err != nil {
		return "", false, err
	}
	if sourceAmbiguous {
		return "", false, nil
	}
	record, found, ambiguous, err := uniqueLiveEventMatchForEventTx(ctx, tx, event)
	if err != nil {
		return "", false, err
	}
	if ambiguous {
		return "", false, nil
	}
	if sourceFound && found && sourceRecord.ID != record.ID {
		return "", false, nil
	}
	if sourceFound {
		record = sourceRecord
		found = true
	}
	if found {
		if supportingEventConflict(record.Event, event) {
			if err := tx.Rollback(); err != nil {
				return "", false, err
			}
			committed = true
			if err := s.recordStagedConflictEventObservationsAfterRollbackTx(ctx, scope, sourceCtx, record, event); err != nil {
				return "", false, err
			}
			return "", false, nil
		}
		if err := updateSupportingMatchedEventTx(ctx, tx, record, event); err != nil {
			return "", false, err
		}
		if scope != "" {
			if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthoritySupporting, record, event); err != nil {
				return "", false, err
			}
		}
		writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, record.ID, sourceID, sourceCtx, sourceLinkAuthoritySupporting, sourceLinkConflictPolicyNoMove, now)
		if err != nil {
			return "", false, err
		}
		if writeResult.Ambiguous {
			return "", false, nil
		}
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		committed = true
		return record.Event.Slug, true, nil
	}

	if near, _, err := supportingNearTitleGuardMatchesTx(ctx, tx, event, s.sourceMetadata); err != nil {
		return "", false, err
	} else if len(near) > 0 {
		if err := tx.Rollback(); err != nil {
			return "", false, err
		}
		committed = true
		if len(near) == 1 {
			if err := s.recordStagedConflictEventObservationsAfterRollbackTx(ctx, scope, sourceCtx, near[0].record, event); err != nil {
				return "", false, err
			}
		}
		return "", false, nil
	}

	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	if _, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return "", false, err
	} else if ok {
		return "", false, nil
	}

	eventID, err := insertEventTx(ctx, tx, event, venueID, sourceID, now)
	if err != nil {
		return "", false, err
	}
	writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, eventID, sourceID, sourceCtx, sourceLinkAuthoritySupporting, sourceLinkConflictPolicyNoMove, now)
	if err != nil {
		return "", false, err
	}
	if writeResult.Ambiguous {
		return "", false, nil
	}
	if scope != "" {
		if err := recordEventObservationsForSourceIdentityContextTx(ctx, tx, scope, sourceID, sourceCtx, seedstore.SourceAuthoritySupporting, eventRecord{ID: eventID, Event: domain.Event{}}, event); err != nil {
			return "", false, err
		}
	}
	if err := refreshEventGenresTx(ctx, tx, eventID, event.Description, nil, now); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	committed = true
	return event.Slug, true, nil
}

func (s *Store) FinalizeOpenEventReviewClusterRestage(ctx context.Context, clusterID int64, evidenceIDs []int64) (*seedstore.EventReviewResolutionSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if clusterID <= 0 {
		return nil, errors.New("event review cluster ID is required")
	}
	keepEvidenceIDs := uniqueInt64s(evidenceIDs)
	if len(keepEvidenceIDs) == 0 {
		return nil, errors.New("at least one evidence ID is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	cluster, ok, err := loadEventReviewClusterTx(ctx, tx, clusterID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("event review cluster %d not found", clusterID)
	}
	if cluster.Status != seedstore.EventReviewClusterStatusOpen {
		return nil, fmt.Errorf("event review cluster %d is not open", clusterID)
	}

	keepIdentityKeyIDs, err := loadEventReviewEvidenceIdentityKeyIDsByEvidenceIDsTx(ctx, tx, keepEvidenceIDs)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := deactivateMissingEventReviewClusterEvidenceTx(ctx, tx, clusterID, keepEvidenceIDs, now); err != nil {
		return nil, err
	}
	if err := deactivateMissingEventReviewClusterIdentityKeysTx(ctx, tx, clusterID, keepIdentityKeyIDs, now); err != nil {
		return nil, err
	}
	if err := pruneOpenEventReviewClusterChoicesTx(ctx, tx, clusterID); err != nil {
		return nil, err
	}
	summary, ok, err := loadEventReviewClusterSummaryByIDTx(ctx, tx, clusterID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("event review cluster %d not found", clusterID)
	}
	scope := eventReviewObservationScopeForClusterSummary(summary)
	autoResolved, err := maybeAutoResolveEventReviewClusterTx(ctx, tx, cluster, scope, s.sourceMetadata, now)
	if err != nil {
		if isRetryableEventReviewStagingConflict(err) {
			return nil, err
		}
		return nil, err
	}
	if autoResolved == nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	resolution, err := loadEventReviewClusterResolutionTx(ctx, tx, clusterID, nil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func singletonResolvedEventFromReviewStageClusterInput(input ingest.ReviewStageClusterInput, publishedAt time.Time) (domain.Event, error) {
	if len(input.Candidates) != 1 {
		return domain.Event{}, errors.New("singleton review cluster promotion requires exactly one candidate")
	}

	candidateInput := input.Candidates[0]
	selectedCandidate := review.Candidate{
		ID:               1,
		ExternalID:       strings.TrimSpace(candidateInput.ExternalID),
		Name:             strings.TrimSpace(candidateInput.Name),
		VenueSlug:        strings.TrimSpace(candidateInput.VenueSlug),
		VenueText:        strings.TrimSpace(candidateInput.VenueText),
		VenueLocationRaw: strings.TrimSpace(candidateInput.VenueLocationRaw),
		RoomText:         strings.TrimSpace(candidateInput.RoomText),
		Rooms:            append([]domain.VenueRoom(nil), candidateInput.Rooms...),
		StartAt:          strings.TrimSpace(candidateInput.StartAt),
		EndAt:            strings.TrimSpace(candidateInput.EndAt),
		Genre:            strings.TrimSpace(candidateInput.Genre),
		Status:           strings.TrimSpace(candidateInput.Status),
		Description:      strings.TrimSpace(candidateInput.Description),
		ImageURL:         strings.TrimSpace(candidateInput.ImageURL),
		ImageSourceURL:   strings.TrimSpace(candidateInput.ImageSourceURL),
		ImageAlt:         strings.TrimSpace(candidateInput.ImageAlt),
		ImageWidth:       candidateInput.ImageWidth,
		ImageHeight:      candidateInput.ImageHeight,
		ImageFocusX:      candidateInput.ImageFocusX,
		ImageFocusY:      candidateInput.ImageFocusY,
		SourceName:       strings.TrimSpace(candidateInput.SourceName),
		SourceURL:        strings.TrimSpace(candidateInput.SourceURL),
		CalendarURL:      strings.TrimSpace(candidateInput.CalendarURL),
		Provenance:       strings.TrimSpace(candidateInput.Provenance),
	}
	selected := make(map[review.Field]review.Candidate, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		selected[field] = selectedCandidate
	}
	group := review.Group{
		Title:      strings.TrimSpace(input.Title),
		SourceName: strings.TrimSpace(input.SourceName),
		SourceURL:  strings.TrimSpace(input.SourceURL),
		Notes:      strings.TrimSpace(input.Notes),
	}
	return buildResolvedEvent(group, selected, publishedAt)
}

func authoritativeSourceEventKeyFromClusterInput(input ingest.ReviewStageClusterInput) string {
	if len(input.Candidates) != 1 {
		return ""
	}
	candidate := input.Candidates[0]
	identities := ingest.SourceIdentities(sourceIdentityInputFromCandidateValues(candidate.ExternalID, candidate.SourceURL, candidate.CalendarURL))
	if key := identities.PrimaryKey(); key != "" {
		return key
	}
	return strings.TrimSpace(input.AuthoritativeSourceEventKey)
}

func reviewStageClusterAuthoritativeSource(input ingest.ReviewStageClusterInput) (reviewGroupAuthoritativeLink, bool) {
	sourceName := strings.TrimSpace(input.AuthoritativeSourceName)
	sourceURL := strings.TrimSpace(input.AuthoritativeSourceURL)
	sourceEventKey := strings.TrimSpace(input.AuthoritativeSourceEventKey)
	if sourceName == "" || sourceURL == "" || sourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceEventKey,
	}, true
}

func authoritativeSingletonSourceEventKeyForClusterInput(sourceMetadata ingest.SourceMetadataLookup, input ingest.ReviewStageClusterInput) string {
	sourceEventKey := authoritativeSourceEventKeyFromClusterInput(input)
	if sourceEventKey == "" || len(input.Candidates) != 1 {
		return ""
	}
	if !sourceMetadataOwnsReviewStageVenue(sourceMetadata, input.SourceName, input.Candidates[0].VenueSlug) {
		return ""
	}
	return sourceEventKey
}

func reviewStageClusterObservationScope(input ingest.ReviewStageClusterInput) (seedstore.ObservationRunScope, bool) {
	if input.ImportRunID > 0 {
		scope, err := seedstore.NewObservationRunScopeImport(input.ImportRunID)
		if err == nil {
			return scope, true
		}
	}
	if importRunID, ok := review.ParseOriginImportRunID(input.Notes); ok {
		scope, err := seedstore.NewObservationRunScopeImport(importRunID)
		if err == nil {
			return scope, true
		}
	}
	return "", false
}

func uniqueInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func loadEventReviewEvidenceIdentityKeyIDsByEvidenceIDsTx(ctx context.Context, q queryer, evidenceIDs []int64) (map[int64]struct{}, error) {
	if len(evidenceIDs) == 0 {
		return map[int64]struct{}{}, nil
	}
	args := make([]any, 0, len(evidenceIDs))
	for _, id := range evidenceIDs {
		args = append(args, id)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT DISTINCT identity_key_id
		FROM event_review_evidence_identity_keys
		WHERE evidence_id IN (`+placeholders(len(evidenceIDs))+`)
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]struct{})
	for rows.Next() {
		var identityKeyID int64
		if err := rows.Scan(&identityKeyID); err != nil {
			return nil, err
		}
		out[identityKeyID] = struct{}{}
	}
	return out, rows.Err()
}

func normalizeStageEventReviewEvidenceInput(input seedstore.StageEventReviewEvidenceInput) seedstore.StageEventReviewEvidenceInput {
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.StagingKey = strings.TrimSpace(input.StagingKey)
	input.ConflictType = strings.TrimSpace(input.ConflictType)
	input.ConflictReason = strings.TrimSpace(input.ConflictReason)
	input.EvidenceFingerprint = strings.TrimSpace(input.EvidenceFingerprint)
	input.WeakEvidenceReason = strings.TrimSpace(input.WeakEvidenceReason)
	input.SourceIdentityKeys = normalizeStageEventReviewIdentityKeys(input.SourceIdentityKeys)
	input.ExactIdentityKeys = normalizeStageEventReviewIdentityKeys(input.ExactIdentityKeys)
	return input
}

func eventReviewObservationScopeForRunRef(runRef seedstore.EventReviewRunRef) seedstore.ObservationRunScope {
	switch runRef.Kind {
	case seedstore.EventReviewRunKindImport:
		scope, err := seedstore.NewObservationRunScopeImport(runRef.ID)
		if err == nil {
			return scope
		}
	case seedstore.EventReviewRunKindRepair:
		scope, err := seedstore.NewObservationRunScopeRepair(runRef.ID)
		if err == nil {
			return scope
		}
	}
	return ""
}

func normalizeStageEventReviewIdentityKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func loadEventReviewEvidenceByFingerprintTx(ctx context.Context, q queryer, fingerprint string) (eventReviewEvidenceMaterial, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			e.id,
			e.evidence_fingerprint,
			e.source_id,
			e.event_id,
			e.payload
		FROM event_review_evidence e
		WHERE e.evidence_fingerprint = ?
			AND e.fingerprint_version = 1
		LIMIT 1
	`, strings.TrimSpace(fingerprint))

	var (
		material eventReviewEvidenceMaterial
		eventID  sql.NullInt64
	)
	switch err := row.Scan(&material.EvidenceID, &material.EvidenceFingerprint, &material.SourceID, &eventID, &material.Payload); {
	case errors.Is(err, sql.ErrNoRows):
		return eventReviewEvidenceMaterial{}, false, nil
	case err != nil:
		return eventReviewEvidenceMaterial{}, false, err
	}
	if eventID.Valid {
		value := eventID.Int64
		material.EventID = &value
	}
	keys, err := loadEventReviewEvidenceIdentityKeysByEvidenceIDTx(ctx, q, material.EvidenceID)
	if err != nil {
		return eventReviewEvidenceMaterial{}, false, err
	}
	material.SourceIdentityKeys = keys.SourceIdentityKeys
	material.ExactIdentityKeys = keys.ExactIdentityKeys
	return material, true, nil
}

type eventReviewEvidenceIdentityKeyLists struct {
	SourceIdentityKeys []string
	ExactIdentityKeys  []string
}

func loadEventReviewEvidenceIdentityKeysByEvidenceIDTx(ctx context.Context, q queryer, evidenceID int64) (eventReviewEvidenceIdentityKeyLists, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			i.key_kind,
			i.normalized_key
		FROM event_review_evidence_identity_keys eki
		JOIN event_review_identity_keys i ON i.id = eki.identity_key_id
		WHERE eki.evidence_id = ?
		ORDER BY eki.id
	`, evidenceID)
	if err != nil {
		return eventReviewEvidenceIdentityKeyLists{}, err
	}
	defer rows.Close()

	var out eventReviewEvidenceIdentityKeyLists
	for rows.Next() {
		var (
			keyKind    string
			normalized string
		)
		if err := rows.Scan(&keyKind, &normalized); err != nil {
			return eventReviewEvidenceIdentityKeyLists{}, err
		}
		switch seedstore.EventReviewIdentityKeyKind(keyKind) {
		case seedstore.EventReviewIdentityKeyKindSource:
			out.SourceIdentityKeys = append(out.SourceIdentityKeys, normalized)
		case seedstore.EventReviewIdentityKeyKindExact:
			out.ExactIdentityKeys = append(out.ExactIdentityKeys, normalized)
		}
	}
	sort.Strings(out.SourceIdentityKeys)
	sort.Strings(out.ExactIdentityKeys)
	return out, rows.Err()
}

func loadTerminalEventReviewClusterForStageInputTx(ctx context.Context, q queryer, input seedstore.StageEventReviewEvidenceInput, existingEvidence eventReviewEvidenceMaterial, existingFound bool) (seedstore.EventReviewCluster, bool, error) {
	if existingFound {
		if cluster, ok, err := loadActiveEventReviewClusterForEvidenceTx(ctx, q, existingEvidence.EvidenceID); err != nil {
			return seedstore.EventReviewCluster{}, false, err
		} else if ok && cluster.Status == seedstore.EventReviewClusterStatusOpen && cluster.StagingKey != nil && strings.HasPrefix(*cluster.StagingKey, "terminal-successor:") {
			return cluster, true, nil
		}
	}
	if cluster, ok, err := loadTerminalEventReviewClusterByIdentityHashesTx(ctx, q, terminalEvidenceIdentityHashesForInput(input)); err != nil {
		return seedstore.EventReviewCluster{}, false, err
	} else if ok {
		return cluster, true, nil
	}
	if existingFound {
		return loadTerminalEventReviewClusterForEvidenceTx(ctx, q, existingEvidence.EvidenceID)
	}
	return seedstore.EventReviewCluster{}, false, nil
}

func stageTerminalEventReviewEvidenceTx(ctx context.Context, tx interface {
	execer
	queryer
}, runRef seedstore.EventReviewRunRef, terminalCluster seedstore.EventReviewCluster, input seedstore.StageEventReviewEvidenceInput, now time.Time) (seedstore.StageEventReviewEvidenceResult, error) {
	result := seedstore.StageEventReviewEvidenceResult{
		ClusterID:         terminalCluster.ID,
		ClusterStatus:     terminalCluster.Status,
		Version:           terminalCluster.Version,
		ClusterReused:     true,
		PreviousClusterID: terminalCluster.PreviousClusterID,
		CanonicalEventID:  terminalCluster.CanonicalEventID,
		ConflictType:      terminalCluster.ConflictType,
		ConflictReason:    terminalCluster.ConflictReason,
	}

	existingEvidence, existingFound, err := loadEventReviewEvidenceByFingerprintTx(ctx, tx, input.EvidenceFingerprint)
	if err != nil {
		return result, err
	}

	terminalResolution, err := loadEventReviewClusterResolutionTx(ctx, tx, terminalCluster.ID, terminalCluster.SupersededByClusterID)
	if err != nil {
		return result, err
	}
	terminalOutcomeMatches, err := terminalEvidenceOutcomeMatchesInputTx(ctx, tx, terminalCluster, terminalResolution, input)
	if err != nil {
		return result, err
	}
	existingMaterialMatches := existingFound && eventReviewEvidenceMaterialMatchesInput(existingEvidence, input)
	exactReplay := existingMaterialMatches && terminalOutcomeMatches
	compatibleFresh := !existingFound && terminalOutcomeMatches
	successorRequired := !terminalOutcomeMatches || (existingFound && !existingMaterialMatches)

	stageInput := input
	if successorRequired && existingFound {
		stageInput.EvidenceFingerprint = eventReviewEvidenceRevisionFingerprint(existingEvidence.EvidenceFingerprint, input)
	}

	if exactReplay || compatibleFresh {
		if exactReplay {
			result.EvidenceID = existingEvidence.EvidenceID
			result.Reused = true
			if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, runRef, terminalCluster.ID, result.EvidenceID, terminalEvidenceProvenanceReason(true, false, false), now); err != nil {
				return result, err
			}
			if terminalResolution != nil && terminalResolution.AppliedAutoResolution != nil {
				result.AutoResolved = true
				result.AutoResolvedResult = terminalResolution.AppliedAutoResolution.Result
				result.CanonicalEventSlug = terminalResolution.AppliedAutoResolution.EventSlug
			}
			return result, nil
		}
		evidenceID, _, err := upsertEventReviewEvidenceTx(ctx, tx, stageInput, now)
		if err != nil {
			return result, err
		}
		result.EvidenceID = evidenceID
		result.Created = true
		identityRows, identityHashes, err := upsertEventReviewIdentityKeysTx(ctx, tx, result.EvidenceID, stageInput, now)
		if err != nil {
			return result, err
		}
		if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, result.EvidenceID, stageInput.SourceID, identityRows); err != nil {
			return result, err
		}
		evidenceEndpoints, err := loadEventReviewEvidenceEndpointsTx(ctx, tx, result.EvidenceID, identityHashes, stageInput)
		if err != nil {
			return result, err
		}
		if conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, evidenceEndpoints); err != nil {
			return result, err
		} else if conflict {
			return result, fmt.Errorf("event review evidence %d has conflicting proposed endpoints version conflict", result.EvidenceID)
		}
		if err := fillEventReviewEvidenceEventIDTx(ctx, tx, result.EvidenceID, stageInput.EventID, now); err != nil {
			return result, err
		}
		if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, runRef, terminalCluster.ID, result.EvidenceID, terminalEvidenceProvenanceReason(exactReplay, compatibleFresh, false), now); err != nil {
			return result, err
		}
		if terminalResolution != nil && terminalResolution.AppliedAutoResolution != nil {
			result.AutoResolved = true
			result.AutoResolvedResult = terminalResolution.AppliedAutoResolution.Result
			result.CanonicalEventSlug = terminalResolution.AppliedAutoResolution.EventSlug
		}
		return result, nil
	}

	previousClusterID := terminalCluster.ID
	successorStagingKey := terminalSuccessorStagingKey(terminalCluster.ID, &previousClusterID, terminalEvidenceIdentityHashesForInput(input), terminalCluster.ConflictType, terminalCluster.ConflictReason)
	successorCluster, successorCreated, err := loadOrCreateTerminalSuccessorClusterTx(ctx, tx, successorStagingKey, terminalCluster, now)
	if err != nil {
		return result, err
	}
	originalFingerprint := input.EvidenceFingerprint
	if existingFound {
		originalFingerprint = existingEvidence.EvidenceFingerprint
	}
	stageInput.EvidenceFingerprint = eventReviewEvidenceRevisionFingerprint(originalFingerprint, input)
	evidenceID, evidenceCreated, err := upsertEventReviewEvidenceTx(ctx, tx, stageInput, now)
	if err != nil {
		return result, err
	}
	result.EvidenceID = evidenceID
	result.Created = evidenceCreated
	result.Reused = !evidenceCreated
	identityRows, identityHashes, err := upsertEventReviewIdentityKeysTx(ctx, tx, result.EvidenceID, stageInput, now)
	if err != nil {
		return result, err
	}
	if err := linkEventReviewEvidenceIdentityKeysTx(ctx, tx, result.EvidenceID, stageInput.SourceID, identityRows); err != nil {
		return result, err
	}
	evidenceEndpoints, err := loadEventReviewEvidenceEndpointsTx(ctx, tx, result.EvidenceID, identityHashes, stageInput)
	if err != nil {
		return result, err
	}
	if conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, evidenceEndpoints); err != nil {
		return result, err
	} else if conflict {
		return result, fmt.Errorf("event review evidence %d has conflicting proposed endpoints version conflict", result.EvidenceID)
	}
	if err := fillEventReviewEvidenceEventIDTx(ctx, tx, result.EvidenceID, stageInput.EventID, now); err != nil {
		return result, err
	}
	if err := linkEventReviewEvidenceToClusterTx(ctx, tx, successorCluster.ID, result.EvidenceID, "terminal successor evidence", now); err != nil {
		return result, err
	}
	if err := linkEventReviewIdentityKeysToClusterTx(ctx, tx, successorCluster.ID, identityRows, now); err != nil {
		return result, err
	}
	if err := recordEventReviewRunEvidenceProvenanceTx(ctx, tx, runRef, successorCluster.ID, result.EvidenceID, terminalEvidenceProvenanceReason(false, false, true), now); err != nil {
		return result, err
	}
	result.ClusterID = successorCluster.ID
	result.ClusterStatus = successorCluster.Status
	result.Version = successorCluster.Version
	result.ClusterCreated = successorCreated
	result.ClusterReused = !successorCreated
	result.PreviousClusterID = successorCluster.PreviousClusterID
	result.CanonicalEventID = successorCluster.CanonicalEventID
	result.ConflictType = successorCluster.ConflictType
	result.ConflictReason = successorCluster.ConflictReason
	result.Attached = true
	return result, nil
}

func eventReviewObservationScopeForClusterSummary(summary seedstore.EventReviewClusterSummary) seedstore.ObservationRunScope {
	if summary.LatestImportRunID == nil || *summary.LatestImportRunID <= 0 {
		return ""
	}
	scope, err := seedstore.NewObservationRunScopeImport(*summary.LatestImportRunID)
	if err != nil {
		return ""
	}
	return scope
}

func terminalEvidenceProvenanceReason(exactReplay, compatibleFresh, successor bool) string {
	if exactReplay {
		return eventReviewEvidenceProvenanceReasonExact
	}
	if compatibleFresh {
		return eventReviewEvidenceProvenanceReasonCompatible
	}
	if successor {
		return eventReviewEvidenceProvenanceReasonRevised
	}
	return eventReviewEvidenceProvenanceReasonStaged
}

func upsertEventReviewEvidenceTx(ctx context.Context, tx interface {
	execer
	queryer
}, input seedstore.StageEventReviewEvidenceInput, now time.Time) (int64, bool, error) {
	var existingID int64
	switch err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM event_review_evidence
		WHERE evidence_fingerprint = ?
			AND fingerprint_version = 1
		LIMIT 1
	`, input.EvidenceFingerprint).Scan(&existingID); {
	case errors.Is(err, sql.ErrNoRows):
	default:
		if err != nil {
			return 0, false, err
		}
		terminalLinked, err := eventReviewEvidenceIsTerminalLinkedTx(ctx, tx, existingID)
		if err != nil {
			return 0, false, err
		}
		if terminalLinked {
			material, err := loadEventReviewEvidenceMaterialTx(ctx, tx, existingID)
			if err != nil {
				return 0, false, err
			}
			if !eventReviewEvidenceMaterialMatchesInput(material, input) {
				return 0, false, fmt.Errorf("event review evidence %d is terminal-linked and immutable", existingID)
			}
			return existingID, false, nil
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_review_evidence
			SET source_id = ?,
				payload = ?,
				updated_at = ?
			WHERE id = ?
		`, input.SourceID, input.Payload, formatRFC3339UTC(now), existingID); err != nil {
			return 0, false, err
		}
		return existingID, false, nil
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_review_evidence (
			source_id,
			event_id,
			evidence_fingerprint,
			fingerprint_version,
			payload,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?, ?)
	`, input.SourceID, input.EventID, input.EvidenceFingerprint, input.Payload, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, false, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

type eventReviewIdentityStageRows struct {
	rows []*eventReviewIdentityKeyRow
}

func (r eventReviewIdentityStageRows) identityKeyIDs() []int64 {
	out := make([]int64, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row.id)
	}
	return out
}

type eventReviewIdentityKeyRow struct {
	id      int64
	hash    string
	kind    seedstore.EventReviewIdentityKeyKind
	key     string
	created bool
}

func upsertEventReviewIdentityKeysTx(ctx context.Context, tx interface {
	execer
	queryer
}, evidenceID int64, input seedstore.StageEventReviewEvidenceInput, now time.Time) (eventReviewIdentityStageRows, map[int64]string, error) {
	var rows eventReviewIdentityStageRows
	hashes := make(map[int64]string)
	appendKey := func(kind seedstore.EventReviewIdentityKeyKind, normalizedKey string, role seedstore.EventReviewEvidenceIdentityKeyRole) error {
		row, err := upsertEventReviewIdentityKeyTx(ctx, tx, kind, normalizedKey, now)
		if err != nil {
			return err
		}
		rows.rows = append(rows.rows, row)
		hashes[row.id] = row.hash
		return nil
	}

	for _, key := range input.SourceIdentityKeys {
		if err := appendKey(seedstore.EventReviewIdentityKeyKindSource, key, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			return eventReviewIdentityStageRows{}, nil, err
		}
	}
	for _, key := range input.ExactIdentityKeys {
		if err := appendKey(seedstore.EventReviewIdentityKeyKindExact, key, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
			return eventReviewIdentityStageRows{}, nil, err
		}
	}

	return rows, hashes, nil
}

func upsertEventReviewIdentityKeyTx(ctx context.Context, tx interface {
	execer
	queryer
}, kind seedstore.EventReviewIdentityKeyKind, normalizedKey string, now time.Time) (*eventReviewIdentityKeyRow, error) {
	if !kind.Valid() {
		return nil, fmt.Errorf("invalid event review identity key kind %q", kind)
	}
	normalizedKey = strings.TrimSpace(normalizedKey)
	if normalizedKey == "" {
		return nil, errors.New("event review identity key is required")
	}
	hash := buildEventReviewIdentityKeyHash(kind, eventReviewIdentityKeyVersion, normalizedKey)

	var id int64
	switch err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM event_review_identity_keys
		WHERE identity_key_hash = ?
		LIMIT 1
	`, hash).Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		res, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_identity_keys (
				identity_key_hash,
				key_kind,
				key_version,
				normalized_key,
				created_at
			) VALUES (?, ?, ?, ?, ?)
		`, hash, string(kind), eventReviewIdentityKeyVersion, normalizedKey, formatRFC3339UTC(now))
		if err != nil {
			return nil, err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return nil, err
		}
	default:
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_review_identity_keys
			SET key_kind = ?,
				key_version = ?,
				normalized_key = ?
			WHERE id = ?
		`, string(kind), eventReviewIdentityKeyVersion, normalizedKey, id); err != nil {
			return nil, err
		}
	}
	return &eventReviewIdentityKeyRow{
		id:   id,
		hash: hash,
		kind: kind,
		key:  normalizedKey,
	}, nil
}

func linkEventReviewEvidenceIdentityKeysTx(ctx context.Context, tx interface {
	execer
	queryer
}, evidenceID, sourceID int64, rows eventReviewIdentityStageRows) error {
	terminalLinked := false
	terminalLinkedLoaded := false
	for _, row := range rows.rows {
		role := identityRoleForKeyKind(row.kind)
		var exists int
		switch err := tx.QueryRowContext(ctx, `
			SELECT 1
			FROM event_review_evidence_identity_keys
			WHERE evidence_id = ?
				AND identity_key_id = ?
				AND role = ?
			LIMIT 1
		`, evidenceID, row.id, string(role)).Scan(&exists); {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		default:
			continue
		}
		if !terminalLinkedLoaded {
			var err error
			terminalLinked, err = eventReviewEvidenceIsTerminalLinkedTx(ctx, tx, evidenceID)
			if err != nil {
				return err
			}
			terminalLinkedLoaded = true
		}
		if terminalLinked {
			return fmt.Errorf("event review evidence %d is terminal-linked and immutable", evidenceID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_evidence_identity_keys (
				evidence_id,
				identity_key_id,
				source_id,
				role
			) VALUES (?, ?, ?, ?)
		`, evidenceID, row.id, sourceID, string(role)); err != nil {
			return err
		}
	}
	return nil
}

func identityRoleForKeyKind(kind seedstore.EventReviewIdentityKeyKind) seedstore.EventReviewEvidenceIdentityKeyRole {
	switch kind {
	case seedstore.EventReviewIdentityKeyKindExact:
		return seedstore.EventReviewEvidenceIdentityKeyRoleExact
	default:
		return seedstore.EventReviewEvidenceIdentityKeyRoleObserved
	}
}

func loadActiveEventReviewClusterForEvidenceTx(ctx context.Context, q queryer, evidenceID int64) (seedstore.EventReviewCluster, bool, error) {
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
		FROM event_review_cluster_evidence e
		JOIN event_review_clusters c ON c.id = e.cluster_id
		WHERE e.evidence_id = ?
			AND e.active = 1
		LIMIT 1
	`, evidenceID)
	return scanEventReviewClusterRow(row)
}

func loadEventReviewClustersByIDsTx(ctx context.Context, q queryer, ids []int64) ([]seedstore.EventReviewCluster, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query := `
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
		WHERE id IN (` + placeholders(len(ids)) + `)
		ORDER BY id
	`
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []seedstore.EventReviewCluster
	for rows.Next() {
		cluster, err := scanEventReviewClusterRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cluster)
	}
	return out, rows.Err()
}

func loadOpenEventReviewClusterIDsByIdentityIDsTx(ctx context.Context, q queryer, identityKeyIDs []int64) (map[int64]struct{}, error) {
	out := make(map[int64]struct{})
	if len(identityKeyIDs) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(identityKeyIDs))
	for _, id := range identityKeyIDs {
		args = append(args, id)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT DISTINCT c.id
		FROM event_review_cluster_identity_keys i
		JOIN event_review_clusters c ON c.id = i.cluster_id
		WHERE i.active = 1
			AND c.status = ?
			AND i.identity_key_id IN (`+placeholders(len(identityKeyIDs))+`)
		ORDER BY c.id
	`, append([]any{string(seedstore.EventReviewClusterStatusOpen)}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var clusterID int64
		if err := rows.Scan(&clusterID); err != nil {
			return nil, err
		}
		out[clusterID] = struct{}{}
	}
	return out, rows.Err()
}

func loadResolvedEventReviewClusterByIdentityIDsTx(ctx context.Context, q queryer, identityKeyIDs []int64) (seedstore.EventReviewCluster, bool, error) {
	if len(identityKeyIDs) == 0 {
		return seedstore.EventReviewCluster{}, false, nil
	}
	args := make([]any, 0, len(identityKeyIDs))
	for _, id := range identityKeyIDs {
		args = append(args, id)
	}
	query := `
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
		FROM event_review_cluster_identity_keys i
		JOIN event_review_clusters c ON c.id = i.cluster_id
		WHERE i.active = 1
			AND c.status = ?
			AND i.identity_key_id IN (` + placeholders(len(identityKeyIDs)) + `)
		ORDER BY c.id
		LIMIT 1
	`
	row := q.QueryRowContext(ctx, query, append([]any{string(seedstore.EventReviewClusterStatusResolved)}, args...)...)
	return scanEventReviewClusterRow(row)
}

func loadEventReviewClusterEndpointsTx(ctx context.Context, q queryer, clusterID int64) (eventReviewClusterEndpointSet, error) {
	endpoints := make(eventReviewClusterEndpointSet)
	rows, err := q.QueryContext(ctx, `
		SELECT e.evidence_fingerprint, e.event_id
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		WHERE ce.cluster_id = ?
			AND ce.active = 1
		ORDER BY ce.evidence_id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var fingerprint string
		var eventID sql.NullInt64
		if err := rows.Scan(&fingerprint, &eventID); err != nil {
			return nil, err
		}
		endpoints[eventReviewSeparationEndpointKeyEvidence(fingerprint)] = struct{}{}
		if eventID.Valid {
			endpoints[seedstore.EventReviewSeparationEventEndpointKey(eventID.Int64)] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = q.QueryContext(ctx, `
		SELECT i.identity_key_hash
		FROM event_review_cluster_identity_keys ci
		JOIN event_review_identity_keys i ON i.id = ci.identity_key_id
		WHERE ci.cluster_id = ?
			AND ci.active = 1
		ORDER BY ci.identity_key_id
	`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		endpoints[EventReviewSeparationEndpointKeyIdentity(hash)] = struct{}{}
	}
	return endpoints, rows.Err()
}

func loadEventReviewEvidenceEndpointsTx(ctx context.Context, q queryer, evidenceID int64, identityHashes map[int64]string, input seedstore.StageEventReviewEvidenceInput) (eventReviewClusterEndpointSet, error) {
	endpoints := make(eventReviewClusterEndpointSet)
	row := q.QueryRowContext(ctx, `
		SELECT evidence_fingerprint, event_id
		FROM event_review_evidence
		WHERE id = ?
	`, evidenceID)
	var fingerprint string
	var eventID sql.NullInt64
	if err := row.Scan(&fingerprint, &eventID); err != nil {
		return nil, err
	}
	endpoints[eventReviewSeparationEndpointKeyEvidence(fingerprint)] = struct{}{}
	if eventID.Valid {
		endpoints[seedstore.EventReviewSeparationEventEndpointKey(eventID.Int64)] = struct{}{}
	}
	if input.EventID != nil {
		endpoints[seedstore.EventReviewSeparationEventEndpointKey(*input.EventID)] = struct{}{}
	}
	for _, hash := range identityHashes {
		endpoints[EventReviewSeparationEndpointKeyIdentity(hash)] = struct{}{}
	}
	return endpoints, nil
}

func eventReviewEvidenceEndpointsForInput(input seedstore.StageEventReviewEvidenceInput, identityHashes map[int64]string) eventReviewClusterEndpointSet {
	endpoints := make(eventReviewClusterEndpointSet)
	endpoints[eventReviewSeparationEndpointKeyEvidence(strings.TrimSpace(input.EvidenceFingerprint))] = struct{}{}
	if input.EventID != nil {
		endpoints[seedstore.EventReviewSeparationEventEndpointKey(*input.EventID)] = struct{}{}
	}
	for _, hash := range identityHashes {
		endpoints[EventReviewSeparationEndpointKeyIdentity(hash)] = struct{}{}
	}
	return endpoints
}

func hasActiveEventReviewSeparationAmongKeysTx(ctx context.Context, q queryer, keys eventReviewClusterEndpointSet) (bool, error) {
	if len(keys) < 2 {
		return false, nil
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	args := make([]any, 0, len(ordered)*2)
	for _, key := range ordered {
		args = append(args, key)
	}
	for _, key := range ordered {
		args = append(args, key)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT endpoint_a_key, endpoint_b_key
		FROM event_review_separations
		WHERE active = 1
			AND (
				endpoint_a_key IN (`+placeholders(len(ordered))+`)
				OR endpoint_b_key IN (`+placeholders(len(ordered))+`)
			)
		ORDER BY id
	`, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return false, err
		}
		if _, ok := keys[a]; ok {
			if _, ok := keys[b]; ok {
				return true, nil
			}
		}
	}
	return false, rows.Err()
}

func unionEventReviewEndpointSets(sets ...eventReviewClusterEndpointSet) eventReviewClusterEndpointSet {
	out := make(eventReviewClusterEndpointSet)
	for _, set := range sets {
		for key := range set {
			out[key] = struct{}{}
		}
	}
	return out
}

func createEventReviewClusterTx(ctx context.Context, tx execer, status seedstore.EventReviewClusterStatus, stagingKey *string, stagingKeyVersion int, previousClusterID *int64, canonicalEventID *int64, conflictType, conflictReason string, now time.Time) (int64, error) {
	if status == seedstore.EventReviewClusterStatusOpen {
		if stagingKey == nil || strings.TrimSpace(*stagingKey) == "" {
			return 0, errors.New("event review cluster staging key is required")
		}
		if stagingKeyVersion <= 0 {
			return 0, errors.New("event review cluster staging key version is required")
		}
		trimmedStagingKey := strings.TrimSpace(*stagingKey)
		stagingKey = &trimmedStagingKey
	} else if stagingKey != nil {
		trimmedStagingKey := strings.TrimSpace(*stagingKey)
		if trimmedStagingKey == "" {
			stagingKey = nil
		} else {
			stagingKey = &trimmedStagingKey
		}
	}
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
		) VALUES (?, 1, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
	`, string(status), stagingKey, stagingKeyVersion, previousClusterID, canonicalEventID, strings.TrimSpace(conflictType), strings.TrimSpace(conflictReason), formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func stagingKeyPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func linkEventReviewEvidenceToClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID, evidenceID int64, reason string, now time.Time) error {
	if clusterID <= 0 || evidenceID <= 0 {
		return errors.New("event review cluster evidence link requires IDs")
	}
	exists, err := clusterHasActiveEvidenceTx(ctx, tx, clusterID, evidenceID)
	if err != nil || exists {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO event_review_cluster_evidence (
			cluster_id,
			evidence_id,
			active,
			linked_at,
			unlinked_at,
			link_reason
		) VALUES (?, ?, 1, ?, NULL, ?)
	`, clusterID, evidenceID, formatRFC3339UTC(now), strings.TrimSpace(reason))
	return err
}

func linkEventReviewIdentityKeysToClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID int64, rows eventReviewIdentityStageRows, now time.Time) error {
	for _, row := range rows.rows {
		exists, err := clusterHasActiveIdentityKeyTx(ctx, tx, clusterID, row.id)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_cluster_identity_keys (
				cluster_id,
				identity_key_id,
				active,
				linked_at,
				unlinked_at
			) VALUES (?, ?, 1, ?, NULL)
		`, clusterID, row.id, formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	return nil
}

func linkEventReviewRunToClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, runRef seedstore.EventReviewRunRef, clusterID int64, now time.Time) error {
	if clusterID <= 0 || !runRef.Valid() {
		return errors.New("event review run link requires a valid run ref and cluster ID")
	}
	switch runRef.Kind {
	case seedstore.EventReviewRunKindImport:
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at)
			VALUES (?, ?, ?)
		`, runRef.ID, clusterID, formatRFC3339UTC(now)); err != nil {
			return err
		}
	case seedstore.EventReviewRunKindRepair:
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO repair_run_event_review_clusters (repair_run_id, cluster_id, linked_at)
			VALUES (?, ?, ?)
		`, runRef.ID, clusterID, formatRFC3339UTC(now)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported event review run kind %q", runRef.Kind)
	}
	return nil
}

func touchOpenEventReviewClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID int64, version int, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, formatRFC3339UTC(now), clusterID, version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event review cluster %d version conflict", clusterID)
	}
	return nil
}

func fillEventReviewEvidenceEventIDTx(ctx context.Context, tx interface {
	execer
	queryer
}, evidenceID int64, eventID *int64, now time.Time) error {
	if eventID == nil {
		return nil
	}
	var existingEventID sql.NullInt64
	switch err := tx.QueryRowContext(ctx, `
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, evidenceID).Scan(&existingEventID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if existingEventID.Valid {
		return nil
	}
	terminalLinked, err := eventReviewEvidenceIsTerminalLinkedTx(ctx, tx, evidenceID)
	if err != nil {
		return err
	}
	if terminalLinked {
		return fmt.Errorf("event review evidence %d is terminal-linked and immutable", evidenceID)
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE event_review_evidence
		SET event_id = ?,
			updated_at = ?
		WHERE id = ?
			AND event_id IS NULL
	`, *eventID, formatRFC3339UTC(now), evidenceID)
	return err
}

func setOpenEventReviewClusterLineageTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID int64, version int, resolvedCluster seedstore.EventReviewCluster, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET previous_cluster_id = COALESCE(previous_cluster_id, ?),
			canonical_event_id = COALESCE(canonical_event_id, ?),
			conflict_type = CASE
				WHEN TRIM(conflict_type) = '' THEN ?
				ELSE conflict_type
			END,
			conflict_reason = CASE
				WHEN TRIM(conflict_reason) = '' THEN ?
				ELSE conflict_reason
			END,
			updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, resolvedCluster.ID, resolvedCluster.CanonicalEventID, resolvedCluster.ConflictType, resolvedCluster.ConflictReason, formatRFC3339UTC(now), clusterID, version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event review cluster %d version conflict", clusterID)
	}
	return nil
}

func backfillOpenEventReviewClusterConflictMetadataTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID int64, version int, conflictType, conflictReason string, now time.Time) error {
	conflictType = strings.TrimSpace(conflictType)
	conflictReason = strings.TrimSpace(conflictReason)
	if conflictType == "" && conflictReason == "" {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET conflict_type = CASE
				WHEN TRIM(conflict_type) = '' THEN ?
				ELSE conflict_type
			END,
			conflict_reason = CASE
				WHEN TRIM(conflict_reason) = '' THEN ?
				ELSE conflict_reason
			END,
			updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, conflictType, conflictReason, formatRFC3339UTC(now), clusterID, version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("event review cluster %d version conflict", clusterID)
	}
	return nil
}

func mergeEventReviewClustersIfNeededTx(ctx context.Context, tx interface {
	execer
	queryer
}, survivor *seedstore.EventReviewCluster, survivorEndpoints eventReviewClusterEndpointSet, openClusters []seedstore.EventReviewCluster, evidenceEndpoints eventReviewClusterEndpointSet, now time.Time) (bool, []int64, []int64, []int64, error) {
	if survivor == nil {
		return false, nil, nil, nil, nil
	}
	merged := false
	var mergedIDs []int64
	var supersededIDs []int64
	var skippedIDs []int64

	for i := range openClusters {
		cluster := openClusters[i]
		if cluster.ID == survivor.ID {
			continue
		}

		endpoints, err := loadEventReviewClusterEndpointsTx(ctx, tx, cluster.ID)
		if err != nil {
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		conflict, err := hasActiveEventReviewSeparationAmongKeysTx(ctx, tx, unionEventReviewEndpointSets(evidenceEndpoints, survivorEndpoints, endpoints))
		if err != nil {
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		if conflict {
			skippedIDs = append(skippedIDs, cluster.ID)
			continue
		}
		if err := touchOpenEventReviewClusterTx(ctx, tx, cluster.ID, cluster.Version, now); err != nil {
			if isRetryableEventReviewStagingConflict(err) {
				return merged, mergedIDs, supersededIDs, skippedIDs, err
			}
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		if err := moveActiveEventReviewEvidenceLinksTx(ctx, tx, cluster.ID, survivor.ID, now); err != nil {
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		if err := moveActiveEventReviewIdentityLinksTx(ctx, tx, cluster.ID, survivor.ID, now); err != nil {
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		if err := supersedeEventReviewClusterTx(ctx, tx, cluster, survivor.ID, now); err != nil {
			if strings.Contains(err.Error(), "rejected") {
				return merged, mergedIDs, supersededIDs, skippedIDs, fmt.Errorf("event review cluster %d version conflict", cluster.ID)
			}
			return merged, mergedIDs, supersededIDs, skippedIDs, err
		}
		merged = true
		mergedIDs = append(mergedIDs, cluster.ID)
		supersededIDs = append(supersededIDs, cluster.ID)
		survivorEndpoints = unionEventReviewEndpointSets(survivorEndpoints, endpoints)
	}
	return merged, mergedIDs, supersededIDs, skippedIDs, nil
}

func pruneOpenEventReviewClusterChoicesTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID int64) error {
	cluster, ok, err := loadEventReviewClusterTx(ctx, tx, clusterID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("event review cluster %d not found", clusterID)
	}
	activeSourceIdentityKeys, err := loadEventReviewClusterActiveSourceIdentityKeySetTx(ctx, tx, clusterID)
	if err != nil {
		return err
	}
	activeEvidenceIDs, activeEventIDs, err := loadEventReviewClusterActiveEvidenceAndEventIDsTx(ctx, tx, clusterID)
	if err != nil {
		return err
	}
	activeFieldNames, err := loadEventReviewClusterObservationFieldNamesTx(ctx, tx, clusterID)
	if err != nil {
		return err
	}

	sourceIdentityChoices, err := loadEventReviewClusterSourceIdentityChoiceSummariesTx(ctx, tx, clusterID)
	if err != nil {
		return err
	}
	for _, choice := range sourceIdentityChoices {
		if _, ok := activeSourceIdentityKeys[importCandidateSourceIdentityKey(choice.SourceID, choice.SourceIdentityKey)]; ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM event_review_source_identity_choices WHERE id = ?`, choice.ID); err != nil {
			return err
		}
	}

	for _, table := range []string{"event_review_canonical_choices", "event_review_draft_choices"} {
		choices, err := loadEventReviewClusterChoiceSummariesTx(ctx, tx, clusterID, table)
		if err != nil {
			return err
		}
		for _, choice := range choices {
			if eventReviewChoiceIsStillValid(choice, activeEvidenceIDs, activeEventIDs, activeFieldNames, cluster.CanonicalEventID) {
				continue
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE id = ?`, choice.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadEventReviewClusterActiveEvidenceAndEventIDsTx(ctx context.Context, q queryer, clusterID int64) (map[int64]struct{}, map[int64]struct{}, error) {
	evidenceSummaries, err := loadEventReviewClusterEvidenceSummariesTx(ctx, q, clusterID)
	if err != nil {
		return nil, nil, err
	}
	evidenceIDs := make(map[int64]struct{}, len(evidenceSummaries))
	eventIDs := make(map[int64]struct{})
	for _, evidence := range evidenceSummaries {
		evidenceIDs[evidence.EvidenceID] = struct{}{}
		if evidence.EventID != nil && *evidence.EventID > 0 {
			eventIDs[*evidence.EventID] = struct{}{}
		}
	}
	return evidenceIDs, eventIDs, nil
}

func loadEventReviewClusterObservationFieldNamesTx(ctx context.Context, q queryer, clusterID int64) (map[string]struct{}, error) {
	observations, err := loadEventReviewClusterObservationSummariesTx(ctx, q, clusterID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(observations))
	for _, observation := range observations {
		fieldName := strings.TrimSpace(observation.FieldName)
		if fieldName == "" {
			continue
		}
		out[fieldName] = struct{}{}
	}
	return out, nil
}

func eventReviewChoiceIsStillValid(choice seedstore.EventReviewClusterChoiceSummary, activeEvidenceIDs, activeEventIDs map[int64]struct{}, activeFieldNames map[string]struct{}, canonicalEventID *int64) bool {
	switch choice.ChoiceKind {
	case seedstore.EventReviewChoiceKindEvent:
		if choice.EventID == nil {
			return false
		}
		if canonicalEventID != nil && *choice.EventID == *canonicalEventID {
			return true
		}
		_, ok := activeEventIDs[*choice.EventID]
		return ok
	case seedstore.EventReviewChoiceKindEvidence:
		if choice.EvidenceID == nil {
			return false
		}
		_, ok := activeEvidenceIDs[*choice.EvidenceID]
		return ok
	case seedstore.EventReviewChoiceKindManual:
		_, ok := activeFieldNames[strings.TrimSpace(choice.FieldName)]
		return ok
	default:
		return false
	}
}

func deactivateFreshOpenEventReviewClusterLinksTx(ctx context.Context, tx interface {
	execer
	queryer
}, clusterID, keepEvidenceID int64, identityRows eventReviewIdentityStageRows, now time.Time) error {
	keepEvidenceIDs := []int64{keepEvidenceID}
	keepIdentityKeyIDs := make(map[int64]struct{}, len(identityRows.rows))
	for _, row := range identityRows.rows {
		keepIdentityKeyIDs[row.id] = struct{}{}
	}
	if err := deactivateMissingEventReviewClusterEvidenceTx(ctx, tx, clusterID, keepEvidenceIDs, now); err != nil {
		return err
	}
	return deactivateMissingEventReviewClusterIdentityKeysTx(ctx, tx, clusterID, keepIdentityKeyIDs, now)
}

func moveActiveEventReviewEvidenceLinksTx(ctx context.Context, tx interface {
	execer
	queryer
}, fromClusterID, toClusterID int64, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, evidence_id, linked_at, link_reason
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
		ORDER BY evidence_id, id
	`, fromClusterID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var row eventReviewEvidenceLinkRow
		if err := rows.Scan(&row.id, &row.evidenceID, &row.linkedAt, &row.reason); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_review_cluster_evidence
			SET active = 0,
				unlinked_at = ?
			WHERE id = ?
		`, formatRFC3339UTC(now), row.id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_cluster_evidence (
				cluster_id,
				evidence_id,
				active,
				linked_at,
				unlinked_at,
				link_reason
			) VALUES (?, ?, 1, ?, NULL, ?)
		`, toClusterID, row.evidenceID, row.linkedAt, row.reason); err != nil {
			return err
		}
	}
	return rows.Err()
}

func moveActiveEventReviewIdentityLinksTx(ctx context.Context, tx interface {
	execer
	queryer
}, fromClusterID, toClusterID int64, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, identity_key_id, linked_at
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND active = 1
		ORDER BY identity_key_id, id
	`, fromClusterID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var row eventReviewIdentityLinkRow
		if err := rows.Scan(&row.id, &row.identityKeyID, &row.linkedAt); err != nil {
			return err
		}
		exists, err := clusterHasActiveIdentityKeyTx(ctx, tx, toClusterID, row.identityKeyID)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_review_cluster_identity_keys
			SET active = 0,
				unlinked_at = ?
			WHERE id = ?
		`, formatRFC3339UTC(now), row.id); err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_review_cluster_identity_keys (
				cluster_id,
				identity_key_id,
				active,
				linked_at,
				unlinked_at
			) VALUES (?, ?, 1, ?, NULL)
		`, toClusterID, row.identityKeyID, row.linkedAt); err != nil {
			return err
		}
	}
	return rows.Err()
}

func clusterHasActiveEvidenceTx(ctx context.Context, q queryer, clusterID, evidenceID int64) (bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT 1
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND evidence_id = ?
			AND active = 1
		LIMIT 1
	`, clusterID, evidenceID)
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

func clusterNeedsEventReviewAttachmentTx(ctx context.Context, q queryer, clusterID, evidenceID int64, identityRows eventReviewIdentityStageRows) (bool, error) {
	exists, err := clusterHasActiveEvidenceTx(ctx, q, clusterID, evidenceID)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	for _, row := range identityRows.rows {
		exists, err := clusterHasActiveIdentityKeyTx(ctx, q, clusterID, row.id)
		if err != nil {
			return false, err
		}
		if !exists {
			return true, nil
		}
	}
	return false, nil
}

func clusterHasActiveIdentityKeyTx(ctx context.Context, q queryer, clusterID, identityKeyID int64) (bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT 1
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND identity_key_id = ?
			AND active = 1
		LIMIT 1
	`, clusterID, identityKeyID)
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

func isRetryableEventReviewStagingConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "version conflict")
}

func buildEventReviewIdentityKeyHash(kind seedstore.EventReviewIdentityKeyKind, version int, normalizedKey string) string {
	material := fmt.Sprintf("event-review:v%d:%s:%s", version, kind, strings.TrimSpace(normalizedKey))
	sum := sha256.Sum256([]byte(material))
	return "event-review:v1:" + hex.EncodeToString(sum[:])
}

func eventReviewSeparationEndpointKeyEvidence(fingerprint string) string {
	return "evidence:" + strings.TrimSpace(fingerprint)
}

func EventReviewSeparationEndpointKeyIdentity(hash string) string {
	return "identity:" + strings.TrimSpace(hash)
}

func scanEventReviewClusterRow(row interface {
	Scan(dest ...any) error
}) (seedstore.EventReviewCluster, bool, error) {
	var (
		cluster      seedstore.EventReviewCluster
		status       string
		stagingKey   sql.NullString
		stagingVer   sql.NullInt64
		supersededBy sql.NullInt64
		previous     sql.NullInt64
		canonical    sql.NullInt64
		createdAt    string
		updatedAt    string
	)
	switch err := row.Scan(
		&cluster.ID,
		&status,
		&cluster.Version,
		&stagingKey,
		&stagingVer,
		&supersededBy,
		&previous,
		&canonical,
		&cluster.ConflictType,
		&cluster.ConflictReason,
		&createdAt,
		&updatedAt,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return seedstore.EventReviewCluster{}, false, nil
	case err != nil:
		return seedstore.EventReviewCluster{}, false, err
	}

	cluster.Status = seedstore.EventReviewClusterStatus(status)
	if stagingKey.Valid {
		cluster.StagingKey = &stagingKey.String
	}
	if stagingVer.Valid {
		cluster.StagingKeyVersion = int(stagingVer.Int64)
	}
	if supersededBy.Valid {
		cluster.SupersededByClusterID = &supersededBy.Int64
	}
	if previous.Valid {
		cluster.PreviousClusterID = &previous.Int64
	}
	if canonical.Valid {
		cluster.CanonicalEventID = &canonical.Int64
	}
	created, err := parseRFC3339UTC(createdAt)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	updated, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return seedstore.EventReviewCluster{}, false, err
	}
	cluster.CreatedAt = created
	cluster.UpdatedAt = updated
	return cluster, true, nil
}

func scanEventReviewClusterRows(rows *sql.Rows) (seedstore.EventReviewCluster, error) {
	cluster, ok, err := scanEventReviewClusterRow(rows)
	if err != nil {
		return seedstore.EventReviewCluster{}, err
	}
	if !ok {
		return seedstore.EventReviewCluster{}, sql.ErrNoRows
	}
	return cluster, nil
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func sortedInt64Keys(keys map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
