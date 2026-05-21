package sqlite

import (
	"context"
	"database/sql"
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

type eventReviewResolutionSnapshot struct {
	ClusterID             int64                                               `json:"cluster_id"`
	ExpectedVersion       int                                                 `json:"expected_version"`
	CurrentVersion        int                                                 `json:"current_version"`
	CurrentStatus         seedstore.EventReviewClusterStatus                  `json:"current_status"`
	TargetStatus          seedstore.EventReviewResolutionStatus               `json:"target_status"`
	DiscardReason         string                                              `json:"discard_reason,omitempty"`
	SupersededByClusterID int64                                               `json:"superseded_by_cluster_id,omitempty"`
	CanonicalEventID      *int64                                              `json:"canonical_event_id,omitempty"`
	RepairRunID           *int64                                              `json:"repair_run_id,omitempty"`
	AppliedAutoResolution *eventReviewResolutionAppliedAutoResolutionSnapshot `json:"applied_auto_resolution,omitempty"`
	AppliedImportListing  *eventReviewResolutionAppliedImportListingSnapshot  `json:"applied_import_listing,omitempty"`
	AppliedTitleRepair    *eventReviewResolutionAppliedTitleRepairSnapshot    `json:"applied_title_repair,omitempty"`
	AppliedLiveActions    []eventReviewResolutionAppliedLiveActionSnapshot    `json:"applied_live_actions,omitempty"`
	RecordedAt            string                                              `json:"recorded_at"`
}

type eventReviewResolutionAppliedAutoResolutionSnapshot struct {
	EventID       int64  `json:"event_id"`
	EventSlug     string `json:"event_slug,omitempty"`
	Result        string `json:"result,omitempty"`
	SourceID      int64  `json:"source_id,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	EvidenceCount int    `json:"evidence_count,omitempty"`
}

type eventReviewResolutionAppliedLiveActionSnapshot struct {
	EventID   int64                               `json:"event_id"`
	EventSlug string                              `json:"event_slug,omitempty"`
	Action    seedstore.EventReviewLiveActionKind `json:"action"`
	Reason    string                              `json:"reason,omitempty"`
}

type eventReviewResolutionAppliedTitleRepairSnapshot struct {
	EventID  int64  `json:"event_id"`
	OldTitle string `json:"old_title,omitempty"`
	NewTitle string `json:"new_title,omitempty"`
	OldSlug  string `json:"old_slug,omitempty"`
	NewSlug  string `json:"new_slug,omitempty"`
}

type eventReviewResolutionAppliedImportListingSnapshot struct {
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

func (s *Store) ResolveEventReviewCluster(ctx context.Context, input seedstore.EventReviewResolutionInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	cluster, tx, err := beginOpenEventReviewClusterTx(ctx, s.db, input)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	switch cluster.ConflictType {
	case historicalDuplicateRepairConflictType:
		if cluster.CanonicalEventID == nil || *cluster.CanonicalEventID <= 0 {
			return fmt.Errorf("historical duplicate event review cluster %d requires a canonical event", cluster.ID)
		}

		liveActions, err := loadEventReviewClusterLiveActionSummariesTx(ctx, tx, cluster.ID)
		if err != nil {
			return err
		}
		if len(liveActions) == 0 {
			return fmt.Errorf("historical duplicate event review cluster %d has no stored live actions", cluster.ID)
		}

		canonicalEventID := *cluster.CanonicalEventID
		seenEventIDs := make(map[int64]struct{}, len(liveActions))
		withholdEventIDs := make([]int64, 0, len(liveActions))
		appliedActions := make([]eventReviewResolutionAppliedLiveActionSnapshot, 0, len(liveActions))
		keepCount := 0
		for _, action := range liveActions {
			if !action.Action.Valid() {
				return fmt.Errorf("historical duplicate event review cluster %d has unsupported live action %q for event %d", cluster.ID, action.Action, action.EventID)
			}
			if _, ok := seenEventIDs[action.EventID]; ok {
				return fmt.Errorf("historical duplicate event review cluster %d has duplicate live action event %d", cluster.ID, action.EventID)
			}
			seenEventIDs[action.EventID] = struct{}{}
			appliedActions = append(appliedActions, eventReviewResolutionAppliedLiveActionSnapshot{
				EventID:   action.EventID,
				EventSlug: action.EventSlug,
				Action:    action.Action,
				Reason:    action.Reason,
			})
			switch action.Action {
			case seedstore.EventReviewLiveActionKindKeepSeparate:
				if action.EventID != canonicalEventID {
					return fmt.Errorf("historical duplicate event review cluster %d keep_separate action must target canonical event %d", cluster.ID, canonicalEventID)
				}
				keepCount++
			case seedstore.EventReviewLiveActionKindWithholdDuplicate:
				if action.EventID == canonicalEventID {
					return fmt.Errorf("historical duplicate event review cluster %d withhold_duplicate action must target a non-canonical event", cluster.ID)
				}
				withholdEventIDs = append(withholdEventIDs, action.EventID)
			default:
				return fmt.Errorf("historical duplicate event review cluster %d has unsupported live action %q for event %d", cluster.ID, action.Action, action.EventID)
			}
		}
		if keepCount != 1 {
			return fmt.Errorf("historical duplicate event review cluster %d requires exactly one keep_separate action for canonical event %d", cluster.ID, canonicalEventID)
		}
		if len(withholdEventIDs) == 0 {
			return fmt.Errorf("historical duplicate event review cluster %d requires at least one withhold_duplicate action", cluster.ID)
		}

		now := time.Now().UTC()
		repairRunNotes := fmt.Sprintf("historical duplicate event review cluster %d resolution", cluster.ID)
		repairRunID, err := createHistoricalDuplicateRepairRunTx(ctx, tx, now, repairRunNotes)
		if err != nil {
			return err
		}
		for _, loserID := range withholdEventIDs {
			if _, err := withholdHistoricalDuplicateEventTx(ctx, tx, loserID, canonicalEventID, repairRunID, now, historicalDuplicateWithholdOptions{
				AllowReviewed:          true,
				DetachLoserSourceLinks: true,
			}); err != nil {
				return err
			}
		}
		if err := finishHistoricalDuplicateRepairRunTx(ctx, tx, repairRunID, historicalDuplicateRepairRunStatusSucceeded, repairRunNotes); err != nil {
			return err
		}

		snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusResolved, "", nil, &repairRunID, nil, nil, nil, appliedActions, now)
		if err != nil {
			return err
		}
		if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusResolved, snapshot, "", now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE event_review_clusters
			SET status = ?, version = version + 1, updated_at = ?
			WHERE id = ?
				AND version = ?
				AND status = ?
		`, string(seedstore.EventReviewClusterStatusResolved), formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
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
	case seedstore.EventReviewConflictTypeImportReview:
		if cluster.ConflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
			return fmt.Errorf("import review event review cluster %d conflict reason %q is not supported", cluster.ID, cluster.ConflictReason)
		}
		if err := resolveImportReviewClusterTx(ctx, tx, s, cluster); err != nil {
			return err
		}
		return tx.Commit()
	case eventTitleRepairConflictType:
		draftChoices, err := loadEventReviewClusterChoiceSummariesTx(ctx, tx, cluster.ID, "event_review_draft_choices")
		if err != nil {
			return err
		}
		readiness, err := loadEventReviewTitleRepairReadinessTx(ctx, tx, seedstore.EventReviewClusterSummary{
			ID:               cluster.ID,
			Status:           cluster.Status,
			Version:          cluster.Version,
			ConflictType:     cluster.ConflictType,
			ConflictReason:   cluster.ConflictReason,
			CanonicalEventID: cluster.CanonicalEventID,
		}, draftChoices)
		if err != nil {
			return err
		}
		if readiness == nil || !readiness.Eligible {
			reasons := ""
			if readiness != nil {
				reasons = strings.Join(readiness.BlockingReasons, "; ")
			}
			if reasons == "" {
				reasons = "title repair cluster is not eligible"
			}
			return fmt.Errorf("title repair event review cluster %d is not eligible: %s", cluster.ID, reasons)
		}

		now := time.Now().UTC()
		applied := eventReviewResolutionAppliedTitleRepairSnapshot{
			EventID:  readiness.CanonicalEventID,
			OldTitle: readiness.CurrentTitle,
			NewTitle: readiness.DraftTitle,
			OldSlug:  readiness.CurrentSlug,
			NewSlug:  readiness.DraftSlug,
		}
		if err := updateEventTitleTx(ctx, tx, readiness.CanonicalEventID, readiness.DraftSlug, readiness.DraftTitle, now); err != nil {
			return err
		}
		snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusResolved, "", nil, nil, nil, nil, &applied, nil, now)
		if err != nil {
			return err
		}
		if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusResolved, snapshot, "", now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE event_review_clusters
			SET status = ?, version = version + 1, updated_at = ?
			WHERE id = ?
				AND version = ?
				AND status = ?
		`, string(seedstore.EventReviewClusterStatusResolved), formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
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
	default:
		return fmt.Errorf("event review cluster %d conflict type %q is not supported", cluster.ID, cluster.ConflictType)
	}
}

func resolveImportReviewClusterTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, cluster seedstore.EventReviewCluster) error {
	evidence, err := loadEventReviewClusterEvidenceSummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}
	if len(evidence) == 0 {
		return fmt.Errorf("import review event review cluster %d is not eligible: no active evidence rows are present", cluster.ID)
	}

	summary := seedstore.EventReviewClusterSummary{
		ID:               cluster.ID,
		Status:           cluster.Status,
		Version:          cluster.Version,
		ConflictType:     cluster.ConflictType,
		ConflictReason:   cluster.ConflictReason,
		CanonicalEventID: cluster.CanonicalEventID,
	}
	if len(evidence) == 1 {
		readiness := loadEventReviewImportReadinessTx(summary, evidence, nil, nil, nil, nil)
		if readiness == nil || !readiness.NewListingScope || len(readiness.Candidates) != 1 {
			reasons := ""
			if readiness != nil {
				reasons = strings.Join(readiness.BlockingReasons, "; ")
			}
			if reasons == "" {
				reasons = "import review cluster is not eligible"
			}
			return fmt.Errorf("import review event review cluster %d is not eligible: %s", cluster.ID, reasons)
		}
		candidate := readiness.Candidates[0]
		return applyImportReviewListingEvidenceTx(ctx, tx, s, cluster, evidence[0], candidate, nil)
	}

	evidenceIdentityKeys, err := loadEventReviewEvidenceIdentityKeySummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}
	exactIdentityMatches, err := loadEventReviewClusterExactIdentityMatchSummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}
	sourceIdentityLinks, err := loadEventReviewClusterSourceIdentityLinkSummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}
	sourceIdentityChoices, err := loadEventReviewClusterSourceIdentityChoiceSummariesTx(ctx, tx, cluster.ID)
	if err != nil {
		return err
	}

	readiness := loadEventReviewImportReadinessTx(summary, evidence, evidenceIdentityKeys, exactIdentityMatches, sourceIdentityLinks, sourceIdentityChoices)
	if readiness == nil || readiness.SelectedCandidateReadiness == nil || !readiness.SelectedCandidateReadiness.Eligible {
		reasons := ""
		if readiness != nil && readiness.SelectedCandidateReadiness != nil {
			reasons = strings.Join(readiness.SelectedCandidateReadiness.BlockingReasons, "; ")
		}
		if reasons == "" {
			reasons = "selected candidate readiness is not eligible"
		}
		return fmt.Errorf("import review event review cluster %d is not eligible: %s", cluster.ID, reasons)
	}

	selectedEvidenceID := readiness.SelectedCandidateReadiness.EvidenceID
	var selectedEvidence *seedstore.EventReviewClusterEvidenceSummary
	for i := range evidence {
		if evidence[i].EvidenceID == selectedEvidenceID {
			selectedEvidence = &evidence[i]
			break
		}
	}
	if selectedEvidence == nil {
		return fmt.Errorf("import review event review cluster %d selected evidence is not active", cluster.ID)
	}

	candidateByEvidenceID := make(map[int64]seedstore.EventReviewImportCandidateSummary, len(readiness.Candidates))
	for _, candidate := range readiness.Candidates {
		candidateByEvidenceID[candidate.EvidenceID] = candidate
	}
	candidate, ok := candidateByEvidenceID[selectedEvidenceID]
	if !ok {
		return fmt.Errorf("import review event review cluster %d selected evidence is not active", cluster.ID)
	}

	selectedSourceKeys := make([]string, 0, len(readiness.SelectedCandidateReadiness.SelectedSourceKeys))
	for _, selectedSourceKey := range readiness.SelectedCandidateReadiness.SelectedSourceKeys {
		if key := strings.TrimSpace(selectedSourceKey.SourceIdentityKey); key != "" {
			selectedSourceKeys = append(selectedSourceKeys, key)
		}
	}
	if len(selectedSourceKeys) == 0 {
		return fmt.Errorf("import review event review cluster %d selected candidate has no selected source identity keys", cluster.ID)
	}

	return applyImportReviewListingEvidenceTx(ctx, tx, s, cluster, *selectedEvidence, candidate, selectedSourceKeys)
}

func applyImportReviewListingEvidenceTx(ctx context.Context, tx interface {
	execer
	queryer
}, s *Store, cluster seedstore.EventReviewCluster, evidence seedstore.EventReviewClusterEvidenceSummary, candidate seedstore.EventReviewImportCandidateSummary, selectedSourceKeys []string) error {
	if candidate.SourceAuthority != seedstore.SourceAuthoritySupporting {
		return fmt.Errorf("import review event review cluster %d requires a supporting candidate", cluster.ID)
	}

	parsed, err := parseImportReviewCandidatePayload(evidence.Payload)
	if err != nil {
		return fmt.Errorf("import review event review cluster %d payload could not be parsed: %w", cluster.ID, err)
	}
	if strings.TrimSpace(parsed.Title) == "" {
		return fmt.Errorf("import review event review cluster %d requires a candidate title", cluster.ID)
	}
	start, err := parseRFC3339UTC(strings.TrimSpace(parsed.StartAt))
	if err != nil {
		return fmt.Errorf("import review event review cluster %d requires a valid start time: %w", cluster.ID, err)
	}
	var end time.Time
	if endText := strings.TrimSpace(parsed.EndAt); endText != "" {
		end, err = parseRFC3339UTC(endText)
		if err != nil {
			return fmt.Errorf("import review event review cluster %d has invalid end time: %w", cluster.ID, err)
		}
	}

	candidateInput := review.CandidateInput{
		ExternalID:     strings.TrimSpace(parsed.ExternalID),
		Name:           strings.TrimSpace(parsed.Title),
		VenueSlug:      strings.TrimSpace(parsed.VenueSlug),
		VenueText:      strings.TrimSpace(parsed.VenueText),
		RoomText:       strings.TrimSpace(parsed.RoomText),
		Rooms:          parsedRoomsFromImportReviewPayload(parsed.Rooms),
		StartAt:        formatRFC3339UTC(start),
		EndAt:          nullableImportReviewEndAt(end),
		Genre:          strings.TrimSpace(parsed.Genre),
		Status:         strings.TrimSpace(parsed.Status),
		Description:    strings.TrimSpace(parsed.Description),
		ImageURL:       strings.TrimSpace(parsed.ImageURL),
		ImageSourceURL: strings.TrimSpace(parsed.ImageSourceURL),
		ImageAlt:       strings.TrimSpace(parsed.ImageAlt),
		ImageWidth:     parsed.ImageWidth,
		ImageHeight:    parsed.ImageHeight,
		ImageFocusX:    parsed.ImageFocusX,
		ImageFocusY:    parsed.ImageFocusY,
		SourceName:     firstNonEmptyImportReviewText(parsed.SourceName, evidence.SourceName),
		SourceURL:      firstNonEmptyImportReviewText(parsed.SourceURL, evidence.SourceURL),
		CalendarURL:    strings.TrimSpace(parsed.CalendarURL),
		Provenance:     strings.TrimSpace(parsed.Provenance),
	}
	if candidateInput.SourceName == "" {
		candidateInput.SourceName = strings.TrimSpace(evidence.SourceName)
	}
	if candidateInput.SourceURL == "" {
		candidateInput.SourceURL = strings.TrimSpace(evidence.SourceURL)
	}
	if candidateInput.VenueSlug == "" && candidateInput.VenueText == "" {
		return fmt.Errorf("import review event review cluster %d requires a venue", cluster.ID)
	}

	now := time.Now().UTC()
	venue, err := resolveImportReviewVenueTx(ctx, tx, candidateInput)
	if err != nil {
		return err
	}
	candidateInput.VenueSlug = strings.TrimSpace(venue.Slug)
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, candidateInput.VenueSlug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("import review event review cluster %d venue %q could not be loaded", cluster.ID, candidateInput.VenueSlug)
	}

	event := domain.Event{
		Slug:               "",
		Name:               candidateInput.Name,
		VenueSlug:          candidateInput.VenueSlug,
		Rooms:              append([]domain.VenueRoom(nil), candidateInput.Rooms...),
		RoomText:           candidateInput.RoomText,
		Start:              start,
		End:                end,
		Genre:              candidateInput.Genre,
		Status:             candidateInput.Status,
		Description:        candidateInput.Description,
		ImageURL:           candidateInput.ImageURL,
		ImageSourceURL:     candidateInput.ImageSourceURL,
		ImageAlt:           candidateInput.ImageAlt,
		ImageWidth:         candidateInput.ImageWidth,
		ImageHeight:        candidateInput.ImageHeight,
		ImageFocusX:        candidateInput.ImageFocusX,
		ImageFocusY:        candidateInput.ImageFocusY,
		SourceName:         candidateInput.SourceName,
		SourceURL:          candidateInput.SourceURL,
		OfficialListingURL: candidateInput.SourceURL,
		CalendarURL:        strings.TrimSpace(parsed.CalendarURL),
		LastChecked:        now,
		Origin:             domain.OriginLive,
		PublicationState:   domain.PublicationStateReviewed,
	}
	event.Slug, err = buildLiveEventSlug(event.Name, event.VenueSlug, event.Start)
	if err != nil {
		return fmt.Errorf("import review event review cluster %d cannot build event slug: %w", cluster.ID, err)
	}
	event = s.decorateEventForPublish(event)
	if err := event.ValidateCanonical(); err != nil {
		return fmt.Errorf("import review event review cluster %d is not a valid event: %w", cluster.ID, err)
	}

	if _, ok, err := loadLiveEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("import review event review cluster %d event slug %q already belongs to a live event", cluster.ID, event.Slug)
	}
	exactKey, err := buildImportReviewExactIdentityKey(event)
	if err != nil {
		return err
	}
	if _, ok, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, tx, exactKey); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("import review event review cluster %d exact identity already belongs to a live event", cluster.ID)
	}

	sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentitySupporting, candidateInput.SourceName, candidateInput.SourceURL, "", "", "", candidateInput, "import_review_accept_new_listing")
	if selectedSourceKeys != nil {
		selectedIdentities := ingest.SourceIdentitiesFromKeys(selectedSourceKeys)
		if len(selectedIdentities.Keys()) == 0 {
			return fmt.Errorf("import review event review cluster %d selected candidate has no selected source identity keys", cluster.ID)
		}
		sourceCtx = reviewSourceIdentityContext{
			SourceName:            candidateInput.SourceName,
			SourceURL:             candidateInput.SourceURL,
			Identities:            selectedIdentities,
			PrimaryObservationKey: selectedIdentities.PrimaryKey(),
			CandidateProvenance:   strings.TrimSpace(candidateInput.Provenance),
			Entrypoint:            "import_review_accept_selected_candidate",
		}
	}
	if record, ok, ambiguous, err := resolveLiveEventRecordBySourceIdentitiesTx(ctx, tx, evidence.SourceID, sourceCtx.Identities); err != nil {
		return err
	} else if ambiguous {
		return fmt.Errorf("import review event review cluster %d source identities resolve ambiguously", cluster.ID)
	} else if ok {
		return fmt.Errorf("import review event review cluster %d source identities already belong to live event %d", cluster.ID, record.ID)
	}
	if near, _, err := guardedNearLiveEventMatchForEventTx(ctx, tx, event, s.sourceMetadata); err != nil {
		return err
	} else if len(near) > 0 {
		return fmt.Errorf("import review event review cluster %d matches an existing live event too closely", cluster.ID)
	}

	eventID, err := insertEventTx(ctx, tx, event, venueID, evidence.SourceID, now)
	if err != nil {
		return err
	}
	if writeResult, err := ensureEventSourceLinkForSourceIdentityContextTx(ctx, tx, eventID, evidence.SourceID, sourceCtx, sourceLinkAuthoritySupporting, sourceLinkConflictPolicyNoMove, now); err != nil {
		return err
	} else if writeResult.Ambiguous {
		return fmt.Errorf("import review event review cluster %d source identity link is ambiguous", cluster.ID)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_evidence
		SET event_id = ?,
			updated_at = ?
		WHERE id = ?
			AND event_id IS NULL
	`, eventID, formatRFC3339UTC(now), evidence.EvidenceID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("import review event review cluster %d selected evidence update was rejected", cluster.ID)
	}
	if err := refreshEventGenresTx(ctx, tx, eventID, event.Description, nil, now); err != nil {
		return err
	}

	applied := eventReviewResolutionAppliedImportListingSnapshot{
		EventID:    eventID,
		EventSlug:  event.Slug,
		Title:      event.Name,
		VenueSlug:  event.VenueSlug,
		VenueName:  venue.Name,
		StartAt:    formatRFC3339UTC(event.Start),
		SourceID:   evidence.SourceID,
		SourceName: candidateInput.SourceName,
		SourceURL:  candidateInput.SourceURL,
		EvidenceID: evidence.EvidenceID,
	}
	snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusResolved, "", nil, nil, nil, &applied, nil, nil, now)
	if err != nil {
		return err
	}
	if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusResolved, snapshot, "", now); err != nil {
		return err
	}
	clusterUpdateRes, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET status = ?, canonical_event_id = ?, version = version + 1, updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, string(seedstore.EventReviewClusterStatusResolved), eventID, formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	clusterRowsAffected, err := clusterUpdateRes.RowsAffected()
	if err != nil {
		return err
	}
	if clusterRowsAffected != 1 {
		return fmt.Errorf("event review cluster %d update was rejected", cluster.ID)
	}
	return nil
}

type eventReviewImportReviewListingPayload struct {
	SourceAuthority string                                      `json:"source_authority"`
	SourceName      string                                      `json:"source_name,omitempty"`
	SourceURL       string                                      `json:"source_url,omitempty"`
	CalendarURL     string                                      `json:"calendar_url,omitempty"`
	Provenance      string                                      `json:"provenance,omitempty"`
	ExternalID      string                                      `json:"candidate_external_id,omitempty"`
	Title           string                                      `json:"candidate_title,omitempty"`
	VenueSlug       string                                      `json:"candidate_venue_slug,omitempty"`
	VenueText       string                                      `json:"candidate_venue_text,omitempty"`
	RoomText        string                                      `json:"candidate_room_text,omitempty"`
	Rooms           []eventReviewImportReviewListingRoomPayload `json:"candidate_rooms,omitempty"`
	StartAt         string                                      `json:"candidate_start_at,omitempty"`
	EndAt           string                                      `json:"candidate_end_at,omitempty"`
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
}

type eventReviewImportReviewListingRoomPayload struct {
	VenueSlug string `json:"venue_slug"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
}

func parseImportReviewCandidatePayload(payload string) (eventReviewImportCandidatePayload, error) {
	var parsed eventReviewImportCandidatePayload
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		return eventReviewImportCandidatePayload{}, err
	}
	return parsed, nil
}

func resolveImportReviewVenueTx(ctx context.Context, q queryer, candidate review.CandidateInput) (domain.Venue, error) {
	if venueSlug := strings.TrimSpace(candidate.VenueSlug); venueSlug != "" {
		venue, ok, err := loadVenueBySlug(ctx, q, venueSlug)
		if err != nil {
			return domain.Venue{}, err
		}
		if ok {
			return venue, nil
		}
	}

	matcher, err := loadVenueMatcher(ctx, q)
	if err != nil {
		return domain.Venue{}, err
	}
	match := matcher.matchCandidate(review.Candidate{
		ExternalID:  strings.TrimSpace(candidate.ExternalID),
		Name:        strings.TrimSpace(candidate.Name),
		VenueSlug:   strings.TrimSpace(candidate.VenueSlug),
		VenueText:   strings.TrimSpace(candidate.VenueText),
		RoomText:    strings.TrimSpace(candidate.RoomText),
		Rooms:       append([]domain.VenueRoom(nil), candidate.Rooms...),
		StartAt:     strings.TrimSpace(candidate.StartAt),
		EndAt:       strings.TrimSpace(candidate.EndAt),
		Genre:       strings.TrimSpace(candidate.Genre),
		Status:      strings.TrimSpace(candidate.Status),
		Description: strings.TrimSpace(candidate.Description),
		SourceName:  strings.TrimSpace(candidate.SourceName),
		SourceURL:   strings.TrimSpace(candidate.SourceURL),
		CalendarURL: strings.TrimSpace(candidate.CalendarURL),
	})
	switch match.status {
	case venueMatchResolved:
		venue, ok, err := loadVenueBySlug(ctx, q, match.slug)
		if err != nil {
			return domain.Venue{}, err
		}
		if ok {
			return venue, nil
		}
		return domain.Venue{}, fmt.Errorf("import review venue match %q could not be loaded", match.slug)
	case venueMatchAmbiguous:
		return domain.Venue{}, fmt.Errorf("import review venue is ambiguous")
	default:
		return domain.Venue{}, fmt.Errorf("import review venue could not be resolved")
	}
}

func parsedRoomsFromImportReviewPayload(rooms []eventReviewImportReviewListingRoomPayload) []domain.VenueRoom {
	if len(rooms) == 0 {
		return nil
	}
	out := make([]domain.VenueRoom, 0, len(rooms))
	for _, room := range rooms {
		venueSlug := strings.TrimSpace(room.VenueSlug)
		slug := strings.TrimSpace(room.Slug)
		name := strings.TrimSpace(room.Name)
		if slug == "" && name != "" {
			slug = slugFromText(name)
		}
		if venueSlug == "" || slug == "" || name == "" {
			continue
		}
		out = append(out, domain.VenueRoom{
			VenueSlug: venueSlug,
			Slug:      slug,
			Name:      name,
		})
	}
	return out
}

func firstNonEmptyImportReviewText(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nullableImportReviewEndAt(end time.Time) string {
	if end.IsZero() {
		return ""
	}
	return formatRFC3339UTC(end)
}

func buildImportReviewExactIdentityKey(event domain.Event) (string, error) {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("import review event %q cannot produce exact identity material", event.Slug)
	}
	return buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle), nil
}

func (s *Store) DiscardEventReviewCluster(ctx context.Context, input seedstore.EventReviewDiscardInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	cluster, tx, err := beginOpenEventReviewClusterTx(ctx, s.db, input.EventReviewResolutionInput)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	reason := strings.TrimSpace(input.Reason)
	if reason == "" {
		return errors.New("discard reason is required")
	}

	now := time.Now().UTC()
	snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusDiscarded, reason, nil, nil, nil, nil, nil, nil, now)
	if err != nil {
		return err
	}
	if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusDiscarded, snapshot, reason, now); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET status = ?, version = version + 1, updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, string(seedstore.EventReviewClusterStatusDiscarded), formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
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

func (s *Store) SupersedeEventReviewCluster(ctx context.Context, input seedstore.EventReviewSupersedeInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if input.SupersededByClusterID <= 0 {
		return errors.New("superseded_by_cluster_id is required")
	}
	if input.SupersededByClusterID == input.ClusterID {
		return fmt.Errorf("event review cluster %d cannot supersede itself", input.ClusterID)
	}

	cluster, tx, err := beginOpenEventReviewClusterTx(ctx, s.db, input.EventReviewResolutionInput)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	supersedingCluster, ok, err := loadEventReviewClusterTx(ctx, tx, input.SupersededByClusterID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("event review cluster %d not found", input.SupersededByClusterID)
	}
	if supersedingCluster.ID == cluster.ID {
		return fmt.Errorf("event review cluster %d cannot supersede itself", cluster.ID)
	}

	now := time.Now().UTC()
	if err := supersedeEventReviewClusterTx(ctx, tx, cluster, input.SupersededByClusterID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func beginOpenEventReviewClusterTx(ctx context.Context, db *sql.DB, input seedstore.EventReviewResolutionInput) (seedstore.EventReviewCluster, *sql.Tx, error) {
	if input.ClusterID <= 0 {
		return seedstore.EventReviewCluster{}, nil, errors.New("event review cluster ID is required")
	}
	if input.ExpectedVersion <= 0 {
		return seedstore.EventReviewCluster{}, nil, errors.New("expected version is required")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return seedstore.EventReviewCluster{}, nil, err
	}

	cluster, ok, err := loadEventReviewClusterTx(ctx, tx, input.ClusterID)
	if err != nil {
		_ = tx.Rollback()
		return seedstore.EventReviewCluster{}, nil, err
	}
	if !ok {
		_ = tx.Rollback()
		return seedstore.EventReviewCluster{}, nil, fmt.Errorf("event review cluster %d not found", input.ClusterID)
	}
	if cluster.Status != seedstore.EventReviewClusterStatusOpen {
		_ = tx.Rollback()
		return seedstore.EventReviewCluster{}, nil, fmt.Errorf("event review cluster %d is not open", input.ClusterID)
	}
	if cluster.Version != input.ExpectedVersion {
		_ = tx.Rollback()
		return seedstore.EventReviewCluster{}, nil, fmt.Errorf("event review cluster %d has version %d, want %d", input.ClusterID, cluster.Version, input.ExpectedVersion)
	}
	return cluster, tx, nil
}

func loadEventReviewClusterTx(ctx context.Context, q queryer, clusterID int64) (seedstore.EventReviewCluster, bool, error) {
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
		WHERE id = ?
	`, clusterID)

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

func supersedeEventReviewClusterTx(ctx context.Context, tx execer, cluster seedstore.EventReviewCluster, supersededByClusterID int64, now time.Time) error {
	snapshot, err := marshalEventReviewResolutionSnapshot(cluster, seedstore.EventReviewResolutionStatusSuperseded, "", &supersededByClusterID, nil, nil, nil, nil, nil, now)
	if err != nil {
		return err
	}
	if _, err := insertEventReviewResolutionTx(ctx, tx, cluster.ID, seedstore.EventReviewResolutionStatusSuperseded, snapshot, "", now); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_review_clusters
		SET status = ?, superseded_by_cluster_id = ?, version = version + 1, updated_at = ?
		WHERE id = ?
			AND version = ?
			AND status = ?
	`, string(seedstore.EventReviewClusterStatusSuperseded), supersededByClusterID, formatRFC3339UTC(now), cluster.ID, cluster.Version, string(seedstore.EventReviewClusterStatusOpen))
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
	return nil
}

func marshalEventReviewResolutionSnapshot(cluster seedstore.EventReviewCluster, targetStatus seedstore.EventReviewResolutionStatus, discardReason string, supersededByClusterID *int64, repairRunID *int64, appliedAutoResolution *eventReviewResolutionAppliedAutoResolutionSnapshot, appliedImportListing *eventReviewResolutionAppliedImportListingSnapshot, appliedTitleRepair *eventReviewResolutionAppliedTitleRepairSnapshot, appliedLiveActions []eventReviewResolutionAppliedLiveActionSnapshot, now time.Time) (string, error) {
	snapshot := eventReviewResolutionSnapshot{
		ClusterID:             cluster.ID,
		ExpectedVersion:       cluster.Version,
		CurrentVersion:        cluster.Version,
		CurrentStatus:         cluster.Status,
		TargetStatus:          targetStatus,
		DiscardReason:         strings.TrimSpace(discardReason),
		CanonicalEventID:      cluster.CanonicalEventID,
		RepairRunID:           repairRunID,
		AppliedAutoResolution: appliedAutoResolution,
		AppliedImportListing:  appliedImportListing,
		AppliedTitleRepair:    appliedTitleRepair,
		AppliedLiveActions:    appliedLiveActions,
		RecordedAt:            formatRFC3339UTC(now),
	}
	if supersededByClusterID != nil {
		snapshot.SupersededByClusterID = *supersededByClusterID
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func insertEventReviewResolutionTx(ctx context.Context, tx execer, clusterID int64, status seedstore.EventReviewResolutionStatus, snapshot, discardReason string, now time.Time) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, string(status), snapshot, strings.TrimSpace(discardReason), formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
