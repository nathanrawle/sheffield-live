package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func TestEventReviewReadModelsListAndDetail(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-hall", "Event Review Hall", domain.OriginLive)

	insertLegacyEvent(t, db, "canonical-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "evidence-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "action-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "fallback-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "terminal-event", venueID, sourceID, domain.OriginLive)

	canonicalEventID := mustEventIDBySlug(t, db, "canonical-event")
	evidenceEventID := mustEventIDBySlug(t, db, "evidence-event")
	actionEventID := mustEventIDBySlug(t, db, "action-event")
	fallbackEventID := mustEventIDBySlug(t, db, "fallback-event")
	terminalEventID := mustEventIDBySlug(t, db, "terminal-event")

	if _, err := db.Exec(`
		INSERT INTO import_runs (started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?)
	`, "2026-05-15T09:00:00Z", "2026-05-15T09:05:00Z", "succeeded", "import run"); err != nil {
		t.Fatalf("insert import run: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_runs (started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?)
	`, "2026-05-15T09:10:00Z", "2026-05-15T09:15:00Z", "succeeded", "repair run"); err != nil {
		t.Fatalf("insert repair run: %v", err)
	}

	importRunID := mustSingleInt64(t, db, `SELECT id FROM import_runs ORDER BY id LIMIT 1`)
	repairRunID := mustSingleInt64(t, db, `SELECT id FROM repair_runs ORDER BY id LIMIT 1`)

	stagingKey := "repair-queue-a"
	olderClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "type-a", "reason-a", time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC))
	newerClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &stagingKey, 3, &canonicalEventID, "type-b", "reason-b", time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC))
	fallbackClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "type-fallback", "reason-fallback", time.Date(2026, time.May, 15, 10, 30, 0, 0, time.UTC))
	terminalClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusResolved), &stagingKey, 4, &terminalEventID, "historical_duplicate", "reason-c", time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC))
	terminalSnapshot, err := json.Marshal(map[string]any{
		"cluster_id":       terminalClusterID,
		"expected_version": 1,
		"current_version":  1,
		"current_status":   "open",
		"target_status":    "resolved",
		"repair_run_id":    repairRunID,
		"applied_import_listing": map[string]any{
			"event_id":    terminalEventID,
			"event_slug":  "terminal-event",
			"title":       "Terminal Event",
			"venue_slug":  "event-review-hall",
			"venue_name":  "Event Review Hall",
			"start_at":    "2026-05-15T12:00:00Z",
			"source_id":   sourceID,
			"source_name": "Store test source",
			"source_url":  "https://source.example.test",
			"evidence_id": 99,
		},
		"applied_live_actions": []map[string]any{{"event_id": terminalEventID, "event_slug": "terminal-event", "action": "keep_separate", "reason": "keep"}},
		"recorded_at":          "2026-05-15T12:05:00Z",
	})
	if err != nil {
		t.Fatalf("marshal terminal snapshot: %v", err)
	}
	insertEventReviewResolutionOK(t, db, terminalClusterID, seedstore.EventReviewResolutionStatusResolved, string(terminalSnapshot), "")

	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &evidenceEventID, "fingerprint-1", `{"payload":"evidence"}`)
	insertEventReviewClusterEvidenceOK(t, db, newerClusterID, evidenceID, true, time.Date(2026, time.May, 15, 11, 5, 0, 0, time.UTC), nil, "active evidence")
	fallbackEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &fallbackEventID, "fingerprint-2", `{"payload":"fallback"}`)
	insertEventReviewClusterEvidenceOK(t, db, fallbackClusterID, fallbackEvidenceID, true, time.Date(2026, time.May, 15, 10, 35, 0, 0, time.UTC), nil, "fallback evidence")
	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, importRunID, newerClusterID, "2026-05-15T11:05:00Z"); err != nil {
		t.Fatalf("link import run: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_run_event_review_clusters (repair_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, repairRunID, newerClusterID, "2026-05-15T11:06:00Z"); err != nil {
		t.Fatalf("link repair run: %v", err)
	}

	canonicalChoiceEventID := actionEventID
	insertEventReviewCanonicalChoiceOK(t, db, newerClusterID, "canonical_event_id", seedstore.EventReviewChoiceKindEvent, &canonicalChoiceEventID, nil, "canonical", time.Date(2026, time.May, 15, 11, 10, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, newerClusterID, "title", seedstore.EventReviewChoiceKindEvidence, nil, &evidenceID, "draft title", time.Date(2026, time.May, 15, 11, 11, 0, 0, time.UTC))
	insertEventReviewLiveActionOK(t, db, newerClusterID, actionEventID, seedstore.EventReviewLiveActionKindWithholdDuplicate, "withhold action")

	clusters, err := st.ListOpenEventReviewClusters(context.Background())
	if err != nil {
		t.Fatalf("list open event review clusters: %v", err)
	}
	if len(clusters) != 3 {
		t.Fatalf("open cluster count = %d, want 3", len(clusters))
	}
	if clusters[0].ID != newerClusterID {
		t.Fatalf("first cluster ID = %d, want %d", clusters[0].ID, newerClusterID)
	}
	if clusters[0].DisplayTitle != "Legacy Event" || clusters[0].DisplayVenueSlug != "event-review-hall" || clusters[0].DisplayVenueName != "Event Review Hall" {
		t.Fatalf("newer cluster display fields = %#v", clusters[0])
	}
	if clusters[0].DisplayStartAt == nil || !clusters[0].DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("newer cluster display start = %v", clusters[0].DisplayStartAt)
	}
	if clusters[0].EvidenceCount != 1 {
		t.Fatalf("newer cluster evidence count = %d, want 1", clusters[0].EvidenceCount)
	}
	if clusters[0].LatestImportRunID == nil || *clusters[0].LatestImportRunID != importRunID {
		t.Fatalf("newer cluster latest import run = %v, want %d", clusters[0].LatestImportRunID, importRunID)
	}
	if clusters[0].LatestRepairRunID == nil || *clusters[0].LatestRepairRunID != repairRunID {
		t.Fatalf("newer cluster latest repair run = %v, want %d", clusters[0].LatestRepairRunID, repairRunID)
	}
	if clusters[1].ID != fallbackClusterID {
		t.Fatalf("second cluster ID = %d, want %d", clusters[1].ID, fallbackClusterID)
	}
	if clusters[1].DisplayTitle != "Legacy Event" || clusters[1].DisplayVenueSlug != "event-review-hall" || clusters[1].DisplayVenueName != "Event Review Hall" {
		t.Fatalf("fallback cluster display fields = %#v", clusters[1])
	}
	if clusters[1].DisplayStartAt == nil || !clusters[1].DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("fallback cluster display start = %v", clusters[1].DisplayStartAt)
	}
	if clusters[1].EvidenceCount != 1 {
		t.Fatalf("fallback cluster evidence count = %d, want 1", clusters[1].EvidenceCount)
	}
	if clusters[1].LatestImportRunID != nil || clusters[1].LatestRepairRunID != nil {
		t.Fatalf("fallback cluster latest run IDs = %v / %v, want nil / nil", clusters[1].LatestImportRunID, clusters[1].LatestRepairRunID)
	}
	if clusters[2].ID != olderClusterID {
		t.Fatalf("third cluster ID = %d, want %d", clusters[2].ID, olderClusterID)
	}
	if clusters[2].EvidenceCount != 0 {
		t.Fatalf("older cluster evidence count = %d, want 0", clusters[2].EvidenceCount)
	}
	if clusters[2].DisplayTitle != "" || clusters[2].DisplayVenueSlug != "" || clusters[2].DisplayVenueName != "" || clusters[2].DisplayStartAt != nil {
		t.Fatalf("older cluster display fields = %#v, want empty", clusters[2])
	}
	if older, ok, err := st.LoadEventReviewCluster(context.Background(), olderClusterID); err != nil {
		t.Fatalf("load older cluster: %v", err)
	} else if !ok {
		t.Fatal("older cluster load returned ok=false")
	} else if older.Summary.DisplayTitle != "" || older.Summary.DisplayVenueSlug != "" || older.Summary.DisplayVenueName != "" || older.Summary.DisplayStartAt != nil {
		t.Fatalf("older cluster display fields = %#v, want empty", older.Summary)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), newerClusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load event review cluster ok = false, want true")
	}
	if detail.Summary.ID != newerClusterID {
		t.Fatalf("detail cluster ID = %d, want %d", detail.Summary.ID, newerClusterID)
	}
	if detail.Summary.CanonicalEventSlug != "canonical-event" {
		t.Fatalf("canonical event slug = %q, want %q", detail.Summary.CanonicalEventSlug, "canonical-event")
	}
	if detail.Summary.DisplayTitle != "Legacy Event" || detail.Summary.DisplayVenueSlug != "event-review-hall" || detail.Summary.DisplayVenueName != "Event Review Hall" {
		t.Fatalf("detail display fields = %#v", detail.Summary)
	}
	if detail.Summary.DisplayStartAt == nil || !detail.Summary.DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("detail display start = %v", detail.Summary.DisplayStartAt)
	}
	if len(detail.Evidence) != 1 {
		t.Fatalf("detail evidence count = %d, want 1", len(detail.Evidence))
	}
	if got := detail.Evidence[0]; got.SourceName != "Store test source" {
		t.Fatalf("evidence source name = %q, want %q", got.SourceName, "Store test source")
	}
	if got := detail.Evidence[0]; got.EventSlug != "evidence-event" {
		t.Fatalf("evidence event slug = %q, want %q", got.EventSlug, "evidence-event")
	}
	if len(detail.CanonicalChoices) != 1 || detail.CanonicalChoices[0].FieldName != "canonical_event_id" {
		t.Fatalf("canonical choices = %#v, want canonical_event_id", detail.CanonicalChoices)
	}
	if len(detail.DraftChoices) != 1 || detail.DraftChoices[0].FieldName != "title" {
		t.Fatalf("draft choices = %#v, want title", detail.DraftChoices)
	}
	if len(detail.LiveActions) != 1 || detail.LiveActions[0].EventSlug != "action-event" {
		t.Fatalf("live actions = %#v, want action-event", detail.LiveActions)
	}
	if len(detail.ClusterIdentityKeys) != 0 {
		t.Fatalf("detail cluster identity keys = %#v, want none", detail.ClusterIdentityKeys)
	}
	if len(detail.EvidenceIdentityKeys) != 0 {
		t.Fatalf("detail evidence identity keys = %#v, want none", detail.EvidenceIdentityKeys)
	}
	if detail.TitleRepairReadiness != nil {
		t.Fatalf("detail title repair readiness = %#v, want nil", detail.TitleRepairReadiness)
	}

	terminalDetail, ok, err := st.LoadEventReviewCluster(context.Background(), terminalClusterID)
	if err != nil {
		t.Fatalf("load terminal event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load terminal cluster ok = false, want true")
	}
	if terminalDetail.Summary.Status != seedstore.EventReviewClusterStatusResolved {
		t.Fatalf("terminal cluster status = %q, want resolved", terminalDetail.Summary.Status)
	}
	if terminalDetail.Resolution == nil {
		t.Fatal("terminal cluster resolution is nil")
	}
	if terminalDetail.Resolution.Status != seedstore.EventReviewResolutionStatusResolved {
		t.Fatalf("terminal resolution status = %q, want resolved", terminalDetail.Resolution.Status)
	}
	if terminalDetail.Resolution.RepairRunID == nil || *terminalDetail.Resolution.RepairRunID != repairRunID {
		t.Fatalf("terminal resolution repair run id = %v, want %d", terminalDetail.Resolution.RepairRunID, repairRunID)
	}
	if terminalDetail.Resolution.AppliedImportListing == nil || terminalDetail.Resolution.AppliedImportListing.EventID != terminalEventID || terminalDetail.Resolution.AppliedImportListing.EventSlug != "terminal-event" || terminalDetail.Resolution.AppliedImportListing.Title != "Terminal Event" || terminalDetail.Resolution.AppliedImportListing.VenueSlug != "event-review-hall" || terminalDetail.Resolution.AppliedImportListing.VenueName != "Event Review Hall" || terminalDetail.Resolution.AppliedImportListing.SourceID != sourceID || terminalDetail.Resolution.AppliedImportListing.EvidenceID != 99 {
		t.Fatalf("terminal applied import listing = %#v", terminalDetail.Resolution.AppliedImportListing)
	}
	if terminalDetail.Resolution.AppliedImportListing.StartAt.IsZero() || !terminalDetail.Resolution.AppliedImportListing.StartAt.Equal(time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("terminal applied import listing start = %v", terminalDetail.Resolution.AppliedImportListing.StartAt)
	}
	if len(terminalDetail.Resolution.AppliedLiveActions) != 1 || terminalDetail.Resolution.AppliedLiveActions[0].Action != seedstore.EventReviewLiveActionKindKeepSeparate {
		t.Fatalf("terminal applied live actions = %#v", terminalDetail.Resolution.AppliedLiveActions)
	}
	if len(terminalDetail.Evidence) != 0 {
		t.Fatalf("terminal cluster evidence count = %d, want 0", len(terminalDetail.Evidence))
	}
}

func TestEventReviewReadModelsTitleRepairReadiness(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-title-repair-hall", "Event Review Title Repair Hall", domain.OriginLive)

	insertLegacyEvent(t, db, "title-repair-eligible", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "title-repair-missing-slug", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "title-repair-withheld", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "title-repair-conflict", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "title-repair-slug-conflict", venueID, sourceID, domain.OriginLive)

	withheldEventID := mustEventIDBySlug(t, db, "title-repair-withheld")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?, withheld_reason = ?
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), "duplicate listing", withheldEventID); err != nil {
		t.Fatalf("withheld canonical event: %v", err)
	}

	eligibleCanonicalID := mustEventIDBySlug(t, db, "title-repair-eligible")
	missingSlugCanonicalID := mustEventIDBySlug(t, db, "title-repair-missing-slug")
	conflictCanonicalID := mustEventIDBySlug(t, db, "title-repair-conflict")
	slugConflictEventID := mustEventIDBySlug(t, db, "title-repair-slug-conflict")

	eligibleStagingKey := "title-repair-eligible"
	eligibleClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &eligibleStagingKey, eventTitleRepairStagingKeyVersion, &eligibleCanonicalID, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, eligibleClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Proposed Title", time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, eligibleClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "proposed-title", time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC))
	eligibleDetail, ok, err := st.LoadEventReviewCluster(context.Background(), eligibleClusterID)
	if err != nil {
		t.Fatalf("load eligible title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("eligible title repair cluster load returned ok=false")
	}
	if eligibleDetail.TitleRepairReadiness == nil {
		t.Fatal("eligible readiness is nil")
	}
	if got := eligibleDetail.TitleRepairReadiness; got.CanonicalEventID != eligibleCanonicalID || got.CurrentTitle != "Legacy Event" || got.CurrentSlug != "title-repair-eligible" || !got.CurrentEventLive || got.DraftTitle != "Proposed Title" || got.DraftSlug != "proposed-title" || !got.Eligible || len(got.BlockingReasons) != 0 {
		t.Fatalf("eligible readiness = %#v", got)
	}

	missingSlugStagingKey := "title-repair-missing-slug"
	missingSlugClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &missingSlugStagingKey, eventTitleRepairStagingKeyVersion, &missingSlugCanonicalID, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, time.Date(2026, time.May, 15, 10, 3, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, missingSlugClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Missing Slug Title", time.Date(2026, time.May, 15, 10, 4, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, missingSlugClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "", time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC))
	missingSlugDetail, ok, err := st.LoadEventReviewCluster(context.Background(), missingSlugClusterID)
	if err != nil {
		t.Fatalf("load missing slug title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("missing slug title repair cluster load returned ok=false")
	}
	if missingSlugDetail.TitleRepairReadiness == nil {
		t.Fatal("missing slug readiness is nil")
	}
	if got := missingSlugDetail.TitleRepairReadiness; got.Eligible || len(got.BlockingReasons) == 0 || got.DraftSlug != "" {
		t.Fatalf("missing slug readiness = %#v", got)
	}

	withheldStagingKey := "title-repair-withheld"
	withheldClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &withheldStagingKey, eventTitleRepairStagingKeyVersion, &withheldEventID, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, time.Date(2026, time.May, 15, 10, 6, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, withheldClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Withheld Title", time.Date(2026, time.May, 15, 10, 7, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, withheldClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "withheld-title", time.Date(2026, time.May, 15, 10, 8, 0, 0, time.UTC))
	withheldDetail, ok, err := st.LoadEventReviewCluster(context.Background(), withheldClusterID)
	if err != nil {
		t.Fatalf("load withheld title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("withheld title repair cluster load returned ok=false")
	}
	if withheldDetail.TitleRepairReadiness == nil {
		t.Fatal("withheld readiness is nil")
	}
	if got := withheldDetail.TitleRepairReadiness; got.CurrentEventLive || got.Eligible || len(got.BlockingReasons) == 0 {
		t.Fatalf("withheld readiness = %#v", got)
	}

	conflictStagingKey := "title-repair-conflict"
	conflictClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &conflictStagingKey, eventTitleRepairStagingKeyVersion, &conflictCanonicalID, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, time.Date(2026, time.May, 15, 10, 9, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, conflictClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Conflict Title", time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, conflictClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "title-repair-slug-conflict", time.Date(2026, time.May, 15, 10, 11, 0, 0, time.UTC))
	conflictDetail, ok, err := st.LoadEventReviewCluster(context.Background(), conflictClusterID)
	if err != nil {
		t.Fatalf("load slug conflict title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("slug conflict title repair cluster load returned ok=false")
	}
	if conflictDetail.TitleRepairReadiness == nil {
		t.Fatal("slug conflict readiness is nil")
	}
	if got := conflictDetail.TitleRepairReadiness; got.Eligible || !got.SlugConflictResolutionAvailable || got.SlugConflictEventID == nil || *got.SlugConflictEventID != slugConflictEventID || got.SlugConflictEventSlug != "title-repair-slug-conflict" {
		t.Fatalf("slug conflict readiness = %#v", got)
	}
	if _, err := insertEventReviewSeparation(t, db,
		seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(conflictCanonicalID),
			EventID: int64Ptr(conflictCanonicalID),
		},
		seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(slugConflictEventID),
			EventID: int64Ptr(slugConflictEventID),
		},
		true,
		"title repair separated slug conflict",
		time.Date(2026, time.May, 15, 10, 11, 30, 0, time.UTC),
		time.Date(2026, time.May, 15, 10, 11, 30, 0, time.UTC)); err != nil {
		t.Fatalf("insert title repair separation: %v", err)
	}
	separatedConflictDetail, ok, err := st.LoadEventReviewCluster(context.Background(), conflictClusterID)
	if err != nil {
		t.Fatalf("load separated slug conflict title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("separated slug conflict title repair cluster load returned ok=false")
	}
	if got := separatedConflictDetail.TitleRepairReadiness; got == nil || got.SlugConflictResolutionAvailable || !hasString(got.BlockingReasons, "target slug conflict is already marked separate") {
		t.Fatalf("separated slug conflict readiness = %#v", got)
	}

	authoritativeStagingKey := "title-repair-authoritative"
	authoritativeClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &authoritativeStagingKey, eventTitleRepairStagingKeyVersion, &eligibleCanonicalID, eventTitleRepairConflictType, eventTitleRepairConflictReasonAuthoritativeSlugConflict, time.Date(2026, time.May, 15, 10, 12, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, authoritativeClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Authoritative Title", time.Date(2026, time.May, 15, 10, 13, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, authoritativeClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "title-repair-slug-conflict", time.Date(2026, time.May, 15, 10, 14, 0, 0, time.UTC))
	authoritativeDetail, ok, err := st.LoadEventReviewCluster(context.Background(), authoritativeClusterID)
	if err != nil {
		t.Fatalf("load authoritative title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("authoritative title repair cluster load returned ok=false")
	}
	if authoritativeDetail.TitleRepairReadiness == nil {
		t.Fatal("authoritative readiness is nil")
	}
	if got := authoritativeDetail.TitleRepairReadiness; got.Eligible || !got.SlugConflictResolutionAvailable || got.SlugConflictEventID == nil || *got.SlugConflictEventID != slugConflictEventID {
		t.Fatalf("authoritative readiness = %#v", got)
	}

	nilCanonicalStagingKey := "title-repair-missing-canonical"
	nilCanonicalClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), &nilCanonicalStagingKey, eventTitleRepairStagingKeyVersion, nil, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, nilCanonicalClusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, "Missing Canonical Title", time.Date(2026, time.May, 15, 10, 16, 0, 0, time.UTC))
	insertEventReviewDraftChoiceOK(t, db, nilCanonicalClusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, "missing-canonical-title", time.Date(2026, time.May, 15, 10, 17, 0, 0, time.UTC))
	nilCanonicalDetail, ok, err := st.LoadEventReviewCluster(context.Background(), nilCanonicalClusterID)
	if err != nil {
		t.Fatalf("load missing canonical title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("missing canonical title repair cluster load returned ok=false")
	}
	if nilCanonicalDetail.TitleRepairReadiness == nil {
		t.Fatal("missing canonical readiness is nil")
	}
	if got := nilCanonicalDetail.TitleRepairReadiness; got.CanonicalEventID != 0 || got.Eligible || len(got.BlockingReasons) == 0 {
		t.Fatalf("missing canonical readiness = %#v", got)
	}

	nonTitleRepairClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, &eligibleCanonicalID, "historical_duplicate", "reason", time.Date(2026, time.May, 15, 10, 18, 0, 0, time.UTC))
	nonTitleRepairDetail, ok, err := st.LoadEventReviewCluster(context.Background(), nonTitleRepairClusterID)
	if err != nil {
		t.Fatalf("load non-title-repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("non-title-repair cluster load returned ok=false")
	}
	if nonTitleRepairDetail.TitleRepairReadiness != nil {
		t.Fatalf("non-title-repair readiness = %#v, want nil", nonTitleRepairDetail.TitleRepairReadiness)
	}
}

func TestEventReviewReadModelsHistoricalDuplicateReadiness(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	fixture := seedHistoricalDuplicateResolutionFixture(t, db)
	sourceID := mustEnsureSourceID(t, st, "Historical duplicate readiness alias source", "https://example.test/historical-duplicate-readiness")
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "historical-duplicate-alias-owner", venueID, sourceID, domain.OriginLive)
	aliasOwnerID := mustEventIDBySlug(t, db, "historical-duplicate-alias-owner")
	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?)
	`, "historical-duplicate-loser", string(seedstore.SlugAliasTargetKindEvent), aliasOwnerID, "readiness test", "2026-05-15T09:00:00Z", "2026-05-15T09:00:00Z"); err != nil {
		t.Fatalf("insert slug alias conflict: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load historical duplicate detail: %v", err)
	}
	if !ok {
		t.Fatal("historical duplicate cluster load returned ok=false")
	}
	readiness := detail.HistoricalDuplicateReadiness
	if readiness == nil {
		t.Fatal("historical duplicate readiness = nil")
	}
	if readiness.CanResolveLiveActions {
		t.Fatalf("can resolve live actions = true, want blocked; blockers=%#v", readiness.LiveActionBlockingReasons)
	}
	if !hasString(readiness.LiveActionBlockingReasons, "slug alias already points to a different event") {
		t.Fatalf("live action blockers = %#v, want slug alias blocker", readiness.LiveActionBlockingReasons)
	}
	if !readiness.CanKeepAllSeparate || len(readiness.KeepSeparateBlockingReasons) != 0 {
		t.Fatalf("keep-separate readiness = can=%v blockers=%#v, want eligible", readiness.CanKeepAllSeparate, readiness.KeepSeparateBlockingReasons)
	}
	if len(readiness.Events) != 2 {
		t.Fatalf("readiness events = %#v, want 2", readiness.Events)
	}
	for _, event := range readiness.Events {
		if !event.KeepEligible {
			t.Fatalf("event readiness = %#v, want keep eligible", event)
		}
	}
}

func TestEventReviewReadModelsImportReadiness(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-import-readiness-hall", "Event Review Import Readiness Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "import-readiness-existing", venueID, sourceID, domain.OriginLive)

	existingEventID := mustEventIDBySlug(t, db, "import-readiness-existing")

	importPayload := func(title, venueSlug, venueText, startAt, endAt, externalID string) string {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"source_authority":      "supporting",
			"candidate_external_id": externalID,
			"candidate_title":       title,
			"candidate_venue_slug":  venueSlug,
			"candidate_venue_text":  venueText,
			"candidate_start_at":    startAt,
			"candidate_end_at":      endAt,
			"calendar_url":          "https://calendar.example.test/listing.ics",
		})
		if err != nil {
			t.Fatalf("marshal import payload: %v", err)
		}
		return string(payload)
	}

	stageImportCluster := func(t *testing.T, status string, canonicalEventID *int64, conflictReason string, payloads []string, eventIDs []*int64) int64 {
		t.Helper()
		clusterID := insertEventReviewClusterAt(t, db, status, nil, 0, canonicalEventID, seedstore.EventReviewConflictTypeImportReview, conflictReason, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC))
		for i, payload := range payloads {
			var eventID *int64
			if i < len(eventIDs) {
				eventID = eventIDs[i]
			}
			evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, eventID, "import-readiness-"+strconv.FormatInt(clusterID, 10)+"-"+strconv.Itoa(i), payload)
			insertEventReviewClusterEvidenceOK(t, db, clusterID, evidenceID, true, time.Date(2026, time.May, 15, 10, i, 0, 0, time.UTC), nil, "import readiness evidence")
		}
		return clusterID
	}

	t.Run("non-import nil", func(t *testing.T) {
		clusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "historical_duplicate", "reason", time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC))
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load non-import cluster: %v", err)
		}
		if !ok {
			t.Fatal("non-import cluster load returned ok=false")
		}
		if detail.ImportReadiness != nil {
			t.Fatalf("import readiness = %#v, want nil", detail.ImportReadiness)
		}
	})

	t.Run("import type with other reason nil", func(t *testing.T) {
		clusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, seedstore.EventReviewConflictTypeImportReview, "other_reason", time.Date(2026, time.May, 15, 9, 5, 0, 0, time.UTC))
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load import other-reason cluster: %v", err)
		}
		if !ok {
			t.Fatal("import other-reason cluster load returned ok=false")
		}
		if detail.ImportReadiness != nil {
			t.Fatalf("import other-reason readiness = %#v, want nil", detail.ImportReadiness)
		}
	})

	t.Run("eligible", func(t *testing.T) {
		payload := importPayload(
			"Import Ready Title",
			"event-review-import-ready",
			"Event Review Import Ready",
			"2026-05-10T19:00:00Z",
			"2026-05-10T21:00:00Z",
			"external-ready",
		)
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payload}, nil)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load eligible import cluster: %v", err)
		}
		if !ok {
			t.Fatal("eligible import cluster load returned ok=false")
		}
		if detail.ImportReadiness == nil {
			t.Fatal("eligible import readiness is nil")
		}
		got := detail.ImportReadiness
		if got.CandidateCount != 1 || !got.NewListingScope || len(got.BlockingReasons) != 0 || len(got.PayloadWarnings) != 0 {
			t.Fatalf("eligible import readiness = %#v", got)
		}
		if len(got.Candidates) != 1 {
			t.Fatalf("eligible candidates = %#v", got.Candidates)
		}
		candidate := got.Candidates[0]
		if candidate.SourceAuthority != seedstore.SourceAuthoritySupporting || candidate.ExternalID != "external-ready" || candidate.Title != "Import Ready Title" || candidate.VenueSlug != "event-review-import-ready" || candidate.VenueText != "Event Review Import Ready" || candidate.CalendarURL != "https://calendar.example.test/listing.ics" {
			t.Fatalf("eligible candidate = %#v", candidate)
		}
		if candidate.StartAt == nil || candidate.EndAt == nil || !candidate.StartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) || !candidate.EndAt.Equal(time.Date(2026, time.May, 10, 21, 0, 0, 0, time.UTC)) {
			t.Fatalf("eligible candidate times = %#v", candidate)
		}
	})

	t.Run("multiple evidence blocks", func(t *testing.T) {
		payloadA := importPayload("First Title", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "2026-05-10T21:00:00Z", "external-a")
		payloadB := importPayload("Second Title", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:30:00Z", "2026-05-10T21:30:00Z", "external-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load multi evidence import cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("multi evidence import readiness missing")
		}
		if detail.ImportReadiness.CandidateCount != 2 || detail.ImportReadiness.NewListingScope {
			t.Fatalf("multi evidence readiness = %#v", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.BlockingReasons, "multiple active evidence rows are present") {
			t.Fatalf("multi evidence blockers = %#v", detail.ImportReadiness.BlockingReasons)
		}
	})

	t.Run("comparison scope", func(t *testing.T) {
		payloadA := importPayload("Same Show - Leadmill", "", "Leadmill", "2026-05-10T19:00:00Z", "2026-05-10T21:00:00Z", "external-a")
		payloadB := importPayload("Same Show", "leadmill", "Leadmill", "2026-05-10T19:00:00Z", "2026-05-10T21:00:00Z", "external-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load comparison import cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("comparison import readiness missing")
		}
		if detail.ImportReadiness.NewListingScope {
			t.Fatalf("comparison readiness = %#v, want blocked for singleton resolver", detail.ImportReadiness)
		}
		if !detail.ImportReadiness.CandidateComparisonScope {
			t.Fatalf("comparison readiness = %#v, want comparison scope", detail.ImportReadiness)
		}
		if len(detail.ImportReadiness.ComparisonBlockingReasons) != 0 {
			t.Fatalf("comparison blockers = %#v, want none", detail.ImportReadiness.ComparisonBlockingReasons)
		}
		if len(detail.ImportReadiness.IdentityRows) != 5 || len(detail.ImportReadiness.RawRows) != 6 {
			t.Fatalf("comparison rows = %#v / %#v", detail.ImportReadiness.IdentityRows, detail.ImportReadiness.RawRows)
		}

		titleRow := mustImportReadinessIdentityRow(t, detail.ImportReadiness.IdentityRows, "clean_title")
		if !titleRow.Consensus || len(titleRow.Values) != 2 || titleRow.Values[0].Normalized != "same show" || titleRow.Values[1].Normalized != "same show" {
			t.Fatalf("comparison clean title row = %#v", titleRow)
		}
		venueRow := mustImportReadinessIdentityRow(t, detail.ImportReadiness.IdentityRows, "venue_slug")
		if !venueRow.Consensus || len(venueRow.Values) != 2 || venueRow.Values[0].Normalized != "leadmill" || venueRow.Values[1].Normalized != "leadmill" {
			t.Fatalf("comparison venue row = %#v", venueRow)
		}
		if venueRow.Values[0].Warning != "venue normalized from raw text" && venueRow.Values[1].Warning != "venue normalized from raw text" {
			t.Fatalf("comparison venue warnings = %#v", venueRow)
		}
		exactRow := mustImportReadinessIdentityRow(t, detail.ImportReadiness.IdentityRows, "exact_identity")
		if !exactRow.Consensus || exactRow.Values[0].Normalized == "" || exactRow.Values[0].Normalized != exactRow.Values[1].Normalized {
			t.Fatalf("comparison exact identity row = %#v", exactRow)
		}
		externalRow := mustImportReadinessComparisonRow(t, detail.ImportReadiness.RawRows, "external_id")
		if externalRow.Consensus || externalRow.Values[0].Value == externalRow.Values[1].Value {
			t.Fatalf("comparison external ids = %#v", externalRow)
		}
	})

	t.Run("selected candidate readiness requires selected choices", func(t *testing.T) {
		payloadA := importPayload("Selected Choice A", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "selected-a")
		payloadB := importPayload("Selected Choice B", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T20:00:00Z", "", "selected-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load unselected candidate readiness cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("unselected candidate readiness missing")
		}
		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if selectedReadiness.Eligible {
			t.Fatalf("selected candidate readiness = %#v, want blocked", selectedReadiness)
		}
		if !hasString(selectedReadiness.BlockingReasons, "no selected source identity choices") {
			t.Fatalf("selected candidate readiness blockers = %#v", selectedReadiness.BlockingReasons)
		}
	})

	t.Run("selected candidate readiness spans multiple candidates", func(t *testing.T) {
		payloadA := importPayload("Selected Choice A", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "selected-a")
		payloadB := importPayload("Selected Choice B", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T20:00:00Z", "", "selected-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)

		var evidenceIDs []int64
		rows, err := db.Query(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
		`, clusterID)
		if err != nil {
			t.Fatalf("query selected candidate evidence ids: %v", err)
		}
		for rows.Next() {
			var evidenceID int64
			if err := rows.Scan(&evidenceID); err != nil {
				rows.Close()
				t.Fatalf("scan selected candidate evidence id: %v", err)
			}
			evidenceIDs = append(evidenceIDs, evidenceID)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close selected candidate evidence rows: %v", err)
		}
		if len(evidenceIDs) != 2 {
			t.Fatalf("selected candidate evidence ids = %#v, want 2", evidenceIDs)
		}

		selectedAKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-a-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-a")
		selectedBKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-b-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-b")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceIDs[0], selectedAKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert selected candidate evidence a identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceIDs[1], selectedBKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert selected candidate evidence b identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "selected-a", true, "selected a", time.Date(2026, time.May, 15, 9, 40, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert selected candidate choice a: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "selected-b", true, "selected b", time.Date(2026, time.May, 15, 9, 41, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert selected candidate choice b: %v", err)
		}

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load multi-selected candidate cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("multi-selected candidate readiness missing")
		}
		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if selectedReadiness.Eligible {
			t.Fatalf("selected candidate readiness = %#v, want blocked", selectedReadiness)
		}
		if !hasString(selectedReadiness.BlockingReasons, "selected source identity choices span multiple candidates") {
			t.Fatalf("selected candidate readiness blockers = %#v", selectedReadiness.BlockingReasons)
		}
		if len(selectedReadiness.SelectedSourceKeys) != 2 {
			t.Fatalf("selected candidate selected source keys = %#v, want 2", selectedReadiness.SelectedSourceKeys)
		}
	})

	t.Run("selected candidate readiness eligible", func(t *testing.T) {
		payload := importPayload("Eligible Selected Candidate", "event-review-selected-ready", "Event Review Selected Ready", "2026-05-10T19:00:00Z", "", "selected-ready")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payload}, nil)

		var evidenceID int64
		if err := db.QueryRow(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
			LIMIT 1
		`, clusterID).Scan(&evidenceID); err != nil {
			t.Fatalf("lookup eligible selected candidate evidence id: %v", err)
		}

		selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-ready-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-ready")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, selectedKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert eligible selected candidate evidence identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "selected-ready", true, "selected ready", time.Date(2026, time.May, 15, 9, 50, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert eligible selected candidate choice: %v", err)
		}
		unselectedLinkedEventID := mustInsertExactIdentityEvent(t, db, "selected-ready-unselected-link", "Linked Unselected Ready", venueID, sourceID, time.Date(2026, time.May, 10, 21, 0, 0, 0, time.UTC), time.Date(2026, time.May, 10, 23, 0, 0, 0, time.UTC), time.Date(2026, time.May, 15, 9, 25, 0, 0, time.UTC), domain.OriginLive)
		if _, err := db.Exec(`
			INSERT INTO event_source_links (
				event_id,
				source_id,
				source_event_key,
				is_authoritative,
				created_at,
				updated_at
			) VALUES (?, ?, ?, 0, ?, ?)
		`, unselectedLinkedEventID, sourceID, "unselected-ready", "2026-05-15T09:25:00Z", "2026-05-15T09:25:00Z"); err != nil {
			t.Fatalf("insert unselected linked source identity: %v", err)
		}
		unselectedKeyID := insertEventReviewIdentityKeyOK(t, db, "unselected-ready-hash", seedstore.EventReviewIdentityKeyKindSource, "unselected-ready")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, unselectedKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert eligible unselected candidate evidence identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "unselected-ready", false, "not selected", time.Date(2026, time.May, 15, 9, 51, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert eligible unselected candidate choice: %v", err)
		}

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load eligible selected candidate cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("eligible selected candidate readiness missing")
		}
		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if !selectedReadiness.Eligible || len(selectedReadiness.BlockingReasons) != 0 {
			t.Fatalf("selected candidate readiness = %#v, want eligible", selectedReadiness)
		}
		if selectedReadiness.EvidenceID != evidenceID || selectedReadiness.EvidenceFingerprint == "" || selectedReadiness.Title != "Eligible Selected Candidate" || selectedReadiness.VenueSlug != "event-review-selected-ready" || selectedReadiness.VenueText != "Event Review Selected Ready" {
			t.Fatalf("eligible selected candidate readiness = %#v", selectedReadiness)
		}
		if selectedReadiness.StartAt == nil || !selectedReadiness.StartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
			t.Fatalf("eligible selected candidate start = %#v", selectedReadiness.StartAt)
		}
		if len(selectedReadiness.SelectedSourceKeys) != 1 || len(selectedReadiness.ExactKeys) != 0 || len(selectedReadiness.SourceKeys) != 2 {
			t.Fatalf("eligible selected candidate keys = %#v / %#v / %#v", selectedReadiness.SelectedSourceKeys, selectedReadiness.ExactKeys, selectedReadiness.SourceKeys)
		}
		var sawUnselectedLinked bool
		for _, sourceKey := range selectedReadiness.SourceKeys {
			if sourceKey.SourceIdentityKey != "unselected-ready" {
				continue
			}
			sawUnselectedLinked = true
			if sourceKey.ChoiceSelected {
				t.Fatalf("unselected source key unexpectedly marked selected: %#v", sourceKey)
			}
			if sourceKey.LinkedEventID == nil || *sourceKey.LinkedEventID != unselectedLinkedEventID {
				t.Fatalf("unselected source key linked event = %#v", sourceKey)
			}
		}
		if !sawUnselectedLinked {
			t.Fatalf("eligible selected candidate source keys = %#v, want unselected-ready present", selectedReadiness.SourceKeys)
		}
	})

	t.Run("selected candidate readiness stays eligible with multiple active evidence rows", func(t *testing.T) {
		payloadA := importPayload("Primary Selected Candidate", "event-review-multi-selected", "Event Review Multi Selected", "2026-05-10T19:00:00Z", "", "multi-selected-a")
		payloadB := importPayload("Secondary Candidate", "event-review-multi-selected", "Event Review Multi Selected", "2026-05-10T20:00:00Z", "", "multi-selected-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)

		var evidenceIDs []int64
		rows, err := db.Query(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
		`, clusterID)
		if err != nil {
			t.Fatalf("query multi-evidence selected candidate evidence ids: %v", err)
		}
		for rows.Next() {
			var evidenceID int64
			if err := rows.Scan(&evidenceID); err != nil {
				rows.Close()
				t.Fatalf("scan multi-evidence selected candidate evidence id: %v", err)
			}
			evidenceIDs = append(evidenceIDs, evidenceID)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close multi-evidence selected candidate rows: %v", err)
		}
		if len(evidenceIDs) != 2 {
			t.Fatalf("multi-evidence selected candidate ids = %#v, want 2", evidenceIDs)
		}

		selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "multi-selected-a-hash", seedstore.EventReviewIdentityKeyKindSource, "multi-selected-a")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceIDs[0], selectedKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert multi-evidence selected candidate identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "multi-selected-a", true, "selected multi-evidence candidate", time.Date(2026, time.May, 15, 9, 45, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert multi-evidence selected candidate choice: %v", err)
		}

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load multi-evidence selected candidate cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("multi-evidence selected candidate readiness missing")
		}
		if detail.ImportReadiness.NewListingScope {
			t.Fatalf("multi-evidence selected candidate readiness = %#v, want singleton scope blocked", detail.ImportReadiness)
		}
		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if !selectedReadiness.Eligible || len(selectedReadiness.BlockingReasons) != 0 {
			t.Fatalf("multi-evidence selected candidate readiness = %#v, want eligible", selectedReadiness)
		}
		if selectedReadiness.EvidenceID != evidenceIDs[0] || selectedReadiness.Title != "Primary Selected Candidate" || selectedReadiness.VenueSlug != "event-review-multi-selected" || selectedReadiness.VenueText != "Event Review Multi Selected" {
			t.Fatalf("multi-evidence selected candidate readiness = %#v", selectedReadiness)
		}
		if selectedReadiness.StartAt == nil || !selectedReadiness.StartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
			t.Fatalf("multi-evidence selected candidate start = %#v", selectedReadiness.StartAt)
		}
		if len(selectedReadiness.SelectedSourceKeys) != 1 || len(selectedReadiness.SourceKeys) != 1 {
			t.Fatalf("multi-evidence selected candidate keys = %#v / %#v", selectedReadiness.SelectedSourceKeys, selectedReadiness.SourceKeys)
		}
	})

	t.Run("selected candidate readiness blocks malformed payloads", func(t *testing.T) {
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{"{bad payload"}, nil)
		var evidenceID int64
		if err := db.QueryRow(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
			LIMIT 1
		`, clusterID).Scan(&evidenceID); err != nil {
			t.Fatalf("lookup malformed selected candidate evidence id: %v", err)
		}

		selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "malformed-selected-hash", seedstore.EventReviewIdentityKeyKindSource, "malformed-selected")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, selectedKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert malformed selected candidate identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "malformed-selected", true, "selected malformed candidate", time.Date(2026, time.May, 15, 9, 55, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert malformed selected candidate choice: %v", err)
		}

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load malformed selected candidate cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("malformed selected candidate readiness missing")
		}
		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if selectedReadiness.Eligible {
			t.Fatalf("malformed selected candidate readiness = %#v, want blocked", selectedReadiness)
		}
		if !hasString(selectedReadiness.BlockingReasons, "selected candidate payload could not be parsed") {
			t.Fatalf("malformed selected candidate blockers = %#v", selectedReadiness.BlockingReasons)
		}
		if selectedReadiness.EvidenceID != evidenceID || selectedReadiness.EvidenceFingerprint == "" || len(selectedReadiness.SelectedSourceKeys) != 1 || len(selectedReadiness.SourceKeys) != 1 {
			t.Fatalf("malformed selected candidate readiness = %#v", selectedReadiness)
		}
		if selectedReadiness.SelectedSourceKeys[0].SourceIdentityKey != "malformed-selected" || !selectedReadiness.SelectedSourceKeys[0].ChoiceSelected {
			t.Fatalf("malformed selected candidate selected key = %#v", selectedReadiness.SelectedSourceKeys[0])
		}
		if selectedReadiness.Title != "" || selectedReadiness.VenueSlug != "" || selectedReadiness.VenueText != "" || selectedReadiness.StartAt != nil {
			t.Fatalf("malformed selected candidate parsed fields = %#v", selectedReadiness)
		}
	})

	t.Run("candidate identity statuses", func(t *testing.T) {
		identityVenueSlug := "event-review-candidate-status-hall"
		identityVenueText := "Event Review Candidate Status Hall"
		identityStartA := time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)
		identityEndA := identityStartA.Add(2 * time.Hour)
		identityStartB := time.Date(2026, time.May, 10, 20, 0, 0, 0, time.UTC)
		identityEndB := identityStartB.Add(2 * time.Hour)
		startCheckedA := time.Date(2026, time.May, 15, 9, 10, 0, 0, time.UTC)
		startCheckedB := time.Date(2026, time.May, 15, 9, 20, 0, 0, time.UTC)

		sourceLinkedID := sourceID
		otherSourceRes, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Store test source 2", "https://example.test/store-test-2")
		if err != nil {
			t.Fatalf("insert second source: %v", err)
		}
		otherSourceID, err := otherSourceRes.LastInsertId()
		if err != nil {
			t.Fatalf("second source id: %v", err)
		}
		venueID := insertLegacyVenue(t, db, identityVenueSlug, identityVenueText, domain.OriginLive)

		exactPayload := importPayload("Alpha Candidate", identityVenueSlug, identityVenueText, identityStartA.Format(time.RFC3339), identityEndA.Format(time.RFC3339), "external-alpha")
		unmatchedPayload := importPayload("Gamma Candidate", identityVenueSlug, identityVenueText, identityStartB.Format(time.RFC3339), identityEndB.Format(time.RFC3339), "external-gamma")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{exactPayload, unmatchedPayload}, nil)

		var evidenceIDs []int64
		rows, err := db.Query(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
		`, clusterID)
		if err != nil {
			t.Fatalf("query candidate evidence ids: %v", err)
		}
		for rows.Next() {
			var evidenceID int64
			if err := rows.Scan(&evidenceID); err != nil {
				rows.Close()
				t.Fatalf("scan candidate evidence id: %v", err)
			}
			evidenceIDs = append(evidenceIDs, evidenceID)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close candidate evidence rows: %v", err)
		}
		if len(evidenceIDs) != 2 {
			t.Fatalf("candidate evidence ids = %#v, want 2", evidenceIDs)
		}
		exactEvidenceID := evidenceIDs[0]
		unmatchedEvidenceID := evidenceIDs[1]

		exactLinkedEventID := mustInsertExactIdentityEvent(t, db, "candidate-exact-linked-event", "Alpha Candidate", venueID, sourceID, identityStartA, identityEndA, startCheckedA, domain.OriginLive)
		sourceLinkedEventID := mustInsertExactIdentityEvent(t, db, "candidate-source-linked-event", "Source Linked Event", venueID, sourceLinkedID, identityStartB, identityEndB, startCheckedB, domain.OriginLive)
		tx, err := st.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin exact linked identity tx: %v", err)
		}
		if err := ensureActiveExactIdentityTx(context.Background(), tx, exactLinkedEventID, domain.Event{
			Slug:             "candidate-exact-linked-event",
			Name:             "Alpha Candidate",
			VenueSlug:        identityVenueSlug,
			Start:            identityStartA,
			Origin:           domain.OriginLive,
			LastChecked:      startCheckedA,
			PublicationState: domain.PublicationStateReviewed,
		}, 0, startCheckedA); err != nil {
			t.Fatalf("ensure exact linked identity: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit exact linked identity tx: %v", err)
		}

		exactLinkedTitle := normalizeExactIdentityCleanTitle("Alpha Candidate")
		exactLinkedKey := buildExactIdentityKey(exactIdentityKeyVersion, identityVenueSlug, identityStartA, exactLinkedTitle)
		exactLinkedHash := "candidate-exact-linked-hash"
		exactLinkedKeyID := insertEventReviewIdentityKeyOK(t, db, exactLinkedHash, seedstore.EventReviewIdentityKeyKindExact, exactLinkedKey)
		sourceLinkedKeyID := insertEventReviewIdentityKeyOK(t, db, "candidate-source-linked-hash", seedstore.EventReviewIdentityKeyKindSource, "candidate-source-key")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, exactEvidenceID, exactLinkedKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
			t.Fatalf("insert exact linked evidence identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, exactEvidenceID, exactLinkedKeyID, &otherSourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
			t.Fatalf("insert duplicate exact linked evidence identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, exactEvidenceID, sourceLinkedKeyID, &sourceLinkedID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert source linked evidence identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, exactEvidenceID, sourceLinkedKeyID, &sourceLinkedID, seedstore.EventReviewEvidenceIdentityKeyRoleDerived); err != nil {
			t.Fatalf("insert duplicate source linked evidence identity: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO event_source_links (
				source_id,
				event_id,
				source_event_key,
				is_authoritative,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`, sourceLinkedID, sourceLinkedEventID, "candidate-source-key", 1, "2026-05-15T09:30:00Z", "2026-05-15T09:30:00Z"); err != nil {
			t.Fatalf("insert source linked event source link: %v", err)
		}

		unmatchedExactTitle := normalizeExactIdentityCleanTitle("Gamma Candidate")
		unmatchedExactKey := buildExactIdentityKey(exactIdentityKeyVersion, identityVenueSlug, identityStartB, unmatchedExactTitle)
		unmatchedExactKeyID := insertEventReviewIdentityKeyOK(t, db, "candidate-unmatched-exact-hash", seedstore.EventReviewIdentityKeyKindExact, unmatchedExactKey)
		unmatchedSourceKeyID := insertEventReviewIdentityKeyOK(t, db, "candidate-unmatched-source-hash", seedstore.EventReviewIdentityKeyKindSource, "candidate-unmatched-source-key")
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, unmatchedEvidenceID, unmatchedExactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
			t.Fatalf("insert unmatched exact evidence identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, unmatchedEvidenceID, unmatchedSourceKeyID, &otherSourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert unmatched source evidence identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceLinkedID, "candidate-source-key", true, "preferred source identity", time.Date(2026, time.May, 15, 9, 30, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert chosen source identity: %v", err)
		}
		if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, otherSourceID, "candidate-unmatched-source-key", false, "not selected", time.Date(2026, time.May, 15, 9, 31, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert unselected source identity: %v", err)
		}

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load candidate identity cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("candidate identity readiness missing")
		}
		statuses := detail.ImportReadiness.CandidateIdentityStatuses
		if len(statuses) != 2 {
			t.Fatalf("candidate identity statuses = %#v, want 2", statuses)
		}
		exactStatus := mustImportReadinessCandidateIdentityStatus(t, statuses, exactEvidenceID)
		if exactStatus.ParseWarning != "" || len(exactStatus.ExactKeys) != 1 || len(exactStatus.SourceKeys) != 1 {
			t.Fatalf("exact candidate identity status = %#v", exactStatus)
		}
		exactKey := mustImportReadinessCandidateExactKeyStatus(t, exactStatus.ExactKeys, exactLinkedKey)
		if exactKey.LinkedEventID == nil || *exactKey.LinkedEventID != exactLinkedEventID || exactKey.LinkedEventSlug != "candidate-exact-linked-event" || exactKey.LinkedEventTitle != "Alpha Candidate" {
			t.Fatalf("exact candidate exact key = %#v", exactKey)
		}
		if exactKey.IdentityKeyHash != exactLinkedHash {
			t.Fatalf("exact candidate exact key hash = %q, want %q", exactKey.IdentityKeyHash, exactLinkedHash)
		}
		sourceKey := mustImportReadinessCandidateSourceKeyStatus(t, exactStatus.SourceKeys, sourceLinkedID, "candidate-source-key")
		if sourceKey.LinkedEventID == nil || *sourceKey.LinkedEventID != sourceLinkedEventID || sourceKey.LinkedEventSlug != "candidate-source-linked-event" || sourceKey.LinkedEventTitle != "Source Linked Event" || !sourceKey.Authoritative {
			t.Fatalf("exact candidate source key = %#v", sourceKey)
		}
		if sourceKey.SourceName != "Store test source" {
			t.Fatalf("exact candidate source key source name = %q", sourceKey.SourceName)
		}
		if !sourceKey.ChoiceSelected || sourceKey.ChoiceReason != "preferred source identity" || sourceKey.ChoiceUpdatedAt == nil || !sourceKey.ChoiceUpdatedAt.Equal(time.Date(2026, time.May, 15, 9, 30, 0, 0, time.UTC)) {
			t.Fatalf("exact candidate source key choice = %#v", sourceKey)
		}

		unmatchedStatus := mustImportReadinessCandidateIdentityStatus(t, statuses, unmatchedEvidenceID)
		if unmatchedStatus.ParseWarning != "" || len(unmatchedStatus.ExactKeys) != 1 || len(unmatchedStatus.SourceKeys) != 1 {
			t.Fatalf("unmatched candidate identity status = %#v", unmatchedStatus)
		}
		unmatchedExactKeyStatus := mustImportReadinessCandidateExactKeyStatus(t, unmatchedStatus.ExactKeys, unmatchedExactKey)
		if unmatchedExactKeyStatus.LinkedEventID != nil || unmatchedExactKeyStatus.LinkedEventSlug != "" || unmatchedExactKeyStatus.LinkedEventTitle != "" {
			t.Fatalf("unmatched candidate exact key linked event = %#v", unmatchedExactKeyStatus)
		}
		unmatchedSourceKeyStatus := mustImportReadinessCandidateSourceKeyStatus(t, unmatchedStatus.SourceKeys, otherSourceID, "candidate-unmatched-source-key")
		if unmatchedSourceKeyStatus.LinkedEventID != nil || unmatchedSourceKeyStatus.LinkedEventSlug != "" || unmatchedSourceKeyStatus.LinkedEventTitle != "" {
			t.Fatalf("unmatched candidate source key linked event = %#v", unmatchedSourceKeyStatus)
		}
		if unmatchedSourceKeyStatus.Authoritative {
			t.Fatalf("unmatched candidate source key authoritative = %#v", unmatchedSourceKeyStatus)
		}
		if unmatchedSourceKeyStatus.ChoiceSelected || unmatchedSourceKeyStatus.ChoiceReason != "not selected" || unmatchedSourceKeyStatus.ChoiceUpdatedAt == nil || !unmatchedSourceKeyStatus.ChoiceUpdatedAt.Equal(time.Date(2026, time.May, 15, 9, 31, 0, 0, time.UTC)) {
			t.Fatalf("unmatched candidate source key choice = %#v", unmatchedSourceKeyStatus)
		}

		selectedReadiness := detail.ImportReadiness.SelectedCandidateReadiness
		if selectedReadiness == nil {
			t.Fatal("selected candidate readiness is nil")
		}
		if selectedReadiness.EvidenceID != exactEvidenceID || selectedReadiness.EvidenceFingerprint != exactStatus.EvidenceFingerprint || selectedReadiness.Title != "Alpha Candidate" || selectedReadiness.VenueSlug != identityVenueSlug || selectedReadiness.VenueText != identityVenueText {
			t.Fatalf("selected candidate readiness = %#v", selectedReadiness)
		}
		if selectedReadiness.StartAt == nil || !selectedReadiness.StartAt.Equal(identityStartA) {
			t.Fatalf("selected candidate readiness start = %#v", selectedReadiness.StartAt)
		}
		if selectedReadiness.Eligible {
			t.Fatalf("selected candidate readiness = %#v, want blocked", selectedReadiness)
		}
		if !hasString(selectedReadiness.BlockingReasons, "selected candidate exact identity already links to live event") || !hasString(selectedReadiness.BlockingReasons, "selected candidate source identity already links to live event") {
			t.Fatalf("selected candidate readiness blockers = %#v", selectedReadiness.BlockingReasons)
		}
		if len(selectedReadiness.SelectedSourceKeys) != 1 {
			t.Fatalf("selected candidate selected source keys = %#v, want 1", selectedReadiness.SelectedSourceKeys)
		}
		if len(selectedReadiness.ExactKeys) != 1 || len(selectedReadiness.SourceKeys) != 1 {
			t.Fatalf("selected candidate key lists = %#v / %#v", selectedReadiness.ExactKeys, selectedReadiness.SourceKeys)
		}
	})

	t.Run("malformed payload warns", func(t *testing.T) {
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{"{bad payload"}, nil)
		var evidenceID int64
		if err := db.QueryRow(`
			SELECT e.id
			FROM event_review_evidence e
			JOIN event_review_cluster_evidence ce ON ce.evidence_id = e.id
			WHERE ce.cluster_id = ?
				AND ce.active = 1
			ORDER BY ce.id
			LIMIT 1
		`, clusterID).Scan(&evidenceID); err != nil {
			t.Fatalf("lookup malformed evidence id: %v", err)
		}
		malformedExactKeyID := insertEventReviewIdentityKeyOK(t, db, "malformed-exact-hash", seedstore.EventReviewIdentityKeyKindExact, "malformed-exact-key")
		malformedSourceKeyID := insertEventReviewIdentityKeyOK(t, db, "malformed-source-hash", seedstore.EventReviewIdentityKeyKindSource, "malformed-source-key")
		if _, err := db.Exec(`
			INSERT INTO event_exact_identities (
				event_id,
				identity_key,
				key_version,
				venue_slug,
				utc_start_at,
				clean_title,
				active,
				created_at,
				updated_at,
				deactivated_at,
				deactivated_reason,
				repair_run_id
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, existingEventID, "malformed-exact-key", exactIdentityKeyVersion, "event-review-import-readiness-hall", "2026-05-10T19:00:00Z", "Import Readiness Existing", 1, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z", nil, "", nil); err != nil {
			t.Fatalf("insert malformed linked exact identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, malformedExactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
			t.Fatalf("insert malformed exact evidence identity: %v", err)
		}
		if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, malformedSourceKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
			t.Fatalf("insert malformed source evidence identity: %v", err)
		}
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load malformed import cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("malformed import readiness missing")
		}
		if detail.ImportReadiness.NewListingScope {
			t.Fatalf("malformed import readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if len(detail.ImportReadiness.PayloadWarnings) == 0 || !hasString(detail.ImportReadiness.BlockingReasons, "payload could not be parsed") {
			t.Fatalf("malformed readiness = %#v", detail.ImportReadiness)
		}
		if detail.ImportReadiness.CandidateComparisonScope {
			t.Fatalf("malformed comparison readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.ComparisonBlockingReasons, "payload could not be parsed") {
			t.Fatalf("malformed comparison blockers = %#v", detail.ImportReadiness.ComparisonBlockingReasons)
		}
		if len(detail.ImportReadiness.CandidateIdentityStatuses) != 1 {
			t.Fatalf("malformed candidate identity statuses = %#v, want 1", detail.ImportReadiness.CandidateIdentityStatuses)
		}
		status := detail.ImportReadiness.CandidateIdentityStatuses[0]
		if status.ParseWarning == "" || len(status.ExactKeys) != 1 || len(status.SourceKeys) != 1 {
			t.Fatalf("malformed candidate identity status = %#v", status)
		}
		if status.ExactKeys[0].NormalizedKey != "malformed-exact-key" || status.ExactKeys[0].LinkedEventID == nil || *status.ExactKeys[0].LinkedEventID != existingEventID {
			t.Fatalf("malformed exact key = %#v", status.ExactKeys[0])
		}
		if status.SourceKeys[0].SourceIdentityKey != "malformed-source-key" || status.SourceKeys[0].LinkedEventID != nil {
			t.Fatalf("malformed source key = %#v", status.SourceKeys[0])
		}
		target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, evidenceID, existingEventID, seedstore.EventReviewImportTargetBasisExactIdentity)
		if !hasString(target.BlockingReasons, "candidate payload could not be materialized") {
			t.Fatalf("malformed exact target blockers = %#v, want materialization blocker", target.BlockingReasons)
		}
	})

	t.Run("terminal cluster blocks", func(t *testing.T) {
		payload := importPayload("Terminal Title", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "external-terminal")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusResolved), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payload}, nil)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load terminal import cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("terminal import readiness missing")
		}
		if detail.ImportReadiness.NewListingScope {
			t.Fatalf("terminal readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.BlockingReasons, "cluster is not open") {
			t.Fatalf("terminal blockers = %#v", detail.ImportReadiness.BlockingReasons)
		}
	})

	t.Run("canonical and existing event block", func(t *testing.T) {
		payload := importPayload("Existing Event Title", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "external-existing")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), &existingEventID, seedstore.EventReviewConflictReasonIngestCandidate, []string{payload}, []*int64{&existingEventID})
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load canonical/event-id blocked import cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("blocked import readiness missing")
		}
		if detail.ImportReadiness.NewListingScope {
			t.Fatalf("blocked readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.BlockingReasons, "canonical event is already set") || !hasString(detail.ImportReadiness.BlockingReasons, "evidence already references existing event") {
			t.Fatalf("blocked readiness blockers = %#v", detail.ImportReadiness.BlockingReasons)
		}
		if detail.ImportReadiness.CandidateComparisonScope {
			t.Fatalf("blocked comparison readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.ComparisonBlockingReasons, "canonical event is already set") || !hasString(detail.ImportReadiness.ComparisonBlockingReasons, "evidence already references existing event") {
			t.Fatalf("blocked comparison blockers = %#v", detail.ImportReadiness.ComparisonBlockingReasons)
		}
	})

	t.Run("withheld canonical existing target blocks", func(t *testing.T) {
		insertLegacyEvent(t, db, "import-readiness-withheld-canonical", venueID, sourceID, domain.OriginLive)
		withheldEventID := mustEventIDBySlug(t, db, "import-readiness-withheld-canonical")
		if _, err := db.Exec(`
			UPDATE events
			SET publication_state = ?,
				withheld_reason = ?
			WHERE id = ?
		`, string(domain.PublicationStateWithheld), "duplicate listing", withheldEventID); err != nil {
			t.Fatalf("withhold canonical target: %v", err)
		}

		payload := importPayload("Withheld Existing Target", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "external-withheld-existing")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), &withheldEventID, seedstore.EventReviewConflictReasonIngestCandidate, []string{payload}, []*int64{&withheldEventID})
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load withheld canonical target cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("withheld canonical target readiness missing")
		}
		target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, detail.Evidence[0].EvidenceID, withheldEventID, seedstore.EventReviewImportTargetBasisCanonicalEvent)
		if !hasString(target.BlockingReasons, "target event is not live/non-withheld") {
			t.Fatalf("withheld canonical target blockers = %#v, want target state blocker", target.BlockingReasons)
		}
		target = mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, detail.Evidence[0].EvidenceID, withheldEventID, seedstore.EventReviewImportTargetBasisEvidenceEvent)
		if !hasString(target.BlockingReasons, "target event is not live/non-withheld") {
			t.Fatalf("withheld evidence target blockers = %#v, want target state blocker", target.BlockingReasons)
		}
	})

	t.Run("comparison invalid start blocks", func(t *testing.T) {
		payloadA := importPayload("Invalid Start Title", "event-review-import-ready", "Event Review Import Ready", "not-a-time", "", "external-invalid-start-a")
		payloadB := importPayload("Invalid Start Title", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:30:00Z", "", "external-invalid-start-b")
		clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{payloadA, payloadB}, nil)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
		if err != nil {
			t.Fatalf("load invalid start comparison cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("invalid start comparison readiness missing")
		}
		if detail.ImportReadiness.CandidateComparisonScope {
			t.Fatalf("invalid start comparison readiness = %#v, want blocked", detail.ImportReadiness)
		}
		if !hasString(detail.ImportReadiness.ComparisonBlockingReasons, "candidate start is required") {
			t.Fatalf("invalid start comparison blockers = %#v", detail.ImportReadiness.ComparisonBlockingReasons)
		}
	})

	for _, tc := range []struct {
		name      string
		payload   string
		wantBlock string
	}{
		{
			name:      "missing title",
			payload:   importPayload("", "event-review-import-ready", "Event Review Import Ready", "2026-05-10T19:00:00Z", "", "external-missing-title"),
			wantBlock: "candidate title is required",
		},
		{
			name:      "missing start",
			payload:   importPayload("Missing Start", "event-review-import-ready", "Event Review Import Ready", "", "", "external-missing-start"),
			wantBlock: "candidate start is required",
		},
		{
			name:      "invalid start",
			payload:   importPayload("Invalid Start", "event-review-import-ready", "Event Review Import Ready", "not-a-time", " 2026-05-10T21:00:00Z ", "external-invalid-start"),
			wantBlock: "candidate start is required",
		},
		{
			name:      "missing venue",
			payload:   importPayload("Missing Venue", "", "", "2026-05-10T19:00:00Z", "", "external-missing-venue"),
			wantBlock: "candidate venue is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clusterID := stageImportCluster(t, string(seedstore.EventReviewClusterStatusOpen), nil, seedstore.EventReviewConflictReasonIngestCandidate, []string{tc.payload}, nil)
			detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
			if err != nil {
				t.Fatalf("load blocked import cluster: %v", err)
			}
			if !ok || detail.ImportReadiness == nil {
				t.Fatal("blocked import readiness missing")
			}
			if detail.ImportReadiness.NewListingScope {
				t.Fatalf("blocked readiness = %#v, want blocked", detail.ImportReadiness)
			}
			if !hasString(detail.ImportReadiness.BlockingReasons, tc.wantBlock) {
				t.Fatalf("blocked readiness blockers = %#v, want %q", detail.ImportReadiness.BlockingReasons, tc.wantBlock)
			}
			if tc.name == "invalid start" && countString(detail.ImportReadiness.BlockingReasons, tc.wantBlock) != 1 {
				t.Fatalf("invalid start blocker count in %#v, want exactly one %q", detail.ImportReadiness.BlockingReasons, tc.wantBlock)
			}
		})
	}
}

func TestEventReviewReadModelsDetailIdentityKeys(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-key-hall", "Event Review Key Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "identity-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "inactive-identity-event", venueID, sourceID, domain.OriginLive)

	eventID := mustEventIDBySlug(t, db, "identity-event")
	inactiveEventID := mustEventIDBySlug(t, db, "inactive-identity-event")

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	sourceHash := "source-hash"
	exactHash := "exact-hash"
	inactiveHash := "inactive-hash"
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, sourceHash, seedstore.EventReviewIdentityKeyKindSource, "source-normalized")
	exactKeyID := insertEventReviewIdentityKeyOK(t, db, exactHash, seedstore.EventReviewIdentityKeyKindExact, "exact-normalized")
	inactiveKeyID := insertEventReviewIdentityKeyOK(t, db, inactiveHash, seedstore.EventReviewIdentityKeyKindManual, "inactive-normalized")

	activeEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &eventID, "fingerprint-active", `{"payload":"active"}`)
	inactiveEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &inactiveEventID, "fingerprint-inactive", `{"payload":"inactive"}`)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, activeEvidenceID, true, time.Date(2026, time.May, 15, 10, 20, 0, 0, time.UTC), nil, "active evidence"); err != nil {
		t.Fatalf("insert active cluster evidence link: %v", err)
	}
	inactiveEvidenceUnlinkedAt := time.Date(2026, time.May, 15, 10, 26, 0, 0, time.UTC)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, inactiveEvidenceID, false, time.Date(2026, time.May, 15, 10, 25, 0, 0, time.UTC), &inactiveEvidenceUnlinkedAt, "inactive evidence"); err != nil {
		t.Fatalf("insert inactive cluster evidence link: %v", err)
	}

	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, sourceKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert active source cluster identity key: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, exactKeyID, true, time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert active exact cluster identity key: %v", err)
	}
	inactiveUnlinkedAt := time.Date(2026, time.May, 15, 10, 11, 0, 0, time.UTC)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, inactiveKeyID, false, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), &inactiveUnlinkedAt); err != nil {
		t.Fatalf("insert inactive cluster identity key: %v", err)
	}

	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceID, exactKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert active exact evidence identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceID, sourceKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert active source evidence identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, inactiveEvidenceID, inactiveKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleDerived); err != nil {
		t.Fatalf("insert inactive evidence identity key: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load event review cluster ok = false, want true")
	}
	if len(detail.ClusterIdentityKeys) != 2 {
		t.Fatalf("cluster identity keys = %#v, want 2 active rows", detail.ClusterIdentityKeys)
	}
	if detail.ClusterIdentityKeys[0].KeyKind != seedstore.EventReviewIdentityKeyKindExact || detail.ClusterIdentityKeys[0].NormalizedKey != "exact-normalized" {
		t.Fatalf("first cluster identity key = %#v, want exact-normalized", detail.ClusterIdentityKeys[0])
	}
	if detail.ClusterIdentityKeys[1].KeyKind != seedstore.EventReviewIdentityKeyKindSource || detail.ClusterIdentityKeys[1].NormalizedKey != "source-normalized" {
		t.Fatalf("second cluster identity key = %#v, want source-normalized", detail.ClusterIdentityKeys[1])
	}
	if len(detail.EvidenceIdentityKeys) != 2 {
		t.Fatalf("evidence identity keys = %#v, want 2 active rows", detail.EvidenceIdentityKeys)
	}
	if detail.EvidenceIdentityKeys[0].Role != seedstore.EventReviewEvidenceIdentityKeyRoleExact || detail.EvidenceIdentityKeys[0].IdentityKeyHash != exactHash || detail.EvidenceIdentityKeys[0].KeyVersion != 1 {
		t.Fatalf("first evidence identity key = %#v, want exact hash/version", detail.EvidenceIdentityKeys[0])
	}
	if detail.EvidenceIdentityKeys[0].SourceID == nil || *detail.EvidenceIdentityKeys[0].SourceID != sourceID {
		t.Fatalf("first evidence identity source id = %v, want %d", detail.EvidenceIdentityKeys[0].SourceID, sourceID)
	}
	if detail.EvidenceIdentityKeys[1].Role != seedstore.EventReviewEvidenceIdentityKeyRoleObserved || detail.EvidenceIdentityKeys[1].IdentityKeyHash != sourceHash || detail.EvidenceIdentityKeys[1].KeyVersion != 1 {
		t.Fatalf("second evidence identity key = %#v, want observed source hash/version", detail.EvidenceIdentityKeys[1])
	}
	if detail.EvidenceIdentityKeys[1].SourceID != nil {
		t.Fatalf("second evidence identity source id = %v, want nil", detail.EvidenceIdentityKeys[1].SourceID)
	}
}

func TestEventReviewReadModelsExactIdentityMatches(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-exact-hall", "Event Review Exact Hall", domain.OriginLive)

	liveStart := time.Date(2026, time.May, 15, 19, 0, 0, 0, time.UTC)
	liveEnd := liveStart.Add(2 * time.Hour)
	liveLastChecked := time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC)
	liveEventID := mustInsertExactIdentityEvent(t, db, "exact-live-match-event", "Alpha Live Match", venueID, sourceID, liveStart, liveEnd, liveLastChecked, domain.OriginLive)
	liveKey := buildExactIdentityKey(exactIdentityKeyVersion, "event-review-exact-hall", liveStart, "Alpha Live Match")
	if _, ok, err := exactIdentityMaterialForEvent(domain.Event{
		Slug:             "exact-live-match-event",
		Name:             "Alpha Live Match",
		VenueSlug:        "event-review-exact-hall",
		Start:            liveStart,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}); err != nil {
		t.Fatalf("live exact identity material: %v", err)
	} else if !ok {
		t.Fatal("live exact identity material not available")
	}
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin live exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, liveEventID, domain.Event{
		Slug:             "exact-live-match-event",
		Name:             "Alpha Live Match",
		VenueSlug:        "event-review-exact-hall",
		Start:            liveStart,
		Origin:           domain.OriginLive,
		LastChecked:      liveLastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, liveLastChecked); err != nil {
		t.Fatalf("ensure live exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit live exact identity tx: %v", err)
	}

	withheldStart := liveStart.Add(1 * time.Hour)
	withheldEnd := withheldStart.Add(2 * time.Hour)
	withheldLastChecked := time.Date(2026, time.May, 15, 9, 30, 0, 0, time.UTC)
	withheldEventID := mustInsertExactIdentityEvent(t, db, "exact-withheld-event", "Gamma Withheld", venueID, sourceID, withheldStart, withheldEnd, withheldLastChecked, domain.OriginLive)
	withheldKey := buildExactIdentityKey(exactIdentityKeyVersion, "event-review-exact-hall", withheldStart, "Gamma Withheld")
	if _, ok, err := exactIdentityMaterialForEvent(domain.Event{
		Slug:             "exact-withheld-event",
		Name:             "Gamma Withheld",
		VenueSlug:        "event-review-exact-hall",
		Start:            withheldStart,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}); err != nil {
		t.Fatalf("withheld exact identity material: %v", err)
	} else if !ok {
		t.Fatal("withheld exact identity material not available")
	}
	tx, err = st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin withheld exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, withheldEventID, domain.Event{
		Slug:             "exact-withheld-event",
		Name:             "Gamma Withheld",
		VenueSlug:        "event-review-exact-hall",
		Start:            withheldStart,
		Origin:           domain.OriginLive,
		LastChecked:      withheldLastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, withheldLastChecked); err != nil {
		t.Fatalf("ensure withheld exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit withheld exact identity tx: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET publication_state = ?, withheld_reason = ? WHERE id = ?`, string(domain.PublicationStateWithheld), "duplicate listing", withheldEventID); err != nil {
		t.Fatalf("withhold exact identity event: %v", err)
	}

	nonLiveStart := liveStart.Add(2 * time.Hour)
	nonLiveEnd := nonLiveStart.Add(2 * time.Hour)
	nonLiveLastChecked := time.Date(2026, time.May, 15, 9, 45, 0, 0, time.UTC)
	nonLiveEventID := mustInsertExactIdentityEvent(t, db, "exact-non-live-event", "Delta Non Live", venueID, sourceID, nonLiveStart, nonLiveEnd, nonLiveLastChecked, domain.OriginTest)
	nonLiveKey := buildExactIdentityKey(exactIdentityKeyVersion, "event-review-exact-hall", nonLiveStart, "Delta Non Live")
	if _, err := db.Exec(`
		INSERT INTO event_exact_identities (
			event_id,
			identity_key,
			key_version,
			venue_slug,
			utc_start_at,
			clean_title,
			active,
			created_at,
			updated_at,
			deactivated_at,
			deactivated_reason,
			repair_run_id
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, '', NULL)
	`, nonLiveEventID, nonLiveKey, exactIdentityKeyVersion, "event-review-exact-hall", formatRFC3339UTC(nonLiveStart), "delta non live", formatRFC3339UTC(nonLiveLastChecked), formatRFC3339UTC(nonLiveLastChecked)); err != nil {
		t.Fatalf("insert non-live exact identity row: %v", err)
	}

	clusterOnlyKey := "cluster-only-exact-key"
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	clusterLiveKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, liveKey)
	clusterOnlyKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, clusterOnlyKey)
	withheldKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, withheldKey)
	nonLiveKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, nonLiveKey)
	sourceKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindSource, eventReviewIdentityKeyVersion, "source-observed")
	clusterLiveKeyID := insertEventReviewIdentityKeyOK(t, db, clusterLiveKeyHash, seedstore.EventReviewIdentityKeyKindExact, liveKey)
	clusterOnlyKeyID := insertEventReviewIdentityKeyOK(t, db, clusterOnlyKeyHash, seedstore.EventReviewIdentityKeyKindExact, clusterOnlyKey)
	withheldKeyID := insertEventReviewIdentityKeyOK(t, db, withheldKeyHash, seedstore.EventReviewIdentityKeyKindExact, withheldKey)
	nonLiveKeyID := insertEventReviewIdentityKeyOK(t, db, nonLiveKeyHash, seedstore.EventReviewIdentityKeyKindExact, nonLiveKey)
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, sourceKeyHash, seedstore.EventReviewIdentityKeyKindSource, "source-observed")

	activeEvidenceOneID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "exact-live-fingerprint-1", `{"payload":"exact-live-1"}`)
	activeEvidenceTwoID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "exact-live-fingerprint-2", `{"payload":"exact-live-2"}`)
	inactiveEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "exact-live-fingerprint-inactive", `{"payload":"exact-live-inactive"}`)
	withheldEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "exact-withheld-fingerprint", `{"payload":"exact-withheld"}`)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, activeEvidenceOneID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil, "active evidence one"); err != nil {
		t.Fatalf("insert active evidence one link: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, activeEvidenceTwoID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil, "active evidence two"); err != nil {
		t.Fatalf("insert active evidence two link: %v", err)
	}
	inactiveUnlinkedAt := time.Date(2026, time.May, 15, 10, 3, 0, 0, time.UTC)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, inactiveEvidenceID, false, time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC), &inactiveUnlinkedAt, "inactive evidence"); err != nil {
		t.Fatalf("insert inactive evidence link: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, withheldEvidenceID, true, time.Date(2026, time.May, 15, 10, 4, 0, 0, time.UTC), nil, "withheld evidence"); err != nil {
		t.Fatalf("insert withheld evidence link: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, clusterLiveKeyID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster live exact identity key: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, clusterOnlyKeyID, true, time.Date(2026, time.May, 15, 10, 11, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster-only exact identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceOneID, clusterLiveKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert active evidence one exact identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceTwoID, clusterLiveKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert active evidence two exact identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, inactiveEvidenceID, clusterLiveKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert inactive evidence exact identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, withheldEvidenceID, withheldKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert withheld evidence exact identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceOneID, sourceKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert source identity key on active evidence: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, withheldEvidenceID, sourceKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert source identity key on withheld evidence: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, nonLiveKeyID, true, time.Date(2026, time.May, 15, 10, 12, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster non-live exact identity key: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
	if err != nil {
		t.Fatalf("load exact identity cluster: %v", err)
	}
	if !ok {
		t.Fatal("load exact identity cluster ok = false, want true")
	}
	if len(detail.ExactIdentityMatches) != 4 {
		t.Fatalf("exact identity matches = %#v, want 4 rows", detail.ExactIdentityMatches)
	}
	for i := 1; i < len(detail.ExactIdentityMatches); i++ {
		if detail.ExactIdentityMatches[i-1].NormalizedKey > detail.ExactIdentityMatches[i].NormalizedKey {
			t.Fatalf("exact identity matches not ordered by normalized key: %#v", detail.ExactIdentityMatches)
		}
	}

	liveRow := mustExactIdentityMatchRow(t, detail.ExactIdentityMatches, liveKey)
	if liveRow.EvidenceCount != 2 {
		t.Fatalf("live exact identity evidence count = %d, want 2", liveRow.EvidenceCount)
	}
	if liveRow.LinkedEventID == nil || *liveRow.LinkedEventID != liveEventID || liveRow.LinkedEventSlug != "exact-live-match-event" || liveRow.LinkedEventTitle != "Alpha Live Match" || liveRow.LinkedEventVenueSlug != "event-review-exact-hall" {
		t.Fatalf("live exact identity linked event = %#v", liveRow)
	}
	if liveRow.LinkedEventStartAt == nil || !liveRow.LinkedEventStartAt.Equal(liveStart) {
		t.Fatalf("live exact identity start = %v, want %v", liveRow.LinkedEventStartAt, liveStart)
	}

	clusterOnlyRow := mustExactIdentityMatchRow(t, detail.ExactIdentityMatches, clusterOnlyKey)
	if clusterOnlyRow.EvidenceCount != 0 {
		t.Fatalf("cluster-only exact identity evidence count = %d, want 0", clusterOnlyRow.EvidenceCount)
	}
	if clusterOnlyRow.LinkedEventID != nil || clusterOnlyRow.LinkedEventSlug != "" || clusterOnlyRow.LinkedEventTitle != "" || clusterOnlyRow.LinkedEventVenueSlug != "" || clusterOnlyRow.LinkedEventStartAt != nil {
		t.Fatalf("cluster-only exact identity linked event = %#v, want no live match", clusterOnlyRow)
	}

	withheldRow := mustExactIdentityMatchRow(t, detail.ExactIdentityMatches, withheldKey)
	if withheldRow.EvidenceCount != 1 {
		t.Fatalf("withheld exact identity evidence count = %d, want 1", withheldRow.EvidenceCount)
	}
	if withheldRow.LinkedEventID != nil || withheldRow.LinkedEventSlug != "" || withheldRow.LinkedEventTitle != "" || withheldRow.LinkedEventVenueSlug != "" || withheldRow.LinkedEventStartAt != nil {
		t.Fatalf("withheld exact identity linked event = %#v, want no live match", withheldRow)
	}

	nonLiveRow := mustExactIdentityMatchRow(t, detail.ExactIdentityMatches, nonLiveKey)
	if nonLiveRow.EvidenceCount != 0 {
		t.Fatalf("non-live exact identity evidence count = %d, want 0", nonLiveRow.EvidenceCount)
	}
	if nonLiveRow.LinkedEventID != nil || nonLiveRow.LinkedEventSlug != "" || nonLiveRow.LinkedEventTitle != "" || nonLiveRow.LinkedEventVenueSlug != "" || nonLiveRow.LinkedEventStartAt != nil {
		t.Fatalf("non-live exact identity linked event = %#v, want no live match", nonLiveRow)
	}
	if nonLiveRow.IdentityKeyID != nonLiveKeyID {
		t.Fatalf("non-live exact identity key id = %d, want %d", nonLiveRow.IdentityKeyID, nonLiveKeyID)
	}
	for _, row := range detail.ExactIdentityMatches {
		if row.NormalizedKey == "source-observed" {
			t.Fatalf("source identity key leaked into exact matches: %#v", detail.ExactIdentityMatches)
		}
	}
}

func TestEventReviewReadModelsClusterObservations(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-observation-hall", "Event Review Observation Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "observation-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "observation-noise-event", venueID, sourceID, domain.OriginLive)

	noiseEventID := mustEventIDBySlug(t, db, "observation-noise-event")
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)

	now := time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)
	later := now.Add(5 * time.Minute)
	if _, err := db.Exec(`
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			event_review_cluster_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:301", sourceID, "beta-identity", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindEventReviewCluster), clusterID, "title", "Raw beta title", "Normalized beta title", "Canonical before beta", "Canonical normalized beta", "applied", 0, now.Format(time.RFC3339), later.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert beta cluster observation: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			event_review_cluster_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:301", sourceID, "alpha-identity", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindEventReviewCluster), clusterID, "genre", "Raw alpha genre", "Normalized alpha genre", "", "", "conflict_observed", 1, now.Format(time.RFC3339), later.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert alpha cluster observation: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("get db connection: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			event_review_cluster_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "repair:77", 999, "missing-source-identity", string(seedstore.SourceAuthorityAuthoritative), string(seedstore.ObservationTargetKindEventReviewCluster), clusterID, "venue", "Raw missing venue", "", "", "", "missing_source", 0, now.Add(-time.Minute).Format(time.RFC3339), now.Add(-time.Minute).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert missing-source cluster observation: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			event_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:302", sourceID, "noise-event-identity", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindEvent), noiseEventID, "title", "Noise title", "Noise normalized", "", "", "staged_for_review", 0, now.Add(2*time.Minute).Format(time.RFC3339), now.Add(2*time.Minute).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert noise event observation: %v", err)
	}
	reviewGroupIDRes, err := conn.ExecContext(context.Background(), `
		INSERT INTO review_groups (
			title, source_name, source_url, status, notes, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "Noise review group 2", "Store test source", "https://example.test/store-test", "open", "", now.Add(time.Minute).Format(time.RFC3339), now.Add(time.Minute).Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert review group 2: %v", err)
	}
	reviewGroupID, err := reviewGroupIDRes.LastInsertId()
	if err != nil {
		t.Fatalf("review group id: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			review_group_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "repair:78", sourceID, "review-group-identity", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindReviewGroup), reviewGroupID, "title", "Review group title", "Review group normalized", "", "", "staged_for_review", 0, now.Add(3*time.Minute).Format(time.RFC3339), now.Add(3*time.Minute).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert review group observation: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("re-enable foreign keys: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close db connection: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load event review cluster ok = false, want true")
	}
	if len(detail.Observations) != 3 {
		t.Fatalf("observations = %#v, want 3 cluster-target rows", detail.Observations)
	}
	if detail.Observations[0].RunScope != "import:301" || detail.Observations[0].SourceName != "Store test source" || detail.Observations[0].SourceURL != "https://example.test/store-test" {
		t.Fatalf("first observation = %#v", detail.Observations[0])
	}
	if detail.Observations[0].SourceIdentityKey != "alpha-identity" || detail.Observations[0].FieldName != "genre" || detail.Observations[0].Outcome != "conflict_observed" || !detail.Observations[0].IsConflict {
		t.Fatalf("first observation fields = %#v", detail.Observations[0])
	}
	if detail.Observations[1].SourceIdentityKey != "beta-identity" || detail.Observations[1].FieldName != "title" || detail.Observations[1].Outcome != "applied" {
		t.Fatalf("second observation fields = %#v", detail.Observations[1])
	}
	if detail.Observations[2].SourceID != 999 || detail.Observations[2].SourceName != "" || detail.Observations[2].SourceURL != "" || detail.Observations[2].SourceIdentityKey != "missing-source-identity" || detail.Observations[2].FieldName != "venue" {
		t.Fatalf("third observation sparse source fields = %#v", detail.Observations[2])
	}
	for _, obs := range detail.Observations {
		if obs.RunScope == "import:302" || obs.RunScope == "repair:78" {
			t.Fatalf("unexpected non-cluster observation included: %#v", obs)
		}
	}
}

func TestEventReviewReadModelsSourceIdentityLinks(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-source-link-hall", "Event Review Source Link Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "source-link-linked-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "source-link-inactive-event", venueID, sourceID, domain.OriginLive)

	linkedEventID := mustEventIDBySlug(t, db, "source-link-linked-event")
	inactiveLinkedEventID := mustEventIDBySlug(t, db, "source-link-inactive-event")
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)

	alphaKeyID := insertEventReviewIdentityKeyOK(t, db, "source-linked-hash", seedstore.EventReviewIdentityKeyKindSource, "source-linked")
	betaKeyID := insertEventReviewIdentityKeyOK(t, db, "source-unlinked-hash", seedstore.EventReviewIdentityKeyKindSource, "source-unlinked")
	inactiveKeyID := insertEventReviewIdentityKeyOK(t, db, "source-inactive-hash", seedstore.EventReviewIdentityKeyKindSource, "source-inactive")
	exactKeyID := insertEventReviewIdentityKeyOK(t, db, "exact-ignore-hash", seedstore.EventReviewIdentityKeyKindExact, "exact-ignore")
	manualKeyID := insertEventReviewIdentityKeyOK(t, db, "manual-ignore-hash", seedstore.EventReviewIdentityKeyKindManual, "manual-ignore")

	alphaEvidenceOneID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-linked-evidence-1", `{"payload":"alpha-1"}`)
	alphaEvidenceTwoID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-linked-evidence-2", `{"payload":"alpha-2"}`)
	betaEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-unlinked-evidence", `{"payload":"beta"}`)
	inactiveEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-inactive-evidence", `{"payload":"inactive"}`)

	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, alphaEvidenceOneID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil, "alpha one"); err != nil {
		t.Fatalf("insert alpha evidence one link: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, alphaEvidenceTwoID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil, "alpha two"); err != nil {
		t.Fatalf("insert alpha evidence two link: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, betaEvidenceID, true, time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC), nil, "beta"); err != nil {
		t.Fatalf("insert beta evidence link: %v", err)
	}
	inactiveUnlinkedAt := time.Date(2026, time.May, 15, 10, 4, 30, 0, time.UTC)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, inactiveEvidenceID, false, time.Date(2026, time.May, 15, 10, 3, 0, 0, time.UTC), &inactiveUnlinkedAt, "inactive"); err != nil {
		t.Fatalf("insert inactive evidence link: %v", err)
	}

	if _, err := insertEventReviewEvidenceIdentityKey(t, db, alphaEvidenceOneID, alphaKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert alpha evidence one source identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, alphaEvidenceTwoID, alphaKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert alpha evidence two source identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, betaEvidenceID, betaKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert beta evidence source identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, inactiveEvidenceID, inactiveKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert inactive evidence source identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, betaEvidenceID, exactKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert exact evidence identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, betaEvidenceID, manualKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleDerived); err != nil {
		t.Fatalf("insert manual evidence identity: %v", err)
	}

	linkedAt := time.Date(2026, time.May, 15, 10, 4, 0, 0, time.UTC)
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id,
			event_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, sourceID, linkedEventID, "source-linked", 1, linkedAt.Format(time.RFC3339), linkedAt.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert linked source identity: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id,
			event_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, sourceID, inactiveLinkedEventID, "source-inactive", 0, linkedAt.Add(1*time.Minute).Format(time.RFC3339), linkedAt.Add(1*time.Minute).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert inactive source identity link: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load event review cluster ok = false, want true")
	}
	if len(detail.SourceIdentityLinks) != 2 {
		t.Fatalf("source identity links = %#v, want 2 active source rows", detail.SourceIdentityLinks)
	}
	if detail.SourceIdentityLinks[0].SourceIdentityKey != "source-linked" || detail.SourceIdentityLinks[0].EvidenceCount != 2 {
		t.Fatalf("first source identity link = %#v, want collapsed linked row", detail.SourceIdentityLinks[0])
	}
	if detail.SourceIdentityLinks[0].LinkedEventID == nil || *detail.SourceIdentityLinks[0].LinkedEventID != linkedEventID || detail.SourceIdentityLinks[0].LinkedEventSlug != "source-link-linked-event" || detail.SourceIdentityLinks[0].LinkedEventTitle != "Legacy Event" {
		t.Fatalf("first source identity linked event = %#v", detail.SourceIdentityLinks[0])
	}
	if !detail.SourceIdentityLinks[0].Authoritative {
		t.Fatalf("first source identity link authoritative = false, want true")
	}
	if detail.SourceIdentityLinks[0].LinkUpdatedAt == nil || !detail.SourceIdentityLinks[0].LinkUpdatedAt.Equal(linkedAt) {
		t.Fatalf("first source identity link updated at = %v, want %v", detail.SourceIdentityLinks[0].LinkUpdatedAt, linkedAt)
	}
	if detail.SourceIdentityLinks[1].SourceIdentityKey != "source-unlinked" || detail.SourceIdentityLinks[1].EvidenceCount != 1 {
		t.Fatalf("second source identity link = %#v, want unlinked row", detail.SourceIdentityLinks[1])
	}
	if detail.SourceIdentityLinks[1].LinkedEventID != nil || detail.SourceIdentityLinks[1].LinkedEventSlug != "" || detail.SourceIdentityLinks[1].LinkedEventTitle != "" {
		t.Fatalf("second source identity linked event = %#v, want unlinked", detail.SourceIdentityLinks[1])
	}
	if detail.SourceIdentityLinks[1].Authoritative {
		t.Fatalf("second source identity link authoritative = true, want false")
	}
	if detail.SourceIdentityLinks[1].LinkUpdatedAt != nil {
		t.Fatalf("second source identity link updated at = %v, want nil", detail.SourceIdentityLinks[1].LinkUpdatedAt)
	}
	for _, row := range detail.SourceIdentityLinks {
		if row.SourceIdentityKey == "source-inactive" || row.SourceIdentityKey == "exact-ignore" || row.SourceIdentityKey == "manual-ignore" {
			t.Fatalf("unexpected source identity link included: %#v", row)
		}
	}
}

func TestEventReviewReadModelsSeparations(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-separation-hall", "Event Review Separation Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "separation-canonical", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "separation-active", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "separation-inactive", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "separation-unrelated", venueID, sourceID, domain.OriginLive)

	canonicalEventID := mustEventIDBySlug(t, db, "separation-canonical")
	activeEventID := mustEventIDBySlug(t, db, "separation-active")
	inactiveEventID := mustEventIDBySlug(t, db, "separation-inactive")
	unrelatedEventID := mustEventIDBySlug(t, db, "separation-unrelated")

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	activeEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &activeEventID, "separation-fingerprint-active", `{"payload":"active"}`)
	inactiveEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &inactiveEventID, "separation-fingerprint-inactive", `{"payload":"inactive"}`)
	clusterIdentityHash := "cluster-separation-hash"
	evidenceIdentityHash := "evidence-separation-hash"
	clusterIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, clusterIdentityHash, seedstore.EventReviewIdentityKeyKindExact, "cluster-separation-normalized")
	evidenceIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, evidenceIdentityHash, seedstore.EventReviewIdentityKeyKindSource, "evidence-separation-normalized")
	unrelatedIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, "unrelated-separation-hash", seedstore.EventReviewIdentityKeyKindManual, "unrelated-normalized")

	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, activeEvidenceID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil, "active evidence"); err != nil {
		t.Fatalf("insert active cluster evidence link: %v", err)
	}
	inactiveUnlinkedAt := time.Date(2026, time.May, 15, 10, 6, 0, 0, time.UTC)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterID, inactiveEvidenceID, false, time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), &inactiveUnlinkedAt, "inactive evidence"); err != nil {
		t.Fatalf("insert inactive cluster evidence link: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, clusterIdentityKeyID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, activeEvidenceID, evidenceIdentityKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert evidence identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, inactiveEvidenceID, unrelatedIdentityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleDerived); err != nil {
		t.Fatalf("insert inactive evidence identity key: %v", err)
	}

	separatorEventAKey := seedstore.EventReviewSeparationEventEndpointKey(activeEventID)
	separatorEvidenceKey := eventReviewSeparationEndpointKeyEvidence("separation-fingerprint-active")
	separatorClusterIdentityKey := EventReviewSeparationEndpointKeyIdentity(clusterIdentityHash)
	separatorEvidenceIdentityKey := EventReviewSeparationEndpointKeyIdentity(evidenceIdentityHash)
	separatorCanonicalKey := seedstore.EventReviewSeparationEventEndpointKey(canonicalEventID)
	separatorCanonicalEvidenceKey := eventReviewSeparationEndpointKeyEvidence("separation-fingerprint-inactive")
	unrelatedEventKey := seedstore.EventReviewSeparationEventEndpointKey(unrelatedEventID)

	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:        separatorEvidenceKey,
		EvidenceID: int64Ptr(activeEvidenceID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           separatorClusterIdentityKey,
		IdentityKeyID: int64Ptr(clusterIdentityKeyID),
	}, true, "evidence to cluster identity", time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert evidence separation: %v", err)
	}
	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           separatorClusterIdentityKey,
		IdentityKeyID: int64Ptr(clusterIdentityKeyID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           separatorEvidenceIdentityKey,
		IdentityKeyID: int64Ptr(evidenceIdentityKeyID),
	}, true, "identity to evidence identity", time.Date(2026, time.May, 15, 10, 11, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 11, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert identity separation: %v", err)
	}
	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
		Key:     separatorEventAKey,
		EventID: int64Ptr(activeEventID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:        separatorEvidenceKey,
		EvidenceID: int64Ptr(activeEvidenceID),
	}, true, "event to evidence", time.Date(2026, time.May, 15, 10, 12, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 12, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert event separation: %v", err)
	}
	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:             seedstore.EventReviewSeparationEndpointKindEvent,
		Key:              separatorCanonicalKey,
		EventID:          int64Ptr(canonicalEventID),
		CanonicalEventID: int64Ptr(canonicalEventID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:        separatorCanonicalEvidenceKey,
		EvidenceID: int64Ptr(inactiveEvidenceID),
	}, true, "canonical sparse display", time.Date(2026, time.May, 15, 10, 13, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 13, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert canonical separation: %v", err)
	}
	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
		Key:     unrelatedEventKey,
		EventID: int64Ptr(unrelatedEventID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity("unrelated-separation-hash"),
		IdentityKeyID: int64Ptr(unrelatedIdentityKeyID),
	}, true, "unrelated active separation", time.Date(2026, time.May, 15, 10, 14, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 14, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert unrelated separation: %v", err)
	}
	if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
		Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:        separatorEvidenceKey,
		EvidenceID: int64Ptr(activeEvidenceID),
	}, seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           separatorClusterIdentityKey,
		IdentityKeyID: int64Ptr(clusterIdentityKeyID),
	}, false, "inactive separation", time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert inactive separation: %v", err)
	}

	// Clear the canonical event slug so the canonical endpoint renders with sparse joins.
	if _, err := db.Exec(`UPDATE events SET slug = '' WHERE id = ?`, canonicalEventID); err != nil {
		t.Fatalf("blank canonical event slug for sparse display: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load event review cluster ok = false, want true")
	}
	if len(detail.Separations) != 4 {
		t.Fatalf("separations = %#v, want 4 active related rows", detail.Separations)
	}

	var (
		evidenceSeparation  *seedstore.EventReviewClusterSeparationSummary
		identitySeparation  *seedstore.EventReviewClusterSeparationSummary
		eventSeparation     *seedstore.EventReviewClusterSeparationSummary
		canonicalSeparation *seedstore.EventReviewClusterSeparationSummary
	)
	for i := range detail.Separations {
		sep := &detail.Separations[i]
		switch sep.Reason {
		case "evidence to cluster identity":
			evidenceSeparation = sep
		case "identity to evidence identity":
			identitySeparation = sep
		case "event to evidence":
			eventSeparation = sep
		case "canonical sparse display":
			canonicalSeparation = sep
		default:
			if sep.Reason == "unrelated active separation" || sep.Reason == "inactive separation" {
				t.Fatalf("unexpected separation included: %#v", sep)
			}
		}
	}
	if evidenceSeparation == nil || identitySeparation == nil || eventSeparation == nil || canonicalSeparation == nil {
		t.Fatalf("missing expected separations: %#v", detail.Separations)
	}
	if evidenceSeparation.EndpointA.Kind != seedstore.EventReviewSeparationEndpointKindEvidence || evidenceSeparation.EndpointA.EvidenceFingerprint != "separation-fingerprint-active" {
		t.Fatalf("evidence separation endpoint A = %#v", evidenceSeparation.EndpointA)
	}
	if evidenceSeparation.EndpointB.Kind != seedstore.EventReviewSeparationEndpointKindIdentityKey || evidenceSeparation.EndpointB.IdentityKeyHash != clusterIdentityHash || evidenceSeparation.EndpointB.IdentityKeyKind != seedstore.EventReviewIdentityKeyKindExact || evidenceSeparation.EndpointB.NormalizedKey != "cluster-separation-normalized" {
		t.Fatalf("evidence separation endpoint B = %#v", evidenceSeparation.EndpointB)
	}
	if identitySeparation.EndpointA.Kind != seedstore.EventReviewSeparationEndpointKindIdentityKey || identitySeparation.EndpointA.IdentityKeyHash != clusterIdentityHash || identitySeparation.EndpointA.IdentityKeyKind != seedstore.EventReviewIdentityKeyKindExact || identitySeparation.EndpointA.NormalizedKey != "cluster-separation-normalized" {
		t.Fatalf("identity separation endpoint A = %#v", identitySeparation.EndpointA)
	}
	if identitySeparation.EndpointB.IdentityKeyHash != evidenceIdentityHash || identitySeparation.EndpointB.IdentityKeyKind != seedstore.EventReviewIdentityKeyKindSource || identitySeparation.EndpointB.NormalizedKey != "evidence-separation-normalized" {
		t.Fatalf("identity separation endpoint B = %#v", identitySeparation.EndpointB)
	}
	if eventSeparation.EndpointA.Kind != seedstore.EventReviewSeparationEndpointKindEvent || eventSeparation.EndpointA.EventID == nil || *eventSeparation.EndpointA.EventID != activeEventID || eventSeparation.EndpointA.EventSlug != "separation-active" {
		t.Fatalf("event separation endpoint A = %#v", eventSeparation.EndpointA)
	}
	if eventSeparation.EndpointB.Kind != seedstore.EventReviewSeparationEndpointKindEvidence || eventSeparation.EndpointB.EvidenceFingerprint != "separation-fingerprint-active" {
		t.Fatalf("event separation endpoint B = %#v", eventSeparation.EndpointB)
	}
	if canonicalSeparation.EndpointA.Kind != seedstore.EventReviewSeparationEndpointKindEvent || canonicalSeparation.EndpointA.EventID == nil || *canonicalSeparation.EndpointA.EventID != canonicalEventID {
		t.Fatalf("canonical separation endpoint A = %#v", canonicalSeparation.EndpointA)
	}
	if canonicalSeparation.EndpointA.EventSlug != "" || canonicalSeparation.EndpointA.CanonicalEventID == nil || *canonicalSeparation.EndpointA.CanonicalEventID != canonicalEventID || canonicalSeparation.EndpointA.CanonicalEventSlug != "" {
		t.Fatalf("canonical sparse separation endpoint A = %#v", canonicalSeparation.EndpointA)
	}
	if canonicalSeparation.EndpointB.Kind != seedstore.EventReviewSeparationEndpointKindEvidence || canonicalSeparation.EndpointB.EvidenceID == nil || *canonicalSeparation.EndpointB.EvidenceID != inactiveEvidenceID {
		t.Fatalf("canonical sparse separation endpoint B = %#v", canonicalSeparation.EndpointB)
	}
}

func TestEventReviewReadModelsListForImportRun(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-import-run-hall", "Event Review Import Run Hall", domain.OriginLive)

	insertLegacyEvent(t, db, "import-run-open", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "import-run-resolved", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "import-run-other", venueID, sourceID, domain.OriginLive)

	openEventID := mustEventIDBySlug(t, db, "import-run-open")
	resolvedEventID := mustEventIDBySlug(t, db, "import-run-resolved")
	otherEventID := mustEventIDBySlug(t, db, "import-run-other")

	if _, err := db.Exec(`INSERT INTO import_runs (id, started_at, finished_at, status, notes) VALUES (?, ?, ?, ?, ?)`, 41, "2026-05-15T08:00:00Z", "2026-05-15T08:05:00Z", "succeeded", "import run 41"); err != nil {
		t.Fatalf("insert import run 41: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_runs (id, started_at, finished_at, status, notes) VALUES (?, ?, ?, ?, ?)`, 42, "2026-05-15T08:10:00Z", "2026-05-15T08:15:00Z", "succeeded", "import run 42"); err != nil {
		t.Fatalf("insert import run 42: %v", err)
	}

	openClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, &openEventID, "historical_duplicate", "supporting_clean_title", time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC))
	resolvedClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, 0, &resolvedEventID, "historical_duplicate", "supporting_clean_title", time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC))
	otherClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusDiscarded), nil, 0, &otherEventID, "historical_duplicate", "supporting_clean_title", time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC))

	openEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &openEventID, "import-run-open-fingerprint", `{"payload":"open"}`)
	insertEventReviewClusterEvidenceOK(t, db, openClusterID, openEvidenceID, true, time.Date(2026, time.May, 15, 10, 0, 30, 0, time.UTC), nil, "open evidence")
	resolvedEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &resolvedEventID, "import-run-resolved-fingerprint", `{"payload":"resolved"}`)
	insertEventReviewClusterEvidenceOK(t, db, resolvedClusterID, resolvedEvidenceID, true, time.Date(2026, time.May, 15, 10, 1, 30, 0, time.UTC), nil, "resolved evidence")
	otherEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &otherEventID, "import-run-other-fingerprint", `{"payload":"other"}`)
	insertEventReviewClusterEvidenceOK(t, db, otherClusterID, otherEvidenceID, true, time.Date(2026, time.May, 15, 10, 2, 30, 0, time.UTC), nil, "other evidence")

	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, 41, openClusterID, "2026-05-15T10:00:00Z"); err != nil {
		t.Fatalf("link open cluster: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, 41, resolvedClusterID, "2026-05-15T10:01:00Z"); err != nil {
		t.Fatalf("link resolved cluster: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at) VALUES (?, ?, ?)`, 42, otherClusterID, "2026-05-15T10:02:00Z"); err != nil {
		t.Fatalf("link other cluster: %v", err)
	}

	clusters, err := st.ListEventReviewClustersForImportRun(context.Background(), 41)
	if err != nil {
		t.Fatalf("list event review clusters for import run: %v", err)
	}
	if len(clusters) != 2 {
		t.Fatalf("clusters = %d, want 2: %#v", len(clusters), clusters)
	}
	if clusters[0].ID != resolvedClusterID || clusters[1].ID != openClusterID {
		t.Fatalf("cluster order = %d, %d; want %d, %d", clusters[0].ID, clusters[1].ID, resolvedClusterID, openClusterID)
	}
	if clusters[0].Status != seedstore.EventReviewClusterStatusResolved || clusters[1].Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("cluster statuses = %#v", clusters)
	}
	for i, cluster := range clusters {
		if cluster.DisplayTitle == "" || cluster.DisplayVenueSlug != "event-review-import-run-hall" || cluster.DisplayVenueName != "Event Review Import Run Hall" {
			t.Fatalf("cluster %d display fields = %#v", i, cluster)
		}
		if cluster.DisplayStartAt == nil || !cluster.DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
			t.Fatalf("cluster %d display start = %v", i, cluster.DisplayStartAt)
		}
		if cluster.CanonicalEventID == nil {
			t.Fatalf("cluster %d canonical event id is nil", i)
		}
		if cluster.LatestImportRunID == nil || *cluster.LatestImportRunID != 41 {
			t.Fatalf("cluster %d latest import run = %v, want 41", i, cluster.LatestImportRunID)
		}
	}
	if _, err := st.ListEventReviewClustersForImportRun(context.Background(), 0); err == nil {
		t.Fatal("invalid import run id should fail")
	}
	if _, err := st.ListEventReviewClustersForImportRun(context.Background(), -1); err == nil {
		t.Fatal("negative import run id should fail")
	}
}

func TestEventReviewReadModelsClosedListOrdersAndLimits(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-history-hall", "Event Review History Hall", domain.OriginLive)

	insertLegacyEvent(t, db, "history-open", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "history-resolved", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "history-discarded", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "history-superseded", venueID, sourceID, domain.OriginLive)

	openEventID := mustEventIDBySlug(t, db, "history-open")
	resolvedEventID := mustEventIDBySlug(t, db, "history-resolved")
	discardedEventID := mustEventIDBySlug(t, db, "history-discarded")
	supersededEventID := mustEventIDBySlug(t, db, "history-superseded")

	if _, err := db.Exec(`INSERT INTO import_runs (started_at, finished_at, status, notes) VALUES (?, ?, ?, ?)`, "2026-05-15T08:00:00Z", "2026-05-15T08:05:00Z", "succeeded", "import run 1"); err != nil {
		t.Fatalf("insert import run 1: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO import_runs (started_at, finished_at, status, notes) VALUES (?, ?, ?, ?)`, "2026-05-15T08:10:00Z", "2026-05-15T08:15:00Z", "succeeded", "import run 2"); err != nil {
		t.Fatalf("insert import run 2: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO repair_runs (started_at, finished_at, status, notes) VALUES (?, ?, ?, ?)`, "2026-05-15T08:20:00Z", "2026-05-15T08:25:00Z", "succeeded", "repair run 1"); err != nil {
		t.Fatalf("insert repair run 1: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO repair_runs (started_at, finished_at, status, notes) VALUES (?, ?, ?, ?)`, "2026-05-15T08:30:00Z", "2026-05-15T08:35:00Z", "succeeded", "repair run 2"); err != nil {
		t.Fatalf("insert repair run 2: %v", err)
	}

	importRunID := mustSingleInt64(t, db, `SELECT id FROM import_runs ORDER BY id DESC LIMIT 1`)
	repairRunID := mustSingleInt64(t, db, `SELECT id FROM repair_runs ORDER BY id DESC LIMIT 1`)

	_ = insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, &openEventID, "open-type", "open-reason", time.Date(2026, time.May, 15, 8, 45, 0, 0, time.UTC))
	resolvedClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, 0, &resolvedEventID, "resolved-type", "resolved-reason", time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC))
	discardedClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusDiscarded), nil, 0, &discardedEventID, "discarded-type", "discarded-reason", time.Date(2026, time.May, 15, 9, 5, 0, 0, time.UTC))
	supersededClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusSuperseded), nil, 0, &supersededEventID, "superseded-type", "superseded-reason", time.Date(2026, time.May, 15, 9, 5, 0, 0, time.UTC), &resolvedClusterID)

	if _, err := db.Exec(`
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, resolvedClusterID, string(seedstore.EventReviewResolutionStatusResolved), `{"cluster":"resolved"}`, "", "2026-05-15T09:00:00Z", "2026-05-15T09:00:00Z"); err != nil {
		t.Fatalf("insert resolved resolution: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, discardedClusterID, string(seedstore.EventReviewResolutionStatusDiscarded), `{"cluster":"discarded"}`, "duplicate cluster", "2026-05-15T09:05:00Z", "2026-05-15T09:05:00Z"); err != nil {
		t.Fatalf("insert discarded resolution: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, supersededClusterID, string(seedstore.EventReviewResolutionStatusSuperseded), `{"cluster":"superseded"}`, "", "2026-05-15T09:05:00Z", "2026-05-15T09:05:00Z"); err != nil {
		t.Fatalf("insert superseded resolution: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, importRunID, supersededClusterID, "2026-05-15T09:06:00Z"); err != nil {
		t.Fatalf("link superseded import run: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_run_event_review_clusters (repair_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, repairRunID, supersededClusterID, "2026-05-15T09:07:00Z"); err != nil {
		t.Fatalf("link superseded repair run: %v", err)
	}

	closed, err := st.ListClosedEventReviewClusters(context.Background(), 10)
	if err != nil {
		t.Fatalf("list closed event review clusters: %v", err)
	}
	if len(closed) != 3 {
		t.Fatalf("closed cluster count = %d, want 3", len(closed))
	}
	if closed[0].ID != supersededClusterID || closed[1].ID != discardedClusterID || closed[2].ID != resolvedClusterID {
		t.Fatalf("closed cluster order = %#v", []int64{closed[0].ID, closed[1].ID, closed[2].ID})
	}
	if closed[0].Status != seedstore.EventReviewClusterStatusSuperseded || closed[1].Status != seedstore.EventReviewClusterStatusDiscarded || closed[2].Status != seedstore.EventReviewClusterStatusResolved {
		t.Fatalf("closed statuses = %#v", []seedstore.EventReviewClusterStatus{closed[0].Status, closed[1].Status, closed[2].Status})
	}
	if closed[1].DiscardReason != "duplicate cluster" {
		t.Fatalf("discard reason = %q, want duplicate cluster", closed[1].DiscardReason)
	}
	if closed[0].ResolutionID == 0 {
		t.Fatal("history resolution id is zero")
	}
	if closed[0].ResolutionCreatedAt.IsZero() {
		t.Fatal("history resolution created at is zero")
	}
	if closed[0].LatestImportRunID == nil || *closed[0].LatestImportRunID != importRunID {
		t.Fatalf("history import run id = %v, want %d", closed[0].LatestImportRunID, importRunID)
	}
	if closed[0].LatestRepairRunID == nil || *closed[0].LatestRepairRunID != repairRunID {
		t.Fatalf("history repair run id = %v, want %d", closed[0].LatestRepairRunID, repairRunID)
	}
	if closed[0].SupersededByClusterID == nil || *closed[0].SupersededByClusterID != resolvedClusterID {
		t.Fatalf("history superseded by cluster id = %v, want %d", closed[0].SupersededByClusterID, resolvedClusterID)
	}
	if closed[0].DisplayTitle != "Legacy Event" || closed[0].DisplayVenueSlug != "event-review-history-hall" || closed[0].DisplayVenueName != "Event Review History Hall" {
		t.Fatalf("history display fields = %#v", closed[0])
	}
	if closed[0].DisplayStartAt == nil || !closed[0].DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("history display start = %v", closed[0].DisplayStartAt)
	}
	if closed[0].ResolutionStatus != seedstore.EventReviewResolutionStatusSuperseded {
		t.Fatalf("resolution status = %q, want superseded", closed[0].ResolutionStatus)
	}
	if closed[0].ResolvedAt.IsZero() {
		t.Fatal("resolved at is zero")
	}
	if closed[0].CanonicalEventID == nil || *closed[0].CanonicalEventID != supersededEventID {
		t.Fatalf("canonical event id = %v, want %d", closed[0].CanonicalEventID, supersededEventID)
	}
	if closed[0].CanonicalEventSlug != "history-superseded" {
		t.Fatalf("canonical slug = %q, want history-superseded", closed[0].CanonicalEventSlug)
	}

	limited, err := st.ListClosedEventReviewClusters(context.Background(), 1)
	if err != nil {
		t.Fatalf("list closed event review clusters with limit: %v", err)
	}
	if len(limited) != 1 || limited[0].ID != supersededClusterID {
		t.Fatalf("limited closed clusters = %#v, want first terminal cluster only", limited)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 4 {
		t.Fatalf("event_review_clusters rows = %d, want 4", got)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != 3 {
		t.Fatalf("event_review_resolutions rows = %d, want 3", got)
	}
	if _, ok, err := st.LoadEventReviewCluster(context.Background(), supersededClusterID); err != nil {
		t.Fatalf("load terminal cluster detail unaffected: %v", err)
	} else if !ok {
		t.Fatal("load terminal cluster detail returned ok=false")
	}
}

func TestEventReviewReadModelsDisplayFallbacks(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-fallback-hall", "Event Review Fallback Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "fallback-one", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "fallback-two", venueID, sourceID, domain.OriginLive)

	fallbackOneID := mustEventIDBySlug(t, db, "fallback-one")
	fallbackTwoID := mustEventIDBySlug(t, db, "fallback-two")

	emptyClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "fallback-empty", "fallback-empty-reason", time.Date(2026, time.May, 15, 15, 0, 0, 0, time.UTC))
	emptyLoaded, ok, err := st.LoadEventReviewCluster(context.Background(), emptyClusterID)
	if err != nil {
		t.Fatalf("load empty cluster: %v", err)
	}
	if !ok {
		t.Fatal("load empty cluster ok=false")
	}
	if emptyLoaded.Summary.DisplayTitle != "" || emptyLoaded.Summary.DisplayVenueSlug != "" || emptyLoaded.Summary.DisplayVenueName != "" || emptyLoaded.Summary.DisplayStartAt != nil {
		t.Fatalf("empty cluster display fields = %#v, want empty", emptyLoaded.Summary)
	}

	fallbackClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "fallback-linked", "fallback-linked-reason", time.Date(2026, time.May, 15, 16, 0, 0, 0, time.UTC))
	fallbackEvidenceOneID := insertEventReviewEvidenceOK(t, db, sourceID, &fallbackTwoID, "fallback-fingerprint-1", `{"payload":"fallback two"}`)
	fallbackEvidenceTwoID := insertEventReviewEvidenceOK(t, db, sourceID, &fallbackOneID, "fallback-fingerprint-2", `{"payload":"fallback one"}`)
	insertEventReviewClusterEvidenceOK(t, db, fallbackClusterID, fallbackEvidenceOneID, true, time.Date(2026, time.May, 15, 16, 5, 0, 0, time.UTC), nil, "fallback evidence one")
	insertEventReviewClusterEvidenceOK(t, db, fallbackClusterID, fallbackEvidenceTwoID, true, time.Date(2026, time.May, 15, 16, 6, 0, 0, time.UTC), nil, "fallback evidence two")

	loaded, ok, err := st.LoadEventReviewCluster(context.Background(), fallbackClusterID)
	if err != nil {
		t.Fatalf("load fallback cluster: %v", err)
	}
	if !ok {
		t.Fatal("load fallback cluster ok=false")
	}
	if loaded.Summary.DisplayTitle != "Legacy Event" || loaded.Summary.DisplayVenueSlug != "event-review-fallback-hall" || loaded.Summary.DisplayVenueName != "Event Review Fallback Hall" {
		t.Fatalf("fallback display fields = %#v", loaded.Summary)
	}
	if loaded.Summary.DisplayStartAt == nil || !loaded.Summary.DisplayStartAt.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("fallback display start = %v", loaded.Summary.DisplayStartAt)
	}

	listed, err := st.ListOpenEventReviewClusters(context.Background())
	if err != nil {
		t.Fatalf("list open clusters: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed cluster count = %d, want 2", len(listed))
	}
	if listed[0].ID != fallbackClusterID {
		t.Fatalf("listed first cluster id = %d, want %d", listed[0].ID, fallbackClusterID)
	}
	if listed[0].DisplayTitle != "Legacy Event" {
		t.Fatalf("listed first cluster display title = %q, want Legacy Event", listed[0].DisplayTitle)
	}
}

func TestEventReviewReadModelsResolutionSnapshots(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "event-review-resolution-hall", "Event Review Resolution Hall", domain.OriginLive)
	insertLegacyEvent(t, db, "resolution-canonical", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "resolution-loser", venueID, sourceID, domain.OriginLive)

	canonicalEventID := mustEventIDBySlug(t, db, "resolution-canonical")
	loserEventID := mustEventIDBySlug(t, db, "resolution-loser")

	openClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "open-type", "open-reason", time.Date(2026, time.May, 15, 17, 0, 0, 0, time.UTC))
	openDetail, ok, err := st.LoadEventReviewCluster(context.Background(), openClusterID)
	if err != nil {
		t.Fatalf("load open cluster: %v", err)
	}
	if !ok {
		t.Fatal("open cluster load returned ok=false")
	}
	if openDetail.Resolution != nil {
		t.Fatalf("open cluster resolution = %#v, want nil", openDetail.Resolution)
	}

	malformedClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, 0, &canonicalEventID, "historical_duplicate", "malformed-reason", time.Date(2026, time.May, 15, 17, 10, 0, 0, time.UTC))
	insertEventReviewResolutionOK(t, db, malformedClusterID, seedstore.EventReviewResolutionStatusResolved, `{bad snapshot`, "")
	malformedDetail, ok, err := st.LoadEventReviewCluster(context.Background(), malformedClusterID)
	if err != nil {
		t.Fatalf("load malformed cluster: %v", err)
	}
	if !ok {
		t.Fatal("malformed cluster load returned ok=false")
	}
	if malformedDetail.Resolution == nil {
		t.Fatal("malformed cluster resolution is nil")
	}
	if malformedDetail.Resolution.SnapshotRaw != `{bad snapshot` {
		t.Fatalf("malformed snapshot raw = %q", malformedDetail.Resolution.SnapshotRaw)
	}
	if malformedDetail.Resolution.SnapshotParseWarning == "" {
		t.Fatal("malformed snapshot parse warning is empty")
	}
	if malformedDetail.Resolution.RepairRunID != nil || len(malformedDetail.Resolution.AppliedLiveActions) != 0 {
		t.Fatalf("malformed resolution parsed fields = %#v", malformedDetail.Resolution)
	}

	sparseClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusDiscarded), nil, 0, &canonicalEventID, "historical_duplicate", "sparse-reason", time.Date(2026, time.May, 15, 17, 20, 0, 0, time.UTC))
	sparseSnapshot, err := json.Marshal(map[string]any{
		"cluster_id":       sparseClusterID,
		"expected_version": 1,
		"current_version":  1,
		"current_status":   "open",
		"target_status":    "discarded",
		"recorded_at":      "2026-05-15T17:20:30Z",
	})
	if err != nil {
		t.Fatalf("marshal sparse snapshot: %v", err)
	}
	insertEventReviewResolutionOK(t, db, sparseClusterID, seedstore.EventReviewResolutionStatusDiscarded, string(sparseSnapshot), "duplicate event")
	sparseDetail, ok, err := st.LoadEventReviewCluster(context.Background(), sparseClusterID)
	if err != nil {
		t.Fatalf("load sparse cluster: %v", err)
	}
	if !ok {
		t.Fatal("sparse cluster load returned ok=false")
	}
	if sparseDetail.Resolution == nil {
		t.Fatal("sparse cluster resolution is nil")
	}
	if sparseDetail.Resolution.SnapshotParseWarning != "" {
		t.Fatalf("sparse snapshot parse warning = %q, want empty", sparseDetail.Resolution.SnapshotParseWarning)
	}
	if sparseDetail.Resolution.RepairRunID != nil || sparseDetail.Resolution.SupersededByClusterID != nil || len(sparseDetail.Resolution.AppliedLiveActions) != 0 {
		t.Fatalf("sparse resolution parsed fields = %#v", sparseDetail.Resolution)
	}
	if sparseDetail.Resolution.DiscardReason != "duplicate event" {
		t.Fatalf("sparse resolution discard reason = %q, want duplicate event", sparseDetail.Resolution.DiscardReason)
	}

	supersedingClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, 0, nil, "superseding-type", "superseding-reason", time.Date(2026, time.May, 15, 17, 30, 0, 0, time.UTC))
	supersededClusterID := insertEventReviewClusterAt(t, db, string(seedstore.EventReviewClusterStatusSuperseded), nil, 0, &loserEventID, "historical_duplicate", "superseded-reason", time.Date(2026, time.May, 15, 17, 35, 0, 0, time.UTC), &supersedingClusterID)
	insertEventReviewResolutionOK(t, db, supersededClusterID, seedstore.EventReviewResolutionStatusSuperseded, `{"cluster_id":1,"expected_version":1,"current_version":1,"current_status":"open","target_status":"superseded","recorded_at":"2026-05-15T17:35:30Z"}`, "")
	supersededDetail, ok, err := st.LoadEventReviewCluster(context.Background(), supersededClusterID)
	if err != nil {
		t.Fatalf("load superseded cluster: %v", err)
	}
	if !ok {
		t.Fatal("superseded cluster load returned ok=false")
	}
	if supersededDetail.Resolution == nil {
		t.Fatal("superseded cluster resolution is nil")
	}
	if supersededDetail.Resolution.SupersededByClusterID == nil || *supersededDetail.Resolution.SupersededByClusterID != supersedingClusterID {
		t.Fatalf("superseded resolution superseded-by = %v, want %d", supersededDetail.Resolution.SupersededByClusterID, supersedingClusterID)
	}
}

func insertEventReviewClusterAt(t *testing.T, db *sql.DB, status string, stagingKey *string, stagingKeyVersion int, canonicalEventID *int64, conflictType, conflictReason string, when time.Time, supersededByClusterID ...*int64) int64 {
	t.Helper()

	var stagingKeyValue any
	if stagingKey != nil {
		stagingKeyValue = *stagingKey
	}
	var canonicalValue any
	if canonicalEventID != nil {
		canonicalValue = *canonicalEventID
	}
	var supersededValue any
	if len(supersededByClusterID) > 0 && supersededByClusterID[0] != nil {
		supersededValue = *supersededByClusterID[0]
	}
	res, err := db.Exec(`
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
		) VALUES (?, 1, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
	`, status, stagingKeyValue, stagingKeyVersion, supersededValue, canonicalValue, conflictType, conflictReason, formatRFC3339UTC(when), formatRFC3339UTC(when))
	if err != nil {
		t.Fatalf("insert event review cluster: %v", err)
	}
	clusterID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("cluster last insert id: %v", err)
	}
	return clusterID
}

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func mustImportReadinessIdentityRow(t *testing.T, rows []seedstore.EventReviewImportIdentityRow, fieldName string) seedstore.EventReviewImportIdentityRow {
	t.Helper()

	for _, row := range rows {
		if row.FieldName == fieldName {
			return row
		}
	}
	t.Fatalf("identity row %q not found in %#v", fieldName, rows)
	return seedstore.EventReviewImportIdentityRow{}
}

func mustImportReadinessComparisonRow(t *testing.T, rows []seedstore.EventReviewImportComparisonRow, fieldName string) seedstore.EventReviewImportComparisonRow {
	t.Helper()

	for _, row := range rows {
		if row.FieldName == fieldName {
			return row
		}
	}
	t.Fatalf("comparison row %q not found in %#v", fieldName, rows)
	return seedstore.EventReviewImportComparisonRow{}
}

func mustExactIdentityMatchRow(t *testing.T, rows []seedstore.EventReviewClusterExactIdentityMatchSummary, normalizedKey string) seedstore.EventReviewClusterExactIdentityMatchSummary {
	t.Helper()

	for _, row := range rows {
		if row.NormalizedKey == normalizedKey {
			return row
		}
	}
	t.Fatalf("exact identity match %q not found in %#v", normalizedKey, rows)
	return seedstore.EventReviewClusterExactIdentityMatchSummary{}
}

func mustImportReadinessCandidateIdentityStatus(t *testing.T, statuses []seedstore.EventReviewImportCandidateIdentityStatus, evidenceID int64) seedstore.EventReviewImportCandidateIdentityStatus {
	t.Helper()

	for _, status := range statuses {
		if status.EvidenceID == evidenceID {
			return status
		}
	}
	t.Fatalf("candidate identity status for evidence %d not found in %#v", evidenceID, statuses)
	return seedstore.EventReviewImportCandidateIdentityStatus{}
}

func mustImportReadinessCandidateExactKeyStatus(t *testing.T, keys []seedstore.EventReviewImportCandidateExactIdentityStatus, normalizedKey string) seedstore.EventReviewImportCandidateExactIdentityStatus {
	t.Helper()

	for _, key := range keys {
		if key.NormalizedKey == normalizedKey {
			return key
		}
	}
	t.Fatalf("candidate exact key %q not found in %#v", normalizedKey, keys)
	return seedstore.EventReviewImportCandidateExactIdentityStatus{}
}

func mustImportReadinessCandidateSourceKeyStatus(t *testing.T, keys []seedstore.EventReviewImportCandidateSourceIdentityStatus, sourceID int64, sourceIdentityKey string) seedstore.EventReviewImportCandidateSourceIdentityStatus {
	t.Helper()

	for _, key := range keys {
		if key.SourceID == sourceID && key.SourceIdentityKey == sourceIdentityKey {
			return key
		}
	}
	t.Fatalf("candidate source key %d:%q not found in %#v", sourceID, sourceIdentityKey, keys)
	return seedstore.EventReviewImportCandidateSourceIdentityStatus{}
}

func mustEventIDBySlug(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var id int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, slug).Scan(&id); err != nil {
		t.Fatalf("lookup event %q: %v", slug, err)
	}
	return id
}

func mustSingleInt64(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()

	var id int64
	if err := db.QueryRow(query).Scan(&id); err != nil {
		t.Fatalf("query single int64: %v", err)
	}
	return id
}

func insertEventReviewCanonicalChoiceOK(t *testing.T, db *sql.DB, clusterID int64, fieldName string, choiceKind seedstore.EventReviewChoiceKind, eventID, evidenceID *int64, value string, updatedAt time.Time) int64 {
	t.Helper()

	id, err := insertEventReviewCanonicalChoice(t, db, clusterID, fieldName, choiceKind, eventID, evidenceID, value, updatedAt)
	if err != nil {
		t.Fatalf("insert canonical choice: %v", err)
	}
	return id
}

func insertEventReviewDraftChoiceOK(t *testing.T, db *sql.DB, clusterID int64, fieldName string, choiceKind seedstore.EventReviewChoiceKind, eventID, evidenceID *int64, value string, updatedAt time.Time) int64 {
	t.Helper()

	id, err := insertEventReviewDraftChoice(t, db, clusterID, fieldName, choiceKind, eventID, evidenceID, value, updatedAt)
	if err != nil {
		t.Fatalf("insert draft choice: %v", err)
	}
	return id
}
