package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

func TestRepairHistoricalDuplicateEventsDryRunDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Dry Run Duplicate",
		loserName:  "Dry Run Duplicate",
	})

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("dry-run repair: %v", err)
	}
	if !report.DryRun || report.Applied {
		t.Fatalf("dry-run/applied = %v/%v, want true/false", report.DryRun, report.Applied)
	}
	if got, want := report.Clusters, 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := report.WouldWithhold, 1; got != want {
		t.Fatalf("would_withhold = %d, want %d", got, want)
	}
	if got, want := report.Changes[0].Result, "would_withhold"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}

	state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
	if state != string(domain.PublicationStateProvisional) {
		t.Fatalf("publication state = %q, want provisional", state)
	}
	if canonicalID.Valid || strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
		t.Fatalf("dry-run mutated loser state: canonical=%v reason=%q repair_run=%v", canonicalID, withheldReason, repairRunID)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair_runs rows = %d, want 0", got)
	}
}

func TestRepairHistoricalDuplicateEventsSkipsSeparatedFalsePositiveCluster(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Separated False Positive",
		loserName:  "Separated False Positive",
	})
	if _, err := insertEventReviewSeparation(t, db,
		eventReviewEventSeparationEndpoint(fixture.targetID),
		eventReviewEventSeparationEndpoint(fixture.loserID),
		true,
		"historical duplicate keep separate",
		time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC),
		time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("insert event separation: %v", err)
	}

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("apply repair: %v", err)
	}
	if report.AutoWithheld != 0 || report.EventReviewClustersCreated != 0 {
		t.Fatalf("report auto_withheld=%d event_review_created=%d, want no mutation", report.AutoWithheld, report.EventReviewClustersCreated)
	}
	if len(report.Changes) != 1 || report.Changes[0].Result != "skipped" || report.Changes[0].Reason != "events already marked separate" {
		t.Fatalf("changes = %#v, want separated skip", report.Changes)
	}
	if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateProvisional) {
		t.Fatalf("loser state = %q, want provisional", state)
	}
}

func TestRepairHistoricalDuplicateEventsDryRunNormalizesWithheldStateAndExcludesItFromClusters(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Dry Run Normalize Target",
		loserName:  "Dry Run Normalize Target",
	})
	historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
	if _, err := db.Exec(`
		UPDATE events
		SET canonical_event_id = ?
		WHERE slug = ?
	`, fixture.targetID, fixture.loserSlug); err != nil {
		t.Fatalf("prepare normalization candidate: %v", err)
	}

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("dry-run repair: %v", err)
	}
	if got, want := report.WouldNormalizeWithheldState, 1; got != want {
		t.Fatalf("would_normalize_withheld_state = %d, want %d", got, want)
	}
	if got, want := report.Clusters, 0; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := len(report.Changes), 1; got != want {
		t.Fatalf("changes = %d, want %d", got, want)
	}
	if got, want := report.Changes[0].Result, "would_normalize_withheld_state"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got, want := report.Changes[0].CanonicalEventID, fixture.targetID; got != want {
		t.Fatalf("canonical event id = %d, want %d", got, want)
	}

	state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
	if state != string(domain.PublicationStateReviewed) {
		t.Fatalf("publication state = %q, want reviewed", state)
	}
	if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
		t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.targetID)
	}
	if strings.TrimSpace(withheldReason) != "" {
		t.Fatalf("withheld reason = %q, want empty", withheldReason)
	}
	if repairRunID.Valid {
		t.Fatalf("withheld repair run id = %#v, want NULL", repairRunID)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair_runs rows = %d, want 0", got)
	}
}

func TestRepairHistoricalDuplicateEventsAppliesCanonicalChainNormalizationAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Chain Collapse Target",
		loserName:  "Chain Collapse Target",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, fixture.loserName, "leadmill", fixture.start)

	middleID := historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
		slug:        "chain-collapse-middle",
		name:        fixture.loserName,
		description: "historical duplicate canonical chain middle",
		venueID:     lookupStoreVenueID(t, db, "leadmill"),
		sourceID:    sourceID,
		start:       fixture.start,
		end:         fixture.start.Add(2 * time.Hour),
		lastChecked: time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC),
		origin:      domain.OriginLive,
		publication: domain.PublicationStateWithheld,
	})
	if _, err := db.Exec(`
		UPDATE events
		SET canonical_event_id = ?, withheld_reason = ?
		WHERE id = ?
	`, fixture.targetID, "historical duplicate listing", middleID); err != nil {
		t.Fatalf("prepare withheld chain middle: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?, canonical_event_id = ?, withheld_reason = '', withheld_repair_run_id = NULL
		WHERE id = ?
	`, string(domain.PublicationStateReviewed), middleID, fixture.loserID); err != nil {
		t.Fatalf("prepare normalization candidate: %v", err)
	}

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("apply repair: %v", err)
	}
	if got, want := report.NormalizedWithheldState, 1; got != want {
		t.Fatalf("normalized_withheld_state = %d, want %d", got, want)
	}
	if got, want := report.Clusters, 0; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}

	state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
	if state != string(domain.PublicationStateWithheld) {
		t.Fatalf("publication state = %q, want withheld", state)
	}
	if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
		t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.targetID)
	}
	if withheldReason != "historical duplicate listing" {
		t.Fatalf("withheld reason = %q, want historical duplicate listing", withheldReason)
	}
	if !repairRunID.Valid || repairRunID.Int64 == 0 {
		t.Fatalf("withheld repair run id = %#v, want non-zero", repairRunID)
	}

	rows := mustExactIdentityRowsByEvent(t, db, fixture.loserID)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("exact identity rows = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("exact identity active = %d, want 0", rows[0].Active)
	}
	if rows[0].DeactivatedReason != "historical duplicate listing" {
		t.Fatalf("exact identity deactivated reason = %q, want historical duplicate listing", rows[0].DeactivatedReason)
	}

	aliasTargetID, aliasReason, aliasRepairRunID := mustLoadSlugAlias(t, db, fixture.loserSlug)
	if aliasTargetID != fixture.targetID {
		t.Fatalf("slug alias target id = %d, want %d", aliasTargetID, fixture.targetID)
	}
	if aliasReason != "historical duplicate listing" {
		t.Fatalf("slug alias reason = %q, want historical duplicate listing", aliasReason)
	}
	if aliasRepairRunID != repairRunID.Int64 {
		t.Fatalf("slug alias repair run id = %d, want %d", aliasRepairRunID, repairRunID.Int64)
	}

	second, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("second apply repair: %v", err)
	}
	if got, want := second.NormalizedWithheldState+second.AutoWithheld+second.EventReviewClustersCreated+second.EventReviewClustersReused+second.EventReviewClustersTerminalReused, 0; got != want {
		t.Fatalf("second apply unexpected work: %#v", second)
	}
	if got, want := mustCount(t, db, "repair_runs"), 2; got != want {
		t.Fatalf("repair_runs rows = %d, want %d", got, want)
	}
}

func TestRepairHistoricalDuplicateEventsRejectsInvalidCanonicalRefs(t *testing.T) {
	ctx := context.Background()

	t.Run("missing canonical", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Missing Canonical Target",
			loserName:  "Missing Canonical Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
			t.Fatalf("disable foreign keys: %v", err)
		}
		defer func() {
			if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
				t.Fatalf("re-enable foreign keys: %v", err)
			}
		}()
		if _, err := db.Exec(`
			UPDATE events
			SET canonical_event_id = ?
			WHERE slug = ?
		`, 999999, fixture.loserSlug); err != nil {
			t.Fatalf("prepare missing canonical: %v", err)
		}

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.Failed, 1; got != want {
			t.Fatalf("failed = %d, want %d", got, want)
		}
		if got, want := report.Clusters, 0; got != want {
			t.Fatalf("clusters = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Failure, "missing canonical") {
			t.Fatalf("failure = %q, want missing canonical", report.Changes[0].Failure)
		}

		state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateReviewed) {
			t.Fatalf("publication state = %q, want reviewed", state)
		}
		if !canonicalID.Valid || canonicalID.Int64 != 999999 {
			t.Fatalf("canonical event id = %#v, want 999999", canonicalID)
		}
		if strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
			t.Fatalf("invalid canonical ref mutated row: reason=%q repair_run=%v", withheldReason, repairRunID)
		}
	})

	t.Run("self reference", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Self Reference Target",
			loserName:  "Self Reference Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		if _, err := db.Exec(`
			UPDATE events
			SET canonical_event_id = id
			WHERE slug = ?
		`, fixture.loserSlug); err != nil {
			t.Fatalf("prepare self reference: %v", err)
		}

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.Failed, 1; got != want {
			t.Fatalf("failed = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Failure, "itself") {
			t.Fatalf("failure = %q, want self reference", report.Changes[0].Failure)
		}

		state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateReviewed) {
			t.Fatalf("publication state = %q, want reviewed", state)
		}
		if !canonicalID.Valid || canonicalID.Int64 != fixture.loserID {
			t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.loserID)
		}
		if strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
			t.Fatalf("invalid canonical ref mutated row: reason=%q repair_run=%v", withheldReason, repairRunID)
		}
	})

	t.Run("withheld final canonical", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Withheld Final Canonical Target",
			loserName:  "Withheld Final Canonical Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.targetSlug, domain.PublicationStateWithheld)
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		if _, err := db.Exec(`
			UPDATE events
			SET canonical_event_id = ?
			WHERE slug = ?
		`, fixture.targetID, fixture.loserSlug); err != nil {
			t.Fatalf("prepare withheld final canonical: %v", err)
		}

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.Failed, 1; got != want {
			t.Fatalf("failed = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Failure, "not a live public event") {
			t.Fatalf("failure = %q, want non-public final canonical", report.Changes[0].Failure)
		}

		state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateReviewed) {
			t.Fatalf("publication state = %q, want reviewed", state)
		}
		if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
			t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.targetID)
		}
		if strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
			t.Fatalf("invalid canonical ref mutated row: reason=%q repair_run=%v", withheldReason, repairRunID)
		}
	})
}

func TestRepairHistoricalDuplicateEventsMatchesUnicodeDashAndTokenBoundaries(t *testing.T) {
	ctx := context.Background()

	t.Run("unicode dash variants", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Unicode Dash Target - Part One",
			loserName:  "Unicode Dash Target — Part Two",
		})

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.AutoWithheld, 1; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if !containsString(report.Changes[0].EvidenceTiers, "title_variant_near") {
			t.Fatalf("evidence tiers = %#v, want title_variant_near", report.Changes[0].EvidenceTiers)
		}
		state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateWithheld) {
			t.Fatalf("publication state = %q, want withheld", state)
		}
	})

	t.Run("token internal plus and dash stay separate", func(t *testing.T) {
		cases := []struct {
			name   string
			target string
			loser  string
		}{
			{name: "plus", target: "C++ Night", loser: "C Night"},
			{name: "dash", target: "Post-Punk Night", loser: "Post Night"},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
				defer func() {
					if err := st.Close(); err != nil {
						t.Fatalf("close store: %v", err)
					}
				}()
				defer db.Close()

				historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
					targetName: tc.target,
					loserName:  tc.loser,
				})

				report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
				if err != nil {
					t.Fatalf("apply repair: %v", err)
				}
				if got, want := report.Clusters, 0; got != want {
					t.Fatalf("clusters = %d, want %d", got, want)
				}
				if got, want := report.AutoWithheld+report.EventReviewClustersCreated+report.EventReviewClustersReused+report.EventReviewClustersTerminalReused, 0; got != want {
					t.Fatalf("unexpected work: %#v", report)
				}
			})
		}
	})

	t.Run("plus is split before ampersand for PINS regression", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "PINS",
			loserName:  "PINS plus Gia Ford & Gelder",
		})

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].Reason, "loser has review-only evidence"; got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
		if !containsString(report.Changes[0].EvidenceTiers, "headliner_near") {
			t.Fatalf("evidence tiers = %#v, want headliner_near", report.Changes[0].EvidenceTiers)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateProvisional) {
			t.Fatalf("loser state = %q, want provisional", state)
		}
	})
}

func TestRepairHistoricalDuplicateEventsUsesAuthoritativeCanonicalSelection(t *testing.T) {
	ctx := context.Background()

	t.Run("reviewed supporting loser auto-withheld with unique authoritative canonical", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Authoritative Canonical Target",
			loserName:  "Authoritative Canonical Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "uid:authoritative-canonical", true)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.loserID, "uid:reviewed-support", false)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.AutoWithheld, 1; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "auto_withheld"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].CanonicalEventID, fixture.targetID; got != want {
			t.Fatalf("canonical event id = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].LoserEventIDs, []int64{fixture.loserID}; !equalInt64Slices(got, want) {
			t.Fatalf("loser event ids = %#v, want %#v", got, want)
		}

		state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateWithheld) {
			t.Fatalf("publication state = %q, want withheld", state)
		}
		if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
			t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.targetID)
		}
		if withheldReason != "historical duplicate listing" {
			t.Fatalf("withheld reason = %q, want historical duplicate listing", withheldReason)
		}
		if !repairRunID.Valid || repairRunID.Int64 == 0 {
			t.Fatalf("withheld repair run id = %#v, want non-zero", repairRunID)
		}

		resolvedID, found, ambiguous, err := resolveLiveEventIDBySourceIdentitiesTx(ctx, db, sourceID, ingest.SourceIdentities(sourceIdentityInputForKey("uid:reviewed-support")))
		if err != nil {
			t.Fatalf("resolve loser source identities: %v", err)
		}
		if !found || ambiguous {
			t.Fatalf("resolve loser source identities returned found=%v ambiguous=%v", found, ambiguous)
		}
		if resolvedID != fixture.targetID {
			t.Fatalf("resolved source identities = %d, want %d", resolvedID, fixture.targetID)
		}
		var linkCount int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_source_links
			WHERE event_id = ?
				AND source_id = ?
				AND source_event_key = ?
		`, fixture.loserID, sourceID, "uid:reviewed-support").Scan(&linkCount); err != nil {
			t.Fatalf("count loser source links: %v", err)
		}
		if got, want := linkCount, 1; got != want {
			t.Fatalf("loser source link count = %d, want %d", got, want)
		}
	})

	t.Run("reviewed loser with conflicting source link stages review with canonical prefilled", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Authoritative Review Block Target",
			loserName:  "Authoritative Review Block Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "uid:authoritative-review-block", true)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.loserID, "uid:reviewed-loser-support", false)
		thirdID := historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "reviewed-loser-support-other", "Reviewed Loser Support Other", fixture.start.Add(4*time.Hour))
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, thirdID, "reviewed-loser-support", false)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].CanonicalEventID, fixture.targetID; got != want {
			t.Fatalf("canonical event id = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Reason, "source identity resolves to another live event") {
			t.Fatalf("reason = %q, want source identity conflict", report.Changes[0].Reason)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateReviewed) {
			t.Fatalf("loser state = %q, want reviewed", state)
		}
	})

	t.Run("multiple authoritative candidates stage review", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Multiple Authoritative Target",
			loserName:  "Multiple Authoritative Target",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "uid:authoritative-target", true)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.loserID, "uid:authoritative-loser", true)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].Reason, "multiple authoritative targets"; got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateReviewed) {
			t.Fatalf("loser state = %q, want reviewed", state)
		}
	})

	t.Run("single authoritative canonical stays prefilled when another reviewed authoritative link is bad", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Prefilled Authoritative Canonical",
			loserName:  "Prefilled Authoritative Canonical",
		})
		historicalDuplicateRepairSetPublicationState(t, db, fixture.loserSlug, domain.PublicationStateReviewed)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "uid:prefilled-authoritative-canonical", true)
		otherID := historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "prefilled-authoritative-canonical-other", "Prefilled Authoritative Canonical Other", fixture.start.Add(4*time.Hour))
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, otherID, "url:https://example.com/events/prefilled-authoritative-canonical", false)
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.loserID, "https://example.com/events/prefilled-authoritative-canonical?utm_source=repair", true)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].CanonicalEventID, fixture.targetID; got != want {
			t.Fatalf("canonical event id = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Reason, "reviewed loser has authoritative source link") {
			t.Fatalf("reason = %q, want reviewed loser authoritative link", report.Changes[0].Reason)
		}
		stagingKey := report.Changes[0].StagingKey
		if stagingKey == "" {
			t.Fatal("staging key is empty")
		}
		cluster, ok, err := loadHistoricalDuplicateReviewClusterByStagingKeyVersion(ctx, db, stagingKey)
		if err != nil {
			t.Fatalf("load review cluster: %v", err)
		}
		if !ok {
			t.Fatal("expected event review cluster to exist")
		}
		if cluster.CanonicalEventID == nil || *cluster.CanonicalEventID != fixture.targetID {
			t.Fatalf("persisted cluster canonical id = %v, want %d", cluster.CanonicalEventID, fixture.targetID)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateReviewed) {
			t.Fatalf("loser state = %q, want reviewed", state)
		}
	})

	t.Run("ambiguous authoritative canonical identity stages review", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Ambiguous Authoritative Target",
			loserName:  "Ambiguous Authoritative Target plus Guests",
		})
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "uid:ambiguous-authoritative", true)
		thirdID := historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "ambiguous-authoritative-other", "Ambiguous Authoritative Other", fixture.start.Add(4*time.Hour))
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, thirdID, "ambiguous-authoritative", false)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if !strings.Contains(report.Changes[0].Reason, "ambiguous") {
			t.Fatalf("reason = %q, want ambiguous", report.Changes[0].Reason)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateProvisional) {
			t.Fatalf("loser state = %q, want provisional", state)
		}
	})

	t.Run("authoritative canonical identity resolving to another live event stages review", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer func() {
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}
		}()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Third Event Authoritative Target",
			loserName:  "Third Event Authoritative Target",
		})
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.targetID, "https://example.com/events/third-event-authoritative?utm_source=repair", true)
		thirdID := historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "third-event-authoritative-other", "Third Event Authoritative Other", fixture.start.Add(4*time.Hour))
		historicalDuplicateRepairInsertSourceLink(t, db, sourceID, thirdID, "url:https://example.com/events/third-event-authoritative", false)

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].CanonicalEventID, int64(0); got != want {
			t.Fatalf("canonical event id = %d, want %d", got, want)
		}
		if !strings.Contains(report.Changes[0].Reason, "authoritative source identity resolves to another live event") {
			t.Fatalf("reason = %q, want authoritative source identity conflict", report.Changes[0].Reason)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != string(domain.PublicationStateProvisional) {
			t.Fatalf("loser state = %q, want provisional", state)
		}
	})
}

func TestRepairHistoricalDuplicateEventsDryRunReportsEventReviewCreateWithoutWrites(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Dry Run Review Target",
		loserName:  "Dry Run Review Target plus Guests",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, fixture.loserName, "leadmill", fixture.start)

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("dry-run repair: %v", err)
	}
	if got, want := report.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := report.Changes[0].Result, "would_create_event_review"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair_runs rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event_review_clusters rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_candidates"); got != 0 {
		t.Fatalf("review_candidates rows = %d, want 0", got)
	}
}

func TestRepairHistoricalDuplicateEventsAppliesExactDuplicateAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Exact Duplicate Target",
		loserName:  "Exact Duplicate Target",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, "Exact Duplicate Target", "leadmill", fixture.start)

	report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("apply repair: %v", err)
	}
	if got, want := report.AutoWithheld, 1; got != want {
		t.Fatalf("auto_withheld = %d, want %d", got, want)
	}
	if got, want := report.Changes[0].Result, "auto_withheld"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got, want := report.Changes[0].CanonicalEventID, fixture.targetID; got != want {
		t.Fatalf("canonical event id = %d, want %d", got, want)
	}
	if got, want := report.Changes[0].LoserEventIDs, []int64{fixture.loserID}; !equalInt64Slices(got, want) {
		t.Fatalf("loser event ids = %#v, want %#v", got, want)
	}

	state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
	if state != string(domain.PublicationStateWithheld) {
		t.Fatalf("publication state = %q, want withheld", state)
	}
	if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
		t.Fatalf("canonical event id = %#v, want %d", canonicalID, fixture.targetID)
	}
	if withheldReason != "historical duplicate listing" {
		t.Fatalf("withheld reason = %q, want %q", withheldReason, "historical duplicate listing")
	}
	if !repairRunID.Valid || repairRunID.Int64 == 0 {
		t.Fatalf("withheld repair run id = %#v, want non-zero", repairRunID)
	}

	var aliasTargetID int64
	if err := db.QueryRow(`
		SELECT target_event_id
		FROM slug_aliases
		WHERE alias_slug = ?
			AND target_kind = ?
	`, fixture.loserSlug, "event").Scan(&aliasTargetID); err != nil {
		t.Fatalf("lookup slug alias: %v", err)
	}
	if aliasTargetID != fixture.targetID {
		t.Fatalf("slug alias target = %d, want %d", aliasTargetID, fixture.targetID)
	}

	rows := mustExactIdentityRowsByEvent(t, db, fixture.loserID)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("loser exact identity rows = %d, want %d", got, want)
	}
	if rows[0].Active != 0 {
		t.Fatalf("loser exact identity active = %d, want 0", rows[0].Active)
	}
	if rows[0].DeactivatedReason != "historical duplicate listing" {
		t.Fatalf("loser exact identity deactivated reason = %q, want %q", rows[0].DeactivatedReason, "historical duplicate listing")
	}

	second, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("second apply repair: %v", err)
	}
	if got := second.AutoWithheld + second.AlreadyWithheld + second.Skipped; got != 0 {
		t.Fatalf("second apply unexpected work: %#v", second)
	}
	if got := second.Clusters; got != 0 {
		t.Fatalf("second apply clusters = %d, want 0", got)
	}
}

func TestWithholdHistoricalDuplicateEventNormalizesAlreadyWithheldLoser(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Already Withheld Target",
		loserName:  "Already Withheld Target",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, fixture.loserName, "leadmill", fixture.start)

	repairRunID := mustInsertRepairRun(t, db)
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?, canonical_event_id = ?, withheld_reason = '', withheld_repair_run_id = NULL
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), fixture.targetID, fixture.loserID); err != nil {
		t.Fatalf("prepare already withheld loser: %v", err)
	}
	historicalDuplicateRepairInsertSourceLink(t, db, sourceID, fixture.loserID, "uid:already-withheld-loser", false)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if result, err := withholdHistoricalDuplicateEventTx(ctx, tx, fixture.loserID, fixture.targetID, repairRunID, time.Date(2026, time.May, 12, 10, 0, 0, 0, time.UTC), historicalDuplicateWithholdOptions{
		AllowReviewed:          true,
		DetachLoserSourceLinks: true,
	}); err != nil {
		t.Fatalf("idempotent withhold: %v", err)
	} else if result != "already_withheld" {
		t.Fatalf("result = %q, want already_withheld", result)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	state, canonicalID, withheldReason, withheldRepairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
	if state != string(domain.PublicationStateWithheld) {
		t.Fatalf("publication state = %q, want withheld", state)
	}
	if !canonicalID.Valid || canonicalID.Int64 != fixture.targetID {
		t.Fatalf("canonical id = %#v, want %d", canonicalID, fixture.targetID)
	}
	if withheldReason != "historical duplicate listing" {
		t.Fatalf("withheld reason = %q, want historical duplicate listing", withheldReason)
	}
	if !withheldRepairRunID.Valid || withheldRepairRunID.Int64 != repairRunID {
		t.Fatalf("withheld repair run id = %#v, want %d", withheldRepairRunID, repairRunID)
	}
	if got := mustCountExactIdentityRows(t, db, fixture.loserID); got != 0 {
		t.Fatalf("active exact identities = %d, want 0", got)
	}
	var loserSourceLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE event_id = ?
	`, fixture.loserID).Scan(&loserSourceLinkCount); err != nil {
		t.Fatalf("count loser source links: %v", err)
	}
	if loserSourceLinkCount != 0 {
		t.Fatalf("loser source links = %d, want 0", loserSourceLinkCount)
	}
	aliasTargetID, aliasReason, aliasRepairRunID := mustLoadSlugAlias(t, db, fixture.loserSlug)
	if aliasTargetID != fixture.targetID {
		t.Fatalf("alias target id = %d, want %d", aliasTargetID, fixture.targetID)
	}
	if aliasReason != "historical duplicate listing" {
		t.Fatalf("alias reason = %q, want historical duplicate listing", aliasReason)
	}
	if aliasRepairRunID != repairRunID {
		t.Fatalf("alias repair run id = %d, want %d", aliasRepairRunID, repairRunID)
	}
}

func TestRepairHistoricalDuplicateEventsCreatesFreshRepairRunPerApplyInvocation(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	firstFixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Fresh Repair Run One",
		loserName:  "Fresh Repair Run One",
	})
	firstReport, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("first apply repair: %v", err)
	}
	if got, want := firstReport.AutoWithheld, 1; got != want {
		t.Fatalf("first auto_withheld = %d, want %d", got, want)
	}
	_, _, _, firstRepairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, firstFixture.loserSlug)
	if !firstRepairRunID.Valid || firstRepairRunID.Int64 == 0 {
		t.Fatalf("first repair run id = %#v, want non-zero", firstRepairRunID)
	}

	secondFixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Fresh Repair Run Two",
		loserName:  "Fresh Repair Run Two",
	})
	secondReport, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("second apply repair: %v", err)
	}
	if got, want := secondReport.AutoWithheld, 1; got != want {
		t.Fatalf("second auto_withheld = %d, want %d", got, want)
	}
	_, _, _, secondRepairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, secondFixture.loserSlug)
	if !secondRepairRunID.Valid || secondRepairRunID.Int64 == 0 {
		t.Fatalf("second repair run id = %#v, want non-zero", secondRepairRunID)
	}
	if firstRepairRunID.Int64 == secondRepairRunID.Int64 {
		t.Fatalf("repair run ids = %d and %d, want distinct invocations", firstRepairRunID.Int64, secondRepairRunID.Int64)
	}
	if got, want := mustCount(t, db, "repair_runs"), 2; got != want {
		t.Fatalf("repair_runs rows = %d, want %d", got, want)
	}
}

func TestRepairHistoricalDuplicateEventsClassifiesNearWindowEvidence(t *testing.T) {
	ctx := context.Background()

	t.Run("clean title", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer st.Close()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName:    "Clean Title Near",
			loserName:     "Clean Title Near",
			loserStart:    time.Date(2026, time.May, 12, 19, 30, 0, 0, time.UTC),
			targetStart:   time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC),
			loserChecked:  time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC),
			targetChecked: time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC),
		})

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.AutoWithheld, 1; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if !containsString(report.Changes[0].EvidenceTiers, "clean_title_near") {
			t.Fatalf("evidence tiers = %#v, want clean_title_near", report.Changes[0].EvidenceTiers)
		}
		state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateWithheld) {
			t.Fatalf("publication state = %q, want withheld", state)
		}
	})

	t.Run("repeat show markers force review", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer st.Close()
		defer db.Close()

		venueID := lookupStoreVenueID(t, db, "leadmill")
		targetStart := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
		_ = historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
			slug:        "repeat-marker-early-show",
			name:        "Repeat Marker Night",
			description: "early show",
			venueID:     venueID,
			sourceID:    sourceID,
			start:       targetStart,
			end:         targetStart.Add(2 * time.Hour),
			lastChecked: time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC),
			origin:      domain.OriginLive,
			publication: domain.PublicationStateReviewed,
		})
		_ = historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
			slug:        "repeat-marker-late-show",
			name:        "Repeat Marker Night",
			description: "late show",
			venueID:     venueID,
			sourceID:    sourceID,
			start:       targetStart.Add(30 * time.Minute),
			end:         targetStart.Add(2*time.Hour + 30*time.Minute),
			lastChecked: time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC),
			origin:      domain.OriginLive,
			publication: domain.PublicationStateProvisional,
		})

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.AutoWithheld, 0; got != want {
			t.Fatalf("auto_withheld = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := report.Changes[0].Reason, "repeat performance marker"; got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_groups"), 0; got != want {
			t.Fatalf("review_groups rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_candidates"), 0; got != want {
			t.Fatalf("review_candidates rows = %d, want %d", got, want)
		}

		state, canonicalID, withheldReason, repairRunID := mustLoadHistoricalDuplicateEventWithholdState(t, db, "repeat-marker-late-show")
		if state != string(domain.PublicationStateProvisional) {
			t.Fatalf("publication state = %q, want provisional", state)
		}
		if canonicalID.Valid || strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
			t.Fatalf("repeat-marker loser mutated: canonical=%v reason=%q repair_run=%v", canonicalID, withheldReason, repairRunID)
		}
	})

	t.Run("outside window ignored", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer st.Close()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName:  "Outside Window",
			loserName:   "Outside Window",
			loserStart:  time.Date(2026, time.May, 12, 21, 30, 0, 0, time.UTC),
			targetStart: time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC),
		})

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.Clusters, 0; got != want {
			t.Fatalf("clusters = %d, want %d", got, want)
		}
		state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug)
		if state != string(domain.PublicationStateProvisional) {
			t.Fatalf("publication state = %q, want provisional", state)
		}
	})
}

func TestRepairHistoricalDuplicateEventsStagesAndReusesEventReviewClusters(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Review Group Target",
		loserName:  "Review Group Target plus Guests",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, fixture.loserName, "leadmill", fixture.start)

	first, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("first apply repair: %v", err)
	}
	if got, want := first.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := first.Changes[0].Result, "event_review_created"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	stagingKey := first.Changes[0].StagingKey
	if stagingKey == "" {
		t.Fatal("staging key is empty")
	}

	cluster, ok, err := loadHistoricalDuplicateReviewClusterByStagingKeyVersion(ctx, db, stagingKey)
	if err != nil {
		t.Fatalf("load event review cluster by staging key: %v", err)
	}
	if !ok {
		t.Fatal("expected event review cluster to exist")
	}
	if cluster.Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("cluster status = %q, want open", cluster.Status)
	}
	if got, want := mustCount(t, db, "review_groups"), 0; got != want {
		t.Fatalf("review_groups rows = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "review_candidates"), 0; got != want {
		t.Fatalf("review_candidates rows = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
		t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
	}
	if got, want := cluster.ConflictType, historicalDuplicateRepairConflictType; got != want {
		t.Fatalf("conflict type = %q, want %q", got, want)
	}
	if got, want := cluster.ConflictReason, first.Changes[0].Reason; got != want {
		t.Fatalf("conflict reason = %q, want %q", got, want)
	}
	if cluster.CanonicalEventID == nil || *cluster.CanonicalEventID != first.Changes[0].CanonicalEventID {
		t.Fatalf("canonical event id = %v, want %d", cluster.CanonicalEventID, first.Changes[0].CanonicalEventID)
	}
	if cluster.StagingKey == nil || *cluster.StagingKey != stagingKey {
		t.Fatalf("staging key = %v, want %q", cluster.StagingKey, stagingKey)
	}
	if got, want := cluster.StagingKeyVersion, historicalDuplicateRepairStagingKeyVersion; got != want {
		t.Fatalf("staging key version = %d, want %d", got, want)
	}
	if got, want := first.Changes[0].EventReviewClusterID, cluster.ID; got != want {
		t.Fatalf("event review cluster id = %d, want %d", got, want)
	}
	if got, want := first.Changes[0].EventReviewClusterStatus, string(cluster.Status); got != want {
		t.Fatalf("event review cluster status = %q, want %q", got, want)
	}
	detail, ok, err := st.LoadEventReviewCluster(ctx, cluster.ID)
	if err != nil {
		t.Fatalf("load event review cluster detail: %v", err)
	}
	if !ok {
		t.Fatal("expected event review cluster detail to exist")
	}
	if len(detail.Evidence) != 2 {
		t.Fatalf("cluster evidence = %d, want 2", len(detail.Evidence))
	}
	if len(detail.CanonicalChoices) != 3 {
		t.Fatalf("cluster canonical choices = %d, want 3", len(detail.CanonicalChoices))
	}
	if len(detail.DraftChoices) != 0 {
		t.Fatalf("cluster draft choices = %d, want 0", len(detail.DraftChoices))
	}
	if len(detail.LiveActions) != 2 {
		t.Fatalf("cluster live actions = %d, want 2", len(detail.LiveActions))
	}
	targetRecord, ok, err := loadEventRecordByIDTx(ctx, db, fixture.targetID)
	if err != nil || !ok {
		t.Fatalf("load target record: ok=%v err=%v", ok, err)
	}
	loserRecord, ok, err := loadEventRecordByIDTx(ctx, db, fixture.loserID)
	if err != nil || !ok {
		t.Fatalf("load loser record: ok=%v err=%v", ok, err)
	}
	targetSourceKeys := historicalDuplicateRepairSourceIdentityKeys(targetRecord.Event)
	targetExactKeys := historicalDuplicateRepairExactIdentityKeys(targetRecord.Event)
	loserSourceKeys := historicalDuplicateRepairSourceIdentityKeys(loserRecord.Event)
	loserExactKeys := historicalDuplicateRepairExactIdentityKeys(loserRecord.Event)
	evidenceByEventID := map[int64]seedstore.EventReviewClusterEvidenceSummary{}
	for _, evidence := range detail.Evidence {
		if evidence.EventID == nil {
			t.Fatalf("evidence %d missing event id", evidence.ID)
		}
		evidenceByEventID[*evidence.EventID] = evidence
	}
	targetEvidence, ok := evidenceByEventID[fixture.targetID]
	if !ok {
		t.Fatal("canonical evidence row missing")
	}
	if targetEvidence.SourceID != sourceID {
		t.Fatalf("canonical source id = %d, want %d", targetEvidence.SourceID, sourceID)
	}
	if targetEvidence.SourceName != targetRecord.Event.SourceName {
		t.Fatalf("canonical source name = %q, want %q", targetEvidence.SourceName, targetRecord.Event.SourceName)
	}
	if targetEvidence.SourceURL != targetRecord.Event.SourceURL {
		t.Fatalf("canonical source url = %q, want %q", targetEvidence.SourceURL, targetRecord.Event.SourceURL)
	}
	expectedTargetFingerprint := repairEventReviewEvidenceFingerprint(stagingKey, historicalDuplicateRepairStagingKeyVersion, seedstore.StageRepairEventReviewEvidenceInput{
		SourceID:            sourceID,
		SourceName:          targetEvidence.SourceName,
		SourceURL:           targetEvidence.SourceURL,
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EventID:             int64Ptr(fixture.targetID),
		EvidenceFingerprint: "historical-duplicate-event:" + fmt.Sprint(fixture.targetID),
		Payload:             targetEvidence.Payload,
		SourceIdentityKeys:  targetSourceKeys,
		ExactIdentityKeys:   targetExactKeys,
		WeakEvidence:        false,
	})
	if targetEvidence.EvidenceFingerprint != expectedTargetFingerprint {
		t.Fatalf("canonical fingerprint = %q, want %q", targetEvidence.EvidenceFingerprint, expectedTargetFingerprint)
	}
	var targetPayload struct {
		Role   string `json:"role"`
		Reason string `json:"reason"`
		Source struct {
			Name      string `json:"name"`
			URL       string `json:"url"`
			Authority string `json:"authority"`
		} `json:"source"`
		Event struct {
			ID          int64  `json:"id"`
			Slug        string `json:"slug"`
			Name        string `json:"name"`
			VenueSlug   string `json:"venue_slug"`
			StartAt     string `json:"start_at"`
			EndAt       string `json:"end_at"`
			SourceName  string `json:"source_name"`
			SourceURL   string `json:"source_url"`
			CalendarURL string `json:"calendar_url"`
		} `json:"event"`
		SourceIdentityKeys []string `json:"source_identity_keys"`
		ExactIdentityKeys  []string `json:"exact_identity_keys"`
		ClusterReason      string   `json:"cluster_reason"`
		ReviewState        string   `json:"review_state"`
	}
	if err := json.Unmarshal([]byte(targetEvidence.Payload), &targetPayload); err != nil {
		t.Fatalf("unmarshal canonical payload: %v", err)
	}
	if targetPayload.Role != "canonical" || targetPayload.Source.Authority != string(seedstore.SourceAuthorityAuthoritative) {
		t.Fatalf("canonical payload authority/role = %#v", targetPayload)
	}
	if targetPayload.Source.Name != targetRecord.Event.SourceName || targetPayload.Source.URL != targetRecord.Event.SourceURL {
		t.Fatalf("canonical payload source = %#v", targetPayload.Source)
	}
	if targetPayload.Event.ID != fixture.targetID || targetPayload.Event.Slug != targetRecord.Event.Slug || targetPayload.Event.Name != targetRecord.Event.Name {
		t.Fatalf("canonical payload event = %#v", targetPayload.Event)
	}
	if targetPayload.ReviewState != "reviewed" {
		t.Fatalf("canonical payload review state = %q", targetPayload.ReviewState)
	}
	if got, want := strings.Join(targetPayload.SourceIdentityKeys, ","), strings.Join(targetSourceKeys, ","); got != want {
		t.Fatalf("canonical source identity keys = %q, want %q", got, want)
	}
	if got, want := strings.Join(targetPayload.ExactIdentityKeys, ","), strings.Join(targetExactKeys, ","); got != want {
		t.Fatalf("canonical exact identity keys = %q, want %q", got, want)
	}
	loserEvidence, ok := evidenceByEventID[fixture.loserID]
	if !ok {
		t.Fatal("loser evidence row missing")
	}
	if loserEvidence.SourceID != sourceID {
		t.Fatalf("loser source id = %d, want %d", loserEvidence.SourceID, sourceID)
	}
	var loserPayload struct {
		Role   string `json:"role"`
		Source struct {
			Authority string `json:"authority"`
		} `json:"source"`
		SourceIdentityKeys []string `json:"source_identity_keys"`
		ExactIdentityKeys  []string `json:"exact_identity_keys"`
		ReviewState        string   `json:"review_state"`
	}
	if err := json.Unmarshal([]byte(loserEvidence.Payload), &loserPayload); err != nil {
		t.Fatalf("unmarshal loser payload: %v", err)
	}
	if loserPayload.Role != "loser" || loserPayload.Source.Authority != string(seedstore.SourceAuthoritySupporting) {
		t.Fatalf("loser payload authority/role = %#v", loserPayload)
	}
	if got, want := strings.Join(loserPayload.SourceIdentityKeys, ","), strings.Join(loserSourceKeys, ","); got != want {
		t.Fatalf("loser source identity keys = %q, want %q", got, want)
	}
	if got, want := strings.Join(loserPayload.ExactIdentityKeys, ","), strings.Join(loserExactKeys, ","); got != want {
		t.Fatalf("loser exact identity keys = %q, want %q", got, want)
	}
	if loserPayload.ReviewState != "provisional" {
		t.Fatalf("loser payload review state = %q", loserPayload.ReviewState)
	}

	dryRun, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("dry-run reuse repair: %v", err)
	}
	if got, want := dryRun.EventReviewClustersReused, 1; got != want {
		t.Fatalf("dry-run event review clusters reused = %d, want %d", got, want)
	}
	if got, want := dryRun.Changes[0].Result, "would_reuse_event_review"; got != want {
		t.Fatalf("dry-run result = %q, want %q", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 1; got != want {
		t.Fatalf("repair_runs rows after dry-run = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
		t.Fatalf("event_review_clusters rows after dry-run = %d, want %d", got, want)
	}

	second, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("second apply repair: %v", err)
	}
	if got, want := second.EventReviewClustersReused, 1; got != want {
		t.Fatalf("event review clusters reused = %d, want %d", got, want)
	}
	if got, want := second.Changes[0].Result, "event_review_reused"; got != want {
		t.Fatalf("second result = %q, want %q", got, want)
	}
	if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
		t.Fatalf("event_review_clusters rows after rerun = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 2; got != want {
		t.Fatalf("repair_runs rows after rerun = %d, want %d", got, want)
	}
	if second.Changes[0].EventReviewClusterID != cluster.ID {
		t.Fatalf("rerun cluster id = %d, want %d", second.Changes[0].EventReviewClusterID, cluster.ID)
	}
	if second.Changes[0].EventReviewClusterStatus != string(seedstore.EventReviewClusterStatusOpen) {
		t.Fatalf("rerun cluster status = %q, want open", second.Changes[0].EventReviewClusterStatus)
	}
}

func TestRepairHistoricalDuplicateEventsReusesTerminalEventReviewCluster(t *testing.T) {
	ctx := context.Background()
	st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	defer db.Close()

	fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
		targetName: "Terminal Review Target",
		loserName:  "Terminal Review Target plus Guests",
	})
	mustActivateExactIdentity(t, db, fixture.loserID, fixture.loserSlug, fixture.loserName, "leadmill", fixture.start)

	first, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("first apply repair: %v", err)
	}
	clusterID := first.Changes[0].EventReviewClusterID
	before, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("event review cluster not found")
	}
	if before.Summary.Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("cluster status = %q, want open", before.Summary.Status)
	}
	beforeUpdatedAt := before.Summary.UpdatedAt
	beforeEvidenceCount := len(before.Evidence)
	beforeCanonicalChoices := len(before.CanonicalChoices)
	beforeDraftChoices := len(before.DraftChoices)
	beforeLiveActions := len(before.LiveActions)

	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET status = ?
		WHERE id = ?
	`, string(seedstore.EventReviewClusterStatusResolved), clusterID); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	terminalDryRun, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("dry-run terminal reuse repair: %v", err)
	}
	if got, want := terminalDryRun.EventReviewClustersTerminalReused, 1; got != want {
		t.Fatalf("dry-run terminal reused = %d, want %d", got, want)
	}
	if got, want := terminalDryRun.Changes[0].Result, "would_reuse_terminal_event_review"; got != want {
		t.Fatalf("dry-run terminal result = %q, want %q", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 1; got != want {
		t.Fatalf("repair_runs rows after terminal dry-run = %d, want %d", got, want)
	}

	second, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
	if err != nil {
		t.Fatalf("second apply repair: %v", err)
	}
	if got, want := second.Changes[0].Result, "event_review_terminal_reused"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got, want := second.EventReviewClustersTerminalReused, 1; got != want {
		t.Fatalf("terminal reused = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 2; got != want {
		t.Fatalf("repair_runs rows = %d, want %d", got, want)
	}
	after, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("reload terminal event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("terminal event review cluster not found")
	}
	if after.Summary.Status != seedstore.EventReviewClusterStatusResolved {
		t.Fatalf("cluster status = %q, want resolved", after.Summary.Status)
	}
	if after.Summary.LatestRepairRunID == nil || *after.Summary.LatestRepairRunID != second.RepairRunID {
		t.Fatalf("latest repair run id = %v, want %d", after.Summary.LatestRepairRunID, second.RepairRunID)
	}
	if !after.Summary.UpdatedAt.Equal(beforeUpdatedAt) {
		t.Fatalf("updated_at = %s, want %s", after.Summary.UpdatedAt, beforeUpdatedAt)
	}
	if got, want := len(after.Evidence), beforeEvidenceCount; got != want {
		t.Fatalf("evidence count = %d, want %d", got, want)
	}
	if got, want := len(after.CanonicalChoices), beforeCanonicalChoices; got != want {
		t.Fatalf("canonical choices = %d, want %d", got, want)
	}
	if got, want := len(after.DraftChoices), beforeDraftChoices; got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}
	if got, want := len(after.LiveActions), beforeLiveActions; got != want {
		t.Fatalf("live actions = %d, want %d", got, want)
	}
}

func TestRepairHistoricalDuplicateEventsReviewsConflictAndVariantOnlyClusters(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name  string
		setup func(t *testing.T, db *sql.DB, st *Store, sourceID int64) historicalDuplicateRepairSeedFixture
	}{
		{
			name: "multiple reviewed targets",
			setup: func(t *testing.T, db *sql.DB, st *Store, sourceID int64) historicalDuplicateRepairSeedFixture {
				fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
					targetName: "Multiple Reviewed Targets",
					loserName:  "Multiple Reviewed Targets",
				})
				_ = historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "multiple-reviewed-targets-second", "Multiple Reviewed Targets", fixture.start.Add(15*time.Minute))
				return fixture
			},
		},
		{
			name: "repeat marker",
			setup: func(t *testing.T, db *sql.DB, st *Store, sourceID int64) historicalDuplicateRepairSeedFixture {
				fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
					targetName:   "Repeat Marker Target",
					loserName:    "Repeat Marker Target",
					loserComment: "repeat performance marker",
				})
				return fixture
			},
		},
		{
			name: "title variant near",
			setup: func(t *testing.T, db *sql.DB, st *Store, sourceID int64) historicalDuplicateRepairSeedFixture {
				fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
					targetName: "Variant Target",
					loserName:  "Variant Target (Anniversary Show)",
				})
				return fixture
			},
		},
		{
			name: "headliner near",
			setup: func(t *testing.T, db *sql.DB, st *Store, sourceID int64) historicalDuplicateRepairSeedFixture {
				fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
					targetName: "Headliner Target",
					loserName:  "Headliner Target plus Guests",
				})
				return fixture
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
			defer st.Close()
			defer db.Close()

			fixture := tc.setup(t, db, st, sourceID)
			report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
			if err != nil {
				t.Fatalf("apply repair: %v", err)
			}
			if len(report.Changes[0].EvidenceTiers) == 0 {
				t.Fatalf("evidence tiers are empty: %#v", report.Changes[0])
			}
			if tc.name == "repeat marker" && report.Changes[0].Reason != "repeat performance marker" {
				t.Fatalf("repeat marker reason = %q, want %q", report.Changes[0].Reason, "repeat performance marker")
			}
			switch tc.name {
			case "title variant near":
				if got, want := report.EventReviewClustersCreated, 0; got != want {
					t.Fatalf("event review clusters created = %d, want %d", got, want)
				}
				if got, want := report.AutoWithheld, 1; got != want {
					t.Fatalf("auto_withheld = %d, want %d", got, want)
				}
				if got, want := report.Changes[0].Result, "auto_withheld"; got != want {
					t.Fatalf("result = %q, want %q", got, want)
				}
				if !containsString(report.Changes[0].EvidenceTiers, "title_variant_near") {
					t.Fatalf("evidence tiers = %#v, want title_variant_near", report.Changes[0].EvidenceTiers)
				}
			case "headliner near":
				if got, want := report.EventReviewClustersCreated, 1; got != want {
					t.Fatalf("event review clusters created = %d, want %d", got, want)
				}
				if got, want := report.AutoWithheld, 0; got != want {
					t.Fatalf("auto_withheld = %d, want %d", got, want)
				}
				if got, want := report.Changes[0].Result, "event_review_created"; got != want {
					t.Fatalf("result = %q, want %q", got, want)
				}
				if !containsString(report.Changes[0].EvidenceTiers, "headliner_near") {
					t.Fatalf("evidence tiers = %#v, want headliner_near", report.Changes[0].EvidenceTiers)
				}
			default:
				if got, want := report.EventReviewClustersCreated, 1; got != want {
					t.Fatalf("event review clusters created = %d, want %d", got, want)
				}
				if got, want := report.Changes[0].Result, "event_review_created"; got != want {
					t.Fatalf("result = %q, want %q", got, want)
				}
			}
			if tc.name == "title variant near" {
				if got, want := mustCount(t, db, "event_review_clusters"), 0; got != want {
					t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
				}
			} else {
				if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
					t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
				}
			}
			if got, want := mustCount(t, db, "review_groups"), 0; got != want {
				t.Fatalf("review_groups rows = %d, want %d", got, want)
			}
			if got, want := mustCount(t, db, "review_candidates"), 0; got != want {
				t.Fatalf("review_candidates rows = %d, want %d", got, want)
			}
			expectedState := string(domain.PublicationStateProvisional)
			if tc.name == "title variant near" {
				expectedState = string(domain.PublicationStateWithheld)
			}
			if state, _, _, _ := mustLoadHistoricalDuplicateEventWithholdState(t, db, fixture.loserSlug); state != expectedState {
				t.Fatalf("loser state = %q, want %q", state, expectedState)
			}
		})
	}
}

func TestRepairHistoricalDuplicateEventsRejectsConflictClustersToReview(t *testing.T) {
	ctx := context.Background()

	t.Run("slug alias conflict", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer st.Close()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Alias Conflict Target",
			loserName:  "Alias Conflict Target",
		})
		other := historicalDuplicateRepairInsertReviewedEvent(t, db, sourceID, "alias-conflict-other", "Alias Conflict Other", fixture.loserStart.Add(30*time.Minute))
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
		`, fixture.loserSlug, "event", other, "historical duplicate test", "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
			t.Fatalf("insert slug alias conflict: %v", err)
		}

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_groups"), 0; got != want {
			t.Fatalf("review_groups rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_candidates"), 0; got != want {
			t.Fatalf("review_candidates rows = %d, want %d", got, want)
		}
	})

	t.Run("source link conflict", func(t *testing.T) {
		st, db, sourceID := openHistoricalDuplicateRepairFixture(t)
		defer st.Close()
		defer db.Close()

		fixture := historicalDuplicateRepairSeedPair(t, db, sourceID, historicalDuplicateRepairSeedOptions{
			targetName: "Source Conflict Target",
			loserName:  "Source Conflict Target",
		})
		if _, err := db.Exec(`
			INSERT INTO event_source_links (
				source_id,
				event_id,
				source_event_key,
				is_authoritative,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`, sourceID, fixture.loserID, "uid:source-conflict", 1, "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
			t.Fatalf("insert loser source link: %v", err)
		}

		report, err := st.RepairHistoricalDuplicateEvents(ctx, HistoricalDuplicateRepairOptions{Apply: true, NearWindow: historicalDuplicateRepairMaxWindow})
		if err != nil {
			t.Fatalf("apply repair: %v", err)
		}
		if got, want := report.EventReviewClustersCreated, 1; got != want {
			t.Fatalf("event review clusters created = %d, want %d", got, want)
		}
		if got, want := report.Changes[0].Result, "event_review_created"; got != want {
			t.Fatalf("result = %q, want %q", got, want)
		}
		if got, want := mustCount(t, db, "event_review_clusters"), 1; got != want {
			t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_groups"), 0; got != want {
			t.Fatalf("review_groups rows = %d, want %d", got, want)
		}
		if got, want := mustCount(t, db, "review_candidates"), 0; got != want {
			t.Fatalf("review_candidates rows = %d, want %d", got, want)
		}
	})
}

type historicalDuplicateRepairSeedOptions struct {
	targetName    string
	targetStart   time.Time
	targetChecked time.Time
	loserName     string
	loserStart    time.Time
	loserChecked  time.Time
	loserComment  string
}

type historicalDuplicateRepairSeedFixture struct {
	targetID   int64
	loserID    int64
	targetSlug string
	loserSlug  string
	start      time.Time
	loserStart time.Time
	loserName  string
}

func openHistoricalDuplicateRepairFixture(t *testing.T) (*Store, *sql.DB, int64) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	db := mustRawDB(t, path)
	sourceID := insertStoreTestSource(t, db)
	return st, db, sourceID
}

func historicalDuplicateRepairSeedPair(t *testing.T, db *sql.DB, sourceID int64, opts historicalDuplicateRepairSeedOptions) historicalDuplicateRepairSeedFixture {
	t.Helper()

	venueID := lookupStoreVenueID(t, db, "leadmill")
	targetStart := opts.targetStart
	if targetStart.IsZero() {
		targetStart = time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	}
	loserStart := opts.loserStart
	if loserStart.IsZero() {
		loserStart = targetStart
	}
	targetChecked := opts.targetChecked
	if targetChecked.IsZero() {
		targetChecked = time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC)
	}
	loserChecked := opts.loserChecked
	if loserChecked.IsZero() {
		loserChecked = targetChecked
	}
	targetSlug := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(opts.targetName), " ", "-")) + "-target"
	loserSlug := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(opts.loserName), " ", "-")) + "-loser"
	targetID := historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
		slug:        targetSlug,
		name:        opts.targetName,
		description: "historical duplicate target",
		venueID:     venueID,
		sourceID:    sourceID,
		start:       targetStart,
		end:         targetStart.Add(2 * time.Hour),
		lastChecked: targetChecked,
		origin:      domain.OriginLive,
		publication: domain.PublicationStateReviewed,
	})
	loserID := historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
		slug:        loserSlug,
		name:        opts.loserName,
		description: opts.loserComment,
		venueID:     venueID,
		sourceID:    sourceID,
		start:       loserStart,
		end:         loserStart.Add(2 * time.Hour),
		lastChecked: loserChecked,
		origin:      domain.OriginLive,
		publication: domain.PublicationStateProvisional,
	})
	return historicalDuplicateRepairSeedFixture{
		targetID:   targetID,
		loserID:    loserID,
		targetSlug: targetSlug,
		loserSlug:  loserSlug,
		start:      targetStart,
		loserStart: loserStart,
		loserName:  opts.loserName,
	}
}

type historicalDuplicateRepairInsertEventInput struct {
	slug        string
	name        string
	description string
	venueID     int64
	sourceID    int64
	start       time.Time
	end         time.Time
	lastChecked time.Time
	origin      domain.Origin
	publication domain.PublicationState
}

func historicalDuplicateRepairInsertEvent(t *testing.T, db *sql.DB, input historicalDuplicateRepairInsertEventInput) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, input.slug, input.venueID, input.sourceID, input.name, formatRFC3339UTC(input.start), formatRFC3339UTC(input.end), "Indie", "Listed", input.description, formatRFC3339UTC(input.lastChecked), string(input.origin), string(input.publication))
	if err != nil {
		t.Fatalf("insert event %q: %v", input.slug, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("event id %q: %v", input.slug, err)
	}
	return id
}

func historicalDuplicateRepairInsertReviewedEvent(t *testing.T, db *sql.DB, sourceID int64, slug, name string, start time.Time) int64 {
	t.Helper()

	venueID := lookupStoreVenueID(t, db, "leadmill")
	return historicalDuplicateRepairInsertEvent(t, db, historicalDuplicateRepairInsertEventInput{
		slug:        slug,
		name:        name,
		description: "historical duplicate reviewed event",
		venueID:     venueID,
		sourceID:    sourceID,
		start:       start,
		end:         start.Add(2 * time.Hour),
		lastChecked: start.Add(-24 * time.Hour),
		origin:      domain.OriginLive,
		publication: domain.PublicationStateReviewed,
	})
}

func mustLoadHistoricalDuplicateEventWithholdState(t *testing.T, db *sql.DB, slug string) (string, sql.NullInt64, string, sql.NullInt64) {
	t.Helper()

	var state string
	var canonicalID sql.NullInt64
	var reason sql.NullString
	var repairRunID sql.NullInt64
	if err := db.QueryRow(`
		SELECT publication_state, canonical_event_id, COALESCE(withheld_reason, ''), withheld_repair_run_id
		FROM events
		WHERE slug = ?
	`, slug).Scan(&state, &canonicalID, &reason, &repairRunID); err != nil {
		t.Fatalf("load event state %q: %v", slug, err)
	}
	return state, canonicalID, reason.String, repairRunID
}

func equalInt64Slices(a, b []int64) bool {
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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func historicalDuplicateRepairSetPublicationState(t *testing.T, db *sql.DB, slug string, state domain.PublicationState) {
	t.Helper()

	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?
		WHERE slug = ?
	`, string(state), slug); err != nil {
		t.Fatalf("set publication state for %q: %v", slug, err)
	}
}

func historicalDuplicateRepairInsertSourceLink(t *testing.T, db *sql.DB, sourceID, eventID int64, sourceEventKey string, authoritative bool) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id,
			event_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, sourceID, eventID, sourceEventKey, boolToInt(authoritative), "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
		t.Fatalf("insert event source link for %d/%q: %v", eventID, sourceEventKey, err)
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
