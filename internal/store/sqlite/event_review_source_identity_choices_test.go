package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	seedstore "sheffield-live/internal/store"
)

func TestSetEventReviewSourceIdentityChoicesPersistsAndUpdatesChoiceState(t *testing.T) {
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	clusterID, sourceID, sourceIdentityKey := insertEventReviewSourceIdentityChoiceFixture(t, db, string(seedstore.EventReviewClusterStatusOpen), seedstore.EventReviewConflictTypeImportReview, seedstore.EventReviewConflictReasonIngestCandidate)

	beforeEvents := mustCount(t, db, "events")
	beforeSources := mustCount(t, db, "sources")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")
	beforeEventSourceLinks := mustCount(t, db, "event_source_links")
	beforeExactIdentities := mustCount(t, db, "event_exact_identities")
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")
	beforeResolutions := mustCount(t, db, "event_review_resolutions")

	if err := st.SetEventReviewSourceIdentityChoices(context.Background(), seedstore.SetEventReviewSourceIdentityChoicesInput{
		ClusterID:       clusterID,
		ExpectedVersion: 1,
		Choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
			SourceID:          sourceID,
			SourceIdentityKey: "  " + sourceIdentityKey + "  ",
			Selected:          true,
			SelectionReason:   "preferred source identity",
		}},
	}); err != nil {
		t.Fatalf("set source identity choices: %v", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 2, nil)
	assertEventReviewSourceIdentityChoiceRow(t, db, clusterID, sourceID, sourceIdentityKey, true, "preferred source identity")
	if got := mustCount(t, db, "event_review_source_identity_choices"); got != 1 {
		t.Fatalf("event_review_source_identity_choices rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	if got := mustCount(t, db, "sources"); got != beforeSources {
		t.Fatalf("sources rows = %d, want %d", got, beforeSources)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
	if got := mustCount(t, db, "event_source_links"); got != beforeEventSourceLinks {
		t.Fatalf("event_source_links rows = %d, want %d", got, beforeEventSourceLinks)
	}
	if got := mustCount(t, db, "event_exact_identities"); got != beforeExactIdentities {
		t.Fatalf("event_exact_identities rows = %d, want %d", got, beforeExactIdentities)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != beforeObservations {
		t.Fatalf("event_source_attribute_observations rows = %d, want %d", got, beforeObservations)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}

	if err := st.SetEventReviewSourceIdentityChoices(context.Background(), seedstore.SetEventReviewSourceIdentityChoicesInput{
		ClusterID:       clusterID,
		ExpectedVersion: 2,
		Choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
			SourceID:          sourceID,
			SourceIdentityKey: sourceIdentityKey,
			Selected:          false,
			SelectionReason:   "not selected now",
		}},
	}); err != nil {
		t.Fatalf("update source identity choice: %v", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 3, nil)
	assertEventReviewSourceIdentityChoiceRow(t, db, clusterID, sourceID, sourceIdentityKey, false, "not selected now")
	if got := mustCount(t, db, "event_review_source_identity_choices"); got != 1 {
		t.Fatalf("event_review_source_identity_choices rows after update = %d, want 1", got)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows after update = %d, want %d", got, beforeEvents)
	}
	if got := mustCount(t, db, "sources"); got != beforeSources {
		t.Fatalf("sources rows after update = %d, want %d", got, beforeSources)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows after update = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows after update = %d, want %d", got, beforeReviewCandidates)
	}
	if got := mustCount(t, db, "event_source_links"); got != beforeEventSourceLinks {
		t.Fatalf("event_source_links rows after update = %d, want %d", got, beforeEventSourceLinks)
	}
	if got := mustCount(t, db, "event_exact_identities"); got != beforeExactIdentities {
		t.Fatalf("event_exact_identities rows after update = %d, want %d", got, beforeExactIdentities)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != beforeObservations {
		t.Fatalf("event_source_attribute_observations rows after update = %d, want %d", got, beforeObservations)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows after update = %d, want %d", got, beforeResolutions)
	}
}

func TestSetEventReviewSourceIdentityChoicesRejectsInvalidInputAndState(t *testing.T) {
	type testCase struct {
		name            string
		status          string
		conflictType    string
		conflictReason  string
		expectedVersion int
		choices         []seedstore.EventReviewSourceIdentityChoiceInput
		wantStatus      string
	}

	cases := []testCase{
		{
			name:            "empty choices",
			status:          string(seedstore.EventReviewClusterStatusOpen),
			conflictType:    seedstore.EventReviewConflictTypeImportReview,
			conflictReason:  seedstore.EventReviewConflictReasonIngestCandidate,
			expectedVersion: 1,
			choices:         nil,
			wantStatus:      string(seedstore.EventReviewClusterStatusOpen),
		},
		{
			name:            "stale version",
			status:          string(seedstore.EventReviewClusterStatusOpen),
			conflictType:    seedstore.EventReviewConflictTypeImportReview,
			conflictReason:  seedstore.EventReviewConflictReasonIngestCandidate,
			expectedVersion: 2,
			choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
				SourceID:          1,
				SourceIdentityKey: "source-choice-key",
				Selected:          true,
			}},
			wantStatus: string(seedstore.EventReviewClusterStatusOpen),
		},
		{
			name:            "unknown source key",
			status:          string(seedstore.EventReviewClusterStatusOpen),
			conflictType:    seedstore.EventReviewConflictTypeImportReview,
			conflictReason:  seedstore.EventReviewConflictReasonIngestCandidate,
			expectedVersion: 1,
			choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
				SourceID:          1,
				SourceIdentityKey: "missing-source-key",
				Selected:          true,
			}},
			wantStatus: string(seedstore.EventReviewClusterStatusOpen),
		},
		{
			name:            "duplicate submitted pair",
			status:          string(seedstore.EventReviewClusterStatusOpen),
			conflictType:    seedstore.EventReviewConflictTypeImportReview,
			conflictReason:  seedstore.EventReviewConflictReasonIngestCandidate,
			expectedVersion: 1,
			choices: []seedstore.EventReviewSourceIdentityChoiceInput{
				{
					SourceID:          1,
					SourceIdentityKey: "source-choice-key",
					Selected:          true,
				},
				{
					SourceID:          1,
					SourceIdentityKey: "source-choice-key",
					Selected:          false,
				},
			},
			wantStatus: string(seedstore.EventReviewClusterStatusOpen),
		},
		{
			name:            "terminal cluster",
			status:          string(seedstore.EventReviewClusterStatusResolved),
			conflictType:    seedstore.EventReviewConflictTypeImportReview,
			conflictReason:  seedstore.EventReviewConflictReasonIngestCandidate,
			expectedVersion: 1,
			choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
				SourceID:          1,
				SourceIdentityKey: "source-choice-key",
				Selected:          true,
			}},
			wantStatus: string(seedstore.EventReviewClusterStatusResolved),
		},
		{
			name:            "non-import cluster",
			status:          string(seedstore.EventReviewClusterStatusOpen),
			conflictType:    "historical_duplicate",
			conflictReason:  "reason",
			expectedVersion: 1,
			choices: []seedstore.EventReviewSourceIdentityChoiceInput{{
				SourceID:          1,
				SourceIdentityKey: "source-choice-key",
				Selected:          true,
			}},
			wantStatus: string(seedstore.EventReviewClusterStatusOpen),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, db := openEventReviewSchemaStore(t)
			defer st.Close()

			clusterID, sourceID, sourceIdentityKey := insertEventReviewSourceIdentityChoiceFixture(t, db, tc.status, tc.conflictType, tc.conflictReason)
			beforeChoices := mustCount(t, db, "event_review_source_identity_choices")
			beforeResolutions := mustCount(t, db, "event_review_resolutions")

			choices := make([]seedstore.EventReviewSourceIdentityChoiceInput, len(tc.choices))
			for i, choice := range tc.choices {
				choices[i] = choice
				choices[i].SourceID = sourceID
				if choices[i].SourceIdentityKey == "source-choice-key" {
					choices[i].SourceIdentityKey = sourceIdentityKey
				}
			}

			err := st.SetEventReviewSourceIdentityChoices(context.Background(), seedstore.SetEventReviewSourceIdentityChoicesInput{
				ClusterID:       clusterID,
				ExpectedVersion: tc.expectedVersion,
				Choices:         choices,
			})
			if err == nil {
				t.Fatalf("set source identity choices for %s: expected error", tc.name)
			}

			assertEventReviewClusterState(t, db, clusterID, tc.wantStatus, 1, nil)
			if got := mustCount(t, db, "event_review_source_identity_choices"); got != beforeChoices {
				t.Fatalf("event_review_source_identity_choices rows = %d, want %d", got, beforeChoices)
			}
			if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
				t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
			}
		})
	}
}

func assertEventReviewSourceIdentityChoiceRow(t *testing.T, db *sql.DB, clusterID, sourceID int64, sourceIdentityKey string, wantSelected bool, wantReason string) {
	t.Helper()

	var selected int
	var selectionReason string
	if err := db.QueryRow(`
		SELECT selected, selection_reason
		FROM event_review_source_identity_choices
		WHERE cluster_id = ? AND source_id = ? AND source_identity_key = ?
	`, clusterID, sourceID, sourceIdentityKey).Scan(&selected, &selectionReason); err != nil {
		t.Fatalf("load source identity choice row: %v", err)
	}
	if wantSelected && selected != 1 {
		t.Fatalf("source identity choice selected = %d, want 1", selected)
	}
	if !wantSelected && selected != 0 {
		t.Fatalf("source identity choice selected = %d, want 0", selected)
	}
	if selectionReason != wantReason {
		t.Fatalf("source identity choice reason = %q, want %q", selectionReason, wantReason)
	}
}

func insertEventReviewSourceIdentityChoiceFixture(t *testing.T, db *sql.DB, status, conflictType, conflictReason string) (int64, int64, string) {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	sourceIdentityKey := "source-choice-key"
	clusterID := insertEventReviewClusterAt(t, db, status, nil, 0, nil, conflictType, conflictReason, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC))
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "source-choice-fingerprint", `{"candidate_title":"Source Choice Candidate"}`)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, "source-choice-hash", seedstore.EventReviewIdentityKeyKindSource, sourceIdentityKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, &sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert source identity evidence key: %v", err)
	}
	insertEventReviewClusterEvidenceOK(t, db, clusterID, evidenceID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil, "active source identity evidence")
	return clusterID, sourceID, sourceIdentityKey
}
