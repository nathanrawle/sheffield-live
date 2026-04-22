package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/review"
)

func TestReviewGroupDraftRoundTripDoesNotPublishEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()
	eventCount := mustCount(t, db, "events")

	groupID, err := st.CreateReviewGroup(ctx, review.GroupInput{
		Title:      "Fixture review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:testdata/sidney.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:a.ics",
				Provenance:  "fixture UID candidate-a",
			},
			{
				ExternalID:  "candidate-b",
				Name:        "Candidate B",
				VenueSlug:   "yellow-arch",
				StartAt:     "2026-05-02T19:30:00Z",
				EndAt:       "2026-05-02T22:30:00Z",
				Genre:       "Jazz",
				Status:      "Listed",
				Description: "Second description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:b.ics",
				Provenance:  "fixture UID candidate-b",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if len(group.Candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(group.Candidates))
	}

	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[1].ID},
		{Field: review.FieldStartAt, CandidateID: group.Candidates[0].ID},
		{Field: review.FieldVenueSlug, CandidateID: group.Candidates[1].ID},
	}); err != nil {
		t.Fatalf("save review draft choices: %v", err)
	}

	group, ok, err = st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after save")
	}
	assertDraftChoice(t, group, review.FieldName, group.Candidates[1].ID, "Candidate B")
	assertDraftChoice(t, group, review.FieldStartAt, group.Candidates[0].ID, "2026-05-01T19:00:00Z")
	assertDraftChoice(t, group, review.FieldVenueSlug, group.Candidates[1].ID, "yellow-arch")

	groups, err := st.ListOpenReviewGroups(ctx)
	if err != nil {
		t.Fatalf("list open review groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("open review groups = %d, want 1", len(groups))
	}
	if groups[0].CandidateCount != 2 || groups[0].DraftCount != 3 {
		t.Fatalf("summary counts = candidates %d drafts %d, want 2 and 3", groups[0].CandidateCount, groups[0].DraftCount)
	}
	if got := mustCount(t, db, "events"); got != eventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, eventCount)
	}
}

func TestCreateReviewGroupDefaultsBlankCandidateSourceFieldsAndPreservesProvenance(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID, err := st.CreateReviewGroup(ctx, review.GroupInput{
		Title:      "Source defaults",
		SourceName: "Fixture ICS",
		SourceURL:  "file:defaults.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				Provenance:  "fixture UID candidate-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if group.SourceName != "Fixture ICS" {
		t.Fatalf("group source name = %q, want %q", group.SourceName, "Fixture ICS")
	}
	if group.SourceURL != "file:defaults.ics" {
		t.Fatalf("group source url = %q, want %q", group.SourceURL, "file:defaults.ics")
	}
	if len(group.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(group.Candidates))
	}
	candidate := group.Candidates[0]
	if candidate.SourceName != group.SourceName {
		t.Fatalf("candidate source name = %q, want %q", candidate.SourceName, group.SourceName)
	}
	if candidate.SourceURL != group.SourceURL {
		t.Fatalf("candidate source url = %q, want %q", candidate.SourceURL, group.SourceURL)
	}
	if candidate.Provenance != "fixture UID candidate-a" {
		t.Fatalf("candidate provenance = %q, want %q", candidate.Provenance, "fixture UID candidate-a")
	}
}

func TestStageReviewGroupReusesMatchingGroupAndPreservesDraftChoices(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Stage reuse",
		SourceName: "Fixture ICS",
		SourceURL:  "file:stage-reuse.ics",
		StagingKey: "v1:stage-reuse",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:a.ics",
				Provenance:  "fixture UID candidate-a",
			},
			{
				ExternalID:  "candidate-b",
				Name:        "Candidate B",
				VenueSlug:   "yellow-arch",
				StartAt:     "2026-05-02T19:30:00Z",
				EndAt:       "2026-05-02T22:30:00Z",
				Genre:       "Jazz",
				Status:      "Listed",
				Description: "Second description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:b.ics",
				Provenance:  "fixture UID candidate-b",
			},
		},
	}

	groupID, created, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[1].ID},
		{Field: review.FieldVenueSlug, CandidateID: group.Candidates[0].ID},
	}); err != nil {
		t.Fatalf("save review draft choices: %v", err)
	}

	changed := input
	changed.Title = "Stage reuse changed"
	changed.SourceName = "Changed source name"
	changed.SourceURL = "file:stage-reuse-changed.ics"
	changed.Candidates = append([]review.CandidateInput(nil), input.Candidates...)
	changed.Candidates[0].SourceName = "Changed candidate source A"
	changed.Candidates[0].SourceURL = "file:changed-a.ics"
	changed.Candidates[1].SourceName = "Changed candidate source B"
	changed.Candidates[1].SourceURL = "file:changed-b.ics"

	reusedID, created, err := st.StageReviewGroup(ctx, changed)
	if err != nil {
		t.Fatalf("restage review group: %v", err)
	}
	if created {
		t.Fatal("created = true, want false")
	}
	if reusedID != groupID {
		t.Fatalf("reused id = %d, want %d", reusedID, groupID)
	}

	reused, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after restage")
	}
	if reused.SourceName != input.SourceName {
		t.Fatalf("group source name = %q, want %q", reused.SourceName, input.SourceName)
	}
	if reused.SourceURL != input.SourceURL {
		t.Fatalf("group source url = %q, want %q", reused.SourceURL, input.SourceURL)
	}
	if reused.Candidates[0].SourceName != input.Candidates[0].SourceName {
		t.Fatalf("candidate 0 source name = %q, want %q", reused.Candidates[0].SourceName, input.Candidates[0].SourceName)
	}
	if reused.Candidates[0].SourceURL != input.Candidates[0].SourceURL {
		t.Fatalf("candidate 0 source url = %q, want %q", reused.Candidates[0].SourceURL, input.Candidates[0].SourceURL)
	}
	if reused.Candidates[1].SourceName != input.Candidates[1].SourceName {
		t.Fatalf("candidate 1 source name = %q, want %q", reused.Candidates[1].SourceName, input.Candidates[1].SourceName)
	}
	if reused.Candidates[1].SourceURL != input.Candidates[1].SourceURL {
		t.Fatalf("candidate 1 source url = %q, want %q", reused.Candidates[1].SourceURL, input.Candidates[1].SourceURL)
	}
	assertDraftChoice(t, reused, review.FieldName, group.Candidates[1].ID, "Candidate B")
	assertDraftChoice(t, reused, review.FieldVenueSlug, group.Candidates[0].ID, "leadmill")
}

func TestStageReviewGroupReusesClosedMatchingGroupWithoutReopening(t *testing.T) {
	cases := []struct {
		name       string
		closeGroup func(context.Context, *Store, int64, review.Group) error
		wantStatus string
	}{
		{
			name: "resolved",
			closeGroup: func(ctx context.Context, st *Store, groupID int64, group review.Group) error {
				return st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group))
			},
			wantStatus: review.StatusResolved,
		},
		{
			name: "rejected",
			closeGroup: func(ctx context.Context, st *Store, groupID int64, _ review.Group) error {
				return st.UpdateReviewGroupStatus(ctx, groupID, review.StatusRejected)
			},
			wantStatus: review.StatusRejected,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			input := review.GroupInput{
				Title:      "Closed reuse",
				SourceName: "Fixture ICS",
				SourceURL:  "file:closed-reuse.ics",
				StagingKey: "v1:closed-reuse-" + tc.name,
				Candidates: []review.CandidateInput{
					{
						ExternalID:  "candidate-a",
						Name:        "UTC Show",
						VenueSlug:   "sidney-and-matilda",
						StartAt:     "2026-05-01T19:00:00Z",
						EndAt:       "2026-05-01T22:00:00Z",
						Genre:       "Indie",
						Status:      "Listed",
						Description: "First line",
						SourceName:  "Fixture ICS",
						SourceURL:   "https://example.test/utc-show",
						Provenance:  "fixture UID candidate-a",
					},
				},
			}

			groupID, created, err := st.StageReviewGroup(ctx, input)
			if err != nil {
				t.Fatalf("stage review group: %v", err)
			}
			if !created {
				t.Fatal("created = false, want true")
			}

			group, ok, err := st.LoadReviewGroup(ctx, groupID)
			if err != nil {
				t.Fatalf("load review group: %v", err)
			}
			if !ok {
				t.Fatal("review group not found")
			}
			if err := tc.closeGroup(ctx, st, groupID, group); err != nil {
				t.Fatalf("close review group: %v", err)
			}

			reusedID, created, err := st.StageReviewGroup(ctx, input)
			if err != nil {
				t.Fatalf("restage review group: %v", err)
			}
			if created {
				t.Fatal("created = true, want false")
			}
			if reusedID != groupID {
				t.Fatalf("reused id = %d, want %d", reusedID, groupID)
			}

			reused, ok, err := st.LoadReviewGroup(ctx, groupID)
			if err != nil {
				t.Fatalf("reload review group: %v", err)
			}
			if !ok {
				t.Fatal("review group not found after restage")
			}
			if reused.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q", reused.Status, tc.wantStatus)
			}
		})
	}
}

func TestStageReviewGroupCreatesNewGroupWhenStagingKeyChanges(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	base := review.GroupInput{
		Title:      "Stage change",
		SourceName: "Fixture ICS",
		SourceURL:  "file:stage-change.ics",
		StagingKey: "v1:stage-change-a",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:a.ics",
				Provenance:  "fixture UID candidate-a",
			},
		},
	}
	changed := base
	changed.StagingKey = "v1:stage-change-b"
	changed.Candidates = append([]review.CandidateInput(nil), base.Candidates...)
	changed.Candidates[0].EndAt = "2026-05-01T23:00:00Z"

	firstID, created, err := st.StageReviewGroup(ctx, base)
	if err != nil {
		t.Fatalf("stage first group: %v", err)
	}
	if !created {
		t.Fatal("first group created = false, want true")
	}

	secondID, created, err := st.StageReviewGroup(ctx, changed)
	if err != nil {
		t.Fatalf("stage changed group: %v", err)
	}
	if !created {
		t.Fatal("changed group created = false, want true")
	}
	if secondID == firstID {
		t.Fatal("staging key change reused existing group, want new group")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	if got := mustCount(t, db, "review_groups"); got != 2 {
		t.Fatalf("review groups = %d, want 2", got)
	}
}

func TestCreateReviewGroupWithBlankStagingKeyStillCreatesNewRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Blank staging key",
		SourceName: "Fixture ICS",
		SourceURL:  "file:blank-key.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:a.ics",
				Provenance:  "fixture UID candidate-a",
			},
		},
	}

	firstID, err := st.CreateReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("create first group: %v", err)
	}
	secondID, err := st.CreateReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("create second group: %v", err)
	}
	if secondID == firstID {
		t.Fatal("blank staging key reused existing group, want new row")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	if got := mustCount(t, db, "review_groups"); got != 2 {
		t.Fatalf("review groups = %d, want 2", got)
	}
}

func TestListOpenReviewGroupsOnlyReturnsOpenGroups(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	db := mustRawDB(t, path)
	defer db.Close()
	eventCount := mustCount(t, db, "events")

	openID := mustCreateReviewGroup(t, st, "Open group", "Open candidate")
	resolvedID := mustCreatePublishableReviewGroup(t, st, "Resolved group")
	rejectedID := mustCreateReviewGroup(t, st, "Rejected group", "Rejected candidate")

	resolved, ok, err := st.LoadReviewGroup(ctx, resolvedID)
	if err != nil {
		t.Fatalf("load resolved review group: %v", err)
	}
	if !ok {
		t.Fatal("resolved review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, resolvedID, fullReviewChoices(t, resolved)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	if err := st.UpdateReviewGroupStatus(ctx, rejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject review group: %v", err)
	}

	groups, err := st.ListOpenReviewGroups(ctx)
	if err != nil {
		t.Fatalf("list open review groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("open review groups = %d, want 1", len(groups))
	}
	if groups[0].ID != openID {
		t.Fatalf("open review group ID = %d, want %d", groups[0].ID, openID)
	}
	if got := mustCount(t, db, "events"); got != eventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, eventCount+1)
	}
}

func TestListClosedReviewGroupsReturnsResolvedAndRejectedNewestFirstWithLimit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()

	openID := mustCreateReviewGroup(t, st, "Open group", "Open candidate")
	resolvedID := mustCreatePublishableReviewGroup(t, st, "Resolved group")
	resolved, ok, err := st.LoadReviewGroup(ctx, resolvedID)
	if err != nil {
		t.Fatalf("load resolved review group: %v", err)
	}
	if !ok {
		t.Fatal("resolved review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, resolvedID, fullReviewChoices(t, resolved)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	if err := setReviewGroupUpdatedAt(db, resolvedID, time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("set resolved updated_at: %v", err)
	}
	if err := setReviewGroupUpdatedAt(db, openID, time.Date(2026, time.April, 20, 13, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("set open updated_at: %v", err)
	}

	var rejectedIDs []int64
	for i := 0; i < 51; i++ {
		groupID := mustCreateReviewGroup(t, st, fmt.Sprintf("Rejected group %02d", i), "Rejected candidate")
		if err := st.UpdateReviewGroupStatus(ctx, groupID, review.StatusRejected); err != nil {
			t.Fatalf("reject review group %d: %v", i, err)
		}
		updatedAt := time.Date(2026, time.April, 20, 11, 0, 0, 0, time.UTC).Add(-time.Duration(i) * time.Minute)
		if err := setReviewGroupUpdatedAt(db, groupID, updatedAt); err != nil {
			t.Fatalf("set rejected updated_at %d: %v", i, err)
		}
		rejectedIDs = append(rejectedIDs, groupID)
	}

	groups, err := st.ListClosedReviewGroups(ctx, 50)
	if err != nil {
		t.Fatalf("list closed review groups: %v", err)
	}
	if len(groups) != 50 {
		t.Fatalf("closed review groups = %d, want 50", len(groups))
	}
	if groups[0].ID != resolvedID {
		t.Fatalf("first group ID = %d, want resolved group %d", groups[0].ID, resolvedID)
	}
	if groups[0].Status != review.StatusResolved {
		t.Fatalf("first group status = %q, want %q", groups[0].Status, review.StatusResolved)
	}
	if groups[1].ID != rejectedIDs[0] {
		t.Fatalf("second group ID = %d, want newest rejected group %d", groups[1].ID, rejectedIDs[0])
	}
	for _, group := range groups {
		if group.ID == openID {
			t.Fatal("closed history included open group")
		}
		if group.Status != review.StatusResolved && group.Status != review.StatusRejected {
			t.Fatalf("closed history included status %q", group.Status)
		}
	}
	if groups[len(groups)-1].ID == rejectedIDs[len(rejectedIDs)-1] {
		t.Fatal("closed history included oldest rejected group beyond limit")
	}
}

func TestListReviewGroupsForImportRunReturnsAllStatusesWithStrictNoteMatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	openID := mustCreateReviewGroupForImportRun(t, st, "Open import 12", "Created from manual ingest run 12 review staging.")
	resolvedID := mustCreatePublishableReviewGroupForImportRun(t, st, "Resolved import 12", "Created from import run 12 review staging.")
	rejectedID := mustCreateReviewGroupForImportRun(t, st, "Rejected import 12", "Created from manual ingest run 12 review staging.")
	_ = mustCreateReviewGroupForImportRun(t, st, "Wrong import 123", "Created from manual ingest run 123 review staging.")
	_ = mustCreateReviewGroupForImportRun(t, st, "Malformed import 12abc", "Created from manual ingest run 12abc review staging.")
	_ = mustCreateReviewGroupForImportRun(t, st, "No import", "Created from offline fixture.")

	resolved, ok, err := st.LoadReviewGroup(ctx, resolvedID)
	if err != nil {
		t.Fatalf("load resolved group: %v", err)
	}
	if !ok {
		t.Fatal("resolved group not found")
	}
	open, ok, err := st.LoadReviewGroup(ctx, openID)
	if err != nil {
		t.Fatalf("load open group: %v", err)
	}
	if !ok {
		t.Fatal("open group not found")
	}
	if err := st.SaveReviewDraftChoices(ctx, openID, []review.DraftChoiceInput{{Field: review.FieldName, CandidateID: open.Candidates[0].ID}}); err != nil {
		t.Fatalf("save open draft: %v", err)
	}
	if err := st.ResolveReviewGroup(ctx, resolvedID, fullReviewChoices(t, resolved)); err != nil {
		t.Fatalf("resolve group: %v", err)
	}
	if err := st.UpdateReviewGroupStatus(ctx, rejectedID, review.StatusRejected); err != nil {
		t.Fatalf("reject group: %v", err)
	}

	groups, err := st.ListReviewGroupsForImportRun(ctx, 12)
	if err != nil {
		t.Fatalf("list review groups for import run: %v", err)
	}
	if len(groups) != 3 {
		t.Fatalf("review groups = %d, want 3: %#v", len(groups), groups)
	}

	gotByID := make(map[int64]review.GroupSummary, len(groups))
	for _, group := range groups {
		gotByID[group.ID] = group
	}
	if gotByID[openID].Status != review.StatusOpen {
		t.Fatalf("open group status = %q, want %q", gotByID[openID].Status, review.StatusOpen)
	}
	if gotByID[resolvedID].Status != review.StatusResolved {
		t.Fatalf("resolved group status = %q, want %q", gotByID[resolvedID].Status, review.StatusResolved)
	}
	if gotByID[rejectedID].Status != review.StatusRejected {
		t.Fatalf("rejected group status = %q, want %q", gotByID[rejectedID].Status, review.StatusRejected)
	}
	if gotByID[openID].CandidateCount != 1 || gotByID[openID].DraftCount != 1 {
		t.Fatalf("open group counts = candidates %d drafts %d, want 1 and 1", gotByID[openID].CandidateCount, gotByID[openID].DraftCount)
	}
	for _, group := range groups {
		if group.Title == "Wrong import 123" || group.Title == "Malformed import 12abc" || group.Title == "No import" {
			t.Fatalf("strict import-run match included %q", group.Title)
		}
	}
}

func TestResolveReviewGroupPublishesCanonicalEvent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableReviewGroup(t, st, "Published resolve")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	publishStart := time.Now().UTC().Add(-1 * time.Second)
	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	publishEnd := time.Now().UTC().Add(1 * time.Second)

	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if final.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", final.Status, review.StatusResolved)
	}
	if got := len(final.DraftChoices); got != len(review.CanonicalFields) {
		t.Fatalf("draft choices = %d, want %d", got, len(review.CanonicalFields))
	}

	eventSlug := "live-utc-show-sidney-and-matilda-20260501190000"
	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if event.Name != "UTC Show" {
		t.Fatalf("name = %q, want %q", event.Name, "UTC Show")
	}
	if event.VenueSlug != "sidney-and-matilda" {
		t.Fatalf("venue slug = %q, want %q", event.VenueSlug, "sidney-and-matilda")
	}
	if !event.Start.Equal(time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("start = %v, want %v", event.Start, time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC))
	}
	if !event.End.Equal(time.Date(2026, time.May, 1, 22, 0, 0, 0, time.UTC)) {
		t.Fatalf("end = %v, want %v", event.End, time.Date(2026, time.May, 1, 22, 0, 0, 0, time.UTC))
	}
	if event.Genre != "Indie" {
		t.Fatalf("genre = %q, want %q", event.Genre, "Indie")
	}
	if event.Status != "Listed" {
		t.Fatalf("status = %q, want %q", event.Status, "Listed")
	}
	if event.Description != "First line" {
		t.Fatalf("description = %q, want %q", event.Description, "First line")
	}
	if event.SourceName != "Fixture ICS" {
		t.Fatalf("source name = %q, want %q", event.SourceName, "Fixture ICS")
	}
	if event.SourceURL != "https://example.test/utc-show" {
		t.Fatalf("source url = %q, want %q", event.SourceURL, "https://example.test/utc-show")
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
	}
	if event.LastChecked.IsZero() {
		t.Fatal("last checked is zero")
	}
	if event.LastChecked.Before(publishStart) || event.LastChecked.After(publishEnd) {
		t.Fatalf("last checked = %v, want between %v and %v", event.LastChecked, publishStart, publishEnd)
	}
	if got := mustCount(t, db, "events"); got != beforeCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeCount+1)
	}
}

func TestSaveReviewDraftChoicesUpsertsPerField(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreatePublishableReviewGroup(t, st, "Draft upsert")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[0].ID},
	}); err != nil {
		t.Fatalf("save first draft choice: %v", err)
	}
	before, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after first save: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after first save")
	}
	if _, ok := before.DraftChoices[review.FieldName]; !ok {
		t.Fatal("missing first draft choice")
	}

	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[1].ID},
	}); err != nil {
		t.Fatalf("save replacement draft choice: %v", err)
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after second save: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after second save")
	}
	if got := len(after.DraftChoices); got != 1 {
		t.Fatalf("draft choices = %d, want 1", got)
	}
	choice, ok := after.DraftChoices[review.FieldName]
	if !ok {
		t.Fatal("missing replacement draft choice")
	}
	if choice.CandidateID != group.Candidates[1].ID {
		t.Fatalf("draft choice candidate = %d, want %d", choice.CandidateID, group.Candidates[1].ID)
	}
	wantValue := review.CandidateValue(group.Candidates[1], review.FieldName)
	if choice.Value != wantValue {
		t.Fatalf("draft choice value = %q, want %q", choice.Value, wantValue)
	}

	groups, err := st.ListOpenReviewGroups(ctx)
	if err != nil {
		t.Fatalf("list open review groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("open review groups = %d, want 1", len(groups))
	}
	if groups[0].CandidateCount != 2 {
		t.Fatalf("candidate count = %d, want 2", groups[0].CandidateCount)
	}
	if groups[0].DraftCount != 1 {
		t.Fatalf("draft count = %d, want 1", groups[0].DraftCount)
	}

	db := mustRawDB(t, path)
	defer db.Close()
	var rowCount int
	var storedCandidateID int64
	if err := db.QueryRow(`
		SELECT COUNT(*), MAX(candidate_id)
		FROM review_draft_choices
		WHERE group_id = ? AND field = ?
	`, groupID, string(review.FieldName)).Scan(&rowCount, &storedCandidateID); err != nil {
		t.Fatalf("count stored draft choices: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("stored draft choice rows = %d, want 1", rowCount)
	}
	if storedCandidateID != group.Candidates[1].ID {
		t.Fatalf("stored draft candidate = %d, want %d", storedCandidateID, group.Candidates[1].ID)
	}
}

func TestResolveReviewGroupPublishesSingletonEventWithSourceFallback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Singleton publish")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	candidateID := group.Candidates[0].ID
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET source_name = '', source_url = ''
		WHERE id = ?
	`, candidateID); err != nil {
		t.Fatalf("blank candidate source fields: %v", err)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	eventSlug := "live-solo-show-sidney-and-matilda-20260503190000"
	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if event.SourceName != "Fixture ICS" {
		t.Fatalf("source name = %q, want %q", event.SourceName, "Fixture ICS")
	}
	if event.SourceURL != "file:sidney.ics" {
		t.Fatalf("source url = %q, want %q", event.SourceURL, "file:sidney.ics")
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
	}
	if got := mustCount(t, db, "events"); got != beforeCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeCount+1)
	}
}

func TestSaveReviewDraftRejectsCandidateFromAnotherGroup(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	firstID := mustCreateReviewGroup(t, st, "First", "First candidate")
	secondID := mustCreateReviewGroup(t, st, "Second", "Second candidate")

	second, ok, err := st.LoadReviewGroup(ctx, secondID)
	if err != nil {
		t.Fatalf("load second group: %v", err)
	}
	if !ok || len(second.Candidates) != 1 {
		t.Fatalf("second group = %#v, found %v", second, ok)
	}

	err = st.SaveReviewDraftChoices(ctx, firstID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: second.Candidates[0].ID},
	})
	if err == nil {
		t.Fatal("expected candidate from another group to be rejected")
	}
}

func TestSaveReviewDraftRejectsEmptyChoicesWithoutUpdatingGroup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateReviewGroup(t, st, "Empty draft", "Draft candidate")
	fixedUpdatedAt := time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC)
	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, formatRFC3339UTC(fixedUpdatedAt), groupID); err != nil {
		t.Fatalf("set fixed updated_at: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if err := st.SaveReviewDraftChoices(ctx, groupID, nil); err == nil {
		t.Fatal("expected empty choices to be rejected")
	}

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if !group.UpdatedAt.Equal(fixedUpdatedAt) {
		t.Fatalf("updated_at = %v, want unchanged %v", group.UpdatedAt, fixedUpdatedAt)
	}
	if len(group.DraftChoices) != 0 {
		t.Fatalf("draft choices = %d, want 0", len(group.DraftChoices))
	}
}

func TestSaveReviewDraftRejectsClosedGroup(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreatePublishableReviewGroup(t, st, "Closed draft")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[0].ID},
	}); err == nil {
		t.Fatal("expected closed group draft save to be rejected")
	}
}

func TestUpdateReviewGroupStatusRejectsClosedGroup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreatePublishableReviewGroup(t, st, "Closed status")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	db := mustRawDB(t, path)
	defer db.Close()
	beforeEventCount := mustCount(t, db, "events")

	if err := st.UpdateReviewGroupStatus(ctx, groupID, review.StatusRejected); err == nil {
		t.Fatal("expected closed group status flip to be rejected")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
}

func TestResolveReviewGroupIsAtomic(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableReviewGroup(t, st, "Atomic resolve")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	before, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found before resolve")
	}

	if err := st.ResolveReviewGroup(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[0].ID},
	}); err == nil {
		t.Fatal("expected incomplete resolve to be rejected")
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after failed resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after failed resolve")
	}
	if after.Status != before.Status {
		t.Fatalf("status = %q, want unchanged %q", after.Status, before.Status)
	}
	if len(after.DraftChoices) != 0 {
		t.Fatalf("draft choices = %d, want 0 after failed resolve", len(after.DraftChoices))
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if final.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", final.Status, review.StatusResolved)
	}
	if got := len(final.DraftChoices); got != len(review.CanonicalFields) {
		t.Fatalf("draft choices = %d, want %d", got, len(review.CanonicalFields))
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestResolveReviewGroupRollsBackWhenVenueIsMissing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableReviewGroup(t, st, "Missing venue")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if err := st.SaveReviewDraftChoices(ctx, groupID, []review.DraftChoiceInput{
		{Field: review.FieldName, CandidateID: group.Candidates[1].ID},
	}); err != nil {
		t.Fatalf("save review draft choices: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?
		WHERE id = ?
	`, "missing-venue", group.Candidates[0].ID); err != nil {
		t.Fatalf("blank venue slug: %v", err)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err == nil {
		t.Fatal("expected missing venue to reject resolve")
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after failed resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after failed resolve")
	}
	if after.Status != review.StatusOpen {
		t.Fatalf("status = %q, want %q", after.Status, review.StatusOpen)
	}
	assertDraftChoice(t, after, review.FieldName, group.Candidates[1].ID, "London Show")
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
}

func TestResolveReviewGroupUpsertsSlugConflict(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()

	groupID := mustCreatePublishableReviewGroup(t, st, "Slug conflict")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`
		SELECT id
		FROM sources
		ORDER BY id
		LIMIT 1
	`).Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	conflictSlug := "live-utc-show-sidney-and-matilda-20260501190000"
	if _, err := db.Exec(`
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
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, conflictSlug, venueID, sourceID, "Existing conflict", "2026-05-10T10:00:00Z", "2026-05-10T12:00:00Z", "Other", "Listed", "Conflict row", "2026-05-09T09:00:00Z", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert conflicting event: %v", err)
	}
	beforeEventCount := mustCount(t, db, "events")

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	event, ok := st.EventBySlug(conflictSlug)
	if !ok {
		t.Fatalf("missing published event %q", conflictSlug)
	}
	if event.Name != "UTC Show" {
		t.Fatalf("name = %q, want %q", event.Name, "UTC Show")
	}
	if event.VenueSlug != "sidney-and-matilda" {
		t.Fatalf("venue slug = %q, want %q", event.VenueSlug, "sidney-and-matilda")
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
	}
}

func TestUpdateReviewGroupStatusRejectsInvalidStatus(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateReviewGroup(t, st, "Invalid status", "Draft candidate")

	if err := st.UpdateReviewGroupStatus(ctx, groupID, "published"); err == nil {
		t.Fatal("expected invalid status to be rejected")
	}
	if err := st.UpdateReviewGroupStatus(ctx, groupID, review.StatusResolved); err == nil {
		t.Fatal("expected resolved status to require ResolveReviewGroup")
	}
}

func fullReviewChoices(t *testing.T, group review.Group) []review.DraftChoiceInput {
	t.Helper()

	choices := make([]review.DraftChoiceInput, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		choices = append(choices, review.DraftChoiceInput{
			Field:       field,
			CandidateID: group.Candidates[0].ID,
		})
	}
	return choices
}

func assertDraftChoice(t *testing.T, group review.Group, field review.Field, candidateID int64, value string) {
	t.Helper()

	choice, ok := group.DraftChoices[field]
	if !ok {
		t.Fatalf("missing draft choice for %s", field)
	}
	if choice.CandidateID != candidateID {
		t.Fatalf("%s candidate ID = %d, want %d", field, choice.CandidateID, candidateID)
	}
	if choice.Value != value {
		t.Fatalf("%s value = %q, want %q", field, choice.Value, value)
	}
	if choice.UpdatedAt.IsZero() {
		t.Fatalf("%s updated_at is zero", field)
	}
}

func setReviewGroupUpdatedAt(db *sql.DB, groupID int64, updatedAt time.Time) error {
	_, err := db.Exec(`
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, formatRFC3339UTC(updatedAt), groupID)
	return err
}

func mustCreateReviewGroup(t *testing.T, st *Store, title, candidateName string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:test.ics",
		Candidates: []review.CandidateInput{
			{
				Name:       candidateName,
				StartAt:    "2026-05-01T19:00:00Z",
				SourceName: "Fixture ICS",
				SourceURL:  "file:test.ics",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	return id
}

func mustCreateReviewGroupForImportRun(t *testing.T, st *Store, title, notes string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:test.ics",
		Notes:      notes,
		Candidates: []review.CandidateInput{
			{
				Name:       title + " candidate",
				StartAt:    "2026-05-01T19:00:00Z",
				SourceName: "Fixture ICS",
				SourceURL:  "file:test.ics",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	return id
}

func mustCreatePublishableReviewGroup(t *testing.T, st *Store, title string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:published.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "utc-1",
				Name:        "UTC Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First line",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/utc-show",
				Provenance:  "fixture UID utc-1",
			},
			{
				ExternalID:  "london-1",
				Name:        "London Show",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-02T18:30:00Z",
				EndAt:       "2026-05-02T21:30:00Z",
				Genre:       "Rock",
				Status:      "Listed",
				Description: "London description",
				SourceName:  "Fixture ICS",
				SourceURL:   "file:published.ics",
				Provenance:  "fixture UID london-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	return id
}

func mustCreatePublishableReviewGroupForImportRun(t *testing.T, st *Store, title, notes string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:published.ics",
		Notes:      notes,
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "utc-1",
				Name:        "UTC Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First line",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/utc-show",
				Provenance:  "fixture UID utc-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("create publishable review group: %v", err)
	}
	return id
}

func mustCreatePublishableSingletonReviewGroup(t *testing.T, st *Store, title string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:      title,
		SourceName: "Fixture ICS",
		SourceURL:  "file:sidney.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "solo-1",
				Name:        "Solo Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-03T19:00:00Z",
				EndAt:       "2026-05-03T22:00:00Z",
				Genre:       "Folk",
				Status:      "Listed",
				Description: "One listing",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/solo-show",
				Provenance:  "fixture UID solo-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("create review group: %v", err)
	}
	return id
}
