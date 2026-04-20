package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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
