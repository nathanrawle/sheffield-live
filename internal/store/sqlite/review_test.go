package sqlite

import (
	"context"
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
