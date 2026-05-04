package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
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

	input := review.GroupInput{
		Title:      "Fixture review",
		SourceName: "Fixture ICS",
		SourceURL:  "file:testdata/sidney.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID:       "candidate-a",
				Name:             "Candidate A",
				VenueSlug:        "leadmill",
				VenueText:        "Leadmill",
				VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
				StartAt:          "2026-05-01T19:00:00Z",
				EndAt:            "2026-05-01T22:00:00Z",
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "First description",
				SourceName:       "Fixture ICS",
				SourceURL:        "file:a.ics",
				Provenance:       "fixture UID candidate-a",
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
	if input.Candidates[0].VenueText != "Leadmill" {
		t.Fatalf("fixture venue text = %q, want %q", input.Candidates[0].VenueText, "Leadmill")
	}
	groupID, err := st.CreateReviewGroup(ctx, input)
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

func TestReviewGroupSharedVenueSummaryUsesResolvedIdentity(t *testing.T) {
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

	input := review.GroupInput{
		Title:      "Shared venue summary",
		SourceName: "Fixture ICS",
		SourceURL:  "file:shared-venue.ics",
		Candidates: []review.CandidateInput{
			{
				ExternalID: "candidate-a",
				Name:       "Candidate A",
				VenueSlug:  "leadmill-temp-a",
				VenueText:  "The Leadmill",
				StartAt:    "2026-05-01T19:00:00Z",
				SourceName: "Fixture ICS",
				SourceURL:  "file:a.ics",
				Provenance: "fixture UID candidate-a",
			},
			{
				ExternalID:       "candidate-b",
				Name:             "Candidate B",
				VenueSlug:        "leadmill-temp-b",
				VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
				StartAt:          "2026-05-01T19:00:00Z",
				SourceName:       "Fixture ICS",
				SourceURL:        "file:b.ics",
				Provenance:       "fixture UID candidate-b",
			},
		},
	}

	groupID, err := st.CreateReviewGroup(ctx, input)
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
	if got, want := group.SharedVenueSlug, "leadmill"; got != want {
		t.Fatalf("shared venue slug = %q, want %q", got, want)
	}
	if got, want := group.SharedVenueName, "The Leadmill"; got != want {
		t.Fatalf("shared venue name = %q, want %q", got, want)
	}

	summaries, err := st.ListOpenReviewGroups(ctx)
	if err != nil {
		t.Fatalf("list open review groups: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("open review groups = %d, want 1", len(summaries))
	}
	if got, want := summaries[0].ID, groupID; got != want {
		t.Fatalf("summary id = %d, want %d", got, want)
	}
	if got, want := summaries[0].SharedVenueSlug, "leadmill"; got != want {
		t.Fatalf("summary shared venue slug = %q, want %q", got, want)
	}
	if got, want := summaries[0].SharedVenueName, "The Leadmill"; got != want {
		t.Fatalf("summary shared venue name = %q, want %q", got, want)
	}
}

func TestCreateReviewGroupDefaultsBlankCandidateSourceFieldsAndPreservesProvenance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	db := mustRawDB(t, path)
	defer db.Close()

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			authoritative_source_name,
			authoritative_source_url,
			authoritative_source_event_key,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "Source defaults", "Fixture ICS", "file:defaults.ics", nullableReviewText(""), nullableReviewText(""), nullableReviewText(""), stagingKeyValue(""), review.StatusOpen, "", formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		t.Fatalf("insert review group: %v", err)
	}
	groupID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("review group last insert id: %v", err)
	}

	if err := insertReviewCandidate(ctx, tx, groupID, 1, review.CandidateInput{
		ExternalID:       "candidate-a",
		Name:             "Candidate A",
		VenueSlug:        "leadmill",
		VenueText:        "Leadmill",
		VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
		StartAt:          "2026-05-01T19:00:00Z",
		EndAt:            "2026-05-01T22:00:00Z",
		Genre:            "Indie",
		Status:           "Listed",
		Description:      "First description",
		Provenance:       "fixture UID candidate-a",
	}, "Fixture ICS", "file:defaults.ics"); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit transaction: %v", err)
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
	if candidate.VenueText != "Leadmill" {
		t.Fatalf("candidate venue text = %q, want %q", candidate.VenueText, "Leadmill")
	}
	if candidate.VenueLocationRaw != "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE" {
		t.Fatalf("candidate venue location raw = %q, want %q", candidate.VenueLocationRaw, "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE")
	}
	var venueText string
	var venueLocationRaw string
	if err := db.QueryRow(`SELECT venue_text, venue_location_raw FROM review_candidates WHERE group_id = ? ORDER BY id LIMIT 1`, groupID).Scan(&venueText, &venueLocationRaw); err != nil {
		t.Fatalf("scan review candidate venue evidence: %v", err)
	}
	if venueText != "Leadmill" {
		t.Fatalf("stored venue text = %q, want %q", venueText, "Leadmill")
	}
	if venueLocationRaw != "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE" {
		t.Fatalf("stored venue location raw = %q, want %q", venueLocationRaw, "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE")
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
				ExternalID:       "candidate-a",
				Name:             "Candidate A",
				VenueSlug:        "leadmill",
				VenueText:        "Leadmill",
				VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
				StartAt:          "2026-05-01T19:00:00Z",
				EndAt:            "2026-05-01T22:00:00Z",
				Genre:            "Indie",
				Status:           "Listed",
				Description:      "First description",
				SourceName:       "Fixture ICS",
				SourceURL:        "file:a.ics",
				Provenance:       "fixture UID candidate-a",
			},
			{
				ExternalID:       "candidate-b",
				Name:             "Candidate B",
				VenueSlug:        "yellow-arch",
				VenueText:        "Yellow Arch",
				VenueLocationRaw: "Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX",
				StartAt:          "2026-05-02T19:30:00Z",
				EndAt:            "2026-05-02T22:30:00Z",
				Genre:            "Jazz",
				Status:           "Listed",
				Description:      "Second description",
				SourceName:       "Fixture ICS",
				SourceURL:        "file:b.ics",
				Provenance:       "fixture UID candidate-b",
			},
		},
	}

	stageResult, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	groupID := stageResult.ID
	if !stageResult.Created {
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
	changed.Candidates = []review.CandidateInput{
		{
			ExternalID:       "candidate-b",
			Name:             "Candidate B",
			VenueSlug:        "yellow-arch-restaged",
			VenueText:        "Yellow Arch refreshed",
			VenueLocationRaw: "Yellow Arch refreshed, 30-36 Burton Road, Neepsend, S3 8BX",
			StartAt:          "2026-05-02T19:30:00Z",
			EndAt:            "2026-05-02T22:30:00Z",
			Genre:            "Jazz",
			Status:           "Listed",
			Description:      "Second description",
			SourceName:       "Changed candidate source B",
			SourceURL:        "file:changed-b.ics",
			Provenance:       "fixture UID candidate-b",
		},
		{
			ExternalID:       "candidate-a",
			Name:             "Candidate A",
			VenueSlug:        "leadmill-restaged",
			VenueText:        "Leadmill refreshed",
			VenueLocationRaw: "The Leadmill refreshed, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
			StartAt:          "2026-05-01T19:00:00Z",
			EndAt:            "2026-05-01T22:00:00Z",
			Genre:            "Indie",
			Status:           "Listed",
			Description:      "First description",
			SourceName:       "Changed candidate source A",
			SourceURL:        "file:changed-a.ics",
			Provenance:       "fixture UID candidate-a",
		},
	}

	reusedResult, err := st.StageReviewGroup(ctx, changed)
	if err != nil {
		t.Fatalf("restage review group: %v", err)
	}
	reusedID := reusedResult.ID
	if reusedResult.Created {
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
	if reused.Candidates[0].ID != group.Candidates[0].ID || reused.Candidates[0].Position != group.Candidates[0].Position {
		t.Fatalf("candidate 0 identity changed: got id %d position %d, want id %d position %d", reused.Candidates[0].ID, reused.Candidates[0].Position, group.Candidates[0].ID, group.Candidates[0].Position)
	}
	if reused.Candidates[1].ID != group.Candidates[1].ID || reused.Candidates[1].Position != group.Candidates[1].Position {
		t.Fatalf("candidate 1 identity changed: got id %d position %d, want id %d position %d", reused.Candidates[1].ID, reused.Candidates[1].Position, group.Candidates[1].ID, group.Candidates[1].Position)
	}
	if reused.Candidates[0].VenueText != "Leadmill refreshed" {
		t.Fatalf("candidate 0 venue text = %q, want %q", reused.Candidates[0].VenueText, "Leadmill refreshed")
	}
	if reused.Candidates[0].VenueSlug != "leadmill" {
		t.Fatalf("candidate 0 venue slug = %q, want %q", reused.Candidates[0].VenueSlug, "leadmill")
	}
	if reused.Candidates[0].VenueLocationRaw != "The Leadmill refreshed, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE" {
		t.Fatalf("candidate 0 venue location raw = %q, want %q", reused.Candidates[0].VenueLocationRaw, "The Leadmill refreshed, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE")
	}
	if reused.Candidates[1].VenueText != "Yellow Arch refreshed" {
		t.Fatalf("candidate 1 venue text = %q, want %q", reused.Candidates[1].VenueText, "Yellow Arch refreshed")
	}
	if reused.Candidates[1].VenueSlug != "yellow-arch" {
		t.Fatalf("candidate 1 venue slug = %q, want %q", reused.Candidates[1].VenueSlug, "yellow-arch")
	}
	if reused.Candidates[1].VenueLocationRaw != "Yellow Arch refreshed, 30-36 Burton Road, Neepsend, S3 8BX" {
		t.Fatalf("candidate 1 venue location raw = %q, want %q", reused.Candidates[1].VenueLocationRaw, "Yellow Arch refreshed, 30-36 Burton Road, Neepsend, S3 8BX")
	}
	assertDraftChoice(t, reused, review.FieldName, group.Candidates[1].ID, "Candidate B")
	assertDraftChoice(t, reused, review.FieldVenueSlug, group.Candidates[0].ID, "leadmill")
}

func TestStageReviewGroupRestagingOpenGroupPopulatesAndRefreshesAuthoritativeTuple(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Authoritative restage",
		SourceName: "Fixture ICS",
		SourceURL:  "file:authoritative-restage.ics",
		StagingKey: "v1:authoritative-restage",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "shared-uid",
				Name:        "UTC Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First description",
				SourceName:  "Fixture ICS",
				SourceURL:   "https://example.test/utc-show",
				Provenance:  "fixture UID shared-uid",
			},
		},
	}

	stageResult, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	groupID := stageResult.ID
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}

	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if got := group.AuthoritativeSourceEventKey; got != "" {
		t.Fatalf("initial authoritative source event key = %q, want empty", got)
	}

	populated := input
	populated.AuthoritativeSourceName = "Sidney & Matilda manual ingest"
	populated.AuthoritativeSourceURL = "https://calendar.example.test/live.ics"
	populated.AuthoritativeSourceEventKey = "shared-uid"

	reusedResult, err := st.StageReviewGroup(ctx, populated)
	if err != nil {
		t.Fatalf("restage review group with authoritative tuple: %v", err)
	}
	reusedID := reusedResult.ID
	if reusedResult.Created {
		t.Fatal("created = true, want false")
	}
	if reusedID != groupID {
		t.Fatalf("reused id = %d, want %d", reusedID, groupID)
	}

	group, ok, err = st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after populate: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after populate")
	}
	if got, want := group.AuthoritativeSourceName, populated.AuthoritativeSourceName; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceURL, populated.AuthoritativeSourceURL; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceEventKey, populated.AuthoritativeSourceEventKey; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
	if got, want := group.SourceName, input.SourceName; got != want {
		t.Fatalf("group source name = %q, want preserved %q", got, want)
	}
	if got, want := group.SourceURL, input.SourceURL; got != want {
		t.Fatalf("group source url = %q, want preserved %q", got, want)
	}

	refreshed := populated
	refreshed.AuthoritativeSourceURL = "https://calendar.example.test/live-updated.ics"
	refreshed.AuthoritativeSourceEventKey = "shared-uid-updated"

	reusedResult, err = st.StageReviewGroup(ctx, refreshed)
	if err != nil {
		t.Fatalf("restage review group with refreshed authoritative tuple: %v", err)
	}
	reusedID = reusedResult.ID
	if reusedResult.Created {
		t.Fatal("created = true, want false")
	}
	if reusedID != groupID {
		t.Fatalf("reused id = %d, want %d", reusedID, groupID)
	}

	group, ok, err = st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after refresh: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after refresh")
	}
	if got, want := group.AuthoritativeSourceURL, refreshed.AuthoritativeSourceURL; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceEventKey, refreshed.AuthoritativeSourceEventKey; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
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

			stageResult, err := st.StageReviewGroup(ctx, input)
			if err != nil {
				t.Fatalf("stage review group: %v", err)
			}
			groupID := stageResult.ID
			if !stageResult.Created {
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

			reusedResult, err := st.StageReviewGroup(ctx, input)
			if err != nil {
				t.Fatalf("restage review group: %v", err)
			}
			reusedID := reusedResult.ID
			if reusedResult.Created {
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

	firstResult, err := st.StageReviewGroup(ctx, base)
	if err != nil {
		t.Fatalf("stage first group: %v", err)
	}
	firstID := firstResult.ID
	if !firstResult.Created {
		t.Fatal("first group created = false, want true")
	}

	secondResult, err := st.StageReviewGroup(ctx, changed)
	if err != nil {
		t.Fatalf("stage changed group: %v", err)
	}
	secondID := secondResult.ID
	if !secondResult.Created {
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

func TestStageReviewGroupCanonicalExactMatchPromotesProvisionalEventToReviewed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID, err := st.EnsureSource(ctx, "Fixture ICS", "https://example.test/provisional")
	if err != nil {
		t.Fatalf("ensure source: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "yellow-arch").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}

	start := "2026-05-10T18:30:00Z"
	end := "2026-05-10T22:00:00Z"
	slug := "live-roots-night-yellow-arch-20260510183000"
	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin, publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, "Roots Night", start, end, "Indie", "Listed", "First description", "2026-05-09T12:00:00Z", string(domain.OriginLive), string(domain.PublicationStateProvisional)); err != nil {
		t.Fatalf("insert provisional event: %v", err)
	}

	stageResult, err := st.StageReviewGroup(ctx, review.GroupInput{
		Title:      "Exact canonical match",
		SourceName: "Fixture ICS",
		SourceURL:  "https://example.test/provisional",
		StagingKey: "v1:exact-canonical-match",
		Candidates: []review.CandidateInput{{
			ExternalID:  "candidate-a",
			Name:        "Roots Night",
			VenueSlug:   "yellow-arch",
			StartAt:     start,
			EndAt:       end,
			Genre:       "Indie",
			Status:      "Listed",
			Description: "First description",
			SourceName:  "Fixture ICS",
			SourceURL:   "https://example.test/provisional",
		}},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}
	if !stageResult.AutoResolved {
		t.Fatal("auto resolved = false, want true")
	}
	if got, want := stageResult.AutoResolvedResult, "canonical_exact_match"; got != want {
		t.Fatalf("auto resolved result = %q, want %q", got, want)
	}

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("missing event %q", slug)
	}
	if got, want := event.PublicationState, domain.PublicationStateReviewed; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestStageReviewGroupUnanimousDuplicateAutoResolvesWithNewProvisionalVenue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	result, err := st.StageReviewGroup(ctx, review.GroupInput{
		Title:      "Unanimous duplicate new venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:unanimous-new-venue.ics",
		StagingKey: "v1:unanimous-new-venue",
		Candidates: []review.CandidateInput{
			{
				ExternalID:       "candidate-a",
				Name:             "Unknown venue show",
				VenueSlug:        "imagniary-hal-temp",
				VenueText:        "Imaginary Hall",
				VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
				StartAt:          "2026-05-10T18:30:00Z",
				EndAt:            "2026-05-10T22:00:00Z",
				Status:           "Listed",
				Description:      "Duplicate one",
			},
			{
				ExternalID:       "candidate-b",
				Name:             "Unknown venue show",
				VenueSlug:        "imagniary-hal-temp",
				VenueText:        "Imaginary Hall",
				VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
				StartAt:          "2026-05-10T18:30:00Z",
				EndAt:            "2026-05-10T22:00:00Z",
				Status:           "Listed",
				Description:      "Duplicate one",
			},
		},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}
	if !result.AutoResolved {
		t.Fatal("auto resolved = false, want true")
	}
	if got, want := result.AutoResolvedResult, "unanimous_duplicate"; got != want {
		t.Fatalf("auto resolved result = %q, want %q", got, want)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}
	event, ok := st.EventBySlug("live-unknown-venue-show-imaginary-hall-20260510183000")
	if !ok {
		t.Fatal("published event not found")
	}
	if event.VenueSlug != "imaginary-hall" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "imaginary-hall")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestStageReviewGroupUnanimousDuplicateAutoResolvesWhenExplicitVenueSlugConflictsWithEvidence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	result, err := st.StageReviewGroup(ctx, review.GroupInput{
		Title:      "Unanimous duplicate conflicting venue evidence",
		SourceName: "Fixture ICS",
		SourceURL:  "file:unanimous-conflicting-venue.ics",
		StagingKey: "v1:unanimous-conflicting-venue",
		Candidates: []review.CandidateInput{
			{
				ExternalID:       "candidate-a",
				Name:             "Known venue show",
				VenueSlug:        "leadmill",
				VenueText:        "Sidney & Matilda",
				VenueLocationRaw: "Rivelin Works, 46 Sidney Street, Sheffield",
				StartAt:          "2026-05-10T18:30:00Z",
				EndAt:            "2026-05-10T22:00:00Z",
				Status:           "Listed",
				Description:      "Duplicate one",
			},
			{
				ExternalID:       "candidate-b",
				Name:             "Known venue show",
				VenueSlug:        "leadmill",
				VenueText:        "Sidney & Matilda",
				VenueLocationRaw: "Rivelin Works, 46 Sidney Street, Sheffield",
				StartAt:          "2026-05-10T18:30:00Z",
				EndAt:            "2026-05-10T22:00:00Z",
				Status:           "Listed",
				Description:      "Duplicate one",
			},
		},
	})
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}
	if !result.AutoResolved {
		t.Fatal("auto resolved = false, want true")
	}
	if got, want := result.AutoResolvedResult, "unanimous_duplicate"; got != want {
		t.Fatalf("auto resolved result = %q, want %q", got, want)
	}

	event, ok := st.EventBySlug("live-known-venue-show-leadmill-20260510183000")
	if !ok {
		t.Fatal("published event not found")
	}
	if event.VenueSlug != "leadmill" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "leadmill")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestStageReviewGroupCreatesProvisionalVenueImmediatelyWhenVenueIsMissing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	input := review.GroupInput{
		Title:      "Immediate provisional venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:immediate-provisional.ics",
		StagingKey: "v1:immediate-provisional",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Unknown venue show",
			VenueSlug:        "imagniary-hal-temp",
			VenueText:        "Imaginary Hall",
			VenueLocationRaw: "Imaginary Hall, 1 Void Street, Neepsend, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Should stage with a new provisional venue row.",
		}},
	}

	result, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "1 Void Street,\nNeepsend,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "1 Void Street,\nNeepsend,\nSheffield")
	}
	if venue.Neighbourhood != "Neepsend" {
		t.Fatalf("venue neighbourhood = %q, want %q", venue.Neighbourhood, "Neepsend")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}

	group, ok, err := st.LoadReviewGroup(ctx, result.ID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if got, want := group.Candidates[0].VenueSlug, "imagniary-hal-temp"; got != want {
		t.Fatalf("staged venue slug = %q, want preserved %q", got, want)
	}
}

func TestStageReviewGroupGenericICSLocationVariantsReuseProvisionalVenueSlug(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")

	first := review.GroupInput{
		Title:      "Generic ICS provisional venue one",
		SourceName: "Fixture ICS",
		SourceURL:  "file:generic-provisional-one.ics",
		StagingKey: "v1:generic-provisional-one",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Generic hall show",
			VenueSlug:        "imaginary-hall-1-void-street-sheffield",
			VenueText:        "Imaginary Hall, 1 Void Street, Sheffield",
			VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "First generic location variant.",
		}},
	}
	if _, err := st.StageReviewGroup(ctx, first); err != nil {
		t.Fatalf("stage first review group: %v", err)
	}
	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("normalized provisional venue not found after first stage")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "1 Void Street,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "1 Void Street,\nSheffield")
	}
	if _, ok := st.VenueBySlug("imaginary-hall-1-void-street-sheffield"); ok {
		t.Fatal("full-location provisional venue slug was inserted")
	}

	second := review.GroupInput{
		Title:      "Generic ICS provisional venue two",
		SourceName: "Fixture ICS",
		SourceURL:  "file:generic-provisional-two.ics",
		StagingKey: "v1:generic-provisional-two",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-b",
			Name:             "Generic hall late show",
			VenueSlug:        "imaginary-hall-1-void-street-sheffield-s1-2ja",
			VenueText:        "Imaginary Hall\n1 Void Street\nSheffield\nS1 2JA",
			VenueLocationRaw: "Imaginary Hall\n1 Void Street\nSheffield\nS1 2JA",
			StartAt:          "2026-05-11T18:30:00Z",
			EndAt:            "2026-05-11T22:00:00Z",
			Status:           "Listed",
			Description:      "Second generic location variant.",
		}},
	}
	if _, err := st.StageReviewGroup(ctx, second); err != nil {
		t.Fatalf("stage second review group: %v", err)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	venue, ok = st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("normalized provisional venue not found after second stage")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name after second stage = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "1 Void Street,\nSheffield" {
		t.Fatalf("venue address after second stage = %q, want %q", venue.Address, "1 Void Street,\nSheffield")
	}
	if _, ok := st.VenueBySlug("imaginary-hall-1-void-street-sheffield-s1-2ja"); ok {
		t.Fatal("postcode variant provisional venue slug was inserted")
	}
}

func TestStageReviewGroupCreatesProvisionalVenueFromEscapedICSLocationEvidence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Escaped ICS provisional venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:escaped-venue.ics",
		StagingKey: "v1:escaped-ics-provisional",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Escaped venue show",
			VenueSlug:        "memorial-hall-1-void-street-sheffield",
			VenueText:        "Memorial Hall, Barkers Pool, 1 Void Street, Sheffield",
			VenueLocationRaw: "Memorial Hall\\, Barkers Pool, 1 Void Street, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Should truncate escaped comma venue head.",
		}},
	}

	result, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}

	venue, ok := st.VenueBySlug("memorial-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.Name != "Memorial Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Memorial Hall")
	}
	if venue.Address != "Barkers Pool,\n1 Void Street,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "Barkers Pool,\n1 Void Street,\nSheffield")
	}
}

func TestStageReviewGroupRestagingDoesNotDuplicateOrOverwriteProvisionalVenue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Immediate provisional venue restage",
		SourceName: "Fixture ICS",
		SourceURL:  "file:immediate-provisional-restage.ics",
		StagingKey: "v1:immediate-provisional-restage",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Unknown venue show",
			VenueSlug:        "imagniary-hal-temp",
			VenueText:        "Imaginary Hall",
			VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Should stage with a new provisional venue row.",
		}},
	}

	result, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if _, ok := st.VenueBySlug("imaginary-hall"); !ok {
		t.Fatal("provisional venue not found after initial stage")
	}
	if err := st.UpdateProvisionalVenue(ctx, seedstore.VenueUpdateInput{
		Slug:          "imaginary-hall",
		Name:          "Edited Hall",
		Address:       "99 Edited Street, Sheffield",
		Neighbourhood: "Kelham",
		Description:   "Edited provisional venue",
		Website:       "https://example.test/edited-hall",
		CoverageKind:  domain.CoverageKindProgram,
		CoverageNote:  "Edited note",
	}); err != nil {
		t.Fatalf("edit provisional venue: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")

	restaged := input
	restaged.Candidates = []review.CandidateInput{{
		ExternalID:       "candidate-a",
		Name:             "Unknown venue show",
		VenueSlug:        "imagniary-hal-temp",
		VenueText:        "Imaginary Hall refreshed",
		VenueLocationRaw: "Imaginary Hall refreshed, 1 Void Street, Sheffield",
		StartAt:          "2026-05-10T18:30:00Z",
		EndAt:            "2026-05-10T22:00:00Z",
		Status:           "Listed",
		Description:      "Should stage with a new provisional venue row.",
	}}

	reused, err := st.StageReviewGroup(ctx, restaged)
	if err != nil {
		t.Fatalf("restage review group: %v", err)
	}
	if reused.Created {
		t.Fatal("created = true, want false")
	}
	if reused.ID != result.ID {
		t.Fatalf("reused id = %d, want %d", reused.ID, result.ID)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found after restage")
	}
	if venue.Name != "Edited Hall" {
		t.Fatalf("venue name = %q, want preserved %q", venue.Name, "Edited Hall")
	}
	if venue.Address != "99 Edited Street, Sheffield" {
		t.Fatalf("venue address = %q, want preserved %q", venue.Address, "99 Edited Street, Sheffield")
	}
	if venue.CoverageKind != domain.CoverageKindProgram {
		t.Fatalf("venue coverage kind = %q, want %q", venue.CoverageKind, domain.CoverageKindProgram)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}

	group, ok, err := st.LoadReviewGroup(ctx, result.ID)
	if err != nil {
		t.Fatalf("load review group after restage: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after restage")
	}
	if got, want := group.Candidates[0].VenueText, "Imaginary Hall refreshed"; got != want {
		t.Fatalf("candidate venue text = %q, want %q", got, want)
	}
	if got, want := group.Candidates[0].VenueLocationRaw, "Imaginary Hall refreshed, 1 Void Street, Sheffield"; got != want {
		t.Fatalf("candidate venue location raw = %q, want %q", got, want)
	}
}

func TestStageReviewGroupRestagingLegacyBlankVenueEvidenceBackfillsProvisionalVenue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Legacy blank venue evidence",
		SourceName: "Fixture ICS",
		SourceURL:  "file:legacy-blank-venue.ics",
		StagingKey: "v1:legacy-blank-venue",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Unknown venue show",
			VenueSlug:        "",
			VenueText:        "",
			VenueLocationRaw: "",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Should backfill venue evidence on restage.",
		}},
	}

	result, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if result.Created != true {
		t.Fatal("created = false, want true")
	}
	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("unexpected provisional venue created before restage evidence")
	}
	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")

	restaged := input
	restaged.Candidates = []review.CandidateInput{{
		ExternalID:       "candidate-a",
		Name:             "Unknown venue show",
		VenueSlug:        "imaginary-hall-1-void-street-sheffield",
		VenueText:        "Imaginary Hall, 1 Void Street, Sheffield",
		VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
		StartAt:          "2026-05-10T18:30:00Z",
		EndAt:            "2026-05-10T22:00:00Z",
		Status:           "Listed",
		Description:      "Should backfill venue evidence on restage.",
	}}

	reused, err := st.StageReviewGroup(ctx, restaged)
	if err != nil {
		t.Fatalf("restage review group: %v", err)
	}
	if reused.Created {
		t.Fatal("created = true, want false")
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found after restage")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "1 Void Street,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "1 Void Street,\nSheffield")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}

	group, ok, err := st.LoadReviewGroup(ctx, result.ID)
	if err != nil {
		t.Fatalf("load review group after restage: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after restage")
	}
	if got, want := group.Candidates[0].VenueText, "Imaginary Hall, 1 Void Street, Sheffield"; got != want {
		t.Fatalf("candidate venue text = %q, want %q", got, want)
	}
	if got, want := group.Candidates[0].VenueLocationRaw, "Imaginary Hall, 1 Void Street, Sheffield"; got != want {
		t.Fatalf("candidate venue location raw = %q, want %q", got, want)
	}
	if got, want := group.Candidates[0].VenueSlug, ""; got != want {
		t.Fatalf("candidate venue slug = %q, want preserved %q", got, want)
	}

	reusedAgain, err := st.StageReviewGroup(ctx, restaged)
	if err != nil {
		t.Fatalf("restage review group again: %v", err)
	}
	if reusedAgain.Created {
		t.Fatal("created = true on second restage, want false")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows after second restage = %d, want %d", got, beforeVenueCount+1)
	}
}

func TestStageReviewGroupRestagingLegacyBlankVenueEvidenceRespectsIncomingExactVenueSlug(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	input := review.GroupInput{
		Title:      "Legacy blank venue evidence exact slug",
		SourceName: "Fixture ICS",
		SourceURL:  "file:legacy-blank-venue-exact-slug.ics",
		StagingKey: "v1:legacy-blank-venue-exact-slug",
		Candidates: []review.CandidateInput{{
			ExternalID:       "candidate-a",
			Name:             "Unknown venue show",
			VenueSlug:        "",
			VenueText:        "",
			VenueLocationRaw: "",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Should keep exact slug precedence on restage.",
		}},
	}

	result, err := st.StageReviewGroup(ctx, input)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	if !result.Created {
		t.Fatal("created = false, want true")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")

	restaged := input
	restaged.Candidates = []review.CandidateInput{{
		ExternalID:       "candidate-a",
		Name:             "Unknown venue show",
		VenueSlug:        "leadmill",
		VenueText:        "Imaginary Hall",
		VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
		StartAt:          "2026-05-10T18:30:00Z",
		EndAt:            "2026-05-10T22:00:00Z",
		Status:           "Listed",
		Description:      "Should keep exact slug precedence on restage.",
	}}

	reused, err := st.StageReviewGroup(ctx, restaged)
	if err != nil {
		t.Fatalf("restage review group: %v", err)
	}
	if reused.Created {
		t.Fatal("created = true, want false")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("unexpected provisional venue created from conflicting raw evidence")
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
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?, source_name = '', source_url = ''
		WHERE id = ?
	`, "leadmill-temp", "Sidney & Matilda", "Rivelin Works, 46 Sidney Street, Sheffield", candidateID); err != nil {
		t.Fatalf("rewrite singleton review candidate venue evidence: %v", err)
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
	if event.VenueSlug != "sidney-and-matilda" {
		t.Fatalf("venue slug = %q, want %q", event.VenueSlug, "sidney-and-matilda")
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
	}
	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	assertDraftChoice(t, final, review.FieldVenueSlug, candidateID, "sidney-and-matilda")
	if got := mustCount(t, db, "events"); got != beforeCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeCount+1)
	}
}

func TestCreateReviewGroupPersistsAuthoritativeSourceMetadata(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative persisted")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if got, want := group.AuthoritativeSourceName, "Sidney & Matilda manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceURL, "https://calendar.example.test/live.ics"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceEventKey, "shared-uid"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestResolveReviewGroupUsesAuthoritativeSourceLinkIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative resolve")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	candidateID := group.Candidates[0].ID

	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "leadmill-temp", "Sidney & Matilda", "Rivelin Works, 46 Sidney Street, Sheffield", candidateID); err != nil {
		t.Fatalf("rewrite authoritative review candidate venue evidence: %v", err)
	}

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "sidney-and-matilda").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Sidney & Matilda manual ingest", "https://calendar.example.test/live.ics"); err != nil {
		t.Fatalf("insert authoritative source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Sidney & Matilda manual ingest", "https://calendar.example.test/live.ics").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	const linkedSlug = "existing-linked-event"
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
	`, linkedSlug, venueID, sourceID, "Old linked event", "2026-04-30T18:00:00Z", "2026-04-30T21:00:00Z", "Old genre", "Listed", "Old description", "2026-04-29T10:00:00Z", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert linked event: %v", err)
	}
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, linkedSlug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, eventID, sourceID, "shared-uid", "2026-04-29T10:00:00Z", "2026-04-29T10:00:00Z"); err != nil {
		t.Fatalf("insert source link: %v", err)
	}
	beforeEventCount := mustCount(t, db, "events")

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	if _, ok := st.EventBySlug("live-utc-show-sidney-and-matilda-20260501190000"); ok {
		t.Fatal("unexpected slug-upsert event created")
	}

	event, ok := st.EventBySlug(linkedSlug)
	if !ok {
		t.Fatalf("missing linked event %q", linkedSlug)
	}
	if event.Name != "UTC Show" {
		t.Fatalf("name = %q, want %q", event.Name, "UTC Show")
	}
	if event.SourceName != "Sidney & Matilda manual ingest" {
		t.Fatalf("source name = %q, want %q", event.SourceName, "Sidney & Matilda manual ingest")
	}
	if event.SourceURL != "https://calendar.example.test/live.ics" {
		t.Fatalf("source url = %q, want %q", event.SourceURL, "https://calendar.example.test/live.ics")
	}
	if event.VenueSlug != "sidney-and-matilda" {
		t.Fatalf("venue slug = %q, want %q", event.VenueSlug, "sidney-and-matilda")
	}
	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	assertDraftChoice(t, final, review.FieldVenueSlug, candidateID, "sidney-and-matilda")
	if event.Genre != "Indie" {
		t.Fatalf("genre = %q, want %q", event.Genre, "Indie")
	}
	if event.Description != "First line" {
		t.Fatalf("description = %q, want %q", event.Description, "First line")
	}
}

func TestResolveReviewGroupRollsBackWhenVenueResolutionFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Failed venue resolution")
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
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "imaginary-hall", "Sidney & Matilda", "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE", candidateID); err != nil {
		t.Fatalf("rewrite review candidate venue evidence: %v", err)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err == nil {
		t.Fatal("expected venue resolution failure")
	}

	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	if got := mustCount(t, db, "review_draft_choices"); got != 0 {
		t.Fatalf("review_draft_choices rows = %d, want 0", got)
	}
	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("unexpected provisional venue created for ambiguous evidence")
	}

	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after failed resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after failed resolve")
	}
	if final.Status != review.StatusOpen {
		t.Fatalf("status = %q, want %q", final.Status, review.StatusOpen)
	}
	if len(final.DraftChoices) != 0 {
		t.Fatalf("draft choices = %d, want 0", len(final.DraftChoices))
	}
}

func TestResolveReviewGroupAuthoritativePathCreatesSecondarySourceInfoRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative secondary info")
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
	rows := loadSecondarySourceInfoRows(t, db)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("secondary source info rows = %d, want %d", got, want)
	}
	if rows[0].InfoType != "description" || rows[0].Value != "First line" {
		t.Fatalf("first secondary row = %#v, want description First line", rows[0])
	}
	if rows[1].InfoType != "genre" || rows[1].Value != "Indie" {
		t.Fatalf("second secondary row = %#v, want genre Indie", rows[1])
	}
}

func TestResolveReviewGroupAuthoritativePathUpsertsSecondarySourceInfoRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	firstGroupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative secondary first")
	firstGroup, ok, err := st.LoadReviewGroup(ctx, firstGroupID)
	if err != nil {
		t.Fatalf("load first review group: %v", err)
	}
	if !ok {
		t.Fatal("first review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, firstGroupID, fullReviewChoices(t, firstGroup)); err != nil {
		t.Fatalf("resolve first review group: %v", err)
	}

	secondGroupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative secondary second")
	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET genre = ?, description = ?
		WHERE group_id = ? AND position = 1
	`, "Ambient", "Updated secondary description", secondGroupID); err != nil {
		t.Fatalf("update second review candidate: %v", err)
	}
	secondGroup, ok, err := st.LoadReviewGroup(ctx, secondGroupID)
	if err != nil {
		t.Fatalf("load second review group: %v", err)
	}
	if !ok {
		t.Fatal("second review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, secondGroupID, fullReviewChoices(t, secondGroup)); err != nil {
		t.Fatalf("resolve second review group: %v", err)
	}

	rows := loadSecondarySourceInfoRows(t, db)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("secondary source info rows = %d, want %d", got, want)
	}
	if rows[0].InfoType != "description" || rows[0].Value != "Updated secondary description" {
		t.Fatalf("first secondary row = %#v, want updated description", rows[0])
	}
	if rows[1].InfoType != "genre" || rows[1].Value != "Ambient" {
		t.Fatalf("second secondary row = %#v, want updated genre", rows[1])
	}
}

func TestResolveReviewGroupAuthoritativePathReconcilesStaleSecondarySourceInfoRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	firstGroupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative stale secondary first")
	firstGroup, ok, err := st.LoadReviewGroup(ctx, firstGroupID)
	if err != nil {
		t.Fatalf("load first review group: %v", err)
	}
	if !ok {
		t.Fatal("first review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, firstGroupID, fullReviewChoices(t, firstGroup)); err != nil {
		t.Fatalf("resolve first review group: %v", err)
	}

	secondGroupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative stale secondary second")
	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET genre = '', description = ''
		WHERE group_id = ? AND position = 1
	`, secondGroupID); err != nil {
		t.Fatalf("blank second review candidate secondary info: %v", err)
	}
	secondGroup, ok, err := st.LoadReviewGroup(ctx, secondGroupID)
	if err != nil {
		t.Fatalf("load second review group: %v", err)
	}
	if !ok {
		t.Fatal("second review group not found")
	}
	if err := st.ResolveReviewGroup(ctx, secondGroupID, fullReviewChoices(t, secondGroup)); err != nil {
		t.Fatalf("resolve second review group: %v", err)
	}

	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
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

func TestResolveReviewGroupRollsBackProvisionalVenueWhenLaterCanonicalUpdateFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Atomic provisional rollback")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "imagniary-hal-temp", "Imaginary Hall", "Imaginary Hall, 1 Void Street, Sheffield", group.Candidates[0].ID); err != nil {
		t.Fatalf("rewrite venue evidence: %v", err)
	}

	var canonicalEventID int64
	if err := db.QueryRow(`
		SELECT id
		FROM events
		ORDER BY id
		LIMIT 1
	`).Scan(&canonicalEventID); err != nil {
		t.Fatalf("lookup canonical event id: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET canonical_event_id = ?
		WHERE id = ?
	`, canonicalEventID, group.Candidates[0].ID); err != nil {
		t.Fatalf("set canonical event id: %v", err)
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
	`, "live-solo-show-imaginary-hall-20260503190000", venueID, sourceID, "Existing conflict", "2026-05-10T10:00:00Z", "2026-05-10T12:00:00Z", "Other", "Listed", "Conflict row", "2026-05-09T09:00:00Z", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert conflicting event: %v", err)
	}
	beforeEventCount++

	err = st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group))
	if err == nil {
		t.Fatal("expected resolve review group to fail")
	}
	if got, want := err.Error(), `review event slug "live-solo-show-imaginary-hall-20260503190000" already belongs to a different event`; got != want {
		t.Fatalf("resolve error = %q, want %q", got, want)
	}

	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("provisional venue row survived rolled-back resolve")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
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
	if len(after.DraftChoices) != 0 {
		t.Fatalf("draft choices = %d, want 0 after failed resolve", len(after.DraftChoices))
	}
}

func TestResolveReviewGroupCreatesProvisionalVenueWhenVenueIsMissing(t *testing.T) {
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
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Missing venue")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "imaginary-hall", "Imaginary Hall", "Imaginary Hall, 1 Void Street, Neepsend, Sheffield", group.Candidates[0].ID); err != nil {
		t.Fatalf("rewrite venue evidence: %v", err)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.Name != "Imaginary Hall" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Imaginary Hall")
	}
	if venue.Address != "1 Void Street,\nNeepsend,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "1 Void Street,\nNeepsend,\nSheffield")
	}
	if venue.Neighbourhood != "Neepsend" {
		t.Fatalf("venue neighbourhood = %q, want %q", venue.Neighbourhood, "Neepsend")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}
	if venue.Origin != domain.OriginLive {
		t.Fatalf("venue origin = %q, want %q", venue.Origin, domain.OriginLive)
	}

	event, ok := st.EventBySlug("live-solo-show-imaginary-hall-20260503190000")
	if !ok {
		t.Fatal("published event not found")
	}
	if event.VenueSlug != "imaginary-hall" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "imaginary-hall")
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if after.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", after.Status, review.StatusResolved)
	}
	assertDraftChoice(t, after, review.FieldVenueSlug, group.Candidates[0].ID, "imaginary-hall")
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestResolveReviewGroupUsesExplicitVenueSlugWhenVenueEvidenceConflicts(t *testing.T) {
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

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Conflicting venue evidence")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "leadmill", "Sidney & Matilda", "Rivelin Works, 46 Sidney Street, Sheffield", group.Candidates[0].ID); err != nil {
		t.Fatalf("rewrite venue evidence: %v", err)
	}

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}

	event, ok := st.EventBySlug("live-solo-show-leadmill-20260503190000")
	if !ok {
		t.Fatal("published event not found")
	}
	if event.VenueSlug != "leadmill" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "leadmill")
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	assertDraftChoice(t, after, review.FieldVenueSlug, group.Candidates[0].ID, "leadmill")
}

func TestResolveReviewGroupCreatesProvisionalVenueFromNormalizedHumanEvidence(t *testing.T) {
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
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	groupID := mustCreatePublishableSingletonReviewGroup(t, st, "Missing venue normalized")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "imagniary-hal-temp", "Imaginary Hall", "Imaginary Hall, 1 Void Street, Sheffield", group.Candidates[0].ID); err != nil {
		t.Fatalf("rewrite venue evidence: %v", err)
	}

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	sourceID := mustEnsureReviewTestSource(t, db)
	canonicalEventID := mustInsertReviewTestEvent(t, db, sourceID, "canonical-rollback-anchor", "sidney-and-matilda", "Canonical Rollback Anchor")
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET canonical_event_id = ?
		WHERE id = ?
	`, canonicalEventID, group.Candidates[0].ID); err != nil {
		t.Fatalf("set canonical event id: %v", err)
	}
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
	`, "live-solo-show-imaginary-hall-20260503190000", venueID, sourceID, "Existing conflict", "2026-05-10T10:00:00Z", "2026-05-10T12:00:00Z", "Other", "Listed", "Conflict row", "2026-05-09T09:00:00Z", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert conflicting event: %v", err)
	}
	beforeEventCount = mustCount(t, db, "events")

	err = st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group))
	if err == nil {
		t.Fatal("expected resolve review group to fail")
	}
	if got, want := err.Error(), `review event slug "live-solo-show-imaginary-hall-20260503190000" already belongs to a different event`; got != want {
		t.Fatalf("resolve error = %q, want %q", got, want)
	}

	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("provisional venue row survived rolled-back resolve")
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if after.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", after.Status, review.StatusResolved)
	}
	assertDraftChoice(t, after, review.FieldVenueSlug, group.Candidates[0].ID, "imaginary-hall")
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestResolveReviewGroupAuthoritativePathCreatesProvisionalVenueWhenVenueIsMissing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	groupID := mustCreateAuthoritativeReviewGroup(t, st, "Authoritative missing venue")
	group, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE group_id = ?
	`, "imaginary-hall", "Imaginary Hall", "Imaginary Hall, 1 Void Street, Sheffield", groupID); err != nil {
		t.Fatalf("set missing venue: %v", err)
	}
	beforeEventCount := mustCount(t, db, "events")

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve authoritative review group: %v", err)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}

	event, ok := st.EventBySlug("live-utc-show-imaginary-hall-20260501190000")
	if !ok {
		t.Fatal("published event not found")
	}
	if event.VenueSlug != "imaginary-hall" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "imaginary-hall")
	}
	if event.SourceName != "Sidney & Matilda manual ingest" {
		t.Fatalf("event source name = %q, want %q", event.SourceName, "Sidney & Matilda manual ingest")
	}
	if event.SourceURL != "https://calendar.example.test/live.ics" {
		t.Fatalf("event source url = %q, want %q", event.SourceURL, "https://calendar.example.test/live.ics")
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if after.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", after.Status, review.StatusResolved)
	}
	assertDraftChoice(t, after, review.FieldVenueSlug, group.Candidates[0].ID, "imaginary-hall")
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 2 {
		t.Fatalf("event_secondary_source_info rows = %d, want 2", got)
	}
}

func TestResolveReviewGroupRejectsMissingEndForOwnedVenueSource(t *testing.T) {
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

	groupID, err := st.CreateReviewGroup(ctx, review.GroupInput{
		Title:      "Cafe No. 9 review",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-review-missing-end",
			Name:        "Cafe No. 9 Late Show",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T18:30:00Z",
			Status:      "Listed",
			Description: "Missing end time from source page",
			SourceName:  "Cafe No. 9 manual ingest",
			SourceURL:   "https://www.wegottickets.com/Cafe9",
		}},
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

	if err := st.ResolveReviewGroup(ctx, groupID, fullReviewChoices(t, group)); err != nil {
		t.Fatalf("resolve review group: %v", err)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}

	after, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	if after.Status != review.StatusResolved {
		t.Fatalf("status = %q, want %q", after.Status, review.StatusResolved)
	}

	event, ok := st.EventBySlug("live-cafe-no-9-late-show-cafe-no-9-20260510183000")
	if !ok {
		t.Fatal("published event not found")
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
}

func TestPromoteSingletonReviewGroupIfMissingCreatesProvisionalVenueWhenVenueIsUnknown(t *testing.T) {
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
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	input := review.GroupInput{
		Title:      "Unknown venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:fixture.ics",
		Candidates: []review.CandidateInput{{
			ExternalID:       "missing-venue-1",
			Name:             "Unknown venue show",
			VenueSlug:        "imagniary-hal-temp",
			VenueText:        "Imaginary Hall",
			VenueLocationRaw: "Imaginary Hall, 1 Void Street, Neepsend, Sheffield",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Missing venue row",
		}},
	}
	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, input)
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if got, want := eventSlug, "live-unknown-venue-show-imaginary-hall-20260510183000"; got != want {
		t.Fatalf("event slug = %q, want %q", got, want)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}
	if venue.Address != "1 Void Street,\nNeepsend,\nSheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "1 Void Street,\nNeepsend,\nSheffield")
	}
	if venue.Neighbourhood != "Neepsend" {
		t.Fatalf("venue neighbourhood = %q, want %q", venue.Neighbourhood, "Neepsend")
	}

	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if event.VenueSlug != "imaginary-hall" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "imaginary-hall")
	}

	secondSlug, secondPromoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, input)
	if err != nil {
		t.Fatalf("rerun promote singleton review group: %v", err)
	}
	if !secondPromoted {
		t.Fatal("rerun promoted = false, want true")
	}
	if secondSlug != eventSlug {
		t.Fatalf("rerun slug = %q, want %q", secondSlug, eventSlug)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount+1 {
		t.Fatalf("rerun venues rows = %d, want %d", got, beforeVenueCount+1)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("rerun events rows = %d, want %d", got, beforeEventCount+1)
	}
}

func TestPromoteSingletonReviewGroupIfMissingFallsBackWhenVenueEvidenceIsAmbiguous(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	defer db.Close()
	beforeVenueCount := mustCount(t, db, "venues")
	beforeEventCount := mustCount(t, db, "events")

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Ambiguous unknown venue",
		SourceName: "Fixture ICS",
		SourceURL:  "file:fixture.ics",
		Candidates: []review.CandidateInput{{
			ExternalID:       "ambiguous-venue-1",
			Name:             "Ambiguous venue show",
			VenueSlug:        "imagniary-hal-temp",
			VenueText:        "Sidney & Matilda",
			VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
			StartAt:          "2026-05-10T18:30:00Z",
			EndAt:            "2026-05-10T22:00:00Z",
			Status:           "Listed",
			Description:      "Ambiguous venue evidence",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if promoted {
		t.Fatal("promoted = true, want false")
	}
	if eventSlug != "" {
		t.Fatalf("event slug = %q, want empty", eventSlug)
	}
	if got := mustCount(t, db, "venues"); got != beforeVenueCount {
		t.Fatalf("venues rows = %d, want unchanged %d", got, beforeVenueCount)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	if _, ok := st.VenueBySlug("imaginary-hall"); ok {
		t.Fatal("unexpected provisional venue created for ambiguous evidence")
	}
}

func TestPromoteSingletonReviewGroupIfMissingAllowsUnknownEndWhenCanonicalFieldsOtherwiseComplete(t *testing.T) {
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

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Incomplete singleton",
		SourceName: "Fixture ICS",
		SourceURL:  "file:fixture.ics",
		Candidates: []review.CandidateInput{{
			ExternalID:  "singleton-missing-end",
			Name:        "Incomplete singleton",
			VenueSlug:   "sidney-and-matilda",
			StartAt:     "2026-05-10T18:30:00Z",
			Status:      "Listed",
			Description: "Missing end time",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
	if got, want := event.PublicationState, domain.PublicationStateProvisional; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewGroupIfMissingAllowsOwnedVenueBlankEndTime(t *testing.T) {
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

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Cafe No. 9 singleton",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-1",
			Name:        "Cafe No. 9 Late Show",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T18:30:00Z",
			Status:      "Listed",
			Description: "Missing end time from source page",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}

	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	wantStart := time.Date(2026, time.May, 10, 18, 30, 0, 0, time.UTC)
	if !event.Start.Equal(wantStart) {
		t.Fatalf("start = %s, want %s", event.Start.Format(time.RFC3339), wantStart.Format(time.RFC3339))
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
}

func TestPromoteSingletonReviewGroupIfMissingPublishesNonAuthoritativeSingletonWhenSlugAbsent(t *testing.T) {
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

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "The Greystones singleton",
		SourceName: "The Greystones manual ingest",
		SourceURL:  "https://www.mygreystones.co.uk/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "greystones-1",
			Name:        "Roots Night",
			VenueSlug:   "greystones",
			StartAt:     "2026-05-16T19:30:00Z",
			EndAt:       "2026-05-16T22:00:00Z",
			Status:      "Listed",
			Description: "Initial Greystones listing",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}

	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if got, want := event.VenueSlug, "greystones"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := event.PublicationState, domain.PublicationStateProvisional; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewGroupIfMissingFallsBackWhenNonAuthoritativeSlugExists(t *testing.T) {
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

	firstSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "The Greystones singleton",
		SourceName: "The Greystones manual ingest",
		SourceURL:  "https://www.mygreystones.co.uk/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "greystones-existing-1",
			Name:        "Roots Night",
			VenueSlug:   "greystones",
			StartAt:     "2026-05-16T19:30:00Z",
			EndAt:       "2026-05-16T22:00:00Z",
			Status:      "Listed",
			Description: "First description",
		}},
	})
	if err != nil {
		t.Fatalf("first promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("first promoted = false, want true")
	}
	beforeEventCount := mustCount(t, db, "events")

	secondSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "The Greystones singleton updated",
		SourceName: "The Greystones manual ingest",
		SourceURL:  "https://www.mygreystones.co.uk/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "greystones-existing-2",
			Name:        "Roots Night",
			VenueSlug:   "greystones",
			StartAt:     "2026-05-16T19:30:00Z",
			EndAt:       "2026-05-16T22:00:00Z",
			Status:      "Sold out",
			Description: "Updated description",
		}},
	})
	if err != nil {
		t.Fatalf("second promote singleton review group: %v", err)
	}
	if promoted {
		t.Fatal("second promoted = true, want false")
	}
	if secondSlug != "" {
		t.Fatalf("second slug = %q, want empty", secondSlug)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	event, ok := st.EventBySlug(firstSlug)
	if !ok {
		t.Fatalf("missing published event %q", firstSlug)
	}
	if event.Status != "Listed" {
		t.Fatalf("status = %q, want preserved %q", event.Status, "Listed")
	}
	if event.Description != "First description" {
		t.Fatalf("description = %q, want preserved %q", event.Description, "First description")
	}
	if got, want := event.PublicationState, domain.PublicationStateProvisional; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewGroupIfMissingPublishesJazzAtTheLescarSingletonWhenAbsent(t *testing.T) {
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

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Jazz at The Lescar singleton",
		SourceName: "Jazz at The Lescar manual ingest",
		SourceURL:  "http://www.jazzatthelescar.com/index.html",
		Candidates: []review.CandidateInput{{
			ExternalID:  "jazz-lescar-1",
			Name:        "Jazz at The Lescar Quartet",
			VenueSlug:   "lescar",
			StartAt:     "2026-05-14T19:30:00Z",
			Status:      "Listed",
			Description: "Missing published end time",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
	if got, want := event.PublicationState, domain.PublicationStateProvisional; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}

	var storedEnd sql.NullString
	if err := db.QueryRow(`SELECT end_at FROM events WHERE slug = ?`, eventSlug).Scan(&storedEnd); err != nil {
		t.Fatalf("load stored end_at: %v", err)
	}
	if storedEnd.Valid {
		t.Fatalf("stored end_at = %q, want NULL", storedEnd.String)
	}
}

func TestPromoteSingletonReviewGroupIfMissingCreatesNoNewStagedGroupForNonAuthoritativeSingleton(t *testing.T) {
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
	beforeReviewGroupCount := mustCount(t, db, "review_groups")

	groupInput := mustGreystonesSingletonReviewGroupInput(t, 99, "greystones-direct-1", "Roots Night", "2026-05-16T19:30:00Z", "2026-05-16T22:00:00Z")
	if groupInput.StagingKey == "" {
		t.Fatal("staging key = empty, want populated")
	}

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, groupInput)
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroupCount {
		t.Fatalf("review_groups rows = %d, want unchanged %d", got, beforeReviewGroupCount)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}
}

func TestPromoteSingletonReviewGroupIfMissingResolvesMatchingStaleNonAuthoritativeSingletonGroup(t *testing.T) {
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

	mustInsertImportRunFixture(t, st, 98)
	mustInsertImportRunFixture(t, st, 99)

	staleInput := mustGreystonesSingletonReviewGroupInput(t, 98, "greystones-stale-1", "Roots Night", "2026-05-16T19:30:00Z", "2026-05-16T22:00:00Z")
	currentInput := mustGreystonesSingletonReviewGroupInput(t, 99, "greystones-stale-1", "Roots Night", "2026-05-16T19:30:00Z", "2026-05-16T22:00:00Z")
	if staleInput.StagingKey != currentInput.StagingKey {
		t.Fatalf("staging key mismatch: stale %q current %q", staleInput.StagingKey, currentInput.StagingKey)
	}

	stageResult, err := st.StageReviewGroup(ctx, staleInput)
	if err != nil {
		t.Fatalf("stage stale review group: %v", err)
	}
	groupID := stageResult.ID
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}

	db := mustRawDB(t, path)
	defer db.Close()
	beforeEventCount := mustCount(t, db, "events")

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, currentInput)
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "review_groups"); got != 1 {
		t.Fatalf("review_groups rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	staleGroup, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load stale review group: %v", err)
	}
	if !ok {
		t.Fatal("stale review group not found after promotion")
	}
	if staleGroup.Status != review.StatusResolved {
		t.Fatalf("stale group status = %q, want %q", staleGroup.Status, review.StatusResolved)
	}
}

func TestPromoteSingletonReviewGroupIfMissingLinksMatchingStaleNonAuthoritativeSingletonGroupToCurrentImportRun(t *testing.T) {
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

	mustInsertImportRunFixture(t, st, 98)
	mustInsertImportRunFixture(t, st, 99)

	staleInput := mustGreystonesSingletonReviewGroupInput(t, 98, "greystones-link-1", "Roots Night", "2026-05-18T19:30:00Z", "2026-05-18T22:00:00Z")
	currentInput := mustGreystonesSingletonReviewGroupInput(t, 99, "greystones-link-1", "Roots Night", "2026-05-18T19:30:00Z", "2026-05-18T22:00:00Z")

	stageResult, err := st.StageReviewGroup(ctx, staleInput)
	if err != nil {
		t.Fatalf("stage stale review group: %v", err)
	}
	groupID := stageResult.ID
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}

	db := mustRawDB(t, path)
	defer db.Close()

	var beforeLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM import_run_review_groups
		WHERE import_run_id = ? AND review_group_id = ?
	`, 99, groupID).Scan(&beforeLinkCount); err != nil {
		t.Fatalf("count import-run links before promotion: %v", err)
	}
	if beforeLinkCount != 0 {
		t.Fatalf("pre-promotion import-run link count = %d, want 0", beforeLinkCount)
	}

	if _, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, currentInput); err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	} else if !promoted {
		t.Fatal("promoted = false, want true")
	}

	var afterLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM import_run_review_groups
		WHERE import_run_id = ? AND review_group_id = ?
	`, 99, groupID).Scan(&afterLinkCount); err != nil {
		t.Fatalf("count import-run links after promotion: %v", err)
	}
	if afterLinkCount != 1 {
		t.Fatalf("post-promotion import-run link count = %d, want 1", afterLinkCount)
	}
}

func TestPromoteSingletonReviewGroupIfMissingResolvesMatchingStaleStagedGroup(t *testing.T) {
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

	report := ingest.Report{
		Source:      ingest.CafeNo9Source,
		SourceURL:   "https://www.wegottickets.com/Cafe9",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{{
				UID:      "cafe-no-9-stale-1",
				Summary:  "Cafe No. 9 Late Show",
				Location: "Cafe No. 9",
				StartAt:  "2026-05-10T18:30:00Z",
			}},
		}},
	}
	groups := ingest.ReviewGroupsFromReport(report)
	if got, want := len(groups), 1; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}
	groupInput := groups[0]
	if groupInput.StagingKey == "" {
		t.Fatal("staging key = empty, want populated")
	}
	if groupInput.AuthoritativeSourceEventKey == "" {
		t.Fatal("authoritative source event key = empty, want populated")
	}

	stageResult, err := st.StageReviewGroup(ctx, groupInput)
	if err != nil {
		t.Fatalf("stage review group: %v", err)
	}
	groupID := stageResult.ID
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}

	staleGroup, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("load stale review group: %v", err)
	}
	if !ok {
		t.Fatal("stale review group not found")
	}
	if staleGroup.Status != review.StatusOpen {
		t.Fatalf("stale group status = %q, want %q", staleGroup.Status, review.StatusOpen)
	}

	db := mustRawDB(t, path)
	defer db.Close()
	beforeEventCount := mustCount(t, db, "events")

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, groupInput)
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}

	staleGroup, ok, err = st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload stale review group: %v", err)
	}
	if !ok {
		t.Fatal("stale review group not found after promotion")
	}
	if staleGroup.Status != review.StatusResolved {
		t.Fatalf("stale group status = %q, want %q", staleGroup.Status, review.StatusResolved)
	}
}

func TestPromoteSingletonReviewGroupIfMissingLeavesDifferentNonAuthoritativeStagingKeyOpen(t *testing.T) {
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

	mustInsertImportRunFixture(t, st, 98)
	mustInsertImportRunFixture(t, st, 99)

	matchingStaleInput := mustGreystonesSingletonReviewGroupInput(t, 98, "greystones-match-1", "Roots Night", "2026-05-16T19:30:00Z", "2026-05-16T22:00:00Z")
	matchingCurrentInput := mustGreystonesSingletonReviewGroupInput(t, 99, "greystones-match-1", "Roots Night", "2026-05-16T19:30:00Z", "2026-05-16T22:00:00Z")
	otherInput := mustGreystonesSingletonReviewGroupInput(t, 98, "greystones-other-1", "Early Show", "2026-05-17T18:30:00Z", "2026-05-17T21:30:00Z")

	if matchingStaleInput.StagingKey == otherInput.StagingKey {
		t.Fatalf("staging key collision: matching and other both %q", otherInput.StagingKey)
	}

	matchingResult, err := st.StageReviewGroup(ctx, matchingStaleInput)
	if err != nil {
		t.Fatalf("stage matching review group: %v", err)
	}
	if !matchingResult.Created {
		t.Fatal("matching created = false, want true")
	}
	otherResult, err := st.StageReviewGroup(ctx, otherInput)
	if err != nil {
		t.Fatalf("stage other review group: %v", err)
	}
	otherGroupID := otherResult.ID
	if !otherResult.Created {
		t.Fatal("other created = false, want true")
	}

	if _, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, matchingCurrentInput); err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	} else if !promoted {
		t.Fatal("promoted = false, want true")
	}

	otherGroup, ok, err := st.LoadReviewGroup(ctx, otherGroupID)
	if err != nil {
		t.Fatalf("load other review group: %v", err)
	}
	if !ok {
		t.Fatal("other review group not found")
	}
	if otherGroup.Status != review.StatusOpen {
		t.Fatalf("other group status = %q, want %q", otherGroup.Status, review.StatusOpen)
	}
}

func TestPromoteSingletonReviewGroupIfMissingFallsBackWithoutStableSourceKey(t *testing.T) {
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

	eventSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "No stable key",
		SourceName: "Yellow Arch manual ingest",
		SourceURL:  "https://www.yellowarch.com/events/",
		Candidates: []review.CandidateInput{{
			Name:        "No stable key",
			VenueSlug:   "yellow-arch",
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T22:00:00Z",
			Status:      "Listed",
			Description: "Missing external ID",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if eventSlug == "" {
		t.Fatal("event slug = empty, want published slug")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	event, ok := st.EventBySlug(eventSlug)
	if !ok {
		t.Fatalf("missing published event %q", eventSlug)
	}
	if got, want := event.PublicationState, domain.PublicationStateProvisional; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestAuthoritativeOwnedVenueSlugForSourceNameIncludesCafeNo9(t *testing.T) {
	if got, want := ingest.OwnedVenueSlugForReviewStageSourceName("Cafe No. 9 manual ingest"), "cafe-no-9"; got != want {
		t.Fatalf("owned venue slug = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewGroupIfMissingUpdatesLinkedEventInPlace(t *testing.T) {
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

	firstSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "First apply",
		SourceName: "Yellow Arch manual ingest",
		SourceURL:  "https://www.yellowarch.com/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "yellow-arch-1",
			Name:        "Late Junction",
			VenueSlug:   "yellow-arch",
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T22:00:00Z",
			Genre:       "Electronic",
			Status:      "Listed",
			Description: "First description",
		}},
	})
	if err != nil {
		t.Fatalf("first promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("first promoted = false, want true")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}

	secondSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Second apply",
		SourceName: "Yellow Arch manual ingest",
		SourceURL:  "https://www.yellowarch.com/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "yellow-arch-1",
			Name:        "Late Junction Renamed",
			VenueSlug:   "yellow-arch",
			StartAt:     "2026-05-10T19:00:00Z",
			EndAt:       "2026-05-10T23:00:00Z",
			Genre:       "Ambient",
			Status:      "Sold out",
			Description: "Updated description",
		}},
	})
	if err != nil {
		t.Fatalf("second promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("second promoted = false, want true")
	}
	if secondSlug != firstSlug {
		t.Fatalf("second slug = %q, want %q", secondSlug, firstSlug)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount+1)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}

	event, ok := st.EventBySlug(firstSlug)
	if !ok {
		t.Fatalf("missing published event %q", firstSlug)
	}
	if event.Name != "Late Junction Renamed" {
		t.Fatalf("name = %q, want %q", event.Name, "Late Junction Renamed")
	}
	if !event.Start.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("start = %s, want updated start", event.Start.Format(time.RFC3339))
	}
	if !event.End.Equal(time.Date(2026, time.May, 10, 23, 0, 0, 0, time.UTC)) {
		t.Fatalf("end = %s, want updated end", event.End.Format(time.RFC3339))
	}
	if event.Status != "Sold out" {
		t.Fatalf("status = %q, want %q", event.Status, "Sold out")
	}
	if event.Genre != "Ambient" {
		t.Fatalf("genre = %q, want %q", event.Genre, "Ambient")
	}
	if event.Description != "Updated description" {
		t.Fatalf("description = %q, want %q", event.Description, "Updated description")
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}
}

func TestPromoteSingletonReviewGroupIfMissingPreservesCleanDescriptionWhenAuthoritativeUpdateHasWeakCTA(t *testing.T) {
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

	firstSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "First apply",
		SourceName: "Yellow Arch manual ingest",
		SourceURL:  "https://www.yellowarch.com/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "yellow-arch-cta",
			Name:        "Late Junction",
			VenueSlug:   "yellow-arch",
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T22:00:00Z",
			Genre:       "Electronic",
			Status:      "Listed",
			Description: "Existing clean description.",
		}},
	})
	if err != nil {
		t.Fatalf("first promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("first promoted = false, want true")
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEventCount+1)
	}

	secondSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Second apply",
		SourceName: "Yellow Arch manual ingest",
		SourceURL:  "https://www.yellowarch.com/events/",
		Candidates: []review.CandidateInput{{
			ExternalID:  "yellow-arch-cta",
			Name:        "Late Junction Renamed",
			VenueSlug:   "yellow-arch",
			StartAt:     "2026-05-10T19:00:00Z",
			EndAt:       "2026-05-10T23:00:00Z",
			Genre:       "Ambient",
			Status:      "Sold out",
			Description: "Read more",
		}},
	})
	if err != nil {
		t.Fatalf("second promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("second promoted = false, want true")
	}
	if secondSlug != firstSlug {
		t.Fatalf("second slug = %q, want %q", secondSlug, firstSlug)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount+1 {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount+1)
	}

	event, ok := st.EventBySlug(firstSlug)
	if !ok {
		t.Fatalf("missing published event %q", firstSlug)
	}
	if event.Name != "Late Junction Renamed" {
		t.Fatalf("name = %q, want %q", event.Name, "Late Junction Renamed")
	}
	if !event.Start.Equal(time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("start = %s, want updated start", event.Start.Format(time.RFC3339))
	}
	if !event.End.Equal(time.Date(2026, time.May, 10, 23, 0, 0, 0, time.UTC)) {
		t.Fatalf("end = %s, want updated end", event.End.Format(time.RFC3339))
	}
	if event.Status != "Sold out" {
		t.Fatalf("status = %q, want %q", event.Status, "Sold out")
	}
	if event.Genre != "Ambient" {
		t.Fatalf("genre = %q, want %q", event.Genre, "Ambient")
	}
	if event.Description != "Existing clean description." {
		t.Fatalf("description = %q, want %q", event.Description, "Existing clean description.")
	}
}

func TestShouldReplaceDescriptionPolicy(t *testing.T) {
	cases := []struct {
		name     string
		existing string
		incoming string
		want     bool
	}{
		{name: "blank existing clean incoming", existing: "", incoming: "A clean event description with useful details.", want: true},
		{name: "generated existing clean incoming", existing: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; } @media screen {}", incoming: "A clean event description with useful details.", want: true},
		{name: "clean existing clean incoming", existing: "Existing clean description.", incoming: "New clean description.", want: false},
		{name: "blank incoming", existing: "", incoming: "", want: false},
		{name: "generated incoming", existing: "", incoming: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; }", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReplaceDescription(tc.existing, tc.incoming); got != tc.want {
				t.Fatalf("shouldReplaceDescription(%q, %q) = %v, want %v", tc.existing, tc.incoming, got, tc.want)
			}
		})
	}
}

func TestAuthoritativeDescriptionUsablePolicy(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "blank", value: "   ", want: false},
		{name: "first line", value: "First line", want: true},
		{name: "generated block", value: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; }", want: false},
		{name: "generated media query", value: "@media screen {}", want: false},
		{name: "generated script", value: "<script>alert('x')</script>", want: false},
		{name: "buy tickets", value: "buy tickets", want: false},
		{name: "basement buy tickets", value: "basement buy tickets", want: false},
		{name: "tickets", value: "tickets", want: false},
		{name: "book tickets", value: "book tickets", want: false},
		{name: "read more", value: "read more", want: false},
		{name: "find out more", value: "find out more", want: false},
		{name: "more info", value: "more info", want: false},
		{name: "event details", value: "event details", want: false},
		{name: "click here", value: "click here", want: false},
		{name: "back to events", value: "back to events", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := authoritativeDescriptionUsable(tc.value); got != tc.want {
				t.Fatalf("authoritativeDescriptionUsable(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestRepairEventDescriptionsFromReportUpdatesOnlyEligibleDescriptions(t *testing.T) {
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

	blankSlug := mustPromoteCafeNo9Event(t, st, "blank-eligible", "Blank Eligible", "2026-05-10T18:30:00Z", "")
	generatedSlug := mustPromoteCafeNo9Event(t, st, "generated-eligible", "Generated Eligible", "2026-05-11T18:30:00Z", "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; } @media screen {}")
	cleanSlug := mustPromoteCafeNo9Event(t, st, "clean-preserved", "Clean Preserved", "2026-05-12T18:30:00Z", "Existing clean description.")
	beforeGroups := mustCount(t, db, "review_groups")
	beforeEvents := mustCount(t, db, "events")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate("blank-eligible", "Blank Eligible", "2026-05-10T18:30:00Z", "Replacement description for the blank event."),
				cafeNo9RepairCandidate("generated-eligible", "Generated Eligible", "2026-05-11T18:30:00Z", "Replacement description for generated markup."),
				cafeNo9RepairCandidate("clean-preserved", "Clean Preserved", "2026-05-12T18:30:00Z", "Incoming description should not replace clean existing text."),
				cafeNo9RepairCandidate("new-skipped", "New Skipped", "2026-05-13T18:30:00Z", "No existing event should be created by repair."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 2 {
		t.Fatalf("repaired = %d, want 2", repair.Repaired)
	}
	if repair.Unchanged != 1 {
		t.Fatalf("unchanged = %d, want 1", repair.Unchanged)
	}
	if repair.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", repair.Skipped)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeGroups {
		t.Fatalf("review groups = %d, want unchanged %d", got, beforeGroups)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events = %d, want unchanged %d", got, beforeEvents)
	}

	assertEventDescription(t, st, blankSlug, "Replacement description for the blank event.")
	assertEventDescription(t, st, generatedSlug, "Replacement description for generated markup.")
	assertEventDescription(t, st, cleanSlug, "Existing clean description.")
}

func TestRepairEventDescriptionsFromReportSkipsSameSlugUnderDifferentSource(t *testing.T) {
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

	const (
		uid     = "cross-source-slug"
		name    = "Cross Source Slug"
		startAt = "2026-05-14T18:30:00Z"
	)
	_ = mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	otherSourceID := mustEnsureSourceID(t, st, "Different manual ingest", "https://different.example.test/events")
	slug := mustLiveEventSlug(t, name, "cafe-no-9", startAt)
	mustInsertRepairLegacyEvent(t, db, otherSourceID, slug, "cafe-no-9", name, startAt, "")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the cross-source slug case."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 0 || repair.Unchanged != 0 || repair.Skipped != 1 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 0 0 1", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, slug, "")
}

func TestRepairEventDescriptionsFromReportSkipsSameFingerprintUnderDifferentSource(t *testing.T) {
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

	const (
		uid     = "cross-source-fingerprint"
		name    = "Cross Source Fingerprint"
		startAt = "2026-05-15T18:30:00Z"
	)
	_ = mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	otherSourceID := mustEnsureSourceID(t, st, "Different manual ingest", "https://different.example.test/events")
	mustInsertRepairLegacyEvent(t, db, otherSourceID, "legacy-cross-source-fingerprint", "cafe-no-9", name, startAt, "")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the cross-source fingerprint case."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 0 || repair.Unchanged != 0 || repair.Skipped != 1 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 0 0 1", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, "legacy-cross-source-fingerprint", "")
}

func TestRepairEventDescriptionsFromReportRepairsSameSourceLegacyEventWithoutLink(t *testing.T) {
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

	const (
		uid     = "same-source-legacy"
		name    = "Same Source Legacy"
		startAt = "2026-05-16T18:30:00Z"
	)
	sourceID := mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	slug := mustLiveEventSlug(t, name, "cafe-no-9", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, slug, "cafe-no-9", name, startAt, "")
	beforeLinks := mustCount(t, db, "event_source_links")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the same-source legacy event."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 1 || repair.Unchanged != 0 || repair.Skipped != 0 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 1 0 0", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, slug, "Replacement description for the same-source legacy event.")
	if got := mustCount(t, db, "event_source_links"); got != beforeLinks {
		t.Fatalf("event_source_links rows = %d, want unchanged %d", got, beforeLinks)
	}
}

func TestPromoteSingletonReviewGroupIfMissingClearsExistingEndWhenOwnedVenueImportOmitsEnd(t *testing.T) {
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

	firstSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "First apply",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-keep-end",
			Name:        "Cafe No. 9 Late Show",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T21:00:00Z",
			Status:      "Listed",
			Description: "Original end time",
		}},
	})
	if err != nil {
		t.Fatalf("first promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("first promoted = false, want true")
	}

	secondSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Second apply",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-keep-end",
			Name:        "Cafe No. 9 Late Show Updated",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T18:30:00Z",
			Status:      "Sold out",
			Description: "Updated without end time",
		}},
	})
	if err != nil {
		t.Fatalf("second promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("second promoted = false, want true")
	}
	if secondSlug != firstSlug {
		t.Fatalf("second slug = %q, want %q", secondSlug, firstSlug)
	}

	event, ok := st.EventBySlug(firstSlug)
	if !ok {
		t.Fatalf("missing published event %q", firstSlug)
	}
	if event.Name != "Cafe No. 9 Late Show Updated" {
		t.Fatalf("name = %q, want %q", event.Name, "Cafe No. 9 Late Show Updated")
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time after authoritative omission", event.End)
	}
	if event.Status != "Sold out" {
		t.Fatalf("status = %q, want %q", event.Status, "Sold out")
	}
}

func TestPromoteSingletonReviewGroupIfMissingClearsEndWhenOwnedVenueImportMovesStartPastExistingEnd(t *testing.T) {
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

	firstSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "First apply",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-moved-start",
			Name:        "Cafe No. 9 Late Show",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T18:30:00Z",
			EndAt:       "2026-05-10T21:00:00Z",
			Status:      "Listed",
			Description: "Original end time",
		}},
	})
	if err != nil {
		t.Fatalf("first promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("first promoted = false, want true")
	}

	secondSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Second apply",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "cafe-no-9-moved-start",
			Name:        "Cafe No. 9 Late Show Updated",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T22:30:00Z",
			Status:      "Sold out",
			Description: "Updated without end time",
		}},
	})
	if err != nil {
		t.Fatalf("second promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("second promoted = false, want true")
	}
	if secondSlug != firstSlug {
		t.Fatalf("second slug = %q, want %q", secondSlug, firstSlug)
	}

	event, ok := st.EventBySlug(firstSlug)
	if !ok {
		t.Fatalf("missing published event %q", firstSlug)
	}
	wantStart := time.Date(2026, time.May, 10, 22, 30, 0, 0, time.UTC)
	if !event.Start.Equal(wantStart) {
		t.Fatalf("start = %s, want %s", event.Start.Format(time.RFC3339), wantStart.Format(time.RFC3339))
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time after authoritative omission", event.End)
	}
	if event.Name != "Cafe No. 9 Late Show Updated" {
		t.Fatalf("name = %q, want %q", event.Name, "Cafe No. 9 Late Show Updated")
	}
	if event.Status != "Sold out" {
		t.Fatalf("status = %q, want %q", event.Status, "Sold out")
	}
}

func TestPromoteSingletonReviewGroupIfMissingAttachesLegacySlugMatch(t *testing.T) {
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

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	sourceID := mustEnsureReviewTestSource(t, db)

	slug := "live-utc-show-sidney-and-matilda-20260501190000"
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
	`, slug, venueID, sourceID, "Existing conflict", "2026-05-10T10:00:00Z", "2026-05-10T12:00:00Z", "Other", "Listed", "Conflict row", "2026-05-09T09:00:00Z", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert conflicting event: %v", err)
	}
	beforeEventCount := mustCount(t, db, "events")

	gotSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "New listing review: UTC Show",
		SourceName: "Sidney & Matilda manual ingest",
		SourceURL:  "https://calendar.example.test/live.ics",
		Candidates: []review.CandidateInput{{
			ExternalID:  "singleton-1",
			Name:        "UTC Show",
			VenueSlug:   "sidney-and-matilda",
			StartAt:     "2026-05-01T19:00:00Z",
			EndAt:       "2026-05-01T22:00:00Z",
			Genre:       "Indie",
			Status:      "Listed",
			Description: "Auto-promoted candidate",
			SourceName:  "Sidney & Matilda manual ingest",
			SourceURL:   "https://calendar.example.test/live.ics",
			Provenance:  "import run 99; UID singleton-1",
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review group: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if gotSlug != slug {
		t.Fatalf("slug = %q, want %q", gotSlug, slug)
	}
	if got := mustCount(t, db, "events"); got != beforeEventCount {
		t.Fatalf("events rows = %d, want unchanged %d", got, beforeEventCount)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("missing published event %q", slug)
	}
	if event.Name != "UTC Show" {
		t.Fatalf("name = %q, want %q", event.Name, "UTC Show")
	}
	if !event.Start.Equal(time.Date(2026, time.May, 1, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("start = %s, want attached authoritative start", event.Start.Format(time.RFC3339))
	}
	if event.Origin != domain.OriginLive {
		t.Fatalf("origin = %q, want %q", event.Origin, domain.OriginLive)
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
	candidateID := group.Candidates[0].ID
	if _, err := db.Exec(`
		UPDATE review_candidates
		SET venue_slug = ?, venue_text = ?, venue_location_raw = ?
		WHERE id = ?
	`, "leadmill-temp", "Sidney & Matilda", "Rivelin Works, 46 Sidney Street, Sheffield", candidateID); err != nil {
		t.Fatalf("rewrite duplicate review candidate venue evidence: %v", err)
	}

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	sourceID := mustEnsureReviewTestSource(t, db)
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
	final, ok, err := st.LoadReviewGroup(ctx, groupID)
	if err != nil {
		t.Fatalf("reload review group after resolve: %v", err)
	}
	if !ok {
		t.Fatal("review group not found after resolve")
	}
	assertDraftChoice(t, final, review.FieldVenueSlug, candidateID, "sidney-and-matilda")
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

func mustReviewCatalog(t *testing.T) *ingest.Catalog {
	t.Helper()

	catalog, err := ingest.LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}
	return catalog
}

func cafeNo9RepairSourceURL(uid string) string {
	return "https://www.wegottickets.com/event/" + uid
}

func mustPromoteCafeNo9Event(t *testing.T, st *Store, externalID, name, startAt, description string) string {
	t.Helper()

	sourceURL := cafeNo9RepairSourceURL(externalID)
	slug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(context.Background(), review.GroupInput{
		Title:      "Cafe No. 9 singleton",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  sourceURL,
		Candidates: []review.CandidateInput{{
			ExternalID:  externalID,
			Name:        name,
			VenueSlug:   "cafe-no-9",
			StartAt:     startAt,
			Status:      "Listed",
			Description: description,
			SourceName:  "Cafe No. 9 manual ingest",
			SourceURL:   sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote Cafe No. 9 event: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	return slug
}

func cafeNo9RepairCandidate(uid, summary, startAt, description string) ingest.EventCandidate {
	return ingest.EventCandidate{
		UID:         uid,
		Summary:     summary,
		Description: description,
		Location:    "Cafe No. 9",
		URL:         cafeNo9RepairSourceURL(uid),
		Status:      "Listed",
		StartAt:     startAt,
	}
}

func mustEnsureSourceID(t *testing.T, st *Store, sourceName, sourceURL string) int64 {
	t.Helper()

	sourceID, err := st.EnsureSource(context.Background(), sourceName, sourceURL)
	if err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	return sourceID
}

func mustLiveEventSlug(t *testing.T, name, venueSlug, startAt string) string {
	t.Helper()

	start, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		t.Fatalf("parse start time: %v", err)
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		t.Fatalf("build live event slug: %v", err)
	}
	return slug
}

func mustInsertRepairLegacyEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name, startAt, description string) {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
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
		) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, startAt, "Test", "Listed", description, "2026-05-01T10:00:00Z", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert repair legacy event: %v", err)
	}
}

func assertEventDescription(t *testing.T, st *Store, slug, want string) {
	t.Helper()

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("missing event %q", slug)
	}
	if event.Description != want {
		t.Fatalf("%s description = %q, want %q", slug, event.Description, want)
	}
}

type secondarySourceInfoRow struct {
	EventID   int64
	SourceID  int64
	VenueSlug string
	EventName string
	StartAt   string
	InfoType  string
	Value     string
}

func loadSecondarySourceInfoRows(t *testing.T, db *sql.DB) []secondarySourceInfoRow {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			event_id,
			source_id,
			venue_slug,
			event_name,
			start_at,
			info_type,
			value
		FROM event_secondary_source_info
		ORDER BY info_type, id
	`)
	if err != nil {
		t.Fatalf("query secondary source info rows: %v", err)
	}
	defer rows.Close()

	var result []secondarySourceInfoRow
	for rows.Next() {
		var row secondarySourceInfoRow
		if err := rows.Scan(&row.EventID, &row.SourceID, &row.VenueSlug, &row.EventName, &row.StartAt, &row.InfoType, &row.Value); err != nil {
			t.Fatalf("scan secondary source info row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate secondary source info rows: %v", err)
	}
	return result
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
	ensureImportRunForNotes(t, st, notes)

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
	ensureImportRunForNotes(t, st, notes)

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

func ensureImportRunForNotes(t *testing.T, st *Store, notes string) {
	t.Helper()

	importRunID, ok := review.ParseOriginImportRunID(notes)
	if !ok {
		return
	}
	if _, err := st.db.Exec(`
		INSERT OR IGNORE INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, importRunID, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "fixture import run"); err != nil {
		t.Fatalf("insert import run fixture %d: %v", importRunID, err)
	}
}

func mustInsertImportRunFixture(t *testing.T, st *Store, importRunID int64) {
	t.Helper()

	if _, err := st.db.Exec(`
		INSERT OR IGNORE INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, importRunID, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "fixture import run"); err != nil {
		t.Fatalf("insert import run fixture %d: %v", importRunID, err)
	}
}

func mustGreystonesSingletonReviewGroupInput(t *testing.T, importRunID int64, uid, summary, startAt, endAt string) review.GroupInput {
	t.Helper()

	groups := ingest.ReviewGroupsFromReport(ingest.Report{
		Source:      ingest.TheGreystonesSource,
		SourceURL:   "https://www.mygreystones.co.uk/events/",
		ImportRunID: importRunID,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.mygreystones.co.uk/events/",
			Candidates: []ingest.EventCandidate{{
				UID:      uid,
				Summary:  summary,
				Location: "The Greystones",
				StartAt:  startAt,
				EndAt:    endAt,
			}},
		}},
	})
	if got, want := len(groups), 1; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}
	return groups[0]
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

func mustEnsureReviewTestSource(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	if _, err := db.Exec(`
		INSERT OR IGNORE INTO sources (name, url)
		VALUES (?, ?)
	`, "Review test source", "https://example.test/review-test"); err != nil {
		t.Fatalf("insert review test source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`
		SELECT id
		FROM sources
		WHERE name = ? AND url = ?
	`, "Review test source", "https://example.test/review-test").Scan(&sourceID); err != nil {
		t.Fatalf("lookup review test source id: %v", err)
	}
	return sourceID
}

func mustInsertReviewTestEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name string) int64 {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id %q: %v", venueSlug, err)
	}
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
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "Indie", "Listed", "Review test event", "2026-04-30T10:00:00Z", string(domain.OriginLive))
	if err != nil {
		t.Fatalf("insert review test event %q: %v", slug, err)
	}
	eventID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("review test event id: %v", err)
	}
	return eventID
}

func mustCreateAuthoritativeReviewGroup(t *testing.T, st *Store, title string) int64 {
	t.Helper()

	id, err := st.CreateReviewGroup(context.Background(), review.GroupInput{
		Title:                       title,
		SourceName:                  "Fixture ICS",
		SourceURL:                   "file:published.ics",
		AuthoritativeSourceName:     "Sidney & Matilda manual ingest",
		AuthoritativeSourceURL:      "https://calendar.example.test/live.ics",
		AuthoritativeSourceEventKey: "shared-uid",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "shared-uid",
				Name:        "UTC Show",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "First line",
				SourceName:  "Candidate source name",
				SourceURL:   "https://example.test/utc-show",
				Provenance:  "fixture UID shared-uid",
			},
		},
	})
	if err != nil {
		t.Fatalf("create authoritative review group: %v", err)
	}
	return id
}
