package sqlite

import (
	"testing"

	"sheffield-live/internal/domain"
)

func TestRoomSetsConflictIgnoresRoomOrder(t *testing.T) {
	a := []domain.VenueRoom{
		{VenueSlug: "sidney-and-matilda", Slug: "gallery", Name: "Gallery"},
		{VenueSlug: "sidney-and-matilda", Slug: "basement", Name: "Basement"},
	}
	b := []domain.VenueRoom{
		{VenueSlug: "sidney-and-matilda", Slug: "basement", Name: "Basement"},
		{VenueSlug: "sidney-and-matilda", Slug: "gallery", Name: "Gallery"},
	}

	if roomSetsConflict(a, b) {
		t.Fatal("roomSetsConflict returned true for same room set in different order")
	}
}

func TestRoomSetsConflictDetectsDifferentRoomSet(t *testing.T) {
	a := []domain.VenueRoom{
		{VenueSlug: "sidney-and-matilda", Slug: "gallery", Name: "Gallery"},
		{VenueSlug: "sidney-and-matilda", Slug: "basement", Name: "Basement"},
	}
	b := []domain.VenueRoom{
		{VenueSlug: "sidney-and-matilda", Slug: "gallery", Name: "Gallery"},
		{VenueSlug: "sidney-and-matilda", Slug: "factory", Name: "Factory"},
	}

	if !roomSetsConflict(a, b) {
		t.Fatal("roomSetsConflict returned false for different room sets")
	}
}

func TestRoomEvidenceConflictsTreatsBlankEvidenceAsUnknown(t *testing.T) {
	rooms := []domain.VenueRoom{{VenueSlug: "sidney-and-matilda", Slug: "factory", Name: "Factory"}}

	if roomEvidenceConflicts("", nil, "FACTORY", rooms) {
		t.Fatal("blank evidence conflicted with concrete room evidence")
	}
	if roomEvidenceConflicts("", nil, "WHOLE VENUE", nil) {
		t.Fatal("blank evidence conflicted with text-only room evidence")
	}
}

func TestRoomEvidenceConflictsDetectsTextOnlyAgainstConcreteRooms(t *testing.T) {
	rooms := []domain.VenueRoom{{VenueSlug: "sidney-and-matilda", Slug: "factory", Name: "Factory"}}

	if !roomEvidenceConflicts("WHOLE VENUE", nil, "FACTORY", rooms) {
		t.Fatal("text-only room evidence did not conflict with concrete rooms")
	}
	if !roomEvidenceConflicts("FACTORY", rooms, "WHOLE VENUE", nil) {
		t.Fatal("concrete rooms did not conflict with text-only room evidence")
	}
}

func TestRoomEvidenceConflictsComparesTextOnlyEvidence(t *testing.T) {
	if roomEvidenceConflicts("  WHOLE   VENUE  ", nil, "Whole Venue", nil) {
		t.Fatal("equivalent text-only room evidence conflicted")
	}
	if !roomEvidenceConflicts("WHOLE VENUE", nil, "FACTORY", nil) {
		t.Fatal("different text-only room evidence did not conflict")
	}
}

func TestRoomEvidenceConflictsPrefersConcreteRoomSets(t *testing.T) {
	a := []domain.VenueRoom{{VenueSlug: "sidney-and-matilda", Slug: "factory", Name: "Factory"}}
	b := []domain.VenueRoom{{VenueSlug: "sidney-and-matilda", Slug: "factory", Name: "Factory"}}

	if roomEvidenceConflicts("FACTORY", a, "Some other text", b) {
		t.Fatal("same concrete room set conflicted because source text differed")
	}
}
