package review

import (
	"testing"

	"sheffield-live/internal/domain"
)

func TestRoomSlugsValueSortsAndDeduplicates(t *testing.T) {
	rooms := []domain.VenueRoom{
		{Slug: "gallery", Name: "Gallery"},
		{Slug: "basement", Name: "Basement"},
		{Slug: "gallery", Name: "Gallery"},
	}

	if got, want := RoomSlugsValue(rooms), "basement, gallery"; got != want {
		t.Fatalf("RoomSlugsValue = %q, want %q", got, want)
	}
}

func TestCandidateRoomValueUsesTextOnlyEvidenceWhenNoRoomsAreLinked(t *testing.T) {
	candidate := Candidate{RoomText: "  WHOLE   VENUE  "}

	if got, want := CandidateValue(candidate, FieldRoomSlugs), "WHOLE VENUE"; got != want {
		t.Fatalf("CandidateValue room field = %q, want %q", got, want)
	}
}

func TestCandidateRoomValuePrefersRoomSlugsOverSourceText(t *testing.T) {
	candidate := Candidate{
		RoomText: "FACTORY",
		Rooms: []domain.VenueRoom{
			{Slug: "factory", Name: "Factory"},
		},
	}

	if got, want := CandidateValue(candidate, FieldRoomSlugs), "factory"; got != want {
		t.Fatalf("CandidateValue room field = %q, want %q", got, want)
	}
}
