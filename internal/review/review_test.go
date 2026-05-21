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

func TestGroupKindParseDefaultsBlankToStandard(t *testing.T) {
	kind, ok := ParseGroupKind("")
	if !ok {
		t.Fatal("ParseGroupKind blank value failed")
	}
	if kind != GroupKindStandard {
		t.Fatalf("ParseGroupKind blank value = %q, want %q", kind, GroupKindStandard)
	}
	if !GroupKindHistoricalDuplicate.Valid() {
		t.Fatal("historical duplicate group kind should be valid")
	}
}

func TestHistoricalDuplicateActionValidation(t *testing.T) {
	if !HistoricalDuplicateActionKeep.Valid() {
		t.Fatal("keep action should be valid")
	}
	if !HistoricalDuplicateActionWithhold.Valid() {
		t.Fatal("withhold action should be valid")
	}
	if HistoricalDuplicateAction("").Valid() {
		t.Fatal("blank historical duplicate action should be invalid")
	}
}
