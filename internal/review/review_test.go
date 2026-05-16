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
