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
