package ingest

import "testing"

func TestVenueSlugFromText(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "empty",
			value: "  ",
			want:  "",
		},
		{
			name:  "sidney and matilda canonicalized",
			value: "Sidney & Matilda",
			want:  "sidney-and-matilda",
		},
		{
			name:  "rivelin works keeps its own slug",
			value: "Rivelin Works",
			want:  "rivelin-works",
		},
		{
			name:  "generic slug",
			value: "The Leadmill",
			want:  "the-leadmill",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := VenueSlugFromText(tc.value); got != tc.want {
				t.Fatalf("VenueSlugFromText(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}
