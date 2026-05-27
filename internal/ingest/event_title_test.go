package ingest

import "testing"

func TestStripVenueNameFromEventTitle(t *testing.T) {
	tests := []struct {
		name      string
		title     string
		venueSlug string
		want      string
	}{
		{
			name:      "prefix colon",
			title:     "The Leadmill: Matinee Noise",
			venueSlug: "leadmill",
			want:      "Matinee Noise",
		},
		{
			name:      "prefix dash",
			title:     "Sidney and Matilda - Courtyard Wildcards",
			venueSlug: "sidney-and-matilda",
			want:      "Courtyard Wildcards",
		},
		{
			name:      "suffix dash",
			title:     "Maybe Gold - Yellow Arch Studios",
			venueSlug: "yellow-arch",
			want:      "Maybe Gold",
		},
		{
			name:      "suffix at sign",
			title:     "Late Show @ Cafe No. 9",
			venueSlug: "cafe-no-9",
			want:      "Late Show",
		},
		{
			name:      "suffix at word",
			title:     "An evening with Ellie Gowers at Cafe No9",
			venueSlug: "cafe-no-9",
			want:      "Ellie Gowers",
		},
		{
			name:      "suffix double slash",
			title:     "Club Night // Yellow Arch",
			venueSlug: "yellow-arch",
			want:      "Club Night",
		},
		{
			name:      "parenthetical venue suffix",
			title:     "Club Night (Yellow Arch)",
			venueSlug: "yellow-arch",
			want:      "Club Night",
		},
		{
			name:      "dash suffix with parenthetical qualifier",
			title:     "PINS plus Gia Ford & Gelder - Yellow Arch (Rescheduled Date)",
			venueSlug: "yellow-arch",
			want:      "PINS plus Gia Ford & Gelder",
		},
		{
			name:      "at suffix with first back to back qualifier",
			title:     "An evening with Artist at Cafe No. 9 (the first of two back to back shows)",
			venueSlug: "cafe-no-9",
			want:      "Artist",
		},
		{
			name:      "at suffix with second back to back qualifier",
			title:     "An evening with Artist at Cafe No. 9 (the second of two back to back shows)",
			venueSlug: "cafe-no-9",
			want:      "Artist",
		},
		{
			name:      "at suffix with dash qualifier",
			title:     "An evening with The 20ft Squid Blues Band at Cafe No9 - The first of two back to back shows",
			venueSlug: "cafe-no-9",
			want:      "The 20ft Squid Blues Band",
		},
		{
			name:      "cafe no 9 house prefix",
			title:     "An evening with Ellie Gowers",
			venueSlug: "cafe-no-9",
			want:      "Ellie Gowers",
		},
		{
			name:      "cafe no 9 house prefix is venue scoped",
			title:     "An evening with Ellie Gowers",
			venueSlug: "sidney-and-matilda",
			want:      "An evening with Ellie Gowers",
		},
		{
			name:      "html entities are decoded",
			title:     "S&amp;amp;M Presents: Dealbreaker",
			venueSlug: "sidney-and-matilda",
			want:      "S&M Presents: Dealbreaker",
		},
		{
			name:      "leading punctuation is trimmed",
			title:     "| Sorebones | EP Release Show w/ YURN & Ella Wingfield",
			venueSlug: "sidney-and-matilda",
			want:      "Sorebones | EP Release Show w/ YURN & Ella Wingfield",
		},
		{
			name:      "case and whitespace",
			title:     "  maybe   gold   -   yellow   arch  ",
			venueSlug: "yellow-arch",
			want:      "maybe gold",
		},
		{
			name:      "corporation alias",
			title:     "Frog Lord at Corporation Sheffield",
			venueSlug: "corporation",
			want:      "Frog Lord",
		},
		{
			name:      "network room suffix",
			title:     "GODETH | Network 3",
			venueSlug: "network",
			want:      "GODETH",
		},
		{
			name:      "network sheffield suffix",
			title:     "Park Drive - Network Sheffield",
			venueSlug: "network",
			want:      "Park Drive",
		},
		{
			name:      "network venue suffix",
			title:     "Club Night @ Network",
			venueSlug: "network",
			want:      "Club Night",
		},
		{
			name:      "bare prefix is preserved",
			title:     "Cafe No. 9 Late Show",
			venueSlug: "cafe-no-9",
			want:      "Cafe No. 9 Late Show",
		},
		{
			name:      "embedded venue phrase is preserved",
			title:     "Jazz at The Lescar Quartet",
			venueSlug: "lescar",
			want:      "Jazz at The Lescar Quartet",
		},
		{
			name:      "branded sidney and matilda abbreviation is preserved",
			title:     "S&M Presents: Dealbreaker",
			venueSlug: "sidney-and-matilda",
			want:      "S&M Presents: Dealbreaker",
		},
		{
			name:      "partial word is preserved",
			title:     "The Leadmillers - Big Night",
			venueSlug: "leadmill",
			want:      "The Leadmillers - Big Night",
		},
		{
			name:      "empty result is preserved",
			title:     "The Greystones - ",
			venueSlug: "greystones",
			want:      "The Greystones -",
		},
		{
			name:      "unknown venue slug is preserved",
			title:     "Late Show - Unknown Room",
			venueSlug: "unknown-room",
			want:      "Late Show - Unknown Room",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripVenueNameFromEventTitle(tc.title, tc.venueSlug); got != tc.want {
				t.Fatalf("stripVenueNameFromEventTitle(%q, %q) = %q, want %q", tc.title, tc.venueSlug, got, tc.want)
			}
		})
	}
}

func TestCleanEventCandidateSummaryForConfigUsesSourceVenueNormalizer(t *testing.T) {
	cfg := sourceConfig{
		Key:                   LeadmillSource,
		VenueNormalizerFamily: "leadmill",
	}
	candidate := EventCandidate{
		Summary:  "Maybe Gold - Yellow Arch",
		Location: "Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX",
	}

	if got, want := cleanEventCandidateSummaryForConfig(cfg, candidate), "Maybe Gold"; got != want {
		t.Fatalf("clean summary = %q, want %q", got, want)
	}
}
