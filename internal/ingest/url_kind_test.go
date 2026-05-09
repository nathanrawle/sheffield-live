package ingest

import "testing"

func TestIsCalendarURLTreatsOpaqueICSURLsAsCalendars(t *testing.T) {
	for _, value := range []string{
		"file:sidney.ics",
		"file:sidney.ical",
	} {
		t.Run(value, func(t *testing.T) {
			if !IsCalendarURL(value) {
				t.Fatalf("IsCalendarURL(%q) = false, want true", value)
			}
		})
	}
}

func TestURLWithTextFragmentUsesWholeEventName(t *testing.T) {
	got := URLWithTextFragment("https://www.sidneyandmatilda.com/events", "S&M Presents: Seazoo")
	want := "https://www.sidneyandmatilda.com/events#:~:text=S%26M%20Presents%3A%20Seazoo"
	if got != want {
		t.Fatalf("URLWithTextFragment(...) = %q, want %q", got, want)
	}
}

func TestURLWithTextFragmentEscapesTextDirectiveSyntax(t *testing.T) {
	got := URLWithTextFragment("https://example.test/listings", "One, Two & Three - Live")
	want := "https://example.test/listings#:~:text=One%2C%20Two%20%26%20Three%20%2D%20Live"
	if got != want {
		t.Fatalf("URLWithTextFragment(...) = %q, want %q", got, want)
	}
}

func TestURLWithTextFragmentCollapsesWhitespace(t *testing.T) {
	got := URLWithTextFragment("https://example.test/listings", "  One\tNight\nOnly  ")
	want := "https://example.test/listings#:~:text=One%20Night%20Only"
	if got != want {
		t.Fatalf("URLWithTextFragment(...) = %q, want %q", got, want)
	}
}
