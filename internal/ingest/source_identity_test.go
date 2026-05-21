package ingest

import (
	"reflect"
	"testing"
)

func TestNormalizeDetailURL(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "normalizes case default ports fragments tracking params and slash",
			raw:  "HTTP://Example.COM:80/event/One/?b=2&utm_source=newsletter&a=1#section",
			want: "http://example.com/event/One?a=1&b=2",
			ok:   true,
		},
		{
			name: "drops default https port and sorts values",
			raw:  "https://example.com:443/event/one/?z=2&b=3&a=1&a=0",
			want: "https://example.com/event/one?a=0&a=1&b=3&z=2",
			ok:   true,
		},
		{
			name: "drops known tracking parameters",
			raw:  "https://example.com/event/one/?fbclid=abc&gclid=def&gbraid=ghi&wbraid=jkl&mc_cid=mno&mc_eid=pqr&x=1",
			want: "https://example.com/event/one?x=1",
			ok:   true,
		},
		{
			name: "preserves meaningful query values",
			raw:  "https://example.com/event/one/?ticket=general&date=2026-05-10",
			want: "https://example.com/event/one?date=2026-05-10&ticket=general",
			ok:   true,
		},
		{
			name: "normalizes root slash",
			raw:  "https://example.com:443",
			want: "https://example.com/",
			ok:   true,
		},
		{
			name: "rejects calendar listing urls",
			raw:  "https://example.com/events/",
			ok:   false,
		},
		{
			name: "rejects feed urls",
			raw:  "https://example.com/calendar.ics",
			ok:   false,
		},
		{
			name: "accepts nested plural detail urls",
			raw:  "https://example.com/events/show-123?utm_source=newsletter",
			want: "https://example.com/events/show-123",
			ok:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NormalizeDetailURL(tc.raw)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("NormalizeDetailURL(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestNormalizeEventIdentityURL(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "normalizes event detail urls",
			raw:  "HTTPS://example.com:443/event/one/?utm_source=newsletter",
			want: "https://example.com/event/one",
			ok:   true,
		},
		{
			name: "rejects root homepages",
			raw:  "https://example.com/",
			ok:   false,
		},
		{
			name: "rejects feed urls",
			raw:  "https://example.com/calendar.ics",
			ok:   false,
		},
		{
			name: "rejects listing urls with calendar query",
			raw:  "https://leadmill.co.uk/listings/?ical=1",
			ok:   false,
		},
		{
			name: "rejects event calendar urls",
			raw:  "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NormalizeEventIdentityURL(tc.raw)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("NormalizeEventIdentityURL(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestSourceIdentityKey(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "prefixes non-url identifiers as uid",
			raw:  "shared-uid",
			want: "uid:shared-uid",
			ok:   true,
		},
		{
			name: "classifies absolute http uid values as url identity",
			raw:  "HTTPS://example.com:443/event/one/?utm_source=newsletter",
			want: "url:https://example.com/event/one",
			ok:   true,
		},
		{
			name: "accepts allowlisted calendar event urls",
			raw:  "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
			want: "url:https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
			ok:   true,
		},
		{
			name: "rejects feed-shaped sidney event urls",
			raw:  "https://www.sidneyandmatilda.com/events/ical.ics?format=ical",
			ok:   false,
		},
		{
			name: "rejects feed-shaped sidney ical urls",
			raw:  "https://www.sidneyandmatilda.com/events/ical.ical?format=ical",
			ok:   false,
		},
		{
			name: "rejects stable source listing urls",
			raw:  "https://leadmill.co.uk/listings/?ical=1",
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SourceIdentityKey(tc.raw)
			if ok != tc.ok {
				t.Fatalf("SourceIdentityKey(%q) ok = %v, want %v", tc.raw, ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("SourceIdentityKey(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestSourceIdentities(t *testing.T) {
	t.Run("dedupes url identities and keeps stable order", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{
			ExternalID: "https://www.wegottickets.com/event/700001?utm_source=x",
			SourceURL:  "https://www.wegottickets.com/event/700001/",
		})

		wantKeys := []string{"url:https://www.wegottickets.com/event/700001"}
		if got := set.Keys(); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
		}

		wantLookup := []string{"url:https://www.wegottickets.com/event/700001", "https://www.wegottickets.com/event/700001"}
		if got := set.LookupKeys(); !reflect.DeepEqual(got, wantLookup) {
			t.Fatalf("LookupKeys() = %#v, want %#v", got, wantLookup)
		}

		if got, want := set.PrimaryKey(), "url:https://www.wegottickets.com/event/700001"; got != want {
			t.Fatalf("PrimaryKey() = %q, want %q", got, want)
		}
	})

	t.Run("includes uid identities", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{ExternalID: "abc"})

		wantKeys := []string{"uid:abc"}
		if got := set.Keys(); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
		}

		wantLookup := []string{"uid:abc", "abc"}
		if got := set.LookupKeys(); !reflect.DeepEqual(got, wantLookup) {
			t.Fatalf("LookupKeys() = %#v, want %#v", got, wantLookup)
		}
	})

	t.Run("accepts sidney calendar urls in external ids", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{
			ExternalID: "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
		})

		wantKeys := []string{"url:https://www.sidneyandmatilda.com/events/dealbreaker?format=ical"}
		if got := set.Keys(); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
		}
	})

	t.Run("allows sidney calendar urls through the event allowlist", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{
			CalendarURL: "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
		})

		wantKeys := []string{"url:https://www.sidneyandmatilda.com/events/dealbreaker?format=ical"}
		if got := set.Keys(); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
		}
	})

	t.Run("rejects feed-shaped sidney calendar urls", func(t *testing.T) {
		for _, calendarURL := range []string{
			"https://www.sidneyandmatilda.com/events/ical.ics?format=ical",
			"https://www.sidneyandmatilda.com/events/ical.ical?format=ical",
		} {
			set := SourceIdentities(SourceIdentityInput{
				CalendarURL: calendarURL,
			})

			if got := set.Keys(); len(got) != 0 {
				t.Fatalf("Keys(%q) = %#v, want empty", calendarURL, got)
			}
		}
	})

	t.Run("rejects generic calendar urls", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{
			CalendarURL: "https://example.com/events/dealbreaker?format=ical",
		})

		if got := set.Keys(); len(got) != 0 {
			t.Fatalf("Keys() = %#v, want empty", got)
		}
	})

	t.Run("rejects absolute url external ids that are not event identities", func(t *testing.T) {
		set := SourceIdentities(SourceIdentityInput{
			ExternalID: "https://leadmill.co.uk/listings/?ical=1",
		})

		if got := set.Keys(); len(got) != 0 {
			t.Fatalf("Keys() = %#v, want empty", got)
		}
	})
}

func TestSourceIdentitiesFromKeys(t *testing.T) {
	set := SourceIdentitiesFromKeys([]string{
		"  uid:alpha  ",
		"alpha",
		"https://example.com/event/one",
		"url:https://example.com/event/two",
	})

	wantKeys := []string{
		"uid:alpha",
		"alpha",
		"https://example.com/event/one",
		"url:https://example.com/event/two",
	}
	if got := set.Keys(); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
	}

	wantLookup := []string{
		"uid:alpha",
		"alpha",
		"https://example.com/event/one",
		"url:https://example.com/event/two",
		"https://example.com/event/two",
	}
	if got := set.LookupKeys(); !reflect.DeepEqual(got, wantLookup) {
		t.Fatalf("LookupKeys() = %#v, want %#v", got, wantLookup)
	}

	if got, want := set.PrimaryKey(), "uid:alpha"; got != want {
		t.Fatalf("PrimaryKey() = %q, want %q", got, want)
	}
}

func TestSourceIdentityLookupKeys(t *testing.T) {
	got := SourceIdentityLookupKeys([]string{
		"uid:abc",
		"url:https://example.com/event/one",
		"url:https://example.com/event/one",
	})

	want := []string{
		"uid:abc",
		"abc",
		"url:https://example.com/event/one",
		"https://example.com/event/one",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SourceIdentityLookupKeys(...) = %#v, want %#v", got, want)
	}
}
