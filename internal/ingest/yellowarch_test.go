package ingest

import "testing"

func TestParseYellowArchPage(t *testing.T) {
	result := ParseYellowArchPage(readFixture(t, "yellow_arch.html"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d: %#v", got, want, result.Candidates)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.Summary, "Late Junction"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := first.URL, "/event/late-junction/"; got != want {
		t.Fatalf("raw url = %q, want %q", got, want)
	}
	if got, want := first.Location, "Yellow Arch Studios"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2026-05-10T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := first.EndAt, "2026-05-10T22:00:00Z"; got != want {
		t.Fatalf("end = %q, want %q", got, want)
	}

	if got, want := result.Skips[0].Reason, "missing event start time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseYellowArchSourcePageResolvesRelativeURLsAndAppliesLimit(t *testing.T) {
	result := ParseYellowArchSourcePage("https://www.yellowarch.com/events/", readFixture(t, "yellow_arch.html"), 1)

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].URL, "https://www.yellowarch.com/event/late-junction/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := result.Candidates[0].UID, "https://www.yellowarch.com/event/late-junction/"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
}

func TestParseYellowArchPageFailsWithoutEventData(t *testing.T) {
	result := ParseYellowArchPage([]byte(`<html><head><script type="application/ld+json">{"@graph":[{"@type":"WebPage"}]}</script></head></html>`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Errors), 1; got != want {
		t.Fatalf("errors = %#v, want %d", result.Errors, want)
	}
}

func TestParseYellowArchPageSkipsEventsMissingTimes(t *testing.T) {
	result := ParseYellowArchPage([]byte(`
		<script type="application/ld+json">
			[{"@type":"Event","name":"Untimed","startDate":"2026-05-01T19:00","location":{"name":"Yellow Arch Studios"}}]
		</script>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %#v, want %d", result.Skips, want)
	}
	if got, want := result.Skips[0].Reason, "missing event end time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseYellowArchPageNormalizesHTMLDescriptionBreaks(t *testing.T) {
	result := ParseYellowArchPage([]byte(`
		<script type="application/ld+json">
			[{
				"@type":"Event",
				"name":"HTML Description",
				"description":"Aggressive Management Presents<br>COPPER LUNGS<br><br><strong>The Electric Lives Tour</strong><p>Support from <em>Cargos</em></p>",
				"startDate":"2026-05-01T19:00",
				"endDate":"2026-05-01T22:00",
				"location":{"name":"Yellow Arch Studios"}
			}]
		</script>
	`))

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d: %#v", got, want, result.Candidates)
	}
	want := "Aggressive Management Presents\nCOPPER LUNGS\n\n**The Electric Lives Tour**\n\nSupport from _Cargos_"
	if got := result.Candidates[0].Description; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestParseYellowArchDetailPageExtractsVisibleEventContent(t *testing.T) {
	detail := ParseYellowArchDetailPage("https://www.yellowarch.com/event/jazz-hot-six-14/", []byte(`
		<html>
		  <head>
		    <link rel="canonical" href="/event/jazz-hot-six-14/">
		  </head>
		  <body>
		    <h1>Jazz Hot Six</h1>
		    <div class="event-single__content">
		      <p>Every first Tuesday of the month</p>
		      <p>Martin Winning - Clarinet<br />Emily Chaplais - Violin<br />Shez Sheridan - Guitar, Lap Steel, Vocals</p>
		      <div class="event-single__venue-address-wrapper">
		        <h3>Venue</h3>
		        <span>Yellow Arch Studios</span>
		      </div>
		    </div>
		  </body>
		</html>
	`))

	if got, want := detail.URL, "https://www.yellowarch.com/event/jazz-hot-six-14/"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := detail.URLAliases, []string{"https://www.yellowarch.com/event/jazz-hot-six-14/"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("url aliases = %#v, want %#v", got, want)
	}
	if got, want := detail.Summary, "Jazz Hot Six"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	want := "Every first Tuesday of the month\n\nMartin Winning - Clarinet\nEmily Chaplais - Violin\nShez Sheridan - Guitar, Lap Steel, Vocals"
	if got := detail.Description; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestParseYellowArchDetailPageFailsClosedOnMissingOrAmbiguousBoundary(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "missing content",
			body: `<div class="event-single__venue-address-wrapper">Venue</div>`,
		},
		{
			name: "missing venue boundary",
			body: `<div class="event-single__content"><p>Description</p></div>`,
		},
		{
			name: "ambiguous content",
			body: `
				<div class="event-single__content"><p>One</p><div class="event-single__venue-address-wrapper">Venue</div></div>
				<div class="event-single__content"><p>Two</p><div class="event-single__venue-address-wrapper">Venue</div></div>
			`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			detail := ParseYellowArchDetailPage("https://www.yellowarch.com/event/test/", []byte(tc.body))
			if detail.Description != "" {
				t.Fatalf("description = %q, want empty", detail.Description)
			}
		})
	}
}
