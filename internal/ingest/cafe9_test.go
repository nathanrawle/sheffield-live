package ingest

import "testing"

func TestParseCafeNo9Page(t *testing.T) {
	result := ParseCafeNo9Page("https://www.wegottickets.com/Cafe9", readFixture(t, "cafe9_page.html"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}

	first := result.Candidates[0]
	if got, want := first.UID, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := first.URL, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := first.Summary, "An evening with Ellie Gowers at Cafe No9"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := first.Location, "Cafe No9"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := first.StartAt, "2026-05-10T18:30:00Z"; got != want {
		t.Fatalf("start = %q, want %q", got, want)
	}
	if got, want := first.Description, "With special support from Robbie Thompson"; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}

	if got, want := result.Skips[0].Reason, "non-music category"; got != want {
		t.Fatalf("first skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "not a Cafe No. 9 venue listing"; got != want {
		t.Fatalf("second skip reason = %q, want %q", got, want)
	}
}

func TestParseCafeNo9SourcePageAppliesLimit(t *testing.T) {
	result := ParseCafeNo9SourcePage("https://www.wegottickets.com/Cafe9", readFixture(t, "cafe9_page.html"), 1)

	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].UID, "https://www.wegottickets.com/event/700001"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
}

func TestExtractCafeNo9SourcePageLinks(t *testing.T) {
	links, err := ExtractCafeNo9SourcePageLinks("https://www.wegottickets.com/Cafe9", readFixture(t, "cafe9_paged_1.html"), 20)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if got, want := len(links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := links[0], "https://www.wegottickets.com/Cafe9/page/2"; got != want {
		t.Fatalf("link = %q, want %q", got, want)
	}
}

func TestExtractCafeNo9SourcePageLinksSkipsPageOneAlias(t *testing.T) {
	links, err := ExtractCafeNo9SourcePageLinks("https://www.wegottickets.com/Cafe9", []byte(`
		<div class="pagination">
			<a href="/Cafe9/page/1#paginate">1</a>
			<a href="/Cafe9/page/2#paginate">2</a>
			<a href="/Cafe9/page/2#paginate">next</a>
		</div>
	`), 20)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if got, want := len(links), 1; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := links[0], "https://www.wegottickets.com/Cafe9/page/2"; got != want {
		t.Fatalf("link = %q, want %q", got, want)
	}
}

func TestExtractCafeNo9SourcePageLinksNormalizesPageOneAliasFromPaginatedPage(t *testing.T) {
	links, err := ExtractCafeNo9SourcePageLinks("https://www.wegottickets.com/Cafe9/page/2", []byte(`
		<div class="pagination">
			<a href="/Cafe9/page/1#paginate">1</a>
			<a href="/Cafe9/page/3#paginate">3</a>
		</div>
	`), 20)
	if err != nil {
		t.Fatalf("extract links: %v", err)
	}
	if got, want := len(links), 2; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := links[0], "https://www.wegottickets.com/Cafe9"; got != want {
		t.Fatalf("links[0] = %q, want %q", got, want)
	}
	if got, want := links[1], "https://www.wegottickets.com/Cafe9/page/3"; got != want {
		t.Fatalf("links[1] = %q, want %q", got, want)
	}
}

func TestParseCafeNo9PageAllowsMissingCategoryOnPaginatedPage(t *testing.T) {
	result := ParseCafeNo9Page("https://www.wegottickets.com/Cafe9/page/2", readFixture(t, "cafe9_paged_2.html"))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 2; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].UID, "https://www.wegottickets.com/event/700101"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
	if got, want := result.Candidates[0].Summary, "An evening with Late Train at Cafe No9"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := result.Skips[0].Reason, "missing music category"; got != want {
		t.Fatalf("first skip reason = %q, want %q", got, want)
	}
	if got, want := result.Skips[1].Reason, "not a Cafe No. 9 venue listing"; got != want {
		t.Fatalf("second skip reason = %q, want %q", got, want)
	}
}

func TestParseCafeNo9PageAllowsMissingCategoryOnRootPageForMusicTitles(t *testing.T) {
	result := ParseCafeNo9Page("https://www.wegottickets.com/Cafe9/", []byte(`
		<h2><a href="/event/700301">An evening with Root Page Artist at Cafe No9</a></h2>
		<p>0 SHEFFIELD: Cafe No9</p>
		<p>P Sunday 17th May, 2026</p>
		<p>N Door time: 7:00pm, Start time: 7:30pm</p>
		<p><a href="/event/700301">Event info</a></p>
	`))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 1; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 0; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Candidates[0].UID, "https://www.wegottickets.com/event/700301"; got != want {
		t.Fatalf("uid = %q, want %q", got, want)
	}
}

func TestParseCafeNo9PageSkipsMissingCategoryOnRootPageForNonMusicTitles(t *testing.T) {
	result := ParseCafeNo9Page("https://www.wegottickets.com/Cafe9/", []byte(`
		<h2><a href="/event/700302">A Christmas Carol - Story told by Jason Buck</a></h2>
		<p>0 SHEFFIELD: Cafe No9</p>
		<p>P Monday 18th May, 2026</p>
		<p>N Door time: 7:00pm, Start time: 7:30pm</p>
		<p><a href="/event/700302">Event info</a></p>
	`))

	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", result.Errors)
	}
	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "missing music category"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseCafeNo9PageSkipsMissingStartTime(t *testing.T) {
	result := ParseCafeNo9Page("https://www.wegottickets.com/Cafe9", []byte(`
		<h2><a href="/event/700005">Untimed at Cafe No9</a></h2>
		<p>0 SHEFFIELD: Cafe#9</p>
		<p>P Sunday 10th May, 2026</p>
		<p>C Music - General</p>
	`))

	if got, want := len(result.Candidates), 0; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := len(result.Skips), 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := result.Skips[0].Reason, "missing event start time"; got != want {
		t.Fatalf("skip reason = %q, want %q", got, want)
	}
}

func TestParseCafeNo9DetailPageExtractsEventInformation(t *testing.T) {
	detail := ParseCafeNo9DetailPage("https://www.wegottickets.com/event/681615", readFixture(t, "cafe9_detail.html"))

	if got, want := detail.URL, "https://www.wegottickets.com/event/681615"; got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
	if got, want := detail.Summary, "An evening with The Leisure Society at Cafe No9"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := detail.Description, "The Leisure Society were founded by Nick Hemming.\nExpect oustanding songwriting and production craft."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}
