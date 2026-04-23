package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunManualSnapshotsAndReportsWithoutEventWrites(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:         "https://www.sidneyandmatilda.com/",
				FinalURL:    "https://www.sidneyandmatilda.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "sidney.html"),
				CapturedAt:  time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
			"https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live": {
				URL:         "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				FinalURL:    "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/calendar",
				Body:        readFixture(t, "sidney.ics"),
				CapturedAt:  time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 1})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if report.Status != importStatusSucceeded {
		t.Fatalf("status = %q, want %q", report.Status, importStatusSucceeded)
	}
	if got, want := len(store.snapshots), 2; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 4; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := store.eventWrites, 0; got != want {
		t.Fatalf("event writes = %d, want %d", got, want)
	}

	var envelope SnapshotEnvelope
	if err := json.Unmarshal([]byte(store.snapshots[0].payload), &envelope); err != nil {
		t.Fatalf("unmarshal snapshot envelope: %v", err)
	}
	if envelope.Body == "" || envelope.SHA256 == "" {
		t.Fatalf("snapshot envelope missing body/sha: %#v", envelope)
	}
}

func TestRunManualYellowArchParsesListingsFromSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 23, 19, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.yellowarch.com/events/": {
				URL:         "https://www.yellowarch.com/events/",
				FinalURL:    "https://www.yellowarch.com/events/",
				Status:      "200 OK",
				StatusCode:  200,
				ContentType: "text/html",
				Body:        readFixture(t, "yellow_arch.html"),
				CapturedAt:  time.Date(2026, 4, 23, 19, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: yellowArchSource, Limit: 20})
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}

	if got, want := report.Status, importStatusSucceeded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Links, 0; got != want {
		t.Fatalf("links = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Skips, 1; got != want {
		t.Fatalf("skips = %d, want %d", got, want)
	}
	if got, want := len(report.Calendars), 1; got != want {
		t.Fatalf("calendars = %d, want %d", got, want)
	}
	if got, want := report.Calendars[0].Candidates[0].Location, "Yellow Arch Studios"; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
	if got, want := store.finishedNotes, "links=0 candidates=2 skips=1 errors=0"; got != want {
		t.Fatalf("finished notes = %q, want %q", got, want)
	}
}

func TestRunManualSourceFetchFailureReturnsErrRunFailed(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		err: errors.New("source fetch failed"),
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], "source fetch failed") {
		t.Fatalf("report errors = %#v, want source fetch failure", report.Errors)
	}
	if store.finishedStatus != importStatusFailed {
		t.Fatalf("finished status = %q, want failed", store.finishedStatus)
	}
	if !strings.Contains(store.finishedNotes, "source fetch failed") {
		t.Fatalf("finished notes = %q, want recorded fetch error", store.finishedNotes)
	}
	if got := len(store.snapshots); got != 0 {
		t.Fatalf("snapshots = %d, want 0", got)
	}
}

func TestRunManualFailsClosedWhenNoLinks(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       []byte(`<a href="/calendar.ics">Other calendar</a>`),
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected report error")
	}
	if store.finishedStatus != importStatusFailed {
		t.Fatalf("finished status = %q, want failed", store.finishedStatus)
	}
}

func TestRunManualSnapshotsThenFailsOnTruncatedSourcePage(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.html"),
				Truncated:  true,
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 20})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if got, want := len(store.snapshots), 1; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if report.Page == nil || !report.Page.Truncated {
		t.Fatalf("page snapshot truncated = %#v, want true", report.Page)
	}
	if got := len(report.Calendars); got != 0 {
		t.Fatalf("calendars = %d, want 0", got)
	}
	if got, want := report.Totals.Errors, 1; got != want {
		t.Fatalf("errors = %d, want %d", got, want)
	}
}

func TestRunManualCalendarErrorsFailStatusAndNotes(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{now: time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)}
	fetcher := fakeFetcher{
		results: map[string]FetchResult{
			"https://www.sidneyandmatilda.com/": {
				URL:        "https://www.sidneyandmatilda.com/",
				FinalURL:   "https://www.sidneyandmatilda.com/events/",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.html"),
				CapturedAt: time.Date(2026, 4, 20, 12, 1, 0, 0, time.UTC),
			},
			"https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live": {
				URL:        "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				FinalURL:   "https://www.sidneyandmatilda.com/calendar-one.ics?name=Sidney&kind=live",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.ics"),
				CapturedAt: time.Date(2026, 4, 20, 12, 2, 0, 0, time.UTC),
			},
			"https://calendar.example.test/calendar-two.ics": {
				URL:        "https://calendar.example.test/calendar-two.ics",
				FinalURL:   "https://calendar.example.test/calendar-two.ics",
				Status:     "200 OK",
				StatusCode: 200,
				Body:       readFixture(t, "sidney.ics"),
				Truncated:  true,
				CapturedAt: time.Date(2026, 4, 20, 12, 3, 0, 0, time.UTC),
			},
		},
	}

	report, err := RunManual(ctx, store, fetcher, Options{Source: DefaultSource, Limit: 2})
	if !errors.Is(err, ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}
	if report.Status != importStatusFailed {
		t.Fatalf("status = %q, want %q", report.Status, importStatusFailed)
	}
	if got, want := len(store.snapshots), 3; got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
	if got, want := report.Totals.Candidates, 3; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := report.Totals.Errors, 1; got != want {
		t.Fatalf("errors = %d, want %d", got, want)
	}
	if len(report.Calendars) != 2 {
		t.Fatalf("calendars = %d, want 2", len(report.Calendars))
	}
	if got := len(report.Calendars[1].Candidates); got != 0 {
		t.Fatalf("truncated calendar candidates = %d, want 0", got)
	}
	if !strings.Contains(store.finishedNotes, "links=2 candidates=3 skips=4 errors=1") {
		t.Fatalf("finished notes = %q, want aggregate summary", store.finishedNotes)
	}
	if !strings.Contains(store.finishedNotes, "ICS response was truncated") {
		t.Fatalf("finished notes = %q, want truncated calendar detail", store.finishedNotes)
	}
	if !strings.Contains(store.finishedNotes, "errors=1") {
		t.Fatalf("finished notes = %q, want errors=1", store.finishedNotes)
	}
}

type fakeFetcher struct {
	results map[string]FetchResult
	err     error
}

func (f fakeFetcher) Fetch(_ context.Context, url string) (FetchResult, error) {
	if f.err != nil {
		return FetchResult{}, f.err
	}
	result, ok := f.results[url]
	if !ok {
		return FetchResult{}, errors.New("unexpected fetch " + url)
	}
	return result, nil
}

type fakeStore struct {
	now            time.Time
	nextSourceID   int64
	nextRunID      int64
	nextSnapshotID int64
	sources        map[string]int64
	snapshots      []fakeSnapshot
	finishedStatus string
	finishedNotes  string
	eventWrites    int
}

type fakeSnapshot struct {
	runID      int64
	sourceID   *int64
	capturedAt time.Time
	payload    string
}

func (s *fakeStore) EnsureSource(_ context.Context, name, url string) (int64, error) {
	if s.sources == nil {
		s.sources = make(map[string]int64)
	}
	key := name + "\x00" + url
	if id, ok := s.sources[key]; ok {
		return id, nil
	}
	s.nextSourceID++
	s.sources[key] = s.nextSourceID
	return s.nextSourceID, nil
}

func (s *fakeStore) CreateImportRun(_ context.Context, _, _ string) (int64, time.Time, error) {
	s.nextRunID++
	return s.nextRunID, s.now, nil
}

func (s *fakeStore) CreateSnapshot(_ context.Context, runID int64, sourceID *int64, capturedAt time.Time, payload string) (int64, time.Time, error) {
	s.nextSnapshotID++
	var sourceCopy *int64
	if sourceID != nil {
		value := *sourceID
		sourceCopy = &value
	}
	s.snapshots = append(s.snapshots, fakeSnapshot{
		runID:      runID,
		sourceID:   sourceCopy,
		capturedAt: capturedAt,
		payload:    payload,
	})
	return s.nextSnapshotID, capturedAt, nil
}

func (s *fakeStore) FinishImportRun(_ context.Context, _ int64, status, notes string) (time.Time, error) {
	s.finishedStatus = status
	s.finishedNotes = notes
	return s.now.Add(time.Minute), nil
}
