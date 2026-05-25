package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	seedstore "sheffield-live/internal/store"
)

const mediaCleanupLocationName = "Europe/London"

type MediaCleanupOptions struct {
	Apply          bool
	Now            time.Time
	Location       *time.Location
	MediaURLPrefix string
	ExistingFiles  []string
}

type MediaCleanupReport struct {
	DryRun             bool               `json:"dry_run"`
	Applied            bool               `json:"applied"`
	ScannedAssets      int                `json:"scanned_assets"`
	ScannedOrphanFiles int                `json:"scanned_orphan_files"`
	ClearedEventImages int                `json:"cleared_event_images"`
	DeletedAssetRows   int                `json:"deleted_asset_rows"`
	DeletedFiles       int                `json:"deleted_files"`
	MissingFiles       int                `json:"missing_files"`
	RetainedFiles      int                `json:"retained_files"`
	Warnings           []string           `json:"warnings,omitempty"`
	Errors             []string           `json:"errors,omitempty"`
	Items              []MediaCleanupItem `json:"items,omitempty"`

	FilesToDelete        []string `json:"-"`
	KnownStoragePaths    []string `json:"-"`
	RetainedStoragePaths []string `json:"-"`
	RetainedPublicURLs   []string `json:"-"`
}

type MediaCleanupItem struct {
	Action      string `json:"action"`
	Reason      string `json:"reason,omitempty"`
	StoragePath string `json:"storage_path,omitempty"`
	SourceURL   string `json:"source_url,omitempty"`
	PublicURL   string `json:"public_url,omitempty"`
	EventSlug   string `json:"event_slug,omitempty"`
	ClusterID   int64  `json:"cluster_id,omitempty"`
	EvidenceID  int64  `json:"evidence_id,omitempty"`
}

type mediaCleanupAsset struct {
	SourceURL   string
	PublicURL   string
	StoragePath string
}

type mediaCleanupEventImageRow struct {
	ID             int64
	Slug           string
	Start          time.Time
	End            time.Time
	ImageURL       string
	ImageSourceURL string
}

type mediaCleanupSourceImageRow struct {
	SourceImageID int64
	mediaCleanupEventImageRow
}

type mediaCleanupEvidencePayload struct {
	StartAt        string `json:"candidate_start_at"`
	EndAt          string `json:"candidate_end_at"`
	ImageURL       string `json:"candidate_image_url,omitempty"`
	ImageSourceURL string `json:"candidate_image_source_url,omitempty"`
}

type mediaCleanupOpenEvidenceRow struct {
	ClusterID  int64
	EvidenceID int64
	Payload    string
}

type mediaCleanupState struct {
	opts                 MediaCleanupOptions
	report               MediaCleanupReport
	assets               []mediaCleanupAsset
	assetBySourceURL     map[string][]int
	assetByPublicURL     map[string][]int
	retainedAssets       map[int]struct{}
	retainedStorage      map[string]struct{}
	retainedPublicURLs   map[string]struct{}
	knownStorage         map[string]struct{}
	existingFiles        map[string]struct{}
	clearEventIDs        []int64
	deleteSourceImageIDs []int64
	deleteAssetSources   []string
	filesToDelete        map[string]struct{}
}

func (s *Store) CleanupMedia(ctx context.Context, opts MediaCleanupOptions) (MediaCleanupReport, error) {
	if s == nil || s.db == nil {
		return MediaCleanupReport{}, errors.New("sqlite store is not open")
	}
	opts, err := normalizeMediaCleanupOptions(opts)
	if err != nil {
		return MediaCleanupReport{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MediaCleanupReport{}, err
	}
	defer tx.Rollback()

	state := newMediaCleanupState(opts)
	if err := state.loadAssets(ctx, tx); err != nil {
		return MediaCleanupReport{}, err
	}
	if err := state.classifyEventRows(ctx, tx); err != nil {
		return MediaCleanupReport{}, err
	}
	if err := state.classifySourceImageRows(ctx, tx); err != nil {
		return MediaCleanupReport{}, err
	}
	if err := state.classifyOpenEvidenceRows(ctx, tx); err != nil {
		return MediaCleanupReport{}, err
	}
	state.classifyAssetRows()
	if opts.Apply {
		if err := state.applyDBCleanup(ctx, tx); err != nil {
			return MediaCleanupReport{}, err
		}
		if err := tx.Commit(); err != nil {
			return MediaCleanupReport{}, err
		}
	}
	state.finalizeReport()
	return state.report, nil
}

func normalizeMediaCleanupOptions(opts MediaCleanupOptions) (MediaCleanupOptions, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	opts.Now = opts.Now.UTC()
	if opts.Location == nil {
		loc, err := time.LoadLocation(mediaCleanupLocationName)
		if err != nil {
			return MediaCleanupOptions{}, fmt.Errorf("load %s location: %w", mediaCleanupLocationName, err)
		}
		opts.Location = loc
	}
	opts.MediaURLPrefix = normalizeMediaCleanupURLPrefix(opts.MediaURLPrefix)
	opts.ExistingFiles = normalizedStoragePathList(opts.ExistingFiles)
	return opts, nil
}

func normalizeMediaCleanupURLPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "/media"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return "/media"
	}
	return prefix
}

func newMediaCleanupState(opts MediaCleanupOptions) *mediaCleanupState {
	return &mediaCleanupState{
		opts: opts,
		report: MediaCleanupReport{
			DryRun:  !opts.Apply,
			Applied: opts.Apply,
		},
		assetBySourceURL:   make(map[string][]int),
		assetByPublicURL:   make(map[string][]int),
		retainedAssets:     make(map[int]struct{}),
		retainedStorage:    make(map[string]struct{}),
		retainedPublicURLs: make(map[string]struct{}),
		knownStorage:       make(map[string]struct{}),
		existingFiles:      stringSet(opts.ExistingFiles),
		filesToDelete:      make(map[string]struct{}),
	}
}

func (s *mediaCleanupState) loadAssets(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `
		SELECT source_url, public_url, storage_path
		FROM image_assets
		ORDER BY source_url
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var asset mediaCleanupAsset
		if err := rows.Scan(&asset.SourceURL, &asset.PublicURL, &asset.StoragePath); err != nil {
			return err
		}
		asset.SourceURL = strings.TrimSpace(asset.SourceURL)
		asset.PublicURL = strings.TrimSpace(asset.PublicURL)
		asset.StoragePath = normalizeStoragePath(asset.StoragePath)
		idx := len(s.assets)
		s.assets = append(s.assets, asset)
		if asset.SourceURL != "" {
			s.assetBySourceURL[asset.SourceURL] = append(s.assetBySourceURL[asset.SourceURL], idx)
		}
		if asset.PublicURL != "" {
			s.assetByPublicURL[asset.PublicURL] = append(s.assetByPublicURL[asset.PublicURL], idx)
		}
		if asset.StoragePath != "" {
			s.knownStorage[asset.StoragePath] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.report.ScannedAssets = len(s.assets)
	return nil
}

func (s *mediaCleanupState) classifyEventRows(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
			slug,
			start_at,
			end_at,
			image_url,
			image_source_url
		FROM events
		WHERE TRIM(image_url) <> ''
			OR TRIM(image_source_url) <> ''
		ORDER BY id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		event, err := scanMediaCleanupEventImageRow(rows)
		if err != nil {
			return err
		}
		displayEnd, ok := mediaCleanupDisplayEnd(event.Start, event.End, s.opts.Location)
		if !ok {
			return fmt.Errorf("event %q has invalid display end", event.Slug)
		}
		passed := !displayEnd.After(s.opts.Now)
		if passed {
			if s.eventImageIsManaged(event) {
				s.clearEventIDs = append(s.clearEventIDs, event.ID)
				s.report.ClearedEventImages++
				s.report.Items = append(s.report.Items, MediaCleanupItem{
					Action:    "clear_event_image",
					Reason:    "event_passed",
					EventSlug: event.Slug,
					PublicURL: strings.TrimSpace(event.ImageURL),
					SourceURL: strings.TrimSpace(event.ImageSourceURL),
				})
			}
			continue
		}
		s.retainImageRefs(event.ImageSourceURL, event.ImageURL)
	}
	return rows.Err()
}

func scanMediaCleanupEventImageRow(rows *sql.Rows) (mediaCleanupEventImageRow, error) {
	var event mediaCleanupEventImageRow
	var startText string
	var endText sql.NullString
	if err := rows.Scan(&event.ID, &event.Slug, &startText, &endText, &event.ImageURL, &event.ImageSourceURL); err != nil {
		return mediaCleanupEventImageRow{}, err
	}
	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return mediaCleanupEventImageRow{}, fmt.Errorf("parse event %q start time: %w", event.Slug, err)
	}
	end, err := parseNullableRFC3339UTC(endText)
	if err != nil {
		return mediaCleanupEventImageRow{}, fmt.Errorf("parse event %q end time: %w", event.Slug, err)
	}
	event.Start = start
	event.End = end
	event.ImageURL = strings.TrimSpace(event.ImageURL)
	event.ImageSourceURL = strings.TrimSpace(event.ImageSourceURL)
	return event, nil
}

func (s *mediaCleanupState) classifySourceImageRows(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `
		SELECT
			i.id,
			e.id,
			e.slug,
			e.start_at,
			e.end_at,
			i.image_url,
			i.image_source_url
		FROM event_source_images i
		JOIN events e ON e.id = i.event_id
		WHERE TRIM(i.image_url) <> ''
			OR TRIM(i.image_source_url) <> ''
		ORDER BY i.id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		row, err := scanMediaCleanupSourceImageRow(rows)
		if err != nil {
			return err
		}
		displayEnd, ok := mediaCleanupDisplayEnd(row.Start, row.End, s.opts.Location)
		if !ok {
			return fmt.Errorf("event %q has invalid display end", row.Slug)
		}
		if displayEnd.After(s.opts.Now) {
			s.retainImageRefs(row.ImageSourceURL, row.ImageURL)
			continue
		}
		if s.eventImageIsManaged(row.mediaCleanupEventImageRow) {
			s.deleteSourceImageIDs = append(s.deleteSourceImageIDs, row.SourceImageID)
			s.report.Items = append(s.report.Items, MediaCleanupItem{
				Action:    "delete_event_source_image",
				Reason:    "event_passed",
				EventSlug: row.Slug,
				PublicURL: strings.TrimSpace(row.ImageURL),
				SourceURL: strings.TrimSpace(row.ImageSourceURL),
			})
		}
	}
	return rows.Err()
}

func scanMediaCleanupSourceImageRow(rows *sql.Rows) (mediaCleanupSourceImageRow, error) {
	var row mediaCleanupSourceImageRow
	var startText string
	var endText sql.NullString
	if err := rows.Scan(&row.SourceImageID, &row.ID, &row.Slug, &startText, &endText, &row.ImageURL, &row.ImageSourceURL); err != nil {
		return mediaCleanupSourceImageRow{}, err
	}
	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return mediaCleanupSourceImageRow{}, fmt.Errorf("parse event %q start time: %w", row.Slug, err)
	}
	end, err := parseNullableRFC3339UTC(endText)
	if err != nil {
		return mediaCleanupSourceImageRow{}, fmt.Errorf("parse event %q end time: %w", row.Slug, err)
	}
	row.Start = start
	row.End = end
	row.ImageURL = strings.TrimSpace(row.ImageURL)
	row.ImageSourceURL = strings.TrimSpace(row.ImageSourceURL)
	return row, nil
}

func (s *mediaCleanupState) classifyOpenEvidenceRows(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `
		SELECT c.id, e.id, e.payload
		FROM event_review_clusters c
		JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id
			AND ce.active = 1
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		WHERE c.status = ?
		ORDER BY c.id, e.id
	`, string(seedstore.EventReviewClusterStatusOpen))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var row mediaCleanupOpenEvidenceRow
		if err := rows.Scan(&row.ClusterID, &row.EvidenceID, &row.Payload); err != nil {
			return err
		}
		s.classifyOpenEvidenceRow(row)
	}
	return rows.Err()
}

func (s *mediaCleanupState) classifyOpenEvidenceRow(row mediaCleanupOpenEvidenceRow) {
	var payload mediaCleanupEvidencePayload
	if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
		s.warnOpenEvidence(row, fmt.Sprintf("parse evidence payload: %v", err))
		s.retainRawPayloadReferences(row.Payload)
		return
	}
	payload.ImageURL = strings.TrimSpace(payload.ImageURL)
	payload.ImageSourceURL = strings.TrimSpace(payload.ImageSourceURL)
	start, err := parseRFC3339UTC(payload.StartAt)
	if err != nil {
		s.warnOpenEvidence(row, fmt.Sprintf("parse candidate start: %v", err))
		s.retainImageRefs(payload.ImageSourceURL, payload.ImageURL)
		return
	}
	var end time.Time
	if strings.TrimSpace(payload.EndAt) != "" {
		end, err = parseRFC3339UTC(payload.EndAt)
		if err != nil {
			s.warnOpenEvidence(row, fmt.Sprintf("parse candidate end: %v", err))
			s.retainImageRefs(payload.ImageSourceURL, payload.ImageURL)
			return
		}
		if end.Before(start) {
			s.warnOpenEvidence(row, "candidate end is before start")
			s.retainImageRefs(payload.ImageSourceURL, payload.ImageURL)
			return
		}
	}
	displayEnd, ok := mediaCleanupDisplayEnd(start, end, s.opts.Location)
	if !ok {
		s.warnOpenEvidence(row, "derive candidate display end")
		s.retainImageRefs(payload.ImageSourceURL, payload.ImageURL)
		return
	}
	if displayEnd.After(s.opts.Now) {
		s.retainImageRefs(payload.ImageSourceURL, payload.ImageURL)
	}
}

func (s *mediaCleanupState) warnOpenEvidence(row mediaCleanupOpenEvidenceRow, reason string) {
	message := fmt.Sprintf("retain open event-review evidence media for cluster %d evidence %d: %s", row.ClusterID, row.EvidenceID, reason)
	s.report.Warnings = append(s.report.Warnings, message)
	s.report.Items = append(s.report.Items, MediaCleanupItem{
		Action:     "retain_open_evidence",
		Reason:     reason,
		ClusterID:  row.ClusterID,
		EvidenceID: row.EvidenceID,
	})
}

func (s *mediaCleanupState) classifyAssetRows() {
	for idx, asset := range s.assets {
		if _, ok := s.retainedAssets[idx]; ok && asset.StoragePath != "" {
			s.retainedStorage[asset.StoragePath] = struct{}{}
		}
	}

	for idx, asset := range s.assets {
		retained := false
		if _, ok := s.retainedAssets[idx]; ok {
			retained = true
		}
		_, exists := s.existingFiles[asset.StoragePath]
		if retained {
			if asset.PublicURL != "" {
				s.retainedPublicURLs[asset.PublicURL] = struct{}{}
			}
			if exists {
				s.report.RetainedFiles++
			} else if asset.StoragePath != "" {
				s.report.MissingFiles++
				message := fmt.Sprintf("retained image asset %q is missing file %q", asset.SourceURL, asset.StoragePath)
				s.report.Warnings = append(s.report.Warnings, message)
				s.report.Items = append(s.report.Items, MediaCleanupItem{
					Action:      "missing_retained_file",
					Reason:      "retained_asset_missing_file",
					StoragePath: asset.StoragePath,
					SourceURL:   asset.SourceURL,
					PublicURL:   asset.PublicURL,
				})
			}
			continue
		}

		s.deleteAssetSources = append(s.deleteAssetSources, asset.SourceURL)
		s.report.DeletedAssetRows++
		item := MediaCleanupItem{
			Action:      "delete_asset_row",
			Reason:      "unreferenced_asset",
			StoragePath: asset.StoragePath,
			SourceURL:   asset.SourceURL,
			PublicURL:   asset.PublicURL,
		}
		if exists && asset.StoragePath != "" {
			if _, retainedPath := s.retainedStorage[asset.StoragePath]; !retainedPath {
				s.filesToDelete[asset.StoragePath] = struct{}{}
			}
		} else if asset.StoragePath != "" {
			s.report.MissingFiles++
			item.Reason = "unreferenced_asset_missing_file"
		}
		s.report.Items = append(s.report.Items, item)
	}
}

func (s *mediaCleanupState) applyDBCleanup(ctx context.Context, tx execer) error {
	for _, eventID := range s.clearEventIDs {
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET
				image_url = '',
				image_source_url = '',
				image_alt = '',
				image_width = 0,
				image_height = 0,
				image_focus_x = 50,
				image_focus_y = 50
			WHERE id = ?
		`, eventID); err != nil {
			return err
		}
	}
	for _, sourceImageID := range s.deleteSourceImageIDs {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM event_source_images
			WHERE id = ?
		`, sourceImageID); err != nil {
			return err
		}
	}
	for _, sourceURL := range s.deleteAssetSources {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM image_assets
			WHERE source_url = ?
		`, sourceURL); err != nil {
			return err
		}
	}
	return nil
}

func (s *mediaCleanupState) finalizeReport() {
	s.report.FilesToDelete = sortedStringSetValues(s.filesToDelete)
	s.report.KnownStoragePaths = sortedStringSetValues(s.knownStorage)
	s.report.RetainedStoragePaths = sortedStringSetValues(s.retainedStorage)
	s.report.RetainedPublicURLs = sortedStringSetValues(s.retainedPublicURLs)
	sort.SliceStable(s.report.Items, func(i, j int) bool {
		left := s.report.Items[i]
		right := s.report.Items[j]
		switch {
		case left.Action != right.Action:
			return left.Action < right.Action
		case left.StoragePath != right.StoragePath:
			return left.StoragePath < right.StoragePath
		case left.SourceURL != right.SourceURL:
			return left.SourceURL < right.SourceURL
		case left.EventSlug != right.EventSlug:
			return left.EventSlug < right.EventSlug
		case left.ClusterID != right.ClusterID:
			return left.ClusterID < right.ClusterID
		default:
			return left.EvidenceID < right.EvidenceID
		}
	})
	sort.Strings(s.report.Warnings)
}

func (s *mediaCleanupState) eventImageIsManaged(event mediaCleanupEventImageRow) bool {
	if mediaURLUnderPrefix(event.ImageURL, s.opts.MediaURLPrefix) {
		return true
	}
	if _, ok := s.assetBySourceURL[event.ImageSourceURL]; ok && strings.TrimSpace(event.ImageSourceURL) != "" {
		return true
	}
	if _, ok := s.assetByPublicURL[event.ImageURL]; ok && strings.TrimSpace(event.ImageURL) != "" {
		return true
	}
	return false
}

func (s *mediaCleanupState) retainImageRefs(sourceURL, imageURL string) {
	sourceURL = strings.TrimSpace(sourceURL)
	imageURL = strings.TrimSpace(imageURL)
	if sourceURL != "" {
		for _, idx := range s.assetBySourceURL[sourceURL] {
			s.retainedAssets[idx] = struct{}{}
		}
	}
	if imageURL != "" {
		if mediaURLUnderPrefix(imageURL, s.opts.MediaURLPrefix) {
			s.retainedPublicURLs[imageURL] = struct{}{}
		}
		for _, idx := range s.assetByPublicURL[imageURL] {
			s.retainedAssets[idx] = struct{}{}
		}
	}
}

func (s *mediaCleanupState) retainRawPayloadReferences(payload string) {
	for idx, asset := range s.assets {
		if asset.SourceURL != "" && strings.Contains(payload, asset.SourceURL) {
			s.retainedAssets[idx] = struct{}{}
		}
		if asset.PublicURL != "" && strings.Contains(payload, asset.PublicURL) {
			s.retainedAssets[idx] = struct{}{}
			if mediaURLUnderPrefix(asset.PublicURL, s.opts.MediaURLPrefix) {
				s.retainedPublicURLs[asset.PublicURL] = struct{}{}
			}
		}
	}
	for storagePath := range s.existingFiles {
		publicURL := mediaCleanupPublicURL(s.opts.MediaURLPrefix, storagePath)
		if publicURL != "" && strings.Contains(payload, publicURL) {
			s.retainedPublicURLs[publicURL] = struct{}{}
		}
	}
}

func mediaCleanupDisplayEnd(start, end time.Time, loc *time.Location) (time.Time, bool) {
	if start.IsZero() || loc == nil {
		return time.Time{}, false
	}
	if !end.IsZero() {
		if end.Before(start) {
			return time.Time{}, false
		}
		return end, true
	}
	localStart := start.In(loc)
	return time.Date(localStart.Year(), localStart.Month(), localStart.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1), true
}

func mediaURLUnderPrefix(value, prefix string) bool {
	value = strings.TrimSpace(value)
	prefix = normalizeMediaCleanupURLPrefix(prefix)
	return value == prefix || strings.HasPrefix(value, prefix+"/")
}

func mediaCleanupPublicURL(prefix, storagePath string) string {
	storagePath = normalizeStoragePath(storagePath)
	if storagePath == "" {
		return ""
	}
	return normalizeMediaCleanupURLPrefix(prefix) + "/" + storagePath
}

func normalizedStoragePathList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = normalizeStoragePath(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeStoragePath(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.TrimLeft(value, "/")
	return value
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedStringSetValues(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
