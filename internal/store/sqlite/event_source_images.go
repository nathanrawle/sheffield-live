package sqlite

import (
	"context"
	"errors"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

type eventSourceImageInput struct {
	EventID           int64
	SourceID          int64
	SourceIdentityKey string
	ImageURL          string
	ImageSourceURL    string
	ImageAlt          string
	ImageWidth        int
	ImageHeight       int
	ImageFocusX       int
	ImageFocusY       int
	FirstSeenAt       time.Time
	LastSeenAt        time.Time
	UpdatedAt         time.Time
}

func (s *Store) EventSourceImagesByEventSlug(ctx context.Context, slug string) ([]seedstore.EventImage, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			i.image_url,
			i.image_source_url,
			i.image_alt,
			i.image_width,
			i.image_height,
			i.image_focus_x,
			i.image_focus_y,
			s.name,
			s.url,
			i.source_identity_key
		FROM event_source_images i
		JOIN events e ON e.id = i.event_id
		JOIN sources s ON s.id = i.source_id
		WHERE e.slug = ?
			AND TRIM(i.image_url) <> ''
		ORDER BY s.name, s.url, i.image_source_url, i.image_url
	`, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []seedstore.EventImage
	for rows.Next() {
		var image seedstore.EventImage
		if err := rows.Scan(&image.ImageURL, &image.ImageSourceURL, &image.Alt, &image.Width, &image.Height, &image.FocusX, &image.FocusY, &image.SourceName, &image.ListingURL, &image.SourceIdentityKey); err != nil {
			return nil, err
		}
		focus := explicitImageFocus(image.FocusX, image.FocusY)
		image.FocusX = focus.X
		image.FocusY = focus.Y
		images = append(images, image)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return images, nil
}

func replaceEventSourceImagesTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if err := deleteEventSourceImagesForEventTx(ctx, tx, eventID); err != nil {
		return err
	}
	return upsertEventSourceImagesTx(ctx, tx, eventID, primary, candidates, now)
}

func syncEventSourceImagesTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	currentSourceIDs := make(map[int64]struct{}, len(candidates))
	for _, candidate := range candidates {
		sourceName, sourceURL, authoritativeMatchURL := eventSourceImageCandidateSource(candidate)
		if sourceName == "" || sourceURL == "" {
			continue
		}
		if sourceName == primary.SourceName && authoritativeMatchURL == primary.SourceURL {
			continue
		}
		sourceID, err := ensureSourceTx(ctx, tx, sourceName, sourceURL)
		if err != nil {
			return err
		}
		currentSourceIDs[sourceID] = struct{}{}
	}
	if err := deleteEventSourceImagesExceptSourcesTx(ctx, tx, eventID, currentSourceIDs); err != nil {
		return err
	}
	return upsertEventSourceImagesTx(ctx, tx, eventID, primary, candidates, now)
}

func upsertEventSourceImagesTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, primary reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	for _, candidate := range candidates {
		sourceName, sourceURL, authoritativeMatchURL := eventSourceImageCandidateSource(candidate)
		if sourceName == "" || sourceURL == "" {
			continue
		}
		if sourceName == primary.SourceName && authoritativeMatchURL == primary.SourceURL {
			continue
		}
		sourceID, err := ensureSourceTx(ctx, tx, sourceName, sourceURL)
		if err != nil {
			return err
		}
		sourceIdentityKey := reviewStoredCandidateSourceIdentities(candidate).PrimaryKey()
		if sourceIdentityKey == "" {
			sourceIdentityKey = ingest.SourceIdentities(ingest.SourceIdentityInput{SourceURL: sourceURL}).PrimaryKey()
		}
		if strings.TrimSpace(candidate.ImageURL) == "" {
			if err := deleteEventSourceImageForEventSourceTx(ctx, tx, eventID, sourceID); err != nil {
				return err
			}
			continue
		}
		if err := upsertEventSourceImageRowTx(ctx, tx, eventSourceImageInput{
			EventID:           eventID,
			SourceID:          sourceID,
			SourceIdentityKey: sourceIdentityKey,
			ImageURL:          candidate.ImageURL,
			ImageSourceURL:    candidate.ImageSourceURL,
			ImageAlt:          candidate.ImageAlt,
			ImageWidth:        candidate.ImageWidth,
			ImageHeight:       candidate.ImageHeight,
			ImageFocusX:       candidate.ImageFocusX,
			ImageFocusY:       candidate.ImageFocusY,
			FirstSeenAt:       now,
			LastSeenAt:        now,
			UpdatedAt:         now,
		}); err != nil {
			return err
		}
	}
	return nil
}

func eventSourceImageCandidateSource(candidate review.Candidate) (sourceName, sourceURL, authoritativeMatchURL string) {
	sourceName = strings.TrimSpace(candidate.SourceName)
	sourceURL = strings.TrimSpace(candidate.SourceURL)
	calendarURL := strings.TrimSpace(candidate.CalendarURL)
	authoritativeMatchURL = sourceURL
	if calendarURL != "" {
		authoritativeMatchURL = calendarURL
	}
	if sourceName == "" || sourceURL == "" {
		sourceURL = calendarURL
	}
	return sourceName, sourceURL, authoritativeMatchURL
}

func upsertEventSourceImageRowTx(ctx context.Context, tx execer, input eventSourceImageInput) error {
	if input.EventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	if input.SourceID <= 0 {
		return errors.New("event source image source ID is required")
	}
	imageURL := strings.TrimSpace(input.ImageURL)
	if imageURL == "" {
		return errors.New("event source image URL is required")
	}
	now := time.Now().UTC()
	firstSeenAt := input.FirstSeenAt
	if firstSeenAt.IsZero() {
		firstSeenAt = now
	}
	lastSeenAt := input.LastSeenAt
	if lastSeenAt.IsZero() {
		lastSeenAt = firstSeenAt
	}
	updatedAt := input.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = lastSeenAt
	}
	focus := explicitImageFocus(input.ImageFocusX, input.ImageFocusY)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO event_source_images (
			event_id,
			source_id,
			source_identity_key,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			first_seen_at,
			last_seen_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id, source_id) DO UPDATE SET
			source_identity_key = excluded.source_identity_key,
			image_url = excluded.image_url,
			image_source_url = excluded.image_source_url,
			image_alt = excluded.image_alt,
			image_width = excluded.image_width,
			image_height = excluded.image_height,
			image_focus_x = excluded.image_focus_x,
			image_focus_y = excluded.image_focus_y,
			last_seen_at = excluded.last_seen_at,
			updated_at = excluded.updated_at
	`, input.EventID, input.SourceID, strings.TrimSpace(input.SourceIdentityKey), imageURL, strings.TrimSpace(input.ImageSourceURL), strings.TrimSpace(input.ImageAlt), input.ImageWidth, input.ImageHeight, focus.X, focus.Y, formatRFC3339UTC(firstSeenAt), formatRFC3339UTC(lastSeenAt), formatRFC3339UTC(updatedAt))
	return err
}

func deleteEventSourceImagesForEventTx(ctx context.Context, tx execer, eventID int64) error {
	if eventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_images
		WHERE event_id = ?
	`, eventID)
	return err
}

func deleteEventSourceImageForEventSourceTx(ctx context.Context, tx execer, eventID, sourceID int64) error {
	if eventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	if sourceID <= 0 {
		return errors.New("event source image source ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_images
		WHERE event_id = ?
			AND source_id = ?
	`, eventID, sourceID)
	return err
}

func deleteEventSourceImagesExceptSourcesTx(ctx context.Context, tx execer, eventID int64, sourceIDs map[int64]struct{}) error {
	if eventID <= 0 {
		return errors.New("event source image event ID is required")
	}
	if len(sourceIDs) == 0 {
		return deleteEventSourceImagesForEventTx(ctx, tx, eventID)
	}
	keep := sortedInt64Keys(sourceIDs)
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_images
		WHERE event_id = ?
			AND source_id NOT IN (`+placeholders(len(keep))+`)
	`, append([]any{eventID}, int64SliceToAny(keep)...)...)
	return err
}

func backfillEventSourceImagesTx(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	now := time.Now().UTC()
	if err := backfillEventSourceImagesFromReviewEvidenceTx(ctx, tx, now); err != nil {
		return err
	}
	return backfillEventSourceImagesFromReviewCandidatesTx(ctx, tx, now)
}

func backfillEventSourceImagesFromReviewEvidenceTx(ctx context.Context, tx interface {
	execer
	queryer
}, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			COALESCE(ev.event_id, c.canonical_event_id) AS target_event_id,
			ev.source_id,
			target.source_id AS canonical_source_id,
			s.url,
			ev.payload,
			ev.created_at,
			ev.updated_at
		FROM event_review_clusters c
		JOIN event_review_cluster_evidence ce ON ce.cluster_id = c.id
			AND ce.active = 1
		JOIN event_review_evidence ev ON ev.id = ce.evidence_id
		JOIN events target ON target.id = COALESCE(ev.event_id, c.canonical_event_id)
		JOIN sources s ON s.id = ev.source_id
		WHERE c.status = ?
			AND COALESCE(ev.event_id, c.canonical_event_id) IS NOT NULL
			AND target.origin = ?
			AND target.publication_state <> ?
			AND TRIM(ev.payload) <> ''
		ORDER BY c.id, ev.id
	`, string(seedstore.EventReviewClusterStatusResolved), string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var eventID int64
		var sourceID int64
		var canonicalSourceID int64
		var sourceURL string
		var payload string
		var createdAtText string
		var updatedAtText string
		if err := rows.Scan(&eventID, &sourceID, &canonicalSourceID, &sourceURL, &payload, &createdAtText, &updatedAtText); err != nil {
			return err
		}
		if sourceID == canonicalSourceID {
			continue
		}
		parsed, err := parseImportReviewCandidatePayload(payload)
		if err != nil {
			continue
		}
		if strings.TrimSpace(parsed.ImageURL) == "" {
			continue
		}
		sourceIdentityKey := ingest.SourceIdentities(sourceIdentityInputFromCandidateValues(parsed.ExternalID, firstNonEmptyReviewString(parsed.SourceURL, sourceURL), parsed.CalendarURL)).PrimaryKey()
		createdAt := parseBackfillTime(createdAtText, now)
		updatedAt := parseBackfillTime(updatedAtText, createdAt)
		if err := upsertEventSourceImageRowTx(ctx, tx, eventSourceImageInput{
			EventID:           eventID,
			SourceID:          sourceID,
			SourceIdentityKey: sourceIdentityKey,
			ImageURL:          parsed.ImageURL,
			ImageSourceURL:    parsed.ImageSourceURL,
			ImageAlt:          parsed.ImageAlt,
			ImageWidth:        parsed.ImageWidth,
			ImageHeight:       parsed.ImageHeight,
			ImageFocusX:       parsed.ImageFocusX,
			ImageFocusY:       parsed.ImageFocusY,
			FirstSeenAt:       createdAt,
			LastSeenAt:        updatedAt,
			UpdatedAt:         updatedAt,
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func backfillEventSourceImagesFromReviewCandidatesTx(ctx context.Context, tx interface {
	execer
	queryer
}, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			c.canonical_event_id,
			target.source_id AS canonical_source_id,
			c.external_id,
			c.source_name,
			c.source_url,
			COALESCE(c.calendar_url, ''),
			c.image_url,
			c.image_source_url,
			c.image_alt,
			c.image_width,
			c.image_height,
			c.image_focus_x,
			c.image_focus_y,
			g.created_at,
			g.updated_at
		FROM review_candidates c
		JOIN review_groups g ON g.id = c.group_id
		JOIN events target ON target.id = c.canonical_event_id
		WHERE c.canonical_event_id IS NOT NULL
			AND TRIM(c.image_url) <> ''
			AND target.origin = ?
			AND target.publication_state <> ?
		ORDER BY c.group_id, c.position, c.id
	`, string(domain.OriginLive), string(domain.PublicationStateWithheld))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var eventID int64
		var canonicalSourceID int64
		var externalID string
		var sourceName string
		var sourceURL string
		var calendarURL string
		var imageURL string
		var imageSourceURL string
		var imageAlt string
		var imageWidth int
		var imageHeight int
		var imageFocusX int
		var imageFocusY int
		var createdAtText string
		var updatedAtText string
		if err := rows.Scan(&eventID, &canonicalSourceID, &externalID, &sourceName, &sourceURL, &calendarURL, &imageURL, &imageSourceURL, &imageAlt, &imageWidth, &imageHeight, &imageFocusX, &imageFocusY, &createdAtText, &updatedAtText); err != nil {
			return err
		}
		sourceName = strings.TrimSpace(sourceName)
		sourceURL = strings.TrimSpace(sourceURL)
		if sourceName == "" || sourceURL == "" {
			continue
		}
		sourceID, err := ensureSourceTx(ctx, tx, sourceName, sourceURL)
		if err != nil {
			return err
		}
		if sourceID == canonicalSourceID {
			continue
		}
		sourceIdentityKey := ingest.SourceIdentities(sourceIdentityInputFromCandidateValues(externalID, sourceURL, calendarURL)).PrimaryKey()
		createdAt := parseBackfillTime(createdAtText, now)
		updatedAt := parseBackfillTime(updatedAtText, createdAt)
		if err := upsertEventSourceImageRowTx(ctx, tx, eventSourceImageInput{
			EventID:           eventID,
			SourceID:          sourceID,
			SourceIdentityKey: sourceIdentityKey,
			ImageURL:          imageURL,
			ImageSourceURL:    imageSourceURL,
			ImageAlt:          imageAlt,
			ImageWidth:        imageWidth,
			ImageHeight:       imageHeight,
			ImageFocusX:       imageFocusX,
			ImageFocusY:       imageFocusY,
			FirstSeenAt:       createdAt,
			LastSeenAt:        updatedAt,
			UpdatedAt:         updatedAt,
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func parseBackfillTime(value string, fallback time.Time) time.Time {
	parsed, err := parseRFC3339UTC(strings.TrimSpace(value))
	if err != nil || parsed.IsZero() {
		return fallback
	}
	return parsed
}
