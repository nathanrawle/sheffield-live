package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"sheffield-live/internal/ingest"
)

func (s *Store) LoadImageAsset(ctx context.Context, sourceURL string) (ingest.ImageAsset, bool, error) {
	if s == nil || s.db == nil {
		return ingest.ImageAsset{}, false, errors.New("sqlite store is not open")
	}
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceURL == "" {
		return ingest.ImageAsset{}, false, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT
			source_url,
			public_url,
			storage_path,
			content_type,
			width,
			height,
			focus_x,
			focus_y,
			bytes,
			sha256,
			copied_at
		FROM image_assets
		WHERE source_url = ?
		LIMIT 1
	`, sourceURL)
	var asset ingest.ImageAsset
	var copiedAt string
	switch err := row.Scan(&asset.SourceURL, &asset.PublicURL, &asset.StoragePath, &asset.ContentType, &asset.Width, &asset.Height, &asset.FocusX, &asset.FocusY, &asset.Bytes, &asset.SHA256, &copiedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return ingest.ImageAsset{}, false, nil
	case err != nil:
		return ingest.ImageAsset{}, false, err
	}
	parsed, err := parseRFC3339UTC(copiedAt)
	if err != nil {
		return ingest.ImageAsset{}, false, err
	}
	focus := explicitImageFocus(asset.FocusX, asset.FocusY)
	asset.FocusX = focus.X
	asset.FocusY = focus.Y
	asset.CopiedAt = parsed
	return asset, true, nil
}

func (s *Store) SaveImageAsset(ctx context.Context, asset ingest.ImageAsset) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	asset.SourceURL = strings.TrimSpace(asset.SourceURL)
	if asset.SourceURL == "" {
		return errors.New("image asset source URL is required")
	}
	if asset.CopiedAt.IsZero() {
		asset.CopiedAt = time.Now().UTC()
	}
	focus := explicitImageFocus(asset.FocusX, asset.FocusY)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO image_assets (
			source_url,
			public_url,
			storage_path,
			content_type,
			width,
			height,
			focus_x,
			focus_y,
			bytes,
			sha256,
			copied_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_url) DO UPDATE SET
			public_url = excluded.public_url,
			storage_path = excluded.storage_path,
			content_type = excluded.content_type,
			width = excluded.width,
			height = excluded.height,
			focus_x = excluded.focus_x,
			focus_y = excluded.focus_y,
			bytes = excluded.bytes,
			sha256 = excluded.sha256,
			copied_at = excluded.copied_at
	`, asset.SourceURL, strings.TrimSpace(asset.PublicURL), strings.TrimSpace(asset.StoragePath), strings.TrimSpace(asset.ContentType), asset.Width, asset.Height, focus.X, focus.Y, asset.Bytes, strings.TrimSpace(asset.SHA256), formatRFC3339UTC(asset.CopiedAt))
	return err
}

func (s *Store) ListImageAssets(ctx context.Context) ([]ingest.ImageAsset, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			source_url,
			public_url,
			storage_path,
			content_type,
			width,
			height,
			focus_x,
			focus_y,
			bytes,
			sha256,
			copied_at
		FROM image_assets
		ORDER BY source_url
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assets []ingest.ImageAsset
	for rows.Next() {
		var asset ingest.ImageAsset
		var copiedAt string
		if err := rows.Scan(&asset.SourceURL, &asset.PublicURL, &asset.StoragePath, &asset.ContentType, &asset.Width, &asset.Height, &asset.FocusX, &asset.FocusY, &asset.Bytes, &asset.SHA256, &copiedAt); err != nil {
			return nil, err
		}
		parsed, err := parseRFC3339UTC(copiedAt)
		if err != nil {
			return nil, err
		}
		focus := explicitImageFocus(asset.FocusX, asset.FocusY)
		asset.FocusX = focus.X
		asset.FocusY = focus.Y
		asset.CopiedAt = parsed
		assets = append(assets, asset)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return assets, nil
}

func (s *Store) UpdateImageAssetFocus(ctx context.Context, sourceURL string, focus ingest.ImageFocus) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceURL == "" {
		return errors.New("image asset source URL is required")
	}
	focus = explicitImageFocus(focus.X, focus.Y)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		UPDATE image_assets
		SET focus_x = ?, focus_y = ?
		WHERE source_url = ?
	`, focus.X, focus.Y, sourceURL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_candidates
		SET image_focus_x = ?, image_focus_y = ?
		WHERE image_source_url = ?
	`, focus.X, focus.Y, sourceURL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET image_focus_x = ?, image_focus_y = ?
		WHERE image_source_url = ?
	`, focus.X, focus.Y, sourceURL); err != nil {
		return err
	}
	return tx.Commit()
}

func explicitImageFocus(x, y int) ingest.ImageFocus {
	return ingest.ImageFocus{
		X: ingest.NormalizeExplicitImageFocusValue(x),
		Y: ingest.NormalizeExplicitImageFocusValue(y),
	}
}
