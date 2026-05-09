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
			bytes,
			sha256,
			copied_at
		FROM image_assets
		WHERE source_url = ?
		LIMIT 1
	`, sourceURL)
	var asset ingest.ImageAsset
	var copiedAt string
	switch err := row.Scan(&asset.SourceURL, &asset.PublicURL, &asset.StoragePath, &asset.ContentType, &asset.Width, &asset.Height, &asset.Bytes, &asset.SHA256, &copiedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return ingest.ImageAsset{}, false, nil
	case err != nil:
		return ingest.ImageAsset{}, false, err
	}
	parsed, err := parseRFC3339UTC(copiedAt)
	if err != nil {
		return ingest.ImageAsset{}, false, err
	}
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
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO image_assets (
			source_url,
			public_url,
			storage_path,
			content_type,
			width,
			height,
			bytes,
			sha256,
			copied_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_url) DO UPDATE SET
			public_url = excluded.public_url,
			storage_path = excluded.storage_path,
			content_type = excluded.content_type,
			width = excluded.width,
			height = excluded.height,
			bytes = excluded.bytes,
			sha256 = excluded.sha256,
			copied_at = excluded.copied_at
	`, asset.SourceURL, strings.TrimSpace(asset.PublicURL), strings.TrimSpace(asset.StoragePath), strings.TrimSpace(asset.ContentType), asset.Width, asset.Height, asset.Bytes, strings.TrimSpace(asset.SHA256), formatRFC3339UTC(asset.CopiedAt))
	return err
}
