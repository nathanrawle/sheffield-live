package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type ImageAsset struct {
	SourceURL   string
	PublicURL   string
	StoragePath string
	ContentType string
	Width       int
	Height      int
	FocusX      int
	FocusY      int
	Bytes       int64
	SHA256      string
	CopiedAt    time.Time
}

type ImageAssetStore interface {
	LoadImageAsset(ctx context.Context, sourceURL string) (ImageAsset, bool, error)
	SaveImageAsset(ctx context.Context, asset ImageAsset) error
}

type ImageStorage interface {
	StoreImage(ctx context.Context, sourceURL string, result FetchResult) (ImageAsset, error)
}

type LocalImageStorage struct {
	root      string
	urlPrefix string
	now       func() time.Time
}

func NewLocalImageStorage(root, urlPrefix string) (*LocalImageStorage, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("media root is required")
	}
	urlPrefix = strings.TrimSpace(urlPrefix)
	if urlPrefix == "" {
		return nil, errors.New("media URL prefix is required")
	}
	if !strings.HasPrefix(urlPrefix, "/") {
		urlPrefix = "/" + urlPrefix
	}
	urlPrefix = strings.TrimRight(urlPrefix, "/")
	if urlPrefix == "" {
		urlPrefix = "/media"
	}
	return &LocalImageStorage{
		root:      root,
		urlPrefix: urlPrefix,
		now:       func() time.Time { return time.Now().UTC() },
	}, nil
}

func (s *LocalImageStorage) StoreImage(ctx context.Context, sourceURL string, result FetchResult) (ImageAsset, error) {
	if s == nil {
		return ImageAsset{}, errors.New("image storage is nil")
	}
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceURL == "" {
		return ImageAsset{}, errors.New("source image URL is required")
	}
	if err := validateRemoteImageURL(sourceURL); err != nil {
		return ImageAsset{}, err
	}
	if result.Truncated {
		return ImageAsset{}, errors.New("image response exceeded size limit")
	}
	if !statusIsOK(result.StatusCode) {
		return ImageAsset{}, fmt.Errorf("image returned HTTP %d", result.StatusCode)
	}
	contentType, ext, err := imageContentTypeAndExt(result.ContentType, result.Body)
	if err != nil {
		return ImageAsset{}, err
	}
	width, height, err := imageDimensions(contentType, result.Body)
	if err != nil {
		return ImageAsset{}, err
	}
	focus := BestEffortImageFocus(contentType, result.Body)

	sum := sha256.Sum256(result.Body)
	sha := hex.EncodeToString(sum[:])
	storagePath := path.Join("events", sha+ext)
	absolutePath := filepath.Join(s.root, filepath.FromSlash(storagePath))
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
		return ImageAsset{}, err
	}
	if err := writeFileIfMissing(absolutePath, result.Body); err != nil {
		return ImageAsset{}, err
	}
	return ImageAsset{
		SourceURL:   sourceURL,
		PublicURL:   s.urlPrefix + "/" + storagePath,
		StoragePath: storagePath,
		ContentType: contentType,
		Width:       width,
		Height:      height,
		FocusX:      focus.X,
		FocusY:      focus.Y,
		Bytes:       int64(len(result.Body)),
		SHA256:      sha,
		CopiedAt:    s.now(),
	}, nil
}

func writeFileIfMissing(path string, body []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(body)
	return err
}

func validateRemoteImageURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported image URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return errors.New("image URL host is required")
	}
	return nil
}

func imageContentTypeAndExt(header string, body []byte) (string, string, error) {
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(header, ";")[0]))
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = strings.ToLower(http.DetectContentType(body))
	}
	switch contentType {
	case "image/jpeg":
		return contentType, ".jpg", nil
	case "image/png":
		return contentType, ".png", nil
	case "image/gif":
		return contentType, ".gif", nil
	case "image/webp":
		return contentType, ".webp", nil
	default:
		return "", "", fmt.Errorf("unsupported image content type %q", contentType)
	}
}

func imageDimensions(contentType string, body []byte) (int, int, error) {
	if contentType == "image/webp" {
		return webPDimensions(body)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	return cfg.Width, cfg.Height, nil
}

func webPDimensions(body []byte) (int, int, error) {
	if len(body) < 20 || string(body[0:4]) != "RIFF" || string(body[8:12]) != "WEBP" {
		return 0, 0, errors.New("invalid WebP header")
	}
	for offset := 12; offset+8 <= len(body); {
		chunkType := string(body[offset : offset+4])
		chunkSize := int(littleEndian32(body[offset+4 : offset+8]))
		payload := offset + 8
		if chunkSize < 0 || payload+chunkSize > len(body) {
			return 0, 0, errors.New("invalid WebP chunk size")
		}
		chunk := body[payload : payload+chunkSize]
		switch chunkType {
		case "VP8X":
			if len(chunk) < 10 {
				return 0, 0, errors.New("invalid VP8X chunk")
			}
			width := 1 + int(littleEndian24(chunk[4:7]))
			height := 1 + int(littleEndian24(chunk[7:10]))
			return width, height, nil
		case "VP8L":
			if len(chunk) < 5 || chunk[0] != 0x2f {
				return 0, 0, errors.New("invalid VP8L chunk")
			}
			width := 1 + int(uint16(chunk[1])|uint16(chunk[2]&0x3f)<<8)
			height := 1 + int(uint16(chunk[2]&0xc0)>>6|uint16(chunk[3])<<2|uint16(chunk[4]&0x0f)<<10)
			return width, height, nil
		case "VP8 ":
			if len(chunk) < 10 || chunk[3] != 0x9d || chunk[4] != 0x01 || chunk[5] != 0x2a {
				return 0, 0, errors.New("invalid VP8 chunk")
			}
			width := int(littleEndian16(chunk[6:8]) & 0x3fff)
			height := int(littleEndian16(chunk[8:10]) & 0x3fff)
			return width, height, nil
		}
		offset = payload + chunkSize
		if chunkSize%2 == 1 {
			offset++
		}
	}
	return 0, 0, errors.New("missing WebP image chunk")
}

func littleEndian16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func littleEndian24(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func littleEndian32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
