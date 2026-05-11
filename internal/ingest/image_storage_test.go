package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalImageStorageStoresSupportedImage(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 3, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	storage, err := NewLocalImageStorage(t.TempDir(), "/media")
	if err != nil {
		t.Fatalf("new local image storage: %v", err)
	}
	asset, err := storage.StoreImage(t.Context(), "https://example.test/show.png", FetchResult{
		URL:         "https://example.test/show.png",
		StatusCode:  http.StatusOK,
		ContentType: "image/png",
		Body:        body.Bytes(),
		CapturedAt:  time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("store image: %v", err)
	}

	if got, want := asset.PublicURL[:len("/media/events/")], "/media/events/"; got != want {
		t.Fatalf("public url prefix = %q, want %q", got, want)
	}
	if got, want := asset.Width, 3; got != want {
		t.Fatalf("width = %d, want %d", got, want)
	}
	if got, want := asset.Height, 2; got != want {
		t.Fatalf("height = %d, want %d", got, want)
	}
	if asset.FocusX == 0 || asset.FocusY == 0 {
		t.Fatalf("focus = %d,%d, want populated focus", asset.FocusX, asset.FocusY)
	}
	if _, err := os.Stat(storage.root + "/" + asset.StoragePath); err != nil {
		t.Fatalf("stored file stat: %v", err)
	}
}

func TestLocalImageStorageStoresNewFileWithPublicMode(t *testing.T) {
	body := testPNGWithDimensions(t, 3, 2)
	storage, err := NewLocalImageStorage(t.TempDir(), "/media")
	if err != nil {
		t.Fatalf("new local image storage: %v", err)
	}
	asset, err := storage.StoreImage(t.Context(), "https://example.test/show.png", FetchResult{
		URL:         "https://example.test/show.png",
		StatusCode:  http.StatusOK,
		ContentType: "image/png",
		Body:        body,
		CapturedAt:  time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("store image: %v", err)
	}

	info, err := os.Stat(filepath.Join(storage.root, filepath.FromSlash(asset.StoragePath)))
	if err != nil {
		t.Fatalf("stored file stat: %v", err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o644); got != want {
		t.Fatalf("stored file mode = %v, want %v", got, want)
	}
}

func TestLocalImageStorageNormalizesURLPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "adds leading slash", prefix: "media", want: "/media"},
		{name: "trims trailing slash", prefix: "/media/", want: "/media"},
		{name: "root falls back", prefix: "/", want: "/media"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage, err := NewLocalImageStorage(t.TempDir(), tc.prefix)
			if err != nil {
				t.Fatalf("new local image storage: %v", err)
			}
			if storage.urlPrefix != tc.want {
				t.Fatalf("url prefix = %q, want %q", storage.urlPrefix, tc.want)
			}
		})
	}
}

func TestLocalImageStorageRejectsUnsupportedContentType(t *testing.T) {
	storage, err := NewLocalImageStorage(t.TempDir(), "/media")
	if err != nil {
		t.Fatalf("new local image storage: %v", err)
	}
	_, err = storage.StoreImage(t.Context(), "https://example.test/show.txt", FetchResult{
		StatusCode:  http.StatusOK,
		ContentType: "text/plain",
		Body:        []byte("not an image"),
	})
	if err == nil {
		t.Fatalf("store image err = nil, want error")
	}
}

func TestLocalImageStorageAcceptsMatchingExistingFile(t *testing.T) {
	body := testPNGWithDimensions(t, 3, 2)
	storage, err := NewLocalImageStorage(t.TempDir(), "/media")
	if err != nil {
		t.Fatalf("new local image storage: %v", err)
	}

	absolutePath := testLocalImageStoragePath(storage.root, body, ".png")
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
		t.Fatalf("mkdirs: %v", err)
	}
	if err := os.WriteFile(absolutePath, body, 0o644); err != nil {
		t.Fatalf("prewrite image: %v", err)
	}

	asset, err := storage.StoreImage(t.Context(), "https://example.test/show.png", FetchResult{
		URL:         "https://example.test/show.png",
		StatusCode:  http.StatusOK,
		ContentType: "image/png",
		Body:        body,
		CapturedAt:  time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("store image: %v", err)
	}
	if got, want := asset.StoragePath, filepath.ToSlash(filepath.Join("events", testLocalImageStorageSHA(body)+".png")); got != want {
		t.Fatalf("storage path = %q, want %q", got, want)
	}
	if got, want := asset.Width, 3; got != want {
		t.Fatalf("width = %d, want %d", got, want)
	}
	if got, want := asset.Height, 2; got != want {
		t.Fatalf("height = %d, want %d", got, want)
	}
}

func TestLocalImageStorageRejectsCorruptExistingFile(t *testing.T) {
	body := testPNGWithDimensions(t, 3, 2)
	storage, err := NewLocalImageStorage(t.TempDir(), "/media")
	if err != nil {
		t.Fatalf("new local image storage: %v", err)
	}

	absolutePath := testLocalImageStoragePath(storage.root, body, ".png")
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
		t.Fatalf("mkdirs: %v", err)
	}
	if err := os.WriteFile(absolutePath, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("prewrite corrupt image: %v", err)
	}

	_, err = storage.StoreImage(t.Context(), "https://example.test/show.png", FetchResult{
		URL:         "https://example.test/show.png",
		StatusCode:  http.StatusOK,
		ContentType: "image/png",
		Body:        body,
		CapturedAt:  time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("store image err = nil, want error")
	}
	if !strings.Contains(err.Error(), "does not match image body") {
		t.Fatalf("store image error = %v, want mismatch error", err)
	}

	got, err := os.ReadFile(absolutePath)
	if err != nil {
		t.Fatalf("read existing file: %v", err)
	}
	if string(got) != "corrupt" {
		t.Fatalf("existing file contents = %q, want corrupt", got)
	}
	entries, err := os.ReadDir(filepath.Dir(absolutePath))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("entries = %d, want %d", got, want)
	}
}

func TestPublishTempFileIfMissingRejectsRaceCreatedMismatchedFinalFile(t *testing.T) {
	dir := t.TempDir()
	body := []byte("expected image body")
	tempFile, err := os.CreateTemp(dir, ".image.tmp-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(body); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	finalPath := filepath.Join(dir, "image.png")
	if err := os.WriteFile(finalPath, []byte("different image body"), 0o644); err != nil {
		t.Fatalf("write race-created final file: %v", err)
	}

	err = publishTempFileIfMissing(tempPath, finalPath, body)
	if err == nil {
		t.Fatal("publish temp file err = nil, want mismatch error")
	}
	if !strings.Contains(err.Error(), "does not match image body") {
		t.Fatalf("publish temp file error = %v, want mismatch error", err)
	}

	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read final file: %v", err)
	}
	if string(got) != "different image body" {
		t.Fatalf("final file contents = %q, want race-created contents", got)
	}
}

func TestPublishTempFileIfMissingAcceptsRaceCreatedMatchingFinalFile(t *testing.T) {
	dir := t.TempDir()
	body := []byte("expected image body")
	tempFile, err := os.CreateTemp(dir, ".image.tmp-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(body); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	finalPath := filepath.Join(dir, "image.png")
	if err := os.WriteFile(finalPath, body, 0o644); err != nil {
		t.Fatalf("write race-created final file: %v", err)
	}

	if err := publishTempFileIfMissing(tempPath, finalPath, body); err != nil {
		t.Fatalf("publish temp file: %v", err)
	}
}

func TestLocalImageStorageStoresOversizedDimensionsWithDefaultFocus(t *testing.T) {
	tests := []struct {
		name   string
		width  int
		height int
	}{
		{name: "dimension limit", width: 8193, height: 1},
		{name: "pixel limit", width: 8192, height: 4883},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := testPNGWithDimensions(t, tc.width, tc.height)
			storage, err := NewLocalImageStorage(t.TempDir(), "/media")
			if err != nil {
				t.Fatalf("new local image storage: %v", err)
			}

			asset, err := storage.StoreImage(t.Context(), "https://example.test/show.png", FetchResult{
				URL:         "https://example.test/show.png",
				StatusCode:  http.StatusOK,
				ContentType: "image/png",
				Body:        body,
				CapturedAt:  time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
			})
			if err != nil {
				t.Fatalf("store image: %v", err)
			}
			if got, want := asset.Width, tc.width; got != want {
				t.Fatalf("width = %d, want %d", got, want)
			}
			if got, want := asset.Height, tc.height; got != want {
				t.Fatalf("height = %d, want %d", got, want)
			}
			if got, want := (ImageFocus{X: asset.FocusX, Y: asset.FocusY}), DefaultImageFocus(); got != want {
				t.Fatalf("focus = %#v, want default", got)
			}
			if _, err := os.Stat(filepath.Join(storage.root, filepath.FromSlash(asset.StoragePath))); err != nil {
				t.Fatalf("stored file stat: %v", err)
			}
		})
	}
}

func testPNGWithDimensions(t *testing.T, width, height int) []byte {
	t.Helper()

	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	data := body.Bytes()
	binary.BigEndian.PutUint32(data[16:20], uint32(width))
	binary.BigEndian.PutUint32(data[20:24], uint32(height))
	binary.BigEndian.PutUint32(data[29:33], crc32.ChecksumIEEE(data[12:29]))
	return append([]byte(nil), data...)
}

func testLocalImageStorageSHA(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum[:])
}

func testLocalImageStoragePath(root string, body []byte, ext string) string {
	return filepath.Join(root, filepath.FromSlash(filepath.Join("events", testLocalImageStorageSHA(body)+ext)))
}
