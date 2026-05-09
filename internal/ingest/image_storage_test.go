package ingest

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"os"
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
	if _, err := os.Stat(storage.root + "/" + asset.StoragePath); err != nil {
		t.Fatalf("stored file stat: %v", err)
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
