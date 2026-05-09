package ingest

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func TestEstimateImageFocusFindsHighContrastRegion(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 100, 100))
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			img.Set(x, y, color.RGBA{R: 180, G: 180, B: 180, A: 255})
		}
	}
	for y := 70; y < 90; y++ {
		for x := 70; x < 90; x++ {
			img.Set(x, y, color.RGBA{A: 255})
		}
	}
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png; charset=binary", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X <= 55 || focus.Y <= 55 {
		t.Fatalf("focus = %d,%d, want lower-right quadrant", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusDefaultsForFlatImages(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 50, 50))
	for y := 0; y < 50; y++ {
		for x := 0; x < 50; x++ {
			img.Set(x, y, color.RGBA{R: 120, G: 120, B: 120, A: 255})
		}
	}
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if !errors.Is(err, ErrImageFocusNoSignal) {
		t.Fatalf("error = %v, want ErrImageFocusNoSignal", err)
	}
	if focus != DefaultImageFocus() {
		t.Fatalf("focus = %#v, want default", focus)
	}
}

func TestEstimateImageFocusDefaultsForWebPWithoutDecoder(t *testing.T) {
	focus, err := EstimateImageFocus("image/webp", []byte("RIFF"))
	if !errors.Is(err, ErrImageFocusUnsupported) {
		t.Fatalf("error = %v, want ErrImageFocusUnsupported", err)
	}
	if focus != DefaultImageFocus() {
		t.Fatalf("focus = %#v, want default", focus)
	}
}

func TestNormalizeImageFocusDefaultsZeroAndClamps(t *testing.T) {
	focus := NormalizeImageFocus(0, 120)
	if focus.X != 50 || focus.Y != 95 {
		t.Fatalf("focus = %d,%d, want 50,95", focus.X, focus.Y)
	}

	focus = NormalizeImageFocus(-10, 3)
	if focus.X != 5 || focus.Y != 5 {
		t.Fatalf("focus = %d,%d, want 5,5", focus.X, focus.Y)
	}
}
