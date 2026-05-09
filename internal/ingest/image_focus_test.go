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
	if focus.X <= 70 || focus.Y <= 70 {
		t.Fatalf("focus = %d,%d, want strong lower-right focus", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusUsesLocalSaliencyInsteadOfGlobalAverage(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 120, 80))
	for y := 0; y < 80; y++ {
		for x := 0; x < 120; x++ {
			img.Set(x, y, color.RGBA{R: 170, G: 170, B: 170, A: 255})
		}
	}
	for y := 10; y < 70; y++ {
		for x := 50; x < 80; x++ {
			if x == 50 || x == 79 || y == 10 || y == 69 {
				img.Set(x, y, color.RGBA{R: 145, G: 145, B: 145, A: 255})
			}
		}
	}
	for y := 24; y < 56; y++ {
		for x := 8; x < 40; x++ {
			img.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X >= 40 {
		t.Fatalf("focus = %d,%d, want left-side salient region", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusPrioritizesSkinToneRegion(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 120, 80))
	for y := 0; y < 80; y++ {
		for x := 0; x < 120; x++ {
			img.Set(x, y, color.RGBA{R: 120, G: 120, B: 120, A: 255})
		}
	}
	for y := 24; y < 56; y++ {
		for x := 8; x < 40; x++ {
			img.Set(x, y, color.RGBA{R: 20, G: 70, B: 240, A: 255})
		}
		for x := 82; x < 114; x++ {
			img.Set(x, y, color.RGBA{R: 184, G: 122, B: 82, A: 255})
		}
	}
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X <= 60 {
		t.Fatalf("focus = %d,%d, want skin-tone region on right", focus.X, focus.Y)
	}
}

func TestFocusPercentSnapsToCentroidSidePastThreshold(t *testing.T) {
	tests := []struct {
		name       string
		normalized float64
		want       int
	}{
		{name: "center stays centered", normalized: 0.5, want: 50},
		{name: "close to center stays centered", normalized: 0.54, want: 50},
		{name: "near center keeps damping", normalized: 0.6, want: 59},
		{name: "left threshold snaps to damped edge", normalized: 0.31, want: 8},
		{name: "right threshold snaps to damped edge", normalized: 0.69, want: 93},
		{name: "far left snaps to edge", normalized: 0.16, want: 0},
		{name: "far right snaps to edge", normalized: 0.84, want: 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := focusPercent(tc.normalized); got != tc.want {
				t.Fatalf("focusPercent(%v) = %d, want %d", tc.normalized, got, tc.want)
			}
		})
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
	if focus.X != 0 || focus.Y != 100 {
		t.Fatalf("focus = %d,%d, want 0,100", focus.X, focus.Y)
	}

	focus = NormalizeImageFocus(-10, 3)
	if focus.X != 0 || focus.Y != 3 {
		t.Fatalf("focus = %d,%d, want 0,3", focus.X, focus.Y)
	}

	focus = NormalizeImageFocus(0, 0)
	if focus != DefaultImageFocus() {
		t.Fatalf("focus = %#v, want default", focus)
	}

	if got := NormalizeImageFocusValue(0); got != 50 {
		t.Fatalf("value focus = %d, want scalar zero to default to 50", got)
	}
	if got := NormalizeExplicitImageFocusValue(0); got != 0 {
		t.Fatalf("explicit value focus = %d, want edge 0", got)
	}
}
