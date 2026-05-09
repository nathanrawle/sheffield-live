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

func TestEstimateImageFocusUsesCoarseBlobForEdgeNoisyBackground(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 160, 120))
	for y := 0; y < 120; y++ {
		for x := 0; x < 160; x++ {
			img.Set(x, y, color.RGBA{R: 155, G: 130, B: 102, A: 255})
			if (x+2*y)%17 == 0 || (2*x-y+160)%23 == 0 || (x-y+120)%19 == 0 {
				img.Set(x, y, color.RGBA{R: 54, G: 38, B: 27, A: 255})
			}
		}
	}
	fillEllipse(img, 80, 68, 23, 35, color.RGBA{R: 58, G: 82, B: 104, A: 255})
	fillEllipse(img, 80, 36, 13, 14, color.RGBA{R: 184, G: 122, B: 82, A: 255})
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X < 40 || focus.X > 60 || focus.Y < 35 || focus.Y > 70 {
		t.Fatalf("focus = %d,%d, want centered human figure", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusPrioritizesSkinToneRegion(t *testing.T) {
	skin := chromaticSkinToneScore(0.72, 0.48, 0.32, rgbHueDegrees(0.72, 0.48, 0.32, 0.72, 0.32), 0.72, 0.32)
	blue := chromaticSkinToneScore(0.08, 0.27, 0.94, rgbHueDegrees(0.08, 0.27, 0.94, 0.94, 0.08), 0.94, 0.08)
	if skin <= 0.5 {
		t.Fatalf("skin score = %v, want strong positive score", skin)
	}
	if blue != 0 {
		t.Fatalf("blue score = %v, want 0", blue)
	}
}

func fillEllipse(img *image.RGBA, centerX, centerY, radiusX, radiusY int, c color.RGBA) {
	for y := centerY - radiusY; y <= centerY+radiusY; y++ {
		for x := centerX - radiusX; x <= centerX+radiusX; x++ {
			dx := float64(x-centerX) / float64(radiusX)
			dy := float64(y-centerY) / float64(radiusY)
			if dx*dx+dy*dy <= 1 {
				img.Set(x, y, c)
			}
		}
	}
}

func TestEstimateImageFocusPrioritizesSepiaSkinToneRange(t *testing.T) {
	profile := imageToneProfileForSample(100, 0, 75)
	skin := monochromeSkinToneScore(0.58, 0.24, profile)
	shadow := monochromeSkinToneScore(0.12, 0.24, profile)
	if profile.sepia <= 0 {
		t.Fatalf("sepia profile = %#v, want positive sepia score", profile)
	}
	if skin <= 0.5 {
		t.Fatalf("sepia skin proxy = %v, want strong positive score", skin)
	}
	if shadow >= skin {
		t.Fatalf("sepia shadow proxy = %v, want less than skin proxy %v", shadow, skin)
	}
}

func TestEstimateImageFocusPrioritizesGrayscaleSkinToneRange(t *testing.T) {
	profile := imageToneProfileForSample(100, 80, 0)
	skin := monochromeSkinToneScore(0.58, 0.02, profile)
	shadow := monochromeSkinToneScore(0.12, 0.02, profile)
	if profile.grayscale <= 0 {
		t.Fatalf("grayscale profile = %#v, want positive grayscale score", profile)
	}
	if skin <= 0.4 {
		t.Fatalf("grayscale skin proxy = %v, want strong positive score", skin)
	}
	if shadow >= skin {
		t.Fatalf("grayscale shadow proxy = %v, want less than skin proxy %v", shadow, skin)
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
