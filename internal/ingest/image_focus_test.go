package ingest

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/png"
	"math"
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

func TestEstimateImageFocusUsesVerticalRectangleWithImageSide(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 180, 120))
	fillRect(img, img.Bounds(), color.RGBA{R: 54, G: 68, B: 82, A: 255})
	fillRect(img, image.Rect(0, 0, 80, 120), color.RGBA{R: 238, G: 232, B: 218, A: 255})
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X < 20 || focus.X > 24 || focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want center of left-side embedded rectangle", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusUsesVerticalRectangleBetweenEdges(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 220, 120))
	fillRect(img, img.Bounds(), color.RGBA{R: 38, G: 46, B: 55, A: 255})
	fillRect(img, image.Rect(90, 0, 170, 120), color.RGBA{R: 230, G: 224, B: 208, A: 255})
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X < 58 || focus.X > 60 || focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want center of embedded rectangle between vertical edges", focus.X, focus.Y)
	}
}

func TestEstimateImageFocusPrefersPhotoSideOverSparseTextSide(t *testing.T) {
	var body bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 240, 120))
	fillRect(img, img.Bounds(), color.RGBA{R: 242, G: 240, B: 234, A: 255})

	for _, rect := range []image.Rectangle{
		image.Rect(18, 16, 42, 18),
		image.Rect(18, 23, 76, 25),
		image.Rect(18, 30, 90, 32),
		image.Rect(40, 76, 108, 78),
		image.Rect(48, 84, 96, 86),
	} {
		fillRect(img, rect, color.RGBA{A: 255})
	}

	fillRect(img, image.Rect(120, 0, 240, 120), color.RGBA{R: 92, G: 88, B: 80, A: 255})
	for y := 0; y < 120; y++ {
		for x := 120; x < 240; x++ {
			if (x*7+y*11)%23 < 10 {
				img.Set(x, y, color.RGBA{R: 72, G: 84, B: 78, A: 255})
			}
			if y > 78 {
				img.Set(x, y, color.RGBA{R: 32, G: 28, B: 24, A: 255})
			}
		}
	}
	fillEllipse(img, 144, 50, 16, 21, color.RGBA{R: 184, G: 122, B: 82, A: 255})
	fillEllipse(img, 194, 52, 18, 23, color.RGBA{R: 188, G: 128, B: 86, A: 255})
	fillRect(img, image.Rect(136, 72, 210, 118), color.RGBA{R: 24, G: 24, B: 24, A: 255})
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	focus, err := EstimateImageFocus("image/png", body.Bytes())
	if err != nil {
		t.Fatalf("estimate image focus: %v", err)
	}
	if focus.X < 60 || focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want photo side preferred over sparse text side", focus.X, focus.Y)
	}
}

func TestVerticalRectangleFocusUsesRegionBoundaryFallback(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 260, 140))
	background := color.RGBA{R: 42, G: 48, B: 58, A: 255}
	poster := color.RGBA{R: 224, G: 216, B: 196, A: 255}
	fillRect(img, img.Bounds(), background)
	for y := 0; y < 140; y++ {
		for x := 80; x < 200; x++ {
			switch {
			case x < 116:
				img.Set(x, y, blendColor(background, poster, float64(x-80)/36))
			case x >= 164:
				img.Set(x, y, blendColor(poster, background, float64(x-164)/36))
			default:
				img.Set(x, y, poster)
			}
		}
	}

	red, green, blue, luma, width, height := focusTestSample(img)
	detection := detectVerticalRectangle(red, green, blue, luma, width, height, defaultVerticalRectangleParams())
	if !detection.Detected {
		t.Fatalf("vertical rectangle focus = %#v, want soft region boundary rectangle detection", detection)
	}
	if detection.Candidate.source != verticalRectangleCandidateSourceRegionBoundary {
		t.Fatalf("candidate source = %v, want region boundary fallback", detection.Candidate.source)
	}
	if detection.Focus.X < 50 || detection.Focus.X > 58 || detection.Focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want center of soft embedded rectangle", detection.Focus.X, detection.Focus.Y)
	}
}

func TestVerticalRectangleFocusUsesPanelRegionFallback(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 260, 140))
	background := color.RGBA{R: 40, G: 48, B: 56, A: 255}
	fillRect(img, img.Bounds(), background)
	for y := 0; y < 140; y++ {
		for x := 82; x < 178; x++ {
			if (x+y)%19 < 9 {
				img.Set(x, y, color.RGBA{R: 218, G: 207, B: 188, A: 255})
			} else {
				img.Set(x, y, color.RGBA{R: 176, G: 116, B: 92, A: 255})
			}
		}
	}

	red, green, blue, luma, width, height := focusTestSample(img)
	detection := detectVerticalRectangle(red, green, blue, luma, width, height, panelRegionOnlyParams())
	if !detection.Detected {
		t.Fatalf("vertical rectangle focus = %#v, want panel region detection", detection)
	}
	if detection.Candidate.source != verticalRectangleCandidateSourcePanelRegion {
		t.Fatalf("candidate source = %v, want panel region fallback", detection.Candidate.source)
	}
	if detection.Focus.X < 48 || detection.Focus.X > 52 || detection.Focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want center of embedded panel", detection.Focus.X, detection.Focus.Y)
	}
}

func TestVerticalRectangleFocusUsesSidePanelRegionFallback(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 220, 140))
	fillRect(img, img.Bounds(), color.RGBA{R: 42, G: 48, B: 58, A: 255})
	for y := 0; y < 140; y++ {
		for x := 0; x < 88; x++ {
			if (x*2+y)%23 < 11 {
				img.Set(x, y, color.RGBA{R: 216, G: 210, B: 192, A: 255})
			} else {
				img.Set(x, y, color.RGBA{R: 130, G: 84, B: 128, A: 255})
			}
		}
	}

	red, green, blue, luma, width, height := focusTestSample(img)
	detection := detectVerticalRectangle(red, green, blue, luma, width, height, panelRegionOnlyParams())
	if !detection.Detected {
		t.Fatalf("vertical rectangle focus = %#v, want side panel region detection", detection)
	}
	if detection.Candidate.source != verticalRectangleCandidateSourcePanelRegion {
		t.Fatalf("candidate source = %v, want panel region fallback", detection.Candidate.source)
	}
	if detection.Focus.X < 18 || detection.Focus.X > 24 || detection.Focus.Y != 50 {
		t.Fatalf("focus = %d,%d, want center of side embedded panel", detection.Focus.X, detection.Focus.Y)
	}
}

func TestVerticalRectangleFocusIgnoresPartialHeightPanelRegion(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 220, 140))
	fillRect(img, img.Bounds(), color.RGBA{R: 42, G: 48, B: 58, A: 255})
	fillRect(img, image.Rect(70, 30, 150, 108), color.RGBA{R: 222, G: 214, B: 194, A: 255})

	red, green, blue, luma, width, height := focusTestSample(img)
	if focus, ok := verticalRectangleFocusWithParams(red, green, blue, luma, width, height, panelRegionOnlyParams()); ok {
		t.Fatalf("vertical rectangle focus = %#v, want no full-height panel region detection", focus)
	}
}

func TestVerticalRectangleFocusIgnoresSmoothWholeImageGradient(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 220, 120))
	left := color.RGBA{R: 32, G: 38, B: 48, A: 255}
	right := color.RGBA{R: 226, G: 218, B: 204, A: 255}
	for y := 0; y < 120; y++ {
		for x := 0; x < 220; x++ {
			img.Set(x, y, blendColor(left, right, float64(x)/219))
		}
	}

	red, green, blue, luma, width, height := focusTestSample(img)
	if focus, ok := verticalRectangleFocus(red, green, blue, luma, width, height); ok {
		t.Fatalf("vertical rectangle focus = %#v, want no embedded rectangle in smooth gradient", focus)
	}
}

func TestVerticalRectangleFocusIgnoresPartialHeightEdges(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 180, 120))
	fillRect(img, img.Bounds(), color.RGBA{R: 60, G: 70, B: 82, A: 255})
	fillRect(img, image.Rect(0, 42, 80, 78), color.RGBA{R: 235, G: 230, B: 218, A: 255})

	red, green, blue, luma, width, height := focusTestSample(img)
	if focus, ok := verticalRectangleFocus(red, green, blue, luma, width, height); ok {
		t.Fatalf("vertical rectangle focus = %#v, want no full-height rectangle detection", focus)
	}
}

func TestVerticalRectangleFocusIgnoresRepeatingVerticalStripes(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 180, 120))
	for x := 0; x < 180; x += 12 {
		c := color.RGBA{R: 52, G: 62, B: 74, A: 255}
		if x/12%2 == 0 {
			c = color.RGBA{R: 230, G: 226, B: 214, A: 255}
		}
		fillRect(img, image.Rect(x, 0, minInt(180, x+12), 120), c)
	}

	red, green, blue, luma, width, height := focusTestSample(img)
	if focus, ok := verticalRectangleFocus(red, green, blue, luma, width, height); ok {
		t.Fatalf("vertical rectangle focus = %#v, want repeating vertical pattern ignored", focus)
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

func fillRect(img *image.RGBA, rect image.Rectangle, c color.RGBA) {
	for y := rect.Min.Y; y < rect.Max.Y; y++ {
		for x := rect.Min.X; x < rect.Max.X; x++ {
			img.Set(x, y, c)
		}
	}
}

func blendColor(from, to color.RGBA, amount float64) color.RGBA {
	if amount < 0 {
		amount = 0
	}
	if amount > 1 {
		amount = 1
	}
	return color.RGBA{
		R: uint8(math.Round(float64(from.R) + (float64(to.R)-float64(from.R))*amount)),
		G: uint8(math.Round(float64(from.G) + (float64(to.G)-float64(from.G))*amount)),
		B: uint8(math.Round(float64(from.B) + (float64(to.B)-float64(from.B))*amount)),
		A: 255,
	}
}

func panelRegionOnlyParams() verticalRectangleParams {
	params := defaultVerticalRectangleParams()
	params.MinMaxEdgeStrength = 2
	params.RegionBoundaryMinMaxStrength = 2
	params.PanelRegionMinCoverage = 0.76
	params.PanelRegionMinMeanContrast = 0.09
	params.PanelRegionMinScore = 0.12
	return params
}

func focusTestSample(img image.Image) ([]float64, []float64, []float64, []float64, int, int) {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	sampleWidth, sampleHeight := focusSampleSize(width, height)
	red := make([]float64, sampleWidth*sampleHeight)
	green := make([]float64, sampleWidth*sampleHeight)
	blue := make([]float64, sampleWidth*sampleHeight)
	luma := make([]float64, sampleWidth*sampleHeight)
	for y := 0; y < sampleHeight; y++ {
		srcY := bounds.Min.Y + minInt(height-1, y*height/sampleHeight)
		for x := 0; x < sampleWidth; x++ {
			srcX := bounds.Min.X + minInt(width-1, x*width/sampleWidth)
			c := color.NRGBAModel.Convert(img.At(srcX, srcY)).(color.NRGBA)
			idx := y*sampleWidth + x
			r := float64(c.R) / 255
			g := float64(c.G) / 255
			b := float64(c.B) / 255
			red[idx] = r
			green[idx] = g
			blue[idx] = b
			luma[idx] = 0.2126*r + 0.7152*g + 0.0722*b
		}
	}
	return red, green, blue, luma, sampleWidth, sampleHeight
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

func TestFocusPercentUsesCentroidOutsideCenterHold(t *testing.T) {
	tests := []struct {
		name       string
		normalized float64
		want       int
	}{
		{name: "center stays centered", normalized: 0.5, want: 50},
		{name: "close to center stays centered", normalized: 0.54, want: 50},
		{name: "right of center uses centroid", normalized: 0.6, want: 60},
		{name: "left side uses centroid", normalized: 0.31, want: 31},
		{name: "right side uses centroid", normalized: 0.69, want: 69},
		{name: "far left uses centroid", normalized: 0.16, want: 16},
		{name: "far right uses centroid", normalized: 0.84, want: 84},
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

func TestNormalizeEstimatedImageFocusPreservesExplicitZero(t *testing.T) {
	if focus := normalizeEstimatedImageFocus(0, 0); focus != (ImageFocus{X: 0, Y: 0}) {
		t.Fatalf("focus = %#v, want explicit zero to remain zero", focus)
	}
}
