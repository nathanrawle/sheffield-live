package ingest

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"math"
	"strings"
)

const (
	DefaultImageFocusX = 50
	DefaultImageFocusY = 50
)

var (
	ErrImageFocusUnsupported = errors.New("image focus unsupported")
	ErrImageFocusNoSignal    = errors.New("image focus has no saliency signal")
)

type ImageFocus struct {
	X int
	Y int
}

func DefaultImageFocus() ImageFocus {
	return ImageFocus{X: DefaultImageFocusX, Y: DefaultImageFocusY}
}

func NormalizeImageFocus(x, y int) ImageFocus {
	return ImageFocus{
		X: NormalizeImageFocusValue(x),
		Y: NormalizeImageFocusValue(y),
	}
}

func NormalizeImageFocusValue(value int) int {
	if value == 0 {
		return DefaultImageFocusX
	}
	if value < 5 {
		return 5
	}
	if value > 95 {
		return 95
	}
	return value
}

func BestEffortImageFocus(contentType string, body []byte) ImageFocus {
	focus, err := EstimateImageFocus(contentType, body)
	if err != nil {
		return DefaultImageFocus()
	}
	return focus
}

func EstimateImageFocus(contentType string, body []byte) (ImageFocus, error) {
	contentType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	if contentType == "image/webp" {
		return DefaultImageFocus(), ErrImageFocusUnsupported
	}
	img, _, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return DefaultImageFocus(), err
	}
	return estimateDecodedImageFocus(img)
}

func estimateDecodedImageFocus(img image.Image) (ImageFocus, error) {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return DefaultImageFocus(), ErrImageFocusNoSignal
	}

	sampleWidth, sampleHeight := focusSampleSize(width, height)
	luma := make([]float64, sampleWidth*sampleHeight)
	saturation := make([]float64, sampleWidth*sampleHeight)
	for y := 0; y < sampleHeight; y++ {
		srcY := bounds.Min.Y + minInt(height-1, y*height/sampleHeight)
		for x := 0; x < sampleWidth; x++ {
			srcX := bounds.Min.X + minInt(width-1, x*width/sampleWidth)
			c := color.NRGBAModel.Convert(img.At(srcX, srcY)).(color.NRGBA)
			idx := y*sampleWidth + x
			r := float64(c.R) / 255
			g := float64(c.G) / 255
			b := float64(c.B) / 255
			luma[idx] = 0.2126*r + 0.7152*g + 0.0722*b
			maxChannel := math.Max(r, math.Max(g, b))
			minChannel := math.Min(r, math.Min(g, b))
			if maxChannel > 0 {
				saturation[idx] = (maxChannel - minChannel) / maxChannel
			}
		}
	}

	var total, weightedX, weightedY float64
	for y := 0; y < sampleHeight; y++ {
		for x := 0; x < sampleWidth; x++ {
			idx := y*sampleWidth + x
			contrast := 0.0
			if x+1 < sampleWidth {
				contrast += math.Abs(luma[idx] - luma[idx+1])
			}
			if y+1 < sampleHeight {
				contrast += math.Abs(luma[idx] - luma[idx+sampleWidth])
			}
			if contrast <= 0 {
				continue
			}
			nx := (float64(x) + 0.5) / float64(sampleWidth)
			ny := (float64(y) + 0.5) / float64(sampleHeight)
			dist := math.Hypot(nx-0.5, ny-0.5) / math.Sqrt(0.5)
			centerBias := 0.75 + 0.25*(1-math.Min(1, dist))
			score := contrast * (1 + 0.6*saturation[idx]) * centerBias
			total += score
			weightedX += score * nx
			weightedY += score * ny
		}
	}
	if total <= 0.000001 {
		return DefaultImageFocus(), ErrImageFocusNoSignal
	}

	return NormalizeImageFocus(
		int(math.Round(weightedX/total*100)),
		int(math.Round(weightedY/total*100)),
	), nil
}

func focusSampleSize(width, height int) (int, int) {
	const maxSample = 128
	if width <= maxSample && height <= maxSample {
		return width, height
	}
	if width >= height {
		return maxSample, maxInt(1, int(math.Round(float64(height)*maxSample/float64(width))))
	}
	return maxInt(1, int(math.Round(float64(width)*maxSample/float64(height)))), maxSample
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
