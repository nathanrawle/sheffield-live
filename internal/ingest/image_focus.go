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
	if x == 0 && y == 0 {
		return DefaultImageFocus()
	}
	return ImageFocus{
		X: NormalizeExplicitImageFocusValue(x),
		Y: NormalizeExplicitImageFocusValue(y),
	}
}

func NormalizeImageFocusValue(value int) int {
	if value == 0 {
		return DefaultImageFocusX
	}
	return NormalizeExplicitImageFocusValue(value)
}

func NormalizeExplicitImageFocusValue(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
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
	skinTone := make([]float64, sampleWidth*sampleHeight)
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
			skinTone[idx] = skinToneScore(r, g, b)
		}
	}

	saliency := make([]float64, sampleWidth*sampleHeight)
	for y := 0; y < sampleHeight; y++ {
		for x := 0; x < sampleWidth; x++ {
			idx := y*sampleWidth + x
			left := luma[y*sampleWidth+maxInt(0, x-1)]
			right := luma[y*sampleWidth+minInt(sampleWidth-1, x+1)]
			up := luma[maxInt(0, y-1)*sampleWidth+x]
			down := luma[minInt(sampleHeight-1, y+1)*sampleWidth+x]
			contrast := math.Hypot(right-left, down-up)
			if contrast > 0 {
				saliency[idx] = contrast * (1 + 0.7*saturation[idx]) * (1 + 2.0*skinTone[idx])
			}
			saliency[idx] += 0.09 * skinTone[idx]
		}
	}

	total, weightedX, weightedY := mostSalientFocusWindow(saliency, sampleWidth, sampleHeight)
	if total <= 0.000001 {
		return DefaultImageFocus(), ErrImageFocusNoSignal
	}

	return NormalizeImageFocus(
		focusPercent(weightedX/total),
		focusPercent(weightedY/total),
	), nil
}

func focusPercent(normalized float64) int {
	const (
		localFocusStrength       = 0.85
		centerHoldThreshold      = 5.0
		sideSnapThresholdPercent = 18.0
		hardSnapThresholdPercent = 34.0
	)
	target := normalized * 100
	offset := target - 50
	absOffset := math.Abs(offset)
	if absOffset <= centerHoldThreshold {
		return 50
	}
	if absOffset >= hardSnapThresholdPercent {
		if offset < 0 {
			return 0
		}
		return 100
	}
	if absOffset >= sideSnapThresholdPercent {
		target = 100
		if offset < 0 {
			target = 0
		}
	}
	percent := 50 + (target-50)*localFocusStrength
	return int(math.Round(percent))
}

func skinToneScore(r, g, b float64) float64 {
	maxChannel := math.Max(r, math.Max(g, b))
	minChannel := math.Min(r, math.Min(g, b))
	if maxChannel < 0.14 || maxChannel-minChannel < 0.035 {
		return 0
	}
	hue := rgbHueDegrees(r, g, b, maxChannel, minChannel)
	if hue > 70 && hue < 335 {
		return 0
	}
	saturation := (maxChannel - minChannel) / maxChannel
	if saturation < 0.08 || saturation > 0.82 {
		return 0
	}
	luma := 0.2126*r + 0.7152*g + 0.0722*b
	if luma < 0.12 || luma > 0.95 {
		return 0
	}
	hueDistance := math.Min(math.Abs(hue-24), math.Min(math.Abs(hue+360-24), math.Abs(hue-360-24)))
	hueScore := 1 - math.Min(1, hueDistance/46)
	warmthScore := clampFloat((r-b+0.08)/0.36, 0, 1)
	saturationScore := 1 - math.Min(1, math.Abs(saturation-0.38)/0.44)
	return hueScore * warmthScore * saturationScore
}

func rgbHueDegrees(r, g, b, maxChannel, minChannel float64) float64 {
	delta := maxChannel - minChannel
	if delta <= 0 {
		return 0
	}
	var hue float64
	switch maxChannel {
	case r:
		hue = math.Mod((g-b)/delta, 6)
	case g:
		hue = (b-r)/delta + 2
	default:
		hue = (r-g)/delta + 4
	}
	hue *= 60
	if hue < 0 {
		hue += 360
	}
	return hue
}

func mostSalientFocusWindow(saliency []float64, width, height int) (float64, float64, float64) {
	windowWidth, windowHeight := focusWindowSize(width, height)
	integral := saliencyIntegral(saliency, width, height)
	bestX := 0
	bestY := 0
	bestScore := 0.0
	for y := 0; y <= height-windowHeight; y++ {
		for x := 0; x <= width-windowWidth; x++ {
			sum := saliencyWindowSum(integral, width, x, y, x+windowWidth, y+windowHeight)
			if sum <= 0 {
				continue
			}
			nx := (float64(x) + float64(windowWidth)/2) / float64(width)
			ny := (float64(y) + float64(windowHeight)/2) / float64(height)
			dist := math.Hypot(nx-0.5, ny-0.5) / math.Sqrt(0.5)
			centerTieBreak := 0.95 + 0.05*(1-math.Min(1, dist))
			score := sum * centerTieBreak
			if score > bestScore {
				bestScore = score
				bestX = x
				bestY = y
			}
		}
	}
	if bestScore <= 0 {
		return 0, 0, 0
	}

	var total, weightedX, weightedY float64
	for y := bestY; y < bestY+windowHeight; y++ {
		for x := bestX; x < bestX+windowWidth; x++ {
			score := saliency[y*width+x]
			if score <= 0 {
				continue
			}
			total += score
			weightedX += score * (float64(x) + 0.5) / float64(width)
			weightedY += score * (float64(y) + 0.5) / float64(height)
		}
	}
	return total, weightedX, weightedY
}

func focusWindowSize(width, height int) (int, int) {
	const fraction = 0.32
	windowWidth := int(math.Round(float64(width) * fraction))
	windowHeight := int(math.Round(float64(height) * fraction))
	return clampInt(windowWidth, minInt(width, 8), width), clampInt(windowHeight, minInt(height, 8), height)
}

func saliencyIntegral(values []float64, width, height int) []float64 {
	stride := width + 1
	integral := make([]float64, (height+1)*stride)
	for y := 0; y < height; y++ {
		rowTotal := 0.0
		for x := 0; x < width; x++ {
			rowTotal += values[y*width+x]
			integral[(y+1)*stride+x+1] = integral[y*stride+x+1] + rowTotal
		}
	}
	return integral
}

func saliencyWindowSum(integral []float64, width, x0, y0, x1, y1 int) float64 {
	stride := width + 1
	return integral[y1*stride+x1] - integral[y0*stride+x1] - integral[y1*stride+x0] + integral[y0*stride+x0]
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

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func clampFloat(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
