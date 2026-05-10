package ingest

import (
	"bytes"
	"embed"
	"errors"
	"image"
	"image/color"
	"math"
	"sort"
	"strings"
	"sync"

	pigo "github.com/esimov/pigo/core"
)

const (
	DefaultImageFocusX = 50
	DefaultImageFocusY = 50
)

var (
	ErrImageFocusUnsupported = errors.New("image focus unsupported")
	ErrImageFocusTooLarge    = errors.New("image focus image too large")
	ErrImageFocusNoSignal    = errors.New("image focus has no saliency signal")
)

//go:embed assets/pigo/*
var pigoAssetFS embed.FS

const (
	facePriorMinScore         = 5.0
	facePriorMaxDetections    = 3
	facePriorMaxBoost         = 0.60
	facePriorMaxScaleFraction = 0.95
	facePriorClusterIoU       = 0.2
	facePriorMinSampleSize    = 20
	facePriorDefaultShift     = 0.1
	facePriorDefaultScale     = 1.1
	facePriorDefaultAngle     = 0.0
)

var (
	defaultFacePriorDetectorOnce sync.Once
	defaultFacePriorDetector     facePriorDetector
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

func normalizeEstimatedImageFocus(x, y int) ImageFocus {
	return ImageFocus{
		X: NormalizeExplicitImageFocusValue(x),
		Y: NormalizeExplicitImageFocusValue(y),
	}
}

func BestEffortImageFocus(contentType string, body []byte) ImageFocus {
	focus, err := EstimateImageFocus(contentType, body)
	if err != nil {
		return DefaultImageFocus()
	}
	return focus
}

func EstimateImageFocusWithinLimits(contentType string, body []byte) (ImageFocus, error) {
	contentType = normalizedImageContentType(contentType)
	width, height, err := imageDimensions(contentType, body)
	if err != nil {
		return DefaultImageFocus(), err
	}
	if !imageWithinFocusLimits(width, height) {
		return DefaultImageFocus(), ErrImageFocusTooLarge
	}
	return EstimateImageFocus(contentType, body)
}

func EstimateImageFocus(contentType string, body []byte) (ImageFocus, error) {
	contentType = normalizedImageContentType(contentType)
	if contentType == "image/webp" {
		return DefaultImageFocus(), ErrImageFocusUnsupported
	}
	img, _, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return DefaultImageFocus(), err
	}
	return estimateDecodedImageFocus(img)
}

func normalizedImageContentType(contentType string) string {
	return strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
}

func estimateDecodedImageFocus(img image.Image) (ImageFocus, error) {
	return estimateDecodedImageFocusWithDetector(img, nil)
}

func estimateDecodedImageFocusWithDetector(img image.Image, detector facePriorDetector) (ImageFocus, error) {
	sample := sampleImageFocus(img)
	if sample.width <= 0 || sample.height <= 0 {
		return DefaultImageFocus(), ErrImageFocusNoSignal
	}

	if focus, ok := verticalRectangleFocus(sample.red, sample.green, sample.blue, sample.luma, sample.width, sample.height); ok {
		return focus, nil
	}
	toneProfile := imageToneProfileForSample(sample.width*sample.height, sample.grayLikePixels, sample.sepiaLikePixels)

	fineSaliency := make([]float64, sample.width*sample.height)
	saturation := make([]float64, sample.width*sample.height)
	chromaticSkinTone := make([]float64, sample.width*sample.height)
	skinTone := make([]float64, sample.width*sample.height)
	for y := 0; y < sample.height; y++ {
		for x := 0; x < sample.width; x++ {
			idx := y*sample.width + x
			maxChannel := math.Max(sample.red[idx], math.Max(sample.green[idx], sample.blue[idx]))
			minChannel := math.Min(sample.red[idx], math.Min(sample.green[idx], sample.blue[idx]))
			hue := rgbHueDegrees(sample.red[idx], sample.green[idx], sample.blue[idx], maxChannel, minChannel)
			if maxChannel > 0 {
				saturation[idx] = (maxChannel - minChannel) / maxChannel
			}
			chromaticSkinTone[idx] = chromaticSkinToneScore(sample.red[idx], sample.green[idx], sample.blue[idx], hue, maxChannel, minChannel)
			skinTone[idx] = maxFloat(chromaticSkinTone[idx], monochromeSkinToneScore(sample.luma[idx], saturation[idx], toneProfile))
			left := sample.luma[y*sample.width+maxInt(0, x-1)]
			right := sample.luma[y*sample.width+minInt(sample.width-1, x+1)]
			up := sample.luma[maxInt(0, y-1)*sample.width+x]
			down := sample.luma[minInt(sample.height-1, y+1)*sample.width+x]
			contrast := math.Hypot(right-left, down-up)
			if contrast > 0 {
				base := contrast * (1 + 0.7*saturation[idx])
				fineSaliency[idx] = base * (1 + 2.0*skinTone[idx])
			}
		}
	}
	coarseSaliency := coarseBlobSaliency(sample.luma, saturation, skinTone, sample.width, sample.height)
	saliency := combineFineAndCoarseSaliency(fineSaliency, coarseSaliency)
	if detector == nil {
		detector = loadDefaultFacePriorDetector()
	}
	if detector != nil {
		if detections, err := detector.Detect(sample); err == nil && len(detections) > 0 {
			detections = filterFacePriorDetections(detections, sample.width, sample.height)
			if len(detections) > 0 {
				saliency = applyFacePriorBoost(saliency, sample.width, sample.height, detections)
			}
		}
	}

	total, weightedX, weightedY := mostSalientFocusWindow(saliency, sample.width, sample.height)
	if total <= 0.000001 {
		return DefaultImageFocus(), ErrImageFocusNoSignal
	}

	return normalizeEstimatedImageFocus(
		focusPercent(weightedX/total),
		focusPercent(weightedY/total),
	), nil
}

type imageFocusSample struct {
	width           int
	height          int
	red             []float64
	green           []float64
	blue            []float64
	luma            []float64
	grayscale       []uint8
	grayLikePixels  int
	sepiaLikePixels int
}

type facePriorDetector interface {
	Detect(imageFocusSample) ([]pigo.Detection, error)
}

type pigoFacePriorDetector struct {
	classifier *pigo.Pigo
}

func sampleImageFocus(img image.Image) imageFocusSample {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return imageFocusSample{}
	}

	sampleWidth, sampleHeight := focusSampleSize(width, height)
	sample := imageFocusSample{
		width:     sampleWidth,
		height:    sampleHeight,
		red:       make([]float64, sampleWidth*sampleHeight),
		green:     make([]float64, sampleWidth*sampleHeight),
		blue:      make([]float64, sampleWidth*sampleHeight),
		luma:      make([]float64, sampleWidth*sampleHeight),
		grayscale: make([]uint8, sampleWidth*sampleHeight),
	}

	for y := 0; y < sampleHeight; y++ {
		srcY := bounds.Min.Y + minInt(height-1, y*height/sampleHeight)
		for x := 0; x < sampleWidth; x++ {
			srcX := bounds.Min.X + minInt(width-1, x*width/sampleWidth)
			c := color.NRGBAModel.Convert(img.At(srcX, srcY)).(color.NRGBA)
			idx := y*sampleWidth + x
			r := float64(c.R) / 255
			g := float64(c.G) / 255
			b := float64(c.B) / 255
			sample.red[idx] = r
			sample.green[idx] = g
			sample.blue[idx] = b
			sample.luma[idx] = 0.2126*r + 0.7152*g + 0.0722*b
			sample.grayscale[idx] = uint8(math.Round((0.299*r + 0.587*g + 0.114*b) * 255))
			maxChannel := math.Max(r, math.Max(g, b))
			minChannel := math.Min(r, math.Min(g, b))
			hue := rgbHueDegrees(r, g, b, maxChannel, minChannel)
			saturation := 0.0
			if maxChannel > 0 {
				saturation = (maxChannel - minChannel) / maxChannel
			}
			if saturation < 0.08 {
				sample.grayLikePixels++
			}
			if sepiaToneScore(hue, saturation, sample.luma[idx]) > 0.4 {
				sample.sepiaLikePixels++
			}
		}
	}
	return sample
}

func loadDefaultFacePriorDetector() facePriorDetector {
	defaultFacePriorDetectorOnce.Do(func() {
		defaultFacePriorDetector, _ = newPigoFacePriorDetector()
	})
	return defaultFacePriorDetector
}

func newPigoFacePriorDetector() (*pigoFacePriorDetector, error) {
	cascade, err := pigoAssetFS.ReadFile("assets/pigo/facefinder")
	if err != nil {
		return nil, err
	}
	classifier, err := pigo.NewPigo().Unpack(cascade)
	if err != nil {
		return nil, err
	}
	return &pigoFacePriorDetector{classifier: classifier}, nil
}

func (detector *pigoFacePriorDetector) Detect(sample imageFocusSample) ([]pigo.Detection, error) {
	if detector == nil || detector.classifier == nil || sample.width <= 0 || sample.height <= 0 {
		return nil, nil
	}

	minSize := maxInt(facePriorMinSampleSize, minInt(sample.width, sample.height)/6)
	maxSize := minInt(sample.width, sample.height)
	if minSize > maxSize {
		minSize = maxSize
	}
	if maxSize < 1 || minSize < 1 {
		return nil, nil
	}

	detections := detector.classifier.RunCascade(pigo.CascadeParams{
		MinSize:     minSize,
		MaxSize:     maxSize,
		ShiftFactor: facePriorDefaultShift,
		ScaleFactor: facePriorDefaultScale,
		ImageParams: pigo.ImageParams{
			Pixels: sample.grayscale,
			Rows:   sample.height,
			Cols:   sample.width,
			Dim:    sample.width,
		},
	}, facePriorDefaultAngle)
	if len(detections) == 0 {
		return nil, nil
	}
	detections = detector.classifier.ClusterDetections(detections, facePriorClusterIoU)
	detections = filterFacePriorDetections(detections, sample.width, sample.height)
	return detections, nil
}

func filterFacePriorDetections(detections []pigo.Detection, width, height int) []pigo.Detection {
	if len(detections) == 0 || width <= 0 || height <= 0 {
		return nil
	}

	minSize := maxInt(facePriorMinSampleSize, minInt(width, height)/6)
	maxSize := minInt(width, height)
	fullImageLimit := maxSize
	if facePriorMaxScaleFraction > 0 && facePriorMaxScaleFraction < 1 {
		fullImageLimit = maxInt(1, int(math.Round(float64(maxSize)*facePriorMaxScaleFraction)))
	}
	accepted := make([]pigo.Detection, 0, len(detections))
	for _, det := range detections {
		if det.Q < facePriorMinScore {
			continue
		}
		if det.Scale < minSize || det.Scale >= fullImageLimit {
			continue
		}
		half := float64(det.Scale) / 2
		row := float64(det.Row)
		col := float64(det.Col)
		if row-half < 0 || col-half < 0 || row+half > float64(height) || col+half > float64(width) {
			continue
		}
		accepted = append(accepted, det)
	}
	sort.Slice(accepted, func(i, j int) bool {
		if accepted[i].Q == accepted[j].Q {
			return accepted[i].Scale > accepted[j].Scale
		}
		return accepted[i].Q > accepted[j].Q
	})
	if len(accepted) > facePriorMaxDetections {
		accepted = accepted[:facePriorMaxDetections]
	}
	return accepted
}

func applyFacePriorBoost(saliency []float64, width, height int, detections []pigo.Detection) []float64 {
	if len(saliency) == 0 || width <= 0 || height <= 0 || len(detections) == 0 {
		return saliency
	}

	boosted := append([]float64(nil), saliency...)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			idx := y*width + x
			value := boosted[idx]
			if value <= 0 {
				continue
			}
			factor := 1.0
			for _, det := range detections {
				boost := facePriorBoostFactor(float64(x)+0.5, float64(y)+0.5, det)
				if boost <= 0 {
					continue
				}
				factor *= 1 + boost
				if factor >= 1+facePriorMaxBoost {
					factor = 1 + facePriorMaxBoost
					break
				}
			}
			boosted[idx] = value * factor
		}
	}
	return boosted
}

func facePriorBoostFactor(x, y float64, det pigo.Detection) float64 {
	radius := float64(det.Scale)
	if radius <= 0 {
		return 0
	}
	dx := math.Abs(x - float64(det.Col))
	dy := math.Abs(y - float64(det.Row))
	if dx > radius || dy > radius {
		return 0
	}
	nx := dx / radius
	ny := dy / radius
	distance := math.Hypot(nx, ny)
	if distance >= 1 {
		return 0
	}
	return facePriorMaxBoost * (1 - distance*distance)
}

func focusPercent(normalized float64) int {
	const centerHoldThreshold = 5.0
	target := normalized * 100
	offset := target - 50
	absOffset := math.Abs(offset)
	if absOffset <= centerHoldThreshold {
		return 50
	}
	return int(math.Round(target))
}

type verticalEdgeCandidate struct {
	x        int
	score    float64
	coverage float64
	run      float64
}

type verticalRectangleCandidate struct {
	left                int
	right               int
	score               float64
	source              verticalRectangleCandidateSource
	contentScore        float64
	coverage            float64
	meanContrast        float64
	meanBoundarySupport float64
	meanBalance         float64
	aspect              float64
	widthFraction       float64
	oneSided            bool
}

type verticalRectangleCandidateSource int

const (
	verticalRectangleCandidateSourceNone verticalRectangleCandidateSource = iota
	verticalRectangleCandidateSourceEdge
	verticalRectangleCandidateSourceRegionBoundary
	verticalRectangleCandidateSourcePanelRegion
)

func (source verticalRectangleCandidateSource) String() string {
	switch source {
	case verticalRectangleCandidateSourceEdge:
		return "edge"
	case verticalRectangleCandidateSourceRegionBoundary:
		return "region_boundary"
	case verticalRectangleCandidateSourcePanelRegion:
		return "panel_region"
	default:
		return ""
	}
}

type verticalRectangleParams struct {
	MaxSample                        int
	MinSampleDimension               int
	ColorContrastWeight              float64
	EdgeGroupDistance                int
	EdgePoolRadius                   int
	MinEdgeCoverage                  float64
	MinEdgeRun                       float64
	MinMeanThresholdRatio            float64
	EdgeScoreCoverageBase            float64
	EdgeScoreRunBase                 float64
	EdgeNeighborWeight               float64
	MinMaxEdgeStrength               float64
	EdgeThresholdFloor               float64
	EdgeThresholdMaxRatio            float64
	EdgeThresholdMeanRatio           float64
	RegionBoundaryStripWidth         int
	RegionBoundaryBandCount          int
	RegionBoundaryMinCoverage        float64
	RegionBoundaryMinRun             float64
	RegionBoundaryMinMaxStrength     float64
	RegionBoundaryThresholdFloor     float64
	RegionBoundaryThresholdMaxRatio  float64
	RegionBoundaryThresholdMeanRatio float64
	RectangleContentScoreWeight      float64
	PanelRegionBandCount             int
	PanelRegionGutterWidth           int
	PanelRegionStep                  int
	PanelRegionWidthStep             int
	PanelRegionMinBandContrast       float64
	PanelRegionMinCoverage           float64
	PanelRegionMinMeanContrast       float64
	PanelRegionMinBoundarySupport    float64
	PanelRegionMinScore              float64
	PanelRegionOutsideAgreementRatio float64
	PanelRegionSideCandidateWeight   float64
	RepeatingEdgeMinCount            int
	RepeatingEdgeScoreRatio          float64
	SideRectangleEdgeWeight          float64
	AspectMin                        float64
	AspectMax                        float64
	AspectTargets                    []float64
	AspectDistanceScale              float64
	AspectScoreFloor                 float64
	AspectScoreRange                 float64
}

type verticalRectangleDetection struct {
	Focus                        ImageFocus
	Detected                     bool
	Candidate                    verticalRectangleCandidate
	EdgeCandidateCount           int
	RegionBoundaryCandidateCount int
	PanelRegionCandidateCount    int
	RectangleCandidateCount      int
	RepeatingPatternRejected     bool
	EdgeThreshold                float64
	RegionBoundaryThreshold      float64
	PanelRegionBestScore         float64
	PanelRegionBestCandidate     verticalRectangleCandidate
	PanelRegionHasBestCandidate  bool
}

func defaultVerticalRectangleParams() verticalRectangleParams {
	return verticalRectangleParams{
		MaxSample:                        128,
		MinSampleDimension:               24,
		ColorContrastWeight:              0.85,
		EdgeGroupDistance:                2,
		EdgePoolRadius:                   1,
		MinEdgeCoverage:                  0.50,
		MinEdgeRun:                       0.40,
		MinMeanThresholdRatio:            0.9,
		EdgeScoreCoverageBase:            0.45,
		EdgeScoreRunBase:                 0.45,
		EdgeNeighborWeight:               0.85,
		MinMaxEdgeStrength:               0.14,
		EdgeThresholdFloor:               0.11,
		EdgeThresholdMaxRatio:            0.18,
		EdgeThresholdMeanRatio:           3.0,
		RegionBoundaryStripWidth:         5,
		RegionBoundaryBandCount:          24,
		RegionBoundaryMinCoverage:        0.84,
		RegionBoundaryMinRun:             0.64,
		RegionBoundaryMinMaxStrength:     0.12,
		RegionBoundaryThresholdFloor:     0.055,
		RegionBoundaryThresholdMaxRatio:  0.32,
		RegionBoundaryThresholdMeanRatio: 1.85,
		RectangleContentScoreWeight:      0.45,
		PanelRegionBandCount:             12,
		PanelRegionGutterWidth:           8,
		PanelRegionStep:                  4,
		PanelRegionWidthStep:             4,
		PanelRegionMinBandContrast:       0.055,
		PanelRegionMinCoverage:           0.96,
		PanelRegionMinMeanContrast:       0.16,
		PanelRegionMinBoundarySupport:    0.14,
		PanelRegionMinScore:              0.60,
		PanelRegionOutsideAgreementRatio: 0.60,
		PanelRegionSideCandidateWeight:   0.82,
		RepeatingEdgeMinCount:            5,
		RepeatingEdgeScoreRatio:          0.62,
		SideRectangleEdgeWeight:          0.92,
		AspectMin:                        0.45,
		AspectMax:                        1.90,
		AspectTargets:                    []float64{0.67, 0.71, 0.77, 1.50, 1.78},
		AspectDistanceScale:              0.45,
		AspectScoreFloor:                 0.55,
		AspectScoreRange:                 0.45,
	}
}

func cloneVerticalRectangleParams(params verticalRectangleParams) verticalRectangleParams {
	params.AspectTargets = append([]float64(nil), params.AspectTargets...)
	return params
}

func verticalRectangleFocus(red, green, blue, luma []float64, width, height int) (ImageFocus, bool) {
	return verticalRectangleFocusWithParams(red, green, blue, luma, width, height, defaultVerticalRectangleParams())
}

func verticalRectangleFocusWithParams(red, green, blue, luma []float64, width, height int, params verticalRectangleParams) (ImageFocus, bool) {
	detection := detectVerticalRectangle(red, green, blue, luma, width, height, params)
	return detection.Focus, detection.Detected
}

func detectVerticalRectangle(red, green, blue, luma []float64, width, height int, params verticalRectangleParams) verticalRectangleDetection {
	result := verticalRectangleDetection{Focus: DefaultImageFocus()}
	if width < params.MinSampleDimension || height < params.MinSampleDimension || len(luma) != width*height || len(red) != len(luma) || len(green) != len(luma) || len(blue) != len(luma) {
		return result
	}

	edges := verticalEdgeStrengths(red, green, blue, luma, width, height, params)
	threshold := verticalEdgeThreshold(edges, params)
	result.EdgeThreshold = threshold
	rawEdgeCandidates := verticalEdgeCandidates(edges, width, height, threshold, params)
	edgeCandidates := groupedVerticalEdgeCandidates(rawEdgeCandidates, params)
	result.EdgeCandidateCount = len(edgeCandidates)
	if detectVerticalRectangleFromCandidates(edgeCandidates, red, green, blue, luma, width, height, params, verticalRectangleCandidateSourceEdge, &result) {
		return result
	}
	if result.RepeatingPatternRejected {
		return result
	}

	rawRegionBoundaryCandidates, regionThreshold := verticalRegionBoundaryCandidates(red, green, blue, luma, width, height, params)
	result.RegionBoundaryThreshold = regionThreshold
	regionBoundaryCandidates := groupedVerticalEdgeCandidates(rawRegionBoundaryCandidates, params)
	result.RegionBoundaryCandidateCount = len(regionBoundaryCandidates)
	if detectVerticalRectangleFromCandidates(regionBoundaryCandidates, red, green, blue, luma, width, height, params, verticalRectangleCandidateSourceRegionBoundary, &result) {
		return result
	}

	panelCandidates, panelBestCandidate, panelHasBestCandidate := panelRegionRectangleCandidates(red, green, blue, luma, width, height, params)
	result.PanelRegionCandidateCount = len(panelCandidates)
	result.PanelRegionBestCandidate = panelBestCandidate
	result.PanelRegionHasBestCandidate = panelHasBestCandidate
	if panelHasBestCandidate {
		result.PanelRegionBestScore = panelBestCandidate.score
	}
	detectVerticalRectangleFromRectangles(panelCandidates, width, &result)
	return result
}

func detectVerticalRectangleFromCandidates(candidates []verticalEdgeCandidate, red, green, blue, luma []float64, width, height int, params verticalRectangleParams, source verticalRectangleCandidateSource, result *verticalRectangleDetection) bool {
	if len(candidates) == 0 {
		return false
	}
	if hasRepeatingVerticalEdgePattern(candidates, params) {
		result.RepeatingPatternRejected = true
		return false
	}

	rectangles := verticalRectangleCandidates(candidates, red, green, blue, luma, width, height, params, source)
	result.RectangleCandidateCount += len(rectangles)
	if len(rectangles) == 0 {
		return false
	}
	sort.Slice(rectangles, func(i, j int) bool {
		return rectangles[i].score > rectangles[j].score
	})

	best := rectangles[0]
	centerX := (float64(best.left) + float64(best.right)) / 2 / float64(width)
	result.Focus = normalizeEstimatedImageFocus(int(math.Round(centerX*100)), DefaultImageFocusY)
	result.Candidate = best
	result.Detected = true
	return true
}

func detectVerticalRectangleFromRectangles(rectangles []verticalRectangleCandidate, width int, result *verticalRectangleDetection) bool {
	result.RectangleCandidateCount += len(rectangles)
	if len(rectangles) == 0 {
		return false
	}
	sort.Slice(rectangles, func(i, j int) bool {
		return rectangles[i].score > rectangles[j].score
	})

	best := rectangles[0]
	centerX := (float64(best.left) + float64(best.right)) / 2 / float64(width)
	result.Focus = normalizeEstimatedImageFocus(int(math.Round(centerX*100)), DefaultImageFocusY)
	result.Candidate = best
	result.Detected = true
	return true
}

func verticalEdgeStrengths(red, green, blue, luma []float64, width, height int, params verticalRectangleParams) []float64 {
	edges := make([]float64, (width-1)*height)
	for y := 0; y < height; y++ {
		for x := 1; x < width; x++ {
			left := y*width + x - 1
			right := left + 1
			dr := red[right] - red[left]
			dg := green[right] - green[left]
			db := blue[right] - blue[left]
			colorContrast := math.Sqrt(dr*dr+dg*dg+db*db) / math.Sqrt(3)
			lumaContrast := math.Abs(luma[right] - luma[left])
			edges[y*(width-1)+x-1] = math.Max(lumaContrast, colorContrast*params.ColorContrastWeight)
		}
	}
	return edges
}

func groupedVerticalEdgeCandidates(raw []verticalEdgeCandidate, params verticalRectangleParams) []verticalEdgeCandidate {
	if len(raw) == 0 {
		return nil
	}
	sort.Slice(raw, func(i, j int) bool {
		return raw[i].x < raw[j].x
	})

	groups := make([]verticalEdgeCandidate, 0, len(raw))
	best := raw[0]
	lastX := raw[0].x
	for _, candidate := range raw[1:] {
		if candidate.x-lastX <= params.EdgeGroupDistance {
			if candidate.score > best.score {
				best = candidate
			}
			lastX = candidate.x
			continue
		}
		groups = append(groups, best)
		best = candidate
		lastX = candidate.x
	}
	groups = append(groups, best)
	return groups
}

func verticalEdgeCandidates(edges []float64, width, height int, threshold float64, params verticalRectangleParams) []verticalEdgeCandidate {
	if threshold <= 0 {
		return nil
	}

	boundaryCount := width - 1
	candidates := make([]verticalEdgeCandidate, 0, boundaryCount)
	for x := 1; x < width; x++ {
		strongRows := 0
		currentRun := 0
		longestRun := 0
		totalStrength := 0.0
		for y := 0; y < height; y++ {
			strength := verticalEdgeStrengthAt(edges, boundaryCount, x, y, params)
			totalStrength += strength
			if strength >= threshold {
				strongRows++
				currentRun++
				if currentRun > longestRun {
					longestRun = currentRun
				}
			} else {
				currentRun = 0
			}
		}
		coverage := float64(strongRows) / float64(height)
		run := float64(longestRun) / float64(height)
		meanStrength := totalStrength / float64(height)
		if coverage < params.MinEdgeCoverage || run < params.MinEdgeRun || meanStrength < threshold*params.MinMeanThresholdRatio {
			continue
		}
		candidates = append(candidates, verticalEdgeCandidate{
			x:        x,
			score:    meanStrength * (params.EdgeScoreCoverageBase + coverage) * (params.EdgeScoreRunBase + run),
			coverage: coverage,
			run:      run,
		})
	}
	return candidates
}

func verticalEdgeStrengthAt(edges []float64, boundaryCount, x, y int, params verticalRectangleParams) float64 {
	radius := maxInt(0, params.EdgePoolRadius)
	strength := 0.0
	for offset := -radius; offset <= radius; offset++ {
		edgeX := x + offset
		if edgeX < 1 || edgeX > boundaryCount {
			continue
		}
		distance := offset
		if distance < 0 {
			distance = -distance
		}
		weight := 1.0
		if distance > 0 {
			weight = math.Pow(params.EdgeNeighborWeight, float64(distance))
		}
		idx := y*boundaryCount + edgeX - 1
		strength = math.Max(strength, edges[idx]*weight)
	}
	return strength
}

func verticalEdgeThreshold(edges []float64, params verticalRectangleParams) float64 {
	if len(edges) == 0 {
		return 0
	}
	sum := 0.0
	maxStrength := 0.0
	for _, edge := range edges {
		sum += edge
		if edge > maxStrength {
			maxStrength = edge
		}
	}
	if maxStrength < params.MinMaxEdgeStrength {
		return 0
	}
	mean := sum / float64(len(edges))
	return maxFloat(params.EdgeThresholdFloor, maxFloat(maxStrength*params.EdgeThresholdMaxRatio, mean*params.EdgeThresholdMeanRatio))
}

func verticalRegionBoundaryCandidates(red, green, blue, luma []float64, width, height int, params verticalRectangleParams) ([]verticalEdgeCandidate, float64) {
	stripWidth := params.RegionBoundaryStripWidth
	if stripWidth <= 0 {
		stripWidth = maxInt(2, minInt(8, width/24))
	}
	if stripWidth < 1 || width < stripWidth*2+1 {
		return nil, 0
	}

	bandCount := params.RegionBoundaryBandCount
	if bandCount <= 0 || bandCount > height {
		bandCount = height
	}
	if bandCount < 1 {
		return nil, 0
	}

	strengths := make([]float64, (width-1)*bandCount)
	for x := stripWidth; x <= width-stripWidth; x++ {
		for band := 0; band < bandCount; band++ {
			y0 := band * height / bandCount
			y1 := (band + 1) * height / bandCount
			strengths[(x-1)*bandCount+band] = verticalRegionBoundaryStrength(red, green, blue, luma, width, x, stripWidth, y0, y1, params)
		}
	}

	threshold := verticalRegionBoundaryThreshold(strengths, params)
	if threshold <= 0 {
		return nil, threshold
	}

	candidates := make([]verticalEdgeCandidate, 0, width-1)
	for x := stripWidth; x <= width-stripWidth; x++ {
		strongBands := 0
		currentRun := 0
		longestRun := 0
		totalStrength := 0.0
		for band := 0; band < bandCount; band++ {
			strength := strengths[(x-1)*bandCount+band]
			totalStrength += strength
			if strength >= threshold {
				strongBands++
				currentRun++
				if currentRun > longestRun {
					longestRun = currentRun
				}
			} else {
				currentRun = 0
			}
		}
		coverage := float64(strongBands) / float64(bandCount)
		run := float64(longestRun) / float64(bandCount)
		meanStrength := totalStrength / float64(bandCount)
		if coverage < params.RegionBoundaryMinCoverage || run < params.RegionBoundaryMinRun || meanStrength < threshold*params.MinMeanThresholdRatio {
			continue
		}
		candidates = append(candidates, verticalEdgeCandidate{
			x:        x,
			score:    meanStrength * (params.EdgeScoreCoverageBase + coverage) * (params.EdgeScoreRunBase + run),
			coverage: coverage,
			run:      run,
		})
	}
	return candidates, threshold
}

func verticalRegionBoundaryStrength(red, green, blue, luma []float64, width, x, stripWidth, y0, y1 int, params verticalRectangleParams) float64 {
	if y1 <= y0 {
		return 0
	}

	var leftRed, leftGreen, leftBlue, leftLuma float64
	var rightRed, rightGreen, rightBlue, rightLuma float64
	for y := y0; y < y1; y++ {
		row := y * width
		for sx := x - stripWidth; sx < x; sx++ {
			idx := row + sx
			leftRed += red[idx]
			leftGreen += green[idx]
			leftBlue += blue[idx]
			leftLuma += luma[idx]
		}
		for sx := x; sx < x+stripWidth; sx++ {
			idx := row + sx
			rightRed += red[idx]
			rightGreen += green[idx]
			rightBlue += blue[idx]
			rightLuma += luma[idx]
		}
	}

	count := float64((y1 - y0) * stripWidth)
	if count <= 0 {
		return 0
	}
	leftRed /= count
	leftGreen /= count
	leftBlue /= count
	leftLuma /= count
	rightRed /= count
	rightGreen /= count
	rightBlue /= count
	rightLuma /= count

	dr := rightRed - leftRed
	dg := rightGreen - leftGreen
	db := rightBlue - leftBlue
	colorContrast := math.Sqrt(dr*dr+dg*dg+db*db) / math.Sqrt(3)
	lumaContrast := math.Abs(rightLuma - leftLuma)
	return math.Max(lumaContrast, colorContrast*params.ColorContrastWeight)
}

func verticalRegionBoundaryThreshold(strengths []float64, params verticalRectangleParams) float64 {
	if len(strengths) == 0 {
		return 0
	}
	sum := 0.0
	maxStrength := 0.0
	for _, strength := range strengths {
		sum += strength
		if strength > maxStrength {
			maxStrength = strength
		}
	}
	if maxStrength < params.RegionBoundaryMinMaxStrength {
		return 0
	}
	mean := sum / float64(len(strengths))
	return maxFloat(params.RegionBoundaryThresholdFloor, maxFloat(maxStrength*params.RegionBoundaryThresholdMaxRatio, mean*params.RegionBoundaryThresholdMeanRatio))
}

func panelRegionRectangleCandidates(red, green, blue, luma []float64, width, height int, params verticalRectangleParams) ([]verticalRectangleCandidate, verticalRectangleCandidate, bool) {
	if width < params.MinSampleDimension || height < params.MinSampleDimension {
		return nil, verticalRectangleCandidate{}, false
	}
	step := maxInt(1, params.PanelRegionStep)
	widthStep := maxInt(1, params.PanelRegionWidthStep)
	gutterWidth := params.PanelRegionGutterWidth
	if gutterWidth <= 0 {
		gutterWidth = maxInt(2, width/18)
	}
	bandCount := params.PanelRegionBandCount
	if bandCount <= 0 || bandCount > height {
		bandCount = height
	}
	if bandCount < 1 {
		return nil, verticalRectangleCandidate{}, false
	}

	minPanelWidth := maxInt(1, int(math.Ceil(params.AspectMin*float64(height))))
	maxPanelWidth := minInt(width-1, int(math.Floor(params.AspectMax*float64(height))))
	if maxPanelWidth < minPanelWidth {
		return nil, verticalRectangleCandidate{}, false
	}

	candidates := make([]verticalRectangleCandidate, 0)
	var bestCandidate verticalRectangleCandidate
	hasBestCandidate := false
	for panelWidth := minPanelWidth; panelWidth <= maxPanelWidth; panelWidth += widthStep {
		leftLimit := width - panelWidth
		for left := 0; left <= leftLimit; left += step {
			right := left + panelWidth
			candidate, ok, evaluated := scorePanelRegionCandidate(red, green, blue, luma, width, height, left, right, gutterWidth, bandCount, params)
			if evaluated && (!hasBestCandidate || candidate.score > bestCandidate.score) {
				bestCandidate = candidate
				hasBestCandidate = true
			}
			if ok {
				candidates = append(candidates, candidate)
			}
		}
		if leftLimit%step != 0 {
			right := width
			left := right - panelWidth
			candidate, ok, evaluated := scorePanelRegionCandidate(red, green, blue, luma, width, height, left, right, gutterWidth, bandCount, params)
			if evaluated && (!hasBestCandidate || candidate.score > bestCandidate.score) {
				bestCandidate = candidate
				hasBestCandidate = true
			}
			if ok {
				candidates = append(candidates, candidate)
			}
		}
	}
	return candidates, bestCandidate, hasBestCandidate
}

func scorePanelRegionCandidate(red, green, blue, luma []float64, width, height, left, right, gutterWidth, bandCount int, params verticalRectangleParams) (verticalRectangleCandidate, bool, bool) {
	candidate := verticalRectangleCandidate{
		left:   left,
		right:  right,
		source: verticalRectangleCandidateSourcePanelRegion,
	}
	if right <= left || left < 0 || right > width {
		return candidate, false, false
	}
	aspect := float64(right-left) / float64(height)
	aspectScore := verticalRectangleAspectScore(aspect, params)
	if aspectScore <= 0 {
		return candidate, false, false
	}
	candidate.aspect = aspect
	candidate.widthFraction = float64(right-left) / float64(width)

	hasLeftGutter := left >= gutterWidth
	hasRightGutter := width-right >= gutterWidth
	if !hasLeftGutter && !hasRightGutter {
		return candidate, false, false
	}

	oneSided := !hasLeftGutter || !hasRightGutter
	candidate.oneSided = oneSided
	strongBands := 0
	totalContrast := 0.0
	totalBalance := 0.0
	totalBoundarySupport := 0.0
	for band := 0; band < bandCount; band++ {
		y0 := band * height / bandCount
		y1 := (band + 1) * height / bandCount
		inside := panelRegionMean(red, green, blue, luma, width, left, right, y0, y1)
		var contrast float64
		var boundarySupport float64
		balance := 1.0
		switch {
		case hasLeftGutter && hasRightGutter:
			leftGutter := panelRegionMean(red, green, blue, luma, width, left-gutterWidth, left, y0, y1)
			rightGutter := panelRegionMean(red, green, blue, luma, width, right, right+gutterWidth, y0, y1)
			leftInsideEdge := panelRegionMean(red, green, blue, luma, width, left, minInt(right, left+gutterWidth), y0, y1)
			rightInsideEdge := panelRegionMean(red, green, blue, luma, width, maxInt(left, right-gutterWidth), right, y0, y1)
			leftContrast := panelRegionContrast(inside, leftGutter, params)
			rightContrast := panelRegionContrast(inside, rightGutter, params)
			contrast = (leftContrast + rightContrast) / 2
			boundarySupport = (panelRegionContrast(leftInsideEdge, leftGutter, params) + panelRegionContrast(rightInsideEdge, rightGutter, params)) / 2
			if leftContrast > 0 || rightContrast > 0 {
				balance = minFloat(leftContrast, rightContrast) / maxFloat(leftContrast, rightContrast)
			}
			outsideContrast := panelRegionContrast(leftGutter, rightGutter, params)
			if params.PanelRegionOutsideAgreementRatio > 0 && contrast > 0 && outsideContrast > contrast*params.PanelRegionOutsideAgreementRatio {
				outsideAgreement := (contrast * params.PanelRegionOutsideAgreementRatio) / outsideContrast
				contrast *= outsideAgreement
				balance *= outsideAgreement
			}
		case hasLeftGutter:
			leftGutter := panelRegionMean(red, green, blue, luma, width, left-gutterWidth, left, y0, y1)
			leftInsideEdge := panelRegionMean(red, green, blue, luma, width, left, minInt(right, left+gutterWidth), y0, y1)
			contrast = panelRegionContrast(inside, leftGutter, params) * params.PanelRegionSideCandidateWeight
			boundarySupport = panelRegionContrast(leftInsideEdge, leftGutter, params) * params.PanelRegionSideCandidateWeight
		case hasRightGutter:
			rightGutter := panelRegionMean(red, green, blue, luma, width, right, right+gutterWidth, y0, y1)
			rightInsideEdge := panelRegionMean(red, green, blue, luma, width, maxInt(left, right-gutterWidth), right, y0, y1)
			contrast = panelRegionContrast(inside, rightGutter, params) * params.PanelRegionSideCandidateWeight
			boundarySupport = panelRegionContrast(rightInsideEdge, rightGutter, params) * params.PanelRegionSideCandidateWeight
		}
		if contrast >= params.PanelRegionMinBandContrast {
			strongBands++
		}
		totalContrast += contrast
		totalBalance += balance
		totalBoundarySupport += boundarySupport
	}

	coverage := float64(strongBands) / float64(bandCount)
	meanContrast := totalContrast / float64(bandCount)
	meanBalance := totalBalance / float64(bandCount)
	meanBoundarySupport := totalBoundarySupport / float64(bandCount)
	sideScore := 1.0
	if oneSided {
		sideScore = params.PanelRegionSideCandidateWeight
	}
	candidate.coverage = coverage
	candidate.meanContrast = meanContrast
	candidate.meanBoundarySupport = meanBoundarySupport
	candidate.meanBalance = meanBalance
	candidate.score = meanContrast * (params.EdgeScoreCoverageBase + coverage) * (0.65 + 0.35*meanBalance) * aspectScore * sideScore
	if coverage < params.PanelRegionMinCoverage || meanContrast < params.PanelRegionMinMeanContrast || meanBoundarySupport < params.PanelRegionMinBoundarySupport || candidate.score < params.PanelRegionMinScore {
		return candidate, false, true
	}
	return candidate, true, true
}

type panelRegionColor struct {
	red   float64
	green float64
	blue  float64
	luma  float64
}

func panelRegionMean(red, green, blue, luma []float64, width, x0, x1, y0, y1 int) panelRegionColor {
	if x1 <= x0 || y1 <= y0 {
		return panelRegionColor{}
	}
	var result panelRegionColor
	for y := y0; y < y1; y++ {
		row := y * width
		for x := x0; x < x1; x++ {
			idx := row + x
			result.red += red[idx]
			result.green += green[idx]
			result.blue += blue[idx]
			result.luma += luma[idx]
		}
	}
	count := float64((x1 - x0) * (y1 - y0))
	result.red /= count
	result.green /= count
	result.blue /= count
	result.luma /= count
	return result
}

func panelRegionContrast(a, b panelRegionColor, params verticalRectangleParams) float64 {
	dr := a.red - b.red
	dg := a.green - b.green
	db := a.blue - b.blue
	colorContrast := math.Sqrt(dr*dr+dg*dg+db*db) / math.Sqrt(3)
	lumaContrast := math.Abs(a.luma - b.luma)
	return math.Max(lumaContrast, colorContrast*params.ColorContrastWeight)
}

func hasRepeatingVerticalEdgePattern(candidates []verticalEdgeCandidate, params verticalRectangleParams) bool {
	if len(candidates) < params.RepeatingEdgeMinCount {
		return false
	}
	byScore := append([]verticalEdgeCandidate(nil), candidates...)
	sort.Slice(byScore, func(i, j int) bool {
		return byScore[i].score > byScore[j].score
	})
	return byScore[params.RepeatingEdgeMinCount-1].score >= byScore[0].score*params.RepeatingEdgeScoreRatio
}

func verticalRectangleCandidates(edges []verticalEdgeCandidate, red, green, blue, luma []float64, width, height int, params verticalRectangleParams, source verticalRectangleCandidateSource) []verticalRectangleCandidate {
	rectangles := make([]verticalRectangleCandidate, 0, len(edges)*len(edges))
	for _, edge := range edges {
		rectangles = appendRectangleCandidate(rectangles, 0, edge.x, edge.score*params.SideRectangleEdgeWeight, red, green, blue, luma, width, height, params, source)
		rectangles = appendRectangleCandidate(rectangles, edge.x, width, edge.score*params.SideRectangleEdgeWeight, red, green, blue, luma, width, height, params, source)
	}
	for i := 0; i < len(edges); i++ {
		for j := i + 1; j < len(edges); j++ {
			left := edges[i]
			right := edges[j]
			score := left.score + right.score
			rectangles = appendRectangleCandidate(rectangles, left.x, right.x, score, red, green, blue, luma, width, height, params, source)
		}
	}
	return rectangles
}

func appendRectangleCandidate(rectangles []verticalRectangleCandidate, left, right int, edgeScore float64, red, green, blue, luma []float64, width, height int, params verticalRectangleParams, source verticalRectangleCandidateSource) []verticalRectangleCandidate {
	if right <= left {
		return rectangles
	}
	aspect := float64(right-left) / float64(height)
	aspectScore := verticalRectangleAspectScore(aspect, params)
	if aspectScore <= 0 {
		return rectangles
	}
	contentScore := rectangleContentScore(red, green, blue, luma, width, height, left, right)
	return append(rectangles, verticalRectangleCandidate{
		left:         left,
		right:        right,
		score:        edgeScore * aspectScore * (1 + params.RectangleContentScoreWeight*contentScore),
		source:       source,
		contentScore: contentScore,
	})
}

func rectangleContentScore(red, green, blue, luma []float64, width, height, left, right int) float64 {
	if width <= 0 || height <= 0 || right <= left || left < 0 || right > width || len(luma) != width*height || len(red) != len(luma) || len(green) != len(luma) || len(blue) != len(luma) {
		return 0
	}

	var lumaSum, saturationSum float64
	count := float64((right - left) * height)
	for y := 0; y < height; y++ {
		row := y * width
		for x := left; x < right; x++ {
			idx := row + x
			lumaSum += luma[idx]
			maxChannel := math.Max(red[idx], math.Max(green[idx], blue[idx]))
			minChannel := math.Min(red[idx], math.Min(green[idx], blue[idx]))
			if maxChannel > 0 {
				saturationSum += (maxChannel - minChannel) / maxChannel
			}
		}
	}
	meanLuma := lumaSum / count
	meanSaturation := saturationSum / count

	var lumaVariance float64
	activeGradientPixels := 0
	gradientPixels := 0
	for y := 0; y < height; y++ {
		row := y * width
		for x := left; x < right; x++ {
			idx := row + x
			delta := luma[idx] - meanLuma
			lumaVariance += delta * delta
			if x <= 0 || x >= width-1 || y <= 0 || y >= height-1 {
				continue
			}
			gradientPixels++
			gradient := math.Hypot(luma[row+x+1]-luma[row+x-1], luma[(y+1)*width+x]-luma[(y-1)*width+x])
			if gradient > 0.05 {
				activeGradientPixels++
			}
		}
	}
	lumaStd := math.Sqrt(lumaVariance / count)
	activeGradientFraction := 0.0
	if gradientPixels > 0 {
		activeGradientFraction = float64(activeGradientPixels) / float64(gradientPixels)
	}

	lumaScore := clampFloat((lumaStd-0.06)/0.22, 0, 1)
	saturationScore := clampFloat((meanSaturation-0.03)/0.25, 0, 1)
	gradientScore := clampFloat((activeGradientFraction-0.06)/0.28, 0, 1)
	return 0.40*lumaScore + 0.25*saturationScore + 0.35*gradientScore
}

func verticalRectangleAspectScore(aspect float64, params verticalRectangleParams) float64 {
	if aspect < params.AspectMin || aspect > params.AspectMax || params.AspectDistanceScale <= 0 {
		return 0
	}
	bestDistance := math.Inf(1)
	for _, ratio := range params.AspectTargets {
		if ratio <= 0 {
			continue
		}
		distance := math.Abs(math.Log(aspect / ratio))
		if distance < bestDistance {
			bestDistance = distance
		}
	}
	if math.IsInf(bestDistance, 1) {
		return 0
	}
	return params.AspectScoreFloor + params.AspectScoreRange*(1-math.Min(1, bestDistance/params.AspectDistanceScale))
}

func coarseBlobSaliency(luma, saturation, skinTone []float64, width, height int) []float64 {
	minDimension := minInt(width, height)
	localRadius := maxInt(2, minDimension/18)
	surroundRadius := maxInt(localRadius+2, minDimension/5)
	localLuma := boxBlur(luma, width, height, localRadius)
	surroundLuma := boxBlur(luma, width, height, surroundRadius)
	localSaturation := boxBlur(saturation, width, height, localRadius)
	surroundSaturation := boxBlur(saturation, width, height, surroundRadius)
	localSkinTone := boxBlur(skinTone, width, height, localRadius)

	coarse := make([]float64, width*height)
	for i := range coarse {
		blobContrast := math.Abs(localLuma[i]-surroundLuma[i]) + 0.35*math.Abs(localSaturation[i]-surroundSaturation[i])
		if blobContrast > 0 {
			coarse[i] = blobContrast * (1 + 1.8*localSkinTone[i])
		}
	}
	return coarse
}

func boxBlur(values []float64, width, height, radius int) []float64 {
	integral := saliencyIntegral(values, width, height)
	blurred := make([]float64, width*height)
	for y := 0; y < height; y++ {
		y0 := maxInt(0, y-radius)
		y1 := minInt(height, y+radius+1)
		for x := 0; x < width; x++ {
			x0 := maxInt(0, x-radius)
			x1 := minInt(width, x+radius+1)
			area := float64((x1 - x0) * (y1 - y0))
			blurred[y*width+x] = saliencyWindowSum(integral, width, x0, y0, x1, y1) / area
		}
	}
	return blurred
}

func combineFineAndCoarseSaliency(fine, coarse []float64) []float64 {
	fineMax := maxSaliency(fine)
	coarseMax := maxSaliency(coarse)
	if fineMax <= 0 {
		return coarse
	}
	if coarseMax <= 0 {
		return fine
	}
	spread := saliencySpread(fine)
	fineWeight := 1 - 0.45*spread
	coarseWeight := 0.35 + 0.75*spread
	combined := make([]float64, len(fine))
	for i := range combined {
		combined[i] = fineWeight*(fine[i]/fineMax) + coarseWeight*(coarse[i]/coarseMax)
	}
	return combined
}

func saliencySpread(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total, squares float64
	for _, value := range values {
		total += value
		squares += value * value
	}
	if total <= 0 || squares <= 0 {
		return 0
	}
	effectiveFraction := total * total / (squares * float64(len(values)))
	return clampFloat((effectiveFraction-0.12)/0.34, 0, 1)
}

func maxSaliency(values []float64) float64 {
	maxValue := 0.0
	for _, value := range values {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

type imageToneProfile struct {
	grayscale float64
	sepia     float64
}

func imageToneProfileForSample(pixelCount, grayLikePixels, sepiaLikePixels int) imageToneProfile {
	if pixelCount <= 0 {
		return imageToneProfile{}
	}
	grayFraction := float64(grayLikePixels) / float64(pixelCount)
	sepiaFraction := float64(sepiaLikePixels) / float64(pixelCount)
	return imageToneProfile{
		grayscale: clampFloat((grayFraction-0.68)/0.24, 0, 1),
		sepia:     clampFloat((sepiaFraction-0.42)/0.32, 0, 1),
	}
}

func chromaticSkinToneScore(r, g, b, hue, maxChannel, minChannel float64) float64 {
	if maxChannel < 0.14 || maxChannel-minChannel < 0.035 {
		return 0
	}
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

func monochromeSkinToneScore(luma, saturation float64, profile imageToneProfile) float64 {
	if profile.grayscale <= 0 && profile.sepia <= 0 {
		return 0
	}
	lumaScore := 1 - math.Min(1, math.Abs(luma-0.56)/0.34)
	grayScore := profile.grayscale * lumaScore * (1 - clampFloat((saturation-0.12)/0.18, 0, 1))
	sepiaScore := profile.sepia * lumaScore * (1 - math.Min(1, math.Abs(saturation-0.24)/0.34))
	return maxFloat(grayScore, sepiaScore)
}

func sepiaToneScore(hue, saturation, luma float64) float64 {
	if saturation < 0.04 || saturation > 0.58 || luma < 0.08 || luma > 0.92 {
		return 0
	}
	hueDistance := math.Min(math.Abs(hue-34), math.Min(math.Abs(hue+360-34), math.Abs(hue-360-34)))
	hueScore := 1 - math.Min(1, hueDistance/34)
	saturationScore := 1 - math.Min(1, math.Abs(saturation-0.22)/0.36)
	return hueScore * saturationScore
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
	return focusSampleSizeWithMax(width, height, defaultVerticalRectangleParams().MaxSample)
}

func focusSampleSizeWithMax(width, height, maxSample int) (int, int) {
	maxSample = maxInt(1, maxSample)
	if width <= maxSample && height <= maxSample {
		return width, height
	}
	if width >= height {
		return maxSample, maxInt(1, int(math.Round(float64(height)*float64(maxSample)/float64(width))))
	}
	return maxInt(1, int(math.Round(float64(width)*float64(maxSample)/float64(height)))), maxSample
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

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
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
