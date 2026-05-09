package ingest

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"math"
	"sort"
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
	red := make([]float64, sampleWidth*sampleHeight)
	green := make([]float64, sampleWidth*sampleHeight)
	blue := make([]float64, sampleWidth*sampleHeight)
	luma := make([]float64, sampleWidth*sampleHeight)
	saturation := make([]float64, sampleWidth*sampleHeight)
	chromaticSkinTone := make([]float64, sampleWidth*sampleHeight)
	skinTone := make([]float64, sampleWidth*sampleHeight)
	var grayLikePixels, sepiaLikePixels int
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
			maxChannel := math.Max(r, math.Max(g, b))
			minChannel := math.Min(r, math.Min(g, b))
			hue := rgbHueDegrees(r, g, b, maxChannel, minChannel)
			if maxChannel > 0 {
				saturation[idx] = (maxChannel - minChannel) / maxChannel
			}
			if saturation[idx] < 0.08 {
				grayLikePixels++
			}
			if sepiaToneScore(hue, saturation[idx], luma[idx]) > 0.4 {
				sepiaLikePixels++
			}
			chromaticSkinTone[idx] = chromaticSkinToneScore(r, g, b, hue, maxChannel, minChannel)
		}
	}
	if focus, ok := verticalRectangleFocus(red, green, blue, luma, sampleWidth, sampleHeight); ok {
		return focus, nil
	}
	toneProfile := imageToneProfileForSample(sampleWidth*sampleHeight, grayLikePixels, sepiaLikePixels)

	fineSaliency := make([]float64, sampleWidth*sampleHeight)
	for y := 0; y < sampleHeight; y++ {
		for x := 0; x < sampleWidth; x++ {
			idx := y*sampleWidth + x
			skinTone[idx] = maxFloat(chromaticSkinTone[idx], monochromeSkinToneScore(luma[idx], saturation[idx], toneProfile))
			left := luma[y*sampleWidth+maxInt(0, x-1)]
			right := luma[y*sampleWidth+minInt(sampleWidth-1, x+1)]
			up := luma[maxInt(0, y-1)*sampleWidth+x]
			down := luma[minInt(sampleHeight-1, y+1)*sampleWidth+x]
			contrast := math.Hypot(right-left, down-up)
			if contrast > 0 {
				base := contrast * (1 + 0.7*saturation[idx])
				fineSaliency[idx] = base * (1 + 2.0*skinTone[idx])
			}
		}
	}
	coarseSaliency := coarseBlobSaliency(luma, saturation, skinTone, sampleWidth, sampleHeight)
	saliency := combineFineAndCoarseSaliency(fineSaliency, coarseSaliency)

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

type verticalEdgeCandidate struct {
	x        int
	score    float64
	coverage float64
	run      float64
}

type verticalRectangleCandidate struct {
	left  int
	right int
	score float64
}

type verticalRectangleParams struct {
	MaxSample               int
	MinSampleDimension      int
	ColorContrastWeight     float64
	EdgeGroupDistance       int
	EdgePoolRadius          int
	MinEdgeCoverage         float64
	MinEdgeRun              float64
	MinMeanThresholdRatio   float64
	EdgeScoreCoverageBase   float64
	EdgeScoreRunBase        float64
	EdgeNeighborWeight      float64
	MinMaxEdgeStrength      float64
	EdgeThresholdFloor      float64
	EdgeThresholdMaxRatio   float64
	EdgeThresholdMeanRatio  float64
	RepeatingEdgeMinCount   int
	RepeatingEdgeScoreRatio float64
	SideRectangleEdgeWeight float64
	AspectMin               float64
	AspectMax               float64
	AspectTargets           []float64
	AspectDistanceScale     float64
	AspectScoreFloor        float64
	AspectScoreRange        float64
}

type verticalRectangleDetection struct {
	Focus                    ImageFocus
	Detected                 bool
	Candidate                verticalRectangleCandidate
	EdgeCandidateCount       int
	RectangleCandidateCount  int
	RepeatingPatternRejected bool
	EdgeThreshold            float64
}

func defaultVerticalRectangleParams() verticalRectangleParams {
	return verticalRectangleParams{
		MaxSample:               128,
		MinSampleDimension:      24,
		ColorContrastWeight:     0.85,
		EdgeGroupDistance:       2,
		EdgePoolRadius:          1,
		MinEdgeCoverage:         0.76,
		MinEdgeRun:              0.68,
		MinMeanThresholdRatio:   0.9,
		EdgeScoreCoverageBase:   0.45,
		EdgeScoreRunBase:        0.45,
		EdgeNeighborWeight:      0.85,
		MinMaxEdgeStrength:      0.14,
		EdgeThresholdFloor:      0.11,
		EdgeThresholdMaxRatio:   0.36,
		EdgeThresholdMeanRatio:  3.0,
		RepeatingEdgeMinCount:   5,
		RepeatingEdgeScoreRatio: 0.62,
		SideRectangleEdgeWeight: 0.92,
		AspectMin:               0.45,
		AspectMax:               1.90,
		AspectTargets:           []float64{0.67, 0.71, 0.77, 1.50, 1.78},
		AspectDistanceScale:     0.45,
		AspectScoreFloor:        0.55,
		AspectScoreRange:        0.45,
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
	if len(edgeCandidates) == 0 {
		return result
	}
	if hasRepeatingVerticalEdgePattern(edgeCandidates, params) {
		result.RepeatingPatternRejected = true
		return result
	}

	rectangles := verticalRectangleCandidates(edgeCandidates, width, height, params)
	result.RectangleCandidateCount = len(rectangles)
	if len(rectangles) == 0 {
		return result
	}
	sort.Slice(rectangles, func(i, j int) bool {
		return rectangles[i].score > rectangles[j].score
	})

	best := rectangles[0]
	centerX := (float64(best.left) + float64(best.right)) / 2 / float64(width)
	result.Focus = NormalizeImageFocus(int(math.Round(centerX*100)), DefaultImageFocusY)
	result.Candidate = best
	result.Detected = true
	return result
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

func verticalRectangleCandidates(edges []verticalEdgeCandidate, width, height int, params verticalRectangleParams) []verticalRectangleCandidate {
	rectangles := make([]verticalRectangleCandidate, 0, len(edges)*len(edges))
	for _, edge := range edges {
		rectangles = appendRectangleCandidate(rectangles, 0, edge.x, edge.score*params.SideRectangleEdgeWeight, width, height, params)
		rectangles = appendRectangleCandidate(rectangles, edge.x, width, edge.score*params.SideRectangleEdgeWeight, width, height, params)
	}
	for i := 0; i < len(edges); i++ {
		for j := i + 1; j < len(edges); j++ {
			left := edges[i]
			right := edges[j]
			score := left.score + right.score
			rectangles = appendRectangleCandidate(rectangles, left.x, right.x, score, width, height, params)
		}
	}
	return rectangles
}

func appendRectangleCandidate(rectangles []verticalRectangleCandidate, left, right int, edgeScore float64, width, height int, params verticalRectangleParams) []verticalRectangleCandidate {
	if right <= left {
		return rectangles
	}
	aspect := float64(right-left) / float64(height)
	aspectScore := verticalRectangleAspectScore(aspect, params)
	if aspectScore <= 0 {
		return rectangles
	}
	return append(rectangles, verticalRectangleCandidate{
		left:  left,
		right: right,
		score: edgeScore * aspectScore,
	})
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
