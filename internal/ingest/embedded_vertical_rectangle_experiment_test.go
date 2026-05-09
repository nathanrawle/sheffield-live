//go:build experiment

package ingest

import (
	"encoding/csv"
	"fmt"
	"image"
	"image/color"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type verticalRectangleExperimentVariant struct {
	name                  string
	edgePoolRadius        int
	edgeThresholdMaxRatio float64
	minEdgeRun            float64
	minEdgeCoverage       float64
	params                verticalRectangleParams
}

type verticalRectangleExperimentImage struct {
	label    string
	filename string
}

type verticalRectangleExperimentOutcome struct {
	label                  string
	detected               bool
	candidateAspect        float64
	candidateWidthFraction float64
	candidateScore         float64
	hasCandidateGeometry   bool
	hasError               bool
}

type verticalRectangleExperimentSummary struct {
	variant            verticalRectangleExperimentVariant
	truePositives      int
	falsePositives     int
	trueNegatives      int
	falseNegatives     int
	positiveTotal      int
	negativeTotal      int
	candidateAspects   []float64
	candidateFractions []float64
	candidateScores    []float64
}

type verticalRectangleExperimentSample struct {
	red    []float64
	green  []float64
	blue   []float64
	luma   []float64
	width  int
	height int
}

func TestEmbeddedVerticalRectangleExperiment(t *testing.T) {
	repoRoot := findExperimentRepoRoot(t)
	inputs := append(
		experimentImages("positive", readExperimentFilenames(t, filepath.Join(repoRoot, ".notes", "embedded-portrait-images.md"))),
		experimentImages("negative", readExperimentFilenames(t, filepath.Join(repoRoot, ".notes", "negative-control-images.md")))...,
	)
	experimentID := time.Now().UTC().Format("20060102T150405Z")
	outputDir := filepath.Join(repoRoot, ".notes", "experiments", "embedded-vertical-rectangle-parsing")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create experiment output dir: %v", err)
	}

	rowPath := filepath.Join(outputDir, experimentID+"-embedded-vertical-rectangle-grid-rows.csv")
	summaryPath := filepath.Join(outputDir, experimentID+"-embedded-vertical-rectangle-grid-summary.csv")
	summaries := runVerticalRectangleGridExperiment(t, repoRoot, experimentID, rowPath, inputs)
	writeVerticalRectangleGridSummary(t, summaryPath, experimentID, summaries)

	t.Logf("wrote embedded vertical rectangle grid rows to %s", rowPath)
	t.Logf("wrote embedded vertical rectangle grid summary to %s", summaryPath)
}

func runVerticalRectangleGridExperiment(t *testing.T, repoRoot, experimentID, rowPath string, inputs []verticalRectangleExperimentImage) []verticalRectangleExperimentSummary {
	t.Helper()
	file, err := os.Create(rowPath)
	if err != nil {
		t.Fatalf("create experiment row csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(verticalRectangleGridRowHeader()); err != nil {
		t.Fatalf("write row csv header: %v", err)
	}

	var summaries []verticalRectangleExperimentSummary
	for _, variant := range verticalRectangleGridVariants() {
		summary := verticalRectangleExperimentSummary{variant: variant}
		for _, input := range inputs {
			row, outcome := runVerticalRectangleExperimentImage(repoRoot, experimentID, variant, input)
			if err := writer.Write(row); err != nil {
				t.Fatalf("write row csv row: %v", err)
			}
			summary.add(outcome)
		}
		summaries = append(summaries, summary)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush row csv: %v", err)
	}
	return summaries
}

func writeVerticalRectangleGridSummary(t *testing.T, summaryPath, experimentID string, summaries []verticalRectangleExperimentSummary) {
	t.Helper()
	file, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create experiment summary csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"experiment_id",
		"variant_name",
		"param_edge_pool_radius",
		"param_edge_threshold_max_ratio",
		"param_min_edge_run",
		"param_min_edge_coverage",
		"true_positives",
		"false_positives",
		"true_negatives",
		"false_negatives",
		"positive_total",
		"negative_total",
		"accuracy",
		"precision",
		"recall",
		"f1",
		"median_candidate_aspect",
		"median_candidate_width_fraction",
		"median_candidate_score",
	}); err != nil {
		t.Fatalf("write summary csv header: %v", err)
	}
	for _, summary := range summaries {
		if err := writer.Write(summary.row(experimentID)); err != nil {
			t.Fatalf("write summary csv row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush summary csv: %v", err)
	}
}

func verticalRectangleGridRowHeader() []string {
	return []string{
		"experiment_id",
		"variant_name",
		"label",
		"image_filename",
		"image_path",
		"image_width",
		"image_height",
		"sample_width",
		"sample_height",
		"detected",
		"is_correct",
		"focus_x",
		"focus_y",
		"candidate_left",
		"candidate_right",
		"candidate_center_x",
		"candidate_aspect",
		"candidate_width_fraction",
		"candidate_score",
		"edge_candidate_count",
		"rectangle_candidate_count",
		"repeating_pattern_rejected",
		"edge_threshold",
		"error",
		"param_edge_pool_radius",
		"param_edge_threshold_max_ratio",
		"param_min_edge_run",
		"param_min_edge_coverage",
	}
}

func verticalRectangleGridVariants() []verticalRectangleExperimentVariant {
	poolRadii := []int{0, 1, 2}
	thresholdRatios := []float64{0.18, 0.24, 0.30, 0.36}
	minRuns := []float64{0.40, 0.55, 0.68}
	minCoverages := []float64{0.50, 0.65, 0.76}

	var variants []verticalRectangleExperimentVariant
	for _, poolRadius := range poolRadii {
		for _, thresholdRatio := range thresholdRatios {
			for _, minRun := range minRuns {
				for _, minCoverage := range minCoverages {
					params := cloneVerticalRectangleParams(defaultVerticalRectangleParams())
					params.EdgePoolRadius = poolRadius
					params.EdgeThresholdMaxRatio = thresholdRatio
					params.MinEdgeRun = minRun
					params.MinEdgeCoverage = minCoverage
					variants = append(variants, verticalRectangleExperimentVariant{
						name:                  verticalRectangleGridVariantName(poolRadius, thresholdRatio, minRun, minCoverage),
						edgePoolRadius:        poolRadius,
						edgeThresholdMaxRatio: thresholdRatio,
						minEdgeRun:            minRun,
						minEdgeCoverage:       minCoverage,
						params:                params,
					})
				}
			}
		}
	}
	return variants
}

func verticalRectangleGridVariantName(poolRadius int, thresholdRatio, minRun, minCoverage float64) string {
	return fmt.Sprintf(
		"pool%d_threshold%s_run%s_coverage%s",
		poolRadius,
		experimentNameFloat(thresholdRatio),
		experimentNameFloat(minRun),
		experimentNameFloat(minCoverage),
	)
}

func experimentNameFloat(value float64) string {
	return strings.ReplaceAll(strconv.FormatFloat(value, 'f', 2, 64), ".", "p")
}

func runVerticalRectangleExperimentImage(repoRoot, experimentID string, variant verticalRectangleExperimentVariant, input verticalRectangleExperimentImage) ([]string, verticalRectangleExperimentOutcome) {
	imagePath := filepath.Join(repoRoot, "data", "media", "events", input.filename)
	row := []string{
		experimentID,
		variant.name,
		input.label,
		input.filename,
		imagePath,
		"",
		"",
		"",
		"",
		"false",
		"false",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"0",
		"0",
		"false",
		"",
		"",
		strconv.Itoa(variant.edgePoolRadius),
		formatExperimentFloat(variant.edgeThresholdMaxRatio),
		formatExperimentFloat(variant.minEdgeRun),
		formatExperimentFloat(variant.minEdgeCoverage),
	}
	outcome := verticalRectangleExperimentOutcome{label: input.label}

	file, err := os.Open(imagePath)
	if err != nil {
		row[23] = err.Error()
		outcome.hasError = true
		return row, outcome
	}
	defer file.Close()

	img, _, err := image.Decode(file)
	if err != nil {
		row[23] = err.Error()
		outcome.hasError = true
		return row, outcome
	}
	bounds := img.Bounds()
	row[5] = strconv.Itoa(bounds.Dx())
	row[6] = strconv.Itoa(bounds.Dy())

	sample := sampleVerticalRectangleExperimentImage(img, variant.params.MaxSample)
	row[7] = strconv.Itoa(sample.width)
	row[8] = strconv.Itoa(sample.height)

	detection := detectVerticalRectangle(sample.red, sample.green, sample.blue, sample.luma, sample.width, sample.height, variant.params)
	outcome.detected = detection.Detected
	row[9] = strconv.FormatBool(detection.Detected)
	row[10] = strconv.FormatBool(outcome.isCorrect())
	row[11] = strconv.Itoa(detection.Focus.X)
	row[12] = strconv.Itoa(detection.Focus.Y)
	if detection.Detected {
		candidateWidth := detection.Candidate.right - detection.Candidate.left
		outcome.candidateAspect = float64(candidateWidth) / float64(sample.height)
		outcome.candidateWidthFraction = float64(candidateWidth) / float64(sample.width)
		outcome.candidateScore = detection.Candidate.score
		outcome.hasCandidateGeometry = true
		row[13] = strconv.Itoa(detection.Candidate.left)
		row[14] = strconv.Itoa(detection.Candidate.right)
		row[15] = formatExperimentFloat((float64(detection.Candidate.left) + float64(detection.Candidate.right)) / 2)
		row[16] = formatExperimentFloat(outcome.candidateAspect)
		row[17] = formatExperimentFloat(outcome.candidateWidthFraction)
		row[18] = formatExperimentFloat(detection.Candidate.score)
	}
	row[19] = strconv.Itoa(detection.EdgeCandidateCount)
	row[20] = strconv.Itoa(detection.RectangleCandidateCount)
	row[21] = strconv.FormatBool(detection.RepeatingPatternRejected)
	row[22] = formatExperimentFloat(detection.EdgeThreshold)

	return row, outcome
}

func (outcome verticalRectangleExperimentOutcome) isCorrect() bool {
	if outcome.hasError {
		return false
	}
	switch outcome.label {
	case "positive":
		return outcome.detected
	case "negative":
		return !outcome.detected
	default:
		return false
	}
}

func (summary *verticalRectangleExperimentSummary) add(outcome verticalRectangleExperimentOutcome) {
	if outcome.label == "positive" {
		summary.positiveTotal++
		if outcome.detected && !outcome.hasError {
			summary.truePositives++
		} else {
			summary.falseNegatives++
		}
	} else {
		summary.negativeTotal++
		if outcome.detected && !outcome.hasError {
			summary.falsePositives++
		} else if !outcome.hasError {
			summary.trueNegatives++
		} else {
			summary.falsePositives++
		}
	}
	if outcome.hasCandidateGeometry {
		summary.candidateAspects = append(summary.candidateAspects, outcome.candidateAspect)
		summary.candidateFractions = append(summary.candidateFractions, outcome.candidateWidthFraction)
		summary.candidateScores = append(summary.candidateScores, outcome.candidateScore)
	}
}

func (summary verticalRectangleExperimentSummary) row(experimentID string) []string {
	precision := safeRatio(summary.truePositives, summary.truePositives+summary.falsePositives)
	recall := safeRatio(summary.truePositives, summary.positiveTotal)
	f1 := 0.0
	if precision+recall > 0 {
		f1 = 2 * precision * recall / (precision + recall)
	}
	return []string{
		experimentID,
		summary.variant.name,
		strconv.Itoa(summary.variant.edgePoolRadius),
		formatExperimentFloat(summary.variant.edgeThresholdMaxRatio),
		formatExperimentFloat(summary.variant.minEdgeRun),
		formatExperimentFloat(summary.variant.minEdgeCoverage),
		strconv.Itoa(summary.truePositives),
		strconv.Itoa(summary.falsePositives),
		strconv.Itoa(summary.trueNegatives),
		strconv.Itoa(summary.falseNegatives),
		strconv.Itoa(summary.positiveTotal),
		strconv.Itoa(summary.negativeTotal),
		formatExperimentFloat(safeRatio(summary.truePositives+summary.trueNegatives, summary.positiveTotal+summary.negativeTotal)),
		formatExperimentFloat(precision),
		formatExperimentFloat(recall),
		formatExperimentFloat(f1),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateAspects)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateFractions)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateScores)),
	}
}

func experimentImages(label string, filenames []string) []verticalRectangleExperimentImage {
	images := make([]verticalRectangleExperimentImage, 0, len(filenames))
	for _, filename := range filenames {
		images = append(images, verticalRectangleExperimentImage{label: label, filename: filename})
	}
	return images
}

func sampleVerticalRectangleExperimentImage(img image.Image, maxSample int) verticalRectangleExperimentSample {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	sampleWidth, sampleHeight := focusSampleSizeWithMax(width, height, maxSample)
	sample := verticalRectangleExperimentSample{
		red:    make([]float64, sampleWidth*sampleHeight),
		green:  make([]float64, sampleWidth*sampleHeight),
		blue:   make([]float64, sampleWidth*sampleHeight),
		luma:   make([]float64, sampleWidth*sampleHeight),
		width:  sampleWidth,
		height: sampleHeight,
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
		}
	}
	return sample
}

func readExperimentFilenames(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read experiment image list: %v", err)
	}
	var filenames []string
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-") {
			continue
		}
		line = strings.TrimPrefix(line, "-")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		filenames = append(filenames, line)
	}
	if len(filenames) == 0 {
		t.Fatalf("no filenames found in %s", path)
	}
	return filenames
}

func findExperimentRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root from %s", dir)
		}
		dir = parent
	}
}

func safeRatio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func medianExperimentValue(values []float64) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle], true
	}
	return (sorted[middle-1] + sorted[middle]) / 2, true
}

func formatOptionalExperimentFloat(value float64, ok bool) string {
	if !ok {
		return ""
	}
	return formatExperimentFloat(value)
}

func formatExperimentFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}
