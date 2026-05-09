//go:build experiment

package ingest

import (
	"encoding/csv"
	"fmt"
	"image"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type regionBoundaryFallbackExperimentVariant struct {
	name   string
	params verticalRectangleParams
}

type regionBoundaryFallbackExperimentInput struct {
	label       string
	filename    string
	path        string
	imageWidth  int
	imageHeight int
	sample      verticalRectangleExperimentSample
}

type regionBoundaryFallbackExperimentOutcome struct {
	label                        string
	detected                     bool
	source                       verticalRectangleCandidateSource
	candidateAspect              float64
	candidateWidthFraction       float64
	candidateScore               float64
	regionBoundaryCandidateCount int
	regionBoundaryThreshold      float64
	hasCandidateGeometry         bool
}

type regionBoundaryFallbackExperimentSummary struct {
	variant                      regionBoundaryFallbackExperimentVariant
	truePositives                int
	falsePositives               int
	trueNegatives                int
	falseNegatives               int
	positiveTotal                int
	negativeTotal                int
	edgeTruePositives            int
	edgeFalsePositives           int
	regionBoundaryTruePositives  int
	regionBoundaryFalsePositives int
	candidateAspects             []float64
	candidateFractions           []float64
	candidateScores              []float64
	regionBoundaryCounts         []float64
	regionBoundaryThresholds     []float64
}

func TestRegionBoundaryFallbackExperiment(t *testing.T) {
	repoRoot := findExperimentRepoRoot(t)
	inputs := loadRegionBoundaryFallbackExperimentInputs(t, repoRoot, append(
		experimentImages("positive", readExperimentFilenames(t, filepath.Join(repoRoot, ".notes", "embedded-portrait-images.md"))),
		experimentImages("negative", readExperimentFilenames(t, filepath.Join(repoRoot, ".notes", "negative-control-images.md")))...,
	))
	experimentID := time.Now().UTC().Format("20060102T150405Z")
	outputDir := filepath.Join(repoRoot, ".notes", "experiments", "embedded-vertical-rectangle-parsing")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create experiment output dir: %v", err)
	}

	rowPath := filepath.Join(outputDir, experimentID+"-region-boundary-fallback-grid-rows.csv")
	summaryPath := filepath.Join(outputDir, experimentID+"-region-boundary-fallback-grid-summary.csv")
	summaries := runRegionBoundaryFallbackExperiment(t, experimentID, rowPath, inputs)
	writeRegionBoundaryFallbackSummary(t, summaryPath, experimentID, summaries)

	t.Logf("wrote region boundary fallback grid rows to %s", rowPath)
	t.Logf("wrote region boundary fallback grid summary to %s", summaryPath)
}

func loadRegionBoundaryFallbackExperimentInputs(t *testing.T, repoRoot string, images []verticalRectangleExperimentImage) []regionBoundaryFallbackExperimentInput {
	t.Helper()
	inputs := make([]regionBoundaryFallbackExperimentInput, 0, len(images))
	maxSample := defaultVerticalRectangleParams().MaxSample
	for _, imageRef := range images {
		imagePath := filepath.Join(repoRoot, "data", "media", "events", imageRef.filename)
		file, err := os.Open(imagePath)
		if err != nil {
			t.Fatalf("open experiment image %s: %v", imageRef.filename, err)
		}
		img, _, err := image.Decode(file)
		if closeErr := file.Close(); closeErr != nil {
			t.Fatalf("close experiment image %s: %v", imageRef.filename, closeErr)
		}
		if err != nil {
			t.Fatalf("decode experiment image %s: %v", imageRef.filename, err)
		}
		bounds := img.Bounds()
		inputs = append(inputs, regionBoundaryFallbackExperimentInput{
			label:       imageRef.label,
			filename:    imageRef.filename,
			path:        imagePath,
			imageWidth:  bounds.Dx(),
			imageHeight: bounds.Dy(),
			sample:      sampleVerticalRectangleExperimentImage(img, maxSample),
		})
	}
	return inputs
}

func runRegionBoundaryFallbackExperiment(t *testing.T, experimentID, rowPath string, inputs []regionBoundaryFallbackExperimentInput) []regionBoundaryFallbackExperimentSummary {
	t.Helper()
	file, err := os.Create(rowPath)
	if err != nil {
		t.Fatalf("create experiment row csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(regionBoundaryFallbackRowHeader()); err != nil {
		t.Fatalf("write row csv header: %v", err)
	}

	var summaries []regionBoundaryFallbackExperimentSummary
	for _, variant := range regionBoundaryFallbackVariants() {
		summary := regionBoundaryFallbackExperimentSummary{variant: variant}
		for _, input := range inputs {
			row, outcome := runRegionBoundaryFallbackExperimentInput(experimentID, variant, input)
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

func regionBoundaryFallbackVariants() []regionBoundaryFallbackExperimentVariant {
	var variants []regionBoundaryFallbackExperimentVariant
	seen := map[string]bool{}
	base := cloneVerticalRectangleParams(defaultVerticalRectangleParams())

	add := func(name string, mutate func(*verticalRectangleParams)) {
		params := cloneVerticalRectangleParams(base)
		if mutate != nil {
			mutate(&params)
		}
		key := regionBoundaryFallbackVariantKey(params)
		if seen[key] {
			return
		}
		seen[key] = true
		variants = append(variants, regionBoundaryFallbackExperimentVariant{name: name, params: params})
	}

	add("baseline", nil)

	for _, coverage := range []float64{0.60, 0.66, 0.72, 0.78, 0.84} {
		for _, run := range []float64{0.32, 0.48, 0.64, 0.80} {
			add(fmt.Sprintf("coverage%s_run%s", experimentNameFloat(coverage), experimentNameFloat(run)), func(params *verticalRectangleParams) {
				params.RegionBoundaryMinCoverage = coverage
				params.RegionBoundaryMinRun = run
			})
		}
	}

	for _, maxRatio := range []float64{0.18, 0.22, 0.26, 0.32, 0.40} {
		for _, meanRatio := range []float64{1.20, 1.50, 1.85, 2.30, 2.80} {
			add(fmt.Sprintf("threshold_ratio%s_mean%s", experimentNameFloat(maxRatio), experimentNameFloat(meanRatio)), func(params *verticalRectangleParams) {
				params.RegionBoundaryThresholdMaxRatio = maxRatio
				params.RegionBoundaryThresholdMeanRatio = meanRatio
			})
		}
	}

	for _, floor := range []float64{0.035, 0.045, 0.055, 0.070, 0.090} {
		add(fmt.Sprintf("floor%s", experimentNameFloat(floor)), func(params *verticalRectangleParams) {
			params.RegionBoundaryThresholdFloor = floor
		})
	}

	for _, minMaxStrength := range []float64{0.08, 0.10, 0.12, 0.16, 0.20} {
		add(fmt.Sprintf("minmax%s", experimentNameFloat(minMaxStrength)), func(params *verticalRectangleParams) {
			params.RegionBoundaryMinMaxStrength = minMaxStrength
		})
	}

	for _, stripWidth := range []int{3, 5, 7, 9, 13} {
		for _, bandCount := range []int{12, 18, 24, 36} {
			add(fmt.Sprintf("strip%d_bands%d", stripWidth, bandCount), func(params *verticalRectangleParams) {
				params.RegionBoundaryStripWidth = stripWidth
				params.RegionBoundaryBandCount = bandCount
			})
		}
	}

	for _, coverage := range []float64{0.72, 0.78, 0.84} {
		for _, run := range []float64{0.32, 0.48, 0.64} {
			for _, maxRatio := range []float64{0.26, 0.32} {
				add(fmt.Sprintf("target_coverage%s_run%s_threshold%s", experimentNameFloat(coverage), experimentNameFloat(run), experimentNameFloat(maxRatio)), func(params *verticalRectangleParams) {
					params.RegionBoundaryMinCoverage = coverage
					params.RegionBoundaryMinRun = run
					params.RegionBoundaryThresholdMaxRatio = maxRatio
				})
			}
		}
	}

	return variants
}

func regionBoundaryFallbackVariantKey(params verticalRectangleParams) string {
	return strings.Join([]string{
		strconv.Itoa(params.RegionBoundaryStripWidth),
		strconv.Itoa(params.RegionBoundaryBandCount),
		formatExperimentFloat(params.RegionBoundaryMinCoverage),
		formatExperimentFloat(params.RegionBoundaryMinRun),
		formatExperimentFloat(params.RegionBoundaryMinMaxStrength),
		formatExperimentFloat(params.RegionBoundaryThresholdFloor),
		formatExperimentFloat(params.RegionBoundaryThresholdMaxRatio),
		formatExperimentFloat(params.RegionBoundaryThresholdMeanRatio),
	}, "|")
}

func runRegionBoundaryFallbackExperimentInput(experimentID string, variant regionBoundaryFallbackExperimentVariant, input regionBoundaryFallbackExperimentInput) ([]string, regionBoundaryFallbackExperimentOutcome) {
	detection := detectVerticalRectangle(input.sample.red, input.sample.green, input.sample.blue, input.sample.luma, input.sample.width, input.sample.height, variant.params)
	outcome := regionBoundaryFallbackExperimentOutcome{
		label:                        input.label,
		detected:                     detection.Detected,
		source:                       detection.Candidate.source,
		regionBoundaryCandidateCount: detection.RegionBoundaryCandidateCount,
		regionBoundaryThreshold:      detection.RegionBoundaryThreshold,
	}

	candidateLeft := ""
	candidateRight := ""
	candidateCenter := ""
	candidateAspect := ""
	candidateWidthFraction := ""
	candidateScore := ""
	candidateSource := ""
	if detection.Detected {
		candidateWidth := detection.Candidate.right - detection.Candidate.left
		outcome.candidateAspect = float64(candidateWidth) / float64(input.sample.height)
		outcome.candidateWidthFraction = float64(candidateWidth) / float64(input.sample.width)
		outcome.candidateScore = detection.Candidate.score
		outcome.hasCandidateGeometry = true

		candidateLeft = strconv.Itoa(detection.Candidate.left)
		candidateRight = strconv.Itoa(detection.Candidate.right)
		candidateCenter = formatExperimentFloat((float64(detection.Candidate.left) + float64(detection.Candidate.right)) / 2)
		candidateAspect = formatExperimentFloat(outcome.candidateAspect)
		candidateWidthFraction = formatExperimentFloat(outcome.candidateWidthFraction)
		candidateScore = formatExperimentFloat(detection.Candidate.score)
		candidateSource = detection.Candidate.source.String()
	}

	return []string{
		experimentID,
		variant.name,
		input.label,
		input.filename,
		input.path,
		strconv.Itoa(input.imageWidth),
		strconv.Itoa(input.imageHeight),
		strconv.Itoa(input.sample.width),
		strconv.Itoa(input.sample.height),
		strconv.FormatBool(detection.Detected),
		strconv.FormatBool(outcome.isCorrect()),
		strconv.Itoa(detection.Focus.X),
		strconv.Itoa(detection.Focus.Y),
		candidateLeft,
		candidateRight,
		candidateCenter,
		candidateAspect,
		candidateWidthFraction,
		candidateScore,
		candidateSource,
		strconv.Itoa(detection.EdgeCandidateCount),
		strconv.Itoa(detection.RegionBoundaryCandidateCount),
		strconv.Itoa(detection.RectangleCandidateCount),
		strconv.FormatBool(detection.RepeatingPatternRejected),
		formatExperimentFloat(detection.EdgeThreshold),
		formatExperimentFloat(detection.RegionBoundaryThreshold),
		"",
		strconv.Itoa(variant.params.RegionBoundaryStripWidth),
		strconv.Itoa(variant.params.RegionBoundaryBandCount),
		formatExperimentFloat(variant.params.RegionBoundaryMinCoverage),
		formatExperimentFloat(variant.params.RegionBoundaryMinRun),
		formatExperimentFloat(variant.params.RegionBoundaryMinMaxStrength),
		formatExperimentFloat(variant.params.RegionBoundaryThresholdFloor),
		formatExperimentFloat(variant.params.RegionBoundaryThresholdMaxRatio),
		formatExperimentFloat(variant.params.RegionBoundaryThresholdMeanRatio),
	}, outcome
}

func regionBoundaryFallbackRowHeader() []string {
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
		"candidate_source",
		"edge_candidate_count",
		"region_boundary_candidate_count",
		"rectangle_candidate_count",
		"repeating_pattern_rejected",
		"edge_threshold",
		"region_boundary_threshold",
		"error",
		"param_region_boundary_strip_width",
		"param_region_boundary_band_count",
		"param_region_boundary_min_coverage",
		"param_region_boundary_min_run",
		"param_region_boundary_min_max_strength",
		"param_region_boundary_threshold_floor",
		"param_region_boundary_threshold_max_ratio",
		"param_region_boundary_threshold_mean_ratio",
	}
}

func (outcome regionBoundaryFallbackExperimentOutcome) isCorrect() bool {
	switch outcome.label {
	case "positive":
		return outcome.detected
	case "negative":
		return !outcome.detected
	default:
		return false
	}
}

func (summary *regionBoundaryFallbackExperimentSummary) add(outcome regionBoundaryFallbackExperimentOutcome) {
	switch outcome.label {
	case "positive":
		summary.positiveTotal++
		if outcome.detected {
			summary.truePositives++
			if outcome.source == verticalRectangleCandidateSourceEdge {
				summary.edgeTruePositives++
			}
			if outcome.source == verticalRectangleCandidateSourceRegionBoundary {
				summary.regionBoundaryTruePositives++
			}
		} else {
			summary.falseNegatives++
		}
	case "negative":
		summary.negativeTotal++
		if outcome.detected {
			summary.falsePositives++
			if outcome.source == verticalRectangleCandidateSourceEdge {
				summary.edgeFalsePositives++
			}
			if outcome.source == verticalRectangleCandidateSourceRegionBoundary {
				summary.regionBoundaryFalsePositives++
			}
		} else {
			summary.trueNegatives++
		}
	}
	if outcome.hasCandidateGeometry {
		summary.candidateAspects = append(summary.candidateAspects, outcome.candidateAspect)
		summary.candidateFractions = append(summary.candidateFractions, outcome.candidateWidthFraction)
		summary.candidateScores = append(summary.candidateScores, outcome.candidateScore)
	}
	summary.regionBoundaryCounts = append(summary.regionBoundaryCounts, float64(outcome.regionBoundaryCandidateCount))
	if outcome.regionBoundaryThreshold > 0 {
		summary.regionBoundaryThresholds = append(summary.regionBoundaryThresholds, outcome.regionBoundaryThreshold)
	}
}

func writeRegionBoundaryFallbackSummary(t *testing.T, summaryPath, experimentID string, summaries []regionBoundaryFallbackExperimentSummary) {
	t.Helper()
	file, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create experiment summary csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(regionBoundaryFallbackSummaryHeader()); err != nil {
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

func regionBoundaryFallbackSummaryHeader() []string {
	return []string{
		"experiment_id",
		"variant_name",
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
		"edge_true_positives",
		"edge_false_positives",
		"region_boundary_true_positives",
		"region_boundary_false_positives",
		"median_candidate_aspect",
		"median_candidate_width_fraction",
		"median_candidate_score",
		"median_region_boundary_candidate_count",
		"median_region_boundary_threshold",
		"param_region_boundary_strip_width",
		"param_region_boundary_band_count",
		"param_region_boundary_min_coverage",
		"param_region_boundary_min_run",
		"param_region_boundary_min_max_strength",
		"param_region_boundary_threshold_floor",
		"param_region_boundary_threshold_max_ratio",
		"param_region_boundary_threshold_mean_ratio",
	}
}

func (summary regionBoundaryFallbackExperimentSummary) row(experimentID string) []string {
	precision := safeRatio(summary.truePositives, summary.truePositives+summary.falsePositives)
	recall := safeRatio(summary.truePositives, summary.positiveTotal)
	f1 := 0.0
	if precision+recall > 0 {
		f1 = 2 * precision * recall / (precision + recall)
	}
	return []string{
		experimentID,
		summary.variant.name,
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
		strconv.Itoa(summary.edgeTruePositives),
		strconv.Itoa(summary.edgeFalsePositives),
		strconv.Itoa(summary.regionBoundaryTruePositives),
		strconv.Itoa(summary.regionBoundaryFalsePositives),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateAspects)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateFractions)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateScores)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.regionBoundaryCounts)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.regionBoundaryThresholds)),
		strconv.Itoa(summary.variant.params.RegionBoundaryStripWidth),
		strconv.Itoa(summary.variant.params.RegionBoundaryBandCount),
		formatExperimentFloat(summary.variant.params.RegionBoundaryMinCoverage),
		formatExperimentFloat(summary.variant.params.RegionBoundaryMinRun),
		formatExperimentFloat(summary.variant.params.RegionBoundaryMinMaxStrength),
		formatExperimentFloat(summary.variant.params.RegionBoundaryThresholdFloor),
		formatExperimentFloat(summary.variant.params.RegionBoundaryThresholdMaxRatio),
		formatExperimentFloat(summary.variant.params.RegionBoundaryThresholdMeanRatio),
	}
}
