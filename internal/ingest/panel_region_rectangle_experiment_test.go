//go:build experiment

package ingest

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type panelRegionRectangleExperimentVariant struct {
	name   string
	params verticalRectangleParams
}

type panelRegionRectangleExperimentOutcome struct {
	label                     string
	detected                  bool
	source                    verticalRectangleCandidateSource
	candidateAspect           float64
	candidateWidthFraction    float64
	candidateScore            float64
	panelRegionCandidateCount int
	panelRegionBestScore      float64
	hasCandidateGeometry      bool
}

type panelRegionRectangleExperimentSummary struct {
	variant                      panelRegionRectangleExperimentVariant
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
	panelRegionTruePositives     int
	panelRegionFalsePositives    int
	candidateAspects             []float64
	candidateFractions           []float64
	candidateScores              []float64
	panelRegionCounts            []float64
	panelRegionBestScores        []float64
}

func TestPanelRegionRectangleExperiment(t *testing.T) {
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

	rowPath := filepath.Join(outputDir, experimentID+"-panel-region-rectangle-grid-rows.csv")
	summaryPath := filepath.Join(outputDir, experimentID+"-panel-region-rectangle-grid-summary.csv")
	summaries := runPanelRegionRectangleExperiment(t, experimentID, rowPath, inputs)
	writePanelRegionRectangleSummary(t, summaryPath, experimentID, summaries)

	t.Logf("wrote panel region rectangle grid rows to %s", rowPath)
	t.Logf("wrote panel region rectangle grid summary to %s", summaryPath)
}

func runPanelRegionRectangleExperiment(t *testing.T, experimentID, rowPath string, inputs []regionBoundaryFallbackExperimentInput) []panelRegionRectangleExperimentSummary {
	t.Helper()
	file, err := os.Create(rowPath)
	if err != nil {
		t.Fatalf("create experiment row csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(panelRegionRectangleRowHeader()); err != nil {
		t.Fatalf("write row csv header: %v", err)
	}

	var summaries []panelRegionRectangleExperimentSummary
	for _, variant := range panelRegionRectangleVariants() {
		summary := panelRegionRectangleExperimentSummary{variant: variant}
		for _, input := range inputs {
			row, outcome := runPanelRegionRectangleExperimentInput(experimentID, variant, input)
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

func panelRegionRectangleVariants() []panelRegionRectangleExperimentVariant {
	var variants []panelRegionRectangleExperimentVariant
	seen := map[string]bool{}
	base := cloneVerticalRectangleParams(defaultVerticalRectangleParams())

	add := func(name string, mutate func(*verticalRectangleParams)) {
		params := cloneVerticalRectangleParams(base)
		if mutate != nil {
			mutate(&params)
		}
		key := panelRegionRectangleVariantKey(params)
		if seen[key] {
			return
		}
		seen[key] = true
		variants = append(variants, panelRegionRectangleExperimentVariant{name: name, params: params})
	}

	add("baseline", nil)

	for _, coverage := range []float64{0.50, 0.62, 0.74, 0.86} {
		for _, score := range []float64{0.05, 0.07, 0.09, 0.12, 0.16, 0.24, 0.32, 0.45, 0.60} {
			add(fmt.Sprintf("coverage%s_score%s", experimentNameFloat(coverage), experimentNameFloat(score)), func(params *verticalRectangleParams) {
				params.PanelRegionMinCoverage = coverage
				params.PanelRegionMinScore = score
			})
		}
	}

	for _, bandContrast := range []float64{0.035, 0.055, 0.075, 0.095} {
		for _, meanContrast := range []float64{0.050, 0.075, 0.100, 0.130} {
			add(fmt.Sprintf("band%s_mean%s", experimentNameFloat(bandContrast), experimentNameFloat(meanContrast)), func(params *verticalRectangleParams) {
				params.PanelRegionMinBandContrast = bandContrast
				params.PanelRegionMinMeanContrast = meanContrast
			})
		}
	}

	for _, gutterWidth := range []int{4, 6, 8, 12, 16} {
		for _, bandCount := range []int{8, 12, 18, 24} {
			add(fmt.Sprintf("gutter%d_bands%d", gutterWidth, bandCount), func(params *verticalRectangleParams) {
				params.PanelRegionGutterWidth = gutterWidth
				params.PanelRegionBandCount = bandCount
			})
		}
	}

	for _, step := range []int{2, 4, 6} {
		for _, widthStep := range []int{2, 4, 6} {
			add(fmt.Sprintf("step%d_widthstep%d", step, widthStep), func(params *verticalRectangleParams) {
				params.PanelRegionStep = step
				params.PanelRegionWidthStep = widthStep
			})
		}
	}

	for _, sideWeight := range []float64{0.60, 0.75, 0.82, 0.95} {
		add(fmt.Sprintf("side_weight%s", experimentNameFloat(sideWeight)), func(params *verticalRectangleParams) {
			params.PanelRegionSideCandidateWeight = sideWeight
		})
	}

	return variants
}

func panelRegionRectangleVariantKey(params verticalRectangleParams) string {
	return strings.Join([]string{
		strconv.Itoa(params.PanelRegionBandCount),
		strconv.Itoa(params.PanelRegionGutterWidth),
		strconv.Itoa(params.PanelRegionStep),
		strconv.Itoa(params.PanelRegionWidthStep),
		formatExperimentFloat(params.PanelRegionMinBandContrast),
		formatExperimentFloat(params.PanelRegionMinCoverage),
		formatExperimentFloat(params.PanelRegionMinMeanContrast),
		formatExperimentFloat(params.PanelRegionMinScore),
		formatExperimentFloat(params.PanelRegionSideCandidateWeight),
	}, "|")
}

func runPanelRegionRectangleExperimentInput(experimentID string, variant panelRegionRectangleExperimentVariant, input regionBoundaryFallbackExperimentInput) ([]string, panelRegionRectangleExperimentOutcome) {
	detection := detectVerticalRectangle(input.sample.red, input.sample.green, input.sample.blue, input.sample.luma, input.sample.width, input.sample.height, variant.params)
	outcome := panelRegionRectangleExperimentOutcome{
		label:                     input.label,
		detected:                  detection.Detected,
		source:                    detection.Candidate.source,
		panelRegionCandidateCount: detection.PanelRegionCandidateCount,
		panelRegionBestScore:      detection.PanelRegionBestScore,
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
		strconv.Itoa(detection.PanelRegionCandidateCount),
		strconv.Itoa(detection.RectangleCandidateCount),
		strconv.FormatBool(detection.RepeatingPatternRejected),
		formatExperimentFloat(detection.EdgeThreshold),
		formatExperimentFloat(detection.RegionBoundaryThreshold),
		formatExperimentFloat(detection.PanelRegionBestScore),
		"",
		strconv.Itoa(variant.params.PanelRegionBandCount),
		strconv.Itoa(variant.params.PanelRegionGutterWidth),
		strconv.Itoa(variant.params.PanelRegionStep),
		strconv.Itoa(variant.params.PanelRegionWidthStep),
		formatExperimentFloat(variant.params.PanelRegionMinBandContrast),
		formatExperimentFloat(variant.params.PanelRegionMinCoverage),
		formatExperimentFloat(variant.params.PanelRegionMinMeanContrast),
		formatExperimentFloat(variant.params.PanelRegionMinScore),
		formatExperimentFloat(variant.params.PanelRegionSideCandidateWeight),
	}, outcome
}

func panelRegionRectangleRowHeader() []string {
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
		"panel_region_candidate_count",
		"rectangle_candidate_count",
		"repeating_pattern_rejected",
		"edge_threshold",
		"region_boundary_threshold",
		"panel_region_best_score",
		"error",
		"param_panel_region_band_count",
		"param_panel_region_gutter_width",
		"param_panel_region_step",
		"param_panel_region_width_step",
		"param_panel_region_min_band_contrast",
		"param_panel_region_min_coverage",
		"param_panel_region_min_mean_contrast",
		"param_panel_region_min_score",
		"param_panel_region_side_candidate_weight",
	}
}

func (outcome panelRegionRectangleExperimentOutcome) isCorrect() bool {
	switch outcome.label {
	case "positive":
		return outcome.detected
	case "negative":
		return !outcome.detected
	default:
		return false
	}
}

func (summary *panelRegionRectangleExperimentSummary) add(outcome panelRegionRectangleExperimentOutcome) {
	switch outcome.label {
	case "positive":
		summary.positiveTotal++
		if outcome.detected {
			summary.truePositives++
			switch outcome.source {
			case verticalRectangleCandidateSourceEdge:
				summary.edgeTruePositives++
			case verticalRectangleCandidateSourceRegionBoundary:
				summary.regionBoundaryTruePositives++
			case verticalRectangleCandidateSourcePanelRegion:
				summary.panelRegionTruePositives++
			}
		} else {
			summary.falseNegatives++
		}
	case "negative":
		summary.negativeTotal++
		if outcome.detected {
			summary.falsePositives++
			switch outcome.source {
			case verticalRectangleCandidateSourceEdge:
				summary.edgeFalsePositives++
			case verticalRectangleCandidateSourceRegionBoundary:
				summary.regionBoundaryFalsePositives++
			case verticalRectangleCandidateSourcePanelRegion:
				summary.panelRegionFalsePositives++
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
	summary.panelRegionCounts = append(summary.panelRegionCounts, float64(outcome.panelRegionCandidateCount))
	if outcome.panelRegionBestScore > 0 {
		summary.panelRegionBestScores = append(summary.panelRegionBestScores, outcome.panelRegionBestScore)
	}
}

func writePanelRegionRectangleSummary(t *testing.T, summaryPath, experimentID string, summaries []panelRegionRectangleExperimentSummary) {
	t.Helper()
	file, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create experiment summary csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(panelRegionRectangleSummaryHeader()); err != nil {
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

func panelRegionRectangleSummaryHeader() []string {
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
		"panel_region_true_positives",
		"panel_region_false_positives",
		"median_candidate_aspect",
		"median_candidate_width_fraction",
		"median_candidate_score",
		"median_panel_region_candidate_count",
		"median_panel_region_best_score",
		"param_panel_region_band_count",
		"param_panel_region_gutter_width",
		"param_panel_region_step",
		"param_panel_region_width_step",
		"param_panel_region_min_band_contrast",
		"param_panel_region_min_coverage",
		"param_panel_region_min_mean_contrast",
		"param_panel_region_min_score",
		"param_panel_region_side_candidate_weight",
	}
}

func (summary panelRegionRectangleExperimentSummary) row(experimentID string) []string {
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
		strconv.Itoa(summary.panelRegionTruePositives),
		strconv.Itoa(summary.panelRegionFalsePositives),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateAspects)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateFractions)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateScores)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionCounts)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestScores)),
		strconv.Itoa(summary.variant.params.PanelRegionBandCount),
		strconv.Itoa(summary.variant.params.PanelRegionGutterWidth),
		strconv.Itoa(summary.variant.params.PanelRegionStep),
		strconv.Itoa(summary.variant.params.PanelRegionWidthStep),
		formatExperimentFloat(summary.variant.params.PanelRegionMinBandContrast),
		formatExperimentFloat(summary.variant.params.PanelRegionMinCoverage),
		formatExperimentFloat(summary.variant.params.PanelRegionMinMeanContrast),
		formatExperimentFloat(summary.variant.params.PanelRegionMinScore),
		formatExperimentFloat(summary.variant.params.PanelRegionSideCandidateWeight),
	}
}
