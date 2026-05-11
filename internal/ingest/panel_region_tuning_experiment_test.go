//go:build experiment

package ingest

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type panelRegionTuningVariant struct {
	stage  string
	name   string
	params verticalRectangleParams
}

type panelRegionTuningOutcome struct {
	label                  string
	detected               bool
	source                 verticalRectangleCandidateSource
	candidateAspect        float64
	candidateWidthFraction float64
	candidateScore         float64
	panelRegionBest        verticalRectangleCandidate
	hasPanelRegionBest     bool
	hasCandidateGeometry   bool
}

type panelRegionTuningSummary struct {
	variant                      panelRegionTuningVariant
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
	panelRegionBestScores        []float64
	panelRegionBestAspects       []float64
	panelRegionBestFractions     []float64
	panelRegionBestCoverages     []float64
	panelRegionBestContrasts     []float64
	panelRegionBestBoundaries    []float64
	panelRegionBestBalances      []float64
}

func TestPanelRegionTuningExperiment(t *testing.T) {
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

	casePath := filepath.Join(outputDir, experimentID+"-panel-region-tuning-cases.csv")
	summaryPath := filepath.Join(outputDir, experimentID+"-panel-region-tuning-summary.csv")
	shortlistPath := filepath.Join(outputDir, experimentID+"-panel-region-tuning-shortlist.csv")
	summaries := runPanelRegionTuningExperiment(t, experimentID, casePath, inputs)
	writePanelRegionTuningSummary(t, summaryPath, experimentID, summaries)
	writePanelRegionTuningShortlist(t, shortlistPath, experimentID, summaries)

	t.Logf("wrote panel region tuning cases to %s", casePath)
	t.Logf("wrote panel region tuning summary to %s", summaryPath)
	t.Logf("wrote panel region tuning shortlist to %s", shortlistPath)
}

func runPanelRegionTuningExperiment(t *testing.T, experimentID, casePath string, inputs []regionBoundaryFallbackExperimentInput) []panelRegionTuningSummary {
	t.Helper()
	file, err := os.Create(casePath)
	if err != nil {
		t.Fatalf("create panel region tuning case csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(panelRegionTuningCaseHeader()); err != nil {
		t.Fatalf("write panel region tuning case header: %v", err)
	}

	var summaries []panelRegionTuningSummary
	for _, variant := range panelRegionTuningVariants() {
		summary := panelRegionTuningSummary{variant: variant}
		for _, input := range inputs {
			row, outcome := runPanelRegionTuningInput(experimentID, variant, input)
			if err := writer.Write(row); err != nil {
				t.Fatalf("write panel region tuning case row: %v", err)
			}
			summary.add(outcome)
		}
		summaries = append(summaries, summary)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush panel region tuning case csv: %v", err)
	}
	return summaries
}

func panelRegionTuningVariants() []panelRegionTuningVariant {
	strict := cloneVerticalRectangleParams(defaultVerticalRectangleParams())
	base := panelRegionTuningExploratoryParams()
	var variants []panelRegionTuningVariant
	seen := map[string]bool{}

	addParams := func(stage, name string, params verticalRectangleParams) {
		key := panelRegionTuningVariantKey(params)
		if seen[key] {
			return
		}
		seen[key] = true
		variants = append(variants, panelRegionTuningVariant{
			stage:  stage,
			name:   name,
			params: cloneVerticalRectangleParams(params),
		})
	}
	add := func(stage, name string, mutate func(*verticalRectangleParams)) {
		params := cloneVerticalRectangleParams(base)
		if mutate != nil {
			mutate(&params)
		}
		addParams(stage, name, params)
	}

	addParams("baseline", "strict_baseline", strict)
	addParams("baseline", "exploratory_base", base)

	for _, score := range []float64{0.08, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20, 0.24, 0.28, 0.32, 0.36, 0.42, 0.50, 0.60} {
		add("one_param", fmt.Sprintf("score%s", panelRegionTuningNameFloat(score)), func(params *verticalRectangleParams) {
			params.PanelRegionMinScore = score
		})
	}
	for _, coverage := range []float64{0.50, 0.62, 0.74, 0.82, 0.90, 0.96, 1.00} {
		add("one_param", fmt.Sprintf("coverage%s", panelRegionTuningNameFloat(coverage)), func(params *verticalRectangleParams) {
			params.PanelRegionMinCoverage = coverage
		})
	}
	for _, bandContrast := range []float64{0.020, 0.035, 0.045, 0.055, 0.070, 0.090, 0.120, 0.160, 0.220} {
		add("one_param", fmt.Sprintf("band%s", panelRegionTuningNameFloat(bandContrast)), func(params *verticalRectangleParams) {
			params.PanelRegionMinBandContrast = bandContrast
		})
	}
	for _, meanContrast := range []float64{0.030, 0.050, 0.070, 0.090, 0.120, 0.160, 0.220, 0.300} {
		add("one_param", fmt.Sprintf("mean%s", panelRegionTuningNameFloat(meanContrast)), func(params *verticalRectangleParams) {
			params.PanelRegionMinMeanContrast = meanContrast
		})
	}
	for _, boundarySupport := range []float64{0.020, 0.035, 0.055, 0.075, 0.100, 0.140, 0.200} {
		add("one_param", fmt.Sprintf("boundary%s", panelRegionTuningNameFloat(boundarySupport)), func(params *verticalRectangleParams) {
			params.PanelRegionMinBoundarySupport = boundarySupport
		})
	}
	for _, outsideAgreement := range []float64{0.20, 0.35, 0.50, 0.60, 0.75, 1.00} {
		add("one_param", fmt.Sprintf("outside%s", panelRegionTuningNameFloat(outsideAgreement)), func(params *verticalRectangleParams) {
			params.PanelRegionOutsideAgreementRatio = outsideAgreement
		})
	}
	for _, gutterWidth := range []int{0, 2, 4, 6, 8, 12, 16, 24, 32} {
		add("one_param", fmt.Sprintf("gutter%d", gutterWidth), func(params *verticalRectangleParams) {
			params.PanelRegionGutterWidth = gutterWidth
		})
	}
	for _, bandCount := range []int{4, 6, 8, 12, 16, 24, 32, 48} {
		add("one_param", fmt.Sprintf("bands%d", bandCount), func(params *verticalRectangleParams) {
			params.PanelRegionBandCount = bandCount
		})
	}
	for _, sideWeight := range []float64{0.25, 0.40, 0.55, 0.70, 0.82, 0.95, 1.10} {
		add("one_param", fmt.Sprintf("side%s", panelRegionTuningNameFloat(sideWeight)), func(params *verticalRectangleParams) {
			params.PanelRegionSideCandidateWeight = sideWeight
		})
	}
	for _, step := range []int{1, 2, 4, 6, 8, 12} {
		add("one_param", fmt.Sprintf("step%d", step), func(params *verticalRectangleParams) {
			params.PanelRegionStep = step
		})
	}
	for _, widthStep := range []int{1, 2, 4, 6, 8, 12} {
		add("one_param", fmt.Sprintf("widthstep%d", widthStep), func(params *verticalRectangleParams) {
			params.PanelRegionWidthStep = widthStep
		})
	}

	for _, score := range []float64{0.12, 0.16, 0.20, 0.24, 0.28, 0.32} {
		for _, coverage := range []float64{0.74, 0.82, 0.90, 0.96} {
			for _, meanContrast := range []float64{0.07, 0.09, 0.12, 0.16} {
				for _, boundarySupport := range []float64{0.035, 0.055, 0.075, 0.100, 0.140} {
					add("threshold_lattice", fmt.Sprintf("score%s_coverage%s_mean%s_boundary%s", panelRegionTuningNameFloat(score), panelRegionTuningNameFloat(coverage), panelRegionTuningNameFloat(meanContrast), panelRegionTuningNameFloat(boundarySupport)), func(params *verticalRectangleParams) {
						params.PanelRegionMinScore = score
						params.PanelRegionMinCoverage = coverage
						params.PanelRegionMinMeanContrast = meanContrast
						params.PanelRegionMinBoundarySupport = boundarySupport
					})
				}
			}
		}
	}

	for _, score := range []float64{0.16, 0.24, 0.32} {
		for _, boundarySupport := range []float64{0.035, 0.055, 0.100} {
			for _, outsideAgreement := range []float64{0.20, 0.35, 0.50, 0.60, 0.75, 1.00} {
				add("outside_lattice", fmt.Sprintf("score%s_boundary%s_outside%s", panelRegionTuningNameFloat(score), panelRegionTuningNameFloat(boundarySupport), panelRegionTuningNameFloat(outsideAgreement)), func(params *verticalRectangleParams) {
					params.PanelRegionMinScore = score
					params.PanelRegionMinBoundarySupport = boundarySupport
					params.PanelRegionOutsideAgreementRatio = outsideAgreement
				})
			}
		}
	}

	for _, step := range []int{1, 2, 4, 6, 8, 12} {
		for _, widthStep := range []int{1, 2, 4, 6, 8, 12} {
			add("step_scan", fmt.Sprintf("stepscan_step%d_widthstep%d", step, widthStep), func(params *verticalRectangleParams) {
				params.PanelRegionStep = step
				params.PanelRegionWidthStep = widthStep
			})
		}
	}

	return variants
}

func panelRegionTuningExploratoryParams() verticalRectangleParams {
	params := cloneVerticalRectangleParams(defaultVerticalRectangleParams())
	params.PanelRegionMinScore = 0.16
	params.PanelRegionMinCoverage = 0.74
	return params
}

func panelRegionTuningNameFloat(value float64) string {
	return strings.ReplaceAll(strconv.FormatFloat(value, 'f', 3, 64), ".", "p")
}

func panelRegionTuningVariantKey(params verticalRectangleParams) string {
	return strings.Join([]string{
		strconv.Itoa(params.PanelRegionBandCount),
		strconv.Itoa(params.PanelRegionGutterWidth),
		strconv.Itoa(params.PanelRegionStep),
		strconv.Itoa(params.PanelRegionWidthStep),
		formatExperimentFloat(params.PanelRegionMinBandContrast),
		formatExperimentFloat(params.PanelRegionMinCoverage),
		formatExperimentFloat(params.PanelRegionMinMeanContrast),
		formatExperimentFloat(params.PanelRegionMinBoundarySupport),
		formatExperimentFloat(params.PanelRegionMinScore),
		formatExperimentFloat(params.PanelRegionOutsideAgreementRatio),
		formatExperimentFloat(params.PanelRegionSideCandidateWeight),
	}, "|")
}

func runPanelRegionTuningInput(experimentID string, variant panelRegionTuningVariant, input regionBoundaryFallbackExperimentInput) ([]string, panelRegionTuningOutcome) {
	detection := detectVerticalRectangle(input.sample.red, input.sample.green, input.sample.blue, input.sample.luma, input.sample.width, input.sample.height, variant.params)
	outcome := panelRegionTuningOutcome{
		label:              input.label,
		detected:           detection.Detected,
		source:             detection.Candidate.source,
		panelRegionBest:    detection.PanelRegionBestCandidate,
		hasPanelRegionBest: detection.PanelRegionHasBestCandidate,
	}

	candidateFields := emptyPanelRegionTuningCandidateFields()
	if detection.Detected {
		candidateWidth := detection.Candidate.right - detection.Candidate.left
		outcome.candidateAspect = float64(candidateWidth) / float64(input.sample.height)
		outcome.candidateWidthFraction = float64(candidateWidth) / float64(input.sample.width)
		outcome.candidateScore = detection.Candidate.score
		outcome.hasCandidateGeometry = true
		candidateFields = panelRegionTuningSelectedCandidateFields(detection.Candidate, input.sample.width, input.sample.height)
	}
	panelRegionFields := panelRegionTuningPanelCandidateFields(detection.PanelRegionBestCandidate, detection.PanelRegionHasBestCandidate, detection.PanelRegionCandidateCount > 0)

	row := []string{
		experimentID,
		variant.stage,
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
	}
	row = append(row, candidateFields...)
	row = append(row,
		strconv.Itoa(detection.EdgeCandidateCount),
		strconv.Itoa(detection.RegionBoundaryCandidateCount),
		strconv.Itoa(detection.PanelRegionCandidateCount),
		strconv.Itoa(detection.RectangleCandidateCount),
		strconv.FormatBool(detection.RepeatingPatternRejected),
		formatExperimentFloat(detection.EdgeThreshold),
		formatExperimentFloat(detection.RegionBoundaryThreshold),
	)
	row = append(row, panelRegionFields...)
	row = append(row, panelRegionTuningParamRow(variant.params)...)
	return row, outcome
}

func panelRegionTuningCaseHeader() []string {
	header := []string{
		"experiment_id",
		"variant_stage",
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
		"candidate_source",
		"candidate_left",
		"candidate_right",
		"candidate_center_x",
		"candidate_aspect",
		"candidate_width_fraction",
		"candidate_score",
		"edge_candidate_count",
		"region_boundary_candidate_count",
		"panel_region_candidate_count",
		"rectangle_candidate_count",
		"repeating_pattern_rejected",
		"edge_threshold",
		"region_boundary_threshold",
		"panel_region_best_score",
		"panel_region_best_left",
		"panel_region_best_right",
		"panel_region_best_center_x",
		"panel_region_best_aspect",
		"panel_region_best_width_fraction",
		"panel_region_best_coverage",
		"panel_region_best_mean_contrast",
		"panel_region_best_mean_boundary_support",
		"panel_region_best_mean_balance",
		"panel_region_best_one_sided",
		"panel_region_best_passed_thresholds",
	}
	return append(header, panelRegionTuningParamHeader()...)
}

func emptyPanelRegionTuningCandidateFields() []string {
	return []string{"", "", "", "", "", "", ""}
}

func panelRegionTuningSelectedCandidateFields(candidate verticalRectangleCandidate, sampleWidth, sampleHeight int) []string {
	candidateWidth := candidate.right - candidate.left
	return []string{
		candidate.source.String(),
		strconv.Itoa(candidate.left),
		strconv.Itoa(candidate.right),
		formatExperimentFloat((float64(candidate.left) + float64(candidate.right)) / 2),
		formatExperimentFloat(float64(candidateWidth) / float64(sampleHeight)),
		formatExperimentFloat(float64(candidateWidth) / float64(sampleWidth)),
		formatExperimentFloat(candidate.score),
	}
}

func panelRegionTuningPanelCandidateFields(candidate verticalRectangleCandidate, ok, passedThresholds bool) []string {
	if !ok {
		return []string{"", "", "", "", "", "", "", "", "", "", "", ""}
	}
	return []string{
		formatExperimentFloat(candidate.score),
		strconv.Itoa(candidate.left),
		strconv.Itoa(candidate.right),
		formatExperimentFloat((float64(candidate.left) + float64(candidate.right)) / 2),
		formatExperimentFloat(candidate.aspect),
		formatExperimentFloat(candidate.widthFraction),
		formatExperimentFloat(candidate.coverage),
		formatExperimentFloat(candidate.meanContrast),
		formatExperimentFloat(candidate.meanBoundarySupport),
		formatExperimentFloat(candidate.meanBalance),
		strconv.FormatBool(candidate.oneSided),
		strconv.FormatBool(passedThresholds),
	}
}

func panelRegionTuningParamHeader() []string {
	return []string{
		"param_panel_region_band_count",
		"param_panel_region_gutter_width",
		"param_panel_region_step",
		"param_panel_region_width_step",
		"param_panel_region_min_band_contrast",
		"param_panel_region_min_coverage",
		"param_panel_region_min_mean_contrast",
		"param_panel_region_min_boundary_support",
		"param_panel_region_min_score",
		"param_panel_region_outside_agreement_ratio",
		"param_panel_region_side_candidate_weight",
	}
}

func panelRegionTuningParamRow(params verticalRectangleParams) []string {
	return []string{
		strconv.Itoa(params.PanelRegionBandCount),
		strconv.Itoa(params.PanelRegionGutterWidth),
		strconv.Itoa(params.PanelRegionStep),
		strconv.Itoa(params.PanelRegionWidthStep),
		formatExperimentFloat(params.PanelRegionMinBandContrast),
		formatExperimentFloat(params.PanelRegionMinCoverage),
		formatExperimentFloat(params.PanelRegionMinMeanContrast),
		formatExperimentFloat(params.PanelRegionMinBoundarySupport),
		formatExperimentFloat(params.PanelRegionMinScore),
		formatExperimentFloat(params.PanelRegionOutsideAgreementRatio),
		formatExperimentFloat(params.PanelRegionSideCandidateWeight),
	}
}

func (outcome panelRegionTuningOutcome) isCorrect() bool {
	switch outcome.label {
	case "positive":
		return outcome.detected
	case "negative":
		return !outcome.detected
	default:
		return false
	}
}

func (summary *panelRegionTuningSummary) add(outcome panelRegionTuningOutcome) {
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
	if outcome.hasPanelRegionBest {
		summary.panelRegionBestScores = append(summary.panelRegionBestScores, outcome.panelRegionBest.score)
		summary.panelRegionBestAspects = append(summary.panelRegionBestAspects, outcome.panelRegionBest.aspect)
		summary.panelRegionBestFractions = append(summary.panelRegionBestFractions, outcome.panelRegionBest.widthFraction)
		summary.panelRegionBestCoverages = append(summary.panelRegionBestCoverages, outcome.panelRegionBest.coverage)
		summary.panelRegionBestContrasts = append(summary.panelRegionBestContrasts, outcome.panelRegionBest.meanContrast)
		summary.panelRegionBestBoundaries = append(summary.panelRegionBestBoundaries, outcome.panelRegionBest.meanBoundarySupport)
		summary.panelRegionBestBalances = append(summary.panelRegionBestBalances, outcome.panelRegionBest.meanBalance)
	}
}

func writePanelRegionTuningSummary(t *testing.T, summaryPath, experimentID string, summaries []panelRegionTuningSummary) {
	t.Helper()
	file, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create panel region tuning summary csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(panelRegionTuningSummaryHeader()); err != nil {
		t.Fatalf("write panel region tuning summary header: %v", err)
	}
	strictTP := panelRegionTuningStrictTruePositives(summaries)
	for _, summary := range summaries {
		if err := writer.Write(summary.row(experimentID, strictTP)); err != nil {
			t.Fatalf("write panel region tuning summary row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush panel region tuning summary csv: %v", err)
	}
}

func panelRegionTuningSummaryHeader() []string {
	header := []string{
		"experiment_id",
		"variant_stage",
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
		"production_candidate",
		"edge_true_positives",
		"edge_false_positives",
		"region_boundary_true_positives",
		"region_boundary_false_positives",
		"panel_region_true_positives",
		"panel_region_false_positives",
		"median_candidate_aspect",
		"median_candidate_width_fraction",
		"median_candidate_score",
		"median_panel_region_best_score",
		"median_panel_region_best_aspect",
		"median_panel_region_best_width_fraction",
		"median_panel_region_best_coverage",
		"median_panel_region_best_mean_contrast",
		"median_panel_region_best_mean_boundary_support",
		"median_panel_region_best_mean_balance",
	}
	return append(header, panelRegionTuningParamHeader()...)
}

func (summary panelRegionTuningSummary) row(experimentID string, strictTP int) []string {
	precision := safeRatio(summary.truePositives, summary.truePositives+summary.falsePositives)
	recall := safeRatio(summary.truePositives, summary.positiveTotal)
	f1 := summary.f1()
	row := []string{
		experimentID,
		summary.variant.stage,
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
		strconv.FormatBool(summary.isProductionCandidate(strictTP)),
		strconv.Itoa(summary.edgeTruePositives),
		strconv.Itoa(summary.edgeFalsePositives),
		strconv.Itoa(summary.regionBoundaryTruePositives),
		strconv.Itoa(summary.regionBoundaryFalsePositives),
		strconv.Itoa(summary.panelRegionTruePositives),
		strconv.Itoa(summary.panelRegionFalsePositives),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateAspects)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateFractions)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.candidateScores)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestScores)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestAspects)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestFractions)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestCoverages)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestContrasts)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestBoundaries)),
		formatOptionalExperimentFloat(medianExperimentValue(summary.panelRegionBestBalances)),
	}
	return append(row, panelRegionTuningParamRow(summary.variant.params)...)
}

func (summary panelRegionTuningSummary) f1() float64 {
	precision := safeRatio(summary.truePositives, summary.truePositives+summary.falsePositives)
	recall := safeRatio(summary.truePositives, summary.positiveTotal)
	if precision+recall == 0 {
		return 0
	}
	return 2 * precision * recall / (precision + recall)
}

func (summary panelRegionTuningSummary) isProductionCandidate(strictTP int) bool {
	return summary.truePositives >= strictTP+2 && summary.falsePositives <= 5
}

func panelRegionTuningStrictTruePositives(summaries []panelRegionTuningSummary) int {
	for _, summary := range summaries {
		if summary.variant.name == "strict_baseline" {
			return summary.truePositives
		}
	}
	return 0
}

func writePanelRegionTuningShortlist(t *testing.T, shortlistPath, experimentID string, summaries []panelRegionTuningSummary) {
	t.Helper()
	file, err := os.Create(shortlistPath)
	if err != nil {
		t.Fatalf("create panel region tuning shortlist csv: %v", err)
	}
	defer file.Close()

	strictTP := panelRegionTuningStrictTruePositives(summaries)
	pareto := panelRegionTuningParetoFrontier(summaries)
	paretoKeys := make(map[string]bool, len(pareto))
	for _, summary := range pareto {
		paretoKeys[summary.variant.name] = true
	}

	shortlist := make([]panelRegionTuningSummary, 0, len(summaries))
	for _, summary := range summaries {
		if paretoKeys[summary.variant.name] || summary.isProductionCandidate(strictTP) {
			shortlist = append(shortlist, summary)
		}
	}
	sort.Slice(shortlist, func(i, j int) bool {
		leftProduction := shortlist[i].isProductionCandidate(strictTP)
		rightProduction := shortlist[j].isProductionCandidate(strictTP)
		if leftProduction != rightProduction {
			return leftProduction
		}
		if shortlist[i].falsePositives != shortlist[j].falsePositives {
			return shortlist[i].falsePositives < shortlist[j].falsePositives
		}
		if shortlist[i].truePositives != shortlist[j].truePositives {
			return shortlist[i].truePositives > shortlist[j].truePositives
		}
		return shortlist[i].f1() > shortlist[j].f1()
	})

	writer := csv.NewWriter(file)
	if err := writer.Write(append([]string{"shortlist_reason"}, panelRegionTuningSummaryHeader()...)); err != nil {
		t.Fatalf("write panel region tuning shortlist header: %v", err)
	}
	for _, summary := range shortlist {
		reasons := make([]string, 0, 2)
		if paretoKeys[summary.variant.name] {
			reasons = append(reasons, "pareto")
		}
		if summary.isProductionCandidate(strictTP) {
			reasons = append(reasons, "production_candidate")
		}
		row := append([]string{strings.Join(reasons, ";")}, summary.row(experimentID, strictTP)...)
		if err := writer.Write(row); err != nil {
			t.Fatalf("write panel region tuning shortlist row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush panel region tuning shortlist csv: %v", err)
	}
}

func panelRegionTuningParetoFrontier(summaries []panelRegionTuningSummary) []panelRegionTuningSummary {
	frontier := make([]panelRegionTuningSummary, 0, len(summaries))
	for _, candidate := range summaries {
		dominated := false
		for _, other := range summaries {
			if other.variant.name == candidate.variant.name {
				continue
			}
			if other.truePositives >= candidate.truePositives && other.falsePositives <= candidate.falsePositives && (other.truePositives > candidate.truePositives || other.falsePositives < candidate.falsePositives) {
				dominated = true
				break
			}
		}
		if !dominated {
			frontier = append(frontier, candidate)
		}
	}
	return frontier
}
