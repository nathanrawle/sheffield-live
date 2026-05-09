//go:build experiment

package ingest

import (
	"encoding/csv"
	"image"
	"image/color"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type verticalRectangleExperimentParam struct {
	name    string
	integer bool
	value   func(verticalRectangleParams) float64
	set     func(*verticalRectangleParams, float64)
}

type verticalRectangleExperimentVariant struct {
	name             string
	changedParameter string
	change           string
	params           verticalRectangleParams
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
	filenames := readEmbeddedPortraitFilenames(t, filepath.Join(repoRoot, ".notes", "embedded-portrait-images.md"))
	experimentID := time.Now().UTC().Format("20060102T150405Z")
	outputDir := filepath.Join(repoRoot, ".notes", "experiments", "embedded-vertical-rectangle-parsing")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create experiment output dir: %v", err)
	}
	outputPath := filepath.Join(outputDir, experimentID+"-embedded-vertical-rectangle-parsing.csv")

	file, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create experiment csv: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	header := append([]string{
		"experiment_id",
		"variant_name",
		"changed_parameter",
		"change",
		"image_filename",
		"image_path",
		"image_width",
		"image_height",
		"sample_width",
		"sample_height",
		"detected",
		"focus_x",
		"focus_y",
		"candidate_left",
		"candidate_right",
		"candidate_center_x",
		"candidate_aspect",
		"candidate_score",
		"edge_candidate_count",
		"rectangle_candidate_count",
		"repeating_pattern_rejected",
		"edge_threshold",
		"error",
	}, verticalRectangleExperimentParamHeaders()...)
	if err := writer.Write(header); err != nil {
		t.Fatalf("write csv header: %v", err)
	}

	for _, variant := range verticalRectangleExperimentVariants() {
		for _, filename := range filenames {
			row := runVerticalRectangleExperimentImage(repoRoot, experimentID, variant, filename)
			if err := writer.Write(row); err != nil {
				t.Fatalf("write csv row: %v", err)
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush csv: %v", err)
	}

	t.Logf("wrote embedded vertical rectangle experiment to %s", outputPath)
}

func verticalRectangleExperimentVariants() []verticalRectangleExperimentVariant {
	baseline := cloneVerticalRectangleParams(defaultVerticalRectangleParams())
	variants := []verticalRectangleExperimentVariant{{
		name:   "baseline",
		change: "baseline",
		params: baseline,
	}}
	for _, definition := range verticalRectangleExperimentParams() {
		for _, change := range []string{"increase_50", "decrease_50"} {
			params := cloneVerticalRectangleParams(baseline)
			value := definition.value(params)
			switch change {
			case "increase_50":
				value *= 1.5
			case "decrease_50":
				value *= 0.5
			}
			if definition.integer {
				value = math.Max(1, math.Round(value))
			}
			definition.set(&params, value)
			variants = append(variants, verticalRectangleExperimentVariant{
				name:             definition.name + "_" + change,
				changedParameter: definition.name,
				change:           change,
				params:           params,
			})
		}
	}
	return variants
}

func runVerticalRectangleExperimentImage(repoRoot, experimentID string, variant verticalRectangleExperimentVariant, filename string) []string {
	imagePath := filepath.Join(repoRoot, "data", "media", "events", filename)
	row := []string{
		experimentID,
		variant.name,
		variant.changedParameter,
		variant.change,
		filename,
		imagePath,
		"",
		"",
		"",
		"",
		"false",
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
	}

	file, err := os.Open(imagePath)
	if err != nil {
		row[22] = err.Error()
		return append(row, verticalRectangleExperimentParamValues(variant.params)...)
	}
	defer file.Close()

	img, _, err := image.Decode(file)
	if err != nil {
		row[22] = err.Error()
		return append(row, verticalRectangleExperimentParamValues(variant.params)...)
	}
	bounds := img.Bounds()
	row[6] = strconv.Itoa(bounds.Dx())
	row[7] = strconv.Itoa(bounds.Dy())

	sample := sampleVerticalRectangleExperimentImage(img, variant.params.MaxSample)
	row[8] = strconv.Itoa(sample.width)
	row[9] = strconv.Itoa(sample.height)

	detection := detectVerticalRectangle(sample.red, sample.green, sample.blue, sample.luma, sample.width, sample.height, variant.params)
	row[10] = strconv.FormatBool(detection.Detected)
	row[11] = strconv.Itoa(detection.Focus.X)
	row[12] = strconv.Itoa(detection.Focus.Y)
	if detection.Detected {
		row[13] = strconv.Itoa(detection.Candidate.left)
		row[14] = strconv.Itoa(detection.Candidate.right)
		row[15] = formatExperimentFloat((float64(detection.Candidate.left) + float64(detection.Candidate.right)) / 2)
		row[16] = formatExperimentFloat(float64(detection.Candidate.right-detection.Candidate.left) / float64(sample.height))
		row[17] = formatExperimentFloat(detection.Candidate.score)
	}
	row[18] = strconv.Itoa(detection.EdgeCandidateCount)
	row[19] = strconv.Itoa(detection.RectangleCandidateCount)
	row[20] = strconv.FormatBool(detection.RepeatingPatternRejected)
	row[21] = formatExperimentFloat(detection.EdgeThreshold)

	return append(row, verticalRectangleExperimentParamValues(variant.params)...)
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

func verticalRectangleExperimentParamHeaders() []string {
	definitions := verticalRectangleExperimentParams()
	headers := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		headers = append(headers, "param_"+definition.name)
	}
	return headers
}

func verticalRectangleExperimentParamValues(params verticalRectangleParams) []string {
	definitions := verticalRectangleExperimentParams()
	values := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		value := definition.value(params)
		if definition.integer {
			values = append(values, strconv.Itoa(int(value)))
			continue
		}
		values = append(values, formatExperimentFloat(value))
	}
	return values
}

func verticalRectangleExperimentParams() []verticalRectangleExperimentParam {
	definitions := []verticalRectangleExperimentParam{
		{name: "max_sample", integer: true, value: func(p verticalRectangleParams) float64 { return float64(p.MaxSample) }, set: func(p *verticalRectangleParams, v float64) { p.MaxSample = int(v) }},
		{name: "min_sample_dimension", integer: true, value: func(p verticalRectangleParams) float64 { return float64(p.MinSampleDimension) }, set: func(p *verticalRectangleParams, v float64) { p.MinSampleDimension = int(v) }},
		{name: "color_contrast_weight", value: func(p verticalRectangleParams) float64 { return p.ColorContrastWeight }, set: func(p *verticalRectangleParams, v float64) { p.ColorContrastWeight = v }},
		{name: "edge_group_distance", integer: true, value: func(p verticalRectangleParams) float64 { return float64(p.EdgeGroupDistance) }, set: func(p *verticalRectangleParams, v float64) { p.EdgeGroupDistance = int(v) }},
		{name: "min_edge_coverage", value: func(p verticalRectangleParams) float64 { return p.MinEdgeCoverage }, set: func(p *verticalRectangleParams, v float64) { p.MinEdgeCoverage = v }},
		{name: "min_edge_run", value: func(p verticalRectangleParams) float64 { return p.MinEdgeRun }, set: func(p *verticalRectangleParams, v float64) { p.MinEdgeRun = v }},
		{name: "min_mean_threshold_ratio", value: func(p verticalRectangleParams) float64 { return p.MinMeanThresholdRatio }, set: func(p *verticalRectangleParams, v float64) { p.MinMeanThresholdRatio = v }},
		{name: "edge_score_coverage_base", value: func(p verticalRectangleParams) float64 { return p.EdgeScoreCoverageBase }, set: func(p *verticalRectangleParams, v float64) { p.EdgeScoreCoverageBase = v }},
		{name: "edge_score_run_base", value: func(p verticalRectangleParams) float64 { return p.EdgeScoreRunBase }, set: func(p *verticalRectangleParams, v float64) { p.EdgeScoreRunBase = v }},
		{name: "edge_neighbor_weight", value: func(p verticalRectangleParams) float64 { return p.EdgeNeighborWeight }, set: func(p *verticalRectangleParams, v float64) { p.EdgeNeighborWeight = v }},
		{name: "min_max_edge_strength", value: func(p verticalRectangleParams) float64 { return p.MinMaxEdgeStrength }, set: func(p *verticalRectangleParams, v float64) { p.MinMaxEdgeStrength = v }},
		{name: "edge_threshold_floor", value: func(p verticalRectangleParams) float64 { return p.EdgeThresholdFloor }, set: func(p *verticalRectangleParams, v float64) { p.EdgeThresholdFloor = v }},
		{name: "edge_threshold_max_ratio", value: func(p verticalRectangleParams) float64 { return p.EdgeThresholdMaxRatio }, set: func(p *verticalRectangleParams, v float64) { p.EdgeThresholdMaxRatio = v }},
		{name: "edge_threshold_mean_ratio", value: func(p verticalRectangleParams) float64 { return p.EdgeThresholdMeanRatio }, set: func(p *verticalRectangleParams, v float64) { p.EdgeThresholdMeanRatio = v }},
		{name: "repeating_edge_min_count", integer: true, value: func(p verticalRectangleParams) float64 { return float64(p.RepeatingEdgeMinCount) }, set: func(p *verticalRectangleParams, v float64) { p.RepeatingEdgeMinCount = int(v) }},
		{name: "repeating_edge_score_ratio", value: func(p verticalRectangleParams) float64 { return p.RepeatingEdgeScoreRatio }, set: func(p *verticalRectangleParams, v float64) { p.RepeatingEdgeScoreRatio = v }},
		{name: "side_rectangle_edge_weight", value: func(p verticalRectangleParams) float64 { return p.SideRectangleEdgeWeight }, set: func(p *verticalRectangleParams, v float64) { p.SideRectangleEdgeWeight = v }},
		{name: "aspect_min", value: func(p verticalRectangleParams) float64 { return p.AspectMin }, set: func(p *verticalRectangleParams, v float64) { p.AspectMin = v }},
		{name: "aspect_max", value: func(p verticalRectangleParams) float64 { return p.AspectMax }, set: func(p *verticalRectangleParams, v float64) { p.AspectMax = v }},
		{name: "aspect_distance_scale", value: func(p verticalRectangleParams) float64 { return p.AspectDistanceScale }, set: func(p *verticalRectangleParams, v float64) { p.AspectDistanceScale = v }},
		{name: "aspect_score_floor", value: func(p verticalRectangleParams) float64 { return p.AspectScoreFloor }, set: func(p *verticalRectangleParams, v float64) { p.AspectScoreFloor = v }},
		{name: "aspect_score_range", value: func(p verticalRectangleParams) float64 { return p.AspectScoreRange }, set: func(p *verticalRectangleParams, v float64) { p.AspectScoreRange = v }},
	}
	for i := range defaultVerticalRectangleParams().AspectTargets {
		targetIndex := i
		definitions = append(definitions, verticalRectangleExperimentParam{
			name: "aspect_target_" + strconv.Itoa(targetIndex),
			value: func(p verticalRectangleParams) float64 {
				return p.AspectTargets[targetIndex]
			},
			set: func(p *verticalRectangleParams, v float64) {
				p.AspectTargets[targetIndex] = v
			},
		})
	}
	return definitions
}

func readEmbeddedPortraitFilenames(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read embedded portrait list: %v", err)
	}
	var filenames []string
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
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

func formatExperimentFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}
