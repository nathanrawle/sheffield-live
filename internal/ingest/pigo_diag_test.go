package ingest

import (
	"image"
	"image/color"
	_ "image/jpeg"
	"math"
	"os"
	"testing"

	pigo "github.com/esimov/pigo/core"
)

func TestPigoDiagKaiaSampleSizes(t *testing.T) {
	file, err := os.Open("testdata/kaia-kater-face-prior.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	img, _, err := image.Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	detector, err := newPigoFacePriorDetector()
	if err != nil {
		t.Fatal(err)
	}
	for _, maxSample := range []int{128, 192, 256, 320, 450} {
		sample := diagnosticPigoSample(img, maxSample)
		detections := detector.classifier.RunCascade(pigo.CascadeParams{
			MinSize:     maxInt(facePriorMinSampleSize, minInt(sample.width, sample.height)/6),
			MaxSize:     minInt(sample.width, sample.height),
			ShiftFactor: facePriorDefaultShift,
			ScaleFactor: facePriorDefaultScale,
			ImageParams: pigo.ImageParams{
				Pixels: sample.grayscale,
				Rows:   sample.height,
				Cols:   sample.width,
				Dim:    sample.width,
			},
		}, 0)
		clustered := detector.classifier.ClusterDetections(detections, facePriorClusterIoU)
		filtered := filterFacePriorDetections(clustered, sample.width, sample.height)
		t.Logf("max=%d sample=%dx%d raw=%d clustered=%d filtered=%d %#v", maxSample, sample.width, sample.height, len(detections), len(clustered), len(filtered), filtered)
	}
}

func diagnosticPigoSample(img image.Image, maxSample int) imageFocusSample {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	sampleWidth, sampleHeight := focusSampleSizeWithMax(width, height, maxSample)
	sample := imageFocusSample{
		width:     sampleWidth,
		height:    sampleHeight,
		grayscale: make([]uint8, sampleWidth*sampleHeight),
	}
	for y := 0; y < sampleHeight; y++ {
		srcY := bounds.Min.Y + minInt(height-1, y*height/sampleHeight)
		for x := 0; x < sampleWidth; x++ {
			srcX := bounds.Min.X + minInt(width-1, x*width/sampleWidth)
			c := color.NRGBAModel.Convert(img.At(srcX, srcY)).(color.NRGBA)
			r := float64(c.R) / 255
			g := float64(c.G) / 255
			b := float64(c.B) / 255
			sample.grayscale[y*sampleWidth+x] = uint8(math.Round((0.299*r + 0.587*g + 0.114*b) * 255))
		}
	}
	return sample
}
