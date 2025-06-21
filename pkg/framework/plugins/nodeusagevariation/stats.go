package nodeusagevariation

import (
	"time"

	"github.com/prometheus/common/model"
)

func computeLinearRegressionFromSamples(samples []model.SamplePair) (slope, intercept, r2 float64) {
	if len(samples) < 2 {
		panic("need at least 2 samples")
	}

	n := float64(len(samples))

	// Convert to float arrays
	var sumX, sumY float64
	x := make([]float64, len(samples))
	y := make([]float64, len(samples))

	for i, s := range samples {
		// Assuming Time has method Unix() or UnixMilli()
		ts := float64(s.Timestamp.Unix()) // adjust if you have UnixMilli()
		val := float64(s.Value)

		x[i] = ts
		y[i] = val

		sumX += ts
		sumY += val
	}

	meanX := sumX / n
	meanY := sumY / n

	// Regression calculation
	var numerator, denominator float64
	for i := range samples {
		numerator += (x[i] - meanX) * (y[i] - meanY)
		denominator += (x[i] - meanX) * (x[i] - meanX)
	}
	slope = numerator / denominator
	intercept = meanY - slope*meanX

	// R² calculation
	var ssRes, ssTot float64
	for i := range samples {
		yi := y[i]
		yPred := slope*x[i] + intercept
		ssRes += (yi - yPred) * (yi - yPred)
		ssTot += (yi - meanY) * (yi - meanY)
	}

	r2 = 1
	if ssTot != 0. {
		r2 = 1.0 - (ssRes / ssTot)
	}
	return
}

func predictNextMinute(values []model.SamplePair) NextMinPrediction {
	slope, intercept, r2 := computeLinearRegressionFromSamples(values)
	nextMin := float64(time.Now().Add(1 * time.Minute).Unix())
	yPred := slope*nextMin + intercept

	return NextMinPrediction{
		pred: yPred,
		r2:   r2,
	}
}
