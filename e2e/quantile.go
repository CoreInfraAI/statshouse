package main

import (
	"math"
	"sort"
)

// Deterministic generators and true-quantile math shared by the expected model
// and the client drivers. The generators are bit-identical across Go, Rust and
// C++: they use only uint64 wrapping arithmetic and one float multiply/divide.

// lcgMul / lcgAdd / lcgSeed are the Knuth MMIX LCG constants. Each driver
// template repeats them as literals; TestDriverLCGIdentity pins the copies.
const (
	lcgMul  uint64 = 6364136223846793005
	lcgAdd  uint64 = 1442695040888963407
	lcgSeed uint64 = 0x9e3779b97f4a7c15 // a fixed, nonzero starting state (golden ratio splinter)
)

// skewedRange is the modulus for the skewed value's raw residue; the emitted
// value is r*r/skewedScale, so values land in [0, (skewedRange-1)²/skewedScale].
const (
	skewedRange = 1000
	skewedScale = 1000.0
)

// quantile returns the q-quantile of an already sorted slice by linear
// interpolation (NumPy "linear", R type 7), the definition the t-digest
// approximates. Empty input yields NaN so a caller bug surfaces.
func quantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n == 1 {
		return sorted[0]
	}
	switch {
	case q <= 0:
		return sorted[0]
	case q >= 1:
		return sorted[n-1]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	if lo >= n-1 {
		return sorted[n-1]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

// quantileOf sorts a copy of values and returns its q-quantile.
func quantileOf(values []float64, q float64) float64 {
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	return quantile(cp, q)
}

// genValueUniform returns {0, 1, ..., n-1}, already sorted.
func genValueUniform(n int) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = float64(i)
	}
	return out
}

// genValueSkewed returns n unsorted values with density ∝ 1/√v (r*r/skewedScale
// of an LCG residue), exercising the t-digest on a non-uniform population.
func genValueSkewed(n int) []float64 {
	out := make([]float64, n)
	x := lcgSeed
	for i := 0; i < n; i++ {
		x = x*lcgMul + lcgAdd
		r := uint32(x>>32) % skewedRange
		out[i] = float64(r) * float64(r) / skewedScale
	}
	return out
}

// genUniqueDistinct returns {1, 2, ..., n}. Above 65536 distinct values
// ChUnique switches to its approximate thinning estimator.
func genUniqueDistinct(n int) []int64 {
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(i + 1)
	}
	return out
}

// withinAbsTol accepts |actual-truth| ≤ max(absFrac·|truth|, minAbs); the
// absolute floor keeps a usable band for near-zero quantiles.
func withinAbsTol(actual, truth, absFrac, minAbs float64) bool {
	tol := math.Max(absFrac*math.Abs(truth), minAbs)
	return math.Abs(actual-truth) <= tol
}

// withinRelTol accepts |actual-truth| ≤ rel·|truth|. The big-unique case uses
// rel=0.02, ~4σ for ChUnique at 100k distinct (1σ≈0.45%).
func withinRelTol(actual, truth, rel float64) bool {
	return math.Abs(actual-truth) <= rel*math.Abs(truth)
}
