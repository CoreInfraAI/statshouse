package main

import (
	"math"
	"reflect"
	"sort"
	"testing"
)

// TestQuantileUniform pins the type-7 quantiles the value_p assertions treat as
// truth; a shift here would silently change every percentile expectation.
func TestQuantileUniform(t *testing.T) {
	xs := genValueUniform(1000) // 0..999, already sorted
	cases := []struct {
		q    float64
		want float64
	}{
		{0.50, 499.5},
		{0.90, 899.1},
		{0.99, 989.01},
		{0.00, 0},
		{1.00, 999},
	}
	for _, c := range cases {
		if got := quantile(xs, c.q); got != c.want {
			t.Errorf("quantile(0..999, %g) = %g, want %g", c.q, got, c.want)
		}
	}
}

func TestQuantileOfUnsorted(t *testing.T) {
	xs := []float64{3, 1, 4, 1, 5, 9, 2, 6}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	if got := quantileOf(xs, 0.5); got != quantile(sorted, 0.5) {
		t.Errorf("quantileOf(unsorted,0.5)=%g != quantile(sorted,0.5)=%g", got, quantile(sorted, 0.5))
	}
}

// TestQuantileEdge: empty is NaN (a caller bug, not a silent 0); q is clamped.
func TestQuantileEdge(t *testing.T) {
	if q := quantile(nil, 0.5); !math.IsNaN(q) {
		t.Errorf("quantile(empty) = %g, want NaN", q)
	}
	if q := quantile([]float64{42}, 0.5); q != 42 {
		t.Errorf("quantile({42}) = %g, want 42", q)
	}
	xs := genValueUniform(10)
	if quantile(xs, -1) != 0 || quantile(xs, 2) != 9 {
		t.Errorf("quantile clamp failed: q=-1→%g q=2→%g", quantile(xs, -1), quantile(xs, 2))
	}
}

// TestGenValueSkewedDeterministic: every client driver must reproduce these
// exact values, or value_p assertions compare against the wrong truth.
func TestGenValueSkewedDeterministic(t *testing.T) {
	got := genValueSkewed(4)
	// Reproduce by hand from lcgSeed.
	x := lcgSeed
	var want []float64
	for i := 0; i < 4; i++ {
		x = x*lcgMul + lcgAdd
		r := uint32(x>>32) % skewedRange
		want = append(want, float64(r)*float64(r)/skewedScale)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("genValueSkewed(4)=%v, want %v", got, want)
	}

	big := genValueSkewed(4000)
	med := quantileOf(big, 0.5)
	if med > 350 { // uniform midpoint would be ~ (999²/1000)/2 ≈ 499
		t.Errorf("skewed median %g too high — distribution not skewed toward 0", med)
	}
}

func TestGenValueUniformAndUnique(t *testing.T) {
	u := genValueUniform(5)
	if !reflect.DeepEqual(u, []float64{0, 1, 2, 3, 4}) {
		t.Errorf("genValueUniform(5)=%v", u)
	}
	d := genUniqueDistinct(3)
	if !reflect.DeepEqual(d, []int64{1, 2, 3}) {
		t.Errorf("genUniqueDistinct(3)=%v", d)
	}
}

func TestWithinTol(t *testing.T) {
	// Percentile: max(1%·|truth|, 1.0).
	if !withinAbsTol(502, 499.5, 0.01, 1.0) {
		t.Error("502 should be within 1% of 499.5")
	}
	if withinAbsTol(510, 499.5, 0.01, 1.0) {
		t.Error("510 should be OUTSIDE 1% of 499.5")
	}
	// Near-zero truth: the 1.0 floor binds.
	if !withinAbsTol(0.9, 0, 0.01, 1.0) {
		t.Error("0.9 should be within the 1.0 floor of truth 0")
	}

	if !withinRelTol(101500, 100000, 0.02) {
		t.Error("101500 should be within 2% of 100000")
	}
	if withinRelTol(105000, 100000, 0.02) {
		t.Error("105000 should be OUTSIDE 2% of 100000")
	}
}
