package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestSignatureAlignment pins that API tag IDs ("key0") match the generator's
// bare positional keys ("0"), and that tagSignature drops the go client's _h tag.
func TestSignatureAlignment(t *testing.T) {
	cases := []struct {
		name    string
		apiTags map[string]apiMetaTag
		genTags []tag
	}{
		{"no tags", map[string]apiMetaTag{}, nil},
		{"tagged", map[string]apiMetaTag{"key0": {"alpha"}, "key1": {"beta"}}, []tag{{"0", "alpha"}, {"1", "beta"}}},
		{"many", map[string]apiMetaTag{"key0": {"a"}, "key1": {"b"}, "key2": {"c"}, "key3": {"d"}, "key4": {"e"}, "key5": {"f"}},
			[]tag{{"0", "a"}, {"1", "b"}, {"2", "c"}, {"3", "d"}, {"4", "e"}, {"5", "f"}}},
		// _h is injected by the client, never generated.
		{"_h stripped", map[string]apiMetaTag{"key0": {"val"}, "_h": {"host-abc"}}, []tag{{"0", "val"}}},
		// rust/cpp send empty tag values verbatim, unlike go.
		{"empty value dropped", map[string]apiMetaTag{"key0": {"val"}, "key1": {""}}, []tag{{"0", "val"}}},
		// The API renders an absent grouped position as " 0" (tag value ID 0); a
		// real "0" value is never generated, so dropping it never over-drops.
		{"sentinel absent-position dropped", map[string]apiMetaTag{"key0": {"m0"}, "key1": {" 0"}}, []tag{{"0", "m0"}}},
	}
	for _, tc := range cases {
		api := tagSignature(tc.apiTags)
		gen := expectedSignature(tc.genTags)
		if api != gen {
			t.Errorf("%s: API signature %q != expected signature %q", tc.name, api, gen)
		}
	}
}

func mkSeriesResp(metas []apiSeriesMeta, data [][]float64, samplingAgg float64, base uint32, nb int) *apiSeriesResponse {
	times := make([]int64, nb)
	for i := 0; i < nb; i++ {
		times[i] = int64(base + uint32(i))
	}
	return &apiSeriesResponse{Data: apiResponseData{
		SamplingFactorAgg: samplingAgg,
		Series:            apiSeries{Time: times, SeriesMeta: metas, SeriesData: data},
	}}
}

// TestCompareCounts: the sampling tripwire must fail even when counts match.
func TestCompareCounts(t *testing.T) {
	base := uint32(1_700_000_000)
	m := metricModel{Name: "e2e_x_go_c_multi", Kind: kindCounter, QBKeys: []string{"0", "1"}, Series: []seriesModel{{
		Tags:   []tag{{"0", "x"}, {"1", "p"}},
		Counts: map[uint32]float64{base: 5, base + 1: 6},
	}}}
	const sig = "key0=x;key1=p"
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"x"}, "key1": {"p"}}}

	t.Run("exact match", func(t *testing.T) {
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 6}}, 0, base, 2)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 0 || len(miss) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("exact match not clean: mm=%v miss=%v ex=%v samp=%g", mm, miss, ex, samp)
		}
	})
	t.Run("wrong count", func(t *testing.T) {
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 99}}, 0, base, 2)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 1 || mm[0].bucket != base+1 || mm[0].actual != 99 {
			t.Errorf("mismatches=%+v, want one at bucket %d actual 99", mm, base+1)
		}
		if len(miss) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: miss=%v ex=%v samp=%g", miss, ex, samp)
		}
	})
	t.Run("missing series", func(t *testing.T) {
		resp := mkSeriesResp(nil, nil, 0, base, 2)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: mm=%v ex=%v samp=%g", mm, ex, samp)
		}
		if len(miss) != 1 || miss[0] != sig {
			t.Errorf("missing=%v, want [%s]", miss, sig)
		}
	})
	t.Run("extra series", func(t *testing.T) {
		extraMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"surprise"}, "key1": {"z"}}}
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta, extraMeta}, [][]float64{{5, 6}, {7, 8}}, 0, base, 2)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 0 || len(miss) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: mm=%v miss=%v samp=%g", mm, miss, samp)
		}
		if len(ex) != 1 || ex[0] != "key0=surprise;key1=z" {
			t.Errorf("extras=%v, want [key0=surprise;key1=z]", ex)
		}
	})
	t.Run("sampling tripwire", func(t *testing.T) {
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 6}}, 2, base, 2)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 0 || len(miss) != 0 || len(ex) != 0 {
			t.Errorf("unexpected non-empty fields: mm=%v miss=%v ex=%v", mm, miss, ex)
		}
		if samp == 0 {
			t.Error("sampling = 0, want nonzero (SamplingFactorAgg=2)")
		}
	})
}

// TestCompareValueAgg: exact aggregates are computed in write order (the
// agent's ValueSum left-fold).
func TestCompareValueAgg(t *testing.T) {
	base := uint32(1_700_000_000)
	vals := []float64{-3.5, -0.01, 2.718281828459045, 100.0}
	m := metricModel{Name: "e2e_x_go_v_mix", Kind: kindValue, QBKeys: []string{"0"}, Series: []seriesModel{{
		Tags:   []tag{{"0", "a"}},
		Values: map[uint32][]float64{base: vals},
	}}}
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"a"}}}
	for _, qw := range []string{"sum", "min", "max", "avg"} {
		exp := valueAggregate(vals, qw)
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{exp}}, 0, base, 1)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: qw})
		if len(mm) != 0 || len(miss) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("qw=%s expected %g not exact: mm=%v miss=%v ex=%v samp=%g", qw, exp, mm, miss, ex, samp)
		}
	}

	resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{valueAggregate(vals, "sum") + 1}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "sum"}); len(mm) != 1 {
		t.Errorf("sum mismatch not flagged: %v", mm)
	}
}

// TestComparePercentile pins the tolerance band around the true type-7 quantile.
func TestComparePercentile(t *testing.T) {
	base := uint32(1_700_000_000)
	vals := genValueUniform(1000) // sorted; p50=499.5
	m := metricModel{Name: "e2e_x_go_vp_mix", Kind: kindValueP, QBKeys: []string{"0"}, Series: []seriesModel{{
		Tags:   []tag{{"0", "unif"}},
		Values: map[uint32][]float64{base: vals},
	}}}
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"unif"}}}
	truth := quantile(vals, 0.5) // 499.5
	// Within tol (max(1%·499.5, 1.0)=4.995): 502 passes.
	resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{502}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "p50", q: 0.5}); len(mm) != 0 {
		t.Errorf("502 within tol of %g should pass: %v", truth, mm)
	}
	// Outside tol: 510 fails.
	resp = mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{510}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "p50", q: 0.5}); len(mm) != 1 {
		t.Errorf("510 outside tol of %g should fail: %v", truth, mm)
	}
}

// TestComparePercentileSkewBand: the skewed generator's steep inverse CDF pushes
// t-digest error past 1% (even on CH), so it gets percentileSkewTol; the band is
// per-generator, so a uniform series in the same metric keeps 1%.
func TestComparePercentileSkewBand(t *testing.T) {
	base := uint32(1_700_000_000)
	skewVals := genValueSkewed(1000) // sorted; steep inverse CDF near the median
	m := metricModel{Name: "e2e_x_go_vp_mix", Kind: kindValueP, QBKeys: []string{"0"}, Series: []seriesModel{
		{Tags: []tag{{"0", "unif"}}, Values: map[uint32][]float64{base: genValueUniform(1000)}},
		{Tags: []tag{{"0", "skew"}}, Values: map[uint32][]float64{base: skewVals}, GenKind: genKindValueSkewed},
	}}
	unifMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"unif"}}}
	skewMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"skew"}}}
	truth := quantile(skewVals, 0.5)

	// +4% on the skew series: inside percentileSkewTol (5%), outside the flat 1%.
	got := truth * 1.04
	resp := mkSeriesResp([]apiSeriesMeta{skewMeta}, [][]float64{{got}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "p50", q: 0.5}); len(mm) != 0 {
		t.Errorf("+4%% on the skew series should pass under the 5%% band (truth=%g got=%g): %v", truth, got, mm)
	}

	unifTruth := quantile(genValueUniform(1000), 0.5)
	resp = mkSeriesResp([]apiSeriesMeta{unifMeta}, [][]float64{{unifTruth * 1.04}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "p50", q: 0.5}); len(mm) != 1 {
		t.Errorf("+4%% on the uniform series must fail the 1%% band (truth=%g): %v", unifTruth, mm)
	}

	resp = mkSeriesResp([]apiSeriesMeta{skewMeta}, [][]float64{{truth * 1.07}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "p50", q: 0.5}); len(mm) != 1 {
		t.Errorf("+7%% on the skew series should fail the 5%% band (truth=%g): %v", truth, mm)
	}
}

// TestCompareUnique: exact up to uniquesHashMaxSize distinct, ±2% above.
func TestCompareUnique(t *testing.T) {
	base := uint32(1_700_000_000)
	mk := func(distinct int) metricModel {
		return metricModel{Name: "e2e_x_go_u", Kind: kindUnique, QBKeys: []string{"0"}, Series: []seriesModel{{
			Tags:    []tag{{"0", "s"}},
			Uniques: map[uint32]int{base: distinct},
		}}}
	}
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"s"}}}

	m := mk(smallUniqueDistinct)
	resp := mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{300}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "unique"}); len(mm) != 0 {
		t.Errorf("exact 300 should pass: %v", mm)
	}
	resp = mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{299}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "unique"}); len(mm) != 1 {
		t.Errorf("exact 299 should fail: %v", mm)
	}

	m = mk(bigUniqueDistinct)
	resp = mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{101500}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "unique"}); len(mm) != 0 {
		t.Errorf("101500 within ±2%% of 100000 should pass: %v", mm)
	}
	resp = mkSeriesResp([]apiSeriesMeta{wantMeta}, [][]float64{{97000}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "unique"}); len(mm) != 1 {
		t.Errorf("97000 outside ±2%% of 100000 should fail: %v", mm)
	}
}

func TestCompareCardinality(t *testing.T) {
	base := uint32(1_700_000_000)
	m := metricModel{Name: "e2e_x_go_s_dist", Kind: kindStag, Series: []seriesModel{
		{Tags: []tag{{"0", "a"}}, Counts: map[uint32]float64{base: 1}},
		{Tags: []tag{{"0", "b"}}, Counts: map[uint32]float64{base: 1}},
		{Tags: nil, Counts: map[uint32]float64{base: 1}}, // empty-value series
	}}
	total := apiSeriesMeta{Tags: map[string]apiMetaTag{}} // no group-by → signature ""
	// 3 distinct series at the bucket.
	resp := mkSeriesResp([]apiSeriesMeta{total}, [][]float64{{3}}, 0, base, 1)
	if mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "cardinality"}); len(mm) != 0 || len(miss) != 0 || len(ex) != 0 || samp != 0 {
		t.Errorf("cardinality 3 should pass clean: mm=%v miss=%v ex=%v samp=%g", mm, miss, ex, samp)
	}
	resp = mkSeriesResp([]apiSeriesMeta{total}, [][]float64{{2}}, 0, base, 1)
	if mm, _, _, _ := compareByFunc(m, resp, queryFunc{qw: "cardinality"}); len(mm) != 1 {
		t.Errorf("cardinality 2 (want 3) should fail: %v", mm)
	}
}

// TestCompareMetaNonCollision: client meta-metrics (statshouse_transport_metrics,
// __src_client_write_err) are never queried, and if one did appear it could only
// surface as an extra, never merge into an expected series.
func TestCompareMetaNonCollision(t *testing.T) {
	base := uint32(1_700_000_000)
	const e2eName = "e2e_runid_go_c_tagged"
	m := metricModel{Name: e2eName, Kind: kindCounter, Series: []seriesModel{{Tags: []tag{{"0", "alpha"}}, Counts: map[uint32]float64{base: 4}}}}
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"alpha"}}}
	metaNames := []string{"__src_client_write_err", "statshouse_transport_metrics"}

	t.Run("query isolates by exact e2e name", func(t *testing.T) {
		for _, meta := range metaNames {
			if meta == e2eName {
				t.Fatalf("test setup: meta-metric %q collides with the e2e name", meta)
			}
		}
		qurl := metricQueryURL("api:10888", m.Name, m.QBKeys, "count", base)
		if !strings.Contains(qurl, e2eName) {
			t.Errorf("metricQueryURL does not embed the exact e2e name %q: %s", e2eName, qurl)
		}
		for _, meta := range metaNames {
			if strings.Contains(qurl, meta) {
				t.Errorf("metricQueryURL leaked meta-metric name %q: %s", meta, qurl)
			}
		}
	})
	t.Run("meta-shaped series is extra, not merged", func(t *testing.T) {
		transportShaped := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"statshouse_transport_metrics"}}}
		errShaped := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"__src_client_write_err"}}}
		resp := mkSeriesResp([]apiSeriesMeta{wantMeta, transportShaped, errShaped}, [][]float64{{4}, {9}, {1}}, 0, base, 1)
		mm, miss, ex, samp := compareByFunc(m, resp, queryFunc{qw: "count"})
		if len(mm) != 0 || len(miss) != 0 || samp != 0 {
			t.Errorf("expected e2e series corrupted: mm=%v miss=%v samp=%g", mm, miss, samp)
		}
		if len(ex) != 2 {
			t.Fatalf("extras=%v, want both meta-shaped series flagged as extra", ex)
		}
	})
}

// TestClientWriteErrForLang: the silent-loss scan ignores other languages, zero
// buckets and the absent-position sentinel " 0".
func TestClientWriteErrForLang(t *testing.T) {
	base := uint32(1_700_000_000)
	goLang := apiSeriesMeta{Tags: map[string]apiMetaTag{"key1": {"1"}}}
	rustLang := apiSeriesMeta{Tags: map[string]apiMetaTag{"key1": {"3"}}}
	// go lost bytes at base; rust's only bucket is 0.
	resp := &apiSeriesResponse{Data: apiResponseData{Series: apiSeries{
		Time: []int64{int64(base)}, SeriesMeta: []apiSeriesMeta{goLang, rustLang}, SeriesData: [][]float64{{2048}, {0}},
	}}}
	if maxLost, found := clientWriteErrForLang(resp, "1"); !found || maxLost != 2048 {
		t.Errorf("go loss not detected: found=%v maxLost=%g, want found=true maxLost=2048", found, maxLost)
	}
	if _, found := clientWriteErrForLang(resp, "3"); found {
		t.Error("rust falsely flagged: its only bucket is 0 (no loss)")
	}

	sentinel := apiSeriesMeta{Tags: map[string]apiMetaTag{"key1": {" 0"}}}
	resp2 := &apiSeriesResponse{Data: apiResponseData{Series: apiSeries{
		Time: []int64{int64(base)}, SeriesMeta: []apiSeriesMeta{sentinel}, SeriesData: [][]float64{{99}},
	}}}
	for _, lang := range []string{"1", "3", "5"} {
		if _, found := clientWriteErrForLang(resp2, lang); found {
			t.Errorf(`sentinel " 0" series falsely matched lang %q`, lang)
		}
	}
}

type scriptedResp struct {
	val float64
	err error
}

// scriptedQuery replays resps, repeating the last one once exhausted.
type scriptedQuery struct {
	resps []scriptedResp
	n     int
}

func scripted(s *scriptedQuery) absenceQueryFunc {
	return func(ctx context.Context) (float64, error) {
		i := s.n
		if i >= len(s.resps) {
			i = len(s.resps) - 1
		}
		r := s.resps[i]
		s.n++
		return r.val, r.err
	}
}

// TestPollAbsenceTripwire: the absence poll fails closed when every query errors
// (a down stack never passes for "absent"), and fails fast on a non-zero point.
func TestPollAbsenceTripwire(t *testing.T) {
	ctx := context.Background()

	const to = 100 * time.Millisecond
	const iv = 5 * time.Millisecond
	errBoom := errors.New("api unreachable")

	t.Run("all queries fail → fail closed", func(t *testing.T) {
		s := &scriptedQuery{resps: []scriptedResp{{err: errBoom}}}
		o := pollAbsenceTripwire(ctx, to, iv, scripted(s))
		if o.ok {
			t.Error("ok=true, want false — a down stack must never pass for absence")
		}
		if o.confirmed {
			t.Error("confirmed=true, want false — no clean query ever ran")
		}
		if !errors.Is(o.queryErr, errBoom) {
			t.Errorf("queryErr=%v, want errBoom", o.queryErr)
		}
	})

	t.Run("clean and absent → pass", func(t *testing.T) {
		s := &scriptedQuery{resps: []scriptedResp{{val: 0}}}
		o := pollAbsenceTripwire(ctx, to, iv, scripted(s))
		if !o.ok {
			t.Errorf("ok=false, want true (absence confirmed): %+v", o)
		}
		if !o.confirmed {
			t.Error("confirmed=false, want true")
		}
	})

	t.Run("violation surfaces → fail fast", func(t *testing.T) {
		s := &scriptedQuery{resps: []scriptedResp{{val: 0}, {val: 42}}}
		start := time.Now()
		o := pollAbsenceTripwire(ctx, to, iv, scripted(s))
		elapsed := time.Since(start)
		if o.ok {
			t.Error("ok=true, want false — a non-zero point surfaced")
		}
		if !o.confirmed {
			t.Error("confirmed=false, want true — clean queries preceded the violation")
		}
		if o.worst != 42 {
			t.Errorf("worst=%g, want 42", o.worst)
		}
		if elapsed >= to {
			t.Errorf("did not fail fast: elapsed=%s >= timeout=%s (queries=%d)", elapsed, to, s.n)
		}
	})

	t.Run("errors then clean → pass", func(t *testing.T) {
		s := &scriptedQuery{resps: []scriptedResp{{err: errBoom}, {err: errBoom}, {val: 0}}}
		o := pollAbsenceTripwire(ctx, to, iv, scripted(s))
		if !o.ok {
			t.Errorf("ok=false, want true (a later clean zero confirms absence): %+v", o)
		}
		if !o.confirmed {
			t.Error("confirmed=false, want true — a clean query eventually ran")
		}
	})
}
