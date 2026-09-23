// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/json"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	statshouse "github.com/VKCOM/statshouse-go"
)

// confStream builds the stream runConformancePhase does, with a fixed run id and now.
func confStream(t *testing.T) metricStream {
	t.Helper()
	return generateStream("20260815-000000", conformanceClientTag, time.Unix(1800000000, 0))
}

func metricByName(t *testing.T, s metricStream, suffix string) metricModel {
	t.Helper()
	m, ok := confMetric(s, suffix)
	require.True(t, ok, "metric *%s not in stream", suffix)
	return m
}

// A duplicate (kind, path) would double-count a pass and mask a dropped sibling.
func TestBuildConformanceRequestsCoverage(t *testing.T) {
	stream := confStream(t)
	reqs := buildConformanceRequests(stream)

	kinds := map[confKind]int{}
	paths := map[string]bool{}
	for _, r := range reqs {
		kinds[r.kind]++
		require.NotEmpty(t, r.label)
		require.NotEmpty(t, r.qw)
		require.NotEmpty(t, r.path)
		key := string(rune(r.kind)) + r.path
		require.False(t, paths[key], "duplicate request path %s", r.path)
		paths[key] = true
	}
	require.Contains(t, kinds, confSeries)
	require.Contains(t, kinds, confTable)
	require.Contains(t, kinds, confPoint)
	require.Contains(t, kinds, confTagValues)

	qwByMetric := map[string]map[string]bool{}
	for _, r := range reqs {
		if r.kind != confSeries || strings.HasPrefix(r.label, "series-mh/") || strings.HasPrefix(r.label, "series-host/") || strings.HasPrefix(r.label, "promql/") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(r.label, "series/"), "/"+r.qw)
		if qwByMetric[name] == nil {
			qwByMetric[name] = map[string]bool{}
		}
		qwByMetric[name][r.qw] = true
	}
	for _, m := range stream.Metrics {
		want := map[string]bool{}
		for _, qf := range funcsFor(m.Kind) {
			want[qf.qw] = true
		}
		require.Equal(t, want, qwByMetric[confShortName(m.Name)], "metric %s function coverage", m.Name)
	}

	// value_p count+sum companions tell data- from estimator-level divergence
	for _, m := range stream.Metrics {
		if m.Kind != kindValueP {
			continue
		}
		for _, qw := range []string{"count", "sum"} {
			var got *confRequest
			for i := range reqs {
				if reqs[i].label == "series-exact/"+confShortName(m.Name)+"/"+qw {
					got = &reqs[i]
				}
			}
			require.NotNil(t, got, "no series-exact request for %s/%s", m.Name, qw)
			u, perr := url.Parse(got.path)
			require.NoError(t, perr)
			require.Equal(t, qw, u.Query().Get("qw"))
		}
	}
}

// Series query strings must match the frozen asserter's byte for byte.
func TestConformanceSeriesParityWithAsserter(t *testing.T) {
	stream := confStream(t)
	reqs := buildConformanceRequests(stream)

	seen := 0
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			want := strings.TrimPrefix(metricQueryURL("ignored", m.Name, m.QBKeys, qf.qw, stream.Base), "http://ignored")
			var got *confRequest
			for i := range reqs {
				if reqs[i].label == "series/"+confShortName(m.Name)+"/"+qf.qw {
					got = &reqs[i]
				}
			}
			require.NotNil(t, got, "no series request for %s/%s", m.Name, qf.qw)
			require.Equal(t, want, got.path, "param parity with the frozen asserter for %s/%s", m.Name, qf.qw)
			seen++
		}
	}
	require.Positive(t, seen)
}

func TestConformanceSpecialRequestShapes(t *testing.T) {
	stream := confStream(t)
	reqs := buildConformanceRequests(stream)
	byLabel := map[string]confRequest{}
	for _, r := range reqs {
		byLabel[r.label] = r
	}
	base := stream.Base

	q := func(path string) url.Values {
		u, err := url.Parse(path)
		require.NoError(t, err)
		return u.Query()
	}

	mh, ok := byLabel["series-mh/c_tagged/count"]
	require.True(t, ok)
	require.Equal(t, "/api/query", strings.SplitN(mh.path, "?", 2)[0])
	require.Equal(t, "1", q(mh.path).Get("mh"))
	require.Equal(t, "count", q(mh.path).Get("qw"))
	host, ok := byLabel["series-host/v_mix/max_count_host"]
	require.True(t, ok)
	require.Equal(t, "max_count_host", q(host.path).Get("qw"))

	month, ok := byLabel["series-month/c_tagged/count"]
	require.True(t, ok)
	require.Equal(t, "/api/query", strings.SplitN(month.path, "?", 2)[0])
	require.Equal(t, "1M", q(month.path).Get("w"))
	require.Equal(t, "count", q(month.path).Get("qw"))

	tvs, ok := byLabel["table/v_mix/sum"]
	require.True(t, ok)
	require.Equal(t, "/api/table", strings.SplitN(tvs.path, "?", 2)[0])
	require.Equal(t, "sum", q(tvs.path).Get("qw"))
	require.Empty(t, q(tvs.path).Get("n"), "v_mix table must not pin n")
	tvc, ok := byLabel["table/c_matrix/count"]
	require.True(t, ok)
	require.Equal(t, "10000", q(tvc.path).Get("n"))

	// point covers exactly the LAST bucket of the stream
	pt, ok := byLabel["point/c_tagged/count"]
	require.True(t, ok)
	require.Equal(t, "/api/point", strings.SplitN(pt.path, "?", 2)[0])
	pq := q(pt.path)
	require.Equal(t, fmtUint(base+numBuckets-1), pq.Get("f"))
	require.Equal(t, fmtUint(base+numBuckets), pq.Get("t"))

	// label names must be bare identifiers (the parser rejects quoted ones)
	prom, ok := byLabel["promql/c_multi|c_matrix/count-by-0"]
	require.True(t, ok)
	multi := metricByName(t, stream, "c_multi")
	matrix := metricByName(t, stream, "c_matrix")
	require.Contains(t, q(prom.path).Get("q"), `__name__=~"`+multi.Name+"|"+matrix.Name+`"`)
	require.Contains(t, q(prom.path).Get("q"), `__what__="count"`)
	require.Contains(t, q(prom.path).Get("q"), `__by__="0"`)

	for _, suffix := range []string{"c_matrix", "u_exact"} {
		tv, ok := byLabel["tag-values/"+suffix+"/k0"]
		require.True(t, ok, suffix)
		require.Equal(t, "/api/metric-tag-values", strings.SplitN(tv.path, "?", 2)[0])
		tvq := q(tv.path)
		m := metricByName(t, stream, suffix)
		require.Equal(t, m.Name, tvq.Get("s"))
		require.Equal(t, "0", tvq.Get("k"))
		require.Equal(t, fmtUint(base), tvq.Get("f"))
		require.Equal(t, fmtUint(base+numBuckets), tvq.Get("t"))
		require.Equal(t, "1000", tvq.Get("n"))
	}
}

func fmtUint(v uint32) string {
	return strings.TrimSpace(strings.Replace(jsonUint(v), "\"", "", -1))
}

func jsonUint(v uint32) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Mirrors the frozen suite: exact for counts, banded percentiles, threshold-split uniques.
func TestConfValueMatches(t *testing.T) {
	require.True(t, confValueMatches("count", 41, 41))
	require.False(t, confValueMatches("count", 41, 41.5))
	// sum/avg tolerate float-accumulation-order noise, not real divergence
	require.True(t, confValueMatches("sum", 644601.744, 644601.7439999995))
	require.True(t, confValueMatches("avg", 1e9, 1e9+0.001))
	require.False(t, confValueMatches("sum", 1e9, 1e9+1e5))
	require.False(t, confValueMatches("avg", 1e9, 1e9+1e5))
	require.True(t, confValueMatches("max_count_host", 7, 7))

	// percentile band: max(1% of |ref|, 1.0)
	require.True(t, confValueMatches("p90", 1000, 992))
	require.False(t, confValueMatches("p90", 1000, 988)) // >1% off
	require.True(t, confValueMatches("p50", 10, 11))     // within the 1.0 floor
	require.False(t, confValueMatches("p99", 10, 11.2))
	require.True(t, confValueMatches("p99", 10, 11))
	for _, qw := range []string{"p50", "p90", "p99"} {
		require.True(t, confValueMatches(qw, 100, 101))
		require.False(t, confValueMatches(qw, 100, 102.01))
	}

	// unique: exact at/below the hash-set threshold, ±2% above
	require.True(t, confValueMatches("unique", 300, 300))
	require.False(t, confValueMatches("unique", 300, 299))
	require.True(t, confValueMatches("unique", float64(uniquesHashMaxSize+1), float64(uniquesHashMaxSize+1)*1.019))
	require.False(t, confValueMatches("unique", float64(uniquesHashMaxSize+1), float64(uniquesHashMaxSize+1)*1.021))
}

// "__name__" stays in the series signature so multi-metric series never alias.
func TestCompareConfSeries(t *testing.T) {
	meta := func(tags map[string]string, hosts ...string) confSeriesMeta {
		m := confSeriesMeta{Tags: map[string]apiMetaTag{}, MaxHosts: hosts}
		for k, v := range tags {
			m.Tags[k] = apiMetaTag{Value: v}
		}
		return m
	}
	build := func(sampling float64, metas ...confSeriesMeta) *confSeriesResp {
		r := &confSeriesResp{}
		r.Data.SamplingFactorSrc = sampling
		r.Data.Series = confSeriesData{
			Time:       []int64{100, 101, 102},
			SeriesMeta: metas,
			SeriesData: make([][]float64, len(metas)),
		}
		for i := range metas {
			r.Data.Series.SeriesData[i] = []float64{1, 2, 3}
		}
		return r
	}

	t.Run("identical", func(t *testing.T) {
		ref := build(0, meta(map[string]string{"key0": "a"}))
		got := build(0, meta(map[string]string{"key0": "a"}))
		require.Empty(t, compareConfSeries(ref, got, "count"))
	})

	t.Run("value band", func(t *testing.T) {
		ref := build(0, meta(map[string]string{"key0": "a"}))
		got := build(0, meta(map[string]string{"key0": "a"}))
		got.Data.Series.SeriesData[0][1] = 2.5 // p90-tolerated, count-divergent
		require.Empty(t, compareConfSeries(ref, got, "p90"))
		got.Data.Series.SeriesData[0][2] = 4 // >1% and >1.0 off both kinds
		diffs := compareConfSeries(ref, got, "count")
		joined := strings.Join(diffs, "\n")
		require.Contains(t, joined, "bucket=101")
		require.Contains(t, joined, "bucket=102")
		require.Contains(t, joined, "duck=4")
	})

	t.Run("missing and extra series", func(t *testing.T) {
		ref := build(0, meta(map[string]string{"key0": "a"}), meta(map[string]string{"key0": "b"}))
		got := build(0, meta(map[string]string{"key0": "a"}), meta(map[string]string{"key0": "c"}))
		diffs := compareConfSeries(ref, got, "count")
		require.Len(t, diffs, 2)
		joined := strings.Join(diffs, "\n")
		require.Contains(t, joined, "absent in duck")
		require.Contains(t, joined, "absent in reference")
	})

	t.Run("multi metric names distinguish series", func(t *testing.T) {
		ref := build(0, meta(map[string]string{"__name__": "m1"}), meta(map[string]string{"__name__": "m2"}))
		got := build(0, meta(map[string]string{"__name__": "m1"}), meta(map[string]string{"__name__": "m2"}))
		require.Empty(t, compareConfSeries(ref, got, "count"))
		// swapping the two metrics' data is a divergence
		got.Data.Series.SeriesData[0], got.Data.Series.SeriesData[1] = []float64{9, 9, 9}, []float64{1, 2, 3}
		require.NotEmpty(t, compareConfSeries(ref, got, "count"))
	})

	t.Run("max hosts exact", func(t *testing.T) {
		ref := build(0, meta(map[string]string{"key0": "a"}, "host-a"))
		same := build(0, meta(map[string]string{"key0": "a"}, "host-a"))
		require.Empty(t, compareConfSeries(ref, same, "count"))
		diff := build(0, meta(map[string]string{"key0": "a"}, "host-b"))
		diffs := compareConfSeries(ref, diff, "count")
		require.Len(t, diffs, 1)
		require.Contains(t, diffs[0], "max_hosts")
	})

	t.Run("sampling and time axis", func(t *testing.T) {
		ref := build(0, meta(nil))
		sampled := build(0.5, meta(nil))
		diffs := compareConfSeries(ref, sampled, "count")
		require.Len(t, diffs, 1)
		require.Contains(t, diffs[0], "sampling_factor")
		shorter := build(0, meta(nil))
		shorter.Data.Series.Time = shorter.Data.Series.Time[:2]
		diffs = compareConfSeries(ref, shorter, "count")
		require.Len(t, diffs, 1)
		require.Contains(t, diffs[0], "time axes differ")
	})
}

func TestCompareConfTable(t *testing.T) {
	row := func(ts int64, sig string, vals ...float64) (struct {
		Time int64                 `json:"time"`
		Data []float64             `json:"data"`
		Tags map[string]apiMetaTag `json:"tags"`
	}, string) {
		var r struct {
			Time int64                 `json:"time"`
			Data []float64             `json:"data"`
			Tags map[string]apiMetaTag `json:"tags"`
		}
		r.Time = ts
		r.Data = vals
		r.Tags = map[string]apiMetaTag{}
		if sig != "" {
			r.Tags["key0"] = apiMetaTag{Value: sig}
		}
		return r, sig
	}
	build := func(more bool, rows ...struct {
		Time int64                 `json:"time"`
		Data []float64             `json:"data"`
		Tags map[string]apiMetaTag `json:"tags"`
	}) *confTableResp {
		r := &confTableResp{}
		r.Data.What = []string{"count"}
		r.Data.More = more
		r.Data.Rows = rows
		return r
	}
	r1, _ := row(100, "a", 5)
	r2, _ := row(101, "b", 7)

	t.Run("order insensitive identical", func(t *testing.T) {
		ref := build(false, r1, r2)
		got := build(false, r2, r1)
		require.Empty(t, compareConfTable(ref, got, "count"))
	})
	t.Run("cell and row divergence", func(t *testing.T) {
		bad, _ := row(100, "a", 6)
		ref := build(false, r1, r2)
		got := build(false, bad)
		diffs := compareConfTable(ref, got, "count")
		require.Len(t, diffs, 2)
		joined := strings.Join(diffs, "\n")
		require.Contains(t, joined, "cell=0 reference=5 duck=6")
		require.Contains(t, joined, "absent in duck")
	})
	t.Run("truncation flag", func(t *testing.T) {
		ref := build(false, r1)
		got := build(true, r1)
		diffs := compareConfTable(ref, got, "count")
		require.Len(t, diffs, 1)
		require.Contains(t, diffs[0], "more=")
	})
	t.Run("sum cells use the float-ordering band", func(t *testing.T) {
		// ~1e-16 relative drift on large sums is float addition order
		a, _ := row(100, "a", 644601.744)
		b, _ := row(100, "a", 644601.7439999995)
		require.Empty(t, compareConfTable(build(false, a), build(false, b), "sum"))
		// a real 0.01% divergence stays far above the band
		big, _ := row(101, "b", 1e6)
		wrong, _ := row(101, "b", 1.0001e6)
		require.NotEmpty(t, compareConfTable(build(false, big), build(false, wrong), "sum"))
		// the band is for sum/avg only: count stays exact
		c1, _ := row(102, "c", 5)
		c2, _ := row(102, "c", 5.0000000001)
		require.NotEmpty(t, compareConfTable(build(false, c1), build(false, c2), "count"))
	})
}

// Points are keyed by tags+host+range: a shifted host or window is a missing point.
func TestCompareConfPoint(t *testing.T) {
	build := func(host string, from, to int64, val float64) *confPointResp {
		r := &confPointResp{}
		r.Data.PointMeta = append(r.Data.PointMeta, struct {
			Tags    map[string]apiMetaTag `json:"tags"`
			MaxHost string                `json:"max_host"`
			FromSec int64                 `json:"from_sec"`
			ToSec   int64                 `json:"to_sec"`
		}{Tags: map[string]apiMetaTag{"key0": {Value: "a"}}, MaxHost: host, FromSec: from, ToSec: to})
		r.Data.PointData = []float64{val}
		return r
	}
	require.Empty(t, compareConfPoint(build("h", 100, 101, 7), build("h", 100, 101, 7), "unique"))

	diffs := compareConfPoint(build("h", 100, 101, 7), build("h", 100, 101, 8), "unique")
	require.Len(t, diffs, 1)
	require.Contains(t, diffs[0], "reference=7")

	require.Empty(t, compareConfPoint(build("h", 100, 101, 100), build("h", 100, 101, 101), "p90"))

	diffs = compareConfPoint(build("h1", 100, 101, 7), build("h2", 100, 101, 7), "count")
	require.Len(t, diffs, 2)
	joined := strings.Join(diffs, "\n")
	require.Contains(t, joined, "absent in reference")
	require.Contains(t, joined, "absent in duck")
}

func TestCompareConfTagValues(t *testing.T) {
	build := func(more bool, pairs ...[2]any) *confTagValuesResp {
		r := &confTagValuesResp{}
		r.Data.TagValuesMore = more
		for _, p := range pairs {
			r.Data.TagValues = append(r.Data.TagValues, struct {
				Value string  `json:"value"`
				Count float64 `json:"count"`
			}{p[0].(string), p[1].(float64)})
		}
		return r
	}
	require.Empty(t, compareConfTagValues(
		build(false, [2]any{"x", 3.0}, [2]any{"y", 4.0}),
		build(false, [2]any{"y", 4.0}, [2]any{"x", 3.0})))

	diffs := compareConfTagValues(
		build(false, [2]any{"x", 3.0}, [2]any{"y", 4.0}),
		build(false, [2]any{"x", 3.0}, [2]any{"y", 4.5}))
	require.Len(t, diffs, 2)
	joined := strings.Join(diffs, "\n")
	require.Contains(t, joined, "absent in duck")
	require.Contains(t, joined, "absent in reference")

	diffs = compareConfTagValues(build(false), build(true))
	require.Len(t, diffs, 1)
	require.Contains(t, diffs[0], "tag_values_more")
}

// confNonEmpty guards against a vacuous pass on an empty answer.
func TestConfNonEmpty(t *testing.T) {
	series := &confSeriesResp{}
	series.Data.Series = confSeriesData{Time: []int64{1}, SeriesMeta: []confSeriesMeta{{}}, SeriesData: [][]float64{{1}}}
	table := &confTableResp{}
	table.Data.Rows = make([]struct {
		Time int64                 `json:"time"`
		Data []float64             `json:"data"`
		Tags map[string]apiMetaTag `json:"tags"`
	}, 1)
	point := &confPointResp{}
	point.Data.PointMeta = make([]struct {
		Tags    map[string]apiMetaTag `json:"tags"`
		MaxHost string                `json:"max_host"`
		FromSec int64                 `json:"from_sec"`
		ToSec   int64                 `json:"to_sec"`
	}, 1)
	point.Data.PointData = []float64{1}
	tags := &confTagValuesResp{}
	tags.Data.TagValues = make([]struct {
		Value string  `json:"value"`
		Count float64 `json:"count"`
	}, 1)

	require.False(t, confNonEmpty(confSeries, confDecoded{series: &confSeriesResp{}}))
	require.False(t, confNonEmpty(confTable, confDecoded{table: &confTableResp{}}))
	require.False(t, confNonEmpty(confPoint, confDecoded{point: &confPointResp{}}))
	require.False(t, confNonEmpty(confTagValues, confDecoded{tags: &confTagValuesResp{}}))

	require.True(t, confNonEmpty(confSeries, confDecoded{series: series}))
	require.True(t, confNonEmpty(confTable, confDecoded{table: table}))
	require.True(t, confNonEmpty(confPoint, confDecoded{point: point}))
	require.True(t, confNonEmpty(confTagValues, confDecoded{tags: tags}))
}

// The decoders must match the API's JSON field names (table from_row/to_row are strings).
func TestConfDecodersReadAPIJSON(t *testing.T) {
	t.Run("series", func(t *testing.T) {
		body := `{"data":{"series":{"time":[100,101],"series_meta":[{"tags":{"key0":{"value":"a"}},"max_hosts":["h1"]}],"series_data":[[1,2]]},"sampling_factor_src":0,"sampling_factor_agg":0}}`
		var r confSeriesResp
		require.NoError(t, json.Unmarshal([]byte(body), &r))
		require.Equal(t, []int64{100, 101}, r.Data.Series.Time)
		require.Equal(t, "a", r.Data.Series.SeriesMeta[0].Tags["key0"].Value)
		require.Equal(t, []string{"h1"}, r.Data.Series.SeriesMeta[0].MaxHosts)
		require.Equal(t, [][]float64{{1, 2}}, r.Data.Series.SeriesData)
	})
	t.Run("table", func(t *testing.T) {
		body := `{"data":{"rows":[{"time":100,"data":[5],"tags":{"key0":{"value":"a"}}}],"what":["count"],"from_row":"1","to_row":"2","more":false}}`
		var r confTableResp
		require.NoError(t, json.Unmarshal([]byte(body), &r))
		require.Len(t, r.Data.Rows, 1)
		require.Equal(t, int64(100), r.Data.Rows[0].Time)
		require.Equal(t, []float64{5}, r.Data.Rows[0].Data)
		require.Equal(t, "a", r.Data.Rows[0].Tags["key0"].Value)
		require.Equal(t, []string{"count"}, r.Data.What)
		require.False(t, r.Data.More)
	})
	t.Run("point", func(t *testing.T) {
		body := `{"data":{"point_meta":[{"tags":{"key0":{"value":"a"}},"max_host":"h1","from_sec":100,"to_sec":101}],"point_data":[7]}}`
		var r confPointResp
		require.NoError(t, json.Unmarshal([]byte(body), &r))
		require.Equal(t, "h1", r.Data.PointMeta[0].MaxHost)
		require.Equal(t, int64(100), r.Data.PointMeta[0].FromSec)
		require.Equal(t, int64(101), r.Data.PointMeta[0].ToSec)
		require.Equal(t, []float64{7}, r.Data.PointData)
	})
	t.Run("tag values", func(t *testing.T) {
		body := `{"data":{"tag_values":[{"value":"a","count":3},{"value":"b","count":4}],"tag_values_more":false}}`
		var r confTagValuesResp
		require.NoError(t, json.Unmarshal([]byte(body), &r))
		require.Len(t, r.Data.TagValues, 2)
		require.Equal(t, "a", r.Data.TagValues[0].Value)
		require.Equal(t, 3.0, r.Data.TagValues[0].Count)
		require.False(t, r.Data.TagValuesMore)
	})
}

// Two stacks share a network; pruning and teardown match on the e2e prefix.
func TestDaemonStackCnameStackTag(t *testing.T) {
	o := daemonStackOpts{runID: "20260815-000000"}
	require.Equal(t, "e2e-20260815-000000-agg", o.cname("agg"))
	o.stackTag = confStackDuck
	require.Equal(t, "e2e-20260815-000000-duck-agg", o.cname("agg"))
	require.True(t, strings.HasPrefix(o.cname("api"), e2ePrefix+"20260815-000000-"))
}

// Log names must stay distinct across the two conformance stacks.
func TestServiceLogNameRunIDAware(t *testing.T) {
	const runID = "20260815-000000"
	require.Equal(t, "ch-agg", serviceLogName("e2e-"+runID+"-ch-agg", runID))
	require.Equal(t, "duck-agg", serviceLogName("e2e-"+runID+"-duck-agg", runID))
	require.Equal(t, "metadata", serviceLogName("e2e-"+runID+"-metadata", runID))
	require.Equal(t, "clickhouse", serviceLogName("e2e-"+runID+"-clickhouse", runID))
	// no runID (or a foreign container): the last-segment fallback
	require.Equal(t, "agg", serviceLogName("e2e-20260815-000000-agg", ""))
	require.Equal(t, "agg", serviceLogName("e2e-99999999-111111-agg", runID))
}

func TestConfShortName(t *testing.T) {
	require.Equal(t, "c_tagged", confShortName("e2e_20260815_000000_conf_c_tagged"))
	require.Equal(t, "plain", confShortName("plain"))
}

// Tags render verbatim, including empty values, as the go driver template does.
func TestConfNamedTagsVerbatim(t *testing.T) {
	got := confNamedTags([]tag{{Key: "0", Val: "a"}, {Key: "1", Val: ""}, {Key: "2", Val: "0"}})
	require.Equal(t, statshouse.NamedTags{{"0", "a"}, {"1", ""}, {"2", "0"}}, got)
}

// Pins statshouse-go v0.5.17: a healthy single-address TCP client's Close() returns
// statshouseGoEmptyAddrErr from the idle secondary pool. If an upgrade makes it nil,
// drop the workaround in seedConformanceStream.
func TestStatshouseGoSingleAddrCloseQuirk(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					c.SetReadDeadline(time.Now().Add(10 * time.Second))
					if _, rerr := c.Read(buf); rerr != nil {
						c.Close()
						return
					}
				}
			}()
		}
	}()

	cl := statshouse.NewClientEx(statshouse.ConfigureArgs{
		StatsHouseAddr: ln.Addr().String(),
		Network:        "tcp",
		MaxBucketSize:  1 << 18,
	})
	cl.NamedCountHistoric("e2e_close_quirk_probe", nil, 1, uint32(time.Now().Unix()-5))
	time.Sleep(500 * time.Millisecond) // let the primary connect and the packet flush

	require.Equal(t, statshouseGoEmptyAddrErr, cl.Close().Error())
}
