// Command e2e's differential conformance mode (--conformance): a ClickHouse
// stack and a duck stack over one shared metadata (so mappings are identical)
// are fed the identical stream, and both apis' decoded answers are compared
// with ClickHouse as the reference.
//
// Seeding is in-process: the go client stamps packets with its own hostname
// (_h), so one process feeding both agents makes max_host values identical
// across backends. The sequence replays drivers/go/main.go.tmpl.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	statshouse "github.com/VKCOM/statshouse-go"
)

// conformanceClientTag keeps conformance metric names apart from the
// per-client streams'.
const conformanceClientTag = "conf"

// conformanceSeedLead is how many seconds before Base the cold-start seeds
// land, matching the go driver template's SeedTS.
const conformanceSeedLead = 60

// conformanceTimeout bounds one request's poll for a non-empty reference and
// agreement; data lands ~24s after the writes.
const conformanceTimeout = 120 * time.Second

const conformancePollInterval = 3 * time.Second

// confDivergenceSamples is how many consecutive disagreeing polls (with a
// non-empty reference) confirm a divergence. Once the reference has data a
// disagreement will not heal, so failing fast keeps a red run within --timeout.
const confDivergenceSamples = 3

// statshouseGoEmptyAddrErr is the spurious Close error of a single-address
// statshouse-go v0.5.17 client (see seedConformanceStream).
const statshouseGoEmptyAddrErr = "empty statshouse address"

// Stack tags (daemonStackOpts.stackTag) for the two conformance stacks.
const (
	confStackCH   = "ch"
	confStackDuck = "duck"
)

// --- request set ------------------------------------------------------

// confKind is which API endpoint a conformance request hits.
type confKind int

const (
	confSeries    confKind = iota // GET /api/query (incl. PromQL-shaped and host-column forms)
	confTable                     // GET /api/table
	confPoint                     // GET /api/point
	confTagValues                 // GET /api/metric-tag-values
)

// confRequest is one request issued verbatim to both apis. qw selects the
// tolerance; it is not necessarily a query param.
type confRequest struct {
	kind  confKind
	label string // human-readable, e.g. "series/c_tagged/count"
	qw    string
	path  string // URL path+query, identical for both backends
}

// buildConformanceRequests derives the request set from one stream: series
// per metric and function, plus host-column, month-LOD, table, point, PromQL
// and tag-values shapes. Metrics are found by their unique generator suffix.
func buildConformanceRequests(stream metricStream) []confRequest {
	var reqs []confRequest
	base := stream.Base

	// Same param shape as the model-verified asserter (metricQueryURL).
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			path := strings.TrimPrefix(metricQueryURL("", m.Name, m.QBKeys, qf.qw, base), "http://")
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "series/" + confShortName(m.Name) + "/" + qf.qw,
				qw:    qf.qw,
				path:  path,
			})
		}
	}

	// Exact count/sum companions for every value_p metric: when a percentile
	// diverges, these tell different ingested data from different quantile
	// folding.
	for _, m := range stream.Metrics {
		if m.Kind != kindValueP {
			continue
		}
		for _, qw := range []string{"count", "sum"} {
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "series-exact/" + confShortName(m.Name) + "/" + qw,
				qw:    qw,
				path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
					q.Set("qw", qw)
				}),
			})
		}
	}

	// Host-column variants.
	if m, ok := confMetric(stream, "c_tagged"); ok {
		reqs = append(reqs, confRequest{
			kind:  confSeries,
			label: "series-mh/c_tagged/count",
			qw:    "count",
			path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
				q.Set("qw", "count")
				q.Set("mh", "1")
			}),
		})
	}
	if m, ok := confMetric(stream, "v_mix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confSeries,
			label: "series-host/v_mix/max_count_host",
			qw:    "max_count_host",
			path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
				q.Set("qw", "max_count_host")
			}),
		})
	}

	// Month-LOD series: the only step whose bucket timestamps depend on the
	// timezone, so a zone bug shows up in the time axis alone. The range starts
	// a month early: a month point exists only where its start is in range.
	if m, ok := confMetric(stream, "c_tagged"); ok {
		reqs = append(reqs, confRequest{
			kind:  confSeries,
			label: "series-month/c_tagged/count",
			qw:    "count",
			path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
				q.Set("qw", "count")
				q.Set("w", "1M")
				q.Set("f", strconv.FormatUint(uint64(base)-32*24*3600, 10))
			}),
		})
	}

	// Table view.
	if m, ok := confMetric(stream, "v_mix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confTable,
			label: "table/v_mix/sum",
			qw:    "sum",
			path:  confTablePath(m.Name, m.QBKeys, base, "sum", 0),
		})
	}
	if m, ok := confMetric(stream, "c_matrix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confTable,
			label: "table/c_matrix/count",
			qw:    "count",
			path:  confTablePath(m.Name, m.QBKeys, base, "count", 10000),
		})
	}

	// Point view over the last bucket.
	pointReq := func(suffix, qw string) {
		if m, ok := confMetric(stream, suffix); ok {
			reqs = append(reqs, confRequest{
				kind:  confPoint,
				label: "point/" + suffix + "/" + qw,
				qw:    qw,
				path: confPointPath(m.Name, m.QBKeys, base, func(q url.Values) {
					q.Set("qw", qw)
				}),
			})
		}
	}
	pointReq("c_tagged", "count")
	pointReq("vp_mix", "p90")
	pointReq("u_exact", "unique")

	// PromQL multi-metric regex, as the UI issues. Label names must stay
	// unquoted: statshouse's parser rejects Prometheus's `"__name__"=~…` form.
	if multi, ok := confMetric(stream, "c_multi"); ok {
		if matrix, ok := confMetric(stream, "c_matrix"); ok {
			promql := fmt.Sprintf(`{__name__=~"%s|%s",__what__="count",__by__="0"}`, multi.Name, matrix.Name)
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "promql/c_multi|c_matrix/count-by-0",
				qw:    "count",
				path: confRangePath(base, func(q url.Values) {
					q.Set("q", promql)
				}),
			})
		}
	}

	// Tag values.
	for _, suffix := range []string{"c_matrix", "u_exact"} {
		if m, ok := confMetric(stream, suffix); ok {
			q := url.Values{}
			q.Set("s", m.Name)
			q.Set("f", strconv.FormatUint(uint64(base), 10))
			q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
			q.Set("k", "0")
			q.Set("n", "1000")
			reqs = append(reqs, confRequest{
				kind:  confTagValues,
				label: "tag-values/" + suffix + "/k0",
				qw:    "count",
				path:  "/api/metric-tag-values?" + q.Encode(),
			})
		}
	}

	return reqs
}

// confMetric finds a stream metric by its generator suffix (c_tagged, v_mix…).
func confMetric(stream metricStream, suffix string) (metricModel, bool) {
	for _, m := range stream.Metrics {
		if strings.HasSuffix(m.Name, suffix) {
			return m, true
		}
	}
	return metricModel{}, false
}

// confShortName strips the run-id/client prefix for labels, cutting at the
// client tag so v_mix and vp_mix do not both become "mix".
func confShortName(name string) string {
	if i := strings.LastIndex(name, "_"+conformanceClientTag+"_"); i >= 0 {
		return name[i+len(conformanceClientTag)+2:]
	}
	if i := strings.LastIndexByte(name, '_'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// confQueryPath builds GET /api/query with metricQueryURL's param shape plus
// whatever set adds.
func confQueryPath(name string, qb []string, base uint32, set func(q url.Values)) string {
	return confRangePath(base, func(q url.Values) {
		q.Set("s", name)
		set(q)
		for _, k := range qb {
			q.Add("qb", k)
		}
	})
}

// confRangePath builds /api/query over the stream's buckets at w=1s, with
// ac=1 to bypass the query cache.
func confRangePath(base uint32, set func(q url.Values)) string {
	q := url.Values{}
	q.Set("f", strconv.FormatUint(uint64(base), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	set(q)
	return "/api/query?" + q.Encode()
}

// confTablePath builds GET /api/table; n caps rows (0 keeps the API default).
func confTablePath(name string, qb []string, base uint32, qw string, n int) string {
	p := confQueryPath(name, qb, base, func(q url.Values) {
		q.Set("qw", qw)
		if n > 0 {
			q.Set("n", strconv.Itoa(n))
		}
	})
	return strings.Replace(p, "/api/query", "/api/table", 1)
}

// confPointPath builds GET /api/point over the stream's last bucket.
func confPointPath(name string, qb []string, base uint32, set func(q url.Values)) string {
	q := url.Values{}
	q.Set("s", name)
	q.Set("f", strconv.FormatUint(uint64(base+numBuckets-1), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	set(q)
	for _, k := range qb {
		q.Add("qb", k)
	}
	return "/api/point?" + q.Encode()
}

// --- decoders ----------------------------------------------------------

// confSeriesResp decodes GET /api/query. A null point decodes to 0, which is
// safe to compare since every expected value is non-zero.
type confSeriesResp struct {
	Data struct {
		Series            confSeriesData `json:"series"`
		SamplingFactorSrc float64        `json:"sampling_factor_src"`
		SamplingFactorAgg float64        `json:"sampling_factor_agg"`
	} `json:"data"`
}

type confSeriesData struct {
	Time       []int64          `json:"time"`
	SeriesMeta []confSeriesMeta `json:"series_meta"`
	SeriesData [][]float64      `json:"series_data"`
}

type confSeriesMeta struct {
	Tags     map[string]apiMetaTag `json:"tags"`
	MaxHosts []string              `json:"max_hosts"`
}

// confTableResp decodes GET /api/table.
type confTableResp struct {
	Data struct {
		Rows []struct {
			Time int64                 `json:"time"`
			Data []float64             `json:"data"`
			Tags map[string]apiMetaTag `json:"tags"`
		} `json:"rows"`
		What []string `json:"what"`
		More bool     `json:"more"`
	} `json:"data"`
}

// confPointResp decodes GET /api/point.
type confPointResp struct {
	Data struct {
		PointMeta []struct {
			Tags    map[string]apiMetaTag `json:"tags"`
			MaxHost string                `json:"max_host"`
			FromSec int64                 `json:"from_sec"`
			ToSec   int64                 `json:"to_sec"`
		} `json:"point_meta"`
		PointData []float64 `json:"point_data"`
	} `json:"data"`
}

// confTagValuesResp decodes GET /api/metric-tag-values.
type confTagValuesResp struct {
	Data struct {
		TagValues []struct {
			Value string  `json:"value"`
			Count float64 `json:"count"`
		} `json:"tag_values"`
		TagValuesMore bool `json:"tag_values_more"`
	} `json:"data"`
}

// confDecoded holds a decoded response; only the request kind's field is set.
type confDecoded struct {
	series *confSeriesResp
	table  *confTableResp
	point  *confPointResp
	tags   *confTagValuesResp
}

// --- comparators --------------------------------------------------------

// confSumOrderTol is the relative sum/avg band for the cross-store
// comparison: the two stores add the same floats in different orders and
// differ in the last ulps, while real divergences are orders of magnitude
// larger.
const confSumOrderTol = 1e-9

// confValueMatches applies the suite's tolerances with ref (CH) as truth:
// exact by default, confSumOrderTol for sum/avg, the percentile band, and the
// approximate band for unique above uniquesHashMaxSize.
func confValueMatches(qw string, ref, got float64) bool {
	switch qw {
	case "p50", "p90", "p99":
		return withinAbsTol(got, ref, percentileTol, percentileMinAbs)
	case "sum", "avg":
		return withinRelTol(got, ref, confSumOrderTol)
	case "unique":
		if ref > uniquesHashMaxSize {
			return withinRelTol(got, ref, uniqueApproxTol)
		}
		return got == ref
	default:
		return got == ref
	}
}

// compareConfSeries compares two /api/query replies: zero sampling factors,
// equal time axes and series sets (by tagSignature), values under
// confValueMatches, and exactly equal max_hosts.
func compareConfSeries(ref, got *confSeriesResp, qw string) []string {
	var diffs []string
	for _, r := range []struct {
		name string
		resp *confSeriesResp
	}{{"reference", ref}, {"duck", got}} {
		if s := r.resp.Data.SamplingFactorSrc + r.resp.Data.SamplingFactorAgg; s != 0 {
			diffs = append(diffs, fmt.Sprintf("%s: sampling_factor_src+agg=%g (expected 0 — data was sampled)", r.name, s))
		}
	}
	if !equalInt64s(ref.Data.Series.Time, got.Data.Series.Time) {
		diffs = append(diffs, fmt.Sprintf("time axes differ: reference %d points, duck %d points", len(ref.Data.Series.Time), len(got.Data.Series.Time)))
	}
	refIdx := indexConfSeries(ref)
	gotIdx := indexConfSeries(got)
	for _, sig := range sortedKeys(refIdx) {
		rs := refIdx[sig]
		gs, ok := gotIdx[sig]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("series{%s} present in reference but absent in duck", sig))
			continue
		}
		for j, ts := range ref.Data.Series.Time {
			rv, gv := atFloat(rs.data, j), atFloat(gs.data, j)
			if !confValueMatches(qw, rv, gv) {
				diffs = append(diffs, fmt.Sprintf("series{%s} bucket=%d reference=%s duck=%g", sig, ts, formatConfRef(qw, rv), gv))
			}
		}
		if strings.Join(rs.hosts, "|") != strings.Join(gs.hosts, "|") {
			diffs = append(diffs, fmt.Sprintf("series{%s} max_hosts reference=%q duck=%q", sig, rs.hosts, gs.hosts))
		}
	}
	for _, sig := range sortedKeys(gotIdx) {
		if _, ok := refIdx[sig]; !ok {
			diffs = append(diffs, fmt.Sprintf("series{%s} present in duck but absent in reference", sig))
		}
	}
	return diffs
}

type confSeriesRow struct {
	data  []float64
	hosts []string
}

// indexConfSeries keys series by tagSignature. Series of different metrics can
// share one (a multi-metric PromQL selector without the name label), and the
// API orders them by a per-process metric order, so such a group is ranked by
// its values and keyed signature#rank.
func indexConfSeries(r *confSeriesResp) map[string]confSeriesRow {
	groups := map[string][]confSeriesRow{}
	for i, meta := range r.Data.Series.SeriesMeta {
		var row confSeriesRow
		if i < len(r.Data.Series.SeriesData) {
			row.data = r.Data.Series.SeriesData[i]
		}
		row.hosts = meta.MaxHosts
		sig := tagSignature(meta.Tags)
		groups[sig] = append(groups[sig], row)
	}
	out := make(map[string]confSeriesRow, len(r.Data.Series.SeriesMeta))
	for sig, rows := range groups {
		if len(rows) == 1 {
			out[sig] = rows[0]
			continue
		}
		sort.Slice(rows, func(a, b int) bool { return slices.Compare(rows[a].data, rows[b].data) < 0 })
		for i, row := range rows {
			out[fmt.Sprintf("%s#%d", sig, i)] = row
		}
	}
	return out
}

// compareConfTable compares two /api/table replies order-insensitively, rows
// keyed by (bucket, tagSignature), cells under confValueMatches.
func compareConfTable(ref, got *confTableResp, qw string) []string {
	var diffs []string
	if strings.Join(ref.Data.What, ",") != strings.Join(got.Data.What, ",") {
		diffs = append(diffs, fmt.Sprintf("what columns differ: reference %v duck %v", ref.Data.What, got.Data.What))
	}
	if ref.Data.More != got.Data.More {
		diffs = append(diffs, fmt.Sprintf("truncation flag differs: reference more=%v duck more=%v", ref.Data.More, got.Data.More))
	}
	type rowKey struct {
		ts  int64
		sig string
	}
	refRows := make(map[rowKey][]float64, len(ref.Data.Rows))
	for _, r := range ref.Data.Rows {
		refRows[rowKey{r.Time, tagSignature(r.Tags)}] = r.Data
	}
	gotRows := make(map[rowKey][]float64, len(got.Data.Rows))
	for _, r := range got.Data.Rows {
		gotRows[rowKey{r.Time, tagSignature(r.Tags)}] = r.Data
	}
	for k, rd := range refRows {
		gd, ok := gotRows[k]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} present in reference but absent in duck", k.sig, k.ts))
			continue
		}
		if len(rd) != len(gd) {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} cell count reference=%d duck=%d", k.sig, k.ts, len(rd), len(gd)))
			continue
		}
		for i, rv := range rd {
			if !confValueMatches(qw, rv, gd[i]) {
				diffs = append(diffs, fmt.Sprintf("table row{%s,%d} cell=%d reference=%s duck=%g", k.sig, k.ts, i, formatConfRef(qw, rv), gd[i]))
			}
		}
	}
	for k := range gotRows {
		if _, ok := refRows[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} present in duck but absent in reference", k.sig, k.ts))
		}
	}
	return diffs
}

// compareConfPoint compares two /api/point replies keyed by tags, host and
// range.
func compareConfPoint(ref, got *confPointResp, qw string) []string {
	var diffs []string
	if len(ref.Data.PointMeta) != len(got.Data.PointMeta) {
		return append(diffs, fmt.Sprintf("point count differs: reference=%d duck=%d", len(ref.Data.PointMeta), len(got.Data.PointMeta)))
	}
	type pointKey struct {
		sig, host string
		from, to  int64
	}
	refPts := make(map[pointKey]float64, len(ref.Data.PointMeta))
	for i, m := range ref.Data.PointMeta {
		var v float64
		if i < len(ref.Data.PointData) {
			v = ref.Data.PointData[i]
		}
		refPts[pointKey{tagSignature(m.Tags), m.MaxHost, m.FromSec, m.ToSec}] = v
	}
	for i, m := range got.Data.PointMeta {
		var v float64
		if i < len(got.Data.PointData) {
			v = got.Data.PointData[i]
		}
		k := pointKey{tagSignature(m.Tags), m.MaxHost, m.FromSec, m.ToSec}
		rv, ok := refPts[k]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("point{%s host=%q from=%d to=%d} present in duck but absent in reference", k.sig, k.host, k.from, k.to))
			continue
		}
		if !confValueMatches(qw, rv, v) {
			diffs = append(diffs, fmt.Sprintf("point{%s} reference=%s duck=%g", k.sig, formatConfRef(qw, rv), v))
		}
		delete(refPts, k)
	}
	for k := range refPts {
		diffs = append(diffs, fmt.Sprintf("point{%s host=%q from=%d to=%d} present in reference but absent in duck", k.sig, k.host, k.from, k.to))
	}
	return diffs
}

// compareConfTagValues compares two /api/metric-tag-values replies exactly,
// order-insensitively.
func compareConfTagValues(ref, got *confTagValuesResp) []string {
	var diffs []string
	if ref.Data.TagValuesMore != got.Data.TagValuesMore {
		diffs = append(diffs, fmt.Sprintf("tag_values_more differs: reference=%v duck=%v", ref.Data.TagValuesMore, got.Data.TagValuesMore))
	}
	key := func(v string, c float64) string { return v + "\x00" + strconv.FormatFloat(c, 'g', -1, 64) }
	refSet := make(map[string]string, len(ref.Data.TagValues))
	for _, tv := range ref.Data.TagValues {
		refSet[key(tv.Value, tv.Count)] = tv.Value
	}
	gotSet := make(map[string]string, len(got.Data.TagValues))
	for _, tv := range got.Data.TagValues {
		gotSet[key(tv.Value, tv.Count)] = tv.Value
	}
	for k, v := range refSet {
		if _, ok := gotSet[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("tag value{%q} with reference count absent in duck", v))
		}
	}
	for k, v := range gotSet {
		if _, ok := refSet[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("tag value{%q} present in duck but absent in reference", v))
		}
	}
	return diffs
}

func compareConfRequest(req confRequest, ref, got confDecoded) []string {
	switch req.kind {
	case confSeries:
		return compareConfSeries(ref.series, got.series, req.qw)
	case confTable:
		return compareConfTable(ref.table, got.table, req.qw)
	case confPoint:
		return compareConfPoint(ref.point, got.point, req.qw)
	case confTagValues:
		return compareConfTagValues(ref.tags, got.tags)
	}
	return []string{fmt.Sprintf("unknown request kind %d", req.kind)}
}

// confNonEmpty reports whether the reference carries data, guarding against
// vacuous passes.
func confNonEmpty(kind confKind, dec confDecoded) bool {
	switch kind {
	case confSeries:
		s := dec.series.Data.Series
		return len(s.Time) > 0 && len(s.SeriesMeta) > 0 && len(s.SeriesData) > 0
	case confTable:
		return len(dec.table.Data.Rows) > 0
	case confPoint:
		return len(dec.point.Data.PointMeta) > 0 && len(dec.point.Data.PointData) > 0
	case confTagValues:
		return len(dec.tags.Data.TagValues) > 0
	}
	return false
}

// formatConfRef renders a reference value for a diff line, with its band.
func formatConfRef(qw string, ref float64) string {
	switch qw {
	case "p50", "p90", "p99":
		return fmt.Sprintf("%g±max(%g%%,1)", ref, percentileTol*100)
	case "unique":
		if ref > uniquesHashMaxSize {
			return fmt.Sprintf("≈%g±%g%%", ref, uniqueApproxTol*100)
		}
	}
	return strconv.FormatFloat(ref, 'g', -1, 64)
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func atFloat(v []float64, i int) float64 {
	if i < len(v) {
		return v[i]
	}
	return 0
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- live: fetch + differential driver ---------------------------------

func fetchConf(ctx context.Context, apiAddr string, req confRequest) (dec confDecoded, body string, status int, err error) {
	body, status, err = httpGet(ctx, "http://"+apiAddr+req.path)
	if err != nil {
		return dec, body, status, err
	}
	if status != 200 {
		return dec, body, status, fmt.Errorf("HTTP %d: %s", status, truncate(strings.TrimSpace(body), 300))
	}
	unmarshal := func(target any) error {
		if jerr := json.Unmarshal([]byte(body), target); jerr != nil {
			return fmt.Errorf("parse response: %w; body: %s", jerr, truncate(body, 300))
		}
		return nil
	}
	switch req.kind {
	case confSeries:
		dec.series = &confSeriesResp{}
		err = unmarshal(dec.series)
	case confTable:
		dec.table = &confTableResp{}
		err = unmarshal(dec.table)
	case confPoint:
		dec.point = &confPointResp{}
		err = unmarshal(dec.point)
	case confTagValues:
		dec.tags = &confTagValuesResp{}
		err = unmarshal(dec.tags)
	}
	return dec, body, status, err
}

// runConformanceDifferential polls each request until the reference is
// non-empty and both answers agree, or confDivergenceSamples consecutive
// disagreements confirm a divergence (recorded with both raw responses).
// cancelled reports that requests were left unexecuted.
func runConformanceDifferential(ctx context.Context, rec *recorder, chAPI, duckAPI string, reqs []confRequest) (passed, failed int, cancelled bool) {
	for _, req := range reqs {
		var (
			diffs       []string
			refBody     string
			gotBody     string
			lastStatus  int
			refSeen     bool
			stableDiffs int
		)
		deadline := time.Now().Add(conformanceTimeout)
		done := false
		for !done {
			if cerr := ctx.Err(); cerr != nil {
				// The run is tearing down; not a divergence.
				return passed, failed, true
			}
			ref, rb, _, rerr := fetchConf(ctx, chAPI, req)
			refBody = rb
			if rerr == nil && confNonEmpty(req.kind, ref) {
				refSeen = true
				got, gb, status, gerr := fetchConf(ctx, duckAPI, req)
				gotBody, lastStatus = gb, status
				switch {
				case gerr != nil:
					diffs = []string{fmt.Sprintf("duck query error: %v", gerr)}
					stableDiffs = 0
				default:
					diffs = compareConfRequest(req, ref, got)
					if len(diffs) == 0 {
						done = true
						break
					}
					stableDiffs++
					if stableDiffs >= confDivergenceSamples {
						done = true
					}
				}
			} else if rerr != nil {
				diffs = []string{fmt.Sprintf("reference query error: %v", rerr)}
				stableDiffs = 0
			} else {
				diffs = []string{"reference response carries no data yet (conveyor has not landed it)"}
				stableDiffs = 0
			}
			if !done {
				if !time.Now().Add(conformancePollInterval).Before(deadline) {
					done = true
				} else {
					select {
					case <-ctx.Done():
						return passed, failed, true
					case <-time.After(conformancePollInterval):
					}
				}
			}
		}
		if len(diffs) == 0 {
			passed++
			rec.logf("PASS conformance %s", req.label)
			fmt.Printf("PASS conformance %s\n", req.label)
			continue
		}
		failed++
		rec.recordFailedQuery(failedQuery{
			Label:      "conformance",
			Client:     conformanceClientTag,
			Func:       req.label,
			URL:        "http://" + duckAPI + req.path,
			HTTPStatus: lastStatus,
			Body:       "REFERENCE (clickhouse):\n" + refBody + "\n\nUNDER TEST (duck):\n" + gotBody,
		})
		detail := strings.Join(diffs, "\n")
		if !refSeen {
			detail += "\n(the ClickHouse reference never became non-empty — see its own PASS lines above)"
		}
		rec.logf("FAIL conformance %s\n%s", req.label, detail)
		fmt.Printf("FAIL conformance %s\n%s\n", req.label, indent(detail))
	}
	return passed, failed, false
}

// --- live: in-process seeding -------------------------------------------

// newConformanceClient builds a client for one agent. MaxBucketSize: the
// default 1024 reservoir would sample the big-unique bucket client-side.
func newConformanceClient(agentAddr string) *statshouse.Client {
	return statshouse.NewClientEx(statshouse.ConfigureArgs{
		StatsHouseAddr: agentAddr,
		Network:        "tcp",
		MaxBucketSize:  1 << 18,
	})
}

// confNamedTags keeps empty values, leaving the drop to the client as the
// driver template does.
func confNamedTags(tags []tag) statshouse.NamedTags {
	out := make(statshouse.NamedTags, 0, len(tags))
	for _, t := range tags {
		out = append(out, [2]string{t.Key, t.Val})
	}
	return out
}

// confSeedMetric sends a cold-start seed with the kind-matching write so
// auto-create derives the right kind.
func confSeedMetric(cl *statshouse.Client, s seedDef, ts uint32) {
	switch s.Kind {
	case kindValue:
		cl.NamedValueHistoric(s.Name, nil, 1, ts)
	case kindUnique:
		cl.NamedUniqueHistoric(s.Name, nil, 1, ts)
	default:
		cl.NamedCountHistoric(s.Name, nil, 1, ts)
	}
}

// confWrite replays one write with the driver template's dispatch and burst
// pacing; value_p/unique payloads are regenerated deterministically.
func confWrite(cl *statshouse.Client, w metricWrite) error {
	tags := confNamedTags(w.Tags)
	switch w.Kind {
	case kindCounter, kindStag:
		cl.NamedCountHistoric(w.Metric, tags, w.Count, w.TS)
	case kindValue:
		cl.NamedValuesHistoric(w.Metric, tags, w.Values, w.TS)
	case kindValueNaN:
		cl.NamedValuesHistoric(w.Metric, tags, []float64{math.NaN()}, w.TS)
	case kindValueInf:
		cl.NamedValuesHistoric(w.Metric, tags, []float64{math.Inf(1)}, w.TS)
	case kindValueP:
		var v []float64
		switch w.Gen.Kind {
		case genKindValueUniform:
			v = genValueUniform(w.Gen.N)
		case genKindValueSkewed:
			v = genValueSkewed(w.Gen.N)
		default:
			return fmt.Errorf("value_p write with unknown generator %q", w.Gen.Kind)
		}
		cl.NamedValuesHistoric(w.Metric, tags, v, w.TS)
		time.Sleep(2 * time.Millisecond)
	case kindUnique:
		var u []int64
		switch w.Gen.Kind {
		case genKindUniqueDistinct:
			u = genUniqueDistinct(w.Gen.N)
		case genKindUniqueDedup:
			u = make([]int64, w.Gen.N*2)
			for i := range u {
				u[i] = int64(i%w.Gen.N) + 1
			}
		default:
			return fmt.Errorf("unique write with unknown generator %q", w.Gen.Kind)
		}
		cl.NamedUniquesHistoric(w.Metric, tags, u, w.TS)
		if w.Gen.Kind == genKindUniqueDistinct {
			time.Sleep(50 * time.Millisecond)
		}
	default:
		return fmt.Errorf("unhandled write kind %q", w.Kind)
	}
	return nil
}

type metricsListNames struct {
	Data struct {
		Metrics []struct {
			Name string `json:"name"`
		} `json:"metrics"`
	} `json:"data"`
}

// waitConformanceMetrics blocks until /api/metrics-list reports every name,
// so mappings exist before the counted writes.
func waitConformanceMetrics(ctx context.Context, apiAddr string, names []string) error {
	return poll(ctx, 60*time.Second, 500*time.Millisecond, func() (bool, error) {
		body, status, err := httpGet(ctx, "http://"+apiAddr+"/api/metrics-list?full=1")
		if err != nil || status != 200 {
			return false, nil
		}
		var r metricsListNames
		if jerr := json.Unmarshal([]byte(body), &r); jerr != nil {
			return false, nil
		}
		have := make(map[string]bool, len(r.Data.Metrics))
		for _, m := range r.Data.Metrics {
			have[m.Name] = true
		}
		for _, n := range names {
			if !have[n] {
				return false, nil
			}
		}
		return true, nil
	})
}

// seedConformanceStream feeds the identical stream to both agents, mirroring
// drivers/go/main.go.tmpl: seeds, metrics-list poll, settle, writes, Close.
func seedConformanceStream(ctx context.Context, rec *recorder, stream metricStream, apiAddrs, agentAddrs [2]string) error {
	if len(agentAddrs) != 2 || len(apiAddrs) != 2 {
		return fmt.Errorf("conformance seeding needs exactly 2 agents and 2 apis")
	}
	ch := newConformanceClient(agentAddrs[0])
	dk := newConformanceClient(agentAddrs[1])

	seeds, names := streamSeeds(stream)
	seedTS := stream.Base - conformanceSeedLead
	for _, s := range seeds {
		confSeedMetric(ch, s, seedTS)
		confSeedMetric(dk, s, seedTS)
	}
	rec.logf("conformance: seeded %d metric(s) to both agents (seed ts=%d)", len(seeds), seedTS)

	for i, api := range apiAddrs {
		if err := waitConformanceMetrics(ctx, api, names); err != nil {
			return fmt.Errorf("seeded metrics never appeared via api %s: %w", api, err)
		}
		rec.logf("conformance: all %d metric(s) visible via api %d (%s)", len(names), i+1, api)
	}
	time.Sleep(2 * time.Second) // mapping-cache settle (driver parity)

	for i, w := range stream.Writes {
		if err := confWrite(ch, w); err != nil {
			return fmt.Errorf("write %d (%s/%s) to ch agent: %w", i, w.Metric, w.Kind, err)
		}
		if err := confWrite(dk, w); err != nil {
			return fmt.Errorf("write %d (%s/%s) to duck agent: %w", i, w.Metric, w.Kind, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	rec.logf("conformance: replayed %d write(s) to both agents", len(stream.Writes))

	for name, cl := range map[string]*statshouse.Client{"ch": ch, "duck": dk} {
		if err := cl.Close(); err != nil {
			// statshouse-go v0.5.17 gives a single-address client an empty
			// secondary connection whose Close reports errEmptyAddr after a clean
			// flush. A primary failure is returned first, so it still surfaces.
			if err.Error() != statshouseGoEmptyAddrErr {
				return fmt.Errorf("%s client close: %w", name, err)
			}
			rec.logf("conformance: %s client closed with the known v0.5.17 single-address quirk (empty secondary pool); primary flushed cleanly", name)
		}
	}
	time.Sleep(time.Second) // drain settle (driver parity)
	return nil
}

// --- live: phase driver --------------------------------------------------

type conformancePhaseOpts struct {
	runID     string
	chAPI     string
	duckAPI   string
	chAgent   string
	duckAgent string
}

// runConformancePhase replaces the client phase under --conformance: seed
// both stacks, check the CH reference against the model (a broken reference
// aborts rather than blessing a coincidence), then run the differential.
func runConformancePhase(ctx context.Context, rec *recorder, o conformancePhaseOpts) (passed, failed int, cancelled bool) {
	stream := generateStream(o.runID, conformanceClientTag, time.Now())

	if err := createValuePMetrics(ctx, rec, o.chAPI, stream); err != nil {
		rec.logf("FAIL conformance: pre-create value_p metrics: %v", err)
		fmt.Printf("FAIL conformance pre-create value_p metrics: %v\n", err)
		return 0, 1, false
	}

	if err := seedConformanceStream(ctx, rec, stream, [2]string{o.chAPI, o.duckAPI}, [2]string{o.chAgent, o.duckAgent}); err != nil {
		rec.logf("FAIL conformance: seed stream: %v", err)
		fmt.Printf("FAIL conformance seed stream: %v\n", err)
		return 0, 1, false
	}

	// ClickHouse must match the model before it is trusted as the reference.
	refPass, refFail := assertStream(ctx, rec, o.chAPI, conformanceClientTag, stream)
	if refFail > 0 {
		rec.logf("FAIL conformance: the ClickHouse reference does not match the expected model (%d assertion(s) failed) — aborting the differential; the reference itself is broken", refFail)
		fmt.Printf("FAIL conformance reference gate: %d CH assertion(s) failed; differential aborted\n", refFail)
		return refPass, refFail, false
	}

	reqs := buildConformanceRequests(stream)
	rec.logf("conformance: comparing %d semantic request(s) between clickhouse (reference) and duck", len(reqs))
	diffPass, diffFail, cancelled := runConformanceDifferential(ctx, rec, o.chAPI, o.duckAPI, reqs)
	return refPass + diffPass, diffFail, cancelled
}
