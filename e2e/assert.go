package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Stream assertions: poll /api/query per (metric, query function) until the
// expected series appear, then compare per bucket and per series — exactly,
// or within a tolerance band for percentiles and the big-unique estimate.

// assertTimeout is the poll window for one (metric, func). The conveyor takes
// ~24s end-to-end; the rest is headroom for auto-create and the big-unique flush.
const assertTimeout = 60 * time.Second

// value_p tolerance band (see withinAbsTol). The t-digest error is ~1/compression
// in quantile space, inside 1% on a flat density. On the skewed generator the
// inverse CDF is steep (dv/dq ≈ 1000 at p50), so quantile-space error grows past
// 1% in value space (observed up to ~2%); skewed series get percentileSkewTol.
const (
	percentileTol     = 0.01
	percentileSkewTol = 0.05
	percentileMinAbs  = 1.0
)

// uniqueApproxTol is the big-unique relative band (~4σ of ChUnique's estimator).
const uniqueApproxTol = 0.02

// apiSeriesResponse mirrors the used slice of the API's query reply. A missing
// point (JSON null) decodes as 0, which no expected value is.
type apiSeriesResponse struct {
	Data apiResponseData `json:"data"`
}

type apiResponseData struct {
	Series            apiSeries `json:"series"`
	SamplingFactorSrc float64   `json:"sampling_factor_src"`
	SamplingFactorAgg float64   `json:"sampling_factor_agg"`
}

type apiSeries struct {
	Time       []int64         `json:"time"`
	SeriesMeta []apiSeriesMeta `json:"series_meta"`
	SeriesData [][]float64     `json:"series_data"`
}

type apiSeriesMeta struct {
	Tags map[string]apiMetaTag `json:"tags"`
}

type apiMetaTag struct {
	Value string `json:"value"`
}

// queryFunc is one query function a metric is asserted with; q is the quantile
// for percentile functions.
type queryFunc struct {
	qw    string
	q     float64
	label string
}

// funcsFor returns the query functions a metric kind is asserted with.
func funcsFor(kind string) []queryFunc {
	switch kind {
	case kindCounter, kindStag:
		// stag asserts cardinality; counter asserts count.
		return []queryFunc{{qw: qwFor(kind), label: qwFor(kind)}}
	case kindValue:
		return []queryFunc{
			{qw: "sum", label: "sum"},
			{qw: "min", label: "min"},
			{qw: "max", label: "max"},
			{qw: "avg", label: "avg"},
		}
	case kindValueP:
		return []queryFunc{
			{qw: "p50", q: 0.50, label: "p50"},
			{qw: "p90", q: 0.90, label: "p90"},
			{qw: "p99", q: 0.99, label: "p99"},
		}
	case kindUnique:
		return []queryFunc{{qw: "unique", label: "unique"}}
	}
	return nil
}

// qwFor maps a kind to its single-function query string (counter/stag).
func qwFor(kind string) string {
	if kind == kindStag {
		return "cardinality"
	}
	return "count"
}

// assertStream asserts every metric across its query functions, one PASS or
// FAIL line per (metric, func). A failure also prints the metric's ledger
// state, so a mismatch and its likely cause read together.
func assertStream(ctx context.Context, rec *recorder, apiAddr, clientTag string, stream metricStream) (passed, failed int) {
	want := ledgerWriteCounts(stream)
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			ok, detail := pollMetricFunc(ctx, rec, apiAddr, clientTag, m, stream.Base, want[m.Name], qf)
			switch {
			case ok:
				passed++
				rec.logf("PASS client=%s metric=%s qw=%s series=%d", clientTag, m.Name, qf.label, len(m.Series))
				fmt.Printf("PASS client=%s metric=%s qw=%s\n", clientTag, m.Name, qf.label)
			default:
				failed++
				rec.logf("FAIL client=%s metric=%s qw=%s\n%s", clientTag, m.Name, qf.label, detail)
				fmt.Printf("FAIL client=%s metric=%s qw=%s\n%s\n", clientTag, m.Name, qf.label, indent(detail))
			}
		}
	}
	return passed, failed
}

// pollMetricFunc queries one (metric, func) until it matches or assertTimeout
// elapses. On failure it records the raw response and appends the metric's
// ledger line to the returned detail.
func pollMetricFunc(ctx context.Context, rec *recorder, apiAddr, clientTag string, m metricModel, base uint32, sentWrites int, qf queryFunc) (bool, string) {
	qurl := metricQueryURL(apiAddr, m.Name, m.QBKeys, qf.qw, base)
	var (
		lastDetail string
		lastBody   string
		lastStatus int
	)
	if err := poll(ctx, assertTimeout, 2*time.Second, func() (bool, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		lastBody, lastStatus = body, status
		if qerr != nil {
			lastDetail = fmt.Sprintf("query error: %v\nurl: %s", qerr, qurl)
			return false, nil
		}
		mismatches, missing, extras, sampling := compareByFunc(m, resp, qf)
		// Sampled data fails even when the values happen to match.
		if len(mismatches) == 0 && len(missing) == 0 && len(extras) == 0 && sampling == 0 {
			return true, nil
		}
		lastDetail = formatFail(m.Name, qurl, qf, mismatches, missing, extras, sampling)
		return false, nil
	}); err != nil {
		// A cancelled run is not a mismatch; skip the failure diagnostics.
		if ctx.Err() != nil {
			return false, ""
		}
		// Timed out still mismatched.
		if rec != nil {
			rec.recordFailedQuery(failedQuery{
				Label: "value", Client: clientTag, Metric: m.Name, Func: qf.label,
				URL: qurl, HTTPStatus: lastStatus, Body: lastBody,
			})
			if rec.verbose {
				rec.dumpQueryResponse(clientTag, m.Name, qf.label, lastBody)
			}
		}
		lastDetail += "\n" + metricLedgerLine(ctx, apiAddr, base, m.Name, sentWrites)
		return false, lastDetail
	}
	// Under -v keep the response that satisfied the assertion.
	if rec != nil && rec.verbose {
		rec.dumpQueryResponse(clientTag, m.Name, qf.label, lastBody)
	}
	return true, ""
}

// metricLedgerLine fetches a diagnostic snapshot of one metric's ledger for a
// value-assertion failure; assertConservationLedger is authoritative.
// sentWrites==0 marks a metric outside the ledger's scope.
func metricLedgerLine(ctx context.Context, apiAddr string, base uint32, name string, sentWrites int) string {
	if sentWrites == 0 {
		return "ledger: not balance-checked (multi-value metric / no ledger-eligible writes)"
	}
	bd, _, err := fetchIngestionBreakdown(ctx, apiAddr, base+numBuckets, map[string]bool{name: true})
	if err != nil {
		return fmt.Sprintf("ledger: unavailable (%v)", err)
	}
	okCached, errSum, _, _ := ledgerBalance(bd[name])
	return formatLedgerLine(sentWrites, okCached, errSum)
}

// ledgerSnapshotCaveat marks an unbalanced inline ledger line as a snapshot
// taken before the ledger assertion converged.
const ledgerSnapshotCaveat = " (snapshot at assertion time; the ledger assertion is authoritative)"

// formatLedgerLine renders the conservation equation verdict for one metric.
func formatLedgerLine(sentWrites int, okCached, errSum float64) string {
	got := okCached + errSum
	switch {
	case got == float64(sentWrites):
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g == sentWrites=%d (balanced)", okCached, errSum, got, sentWrites)
	case got < float64(sentWrites):
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g < sentWrites=%d → %g unaccounted (silent loss)%s",
			okCached, errSum, got, sentWrites, float64(sentWrites)-got, ledgerSnapshotCaveat)
	default:
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g > sentWrites=%d → %g over-counted (double-counting)%s",
			okCached, errSum, got, sentWrites, got-float64(sentWrites), ledgerSnapshotCaveat)
	}
}

// metricQueryURL builds GET /api/query for one metric at 1s LOD over its
// buckets; an empty qb yields a single total series, and ac=1 bypasses the cache.
func metricQueryURL(apiAddr, name string, qb []string, qw string, base uint32) string {
	q := url.Values{}
	q.Set("s", name)
	q.Set("f", strconv.FormatUint(uint64(base), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	// A bare "1" means screen width 1 (auto-resolution); "1s" is a 1-second step.
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", qw)
	for _, k := range qb {
		q.Add("qb", k)
	}
	return "http://" + apiAddr + "/api/query?" + q.Encode()
}

// queryCounterRaw is queryCounter that also returns the raw body and HTTP
// status for the diagnostics artifacts, even on a parse error.
func queryCounterRaw(ctx context.Context, qurl string) (resp *apiSeriesResponse, body string, status int, err error) {
	body, status, err = httpGet(ctx, qurl)
	if err != nil {
		return nil, body, status, err
	}
	if status != 200 {
		return nil, body, status, fmt.Errorf("HTTP %d: %s", status, truncate(strings.TrimSpace(body), 300))
	}
	var r apiSeriesResponse
	if jerr := json.Unmarshal([]byte(body), &r); jerr != nil {
		return nil, body, status, fmt.Errorf("parse response: %w; body: %s", jerr, truncate(body, 300))
	}
	return &r, body, status, nil
}

func queryCounter(ctx context.Context, qurl string) (*apiSeriesResponse, error) {
	resp, _, _, err := queryCounterRaw(ctx, qurl)
	return resp, err
}

// indexResponse turns the API's per-series arrays into map[signature]map[bucket]value,
// the shape every comparator consumes. A missing data point unmarshals to 0.0.
func indexResponse(resp *apiSeriesResponse) map[string]map[uint32]float64 {
	out := make(map[string]map[uint32]float64, len(resp.Data.Series.SeriesMeta))
	for i, meta := range resp.Data.Series.SeriesMeta {
		data := resp.Data.Series.SeriesData[i]
		buckets := make(map[uint32]float64, len(resp.Data.Series.Time))
		for j, ts := range resp.Data.Series.Time {
			if j < len(data) {
				buckets[uint32(ts)] = data[j]
			}
		}
		out[tagSignature(meta.Tags)] = buckets
	}
	return out
}

// seriesMismatch is one (series, bucket) where expected ≠ actual (outside tol).
type seriesMismatch struct {
	seriesSig string
	bucket    uint32
	expected  string // formatted for readability (float or "≈N±tol")
	actual    float64
}

// compareByFunc dispatches to the kind's comparator; sampling must be 0.
func compareByFunc(m metricModel, resp *apiSeriesResponse, qf queryFunc) (mismatches []seriesMismatch, missing, extras []string, sampling float64) {
	switch {
	case qf.qw == "count":
		mismatches, missing, extras = compareCounts(m, resp)
	case qf.qw == "cardinality":
		mismatches, missing, extras = compareCardinality(m, resp)
	case qf.qw == "sum" || qf.qw == "min" || qf.qw == "max" || qf.qw == "avg":
		mismatches, missing, extras = compareValueAgg(m, resp, qf.qw)
	case qf.qw == "p50" || qf.qw == "p90" || qf.qw == "p99":
		mismatches, missing, extras = comparePercentile(m, resp, qf.q)
	case qf.qw == "unique":
		mismatches, missing, extras = compareUnique(m, resp)
	}
	sortMismatches(mismatches)
	sort.Strings(missing)
	sort.Strings(extras)
	sampling = resp.Data.SamplingFactorSrc + resp.Data.SamplingFactorAgg
	return mismatches, missing, extras, sampling
}

// compareCounts compares counts exactly per series, in both directions.
func compareCounts(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, exp := range es.Counts {
			if got[ts] != exp {
				mismatches = append(mismatches, seriesMismatch{sig, ts, strconv.FormatFloat(exp, 'g', -1, 64), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// compareCardinality asserts stag: with no group-by the API returns one series
// (signature "") whose value is the distinct-series count per bucket.
func compareCardinality(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	got, ok := actual[""]
	if !ok {
		// The total series is absent: every populated bucket is missing.
		for bucket := range stagBuckets(m) {
			missing = append(missing, fmt.Sprintf("cardinality total absent at bucket %d", bucket))
		}
		return mismatches, missing, extras
	}
	for bucket, expCount := range stagBuckets(m) {
		if got[bucket] != float64(expCount) {
			mismatches = append(mismatches, seriesMismatch{"(cardinality)", bucket, strconv.Itoa(expCount), got[bucket]})
		}
	}
	// Any series besides the "" total is unexpected.
	for sig := range actual {
		if sig != "" {
			extras = append(extras, sig)
		}
	}
	return mismatches, missing, extras
}

// stagBuckets maps every populated stag bucket to its expected series count.
func stagBuckets(m metricModel) map[uint32]int {
	out := map[uint32]int{}
	for _, es := range m.Series {
		for ts := range es.Counts {
			out[ts]++
		}
	}
	return out
}

// compareValueAgg compares value aggregates exactly: the model folds in write
// order like the agent, so the floats are bit-identical.
func compareValueAgg(m metricModel, resp *apiSeriesResponse, qw string) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, vals := range es.Values {
			exp := valueAggregate(vals, qw)
			if got[ts] != exp {
				mismatches = append(mismatches, seriesMismatch{sig, ts, strconv.FormatFloat(exp, 'g', -1, 64), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// comparePercentile checks each bucket's percentile against the true quantile
// of the sorted model values within the percentile tolerance band.
func comparePercentile(m metricModel, resp *apiSeriesResponse, q float64) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		tol := percentileTol
		if es.GenKind == genKindValueSkewed {
			tol = percentileSkewTol
		}
		for ts, vals := range es.Values {
			truth := quantile(vals, q) // vals stored sorted
			if !withinAbsTol(got[ts], truth, tol, percentileMinAbs) {
				mismatches = append(mismatches, seriesMismatch{sig, ts, fmt.Sprintf("≈%g±tol", truth), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// compareUnique compares uniques exactly up to ChUnique's exact threshold and
// within ±uniqueApproxTol above it.
func compareUnique(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, exp := range es.Uniques {
			approx := exp > uniquesHashMaxSize
			truth := float64(exp)
			match := !approx && got[ts] == truth
			if approx {
				match = withinRelTol(got[ts], truth, uniqueApproxTol)
			}
			if !match {
				note := strconv.FormatFloat(truth, 'g', -1, 64)
				if approx {
					note = fmt.Sprintf("≈%g±%g%%", truth, uniqueApproxTol*100)
				}
				mismatches = append(mismatches, seriesMismatch{sig, ts, note, got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// uniquesHashMaxSize is ChUnique's exact→approximate threshold, replicated to
// avoid importing data_model.
const uniquesHashMaxSize = 1 << 16

// valueAggregate computes the expected aggregate over vals in write order; avg
// is sum/len because the agent defaults count to len(values).
func valueAggregate(vals []float64, qw string) float64 {
	switch qw {
	case "min":
		m := math.Inf(1)
		for _, v := range vals {
			if v < m {
				m = v
			}
		}
		return m
	case "max":
		m := math.Inf(-1)
		for _, v := range vals {
			if v > m {
				m = v
			}
		}
		return m
	case "sum":
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s
	case "avg":
		if len(vals) == 0 {
			return 0
		}
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	}
	return 0
}

// extraSeries returns the API series not present in the expected want set.
func extraSeries(actual map[string]map[uint32]float64, want map[string]bool) []string {
	var extras []string
	for sig := range actual {
		if !want[sig] {
			extras = append(extras, sig)
		}
	}
	return extras
}

func sortMismatches(mm []seriesMismatch) {
	sort.Slice(mm, func(i, j int) bool {
		if mm[i].seriesSig != mm[j].seriesSig {
			return mm[i].seriesSig < mm[j].seriesSig
		}
		return mm[i].bucket < mm[j].bucket
	})
}

func formatFail(name, qurl string, qf queryFunc, mismatches []seriesMismatch, missing, extras []string, sampling float64) string {
	var b strings.Builder
	if sampling != 0 {
		fmt.Fprintf(&b, "sampling_factor_src+agg=%g (expected 0 — data was sampled)\n", sampling)
	}
	for _, mm := range mismatches {
		fmt.Fprintf(&b, "series{%s} bucket=%d expected=%s actual=%g\n", mm.seriesSig, mm.bucket, mm.expected, mm.actual)
	}
	for _, sig := range missing {
		fmt.Fprintf(&b, "series{%s} expected but absent in response\n", sig)
	}
	for _, sig := range extras {
		fmt.Fprintf(&b, "series{%s} present in response but not expected\n", sig)
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}

// tagSignature is the normalized identity of an API series: sorted "k=v" pairs
// without the _h host tag, empty values (rust/cpp send them verbatim) or the
// " 0" sentinel the API renders for an absent group-by position. No harness
// metric uses "0" as a real tag value.
func tagSignature(tags map[string]apiMetaTag) string {
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		if k == "_h" {
			continue
		}
		if v.Value == "" {
			continue
		}
		if strings.TrimSpace(v.Value) == "0" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+tags[k].Value)
	}
	return strings.Join(parts, ";")
}

// expectedSignature mirrors tagSignature for an expected series; the API keys
// series_meta by legacy tag ID ("key0".."key5").
func expectedSignature(tags []tag) string {
	cp := append([]tag(nil), tags...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Key < cp[j].Key })
	parts := make([]string, 0, len(cp))
	for _, t := range cp {
		parts = append(parts, "key"+t.Key+"="+t.Val)
	}
	return strings.Join(parts, ";")
}

// --- silent client-side loss tripwire (TCP backpressure) -------------

// clientWriteErrMetric is the builtin every client emits with the byte count
// it drops under TCP backpressure; without this tripwire that loss would show
// up only as unexplained low counts. Tag 1 is the client language.
const clientWriteErrMetric = "__src_client_write_err"

// clientWriteErrLang maps a driver tag to the language code its client writes
// at tag 1, which isolates one client's loss on the shared stack.
var clientWriteErrLang = map[string]string{
	"go":   "1",
	"rust": "3",
	"cpp":  "5",
}

// writeErrTimeout bounds the absence poll. It runs after the driver exits and
// the conveyor (~24s) is proven live, so 30s covers the remaining drain.
const writeErrTimeout = 30 * time.Second

// absenceQueryFunc is one absence-poll query, returning the largest value seen.
// A clean result, even all zeros, counts as an observation of absence.
type absenceQueryFunc func(ctx context.Context) (worst float64, err error)

// absenceOutcome is the result of a fail-closed absence poll: confirmed means
// at least one query succeeded; queryErr is the last error when none did.
type absenceOutcome struct {
	ok        bool
	worst     float64
	confirmed bool
	queryErr  error
}

// pollAbsenceTripwire polls a metric that must stay zero until a violation
// surfaces or the timeout elapses. It fails closed: if every query errors,
// absence was never observed and the tripwire does not pass.
func pollAbsenceTripwire(ctx context.Context, timeout, interval time.Duration, query absenceQueryFunc) absenceOutcome {
	var (
		lastErr   error
		confirmed bool
		worst     float64
	)
	err := poll(ctx, timeout, interval, func() (bool, error) {
		v, qerr := query(ctx)
		if qerr != nil {
			lastErr = qerr
			return false, nil
		}
		confirmed = true
		if v > worst {
			worst = v
		}
		return worst > 0, nil
	})
	if err == nil {
		// A non-zero point surfaced.
		return absenceOutcome{ok: false, worst: worst, confirmed: true}
	}
	if !confirmed {
		// No clean query: absence was never confirmed.
		return absenceOutcome{ok: false, confirmed: false, queryErr: lastErr}
	}
	// At least one clean query saw only zeros.
	return absenceOutcome{ok: true, worst: worst, confirmed: true}
}

// assertNoClientWriteErr is the silent-loss tripwire: after a driver exits it
// polls __src_client_write_err for the client's language and fails on any
// non-zero point. The metric is realtime, so the window starts at the client
// phase (statusAnchor), not the historic base; 200s covers build, run and the
// conveyor with clock-skew headroom.
func assertNoClientWriteErr(ctx context.Context, rec *recorder, apiAddr, clientTag string, statusAnchor uint32) (ok bool, detail string) {
	lang, knows := clientWriteErrLang[clientTag]
	if !knows {
		return true, ""
	}
	q := url.Values{}
	q.Set("s", clientWriteErrMetric)
	q.Set("f", strconv.FormatUint(uint64(statusAnchor), 10))
	q.Set("t", strconv.FormatUint(uint64(statusAnchor+200), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "sum")
	q.Set("qb", "1") // one series per client language
	qurl := "http://" + apiAddr + "/api/query?" + q.Encode()

	// An absent language series reads as 0, which is a clean observation.
	var (
		violBody   string // raw reply of the first violating query
		violStatus int
	)
	query := func(ctx context.Context) (float64, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		if qerr != nil {
			return 0, qerr
		}
		maxLost, _ := clientWriteErrForLang(resp, lang)
		if maxLost > 0 && violBody == "" {
			violBody, violStatus = body, status
		}
		return maxLost, nil
	}
	o := pollAbsenceTripwire(ctx, writeErrTimeout, 3*time.Second, query)
	if o.ok {
		return true, ""
	}
	if !o.confirmed {
		// recordFailedQuery is nil-safe.
		rec.recordFailedQuery(failedQuery{
			Label: "write_err", Client: clientTag, Metric: clientWriteErrMetric,
			URL: qurl, HTTPStatus: 0, Body: "",
		})
		return false, fmt.Sprintf("client=%s lang=%s could not confirm absence of %s — every query failed over %s: %v\nurl: %s",
			clientTag, lang, clientWriteErrMetric, writeErrTimeout, o.queryErr, qurl)
	}
	// A non-zero point surfaced: the client dropped bytes.
	rec.recordFailedQuery(failedQuery{
		Label: "write_err", Client: clientTag, Metric: clientWriteErrMetric,
		URL: qurl, HTTPStatus: violStatus, Body: violBody,
	})
	return false, fmt.Sprintf("client=%s lang=%s lost_bytes≈%g (silent TCP-backpressure drop; __src_client_write_err non-zero)\nurl: %s",
		clientTag, lang, o.worst, qurl)
}

// clientWriteErrForLang returns the largest lost-byte value of the given
// language's series in a __src_client_write_err reply grouped by language.
func clientWriteErrForLang(resp *apiSeriesResponse, lang string) (maxLost float64, found bool) {
	for i, meta := range resp.Data.Series.SeriesMeta {
		if !seriesMetaHasLang(meta.Tags, lang) {
			continue
		}
		for _, v := range resp.Data.Series.SeriesData[i] {
			if v > 0 {
				found = true
				if v > maxLost {
					maxLost = v
				}
			}
		}
	}
	return maxLost, found
}

// seriesMetaHasLang reports whether any tag in a series_meta carries the given
// language code; with qb=1 the language is the only grouped tag.
func seriesMetaHasLang(tags map[string]apiMetaTag, lang string) bool {
	for _, t := range tags {
		if strings.TrimSpace(t.Value) == lang {
			return true
		}
	}
	return false
}

// Rejection statuses, conservation ledger and sampling tripwire. The agent
// accounts every received event exactly once in __src_ingestion_status: ok_cached
// when accepted, one err_* when rejected. An event for a not-yet-created metric
// goes to __src_ingestion_status_no_shard instead, so unmapped cold-start seeds
// never touch a metric's ledger.

// ingestionStatusTail is the length in seconds of the realtime status window
// starting at statusAnchor. Statuses are recorded at receive time, not at the
// event's historic ts; the tail covers build, run, the conveyor and clock skew.
// Metric names are run-unique, so a wide window picks up no other run.
const ingestionStatusTail = 400

// ingestionStatusNumSeries is the series cap passed as n on the status query.
// The API default of 10 silently drops metrics' statuses.
const ingestionStatusNumSeries = 1000

// ledgerTimeout bounds the ledger/status poll; assertStream runs first, so the
// statuses have usually landed already.
const ledgerTimeout = 90 * time.Second

// samplingTimeout bounds the whole-run __agg_sampling_factor absence poll. It
// runs after assertStream, so any sampling point has long since landed.
const samplingTimeout = 30 * time.Second

// ingestionStatusURL builds the __src_ingestion_status event-count query grouped
// by metric (tag1) and status (tag2) over [anchor, anchor+ingestionStatusTail].
func ingestionStatusURL(apiAddr string, anchor uint32) string {
	q := url.Values{}
	q.Set("s", ingestionStatusMetric)
	q.Set("f", strconv.FormatUint(uint64(anchor), 10))
	q.Set("t", strconv.FormatUint(uint64(anchor+ingestionStatusTail), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "count")
	q.Set("n", strconv.Itoa(ingestionStatusNumSeries))
	q.Add("qb", "1") // metric name
	q.Add("qb", "2") // status value ID
	return "http://" + apiAddr + "/api/query?" + q.Encode()
}

// fetchIngestionBreakdown folds __src_ingestion_status into
// map[metric][statusID]events for the known metrics, and also returns the raw
// response body for the failure artifacts.
func fetchIngestionBreakdown(ctx context.Context, apiAddr string, anchor uint32, known map[string]bool) (map[string]map[int32]float64, string, error) {
	resp, body, _, err := queryCounterRaw(ctx, ingestionStatusURL(apiAddr, anchor))
	if err != nil {
		return nil, "", err
	}
	out := make(map[string]map[int32]float64)
	for i, meta := range resp.Data.Series.SeriesMeta {
		metric, statusID := classifyIngestionSeries(meta.Tags, known)
		if metric == "" || statusID == 0 {
			continue
		}
		sum := seriesSum(resp.Data.Series.SeriesData[i])
		if sum == 0 {
			continue
		}
		if out[metric] == nil {
			out[metric] = make(map[int32]float64)
		}
		out[metric][statusID] += sum
	}
	return out, body, nil
}

// seriesSum sums every bucket value in one series' data array.
func seriesSum(data []float64) float64 {
	var s float64
	for _, v := range data {
		s += v
	}
	return s
}

// classifyIngestionSeries reads a __src_ingestion_status series' metric name
// (key1, restricted to known) and numeric status ID (key2; the API renders IDs,
// not names). Value-based fallbacks cover a future key rename.
func classifyIngestionSeries(tags map[string]apiMetaTag, known map[string]bool) (metric string, statusID int32) {
	if t, ok := tags["key1"]; ok {
		if v := strings.TrimSpace(t.Value); known[v] {
			metric = v
		}
	}
	if t, ok := tags["key2"]; ok {
		if id, err := strconv.Atoi(strings.TrimSpace(t.Value)); err == nil {
			statusID = int32(id)
		}
	}
	if metric == "" { // value-type fallback if key1 is ever renamed
		for _, t := range tags {
			if v := strings.TrimSpace(t.Value); known[v] {
				metric = v
				break
			}
		}
	}
	if statusID == 0 { // value-type fallback if key2 is ever renamed
		for _, t := range tags {
			if id, err := strconv.Atoi(strings.TrimSpace(t.Value)); err == nil && id != 0 {
				statusID = int32(id)
				break
			}
		}
	}
	return metric, statusID
}

// statusIDOKCached is the __src_ingestion_status value ID for an accepted event.
const statusIDOKCached int32 = 10

// ingestionStatusNames maps status IDs to the builtin's display names, for
// failure detail and for isWarnStatus.
var ingestionStatusNames = map[int32]string{
	10: "ok_cached",
	21: "err_metric_not_found",
	23: "err_nan_inf_value",
	24: "err_nan_inf_counter",
	25: "err_negative_counter",
	33: "warn_tag_not_found",
	34: "err_map_invalid_raw_tag_value",
	35: "err_map_tag_value_cached",
	36: "err_map_tag_value",
	39: "err_validate_tag_value_utf8",
	42: "err_metric_disabled",
	46: "warn_map_tag_set_twice",
	47: "warn_deprecated_tag_name",
	48: "err_validate_metric_utf8",
	49: "err_validate_tag_name_utf8",
	50: "err_value_unique_both_set",
	52: "warn_map_invalid_raw_tag_value",
	53: "warn_tag_draft_found",
	54: "err_metric_sharding_failed",
	55: "warn_timestamp_clamped_past",
	56: "warn_timestamp_clamped_future_agg",
	57: "err_metric_builtin",
	59: "warn_timestamp_clamped_future",
	60: "err_too_big_counter",
	61: "err_too_big_value",
	62: "err_zero_counter",
	63: "err_map_tag_value_corrupted",
}

// ingestionStatusName returns the display name for a status ID, or "status_<id>".
func ingestionStatusName(id int32) string {
	if n, ok := ingestionStatusNames[id]; ok {
		return n
	}
	return fmt.Sprintf("status_%d", id)
}

// isWarnStatus reports whether a status is a warning. A warning accompanies an
// accepted (ok_cached) event, so the ledger excludes it. A new upstream warn_*
// ID missing from ingestionStatusNames counts as an error and fails the ledger
// loudly; add it to the map.
func isWarnStatus(id int32) bool {
	return strings.HasPrefix(ingestionStatusName(id), "warn_")
}

// ledgerBalance splits one metric's status counts into the accepted total, the
// error total, and per-status error and warning breakdowns; warnings stay out
// of the balance.
func ledgerBalance(byID map[int32]float64) (okCached, errSum float64, errs, warns map[int32]float64) {
	errs = make(map[int32]float64)
	warns = make(map[int32]float64)
	for id, count := range byID {
		switch {
		case id == statusIDOKCached:
			okCached += count
		case isWarnStatus(id):
			warns[id] = count
		default:
			errSum += count
			errs[id] = count
		}
	}
	return okCached, errSum, errs, warns
}

// ledgerEligibleKind reports whether a kind is in the ledger's exact scope.
// ok_cached counts wire items, not write calls, so the identity
// sentWrites == ok_cached + errors holds only for 1:1 kinds. Unique is out
// because a 100k-value write splits into many packets; value_p is out because
// it is pre-created, so its seed arrives mapped and adds one ok_cached.
func ledgerEligibleKind(kind string) bool {
	return kind != kindUnique && kind != kindValueP
}

// ledgerWriteCounts returns sentWrites per ledger-eligible metric from
// stream.Writes; seeds are not in it, matching their absence from the ledger.
func ledgerWriteCounts(stream metricStream) map[string]int {
	counts := make(map[string]int)
	for _, w := range stream.Writes {
		if !ledgerEligibleKind(w.Kind) {
			continue
		}
		counts[w.Metric]++
	}
	return counts
}

// knownMetricNames is the set of metric names a client generated.
func knownMetricNames(stream metricStream) map[string]bool {
	out := make(map[string]bool, len(stream.Metrics)+len(stream.Rejections))
	for _, m := range stream.Metrics {
		out[m.Name] = true
	}
	for _, r := range stream.Rejections {
		out[r.Name] = true
	}
	return out
}

// pollIngestionLedger polls the __src_ingestion_status breakdown until both
// the rejection statuses and the ledger converge or ledgerTimeout elapses, and
// returns the last clean breakdown and its raw body for both assertions.
func pollIngestionLedger(ctx context.Context, apiAddr string, stream metricStream, statusAnchor uint32) (map[string]map[int32]float64, string) {
	known := knownMetricNames(stream)
	want := ledgerWriteCounts(stream)
	var (
		last     map[string]map[int32]float64
		lastBody string
	)
	_ = poll(ctx, ledgerTimeout, 3*time.Second, func() (bool, error) {
		bd, body, err := fetchIngestionBreakdown(ctx, apiAddr, statusAnchor, known)
		if err != nil {
			return false, nil
		}
		last, lastBody = bd, body
		return rejectionsConverged(stream.Rejections, bd) && ledgerConverged(want, bd), nil
	})
	return last, lastBody
}

// assertRejections checks that each rejected input surfaces its exact status
// with count == sentWrites, one PASS/FAIL per rejection metric. Client-dropped
// cases pass as SKIPs.
func assertRejections(rec *recorder, apiAddr, clientTag string, stream metricStream, statusAnchor uint32, last map[string]map[int32]float64, lastBody string) (passed, failed int) {
	if len(stream.Rejections) == 0 {
		return 0, 0
	}
	// Used in failure records and details.
	qurl := ingestionStatusURL(apiAddr, statusAnchor)
	for _, r := range stream.Rejections {
		if !r.Sent {
			passed++
			rec.logf("PASS client=%s rejection=%s status=%s(%d) SKIP: %s", clientTag, r.Name, r.StatusName, r.StatusID, r.SkipReason)
			fmt.Printf("PASS client=%s rejection=%s status=%s SKIP\n", clientTag, r.Name, r.StatusName)
			continue
		}
		got := statusCount(last[r.Name], r.StatusID)
		if got == float64(r.Writes) {
			passed++
			rec.logf("PASS client=%s rejection=%s status=%s(%d) count=%d", clientTag, r.Name, r.StatusName, r.StatusID, r.Writes)
			fmt.Printf("PASS client=%s rejection=%s status=%s\n", clientTag, r.Name, r.StatusName)
			continue
		}
		failed++
		det := rejectionFailDetail(r, last[r.Name], qurl)
		rec.recordFailedQuery(failedQuery{
			Label: "rejection", Client: clientTag, Metric: r.Name,
			URL: qurl, Body: lastBody,
		})
		rec.logf("FAIL client=%s rejection=%s status=%s(%d)\n%s", clientTag, r.Name, r.StatusName, r.StatusID, indent(det))
		fmt.Printf("FAIL client=%s rejection=%s status=%s(%d)\n%s\n", clientTag, r.Name, r.StatusName, r.StatusID, indent(det))
	}
	return passed, failed
}

// rejectionsConverged reports whether every sent rejection has reached its
// exact status count.
func rejectionsConverged(rejections []rejectionMetric, bd map[string]map[int32]float64) bool {
	for _, r := range rejections {
		if !r.Sent {
			continue
		}
		if statusCount(bd[r.Name], r.StatusID) != float64(r.Writes) {
			return false
		}
	}
	return true
}

// statusCount looks up one status ID's total in a metric's breakdown (nil-safe).
func statusCount(breakdown map[int32]float64, statusID int32) float64 {
	if breakdown == nil {
		return 0
	}
	return breakdown[statusID]
}

// assertConservationLedger checks, for every eligible metric M, the exact
// invariant
//
//	sentWrites(M) == okCached(M) + Σ err_*(M)
//
// Without sampling any drift is a real violation: silent loss (<) or
// double-counting (>). Failures print the full status breakdown.
func assertConservationLedger(rec *recorder, apiAddr, clientTag string, stream metricStream, statusAnchor uint32, last map[string]map[int32]float64, lastBody string) (passed, failed int) {
	want := ledgerWriteCounts(stream)
	excluded := ledgerExcludedMetricNames(stream)
	// Used in failure records and details.
	qurl := ingestionStatusURL(apiAddr, statusAnchor)
	// Sorted iteration for stable output.
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		sentWrites := want[name]
		okCached, errSum, errs, warns := ledgerBalance(last[name])
		if okCached+errSum == float64(sentWrites) {
			passed++
			continue
		}
		failed++
		det := ledgerFailDetail(name, sentWrites, okCached, errSum, errs, warns, qurl)
		rec.recordFailedQuery(failedQuery{
			Label: "ledger", Client: clientTag, Metric: name,
			URL: qurl, Body: lastBody,
		})
		rec.logf("FAIL client=%s ledger metric=%s\n%s", clientTag, name, indent(det))
		fmt.Printf("FAIL client=%s ledger metric=%s\n%s\n", clientTag, name, indent(det))
	}
	exclNote := ""
	if len(excluded) != 0 {
		sort.Strings(excluded)
		exclNote = fmt.Sprintf(" [%d multi-value excluded (item count ≠ write count): %s]", len(excluded), strings.Join(excluded, ","))
	}
	if failed == 0 {
		rec.logf("PASS client=%s conservation ledger: %d metric(s) balance (sentWrites == ok_cached + Σerr_*)%s", clientTag, len(want), exclNote)
		fmt.Printf("PASS client=%s conservation ledger: %d metric(s)%s\n", clientTag, len(want), exclNote)
	} else {
		rec.logf("FAIL client=%s conservation ledger: %d/%d metric(s) unbalanced%s", clientTag, failed, len(want), exclNote)
		fmt.Printf("FAIL client=%s conservation ledger: %d/%d unbalanced%s\n", clientTag, failed, len(want), exclNote)
	}
	return passed, failed
}

// ledgerExcludedMetricNames returns the metrics outside the ledger's scope, so
// the summary names them.
func ledgerExcludedMetricNames(stream metricStream) []string {
	seen := make(map[string]bool)
	var out []string
	for _, m := range stream.Metrics {
		if !ledgerEligibleKind(m.Kind) && !seen[m.Name] {
			seen[m.Name] = true
			out = append(out, m.Name)
		}
	}
	for _, r := range stream.Rejections {
		if !ledgerEligibleKind(r.Kind) && !seen[r.Name] {
			seen[r.Name] = true
			out = append(out, r.Name)
		}
	}
	return out
}

// ledgerConverged reports whether every metric balances yet.
func ledgerConverged(want map[string]int, bd map[string]map[int32]float64) bool {
	for name, sentWrites := range want {
		okCached, errSum, _, _ := ledgerBalance(bd[name])
		if okCached+errSum != float64(sentWrites) {
			return false
		}
	}
	return true
}

// aggSamplingFactorMetric is the builtin the agg writes when it samples an
// insert. The harness's budget flags keep every insert unsampled, so it must
// stay zero for the whole run.
const aggSamplingFactorMetric = "__agg_sampling_factor"

// assertNoAggSampling is the whole-run sampling tripwire: __agg_sampling_factor
// must stay zero, since sampling breaks the ledger's exactness. compareByFunc
// checks each queried view; this covers the whole run over the realtime window
// starting at statusAnchor, and fails closed like every absence poll.
func assertNoAggSampling(ctx context.Context, rec *recorder, apiAddr, clientTag string, statusAnchor uint32) (ok bool, detail string) {
	q := url.Values{}
	q.Set("s", aggSamplingFactorMetric)
	q.Set("f", strconv.FormatUint(uint64(statusAnchor), 10))
	q.Set("t", strconv.FormatUint(uint64(statusAnchor+ingestionStatusTail), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "count")
	qurl := "http://" + apiAddr + "/api/query?" + q.Encode()

	// pollAbsenceTripwire fails closed if every query errors.
	var (
		violBody   string // raw reply of the first violating query
		violStatus int
	)
	query := func(ctx context.Context) (float64, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		if qerr != nil {
			return 0, qerr
		}
		mx := maxSeriesValue(resp)
		if mx > 0 && violBody == "" {
			violBody, violStatus = body, status
		}
		return mx, nil
	}
	o := pollAbsenceTripwire(ctx, samplingTimeout, 3*time.Second, query)
	if o.ok {
		// No insert was sampled.
		return true, ""
	}
	if !o.confirmed {
		rec.recordFailedQuery(failedQuery{
			Label: "sampling", Client: clientTag, Metric: aggSamplingFactorMetric,
			URL: qurl, HTTPStatus: 0, Body: "",
		})
		return false, fmt.Sprintf("client=%s could not confirm absence of %s — every query failed over %s: %v\nurl: %s",
			clientTag, aggSamplingFactorMetric, samplingTimeout, o.queryErr, qurl)
	}
	// An insert was sampled during the run.
	rec.recordFailedQuery(failedQuery{
		Label: "sampling", Client: clientTag, Metric: aggSamplingFactorMetric,
		URL: qurl, HTTPStatus: violStatus, Body: violBody,
	})
	return false, fmt.Sprintf("client=%s %s non-zero (max=%g) — an insert was sampled during the run\nurl: %s",
		clientTag, aggSamplingFactorMetric, o.worst, qurl)
}

// maxSeriesValue is the largest value across every series and bucket in a reply.
func maxSeriesValue(resp *apiSeriesResponse) float64 {
	var mx float64
	for _, data := range resp.Data.Series.SeriesData {
		for _, v := range data {
			if v > mx {
				mx = v
			}
		}
	}
	return mx
}

// sortedStatusIDs returns a breakdown's status IDs in ascending order.
func sortedStatusIDs(m map[int32]float64) []int32 {
	ids := make([]int32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ledgerFailDetail renders the failed equation and the full status breakdown,
// warnings included, for one imbalanced metric.
func ledgerFailDetail(name string, sentWrites int, okCached, errSum float64, errs, warns map[int32]float64, qurl string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "conservation imbalance: ok_cached(%g) + Σerr(%g) = %g ≠ sentWrites=%d\n",
		okCached, errSum, okCached+errSum, sentWrites)
	if okCached+errSum < float64(sentWrites) {
		fmt.Fprintf(&b, "  → %g event(s) UNACCOUNTED (silent loss)\n", float64(sentWrites)-(okCached+errSum))
	} else {
		fmt.Fprintf(&b, "  → %g event(s) OVER-COUNTED (double-counting)\n", (okCached+errSum)-float64(sentWrites))
	}
	fmt.Fprintf(&b, "  full __src_ingestion_status breakdown for %s (status=count):\n", name)
	fmt.Fprintf(&b, "    %s(10)=%g\n", ingestionStatusName(statusIDOKCached), okCached)
	ids := sortedStatusIDs(errs)
	if len(ids) == 0 {
		fmt.Fprintf(&b, "    (no err_* statuses recorded)\n")
	}
	for _, id := range ids {
		fmt.Fprintf(&b, "    %s(%d)=%g\n", ingestionStatusName(id), id, errs[id])
	}
	// Warnings accompany accepted events; shown for context only.
	for _, id := range sortedStatusIDs(warns) {
		fmt.Fprintf(&b, "    %s(%d)=%g (warning — accepted, not a loss)\n", ingestionStatusName(id), id, warns[id])
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}

// rejectionFailDetail renders expected vs got and the full status breakdown for
// one rejection that missed its exact count.
func rejectionFailDetail(r rejectionMetric, breakdown map[int32]float64, qurl string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "expected %s(%d) count=%d, got count=%g\n",
		r.StatusName, r.StatusID, r.Writes, statusCount(breakdown, r.StatusID))
	if len(breakdown) == 0 {
		fmt.Fprintf(&b, "  (no __src_ingestion_status rows for %s — metric never reached the agent, or window missed it)\n", r.Name)
		fmt.Fprintf(&b, "url: %s", qurl)
		return b.String()
	}
	fmt.Fprintf(&b, "  full breakdown for %s (status=count):\n", r.Name)
	for _, id := range sortedStatusIDs(breakdown) {
		fmt.Fprintf(&b, "    %s(%d)=%g\n", ingestionStatusName(id), id, breakdown[id])
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}
