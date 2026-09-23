package main

import (
	"fmt"
	"strings"
	"time"
)

// numBuckets is the number of consecutive 1-second buckets every series fills
// (except big-unique), so a missing point from /api/query is a failure.
const numBuckets = 70

// bigUniqueBuckets is how many buckets the ~100k-distinct unique metric fills;
// all 70 would be ≈168 MB on the wire across three clients.
const bigUniqueBuckets = 10

// bigUniqueDistinct exceeds 65536 to force ChUnique's thinning estimator
// (1σ ≈ 0.45%, so the ±2% band is ~4σ).
const bigUniqueDistinct = 100000

// smallUniqueDistinct is the exact-unique distinct count, well inside
// ChUnique's exact region.
const smallUniqueDistinct = 300

// valuePBucketPoints is the per-bucket value_p sample count: enough for the
// t-digest to place centroids well, small enough to run fast.
const valuePBucketPoints = 2000

// fullKeyCap bounds the rendered full key (name + tags), below the clients'
// own caps (cpp 1024 B, rust 4096 B) so none silently drops a series.
const fullKeyCap = 768

// Metric kinds the generator emits. counter/value/unique auto-create from a
// kind-matching seed; value_p is pre-created (metric_create.go); stag is a
// counter asserted by cardinality. value_nan/value_inf are rejected payloads
// with their own template branches, since neither is a float literal in any
// driver language.
const (
	kindCounter  = "counter"
	kindValue    = "value"
	kindValueP   = "value_p"
	kindUnique   = "unique"
	kindStag     = "stag"
	kindValueNaN = "value_nan" // rejected value payload: NaN  → status 23 err_nan_inf_value
	kindValueInf = "value_inf" // rejected value payload: +Inf → status 61 err_too_big_value
)

// __src_ingestion_status is the agent's per-observation accounting record:
// ok_cached (10) when accepted, one err_* status when rejected. Tags are
// tag1=metric name, tag2=status rendered as its numeric ID (e.g. " 23"), so
// grouping by qb=1&qb=2 yields one series per (metric, status). A +Inf value
// fails ValidateValue as too big (61), not as NaN/Inf (24 is counters only).
const (
	ingestionStatusMetric = "__src_ingestion_status"

	// statusName* only label PASS/FAIL lines; assertions match statusID*.
	statusNameOKCached    = "ok_cached"
	statusNameZeroCounter = "err_zero_counter"     // 62
	statusNameNegCounter  = "err_negative_counter" // 25
	statusNameNanInfValue = "err_nan_inf_value"    // 23
	statusNameTooBigValue = "err_too_big_value"    // 61

	// statusID* are the numeric IDs the query API renders (CodeTagValue).
	statusIDZeroCounter = 62
	statusIDNegCounter  = 25
	statusIDNanInfValue = 23
	statusIDTooBigValue = 61

	// clientTag* are the per-client metric-name prefixes and --client selectors.
	goClientTag   = "go"
	rustClientTag = "rust"
	cppClientTag  = "cpp"
)

// tag is one positional tag: Key is the index ("0".."47"), which round-trips
// without a metric mapping.
type tag struct {
	Key string
	Val string
}

// metricWrite is one observation injected into a client driver template. Tags
// keep empty values so the client's own empty-tag handling is exercised.
type metricWrite struct {
	Kind    string
	Metric  string
	Tags    []tag
	TS      uint32
	Count   float64   // counter / stag
	Values  []float64 // value (literal payload); value_p uses Gen
	Uniques []int64   // unique small (literal); big unique uses Gen
	Gen     *genSpec  // value_p / unique loop descriptor; nil → literal payload
}

// genSpec describes a deterministic generator loop the driver emits in place
// and the harness replicates to build the expected model; N is the item count.
type genSpec struct {
	Kind string
	N    int
}

// genKind* are the genSpec.Kind strings the driver templates match on; do not
// change them. quantile.go holds the Go reference of each formula.
const (
	genKindValueUniform   = "valueUniform"   // 0..N-1
	genKindValueSkewed    = "valueSkewed"    // shared-LCG r²/1000, mass near 0
	genKindUniqueDistinct = "uniqueDistinct" // 1..N, distinct=N
	genKindUniqueDedup    = "uniqueDedup"    // 1..N each emitted twice, distinct=N
)

// seriesModel is one expected series. Tags are normalized as the wire sees
// them (empty values dropped); only the map for the metric's kind is set, keyed
// by bucket timestamp. GenKind lets the percentile asserter widen its band for
// the skewed generator.
type seriesModel struct {
	Tags    []tag
	Counts  map[uint32]float64   // counter / stag: expected count
	Values  map[uint32][]float64 // value / value_p: merged values (sorted for value_p)
	Uniques map[uint32]int       // unique: expected distinct count
	GenKind string               // genSpec.Kind for generator series; "" for literal
}

// metricModel is the expected model for one metric; QBKeys are the tag keys
// the query groups by (empty for stag).
type metricModel struct {
	Name   string
	Kind   string
	QBKeys []string
	Series []seriesModel
}

// metricStream is the generated stream: Writes go to the driver, Metrics is
// the visible expected model, and Rejections are asserted only through
// __src_ingestion_status and the conservation ledger.
type metricStream struct {
	Base       uint32
	Writes     []metricWrite
	Metrics    []metricModel
	Rejections []rejectionMetric
}

// rejectionMetric is one metric whose every write the pipeline rejects
// (Sent==true: expected status count == Writes, ledger balances), or that the
// client drops before the wire (Sent==false: reported as a SKIP).
type rejectionMetric struct {
	Name       string // e2e_<runID>_<client>_c_zero, …
	Kind       string // kindCounter / kindValueNaN / kindValueInf (driver render + seed kind)
	StatusName string // expected __src_ingestion_status name (err_zero_counter, …)
	StatusID   int32  // numeric status (62/25/23/61) — diagnostics only
	Writes     int    // sentWrites: #writes the harness generated for this metric
	Sent       bool   // false ⇒ client drops client-side; ledger/status skip
	SkipReason string // documented reason when Sent==false
}

// generateStream builds the writes and the expected model in one pass. The
// runID and clientTag prefix every metric name, so clients sharing one stack
// never collide; hyphens become underscores because POST /api/metric rejects
// them in metric names.
func generateStream(runID, clientTag string, now time.Time) metricStream {
	prefix := "e2e_" + strings.ReplaceAll(runID, "-", "_") + "_" + clientTag + "_"
	base := uint32(now.Unix()) - 120 // floor(now) − 120s (now is already second-granular)
	b := &streamBuilder{prefix: prefix, base: base}
	b.addCounters()
	b.addValue()
	b.addValueP()
	b.addUnique()
	b.addStag()
	b.addRejections(clientTag)
	if b.maxKey > fullKeyCap {
		panic(fmt.Sprintf("e2e: generated full key %d B exceeds cap %d B", b.maxKey, fullKeyCap))
	}
	return metricStream{Base: base, Writes: b.writes, Metrics: b.metrics, Rejections: b.rejections}
}

// streamBuilder accumulates writes + the expected model and tracks the max
// rendered key length for the fullKeyCap guard.
type streamBuilder struct {
	prefix     string
	base       uint32
	writes     []metricWrite
	metrics    []metricModel
	rejections []rejectionMetric
	maxKey     int
}

// counterSeriesSpec is one counter series: raw tags + a per-bucket count fn.
type counterSeriesSpec struct {
	tags  []tag
	count func(bucket int) float64
}

// addCounterMetric is the shared builder for the counter-kind metrics (counter
// and stag both write counts; stag differs only in how it is asserted). Each
// series fills all numBuckets; empty tag values are kept raw so the client's
// empty-drop is exercised, and dropped again in the expected model.
func (b *streamBuilder) addCounterMetric(suffix, kind string, qb []string, series []counterSeriesSpec) {
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kind, QBKeys: qb}
	seenKeys := map[string]bool{}
	for _, ss := range series {
		counts := make(map[uint32]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			c := ss.count(i)
			counts[ts] = c
			b.writes = append(b.writes, metricWrite{Kind: kind, Metric: name, Tags: ss.tags, Count: c, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		for _, t := range nt {
			seenKeys[t.Key] = true
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Counts: counts})
	}
	if len(qb) == 0 {
		m.QBKeys = sortedKeys(seenKeys) // group-by every tag the series carry
	}
	b.metrics = append(b.metrics, m)
}

// addCounters builds the counter metrics, including a tag matrix covering 1–6
// tags, a value pool, unicode and an empty tag value.
func (b *streamBuilder) addCounters() {
	b.addCounterMetric("c_ones", kindCounter, nil, []counterSeriesSpec{
		{tags: nil, count: func(int) float64 { return 1 }}, // no tags, count 1
	})
	b.addCounterMetric("c_tagged", kindCounter, nil, []counterSeriesSpec{
		{tags: []tag{{"0", "alpha"}, {"1", "beta"}}, count: func(int) float64 { return 1 }},
	})
	b.addCounterMetric("c_multi", kindCounter, nil, []counterSeriesSpec{
		// Four series over tag keys {0,1} exercise group-by splitting.
		{tags: []tag{{"0", "x"}, {"1", "p"}}, count: off(2)},
		{tags: []tag{{"0", "x"}, {"1", "q"}}, count: off(10)},
		{tags: []tag{{"0", "y"}, {"1", "p"}}, count: off(20)},
		{tags: []tag{{"0", "y"}, {"1", "q"}}, count: off(30)},
	})
	b.addCounterMetric("c_empty", kindCounter, nil, []counterSeriesSpec{
		// The client drops the empty tag 1, so the series arrives as {0:"val"}.
		{tags: []tag{{"0", "val"}, {"1", ""}}, count: func(int) float64 { return 3 }},
	})
	b.addCounterMetric("c_unicode", kindCounter, nil, []counterSeriesSpec{
		// Unicode tag values round-trip as UTF-8.
		{tags: []tag{{"0", "東京"}, {"1", "café"}}, count: alt(1, 5)},
	})
	b.addCounterMetric("c_many", kindCounter, nil, []counterSeriesSpec{
		// Six tags, so qb covers indices 0..5.
		{tags: []tag{{"0", "a"}, {"1", "b"}, {"2", "c"}, {"3", "d"}, {"4", "e"}, {"5", "f"}}, count: alt(1, 7)},
	})
	b.addCounterMetric("c_matrix", kindCounter, nil, []counterSeriesSpec{
		// Each series has a distinct count so a group-by error cannot hide. A
		// tagless series is covered by c_ones instead: mixed with tagged series
		// under group-by, the API drops it when the metric's tags are unmapped.
		{tags: []tag{{"0", "m0"}}, count: off(110)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}}, count: off(120)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}}, count: off(130)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}}, count: off(140)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}, {"4", "m4"}}, count: off(145)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}, {"4", "m4"}, {"5", "m5"}}, count: off(150)},
		{tags: []tag{{"0", "p_a"}}, count: off(160)},
		{tags: []tag{{"0", "p_b"}}, count: off(170)},
		{tags: []tag{{"0", "café"}, {"1", "東京"}}, count: off(180)},
		{tags: []tag{{"0", "x"}, {"1", ""}}, count: off(190)}, // empty tag → arrives as {0:"x"}
	})
}

// seedKind returns the write kind that seeds a metric during pre-warm so
// auto-create derives the right metric kind; rejected value kinds seed with a
// valid value=1 write.
func seedKind(kind string) string {
	switch kind {
	case kindValue, kindValueP, kindValueNaN, kindValueInf:
		return kindValue
	case kindUnique:
		return kindUnique
	default: // counter, stag
		return kindCounter
	}
}

// seedDef is one metric's cold-start seed name and write kind.
type seedDef struct {
	Name string
	Kind string
}

// streamSeeds returns the seeds and the parallel name list the driver templates
// consume. A rejection's seed is a valid write that provisions the metric; it
// lands in __src_ingestion_status_no_shard, outside the metric's ledger.
// Client-dropped rejections are not seeded.
func streamSeeds(stream metricStream) (seeds []seedDef, names []string) {
	seeds = make([]seedDef, 0, len(stream.Metrics)+len(stream.Rejections))
	names = make([]string, 0, len(stream.Metrics)+len(stream.Rejections))
	for _, m := range stream.Metrics {
		seeds = append(seeds, seedDef{Name: m.Name, Kind: seedKind(m.Kind)})
		names = append(names, m.Name)
	}
	for _, r := range stream.Rejections {
		if !r.Sent {
			continue
		}
		seeds = append(seeds, seedDef{Name: r.Name, Kind: seedKind(r.Kind)})
		names = append(names, r.Name)
	}
	return seeds, names
}

// off returns a per-bucket count baseN+bucket, distinct per bucket.
func off(baseN int) func(int) float64 {
	return func(bucket int) float64 { return float64(baseN + bucket) }
}

// alt returns a per-bucket count alternating between evenC and oddC.
func alt(evenC, oddC float64) func(int) float64 {
	return func(bucket int) float64 {
		if bucket%2 == 0 {
			return evenC
		}
		return oddC
	}
}

// normalizeTags drops empty-valued tags as the client and receiver do.
func normalizeTags(raw []tag) []tag {
	var out []tag
	for _, t := range raw {
		if t.Val == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// fullKeyLen estimates the rendered full key length for the cap check.
func fullKeyLen(metric string, tags []tag) int {
	n := len(metric)
	for _, t := range tags {
		n += len(t.Key) + len(t.Val)
	}
	return n
}
