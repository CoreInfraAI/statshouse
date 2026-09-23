package main

import (
	"sort"
	"strings"
)

// Non-counter generators. Each builds the driver's writes and the expected
// model from one construction; large payloads are emitted as generator loops
// (genSpec), not literals, so the rendered client source stays compilable.

// addValue builds the value metric: two series of mixed-sign floats. They stay
// below the go client's 1024-value reservoir, so aggregates are exact; the
// model sums in write order like the agent, so float sums are bit-identical.
func (b *streamBuilder) addValue() {
	const suffix = "v_mix"
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kindValue, QBKeys: []string{"0"}}
	series := []struct {
		tags   []tag
		values []float64
	}{
		{tags: []tag{{"0", "a"}}, values: []float64{-3.5, -0.01, 2.718281828459045, 100.0}},
		{tags: []tag{{"0", "b"}}, values: []float64{-99.9, 0.001, 0.125, 7.5}},
	}
	for _, ss := range series {
		vals := append([]float64(nil), ss.values...)
		values := make(map[uint32][]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			values[ts] = vals // same set every bucket; exact per-bucket check over all 70
			b.writes = append(b.writes, metricWrite{Kind: kindValue, Metric: name, Tags: ss.tags, Values: vals, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Values: values})
	}
	b.metrics = append(b.metrics, m)
}

// addValueP builds the value_p metric: a uniform and a skewed series, each
// valuePBucketPoints per bucket. It is pre-created by metric_create.go.
func (b *streamBuilder) addValueP() {
	const suffix = "vp_mix"
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kindValueP, QBKeys: []string{"0"}}
	series := []struct {
		tags []tag
		gen  genSpec
	}{
		{tags: []tag{{"0", "unif"}}, gen: genSpec{Kind: genKindValueUniform, N: valuePBucketPoints}},
		{tags: []tag{{"0", "skew"}}, gen: genSpec{Kind: genKindValueSkewed, N: valuePBucketPoints}},
	}
	for _, ss := range series {
		expected := expectedValues(ss.gen) // sorted reference the asserter quantifies
		values := make(map[uint32][]float64, numBuckets)
		gen := ss.gen // copy: the loop var address would be shared otherwise
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			values[ts] = expected
			b.writes = append(b.writes, metricWrite{Kind: kindValueP, Metric: name, Tags: ss.tags, Gen: &gen, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Values: values, GenKind: ss.gen.Kind})
	}
	b.metrics = append(b.metrics, m)
}

// addUnique builds an exact and an approximate unique metric.
func (b *streamBuilder) addUnique() {
	// Exact: ≤65536 distinct, each value written twice to exercise dedup.
	b.addUniqueMetric("u_exact", []tag{{"0", "small"}}, genSpec{Kind: genKindUniqueDedup, N: smallUniqueDistinct}, numBuckets)
	// Approximate: >65536 distinct, ±2% band, fewer buckets to bound wire and memory.
	b.addUniqueMetric("u_approx", []tag{{"0", "big"}}, genSpec{Kind: genKindUniqueDistinct, N: bigUniqueDistinct}, bigUniqueBuckets)
}

// addUniqueMetric builds one single-series unique metric over nBuckets buckets.
func (b *streamBuilder) addUniqueMetric(suffix string, tags []tag, gen genSpec, nBuckets int) {
	name := b.prefix + suffix
	distinct := expectedUnique(gen)
	m := metricModel{Name: name, Kind: kindUnique, QBKeys: []string{"0"}}
	uniques := make(map[uint32]int, nBuckets)
	for i := 0; i < nBuckets; i++ {
		ts := b.base + uint32(i)
		uniques[ts] = distinct
		g := gen
		b.writes = append(b.writes, metricWrite{Kind: kindUnique, Metric: name, Tags: tags, Gen: &g, TS: ts})
	}
	nt := normalizeTags(tags)
	if n := fullKeyLen(name, tags); n > b.maxKey {
		b.maxKey = n
	}
	m.Series = append(m.Series, seriesModel{Tags: nt, Uniques: uniques})
	b.metrics = append(b.metrics, m)
}

// addStag builds a counter metric whose series differ only in one string tag
// value, asserted via qw=cardinality with no group-by: the expected
// cardinality per bucket is the number of series. The empty value is the
// absent tag, still a distinct row.
func (b *streamBuilder) addStag() {
	const suffix = "s_dist"
	name := b.prefix + suffix
	// The long value stays under the 128-byte receiver cap.
	longVal := "L" + strings.Repeat("x", 120)
	values := []string{"alpha", "beta", "café", "東京", "", longVal}
	m := metricModel{Name: name, Kind: kindStag}
	// Empty QBKeys: the API returns a single total series per bucket.
	for _, v := range values {
		tags := []tag{{"0", v}}
		counts := make(map[uint32]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			counts[ts] = 1
			b.writes = append(b.writes, metricWrite{Kind: kindStag, Metric: name, Tags: tags, Count: 1, TS: ts})
		}
		nt := normalizeTags(tags) // empty value dropped in the model's identity
		if n := fullKeyLen(name, tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Counts: counts})
	}
	b.metrics = append(b.metrics, m)
}

// expectedValues returns the sorted value population of a value generator.
func expectedValues(g genSpec) []float64 {
	switch g.Kind {
	case genKindValueUniform:
		return genValueUniform(g.N)
	case genKindValueSkewed:
		out := genValueSkewed(g.N)
		sort.Float64s(out)
		return out
	}
	return nil
}

// expectedUnique returns the distinct count of a unique generator (N for both).
func expectedUnique(g genSpec) int {
	switch g.Kind {
	case genKindUniqueDistinct, genKindUniqueDedup:
		return g.N
	}
	return 0
}

// addRejections builds the rejection inputs as dedicated metrics, asserted
// through __src_ingestion_status and the conservation ledger. Client-dropped
// counter cases are recorded as SKIPs with no writes.
func (b *streamBuilder) addRejections(clientTag string) {
	switch clientTag {
	case cppClientTag:
		// cpp sends count<=0 and the server rejects it.
		b.addRejectionCounter("c_zero", 0, statusNameZeroCounter, statusIDZeroCounter)
		b.addRejectionCounter("c_neg", -5, statusNameNegCounter, statusIDNegCounter)
	default:
		// go/rust drop count<=0 before the wire.
		b.addRejectionCounterSkipped("c_zero", statusNameZeroCounter, statusIDZeroCounter)
		b.addRejectionCounterSkipped("c_neg", statusNameNegCounter, statusIDNegCounter)
	}
	// No client validates value payloads.
	b.addRejectionValue("v_nan", kindValueNaN, statusNameNanInfValue, statusIDNanInfValue)
	b.addRejectionValue("v_inf", kindValueInf, statusNameTooBigValue, statusIDTooBigValue)
}

// addRejectionCounter builds a counter metric whose every write carries an
// invalid count the server rejects (status 62 zero / 25 negative).
func (b *streamBuilder) addRejectionCounter(suffix string, count float64, statusName string, statusID int32) {
	name := b.prefix + suffix
	for i := 0; i < numBuckets; i++ {
		ts := b.base + uint32(i)
		b.writes = append(b.writes, metricWrite{Kind: kindCounter, Metric: name, Count: count, TS: ts})
	}
	if n := len(name); n > b.maxKey {
		b.maxKey = n
	}
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kindCounter,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     numBuckets,
		Sent:       true,
	})
}

// addRejectionCounterSkipped records a client-dropped counter case with no
// writes, so assertRejections prints an explicit SKIP line.
func (b *streamBuilder) addRejectionCounterSkipped(suffix, statusName string, statusID int32) {
	name := b.prefix + suffix
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kindCounter,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     0,
		Sent:       false,
		SkipReason: "client drops count<=0 before the wire",
	})
}

// addRejectionValue builds a value metric whose every write carries NaN or
// +Inf (status 23 / 61). These need a dedicated kind because neither is a
// float literal in any driver language; the metric auto-creates from a valid
// value=1 seed.
func (b *streamBuilder) addRejectionValue(suffix, kind, statusName string, statusID int32) {
	name := b.prefix + suffix
	for i := 0; i < numBuckets; i++ {
		ts := b.base + uint32(i)
		b.writes = append(b.writes, metricWrite{Kind: kind, Metric: name, TS: ts})
	}
	if n := len(name); n > b.maxKey {
		b.maxKey = n
	}
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kind,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     numBuckets,
		Sent:       true,
	})
}
