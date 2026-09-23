package main

import (
	"strings"
	"testing"
	"time"
)

// generateStream is the single source of truth for every client driver and assertion.
func TestGenerateStream(t *testing.T) {
	const runID = "testrun"
	const clientTag = "go"
	now := time.Unix(1_700_000_000, 0) // second-granular, so floor(now) == now.Unix()
	s := generateStream(runID, clientTag, now)

	if wantBase := uint32(now.Unix()) - 120; s.Base != wantBase {
		t.Fatalf("Base = %d, want %d (floor(now)−120)", s.Base, wantBase)
	}

	kinds := map[string]bool{}
	for _, m := range s.Metrics {
		kinds[m.Kind] = true
	}
	for _, k := range []string{kindCounter, kindValue, kindValueP, kindUnique, kindStag} {
		if !kinds[k] {
			t.Errorf("kind %q not generated (metrics: %v)", k, metricKinds(s))
		}
	}

	prefix := "e2e_" + runID + "_" + clientTag + "_"

	// all series of a metric fill the same number of buckets
	bucketCountOf := func(m metricModel) int {
		if len(m.Series) == 0 {
			return 0
		}
		switch m.Kind {
		case kindCounter, kindStag:
			return len(m.Series[0].Counts)
		case kindValue, kindValueP:
			return len(m.Series[0].Values)
		case kindUnique:
			return len(m.Series[0].Uniques)
		}
		return 0
	}

	wantWrites := 0
	var totalSeries int
	for _, m := range s.Metrics {
		if !strings.HasPrefix(m.Name, prefix) {
			t.Errorf("metric %q missing prefix %q", m.Name, prefix)
		}
		// validMetricName rejects hyphens, which the value_p POST path would hit
		if strings.Contains(m.Name, "-") {
			t.Errorf("metric %q contains a hyphen (not a valid StatsHouse name)", m.Name)
		}
		nb := bucketCountOf(m)
		if nb == 0 {
			t.Errorf("metric %s (%s): no populated buckets in its model", m.Name, m.Kind)
		}
		for si, ser := range m.Series {
			totalSeries++

			for ts := range populatedBuckets(m.Kind, ser) {
				if ts < s.Base || ts >= s.Base+numBuckets {
					t.Errorf("%s series %d: bucket ts=%d outside [%d,%d)", m.Name, si, ts, s.Base, s.Base+numBuckets)
				}
			}
		}
		wantWrites += len(m.Series) * nb
	}
	// rejection writes are in s.Writes but their metrics are not in s.Metrics
	rejNames := make(map[string]bool, len(s.Rejections))
	for _, r := range s.Rejections {
		wantWrites += r.Writes
		rejNames[r.Name] = true
	}
	if totalSeries == 0 {
		t.Fatal("no series generated")
	}

	if len(s.Writes) != wantWrites {
		t.Errorf("len(Writes) = %d, want %d (Σ series×buckets + rejection writes)", len(s.Writes), wantWrites)
	}

	for _, w := range s.Writes {
		if w.TS < s.Base || w.TS >= s.Base+numBuckets {
			t.Errorf("write ts=%d outside [%d,%d)", w.TS, s.Base, s.Base+numBuckets)
		}
		if !strings.HasPrefix(w.Metric, prefix) {
			t.Errorf("write metric %q missing prefix", w.Metric)
		}
		switch w.Kind {
		case kindCounter, kindStag:
			// counter rejections (cpp c_zero/c_neg) legitimately carry count <= 0
			if w.Count <= 0 && !rejNames[w.Metric] {
				t.Errorf("%s write count %g, want > 0 (only rejection metrics may be ≤0)", w.Metric, w.Count)
			}
		case kindValue:
			if len(w.Values) == 0 {
				t.Errorf("%s value write has no values", w.Metric)
			}
		case kindValueNaN, kindValueInf:
			// NaN/+Inf payloads render via dedicated template branches, not Values
		case kindValueP, kindUnique:
			if w.Gen == nil {
				t.Errorf("%s %s write has no generator (Gen==nil)", w.Metric, w.Kind)
			}
		default:
			t.Errorf("write %q has unknown kind %q", w.Metric, w.Kind)
		}
	}

	// fullKeyLen is the generator guard's own estimate, so this also pins its threshold
	maxKey := 0
	for _, w := range s.Writes {
		if n := fullKeyLen(w.Metric, w.Tags); n > maxKey {
			maxKey = n
		}
	}
	if maxKey > fullKeyCap {
		t.Errorf("max full key %d B exceeds cap %d B", maxKey, fullKeyCap)
	}
	if maxKey == 0 {
		t.Error("maxKey = 0; expected at least one non-empty key")
	}
}

// populatedBuckets returns the bucket timestamps a series filled.
func populatedBuckets(kind string, ser seriesModel) map[uint32]struct{} {
	out := map[uint32]struct{}{}
	switch kind {
	case kindCounter, kindStag:
		for ts := range ser.Counts {
			out[ts] = struct{}{}
		}
	case kindValue, kindValueP:
		for ts := range ser.Values {
			out[ts] = struct{}{}
		}
	case kindUnique:
		for ts := range ser.Uniques {
			out[ts] = struct{}{}
		}
	}
	return out
}

func metricKinds(s metricStream) []string {
	out := make([]string, 0, len(s.Metrics))
	for _, m := range s.Metrics {
		out = append(out, m.Name+":"+m.Kind)
	}
	return out
}
