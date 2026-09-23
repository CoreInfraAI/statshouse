package main

import (
	"reflect"
	"strings"
	"testing"
)

// Pure-logic tests for the rejection / conservation-ledger machinery. The API renders
// __src_ingestion_status tag2 as the numeric value ID, so the ledger works in IDs;
// names are display-only.

// Rejected value kinds seed with a valid value write so auto-create derives a value metric.
func TestSeedKind(t *testing.T) {
	cases := []struct {
		kind string
		want string
	}{
		{kindValue, kindValue},
		{kindValueP, kindValue}, // pre-created; seed kind harmless, but value is consistent
		{kindValueNaN, kindValue},
		{kindValueInf, kindValue},
		{kindUnique, kindUnique},
		{kindCounter, kindCounter},
		{kindStag, kindCounter},
	}
	for _, tc := range cases {
		if got := seedKind(tc.kind); got != tc.want {
			t.Errorf("seedKind(%q) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// Warnings accompany an accepted (ok_cached) event, so the ledger must exclude them.
func TestIngestionStatusName(t *testing.T) {
	cases := map[int32]string{
		10: "ok_cached",
		23: "err_nan_inf_value",
		25: "err_negative_counter",
		61: "err_too_big_value",
		62: "err_zero_counter",
		55: "warn_timestamp_clamped_past",
		33: "warn_tag_not_found",
	}
	for id, want := range cases {
		if got := ingestionStatusName(id); got != want {
			t.Errorf("ingestionStatusName(%d) = %q, want %q", id, got, want)
		}
	}
	if got := ingestionStatusName(999); got != "status_999" {
		t.Errorf("ingestionStatusName(999) = %q, want status_999 (forward-compat sentinel)", got)
	}
	warns := []int32{33, 46, 47, 52, 53, 55, 56, 59}
	for _, id := range warns {
		if !isWarnStatus(id) {
			t.Errorf("isWarnStatus(%d) = false, want true", id)
		}
	}
	notWarns := []int32{10, 21, 23, 24, 25, 34, 35, 36, 39, 42, 48, 49, 50, 54, 57, 60, 61, 62, 63}
	for _, id := range notWarns {
		if isWarnStatus(id) {
			t.Errorf("isWarnStatus(%d) = true, want false", id)
		}
	}
}

// key1 is the metric name (must be in known), key2 the status ID; value-type fallbacks
// cover key-name drift, and unknown metrics or sentinels come back empty.
func TestClassifyIngestionSeries(t *testing.T) {
	known := map[string]bool{"e2e_run_go_v_nan": true, "e2e_run_go_c_zero": true}
	cases := []struct {
		name       string
		tags       map[string]apiMetaTag
		wantMetric string
		wantStatus int32
	}{
		{
			"metric + status id",
			map[string]apiMetaTag{"key1": {"e2e_run_go_v_nan"}, "key2": {" 23"}},
			"e2e_run_go_v_nan", 23,
		},
		{
			"ok_cached id",
			map[string]apiMetaTag{"key1": {"e2e_run_go_c_zero"}, "key2": {"10"}},
			"e2e_run_go_c_zero", 10,
		},
		{
			// env (key0) is neither known nor numeric → ignored.
			"env does not masquerade as metric",
			map[string]apiMetaTag{"key0": {"production_env"}, "key1": {"e2e_run_go_v_nan"}, "key2": {"23"}},
			"e2e_run_go_v_nan", 23,
		},
		{
			// Leading-space rendering is trimmed before matching.
			"leading-space id and metric match after trim",
			map[string]apiMetaTag{"key1": {" e2e_run_go_c_zero"}, "key2": {" 62"}},
			"e2e_run_go_c_zero", 62,
		},
		{
			// Another run's metric: the series is dropped.
			"unknown metric dropped",
			map[string]apiMetaTag{"key1": {"e2e_OTHER_RUN_v_nan"}, "key2": {"10"}},
			"", 10,
		},
		{
			"sentinels skipped",
			map[string]apiMetaTag{"key0": {"0"}, "key3": {" 0"}, "key4": {""}, "key1": {"e2e_run_go_c_zero"}, "key2": {"62"}},
			"e2e_run_go_c_zero", 62,
		},
		{
			// Non-standard keys classify by known membership and integer parse.
			"value-type fallback",
			map[string]apiMetaTag{"keyX": {"e2e_run_go_v_nan"}, "keyY": {" 61"}},
			"e2e_run_go_v_nan", 61,
		},
		{
			"no status tag → statusID 0",
			map[string]apiMetaTag{"key1": {"e2e_run_go_v_nan"}},
			"e2e_run_go_v_nan", 0,
		},
		{
			"nothing recognizable",
			map[string]apiMetaTag{"key0": {"production_env"}},
			"", 0,
		},
	}
	for _, tc := range cases {
		gotMetric, gotStatus := classifyIngestionSeries(tc.tags, known)
		if gotMetric != tc.wantMetric || gotStatus != tc.wantStatus {
			t.Errorf("%s: classifyIngestionSeries = (%q, %d), want (%q, %d)",
				tc.name, gotMetric, gotStatus, tc.wantMetric, tc.wantStatus)
		}
	}
}

func TestLedgerBalance(t *testing.T) {
	byID := map[int32]float64{
		statusIDOKCached:    70,
		statusIDZeroCounter: 5,
		statusIDNanInfValue: 3,
		55:                  100, // warn_timestamp_clamped_past — excluded
		33:                  12,  // warn_tag_not_found — excluded
		36:                  2,   // err_map_tag_value
	}
	okCached, errSum, errs, warns := ledgerBalance(byID)
	if okCached != 70 {
		t.Errorf("okCached = %g, want 70", okCached)
	}
	if errSum != 10 { // 5 + 3 + 2
		t.Errorf("errSum = %g, want 10", errSum)
	}
	wantErrs := map[int32]float64{
		statusIDZeroCounter: 5,
		statusIDNanInfValue: 3,
		36:                  2,
	}
	if !reflect.DeepEqual(errs, wantErrs) {
		t.Errorf("errs = %v, want %v", errs, wantErrs)
	}
	// warns are reported for diagnostics but excluded from the balance
	wantWarns := map[int32]float64{
		55: 100,
		33: 12,
	}
	if !reflect.DeepEqual(warns, wantWarns) {
		t.Errorf("warns = %v, want %v", warns, wantWarns)
	}

	ok0, err0, errs0, warns0 := ledgerBalance(nil)
	if ok0 != 0 || err0 != 0 || len(errs0) != 0 || len(warns0) != 0 {
		t.Errorf("ledgerBalance(nil) = (%g, %g, %v, %v), want (0, 0, {}, {})", ok0, err0, errs0, warns0)
	}
}

func TestLedgerFailDetailWarns(t *testing.T) {
	errs := map[int32]float64{statusIDZeroCounter: 5, 36: 2} // 62, 36
	warns := map[int32]float64{55: 100, 33: 12}              // warn_*; 33 sorts before 55
	const qurl = "http://api:10888/api/query?s=__src_ingestion_status&n=1000"
	got := ledgerFailDetail("m", 70, 60, 7, errs, warns, qurl)

	assertContains(t, got, "ok_cached(10)=60")
	assertContains(t, got, "err_map_tag_value(36)=2")
	assertContains(t, got, "err_zero_counter(62)=5")

	assertContains(t, got, "warn_tag_not_found(33)=12 (warning — accepted, not a loss)")
	assertContains(t, got, "warn_timestamp_clamped_past(55)=100 (warning — accepted, not a loss)")

	assertContains(t, got, "url: "+qurl)

	errPos := strings.Index(got, "err_zero_counter(62)=5")
	warnPos := strings.Index(got, "warn_tag_not_found(33)=12")
	if errPos < 0 || warnPos < 0 || warnPos < errPos {
		t.Errorf("expected warn rows to render AFTER err rows\ndetail:\n%s", got)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("detail missing %q\ndetail:\n%s", want, got)
	}
}

// The ledger identity only holds where a write maps 1:1 to a wire item; clients split
// unique/value_p payloads into several items, so those kinds are excluded.
func TestLedgerEligibleKind(t *testing.T) {
	eligible := []string{kindCounter, kindStag, kindValue, kindValueNaN, kindValueInf}
	for _, k := range eligible {
		if !ledgerEligibleKind(k) {
			t.Errorf("ledgerEligibleKind(%q) = false, want true (1:1 write→item)", k)
		}
	}
	for _, k := range []string{kindUnique, kindValueP} {
		if ledgerEligibleKind(k) {
			t.Errorf("ledgerEligibleKind(%q) = true, want false (multi-value: client splits into N items)", k)
		}
	}
}

// Seeds are not in Writes, so cold-start metric-not-found accounting is excluded on both sides.
func TestLedgerWriteCounts(t *testing.T) {
	stream := metricStream{
		Writes: []metricWrite{
			{Kind: kindCounter, Metric: "m1"}, {Kind: kindCounter, Metric: "m1"}, {Kind: kindCounter, Metric: "m1"},
			{Kind: kindValue, Metric: "m2"}, {Kind: kindValue, Metric: "m2"},
			{Kind: kindValueNaN, Metric: "rej"}, // a rejection metric's writes are counted too
			// multi-value kinds are excluded: item count != write count
			{Kind: kindUnique, Metric: "u_split"}, {Kind: kindUnique, Metric: "u_split"},
			{Kind: kindValueP, Metric: "vp_split"},
		},
	}
	want := map[string]int{"m1": 3, "m2": 2, "rej": 1} // u_split/vp_split absent
	if got := ledgerWriteCounts(stream); !reflect.DeepEqual(got, want) {
		t.Errorf("ledgerWriteCounts = %v, want %v (unique/value_p excluded)", got, want)
	}
}

func TestKnownMetricNames(t *testing.T) {
	stream := metricStream{
		Metrics: []metricModel{{Name: "m1"}, {Name: "m2"}},
		Rejections: []rejectionMetric{
			{Name: "rej_a"},
			{Name: "rej_b"},
		},
	}
	want := map[string]bool{"m1": true, "m2": true, "rej_a": true, "rej_b": true}
	if got := knownMetricNames(stream); !reflect.DeepEqual(got, want) {
		t.Errorf("knownMetricNames = %v, want %v", got, want)
	}
}

func TestLedgerConverged(t *testing.T) {
	want := map[string]int{"m1": 70}
	cases := []struct {
		name string
		bd   map[string]map[int32]float64
		want bool
	}{
		{"balanced all-ok", map[string]map[int32]float64{"m1": {statusIDOKCached: 70}}, true},
		{"balanced ok+err", map[string]map[int32]float64{"m1": {statusIDOKCached: 60, statusIDZeroCounter: 10}}, true},
		{"balanced all-rejected", map[string]map[int32]float64{"m1": {statusIDNanInfValue: 70}}, true},
		{"undershoot (loss)", map[string]map[int32]float64{"m1": {statusIDOKCached: 60}}, false},
		{"overshoot (double-count)", map[string]map[int32]float64{"m1": {statusIDOKCached: 80}}, false},
		{"metric absent", map[string]map[int32]float64{}, false},
		{"warn does not help balance", map[string]map[int32]float64{"m1": {statusIDOKCached: 60, 55: 10}}, false},
	}
	for _, tc := range cases {
		if got := ledgerConverged(want, tc.bd); got != tc.want {
			t.Errorf("%s: ledgerConverged = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A Sent==false rejection is a client-side drop with no server status to wait for.
func TestRejectionsConverged(t *testing.T) {
	rejections := []rejectionMetric{
		{Name: "r_sent", StatusID: statusIDZeroCounter, Writes: 5, Sent: true},
		{Name: "r_dropped", StatusID: statusIDNegCounter, Writes: 5, Sent: false}, // client dropped it
	}
	if !rejectionsConverged(rejections, map[string]map[int32]float64{
		"r_sent": {statusIDZeroCounter: 5},
	}) {
		t.Error("expected convergence: sent rejection reached its count, dropped one is unconstrained")
	}
	if rejectionsConverged(rejections, map[string]map[int32]float64{
		"r_sent": {statusIDZeroCounter: 3}, // short of 5
	}) {
		t.Error("expected non-convergence: sent rejection short of its count")
	}
}

// go and rust drop count<=0 before the wire, so their zero/negative counter rejections
// are recorded as skips (Sent==false, no writes); cpp sends them.
func TestAddRejectionsPerClient(t *testing.T) {
	cases := []struct {
		clientTag  string
		wantKinds  []string // kinds in generation order, parallel to wantNames
		wantNames  []string // suffixes
		wantStatus []string
		wantIDs    []int32
		wantSent   []bool // per-rejection Sent flag
		wantWrites []int  // per-rejection write count appended to the builder
	}{
		{
			goClientTag,
			[]string{kindCounter, kindCounter, kindValueNaN, kindValueInf},
			[]string{"c_zero", "c_neg", "v_nan", "v_inf"},
			[]string{statusNameZeroCounter, statusNameNegCounter, statusNameNanInfValue, statusNameTooBigValue},
			[]int32{statusIDZeroCounter, statusIDNegCounter, statusIDNanInfValue, statusIDTooBigValue},
			[]bool{false, false, true, true}, // go drops count<=0 → c_zero/c_neg are SKIPs
			[]int{0, 0, numBuckets, numBuckets},
		},
		{
			rustClientTag,
			[]string{kindCounter, kindCounter, kindValueNaN, kindValueInf},
			[]string{"c_zero", "c_neg", "v_nan", "v_inf"},
			[]string{statusNameZeroCounter, statusNameNegCounter, statusNameNanInfValue, statusNameTooBigValue},
			[]int32{statusIDZeroCounter, statusIDNegCounter, statusIDNanInfValue, statusIDTooBigValue},
			[]bool{false, false, true, true}, // rust drops count<=0 → c_zero/c_neg are SKIPs
			[]int{0, 0, numBuckets, numBuckets},
		},
		{
			cppClientTag,
			[]string{kindCounter, kindCounter, kindValueNaN, kindValueInf},
			[]string{"c_zero", "c_neg", "v_nan", "v_inf"},
			[]string{statusNameZeroCounter, statusNameNegCounter, statusNameNanInfValue, statusNameTooBigValue},
			[]int32{statusIDZeroCounter, statusIDNegCounter, statusIDNanInfValue, statusIDTooBigValue},
			[]bool{true, true, true, true}, // cpp sends count<=0 → all real
			[]int{numBuckets, numBuckets, numBuckets, numBuckets},
		},
	}
	for _, tc := range cases {
		const prefix = "e2e_run_"
		b := &streamBuilder{prefix: prefix + tc.clientTag + "_", base: 1000}
		b.addRejections(tc.clientTag)

		if len(b.rejections) != len(tc.wantNames) {
			t.Errorf("%s: got %d rejections, want %d", tc.clientTag, len(b.rejections), len(tc.wantNames))
			continue
		}
		wantWritesTotal := 0
		for i, r := range b.rejections {
			wantName := prefix + tc.clientTag + "_" + tc.wantNames[i]
			if r.Name != wantName {
				t.Errorf("%s rejection[%d]: Name = %q, want %q", tc.clientTag, i, r.Name, wantName)
			}
			if r.Kind != tc.wantKinds[i] {
				t.Errorf("%s rejection[%d]: Kind = %q, want %q", tc.clientTag, i, r.Kind, tc.wantKinds[i])
			}
			if r.StatusName != tc.wantStatus[i] {
				t.Errorf("%s rejection[%d]: StatusName = %q, want %q", tc.clientTag, i, r.StatusName, tc.wantStatus[i])
			}
			if r.StatusID != tc.wantIDs[i] {
				t.Errorf("%s rejection[%d]: StatusID = %d, want %d", tc.clientTag, i, r.StatusID, tc.wantIDs[i])
			}
			if r.Sent != tc.wantSent[i] {
				t.Errorf("%s rejection[%d] %q: Sent = %v, want %v", tc.clientTag, i, r.Name, r.Sent, tc.wantSent[i])
			}
			if r.Writes != tc.wantWrites[i] {
				t.Errorf("%s rejection[%d] %q: Writes = %d, want %d", tc.clientTag, i, r.Name, r.Writes, tc.wantWrites[i])
			}

			if !r.Sent && r.SkipReason == "" {
				t.Errorf("%s rejection[%d] %q: Sent=false but SkipReason is empty", tc.clientTag, i, r.Name)
			}
			if r.Sent && r.SkipReason != "" {
				t.Errorf("%s rejection[%d] %q: Sent=true but SkipReason=%q is set", tc.clientTag, i, r.Name, r.SkipReason)
			}
			wantWritesTotal += tc.wantWrites[i]
		}

		if len(b.writes) != wantWritesTotal {
			t.Errorf("%s: builder writes = %d, want %d (only Sent==true rejections append writes)",
				tc.clientTag, len(b.writes), wantWritesTotal)
		}
	}
}

// Sent rejections are seeded so auto-create provisions them before the rejected writes;
// a Sent==false one is not, since seeding would orphan an empty metric.
func TestStreamSeedsIncludesRejections(t *testing.T) {
	stream := metricStream{
		Metrics: []metricModel{{Name: "m_counter", Kind: kindCounter}, {Name: "m_value", Kind: kindValue}},
		Rejections: []rejectionMetric{
			{Name: "rej_nan", Kind: kindValueNaN, Sent: true},
			{Name: "rej_zero", Kind: kindCounter, Sent: true},
			{Name: "rej_skipped", Kind: kindCounter, Sent: false}, // client-side drop → never seeded
		},
	}
	seeds, names := streamSeeds(stream)

	if len(seeds) != 4 || len(names) != 4 {
		t.Fatalf("streamSeeds: got %d seeds/%d names, want 4/4 (Sent==false rejection not seeded)", len(seeds), len(names))
	}
	wantSeeds := map[string]string{
		"m_counter": kindCounter,
		"m_value":   kindValue,
		"rej_nan":   kindValue,   // value_nan seeds as value
		"rej_zero":  kindCounter, // counter seeds as counter
	}
	gotSeeds := map[string]string{}
	for _, s := range seeds {
		gotSeeds[s.Name] = s.Kind
	}
	if !reflect.DeepEqual(gotSeeds, wantSeeds) {
		t.Errorf("seeds = %v, want %v", gotSeeds, wantSeeds)
	}
	gotNames := map[string]bool{}
	for _, n := range names {
		gotNames[n] = true
	}
	for n := range wantSeeds {
		if !gotNames[n] {
			t.Errorf("name %q missing from names list %v", n, names)
		}
	}

	if gotNames["rej_skipped"] {
		t.Errorf("Sent==false rejection rej_skipped was seeded (it must not be — the client never sends it): names=%v", names)
	}
}
