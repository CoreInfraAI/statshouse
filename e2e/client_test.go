package main

import (
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRenderGoDriverQuoting: every injected string must be %q-quoted, or an
// embedded quote breaks the rendered Go and go/format rejects it.
func TestRenderGoDriverQuoting(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	tmplPath := filepath.Join(root, "e2e", driverGoDir, "main.go.tmpl")

	const (
		trickyMetric = "e2e_\"quote\"_back\\slash_café_東京"
		trickyVal    = "v\"with quote and a \\ backslash"
	)
	base := uint32(1_700_000_000)
	stream := metricStream{
		Base: base,
		Writes: []metricWrite{
			{Kind: kindCounter, Metric: trickyMetric, Tags: []tag{{"0", trickyVal}}, Count: 1, TS: base},
		},
		Metrics: []metricModel{{Name: trickyMetric, Kind: kindCounter}},
	}

	out := t.TempDir()
	if err := renderGoDriver(tmplPath, stream, out); err != nil {
		t.Fatalf("renderGoDriver: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(out, "main.go"))
	if err != nil {
		t.Fatalf("read rendered driver: %v", err)
	}

	if _, err := format.Source(src); err != nil {
		t.Fatalf("rendered driver is not valid Go: %v\n%s", err, src)
	}
	// %q keeps printable Unicode as-is, so this confirms the values were injected.
	s := string(src)
	if !strings.Contains(s, "caf") || !strings.Contains(s, "東京") {
		t.Errorf("rendered source lost the unicode parts of the injected value")
	}
}

// TestClassifyCloneCache: a git probe that failed under a cancelled context must
// abort rather than tear down a healthy checkout.
func TestClassifyCloneCache(t *testing.T) {
	var (
		errHead    = errors.New("head probe failed")
		errResolve = errors.New("resolve probe failed")
		shaA       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	cases := []struct {
		name       string
		head       string
		headErr    error
		resolved   string
		resolveErr error
		ctxCancel  bool
		want       cloneAction
	}{
		{"exact match → reuse", shaA, nil, shaA, nil, false, cloneReuse},
		{"HEAD ≠ ref → reclone", shaA, nil, shaB, nil, false, cloneReclone},
		{"ref unresolvable → reclone", shaA, nil, "", errResolve, false, cloneReclone},
		{"not a repo (empty head) → reclone", "", nil, "", nil, false, cloneReclone},
		{"unreadable head → reclone", "", errHead, "", nil, false, cloneReclone},

		{"head probe failed + cancelled → abort (keep cache)", shaA, errHead, "", nil, true, cloneAbort},
		{"resolve probe failed + cancelled → abort (keep cache)", shaA, nil, "", errResolve, true, cloneAbort},

		// A probe that completed is trusted even if the context is now cancelled.
		{"completed + later cancel, exact match → reuse", shaA, nil, shaA, nil, true, cloneReuse},
		{"completed + later cancel, mismatch → reclone", shaA, nil, shaB, nil, true, cloneReclone},
	}
	for _, tc := range cases {
		got := classifyCloneCache(tc.head, tc.headErr, tc.resolved, tc.resolveErr, tc.ctxCancel)
		if got != tc.want {
			t.Errorf("%s: classifyCloneCache(%q,%v,%q,%v,%v) = %v, want %v",
				tc.name, tc.head, tc.headErr, tc.resolved, tc.resolveErr, tc.ctxCancel, got, tc.want)
		}
	}
}

// TestDriverLCGIdentity: the skewed-value LCG constants in quantile.go are
// hand-copied into all three driver templates; a one-sided edit would silently
// desync the clients from the expected model.
func TestDriverLCGIdentity(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}

	tokens := map[string]string{
		"seed":    fmt.Sprintf("%x", lcgSeed), // hex form used verbatim in all three
		"mul":     strconv.FormatUint(lcgMul, 10),
		"add":     strconv.FormatUint(lcgAdd, 10),
		"modulus": strconv.Itoa(skewedRange),
	}
	base := uint32(1_700_000_000)
	// Only a valueSkewed write makes a template render its LCG block.
	stream := metricStream{
		Base: base,
		Writes: []metricWrite{{
			Kind: kindValueP, Metric: "e2e_lcg_probe_v", Tags: []tag{{"0", "s"}}, TS: base,
			Gen: &genSpec{Kind: genKindValueSkewed, N: 4},
		}},
		Metrics: []metricModel{{Name: "e2e_lcg_probe_v", Kind: kindValueP, QBKeys: []string{"0"}}},
	}

	drivers := []struct {
		name   string
		render func(tmplPath string, stream metricStream, outDir string) error
		tmpl   string // template dir+file, relative to e2e/
		out    string // rendered output file name
	}{
		{"go", renderGoDriver, filepath.Join(driverGoDir, "main.go.tmpl"), "main.go"},
		{"rust", renderRustDriver, filepath.Join(driverRustDir, "main.rs.tmpl"), "main.rs"},
		{"cpp", renderCppDriver, filepath.Join(driverCppDir, "main.cpp.tmpl"), "main.cpp"},
	}
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			out := t.TempDir()
			if err := d.render(filepath.Join(root, "e2e", d.tmpl), stream, out); err != nil {
				t.Fatalf("render %s driver: %v", d.name, err)
			}
			src, err := os.ReadFile(filepath.Join(out, d.out))
			if err != nil {
				t.Fatalf("read rendered %s driver: %v", d.name, err)
			}
			s := string(src)
			for what, tok := range tokens {
				if !strings.Contains(s, tok) {
					t.Errorf("%s driver: rendered source missing LCG %s token %q — desynced from quantile.go", d.name, what, tok)
				}
			}
		})
	}
}
