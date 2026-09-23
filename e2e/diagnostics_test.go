package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Pure-logic tests for diagnostics, artifacts and --skip-client-build; the networked
// parts run only in the full `go run ./e2e`.

func TestFormatLedgerLine(t *testing.T) {
	cases := []struct {
		name        string
		sentWrites  int
		okCached    float64
		errSum      float64
		wantSub     string // the distinctive substring for the verdict
		wantVerdict string // "balanced" / "silent loss" / "over-counted"
	}{
		{"balanced all-ok", 70, 70, 0, "== sentWrites=70", "balanced"},
		{"balanced ok+err", 70, 60, 10, "== sentWrites=70", "balanced"},
		{"balanced all-rejected", 70, 0, 70, "== sentWrites=70", "balanced"},
		{"silent loss", 70, 60, 0, "< sentWrites=70", "silent loss"},
		{"silent loss partial err", 70, 60, 5, "< sentWrites=70", "silent loss"},
		{"over-count", 70, 80, 0, "> sentWrites=70", "over-counted"},
	}
	for _, tc := range cases {
		got := formatLedgerLine(tc.sentWrites, tc.okCached, tc.errSum)
		if !strings.Contains(got, tc.wantSub) {
			t.Errorf("%s: ledger line missing %q\ngot: %s", tc.name, tc.wantSub, got)
		}
		if !strings.Contains(got, tc.wantVerdict) {
			t.Errorf("%s: ledger line missing verdict %q\ngot: %s", tc.name, tc.wantVerdict, got)
		}
	}
}

// An unbalanced inline ledger line is printed before the ledger poll converges, so it
// carries the snapshot caveat to avoid being read as the final ruling.
func TestFormatLedgerLineCaveat(t *testing.T) {
	if got := formatLedgerLine(70, 70, 0); strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("balanced line must NOT carry the snapshot caveat: %q", got)
	}
	if got := formatLedgerLine(70, 60, 0); !strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("silent-loss line must carry the snapshot caveat: %q", got)
	}
	if got := formatLedgerLine(70, 80, 0); !strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("over-count line must carry the snapshot caveat: %q", got)
	}
}

func TestStreamSourceLabel(t *testing.T) {
	if got := streamSourceLabel(false); got != "generated" {
		t.Errorf("streamSourceLabel(false) = %q, want \"generated\"", got)
	}
	if got := streamSourceLabel(true); got != "replayed (cached build)" {
		t.Errorf("streamSourceLabel(true) = %q, want \"replayed (cached build)\"", got)
	}
}

// A stale binary from a different client ref or arch must never share a cache dir.
func TestClientBuildCacheDir(t *testing.T) {
	got := clientBuildCacheDir("/cache", "go", "abc123", "arm64")
	want := filepath.Join("/cache", "clientbuilds", "go@abc123__arm64")
	if got != want {
		t.Errorf("clientBuildCacheDir = %q, want %q", got, want)
	}

	if clientBuildCacheDir("/cache", "go", "abc123", "arm64") ==
		clientBuildCacheDir("/cache", "go", "def456", "arm64") {
		t.Error("different ref collapsed to the same cache dir")
	}
	if clientBuildCacheDir("/cache", "go", "abc123", "arm64") ==
		clientBuildCacheDir("/cache", "go", "abc123", "amd64") {
		t.Error("different arch collapsed to the same cache dir")
	}
}

// The ingestion-status window is anchored at the realtime client-phase start, not the
// historic stream base: the agent records these builtins at receive time, and a
// --skip-client-build replay keeps an old base.
func TestIngestionStatusURLWindow(t *testing.T) {
	const anchor uint32 = 1715000000
	got := ingestionStatusURL("api:10888", anchor)

	if !strings.Contains(got, fmt.Sprintf("f=%d", anchor)) {
		t.Errorf("ingestionStatusURL: window start must be the anchor f=%d, got %q", anchor, got)
	}

	wantEnd := anchor + ingestionStatusTail
	if !strings.Contains(got, fmt.Sprintf("t=%d", wantEnd)) {
		t.Errorf("ingestionStatusURL: window end must be anchor+ingestionStatusTail t=%d, got %q", wantEnd, got)
	}

	if strings.Contains(got, fmt.Sprintf("t=%d", anchor+numBuckets)) {
		t.Errorf("ingestionStatusURL: leaked the historic base+numBuckets window (numBuckets=%d): %q", numBuckets, got)
	}
}

// SourceHash and BaseImage must survive the round-trip so a replay can detect a
// template or toolchain change.
func TestStreamCacheMetaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const runID = "20260809-135000"
	const base uint32 = 1715000000
	const srcHash = "abc0def1"
	const baseImage = "golang:1.22-alpine"
	if err := saveStreamCacheMeta(dir, runID, base, "rust", "arm64", srcHash, baseImage); err != nil {
		t.Fatalf("saveStreamCacheMeta: %v", err)
	}
	got, err := loadStreamCacheMeta(dir)
	if err != nil {
		t.Fatalf("loadStreamCacheMeta: %v", err)
	}
	if got.RunID != runID || got.Base != base || got.ClientTag != "rust" || got.Arch != "arm64" {
		t.Errorf("round-trip mismatch: got %+v, want {RunID:%s Base:%d ClientTag:rust Arch:arm64}", got, runID, base)
	}
	if got.SourceHash != srcHash {
		t.Errorf("SourceHash round-trip: got %q, want %q (F2 cache-invalidation fingerprint lost)", got.SourceHash, srcHash)
	}
	if got.BaseImage != baseImage {
		t.Errorf("BaseImage round-trip: got %q, want %q (F2 toolchain tag lost)", got.BaseImage, baseImage)
	}

	if _, err := os.Stat(filepath.Join(dir, streamJSONName)); err != nil {
		t.Errorf("stream descriptor not written at %s: %v", filepath.Join(dir, streamJSONName), err)
	}
}

func TestLoadStreamCacheMetaMissing(t *testing.T) {
	if _, err := loadStreamCacheMeta(t.TempDir()); err == nil {
		t.Error("loadStreamCacheMeta on a missing descriptor: want error, got nil")
	}
}

// Replay reconstructs now = base+120; if that stops reproducing base, the cached driver
// binary (with base-derived timestamps) desyncs from the regenerated model.
func TestGenerateStreamBaseInvariant(t *testing.T) {
	const base uint32 = 1715000000
	now := time.Unix(int64(base)+120, 0)
	s := generateStream("run", "go", now)
	if s.Base != base {
		t.Errorf("generateStream(base+120).Base = %d, want %d (replay invariant broken)", s.Base, base)
	}

	s2 := generateStream("run", "go", now)
	if s.Base != s2.Base || len(s.Writes) != len(s2.Writes) || len(s.Metrics) != len(s2.Metrics) {
		t.Error("generateStream is not deterministic across calls with identical inputs")
	}
}

// goDriver returns the real go driver so replay tests run the live cache validation.
func goDriver(t *testing.T) clientDriver {
	t.Helper()
	for _, d := range clientDrivers {
		if d.tag == goClientTag {
			return d
		}
	}
	t.Fatalf("no %q driver in clientDrivers registry", goClientTag)
	return clientDriver{}
}

// renderSourceHashFor returns the hash validateSkipClientBuildCache expects for a descriptor.
func renderSourceHashFor(t *testing.T, d clientDriver, repo string, runID, clientTag string, base uint32) string {
	t.Helper()
	stream := generateStream(runID, clientTag, time.Unix(int64(base)+120, 0))
	src, err := d.renderSource(repo, stream)
	if err != nil {
		t.Fatalf("render driver source for staged descriptor: %v", err)
	}
	return sourceHash(src)
}

func TestStreamForClientPhaseReplay(t *testing.T) {
	d := goDriver(t)
	repo, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const base uint32 = 1715000000

	fresh, cached, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo}, t.TempDir())
	if err != nil {
		t.Fatalf("normal run: %v", err)
	}
	if cached {
		t.Error("normal run: cached=true, want false")
	}
	if fresh.Base == 0 {
		t.Error("normal run: stream.Base == 0 (expected a real base)")
	}
	wantFreshPrefix := "e2e_new_" + goClientTag + "_"
	if len(fresh.Metrics) == 0 || !strings.HasPrefix(fresh.Metrics[0].Name, wantFreshPrefix) {
		t.Errorf("normal run: metric names must embed the current runID, first=%q (want prefix %q)",
			firstMetricName(fresh), wantFreshPrefix)
	}

	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, t.TempDir()); err == nil {
		t.Error("skip with no descriptor: want error, got nil")
	}

	stage := func(t *testing.T, d clientDriver, metaRunID string, base uint32, clientTag, arch string) string {
		t.Helper()
		dir := t.TempDir()
		hash := renderSourceHashFor(t, d, repo, metaRunID, clientTag, base)
		if err := saveStreamCacheMeta(dir, metaRunID, base, clientTag, arch, hash, d.baseImage); err != nil {
			t.Fatalf("saveStreamCacheMeta: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, driverBinName), []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatalf("write dummy driver: %v", err)
		}
		return dir
	}

	// empty hash/baseImage is fine: the binary-exists check runs first
	dir := t.TempDir()
	_ = saveStreamCacheMeta(dir, "old", base, goClientTag, "arm64", "", "")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), "driver binary") {
		t.Errorf("skip with descriptor but no binary: want error mentioning driver binary, got %v", err)
	}

	dir = stage(t, d, "old", base, rustClientTag, "arm64")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), goClientTag) {
		t.Errorf("skip with client mismatch: want error mentioning %q, got %v", goClientTag, err)
	}

	dir = stage(t, d, "old", base, goClientTag, "amd64")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), "arch") {
		t.Errorf("skip with arch mismatch: want error mentioning arch, got %v", err)
	}

	dir = stage(t, d, "old", base, goClientTag, "arm64")
	got, cached, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir)
	if err != nil {
		t.Fatalf("skip with matching cache: %v", err)
	}
	if !cached {
		t.Error("skip with matching cache: cached=false, want true")
	}
	if got.Base != base {
		t.Errorf("regenerated stream Base = %d, want %d (exact replay)", got.Base, base)
	}
	// names embed the descriptor's runID: the cached binary was compiled from it
	wantPrefix := "e2e_old_" + goClientTag + "_"
	if len(got.Metrics) == 0 || !strings.HasPrefix(got.Metrics[0].Name, wantPrefix) {
		t.Errorf("regenerated metric names must embed the DESCRIPTOR runID, first=%q (want prefix %q, NOT e2e_new_%s_)",
			firstMetricName(got), wantPrefix, goClientTag)
	}
}

// A replay must refuse when the rendered driver source or base image no longer matches
// the descriptor, so a stale binary never runs against a different model.
func TestStreamForClientPhaseReplayTemplateChange(t *testing.T) {
	d := goDriver(t)
	repo, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const base uint32 = 1715000000

	stage := func(t *testing.T, srcHash, baseImage string) string {
		t.Helper()
		dir := t.TempDir()
		if err := saveStreamCacheMeta(dir, "old", base, goClientTag, "arm64", srcHash, baseImage); err != nil {
			t.Fatalf("saveStreamCacheMeta: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, driverBinName), []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatalf("write dummy driver: %v", err)
		}
		return dir
	}

	skipOpts := clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}

	staleHashDir := stage(t, "0000000000000000000000000000000000000000000000000000000000000bad", d.baseImage)
	_, _, err = streamForClientPhase(d, skipOpts, staleHashDir)
	if err == nil || !strings.Contains(err.Error(), "template changed since this binary was built") {
		t.Errorf("template drift: want refusal mentioning template change, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "--skip-client-build") {
		t.Errorf("template drift: want actionable --skip-client-build guidance, got %v", err)
	}

	wrongImageDir := stage(t, renderSourceHashFor(t, d, repo, "old", goClientTag, base), "rust:9.99-notreal")
	_, _, err = streamForClientPhase(d, skipOpts, wrongImageDir)
	if err == nil || !strings.Contains(err.Error(), "base image changed since this binary was built") {
		t.Errorf("toolchain drift: want refusal mentioning base image change, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "--skip-client-build") {
		t.Errorf("toolchain drift: want actionable --skip-client-build guidance, got %v", err)
	}

	// the real hash + image is accepted, so the guard discriminates
	realHash := renderSourceHashFor(t, d, repo, "old", goClientTag, base)
	goodDir := stage(t, realHash, d.baseImage)
	if _, cached, err := streamForClientPhase(d, skipOpts, goodDir); err != nil || !cached {
		t.Errorf("matching descriptor: want cached=true no error, got cached=%v err=%v", cached, err)
	}
}

func firstMetricName(s metricStream) string {
	if len(s.Metrics) == 0 {
		return ""
	}
	return s.Metrics[0].Name
}

func TestSanitizeFileName(t *testing.T) {
	cases := map[string]string{
		"go":              "go",
		"count":           "count",
		"__src_ingestion": "__src_ingestion", // builtin "__" prefix preserved
		"":                "_",
		"p50":             "p50",
	}
	for in, want := range cases {
		if got := sanitizeFileName(in); got != want {
			t.Errorf("sanitizeFileName(%q) = %q, want %q", in, got, want)
		}
	}

	for _, in := range []string{"a/b", "a b", "café", "東 京"} {
		got := sanitizeFileName(in)
		if strings.ContainsAny(got, "/ \x00") {
			t.Errorf("sanitizeFileName(%q) = %q (still contains a path separator / space / NUL)", in, got)
		}
	}
}
