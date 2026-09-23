package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// daemonCmds are the daemons to cross-compile. metadata needs cgo (sqlite0 is a
// cgo package), so it is built with a static C cross-link to still run on the
// alpine base; the rest build with CGO_ENABLED=0.
var daemonCmds = []daemonSpec{
	{bin: "statshouse-metadata", pkg: "./cmd/statshouse-metadata", cgo: true},
	{bin: "statshouse-agg", pkg: "./cmd/statshouse-agg", cgo: false},
	{bin: "statshouse-api", pkg: "./cmd/statshouse-api", cgo: false},
	{bin: "statshouse", pkg: "./cmd/statshouse", cgo: false},
}

// duckBuildTags: osusergo makes os/user read /etc/group directly, since a
// static glibc link cannot serve cgo getgrnam (no NSS modules) and the agg's
// fatal ChangeUserGroup would kill it at startup.
const duckBuildTags = "duckdb osusergo"

type daemonSpec struct {
	bin string
	pkg string
	cgo bool
	// duckDB marks the DuckDB-tagged aggregator, linked with duckDBExtLDFlags.
	duckDB bool
}

// buildDaemons cross-compiles the daemons for linux/<arch> into a cache dir
// shared across runs and returns it. A cached binary newer than the newest
// source file is reused.
func buildDaemons(ctx context.Context, repoRoot, arch string, backend storageBackend, log func(string, ...any)) (string, error) {
	binDir, err := daemonBinDir(arch)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", fmt.Errorf("create daemon bin cache %s: %w", binDir, err)
	}
	specs := daemonSpecsFor(backend)
	newest, err := newestSourceMtime(repoRoot)
	if err != nil {
		return "", fmt.Errorf("scan daemon source mtimes: %w", err)
	}
	start := time.Now()
	type pending struct {
		d   daemonSpec
		out string
	}
	var stale []pending
	for _, d := range specs {
		out := filepath.Join(binDir, d.bin)
		if fi, statErr := os.Stat(out); statErr == nil && fi.ModTime().After(newest) {
			continue
		}
		stale = append(stale, pending{d, out})
	}
	// Build concurrently so the slow cgo metadata build (sqlite3.c) overlaps the
	// pure-Go daemons; `go build` is safe to run concurrently on one GOCACHE.
	results := make([]error, len(stale))
	var wg sync.WaitGroup
	for i := range stale {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = buildOneDaemon(ctx, repoRoot, arch, stale[i].d, stale[i].out)
		}(i)
	}
	wg.Wait()
	// First error in declaration order, for a deterministic cause.
	for i := range stale {
		if results[i] != nil {
			return "", results[i]
		}
	}
	log("daemon binaries: %s (arch=%s, backend=%s, built %d/%d, %.1fs)", binDir, arch, backend, len(stale), len(specs), time.Since(start).Seconds())
	return binDir, nil
}

// buildOneDaemon runs `go build` for one command. cgo daemons link static so
// they run on the alpine base. The duck aggregator passes its link recipe via
// -extldflags: CGO_LDFLAGS lands before the package's own LDFLAGS on the link
// line, so it cannot satisfy DuckDB's archive.
func buildOneDaemon(ctx context.Context, repoRoot, arch string, d daemonSpec, out string) error {
	env := append(os.Environ(), "GOOS=linux", "GOARCH="+arch)
	args := []string{"build", "-o", out}
	switch {
	case d.duckDB:
		cc, err := crossCC(arch)
		if err != nil {
			return fmt.Errorf("build %s: %w", d.pkg, err)
		}
		ext, err := duckDBExtLDFlags(cc)
		if err != nil {
			return fmt.Errorf("build %s: %w", d.pkg, err)
		}
		args = append(args, "-tags", duckBuildTags, "-ldflags", "-s -extldflags '"+ext+"'")
		env = append(env, "CGO_ENABLED=1", "CC="+cc)
	case d.cgo:
		cc, err := crossCC(arch)
		if err != nil {
			return fmt.Errorf("build %s: %w", d.pkg, err)
		}
		env = append(env, "CGO_ENABLED=1", "CC="+cc, "CGO_LDFLAGS=-static -s")
	default:
		env = append(env, "CGO_ENABLED=0")
	}
	args = append(args, d.pkg)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = repoRoot
	cmd.Env = env
	var b strings.Builder
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("build %s: %w", d.pkg, ctx.Err())
		}
		return fmt.Errorf("build %s: %w\n%s", d.pkg, err, indent(b.String()))
	}
	return nil
}

// crossCC resolves a C compiler targeting linux/<arch>: the native cc on a
// matching Linux host, else a glibc cross-compiler (sqlite3.c needs glibc's
// LFS64 symbols, which musl lacks).
func crossCC(arch string) (string, error) {
	var cands []string
	if runtime.GOOS == "linux" && normalizeArch(runtime.GOARCH) == normalizeArch(arch) {
		cands = append(cands, "cc", "gcc")
	}
	switch normalizeArch(arch) {
	case "arm64":
		cands = append(cands, "aarch64-linux-gnu-gcc", "aarch64-unknown-linux-gnu-gcc")
	case "amd64":
		cands = append(cands, "x86_64-linux-gnu-gcc", "x86_64-unknown-linux-gnu-gcc")
	default:
		cands = append(cands, arch+"-linux-gnu-gcc")
	}
	for _, c := range cands {
		if lookPath(c) {
			return c, nil
		}
	}
	return "", fmt.Errorf("no C cross-compiler for linux/%s on PATH (tried %s); the metadata daemon needs CGO for sqlite. On macOS arm64 install one, e.g. `brew install aarch64-unknown-linux-gnu`", arch, strings.Join(cands, ", "))
}

func normalizeArch(a string) string {
	switch a {
	case "arm64", "aarch64":
		return "arm64"
	case "amd64", "x86_64":
		return "amd64"
	}
	return a
}

// newestSourceMtime returns the newest mtime among the repo's *.go, go.mod and
// go.sum files, skipping trees that do not feed the daemons.
func newestSourceMtime(repoRoot string) (time.Time, error) {
	skip := map[string]bool{
		".git": true, "node_modules": true, "e2e": true, ".scratch": true,
		"vendor": true, "localdebug": true,
	}
	var newest time.Time
	walkErr := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skip[d.Name()] {
				return filepath.SkipDir
			}
			// statshouse-ui/*.go compiles into the api, but statshouse-ui/build is
			// generated; other build/ dirs (internal/vkgo/build) are real source.
			if d.Name() == "build" && strings.HasSuffix(filepath.Dir(path), "statshouse-ui") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") && name != "go.mod" && name != "go.sum" {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if fi.ModTime().After(newest) {
			newest = fi.ModTime()
		}
		return nil
	})
	if walkErr != nil {
		return time.Time{}, walkErr
	}
	if newest.IsZero() {
		return time.Time{}, fmt.Errorf("no .go/go.mod/go.sum files found under %s", repoRoot)
	}
	return newest, nil
}

// daemonBinDir is the cross-run cache for the compiled daemon binaries.
func daemonBinDir(arch string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for daemon cache: %w", err)
	}
	return filepath.Join(home, ".cache", "statshouse-e2e", "bin", arch), nil
}
