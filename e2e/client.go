package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// The go-client path: render the stream into a synthetic driver module,
// pre-resolve its dependencies on the HOST (containers have no internet), then
// build and run it in a pinned golang container.

const (
	// goBaseImage is multi-arch; the build asserts go env GOARCH == runtime arch.
	goBaseImage = "golang:1.22-alpine"

	goClientName = "statshouse-go" // the active client in e2e/clients.txt
	driverGoDir  = "drivers/go"    // driver template dir, relative to e2e/
	clientModule = "github.com/VKCOM/statshouse-go"

	// preWarmExit is the driver exit code when its cold-start pre-warm poll times
	// out (agent↔aggregator path down). Distinct from 1 (close error) and 2
	// (GOARCH mismatch) so runClientPhase can report one clear cause. Injected
	// into every template as {{.PreWarmExit}}.
	preWarmExit = 3

	// In-container mount points.
	workMount    = "/work"
	clientMount  = "/client"
	modMount     = "/gomodcache"
	gocacheMount = "/gocache"

	// --skip-client-build cache: the driver binary plus a descriptor of the
	// (runID, base) it was compiled from, so the exact stream it embeds can be
	// regenerated for the expected model.
	driverBinName  = "driver"
	streamJSONName = "stream.json"
	driverBinMount = "/driverbin"
)

// clientSpec is one ACTIVE (uncommented) client parsed from e2e/clients.txt.
type clientSpec struct {
	Name string
	URL  string
	Ref  string
}

// clientRunOpts configures the per-language build+run functions.
type clientRunOpts struct {
	stream     metricStream
	spec       clientSpec
	network    string
	agentAddr  string // STATSHOUSE_ADDR
	apiAddr    string // STATSHOUSE_API_ADDR, polled to confirm mappings; "" → fixed fallback
	container  string
	workDir    string // host dir with the rendered driver source
	repoRoot   string
	arch       string // expected GOARCH for the go image's self-check
	cache      string // e2e cache root (~/.cache/statshouse-e2e)
	buildCache string // cached driver binary + stream descriptor
	skipBuild  bool   // --skip-client-build
}

// parseClientsTxt reads the active client lines (name url ref) from e2e/clients.txt.
func parseClientsTxt(path string) ([]clientSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var specs []clientSpec
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 3 {
			return nil, fmt.Errorf("%s: malformed line %q (want 3 fields: name url ref)", path, line)
		}
		specs = append(specs, clientSpec{Name: fs[0], URL: fs[1], Ref: fs[2]})
	}
	return specs, sc.Err()
}

func findClient(specs []clientSpec, name string) (clientSpec, bool) {
	for _, s := range specs {
		if s.Name == name {
			return s, true
		}
	}
	return clientSpec{}, false
}

// e2eCacheDir is the cross-run cache root: ~/.cache/statshouse-e2e/.
func e2eCacheDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for e2e cache: %w", err)
	}
	return filepath.Join(home, ".cache", "statshouse-e2e"), nil
}

// cloneDir is the cached checkout path: <cache>/clients/<name>@<ref>.
func (c clientSpec) cloneDir() (string, error) {
	cache, err := e2eCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cache, "clients", c.Name+"@"+c.Ref), nil
}

// ensureCloned makes the pinned ref available at cloneDir, reusing a checkout
// already at the ref. Runs on the HOST: containers have no network egress.
func (c clientSpec) ensureCloned(ctx context.Context, log func(string, ...any)) (string, error) {
	dir, err := c.cloneDir()
	if err != nil {
		return "", err
	}
	// The ref may be a short SHA, full SHA, branch or tag, so resolve it to a
	// full SHA before comparing with HEAD.
	head, herr := gitHead(ctx, dir)
	resolved, rerr := gitResolveRef(ctx, dir, c.Ref)
	switch classifyCloneCache(head, herr, resolved, rerr, ctx.Err() != nil) {
	case cloneReuse:
		log("%s cached @ %s (reusing %s)", c.Name, c.Ref, dir)
		return dir, nil
	case cloneAbort:
		// Cancelled probe: leave the cache intact.
		if herr != nil {
			return "", fmt.Errorf("inspect cached checkout %s: %w", dir, herr)
		}
		return "", fmt.Errorf("resolve ref %s in cached checkout %s: %w", c.Ref, dir, rerr)
	case cloneReclone:
		switch {
		case herr != nil:
			log("%s: cached checkout unreadable (%v); re-cloning", c.Name, herr)
		case rerr != nil:
			log("%s: could not resolve ref %s in cache (%v); re-cloning", c.Name, c.Ref, rerr)
		default:
			log("%s: cache @ %s not at ref %s; re-cloning", c.Name, head, c.Ref)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("clear stale checkout %s: %w", dir, err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", err
	}
	log("cloning %s @ %s into %s", c.Name, c.Ref, dir)
	// Full clone, then checkout: GitHub rejects `git fetch origin <sha>` for a
	// short SHA, so fetch by name only when the ref is not in the clone.
	if _, err := runOK(ctx, "git", "clone", c.URL, dir); err != nil {
		return "", fmt.Errorf("clone %s: %w", c.URL, err)
	}
	if _, err := runOK(ctx, "git", "-C", dir, "checkout", c.Ref); err != nil {
		// Not in the clone: fetch by name and retry.
		if _, ferr := runOK(ctx, "git", "-C", dir, "fetch", "origin", c.Ref); ferr != nil {
			return "", fmt.Errorf("checkout %s: not in clone, and fetch failed: %w", c.Ref, ferr)
		}
		if _, err := runOK(ctx, "git", "-C", dir, "checkout", c.Ref); err != nil {
			return "", fmt.Errorf("checkout %s: %w", c.Ref, err)
		}
	}
	return dir, nil
}

type cloneAction int

const (
	cloneReuse   cloneAction = iota
	cloneReclone             // stale/missing/unreadable
	cloneAbort               // probe cancelled: keep the cache, surface the error
)

// classifyCloneCache decides what to do with a cached checkout. A probe that
// failed under a cancelled context (run deadline, signal) is inconclusive and
// must never tear down a healthy cache; a completed probe is trusted.
func classifyCloneCache(head string, headErr error, resolved string, resolveErr error, ctxCancelled bool) cloneAction {
	probeFailed := headErr != nil || resolveErr != nil
	if probeFailed && ctxCancelled {
		return cloneAbort
	}
	if probeFailed || head == "" {
		return cloneReclone // unreadable, ref unresolvable, or not a repo
	}
	if resolved != "" && head == resolved {
		return cloneReuse
	}
	return cloneReclone // HEAD ≠ configured ref
}

// gitHead returns the commit a checkout is at, or "" if dir is not a git repo.
// A git failure is returned for the caller to classify, not treated as "not a repo".
func gitHead(ctx context.Context, dir string) (string, error) {
	if !fileExists(filepath.Join(dir, ".git")) {
		return "", nil
	}
	res, err := runOK(ctx, "git", "-C", dir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res), nil
}

// gitResolveRef resolves ref to a full commit SHA inside the checkout at dir,
// or ("", nil) if dir is not a repo.
func gitResolveRef(ctx context.Context, dir, ref string) (string, error) {
	if !fileExists(filepath.Join(dir, ".git")) {
		return "", nil
	}
	res, err := runOK(ctx, "git", "-C", dir, "rev-parse", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res), nil
}

// driverTemplateData is injected into every driver template. SeedTS lies
// outside the asserted window. The quantile LCG constants are literals in each
// template instead, pinned to quantile.go by TestDriverLCGIdentity.
type driverTemplateData struct {
	Writes      []metricWrite
	Seeds       []seedDef
	Metrics     []string
	SeedTS      uint32
	PreWarmExit int
}

// renderGoDriverSource renders and gofmts the go driver template without
// writing it, so the --skip-client-build cache can hash the source.
func renderGoDriverSource(tmplPath string, stream metricStream) (string, error) {
	src, err := renderDriverSource(tmplPath, "go-driver", nil, stream)
	if err != nil {
		return "", err
	}
	formatted, err := format.Source([]byte(src))
	if err != nil {
		return "", fmt.Errorf("gofmt rendered driver: %w\n%s", err, src)
	}
	return string(formatted), nil
}

// renderGoDriver renders the go driver into <outDir>/main.go. gofmt surfaces a
// template bug here rather than later in the container build.
func renderGoDriver(tmplPath string, stream metricStream, outDir string) error {
	src, err := renderGoDriverSource(tmplPath, stream)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, "main.go"), []byte(src), 0o644)
}

// renderDriverSource renders a driver template with the language-specific
// escapers in funcs, so a quote, backslash or non-ASCII byte in an injected
// value can never break the source. It does not write, so the source can be hashed.
func renderDriverSource(tmplPath, name string, funcs template.FuncMap, stream metricStream) (string, error) {
	tmplText, err := os.ReadFile(tmplPath)
	if err != nil {
		return "", fmt.Errorf("read driver template %s: %w", tmplPath, err)
	}
	t, err := template.New(name).Funcs(funcs).Parse(string(tmplText))
	if err != nil {
		return "", fmt.Errorf("parse driver template %s: %w", tmplPath, err)
	}
	seeds, names := streamSeeds(stream)
	var raw bytes.Buffer
	if err := t.Execute(&raw, driverTemplateData{
		Writes:      stream.Writes,
		Seeds:       seeds,
		Metrics:     names,
		SeedTS:      stream.Base - 60,
		PreWarmExit: preWarmExit,
	}); err != nil {
		return "", fmt.Errorf("render driver: %w", err)
	}
	return raw.String(), nil
}

// renderDriver renders a rust/cpp driver template into <outDir>/<outFile>.
func renderDriver(tmplPath, name string, funcs template.FuncMap, stream metricStream, outDir, outFile string) error {
	src, err := renderDriverSource(tmplPath, name, funcs, stream)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, outFile), []byte(src), 0o644)
}

// floatLit renders a full-precision float literal, appending ".0" to a whole
// number so it is typed as a float (Rust types a bare integer as i32).
func floatLit(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// escapeLitBody renders s as the body of a double-quoted literal: " and \ are
// escaped, printable ASCII passes through, and every other byte is formatted
// with nonASCIIFormat (e.g. `\x%02x` for Rust, `\%03o` for C/C++).
func escapeLitBody(s, nonASCIIFormat string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b.WriteString(`\"`)
		case c == '\\':
			b.WriteString(`\\`)
		case c >= 0x20 && c <= 0x7e:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, nonASCIIFormat, c)
		}
	}
	return b.String()
}

// writeGoMod writes the driver module's go.mod, replacing the client module
// with replaceTarget.
func writeGoMod(workDir, replaceTarget string) error {
	gomod := "module main\n\n" +
		"go 1.21\n\n" +
		"require " + clientModule + " v0.0.0-00010101000000-000000000000\n\n" +
		"replace " + clientModule + " => " + replaceTarget + "\n"
	return os.WriteFile(filepath.Join(workDir, "go.mod"), []byte(gomod), 0o644)
}

// copyGoSum copies the pinned client's go.sum into the driver module.
func copyGoSum(workDir, clientGoSum string) error {
	data, err := os.ReadFile(clientGoSum)
	if err != nil {
		return fmt.Errorf("read client go.sum %s: %w", clientGoSum, err)
	}
	return os.WriteFile(filepath.Join(workDir, "go.sum"), data, 0o644)
}

// resolveGoModules populates the shared GOMODCACHE on the HOST for the offline
// (GOPROXY=off) container build. Bare `go mod download` does not fetch a
// replaced module's transitive deps into a custom GOMODCACHE, so `go mod tidy`
// resolves the graph and `go build` forces full source extraction.
func resolveGoModules(ctx context.Context, workDir, gomodcache string) error {
	env := append(os.Environ(), "GOMODCACHE="+gomodcache, "GOFLAGS=-mod=mod")
	for _, args := range [][]string{
		{"go", "mod", "tidy"},
		{"go", "build", "-o", os.DevNull, "."},
	} {
		var b strings.Builder
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = workDir
		cmd.Env = env
		cmd.Stdout = &b
		cmd.Stderr = &b
		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("%s: %w", strings.Join(args, " "), ctx.Err())
			}
			return fmt.Errorf("%s: %w\n%s", strings.Join(args, " "), err, indent(b.String()))
		}
	}
	return nil
}

// rewriteReplaceTarget repoints the go.mod replace target from the host
// checkout to the container mount, keeping the indirect requires tidy added.
func rewriteReplaceTarget(workDir, from, to string) error {
	path := filepath.Join(workDir, "go.mod")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	updated := strings.ReplaceAll(string(data), from, to)
	if updated == string(data) {
		return fmt.Errorf("go.mod: replace target %q not found", from)
	}
	return os.WriteFile(path, []byte(updated), 0o644)
}

var replaceRe = regexp.MustCompile(`(?m)^replace\s+\S+\s+=>\s+\S+\s*$`)

// assertGoModReplace checks go.mod carries exactly one replace directive.
func assertGoModReplace(workDir string) error {
	data, err := os.ReadFile(filepath.Join(workDir, "go.mod"))
	if err != nil {
		return err
	}
	matches := replaceRe.FindAll(data, -1)
	if len(matches) != 1 {
		return fmt.Errorf("go.mod: expected exactly 1 replace directive, found %d", len(matches))
	}
	return nil
}

// clientBuildCacheDir is keyed on client, ref and arch so a binary from a
// different client version or arch is never replayed.
func clientBuildCacheDir(cache, clientTag, ref, arch string) string {
	return filepath.Join(cache, "clientbuilds", clientTag+"@"+ref+"__"+arch)
}

// clientBuildCacheFor resolves a client's pinned spec and its build-cache directory.
func clientBuildCacheFor(repoRoot, cache, clientName, clientTag, arch string) (clientSpec, string, error) {
	clients, err := parseClientsTxt(filepath.Join(repoRoot, "e2e", "clients.txt"))
	if err != nil {
		return clientSpec{}, "", fmt.Errorf("parse e2e/clients.txt: %w", err)
	}
	spec, ok := findClient(clients, clientName)
	if !ok {
		return clientSpec{}, "", fmt.Errorf("no %q entry in e2e/clients.txt", clientName)
	}
	return spec, clientBuildCacheDir(cache, clientTag, spec.Ref, arch), nil
}

// streamCacheMeta is cached next to a driver binary. The binary embeds one
// stream, and generateStream is a pure function of (runID, base), so storing
// those two regenerates the expected model instead of serializing it.
// SourceHash and BaseImage make a replay refuse after a template, generator or
// toolchain change; empty values (older descriptors) skip that check.
type streamCacheMeta struct {
	RunID      string `json:"run_id"`
	Base       uint32 `json:"base"`
	ClientTag  string `json:"client_tag"`
	Arch       string `json:"arch"`
	SourceHash string `json:"source_hash,omitempty"`
	BaseImage  string `json:"base_image,omitempty"`
}

func sourceHash(src string) string {
	h := sha256.Sum256([]byte(src))
	return hex.EncodeToString(h[:])
}

func saveStreamCacheMeta(buildCache, runID string, base uint32, clientTag, arch, srcHash, baseImage string) error {
	meta := streamCacheMeta{RunID: runID, Base: base, ClientTag: clientTag, Arch: arch, SourceHash: srcHash, BaseImage: baseImage}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal stream cache descriptor: %w", err)
	}
	if err := os.MkdirAll(buildCache, 0o755); err != nil {
		return fmt.Errorf("create build cache %s: %w", buildCache, err)
	}
	return os.WriteFile(filepath.Join(buildCache, streamJSONName), append(data, '\n'), 0o644)
}

// removeStreamCacheDescriptor forces a rebuild after a failed build or run: a
// failed compile leaves the old binary behind, and a failed run leaves one that
// was never validated end-to-end.
func removeStreamCacheDescriptor(rec *recorder, clientName, buildCache string) {
	if err := os.Remove(filepath.Join(buildCache, streamJSONName)); err != nil && !os.IsNotExist(err) {
		rec.logf("%s: could not remove stale stream descriptor after failed run: %v", clientName, err)
	}
}

func loadStreamCacheMeta(buildCache string) (streamCacheMeta, error) {
	data, err := os.ReadFile(filepath.Join(buildCache, streamJSONName))
	if err != nil {
		return streamCacheMeta{}, fmt.Errorf("read stream cache descriptor: %w", err)
	}
	var meta streamCacheMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return streamCacheMeta{}, fmt.Errorf("parse stream cache descriptor: %w", err)
	}
	return meta, nil
}

// validateSkipClientBuildCache checks the cached driver matches this client,
// arch, base image and rendered source. It only reads files, so it also runs
// before the stack bring-up to fail a bad skip-run fast.
func validateSkipClientBuildCache(d clientDriver, repoRoot, buildCache, arch string) error {
	descPath := filepath.Join(buildCache, streamJSONName)
	if !fileExists(descPath) {
		return fmt.Errorf(
			"--skip-client-build: %s: no cached build in %s — run once WITHOUT --skip-client-build to build and cache the driver",
			d.name, buildCache)
	}
	meta, err := loadStreamCacheMeta(buildCache)
	if err != nil {
		return fmt.Errorf("--skip-client-build: %s: %w", d.name, err)
	}
	if meta.ClientTag != d.tag {
		return fmt.Errorf(
			"--skip-client-build: %s: cached build is for client %q, not %q — remove %s and rebuild",
			d.name, meta.ClientTag, d.tag, buildCache)
	}
	if meta.Arch != arch {
		return fmt.Errorf(
			"--skip-client-build: %s: cached driver is arch %q, not %q — remove %s and rebuild",
			d.name, meta.Arch, arch, buildCache)
	}
	if !fileExists(filepath.Join(buildCache, driverBinName)) {
		return fmt.Errorf(
			"--skip-client-build: %s: stream descriptor present but no cached driver binary in %s — run once WITHOUT --skip-client-build",
			d.name, buildCache)
	}

	if meta.BaseImage != "" && meta.BaseImage != d.baseImage {
		return fmt.Errorf(
			"--skip-client-build: %s: base image changed since this binary was built (cached %q, now %q) — run once WITHOUT --skip-client-build",
			d.name, meta.BaseImage, d.baseImage)
	}
	if meta.SourceHash != "" {
		// Re-render the stream the binary was compiled from.
		stream := generateStream(meta.RunID, meta.ClientTag, time.Unix(int64(meta.Base)+120, 0))
		src, err := d.renderSource(repoRoot, stream)
		if err != nil {
			return fmt.Errorf("--skip-client-build: %s: re-render driver to verify cache: %w", d.name, err)
		}
		if hash := sourceHash(src); hash != meta.SourceHash {
			return fmt.Errorf(
				"--skip-client-build: %s: template changed since this binary was built — run once WITHOUT --skip-client-build",
				d.name)
		}
	}
	return nil
}

// streamForClientPhase generates a fresh stream, or under --skip-client-build
// replays the one the cached binary was compiled from (cached=true).
func streamForClientPhase(d clientDriver, o clientPhaseOpts, buildCache string) (metricStream, bool, error) {
	if !o.skipClientBuild {
		return generateStream(o.runID, d.tag, time.Now()), false, nil
	}
	if err := validateSkipClientBuildCache(d, o.repoRoot, buildCache, o.arch); err != nil {
		return metricStream{}, false, err
	}
	meta, err := loadStreamCacheMeta(buildCache)
	if err != nil {
		return metricStream{}, false, fmt.Errorf("--skip-client-build: %s: %w", d.name, err)
	}
	// generateStream derives base = now − 120.
	now := time.Unix(int64(meta.Base)+120, 0)
	return generateStream(meta.RunID, meta.ClientTag, now), true, nil
}

func streamSourceLabel(cached bool) string {
	if cached {
		return "replayed (cached build)"
	}
	return "generated"
}

// runCachedDriver runs a cached driver binary in the image it was built in,
// since the rust/cpp drivers link glibc dynamically.
func runCachedDriver(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts, image string) (int, string, error) {
	opts := RunOpts{
		Name:    o.container,
		Image:   image,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.buildCache + ":" + driverBinMount,
		},
		Cmd:    []string{"/bin/sh", "-c", "set -e; echo \"e2e: running cached driver from " + driverBinMount + "\"; " + driverBinMount + "/" + driverBinName},
		AutoRm: true,
	}
	rec.logf("%s: --skip-client-build: running cached driver from %s (%s)", o.container, o.buildCache, image)
	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}

// buildAndRunGoClient clones, renders, resolves modules on the host, then
// builds offline and runs the driver in a container. A non-zero driver exit is
// reported via the exit code, not err.
func buildAndRunGoClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached binary.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, goBaseImage)
	}

	clonePath, err := o.spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverGoDir, "main.go.tmpl")
	if err := renderGoDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered go driver: %s (%d writes)", filepath.Join(o.workDir, "main.go"), len(o.stream.Writes))

	// Host pre-resolve against the host checkout.
	if err := writeGoMod(o.workDir, clonePath); err != nil {
		return 0, "", err
	}
	if err := copyGoSum(o.workDir, filepath.Join(clonePath, "go.sum")); err != nil {
		return 0, "", err
	}
	gomodcache := filepath.Join(o.cache, "gomodcache")
	gocache := filepath.Join(o.cache, "gocache")
	for _, d := range []string{gomodcache, gocache, o.buildCache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return 0, "", fmt.Errorf("create cache %s: %w", d, err)
		}
	}
	if err := resolveGoModules(ctx, o.workDir, gomodcache); err != nil {
		return 0, "", err
	}
	rec.logf("pre-resolved go modules into %s (offline container build)", gomodcache)

	if err := rewriteReplaceTarget(o.workDir, clonePath, clientMount); err != nil {
		return 0, "", err
	}
	if err := assertGoModReplace(o.workDir); err != nil {
		return 0, "", err
	}

	buildRun := "set -e; cd " + workMount +
		`; echo "e2e: GOARCH=$(go env GOARCH) GOOS=$(go env GOOS) GOVERSION=$(go version)"` +
		`; test "$(go env GOARCH)" = "` + o.arch + `" || { echo "e2e: golang image GOARCH does not match runtime arch '""" + o.arch + """'"; exit 2; }` +
		"; go build -o " + driverBinMount + "/" + driverBinName + " ." +
		"; " + driverBinMount + "/" + driverBinName

	opts := RunOpts{
		Name:    o.container,
		Image:   goBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
			"GOMODCACHE=" + modMount,
			"GOCACHE=" + gocacheMount,
			"GOPATH=/tmp/gopath",
			"GOPROXY=off",
			"CGO_ENABLED=0",
		},
		Volumes: []string{
			o.workDir + ":" + workMount, // rw: go build may rewrite go.mod/go.sum under -mod=mod
			clonePath + ":" + clientMount + ":ro",
			gomodcache + ":" + modMount,
			gocache + ":" + gocacheMount,
			o.buildCache + ":" + driverBinMount,
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true,
	}
	rec.logf("go client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}
