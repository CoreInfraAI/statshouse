// Command e2e drives the StatsHouse end-to-end test harness.
//
// The first phase brought up a single-node ClickHouse with the committed schema and
// proved the harness skeleton (runtime abstraction, preflight, readiness probes,
// teardown, artifacts).
//
// This builds on that: it cross-compiles the four daemons (metadata, agg,
// api, agent), bind-mounts each into a minimal alpine image, and brings up the
// full five-service stack — clickhouse, metadata, agg, api, agent — wired by
// inspected IP, then proves /api/query answers on the published port.
//
//	go run ./e2e
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// e2ePrefix namespaces every resource the harness creates so it can prune its
// own leftovers without touching unrelated containers/networks (e.g. a parallel
// project's stack on the same machine).
const e2ePrefix = "e2e-"

// runIDRe constrains --run-id: the value flows into resource names (apple/container
// requires lowercase network names) and the artifacts path, so it must not contain
// path separators, dots, or uppercase. The leading char is alphanumeric to avoid a
// leading dash (which CLIs can reject as a flag-like value).
var runIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func main() {
	var (
		runtimeFlag     = flag.String("runtime", "", "container runtime: \"container\" (apple, default on macOS) or \"docker\" (default on Linux); auto-detected if empty")
		runIDFlag       = flag.String("run-id", "", "run identifier (default: local datetime 20060102-150405)")
		archFlag        = flag.String("arch", "", "GOARCH to cross-compile daemons for (default arm64; the apple/container + lima/arm64 verification path)")
		backendFlag     = flag.String("storage-backend", "clickhouse", "storage backend the daemons run: \"clickhouse\" (default; the usual stack) or \"duck\" (DuckDB embedded in the aggregator; no ClickHouse container, the api reads through the aggregator's store-query RPC)")
		keep            = flag.Bool("keep", false, "keep containers+network after the run for debugging")
		verbose         = flag.Bool("v", false, "verbose: stream container logs live and dump raw API responses to artifacts")
		timeout         = flag.Duration("timeout", 10*time.Minute, "overall run timeout")
		skipClientBuild = flag.Bool("skip-client-build", false, "reuse previously-built client driver binaries + cached stream (skip the in-container compile); fails if no cached build exists for a selected client")
		withUI          = flag.Bool("with-ui", false, "build the npm UI in a pinned node container and serve it from the api's --static-dir (default off: no node, no UI build). On apple/container this needs npm on the host to warm the build cache (apple/container has no in-container network); the docker runtime installs online in the node container")
		apiPortFlag     = flag.String("api-port", "", `override the api published host port: "" uses e2e/config.yaml (default 10888); "auto" picks a free port on 127.0.0.1; or a port number, e.g. "10889", so concurrent runs don't collide`)
		prewarmRetries  = flag.Int("prewarm-retries", 2, "extra attempts when a client's pre-warm times out (driver exit 3, a transient journal-longpoll stall); 0 fails immediately (pre-change behavior)")
		conformance     = flag.Bool("conformance", false, "differential conformance run: boot ClickHouse plus TWO daemon stacks (ch-backed and duck-backed) over one shared metadata, seed the identical deterministic stream to both agents from the harness itself, and compare both apis' decoded answers to every query shape (CH is the reference; divergence fails the run)")
		clientSel       clientFlag
	)
	flag.Var(&clientSel, "client", "client(s) to drive (repeatable; one of: go, rust, cpp). Default: all three")
	flag.Parse()

	backend, err := parseStorageBackend(*backendFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(2)
	}
	if *conformance {
		// Conformance compares the ch and duck backends side by side, so its
		// CH stack must run on ClickHouse (the duck stack is started
		// internally), and it seeds its own stream — no client drivers.
		if backend != backendClickHouse {
			fmt.Fprintf(os.Stderr, "FAIL: --conformance compares clickhouse vs duck and boots its own ClickHouse stack; do not pass --storage-backend (leave it at the default)\n")
			os.Exit(2)
		}
		if len(clientSel) != 0 {
			fmt.Fprintf(os.Stderr, "FAIL: --conformance seeds its stream from the harness itself; --client is not valid in this mode\n")
			os.Exit(2)
		}
	}
	os.Exit(realMain(*runtimeFlag, *runIDFlag, *archFlag, backend, *keep, *verbose, *timeout, clientSel, *skipClientBuild, *withUI, *apiPortFlag, *prewarmRetries, *conformance))
}

// clientFlag is a repeatable --client selector (flag.Var). Each Set appends, so
// `--client=go --client=rust` selects both; absent → empty → selectDrivers picks
// all clients.
type clientFlag []string

func (c *clientFlag) String() string {
	if c == nil {
		return ""
	}
	return strings.Join(*c, ",")
}

func (c *clientFlag) Set(v string) error {
	*c = append(*c, v)
	return nil
}

// clientDriver is one client the harness can build+run+assert. name matches the
// active entry in e2e/clients.txt; tag is the short --client selector AND the
// per-client metric-name prefix that isolates one client's writes from another's
// (stream.go); buildRun is the language-specific clone→render→build→run.
// baseImage is the pinned toolchain tag the driver builds+runs in (used by the
// --skip-client-build cache guard). renderSource renders the stream to driver
// source TEXT (pure, host-side) so the cache guard can hash it without a build.
type clientDriver struct {
	name         string // e.g. "statshouse-go"
	tag          string // e.g. "go"
	baseImage    string // pinned toolchain tag (goBaseImage / rustBaseImage / cppBaseImage)
	buildRun     func(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error)
	renderSource func(repoRoot string, stream metricStream) (string, error)
}

// clientDrivers is the registry of every client the harness can drive, in the
// order a default (no --client) run executes them. Adding a client here (and to
// e2e/clients.txt) is all the wiring the main loop needs.
var clientDrivers = []clientDriver{
	{
		name: goClientName, tag: goClientTag, baseImage: goBaseImage, buildRun: buildAndRunGoClient,
		renderSource: func(root string, s metricStream) (string, error) {
			return renderGoDriverSource(filepath.Join(root, "e2e", driverGoDir, "main.go.tmpl"), s)
		},
	},
	{
		name: rustClientName, tag: rustClientTag, baseImage: rustBaseImage, buildRun: buildAndRunRustClient,
		renderSource: func(root string, s metricStream) (string, error) {
			return renderDriverSource(filepath.Join(root, "e2e", driverRustDir, "main.rs.tmpl"), "rust-driver", rustDriverFuncs(), s)
		},
	},
	{
		name: cppClientName, tag: cppClientTag, baseImage: cppBaseImage, buildRun: buildAndRunCppClient,
		renderSource: func(root string, s metricStream) (string, error) {
			return renderDriverSource(filepath.Join(root, "e2e", driverCppDir, "main.cpp.tmpl"), "cpp-driver", cppDriverFuncs(), s)
		},
	},
}

// selectDrivers resolves the repeatable --client selectors to the drivers to
// run. An empty selection means all of them (the default). Each selector may be
// a tag ("go"/"rust"/"cpp") or the full client name ("statshouse-go"); an
// unknown selector is a hard error. Duplicates collapse to the first occurrence.
func selectDrivers(sels []string) ([]clientDriver, error) {
	if len(sels) == 0 {
		return clientDrivers, nil
	}
	byTag := make(map[string]clientDriver, len(clientDrivers))
	for _, d := range clientDrivers {
		byTag[d.tag] = d
	}
	var out []clientDriver
	seen := make(map[string]bool, len(sels))
	for _, s := range sels {
		s = strings.TrimSpace(s)
		d, ok := byTag[strings.TrimPrefix(s, "statshouse-")]
		if !ok {
			return nil, fmt.Errorf("unknown --client %q (want one of: go, rust, cpp)", s)
		}
		if !seen[d.tag] {
			out = append(out, d)
			seen[d.tag] = true
		}
	}
	return out, nil
}

// validateSkipClientBuild runs the PURE host-side --skip-client-build validation
// for every selected driver: each one's cached build must be present, match the
// client+arch, carry a driver binary, and (for new descriptors) match the current
// base image + rendered-source hash. It is called eagerly — right after driver
// selection, BEFORE the ~1min ClickHouse/daemon stack bring-up — so a bad skip-run
// (no cache, stale cache, template/toolchain drift) fails in <1s instead of after
// the full stack is up (F6). repoRoot/cache mirror the per-client build-cache
// resolution runClientPhase does, so the early check sees exactly what the phase
// will see.
func validateSkipClientBuild(drivers []clientDriver, repoRoot, cache, arch string) error {
	for _, d := range drivers {
		_, buildCache, err := clientBuildCacheFor(repoRoot, cache, d.name, d.tag, arch)
		if err != nil {
			return fmt.Errorf("%s: resolve build cache: %w", d.name, err)
		}
		if err := validateSkipClientBuildCache(d, repoRoot, buildCache, arch); err != nil {
			return err
		}
	}
	return nil
}

// driverTags returns the comma-joined tags of the selected drivers (e.g. "go,
// rust, cpp") for the progress log.
func driverTags(drivers []clientDriver) string {
	tags := make([]string, 0, len(drivers))
	for _, d := range drivers {
		tags = append(tags, d.tag)
	}
	return strings.Join(tags, ", ")
}

func realMain(runtimeFlag, runIDFlag, archFlag string, backend storageBackend, keep, verbose bool, timeout time.Duration, clientSel clientFlag, skipClientBuild, withUI bool, apiPortFlag string, prewarmRetries int, conformance bool) int {
	runID := runIDFlag
	if runID == "" {
		runID = time.Now().Format("20060102-150405")
	}
	if !runIDRe.MatchString(runID) {
		fmt.Fprintf(os.Stderr, "FAIL: invalid --run-id %q: must match %s (lowercase; this value becomes resource names and an artifacts path)\n",
			runID, runIDRe.String())
		return 2
	}

	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		return 2
	}
	artifactsDir := filepath.Join(root, "e2e", "test-results", runID)
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: create artifacts dir %s: %v\n", artifactsDir, err)
		return 2
	}

	// --with-ui is handled early: the npm UI is built in a pinned node
	// container (offline against a host-populated cache on apple/container; online
	// via NAT egress on docker) and its output is mounted into the api as
	// --static-dir=/ui. Off by default — no node, no UI build.

	rec := &recorder{verbose: verbose, artifactsDir: artifactsDir, runID: runID}
	rec.logf("runid=%s artifacts=%s", runID, artifactsDir)

	network := e2ePrefix + runID
	chContainer := e2ePrefix + runID + "-clickhouse"

	// The run context is cancelled by --timeout OR an incoming SIGINT/SIGTERM.
	// Deferred calls (teardown) still run on signal-driven cancel because
	// NotifyContext cancels the context rather than killing the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rt, err := selectRuntime(runtimeFlag)
	if err != nil {
		return fail(rec, artifactsDir, rt, nil, err)
	}
	rec.logf("runtime=%s", rt.Name())

	// --- preflight ---
	if err := rt.EnsureSystem(ctx); err != nil {
		return fail(rec, artifactsDir, rt, nil, fmt.Errorf("preflight ensure-system: %w", err))
	}
	if err := rt.CheckVersion(ctx); err != nil {
		return fail(rec, artifactsDir, rt, nil, fmt.Errorf("preflight version check: %w", err))
	}
	rec.logf("preflight ok (%s)", rt.Name())

	// --with-ui on a runtime WITHOUT in-container network (apple/container) needs host
	// npm: the offline container build consumes a host-populated cache, and populating
	// it passes npm --os/--cpu/--libc (npm >= npmMinMajor). Checked at preflight so a
	// missing/old npm fails before any container or network is created. The docker
	// runtime installs online in the node container (NAT egress), so host npm is not
	// required there. Strictly opt-in: default runs do none of this.
	if withUI && !rt.HasNetworkEgress() {
		if !lookPath("npm") {
			return fail(rec, artifactsDir, rt, nil, fmt.Errorf("--with-ui needs npm on the host to warm the build cache (apple/container has no in-container network); install Node/npm or use the docker runtime"))
		}
		if m := npmMajorVersion(ctx); m < npmMinMajor {
			return fail(rec, artifactsDir, rt, nil, fmt.Errorf("--with-ui needs npm >= %d on the host (found major %d) to populate the offline build cache with --os/--cpu/--libc; upgrade npm or use the docker runtime", npmMinMajor, m))
		}
	}

	// --- resolve selected drivers + e2e cache + arch early ---
	// All three are pure host-side resolutions; doing them here (before any container
	// is created) lets --skip-client-build fail FAST on a missing/stale/drifted cache
	// instead of after the ~1min stack bring-up (F6).
	arch := resolveArch(archFlag)
	drivers, err := selectDrivers(clientSel)
	if err != nil {
		return fail(rec, artifactsDir, rt, nil, err)
	}
	cache, err := e2eCacheDir()
	if err != nil {
		return fail(rec, artifactsDir, rt, nil, fmt.Errorf("resolve e2e cache dir: %w", err))
	}
	if skipClientBuild {
		if err := validateSkipClientBuild(drivers, root, cache, arch); err != nil {
			return fail(rec, artifactsDir, rt, nil, err)
		}
		rec.logf("--skip-client-build: cached build validated for client(s) %s", driverTags(drivers))
	}

	// --- prune leftovers from prior runs (before creating this run's resources) ---
	prunedC, prunedN := pruneStale(ctx, rt)
	if prunedC+prunedN > 0 {
		rec.logf("pruned %d stale container(s), %d stale network(s)", prunedC, prunedN)
	} else {
		rec.logf("no stale e2e-* resources to prune")
	}

	// Every container created during the run, appended as it starts, so teardown
	// and fail() can clean/capture them uniformly (even on partial starts).
	var containers []string

	// Pre-declared so the --keep branch of the teardown closure (below) can read
	// them: both are assigned further down (after loadConfig / startDaemonStack)
	// and stay nil/empty on an early abort, which is exactly the case where the
	// closure must NOT try to print a reachable api address.
	var (
		cfg publishConfig // loaded after the published-port preflight
		ds  *daemonStack  // started after the daemons build
	)

	// --- teardown unless --keep ---
	// Uses a fresh context (not the run ctx, which the --timeout deadline or a
	// signal may have cancelled) so cleanup still runs after the run deadline fires.
	teardown := func() {
		if keep {
			// Print how to reach the kept stack so a human can poke it without
			// re-deriving addresses (--keep leaves the stack running and
			// the harness prints how to reach it). The api is the only published
			// port by default; when it is not published the container IP is reachable
			// only from inside the run network. cfg/ds are nil on an early abort.
			rec.logf("keeping resources (--keep): %d container(s) %v on network %s", len(containers), containers, network)
			addr := ""
			if cfg != nil {
				addr = cfg.hostAddr("api")
			}
			if addr == "" && ds != nil && ds.api != nil {
				addr = net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort)) + "  (container IP — reachable from the run network)"
			}
			if addr != "" {
				now := time.Now()
				rec.logf("  reach the api:  curl 'http://%s/api/query?s=__agg_bucket_receive_delay_sec&f=%d&t=%d&w=1s&qw=count&ac=1'",
					addr, now.Add(-5*time.Minute).Unix(), now.Unix())
				if withUI {
					// The built UI is served by the api from its --static-dir at /.
					// Strip the "(container IP — …)" annotation off addr so this is a
					// real http://host:port/ a browser can open.
					uiAddr := addr
					if i := strings.Index(uiAddr, "  "); i >= 0 {
						uiAddr = uiAddr[:i]
					}
					rec.logf("  reach the UI:   http://%s/", uiAddr)
				}
			}
			rec.logf("  container logs: %s logs <name>   (names: %s)", rt.Name(), strings.Join(containers, " "))
			rec.logf("  tear it down:   %s rm -f %s   &&   %s network rm %s",
				rt.Name(), strings.Join(containers, " "), rt.Name(), network)
			rec.logf("  note: the next `go run ./e2e` prunes this stack.")
			// These keep lines are logged in the DEFERRED teardown, which runs AFTER
			// writeRunArtifacts already wrote summary.txt — re-write the summary so the
			// --keep reachability/note lines are captured in the artifact too (F5).
			writeSummary(artifactsDir, rec.lines)
			return
		}
		tctx, tcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer tcancel()
		var errs []string
		for _, c := range containers {
			if err := rt.Rm(tctx, c, true); err != nil {
				errs = append(errs, c+": "+err.Error())
			}
		}
		if err := rt.NetworkRemove(tctx, network); err != nil {
			errs = append(errs, "network "+network+": "+err.Error())
		}
		switch {
		case len(errs) > 0:
			rec.logf("teardown errors: %s", strings.Join(errs, "; "))
		default:
			rec.logf("teardown ok: removed %d container(s) and network %s", len(containers), network)
		}
	}
	defer teardown()

	if err := rt.NetworkCreate(ctx, network); err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("create network %s: %w", network, err))
	}
	rec.logf("created network %s", network)

	// --- preflight published-port check + load publish config ---
	// Every published host port must be FREE before any container binds one.
	// Two concurrent harness runs collide hard — prune wars plus the fixed api
	// publish port mean one run's assertions silently query the other's api.
	// Bail before the expensive ClickHouse/daemon setup if a configured host
	// port already answers.
	cfg, err = loadConfig(filepath.Join(root, "e2e", "config.yaml"))
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("load e2e/config.yaml: %w", err))
	}
	// Apply --api-port BEFORE the published-port preflight so an explicit free
	// port (or "auto") is what gets checked, and so publishSpec/hostAddr — which
	// both read cfg["api"] — carry the resolved host port through to the -p flag
	// and the harness's own query address.
	cfg, err = resolveAPIPort(cfg, apiPortFlag)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("--api-port: %w", err))
	}
	rec.logf("api published at %s", cfg.hostAddr("api"))
	if err := checkPublishedPortsFree(ctx, cfg); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}

	// --- build the npm UI when --with-ui is set ---
	// Done EARLY — right after the published-port preflight, before ClickHouse and
	// the daemon build — so a UI failure fails fast instead of after the ~1min stack
	// bring-up. Defaults: the placeholder page (e2e/api-static/index.html) mounted at
	// /static. With the flag, the built UI (statshouse-ui/build, index.html at its
	// root) is mounted at /ui instead. The api is built WITHOUT the embed tag, so it
	// always loads index.html from --static-dir — never an embed-tag api build.
	apiStaticDir := filepath.Join(root, "e2e", "api-static")
	apiMountTarget := apiStaticMount
	if withUI {
		// Track the one-shot build container BEFORE launching it: a context-cancelled
		// build SIGKILLs the local CLI and may orphan the container, and this run's
		// teardown (not just the next run's pruneStale) should reap it. Rm is idempotent
		// (resourceInList no-op), so a normally-AutoRm'd container is a harmless no-op
		// here, and dumpServiceLogs filters by presence. Host-npm prerequisites were
		// already checked at preflight (above); no duplicate check here.
		uiBuildC := e2ePrefix + runID + "-uibuild"
		containers = append(containers, uiBuildC)
		uiBuildDir, err := buildUI(ctx, rt, root, cache, uiBuildC, rec.logf)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build ui: %w", err))
		}
		apiStaticDir = uiBuildDir
		apiMountTarget = apiUIMount
	}

	// --- ClickHouse (clickhouse backend only) ---
	// Under duck no ClickHouse container starts and nothing replaces it: the
	// aggregator IS the storage (DuckDB embedded in the duckdb-tagged build),
	// so the stack is one container shorter and the agg's own store-query RPC
	// (probed in startDaemonStack) replaces the ClickHouse schema probe as the
	// storage-readiness gate.
	var chIP string
	if backend == backendDuck {
		rec.logf("storage backend=duck: no ClickHouse container (DuckDB embedded in the aggregator)")
	} else {
		start := time.Now()
		ch, err := startClickHouse(ctx, rt, chContainer, network, root, artifactsDir)
		containers = append(containers, chContainer)
		if ch != nil && ch.ip != "" {
			rec.logf("clickhouse container=%s ip=%s", chContainer, ch.ip)
		} else {
			rec.logf("clickhouse container=%s", chContainer)
		}
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("clickhouse: %w", err))
		}
		if ch.ip == "" {
			// An earlier phase treated a missing IP as non-fatal (it probed CH via Exec). The
			// daemon stack wires to CH by IP, so here it is fatal.
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("clickhouse has no inspected IP on %s; daemons wire to it by IP", network))
		}
		chIP = ch.ip
		rec.logf("clickhouse ready (probes green in %.1fs)", time.Since(start).Seconds())

		tables, err := ch.tables(ctx, rt)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("read tables: %w", err))
		}
		hasReady := false
		for _, t := range tables {
			if t == chReadyTable {
				hasReady = true
			}
		}
		rec.logf("schema loaded: %d tables (%s)", len(tables), strings.Join(tables, ", "))
		if !hasReady {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("readiness table %s missing from SHOW TABLES", chReadyTable))
		}
	}

	// --- build the four daemons (cached across runs) ---
	binDir, err := buildDaemons(ctx, root, arch, backend, rec.logf)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build daemons: %w", err))
	}

	// Shared RPC crypto key: mounted into all four daemons so their cross-
	// container RPC (metadata↔agg, metadata↔api, agg↔agent) passes the nonce
	// exchange, which requires encryption whenever the peers are not on the same
	// machine (always true here). Removed after the run.
	rpcKeyPath, err := writeRPCKey()
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	defer os.Remove(rpcKeyPath)

	start := time.Now()
	chStackTag := ""
	if conformance {
		// Tag the stacks so the ch and duck daemon sets coexist on one network
		// with distinct container names (e2e-<runid>-ch-agg / -duck-agg).
		chStackTag = confStackCH
	}
	ds, err = startDaemonStack(ctx, rt, rec, daemonStackOpts{
		network:      network,
		chIP:         chIP,
		binDir:       binDir,
		runID:        runID,
		cfg:          cfg,
		rpcKeyPath:   rpcKeyPath,
		apiStaticDir: apiStaticDir,
		staticMount:  apiMountTarget,
		backend:      backend,
		stackTag:     chStackTag,
	})
	containers = append(containers, ds.containerNames()...)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("daemon stack: %w", err))
	}
	rec.logf("daemon stack ready (metadata+agg+api+agent green in %.1fs)", time.Since(start).Seconds())

	// --- /api/query answers on the published port ---
	queryAddr := cfg.hostAddr("api") // "127.0.0.1:10888"; falls back to the container IP if not published
	if queryAddr == "" {
		queryAddr = net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort))
	}
	body, err := queryAPI(ctx, queryAddr)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("api query: %w", err))
	}
	rec.logf("/api/query answered 200 on %s (%d bytes)", queryAddr, len(body))

	// --- with-ui: prove the api serves the BUILT UI at /, not the placeholder ---
	// The api serves the static dir's index.html on the same HTTP port as /api/query,
	// so once /api/query answers the UI is reachable too. A positive GET of the root
	// (200 + the built app's React mount) fails the run loudly if --with-ui did not
	// actually wire build/ into --static-dir.
	if withUI {
		uiBody, err := assertUIServed(ctx, queryAddr)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("ui served: %w", err))
		}
		rec.logf("UI served on %s (GET / -> 200, built index.html %d bytes)", queryAddr, len(uiBody))
	}

	// --- gate "stack ready" on a REAL agent→agg→api round-trip, not just TCP
	// dials: the agent↔agg channel can be dead while every TCP probe
	// is green, and every client write then silently times out. A recent point on
	// the agg's receive-delay builtin proves the conveyor is live before clients
	// start. ---
	if err := waitAggConveyor(ctx, queryAddr); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	rec.logf("agent↔agg conveyor live (recent %s point)", queryMetric)

	// --- conformance: boot the SECOND (duck) stack over the SHARED metadata ---
	// buildDaemons is idempotent across backends (one shared bin dir + mtime
	// cache; only statshouse-agg differs, cached separately as the duckdb-tagged
	// build), so this second call only cross-compiles the duck aggregator.
	var duckAPIAddr, duckAgentAddr string
	if conformance {
		duckBinDir, err := buildDaemons(ctx, root, arch, backendDuck, rec.logf)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build duck daemons: %w", err))
		}
		rec.logf("conformance: starting the duck stack over shared metadata %s", ds.metadata.name)
		startDuck := time.Now()
		dsDuck, err := startDaemonStack(ctx, rt, rec, daemonStackOpts{
			network:      network,
			binDir:       duckBinDir,
			runID:        runID,
			cfg:          publishConfig{}, // no published port: the harness dials the container IP directly
			rpcKeyPath:   rpcKeyPath,
			apiStaticDir: apiStaticDir,
			staticMount:  apiMountTarget,
			backend:      backendDuck,
			stackTag:     confStackDuck,
			sharedMeta:   ds.metadata,
		})
		// The duck stack reuses the shared metadata container (already tracked
		// above), so only its own three services are appended — on a partial
		// start too, exactly like the metadata stack's nil-safe failure path:
		// the started containers must still be torn down, and dereferencing a
		// half-built stack would crash the harness before it reports.
		for _, s := range []*service{dsDuck.agg, dsDuck.api, dsDuck.agent} {
			if s != nil {
				containers = append(containers, s.name)
			}
		}
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("duck daemon stack: %w", err))
		}
		rec.logf("duck daemon stack ready (agg+api+agent green in %.1fs)", time.Since(startDuck).Seconds())

		duckAPIAddr = net.JoinHostPort(dsDuck.api.ip, strconv.Itoa(apiPort))
		if _, err := queryAPI(ctx, duckAPIAddr); err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("duck api query: %w", err))
		}
		if err := waitAggConveyor(ctx, duckAPIAddr); err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("duck conveyor: %w", err))
		}
		duckAgentAddr = net.JoinHostPort(dsDuck.agent.ip, strconv.Itoa(agentPort))
		rec.logf("duck stack ready: /api/query 200 + conveyor live on %s (agent %s)", duckAPIAddr, duckAgentAddr)
	}

	// Under -v, stream each daemon container's logs to stderr LIVE while the run
	// proceeds (-v streams logs live). Stopped before teardown (the stop
	// defer is registered after teardown's, so it runs first — LIFO) so the tail
	// goroutine never races the container Rm. containers holds clickhouse + the four
	// daemons by now; client containers run foreground (their stdout already streams).
	var stopStreamer func()
	if verbose {
		stopStreamer = startLogStreamer(ctx, rt, containers, runID)
	}
	defer func() {
		if stopStreamer != nil {
			stopStreamer()
		}
	}()

	// --- client phase: drive each selected client over TCP
	// with explicit historic timestamps, wait for its clean exit, then assert
	// per-kind per-bucket/per-series equality across the full metric matrix
	// (counter/value/value_p/unique/stag). go came first; rust and cpp follow on
	// the same path; a later phase extends the stream + the
	// assertions to all metric kinds. The three are isolated by per-client metric
	// prefixes so their values never collide on the shared stack. ---
	phaseOpts := clientPhaseOpts{
		network:          network,
		agentIP:          ds.agent.ip,
		apiAddr:          queryAddr,
		apiContainerAddr: net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort)),
		runID:            runID,
		arch:             arch,
		repoRoot:         root,
		artifactsDir:     artifactsDir,
		cache:            cache,
		skipClientBuild:  skipClientBuild,
		prewarmRetries:   prewarmRetries,
	}
	var totalPass, totalFail int
	cancelled := false
	if conformance {
		// The conformance phase replaces the client drivers: the harness itself
		// seeds the identical stream to BOTH agents in-process (one hostname →
		// identical _h/max_host across backends), gates on the CH reference
		// matching the frozen model, then runs the differential request set.
		totalPass, totalFail, cancelled = runConformancePhase(ctx, rec, conformancePhaseOpts{
			runID:     runID,
			chAPI:     queryAddr,
			duckAPI:   duckAPIAddr,
			chAgent:   net.JoinHostPort(ds.agent.ip, strconv.Itoa(agentPort)),
			duckAgent: duckAgentAddr,
		})
	} else {
		for _, d := range drivers {
			p, f := runClientPhase(ctx, rt, rec, d, phaseOpts)
			totalPass += p
			totalFail += f
		}
	}
	// The run is a failure if any client's assertions failed (non-zero exit) even
	// though the stack came up. Service logs are dumped on failure for diagnosis.
	if totalFail > 0 {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
		writeRunArtifacts(artifactsDir, rec)
		return 1
	}
	// A cancelled conformance run must not read as PASS: the differential was
	// cut short (deadline or signal), so the unexecuted requests are neither
	// passed nor failed — the run is reported as interrupted and fails.
	if cancelled {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
		writeRunArtifacts(artifactsDir, rec)
		summary := fmt.Sprintf("INTERRUPTED: conformance differential cancelled before finishing — %d passed, results incomplete, runtime=%s, runid=%s",
			totalPass, rt.Name(), runID)
		rec.logf("%s", summary)
		fmt.Println(summary)
		return 1
	}

	// --- PASS summary ---
	// On a successful run the service logs are only captured under -v (matching the
	// failure path, which always dumps them); captured before the summary write so
	// any dump progress lines land in summary.txt too.
	if verbose {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
	}
	names := make([]string, 0, len(drivers))
	for _, d := range drivers {
		names = append(names, d.tag)
	}
	summary := ""
	if conformance {
		summary = fmt.Sprintf("PASS: conformance differential — %d assertion(s) passed (frozen-model reference gate + duck-vs-clickhouse by decoded value), runtime=%s, runid=%s",
			totalPass, rt.Name(), runID)
	} else {
		summary = fmt.Sprintf("PASS: client(s) [%s] drove the full pipeline, %d metric/func assertion(s) passed, runtime=%s, runid=%s",
			strings.Join(names, " "), totalPass, rt.Name(), runID)
	}
	rec.logf("%s", summary)
	writeRunArtifacts(artifactsDir, rec)
	fmt.Println(summary)
	fmt.Printf("/api/query response: %s\n", truncate(strings.TrimSpace(body), 400))
	return 0
}

// clientPhaseOpts configures runClientPhase. It is shared across clients; each
// client folds its own tag into the generated stream's metric-name prefix.
type clientPhaseOpts struct {
	network          string
	agentIP          string // agent container IP on the run network
	apiAddr          string // published api address (host:port) for assertions
	apiContainerAddr string // api container <ip>:port on the run network (driver pre-warm polling)
	runID            string
	arch             string
	repoRoot         string
	artifactsDir     string
	cache            string // e2e cache root (~/.cache/statshouse-e2e)
	skipClientBuild  bool   // --skip-client-build: replay the cached driver binary + stream
	prewarmRetries   int    // --prewarm-retries: extra attempts on a pre-warm timeout (driver exit 3)
}

// preWarmRetryBackoff is the wait between pre-warm retries in runClientPhase:
// long enough for a transient journal-longpoll stall to clear, short enough that
// the default 2 retries (3 attempts) stay well inside the overall run timeout.
const preWarmRetryBackoff = 20 * time.Second

// runClientPhase drives one client: generate its per-client metric stream,
// pre-create the value_p metrics (which never auto-create), build+run its driver
// in the foreground, wait for the clean exit, then assert per-kind per-bucket/
// per-series equality. Returns pass/fail counts for this client. A build/run
// launch error or a non-zero driver exit is reported as a single FAIL line for
// the client (all its metrics fail). The harness waits for the driver process to
// exit before asserting because rust/cpp flush only on destruction.
func runClientPhase(ctx context.Context, rt Runtime, rec *recorder, d clientDriver, o clientPhaseOpts) (passed, failed int) {
	// Resolve this client's pinned ref (e2e/clients.txt) → its per-(tag,ref,arch)
	// build-cache dir. The dir is where a normal build writes the driver binary +
	// stream descriptor, and where --skip-client-build later reads them back. spec
	// is the resolved client handed to the build+run step so it need not re-parse
	// clients.txt itself.
	spec, buildCache, err := clientBuildCacheFor(o.repoRoot, o.cache, d.name, d.tag, o.arch)
	if err != nil {
		rec.logf("FAIL %s: resolve build cache: %v", d.name, err)
		fmt.Printf("FAIL %s: resolve build cache: %v\n", d.tag, err)
		return 0, 1
	}

	// statusAnchor is the wall-clock unix time at client-phase start. The realtime
	// builtins (__src_ingestion_status / __src_client_write_err / __agg_sampling_factor)
	// are recorded by the agent at RECEIVE time (≈ now during the driver run), NOT at
	// the events' historic ts. On a normal run base≈now−120 so the historic base
	// "happens to" cover them, but a --skip-client-build REPLAY keeps the descriptor's
	// OLD base while the agent records THIS run's events at replay-now — so the
	// ledger/rejection/tripwire windows must anchor at statusAnchor, not stream.Base
	// (the matrix alone keeps stream.Base; its historic ts are embedded in the binary).
	statusAnchor := uint32(time.Now().Unix())

	// Stream source. --skip-client-build replays the EXACT stream a cached driver
	// binary was compiled from: the driver embeds the metric names + the historic
	// base, so it only matches the stream it was built against. We cache a tiny
	// descriptor (runID+base) and REGENERATE the deterministic stream from it
	// (generateStream is a pure function of runID+base), so the cached binary and
	// the expected model agree bit-for-bit. A normal run generates a fresh stream,
	// renders+compiles the driver into buildCache, and writes the descriptor.
	stream, cached, err := streamForClientPhase(d, o, buildCache)
	if err != nil {
		rec.logf("FAIL %s: %v", d.name, err)
		fmt.Printf("FAIL %s: %v\n", d.tag, err)
		return 0, 1
	}
	rec.logf("%s: stream %s: base=%d buckets=%d metrics=%d writes=%d",
		d.name, streamSourceLabel(cached), stream.Base, numBuckets, len(stream.Metrics), len(stream.Writes))

	// value_p never auto-creates from a wire payload (autocreate derives only
	// counter/value/unique), so pre-create its mapping before the driver runs.
	// Metric names embed the unique runID, so each POST creates a fresh metric.
	if err := createValuePMetrics(ctx, rec, o.apiAddr, stream); err != nil {
		rec.logf("FAIL %s: pre-create value_p metrics: %v", d.name, err)
		fmt.Printf("FAIL %s: pre-create value_p metrics: %v\n", d.tag, err)
		return 0, len(stream.Metrics)
	}

	agentAddr := net.JoinHostPort(o.agentIP, strconv.Itoa(agentPort))
	clientContainer := e2ePrefix + o.runID + "-client-" + d.tag

	// F7: write the stream descriptor at BUILD time (before the build runs) so a
	// successful build pairs it with the binary EVEN IF the driver run later fails.
	// The old code wrote it only after a clean exit, so a build-succeed/run-fail left
	// a NEW binary paired with an OLD descriptor → the next --skip-client-build
	// replayed a stale model and failed all-absent. A failed build/run deletes the
	// descriptor below (the compiler leaves the OLD binary on a compile failure, so
	// the just-written NEW descriptor would otherwise desync from it). Skipped on the
	// replay path (cached), which already has a matching descriptor.
	if !cached {
		src, rerr := d.renderSource(o.repoRoot, stream)
		var srcHash string
		if rerr != nil {
			// A render failure here will also fail inside buildRun; log and proceed
			// with an empty hash (the descriptor is deleted if the run then fails).
			rec.logf("%s: could not hash driver source for cache descriptor: %v", d.name, rerr)
		} else {
			srcHash = sourceHash(src)
		}
		if err := saveStreamCacheMeta(buildCache, o.runID, stream.Base, d.tag, o.arch, srcHash, d.baseImage); err != nil {
			rec.logf("%s: could not write build descriptor: %v", d.name, err)
		}
	}

	// Drive the client, retrying a transient pre-warm stall. A pre-warm timeout
	// (driver exit preWarmExit) is the common INFRA failure: the api served stale
	// /api/metrics-list during an apple/container journal-longpoll hiccup, so the
	// driver's seeds never mapped and it bailed before any real write. The stall
	// is transient and self-recovers, so re-running the driver a moment later
	// usually clears it. Only preWarmExit is retried — a launch error (runErr) or
	// any other non-zero exit is a real failure and fails fast. buildRun re-runs
	// the per-driver build+run script, but the compile caches are volume-mounted
	// (GOCACHE / rust & cpp object caches), so a repeat build is a cache-warmed
	// near-no-op; the real cost is the driver's 60s pre-warm poll again.
	prewarmAttempts := 1 + o.prewarmRetries
	if prewarmAttempts < 1 {
		prewarmAttempts = 1
	}
	var (
		exitCode int
		output   string
		runErr   error
	)
	for attempt := 1; ; attempt++ {
		exitCode, output, runErr = d.buildRun(ctx, rt, rec, clientRunOpts{
			stream:     stream,
			spec:       spec,
			network:    o.network,
			agentAddr:  agentAddr,
			apiAddr:    o.apiContainerAddr,
			container:  clientContainer,
			workDir:    filepath.Join(o.artifactsDir, "driver-"+d.tag),
			repoRoot:   o.repoRoot,
			arch:       o.arch,
			cache:      o.cache,
			buildCache: buildCache,
			skipBuild:  cached,
		})
		if runErr != nil {
			rec.logf("FAIL %s: build/run did not launch: %v\n%s", d.name, runErr, indent(output))
			fmt.Printf("FAIL %s build/run: %v\n", d.tag, runErr)
			removeStreamCacheDescriptor(rec, d.name, buildCache) // F7: no valid pair from a failed run
			return 0, len(stream.Metrics)
		}
		if exitCode == 0 {
			break
		}
		if exitCode == preWarmExit && attempt < prewarmAttempts {
			rec.logf("FAIL %s: pre-warm timed out (attempt %d/%d) — transient journal stall, retrying in %s\n%s",
				d.name, attempt, prewarmAttempts, preWarmRetryBackoff, indent(output))
			fmt.Printf("FAIL %s: pre-warm timed out (attempt %d/%d) — retrying\n", d.tag, attempt, prewarmAttempts)
			select {
			case <-ctx.Done():
				return 0, len(stream.Metrics)
			case <-time.After(preWarmRetryBackoff):
			}
			continue
		}
		// Terminal non-zero: pre-warm exhausted its retries, or a different exit.
		if exitCode == preWarmExit {
			rec.logf("FAIL %s: pre-warm timed out (metrics never created — agent/agg path down?)\n%s", d.name, indent(output))
			fmt.Printf("FAIL %s: pre-warm timed out (agent/agg path down?)\n", d.tag)
		} else {
			rec.logf("FAIL %s: driver exited %d\n%s", d.name, exitCode, indent(output))
			fmt.Printf("FAIL %s driver exited %d\n", d.tag, exitCode)
		}
		removeStreamCacheDescriptor(rec, d.name, buildCache) // F7: no valid pair from a failed run
		return 0, len(stream.Metrics)
	}
	rec.logf("%s: driver exited 0\n%s", d.name, indent(truncate(strings.TrimSpace(output), 1200)))
	// The descriptor was written at build time (above). A clean exit with no cached
	// binary means the build recipe did not target the cache — logged, not fatal (a
	// later --skip-client-build then fails with "no driver binary", which is correct).
	if !cached && !fileExists(filepath.Join(buildCache, driverBinName)) {
		rec.logf("%s: build did not cache a driver binary at %s", d.name, filepath.Join(buildCache, driverBinName))
	}

	// Silent-loss tripwire: query __src_client_write_err for this
	// client's run window BEFORE the value assertions, so a TCP-backpressure drop
	// is reported as one labelled failure rather than N mysterious "count too low"
	// mismatches whose real cause (bytes never reached the agent) is otherwise
	// invisible. ok=true also covers clients that do not emit the metric.
	werrOK, werrDetail := assertNoClientWriteErr(ctx, rec, o.apiAddr, d.tag, statusAnchor)
	if !werrOK {
		const werrLabel = "client write-error (silent data loss)"
		rec.logf("FAIL %s: %s\n%s", d.name, werrLabel, indent(werrDetail))
		fmt.Printf("FAIL %s: %s\n", d.tag, werrLabel)
	}

	passed, failed = assertStream(ctx, rec, o.apiAddr, d.tag, stream)
	if !werrOK {
		failed++ // count the silent-loss failure alongside any value mismatches
	}

	// rejection statuses (criterion 2), conservation ledger (criteria
	// 3+5), and the whole-run sampling tripwire (criterion 4). They run AFTER the
	// visible matrix so any sampling on a queried view is already caught; each
	// prints its own labelled PASS/FAIL lines inside. Only their FAILURES fold into
	// the client total — the matrix PASS count (passed) stays at the 17 (metric,
	// func) pairs, so the run summary still reads "N metric/func assertion(s)"
	// while the ledger/rejection/sampling lines stand as their own evidence. Their
	// windows anchor at statusAnchor (realtime builtins), NOT stream.Base.
	// Poll the shared __src_ingestion_status breakdown ONCE until both the rejection
	// statuses and the conservation ledger converge (or ledgerTimeout), then render
	// each criterion from that single converged snapshot. The two used to poll
	// separately with identical queries; sharing one poll halves the worst-case
	// wall-clock (one ledgerTimeout budget, not two serial ones).
	ledgerBD, ledgerBody := pollIngestionLedger(ctx, o.apiAddr, stream, statusAnchor)
	rjPassed, rjFailed := assertRejections(rec, o.apiAddr, d.tag, stream, statusAnchor, ledgerBD, ledgerBody)
	ldPassed, ldFailed := assertConservationLedger(rec, o.apiAddr, d.tag, stream, statusAnchor, ledgerBD, ledgerBody)
	if ok, det := assertNoAggSampling(ctx, rec, o.apiAddr, d.tag, statusAnchor); !ok {
		failed++
		const samplingTripwire = "agg sampling tripwire (__agg_sampling_factor non-zero)"
		rec.logf("FAIL %s: %s\n%s", d.name, samplingTripwire, indent(det))
		fmt.Printf("FAIL %s: %s\n", d.tag, samplingTripwire)
	} else {
		// Echo the PASS so the tripwire's success is explicit evidence in the run
		// log, mirroring the ledger/rejection lines — a silent tripwire is
		// indistinguishable from one that never ran.
		rec.logf("PASS %s: agg sampling tripwire — %s absent/zero across the run", d.name, aggSamplingFactorMetric)
		fmt.Printf("PASS %s: agg sampling tripwire — __agg_sampling_factor absent\n", d.tag)
	}
	failed += rjFailed + ldFailed
	rec.logf("%s: assertions: %d PASS, %d FAIL (matrix %d, rejections %d/%d, ledger %d/%d)",
		d.name, passed, failed, passed, rjPassed, rjPassed+rjFailed, ldPassed, ldPassed+ldFailed)
	if failed == 0 {
		fmt.Printf("PASS %s: %d metric/func assertion(s) matched\n", d.tag, passed)
	} else {
		fmt.Printf("FAIL %s: %d PASS, %d FAIL\n", d.tag, passed, failed)
	}
	return passed, failed
}

// resolveArch picks the GOARCH to cross-compile daemons for. An explicit --arch
// wins; otherwise runtime.GOARCH — arm64 on the verified macOS/lima paths, amd64
// on an amd64 Linux box — matching ("detected at preflight, default
// arm64, overridable … an amd64 Linux box builds amd64").
func resolveArch(flagArch string) string {
	if flagArch != "" {
		return flagArch
	}
	return runtime.GOARCH
}

// pruneStale removes every e2e-* container and network it can find. Returns the
// counts removed. Best-effort: errors on individual resources are ignored so one
// stuck resource doesn't abort the run.
//
// The e2e- prefix is shared by every run, so this pruning assumes a single
// harness run at a time: concurrent runs would delete each other's live stack.
func pruneStale(ctx context.Context, rt Runtime) (containers, networks int) {
	for _, c := range listSafe(ctx, rt.ContainerList) {
		if strings.HasPrefix(c, e2ePrefix) {
			if err := rt.Rm(ctx, c, true); err == nil {
				containers++
			}
		}
	}
	for _, n := range listSafe(ctx, rt.NetworkList) {
		if strings.HasPrefix(n, e2ePrefix) {
			if err := rt.NetworkRemove(ctx, n); err == nil {
				networks++
			}
		}
	}
	return containers, networks
}

func listSafe(ctx context.Context, fn func(context.Context) ([]string, error)) []string {
	v, err := fn(ctx)
	if err != nil {
		return nil
	}
	return v
}

// resolveAPIPort applies the --api-port flag to the publish config's "api"
// entry — the single source every downstream reader (publishSpec for the -p
// flag, hostAddr for the harness's own query address) already consults:
//   - "" leaves config.yaml's publish.api untouched (default behavior).
//   - "auto" grabs a free port on the loopback via a brief net.Listen(":0").
//   - any other value must parse as a port number 1-65535 and overrides the host
//     port, keeping the configured host IP (defaulting to 127.0.0.1 when api is
//     unpublished).
//
// The result is validated as host:port (matching loadConfig's validatePublish)
// so checkPublishedPortsFree and publishSpec never see a malformed value.
func resolveAPIPort(cfg publishConfig, flagVal string) (publishConfig, error) {
	if flagVal == "" {
		return cfg, nil
	}
	host := "127.0.0.1"
	if existing := cfg.hostAddr("api"); existing != "" {
		if h, _, perr := net.SplitHostPort(existing); perr == nil && h != "" {
			host = h
		}
	}
	var port int
	switch {
	case flagVal == "auto":
		ln, lerr := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if lerr != nil {
			return nil, fmt.Errorf("pick free api port on %s: %w", host, lerr)
		}
		port = ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
	default:
		n, perr := strconv.Atoi(flagVal)
		if perr != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("%q must be \"auto\" or a port number 1-65535", flagVal)
		}
		port = n
	}
	cfg["api"] = net.JoinHostPort(host, strconv.Itoa(port))
	if err := validatePublish("api", cfg["api"]); err != nil {
		return nil, err
	}
	return cfg, nil
}

// checkPublishedPortsFree dials every host address the config publishes; if any
// already accepts a connection, another process owns that port (a parallel e2e
// run, a --keep stack, or a stray binding). Two concurrent harness runs collide
// hard: they prune each other's containers AND share the fixed api publish port,
// so one run's assertions silently query the other's api. Failing here — before
// the stack publishes anything — is far cheaper than debugging the cross-talk.
// The dial is short: nothing answering returns "refused" instantly; any other
// error is treated as "free" so a flaky loopback never blocks a clean run.
func checkPublishedPortsFree(ctx context.Context, cfg publishConfig) error {
	var clash []string
	for _, host := range cfg {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		c, err := d.DialContext(ctx, "tcp", host)
		if err == nil {
			c.Close()
			clash = append(clash, host)
		}
	}
	if len(clash) > 0 {
		sort.Strings(clash)
		return fmt.Errorf("published port(s) already in use: %s — another e2e run? a --keep stack? stop it, or change e2e/config.yaml publish ports",
			strings.Join(clash, ", "))
	}
	return nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if fileExists(filepath.Join(dir, "go.mod")) && fileExists(filepath.Join(dir, "e2e")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not locate repo root (no go.mod with an e2e/ dir upward of CWD); run from the statshouse checkout")
		}
		dir = parent
	}
}

// recorder captures every progress line for the artifacts summary and prints it
// to stderr (so PASS/FAIL stdout stays clean for scripting). It also accumulates
// the raw JSON of every FAILED /api/query (raw JSON of failed queries
// in the artifacts on every run), and — under -v — the raw JSON of every
// assertion query (pass or fail), so a verbose run leaves a complete response
// trail. verbose/artifactsDir are set once in realMain; the failed-query list is
// guarded by a mutex so the assertion path's appends and the artifact writer's
// snapshot are safe regardless of how they are scheduled. (The live log streamer
// does NOT touch this list — it writes daemon log lines straight to stderr — so it
// is not part of that concurrency.)
type recorder struct {
	lines        []string
	verbose      bool
	artifactsDir string
	runID        string // the e2e-<runID>- prefix of THIS run's containers (stripped by serviceLogName)

	fqMu          sync.Mutex
	failedQueries []failedQuery
}

// failedQuery is one /api/query whose result did not satisfy an assertion, with
// the verbatim response bytes (the exact payload a human needs to diagnose a
// mismatch without rerunning). Serialized into e2e/test-results/<runid>/failed-
// queries.json. Body is "" when the query itself errored before any body (e.g.
// a connection refused); the Label/URL still pinpoints it.
type failedQuery struct {
	Label      string `json:"label"`       // "value" / "write_err" / "sampling"
	Client     string `json:"client"`      // driver tag (go/rust/cpp)
	Metric     string `json:"metric"`      // metric name (or builtin)
	Func       string `json:"func"`        // qw (count/sum/p50/…) for value assertions
	URL        string `json:"url"`         // the exact /api/query URL
	HTTPStatus int    `json:"http_status"` // 0 when the request itself failed
	Body       string `json:"body"`        // raw response JSON (verbatim)
}

func (r *recorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	r.lines = append(r.lines, line)
	fmt.Fprintln(os.Stderr, "[e2e] "+line)
}

// recordFailedQuery appends one failed query for the artifacts dump. Safe for
// concurrent use (the assertion polls and the live streamer run concurrently).
func (r *recorder) recordFailedQuery(fq failedQuery) {
	if r == nil {
		return
	}
	r.fqMu.Lock()
	r.failedQueries = append(r.failedQueries, fq)
	r.fqMu.Unlock()
}

// snapshotFailedQueries returns a copy of the recorded failed queries (nil if
// none), so the writer can serialize without holding the lock across file I/O.
func (r *recorder) snapshotFailedQueries() []failedQuery {
	if r == nil {
		return nil
	}
	r.fqMu.Lock()
	defer r.fqMu.Unlock()
	if len(r.failedQueries) == 0 {
		return nil
	}
	out := make([]failedQuery, len(r.failedQueries))
	copy(out, r.failedQueries)
	return out
}

// dumpQueryResponse writes one assertion query's raw response to
// artifacts/queries/<client>__<metric>__<qw>.json. Used under -v so a verbose
// run leaves the verbatim reply to EVERY assertion query (pass or fail), not
// only the failing ones. Best-effort: a write error is logged, never fatal.
func (r *recorder) dumpQueryResponse(clientTag, metric, qw, body string) {
	if r == nil || r.artifactsDir == "" {
		return
	}
	name := sanitizeFileName(clientTag) + "__" + sanitizeFileName(metric) + "__" + sanitizeFileName(qw) + ".json"
	dir := filepath.Join(r.artifactsDir, "queries")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.logf("could not create queries dir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		r.logf("could not write query response %s: %v", path, err)
		return
	}
}

// writeFailedQueries writes the recorded failed queries as a JSON array to
// <artifacts>/failed-queries.json. Called on every run (pass and fail): a clean
// PASS has no failed queries, so the file is written only when there is at least
// one (an empty array on a green run would be noise). Always returns nil — a
// dump failure is logged but must not mask the run result.
func writeFailedQueries(artifactsDir string, fqs []failedQuery, rec *recorder) {
	if len(fqs) == 0 {
		return
	}
	data, err := json.MarshalIndent(fqs, "", "  ")
	if err != nil {
		if rec != nil {
			rec.logf("could not marshal failed-query dump: %v", err)
		}
		return
	}
	path := filepath.Join(artifactsDir, "failed-queries.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		if rec != nil {
			rec.logf("could not write %s: %v", path, err)
		}
		return
	}
	if rec != nil {
		rec.logf("wrote failed-queries.json (%d failed query/queries)", len(fqs))
	}
}

// sanitizeFileName reduces a string to a filename-safe form (the metric name
// already restricts itself to [A-Za-z0-9_], but the builtin names carry a "__"
// prefix and the qw/client values are short words; this keeps the dump path
// predictable on any platform). Path separators and spaces collapse to "_".
func sanitizeFileName(s string) string {
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// fail records the failure, dumps every started container's logs to artifacts
// (so any daemon crash leaves a <service>.log for diagnosis), writes the summary,
// and returns the exit code. rt is nil before the runtime is selected; containers
// holds whatever was started before the failure (possibly empty).
func fail(rec *recorder, artifactsDir string, rt Runtime, containers []string, err error) int {
	msg := fmt.Sprintf("FAIL: %v", err)
	rec.lines = append(rec.lines, msg)
	fmt.Fprintln(os.Stderr, "[e2e] "+msg)
	dumpServiceLogs(rec, rt, containers, artifactsDir)
	writeRunArtifacts(artifactsDir, rec)
	fmt.Fprintln(os.Stderr, msg)
	return 1
}

// dumpServiceLogs writes each started container's accumulated logs to
// <artifacts>/<service>.log (the service name is the last dash-segment of the
// container name, e.g. e2e-<runid>-metadata -> metadata.log). Best-effort: a
// capture failure is logged but never masks the run result.
func dumpServiceLogs(rec *recorder, rt Runtime, containers []string, artifactsDir string) {
	if rt == nil || len(containers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	existing, err := rt.ContainerList(ctx)
	if err != nil {
		rec.logf("could not capture service logs (list containers): %v", err)
		return
	}
	present := make(map[string]bool, len(existing))
	for _, c := range existing {
		present[c] = true
	}
	for _, c := range containers {
		if !present[c] {
			continue
		}
		logs, lerr := rt.Logs(ctx, c)
		if lerr != nil {
			rec.logf("could not capture logs for %s: %v", c, lerr)
			continue
		}
		name := serviceLogName(c, rec.runID)
		if werr := os.WriteFile(filepath.Join(artifactsDir, name+".log"), []byte(logs), 0o644); werr != nil {
			rec.logf("could not write %s.log: %v", name, werr)
			continue
		}
		rec.logf("wrote %s.log (%d bytes)", name, len(logs))
	}
}

// serviceLogName reduces a container name to its service role. With the run id
// known it strips e2e-<runid>- and keeps the whole remainder, so the conformance
// mode's two stacks stay distinct (e2e-<runid>-ch-agg -> ch-agg, ...-duck-agg ->
// duck-agg); without one (older callers) it falls back to the last dash segment
// (e2e-<runid>-clickhouse -> clickhouse).
func serviceLogName(container string, runID string) string {
	if prefix := e2ePrefix + runID + "-"; runID != "" && strings.HasPrefix(container, prefix) {
		return strings.TrimPrefix(container, prefix)
	}
	if i := strings.LastIndex(container, "-"); i >= 0 {
		return container[i+1:]
	}
	return container
}

func writeSummary(artifactsDir string, lines []string) {
	path := filepath.Join(artifactsDir, "summary.txt")
	// Best-effort; a failure to write the summary must not mask the real result.
	_ = os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

// writeRunArtifacts writes BOTH diagnostics artifacts required on
// every run: the PASS/FAIL summary (always) and the raw JSON of failed queries
// (when any — a clean PASS has none). Called on the pass path, the fail() path,
// and the assertion-failure path, so the artifacts dir is complete regardless of
// where the run stopped.
func writeRunArtifacts(artifactsDir string, rec *recorder) {
	writeFailedQueries(artifactsDir, rec.snapshotFailedQueries(), rec)
	writeSummary(artifactsDir, rec.lines)
}

// startLogStreamer tails every container's logs to stderr while the run proceeds
// (-v streams logs live). apple/container's `logs` is a one-shot fetch
// (no -f follow), so this polls each container on an interval and writes the bytes
// appended since the last fetch, each line prefixed [<service>] to distinguish a
// daemon log line from the harness's own [e2e] progress lines. Best-effort: any
// fetch error is swallowed (a transient CLI hiccup must not abort the run). Returns
// a stop func that cancels the tail goroutine; the caller stops it before teardown.
func startLogStreamer(ctx context.Context, rt Runtime, containers []string, runID string) (stop func()) {
	sctx, cancel := context.WithCancel(ctx)
	var once sync.Once
	stop = func() { once.Do(cancel) }
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		last := make(map[string]int, len(containers))
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				for _, c := range containers {
					tctx, tcancel := context.WithTimeout(sctx, 10*time.Second)
					logs, err := rt.Logs(tctx, c)
					tcancel()
					if err != nil || len(logs) <= last[c] {
						continue
					}
					delta := logs[last[c]:]
					last[c] = len(logs)
					svc := serviceLogName(c, runID)
					for _, line := range strings.Split(strings.TrimRight(delta, "\n"), "\n") {
						if line != "" {
							fmt.Fprintf(os.Stderr, "[%s] %s\n", svc, line)
						}
					}
				}
			}
		}
	}()
	return stop
}
