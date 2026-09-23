// Command e2e is the StatsHouse end-to-end harness. It cross-compiles the
// daemons, brings up metadata, agg, api and agent in containers over either
// storage backend (ClickHouse, or duck with no ClickHouse container), drives
// the real clients against the agent and asserts the api's answers.
// --conformance instead boots both backends over one shared metadata, seeds
// the same stream to both and compares their decoded answers (CH is the
// reference).
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// e2ePrefix namespaces every resource the harness creates so it can prune its
// own leftovers without touching unrelated containers/networks.
const e2ePrefix = "e2e-"

// runIDRe constrains --run-id: it becomes resource names (apple/container
// requires lowercase network names) and an artifacts path; a leading dash would
// read as a CLI flag.
var runIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func main() {
	var (
		runtimeFlag     = flag.String("runtime", "", "container runtime: \"container\" (apple, default on macOS) or \"docker\" (default on Linux); auto-detected if empty")
		runIDFlag       = flag.String("run-id", "", "run identifier (default: local datetime 20060102-150405)")
		archFlag        = flag.String("arch", "", "GOARCH to cross-compile daemons for (default arm64; the apple/container + lima/arm64 verification path)")
		backend         storageBackend
		keep            = flag.Bool("keep", false, "keep containers+network after the run for debugging")
		verbose         = flag.Bool("v", false, "verbose: stream container logs live and dump raw API responses to artifacts")
		timeout         = flag.Duration("timeout", 10*time.Minute, "overall run timeout")
		skipClientBuild = flag.Bool("skip-client-build", false, "reuse previously-built client driver binaries + cached stream (skip the in-container compile); fails if no cached build exists for a selected client")
		withUI          = flag.Bool("with-ui", false, "build the npm UI in a pinned node container and serve it from the api's --static-dir (default off: no node, no UI build). On apple/container this needs npm on the host to warm the build cache (apple/container has no in-container network); the docker runtime installs online in the node container")
		apiPortFlag     = flag.String("api-port", "", `the api's published host port on 127.0.0.1: "" is 10888; "auto" picks a free port; or a port number, e.g. "10889", so concurrent runs don't collide`)
		prewarmRetries  = flag.Int("prewarm-retries", 2, "extra attempts when a client's pre-warm times out (driver exit 3, a transient journal-longpoll stall); 0 fails immediately (pre-change behavior)")
		conformance     = flag.Bool("conformance", false, "differential conformance run: boot ClickHouse plus TWO daemon stacks (ch-backed and duck-backed) over one shared metadata, seed the identical deterministic stream to both agents from the harness itself, and compare both apis' decoded answers to every query shape (CH is the reference; divergence fails the run)")
		clientSel       clientFlag
	)
	flag.Var(&clientSel, "client", "client(s) to drive (repeatable; one of: go, rust, cpp). Default: all three")
	flag.Var(&backend, "storage-backend", "storage backend the daemons run: \"clickhouse\" (default; the usual stack) or \"duck\" (DuckDB embedded in the aggregator; no ClickHouse container, the api reads through the aggregator's RPC)")
	flag.Parse()
	if *conformance {
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

// clientFlag is a repeatable --client selector; empty selects all clients.
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

// clientDriver is one client the harness can build, run and assert. tag is both
// the --client selector and the metric-name prefix isolating this client's
// writes. renderSource is pure so the --skip-client-build guard can hash the
// driver source without building.
type clientDriver struct {
	name         string // e.g. "statshouse-go"
	tag          string // e.g. "go"
	baseImage    string // pinned toolchain tag (goBaseImage / rustBaseImage / cppBaseImage)
	buildRun     func(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error)
	renderSource func(repoRoot string, stream metricStream) (string, error)
}

// clientDrivers lists every client, in default run order.
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

// selectDrivers resolves --client selectors (tag or full client name) to
// drivers; empty means all, duplicates collapse.
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

// validateSkipClientBuild checks every selected driver's cached build on the
// host before the ~1min stack bring-up, so a missing or stale cache fails in
// under a second.
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

// driverTags returns the comma-joined driver tags for the progress log.
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

	rec := &recorder{verbose: verbose, artifactsDir: artifactsDir, runID: runID}
	rec.logf("runid=%s artifacts=%s", runID, artifactsDir)

	network := e2ePrefix + runID
	chContainer := e2ePrefix + runID + "-clickhouse"

	// NotifyContext cancels rather than kills, so deferred teardown still runs
	// on SIGINT/SIGTERM.
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

	// Without in-container network (apple/container) the UI build is offline
	// against a cache warmed by host npm, which needs --os/--cpu/--libc.
	if withUI && !rt.HasNetworkEgress() {
		if !lookPath("npm") {
			return fail(rec, artifactsDir, rt, nil, fmt.Errorf("--with-ui needs npm on the host to warm the build cache (apple/container has no in-container network); install Node/npm or use the docker runtime"))
		}
		if m := npmMajorVersion(ctx); m < npmMinMajor {
			return fail(rec, artifactsDir, rt, nil, fmt.Errorf("--with-ui needs npm >= %d on the host (found major %d) to populate the offline build cache with --os/--cpu/--libc; upgrade npm or use the docker runtime", npmMinMajor, m))
		}
	}

	// Resolved before any container exists so --skip-client-build fails fast.
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

	prunedC, prunedN := pruneStale(ctx, rt)
	if prunedC+prunedN > 0 {
		rec.logf("pruned %d stale container(s), %d stale network(s)", prunedC, prunedN)
	} else {
		rec.logf("no stale e2e-* resources to prune")
	}

	// Every container created during the run, appended as it starts (even on
	// partial starts), for teardown and fail().
	var containers []string

	// Pre-declared for the --keep teardown; empty/nil on an early abort.
	var (
		apiAddr string
		ds      *daemonStack
	)

	// Teardown uses a fresh context: the run ctx may already be cancelled.
	teardown := func() {
		if keep {
			rec.logf("keeping resources (--keep): %d container(s) %v on network %s", len(containers), containers, network)
			addr := apiAddr
			if addr == "" && ds != nil && ds.api != nil {
				addr = net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort)) + "  (container IP — reachable from the run network)"
			}
			if addr != "" {
				now := time.Now()
				rec.logf("  reach the api:  curl 'http://%s/api/query?s=__agg_bucket_receive_delay_sec&f=%d&t=%d&w=1s&qw=count&ac=1'",
					addr, now.Add(-5*time.Minute).Unix(), now.Unix())
				if withUI {
					// Strip the "(container IP — …)" annotation off addr.
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
			// summary.txt was already written; rewrite it to include these lines.
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

	apiAddr, err = resolveAPIAddr(apiPortFlag)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("--api-port: %w", err))
	}
	rec.logf("api published at %s", apiAddr)
	if err := checkPublishedPortFree(ctx, apiAddr); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}

	// The UI is built before ClickHouse so a UI failure fails fast. The api is
	// built without the embed tag, so it always serves --static-dir: the
	// placeholder page by default, the built UI under --with-ui.
	apiStaticDir := filepath.Join(root, "e2e", "api-static")
	apiMountTarget := apiStaticMount
	if withUI {
		// Tracked before launch: a cancelled build can orphan the container.
		uiBuildC := e2ePrefix + runID + "-uibuild"
		containers = append(containers, uiBuildC)
		uiBuildDir, err := buildUI(ctx, rt, root, cache, uiBuildC, rec.logf)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build ui: %w", err))
		}
		apiStaticDir = uiBuildDir
		apiMountTarget = apiUIMount
	}

	// Under duck the aggregator is the storage; its store-query RPC probe (in
	// startDaemonStack) replaces the ClickHouse schema probe.
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

	binDir, err := buildDaemons(ctx, root, arch, backend, rec.logf)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build daemons: %w", err))
	}

	// Shared RPC crypto key: the nonce exchange requires encryption between
	// peers on different machines, which every container pair is.
	rpcKeyPath, err := writeRPCKey()
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	defer os.Remove(rpcKeyPath)

	start := time.Now()
	chStackTag := ""
	if conformance {
		// Distinct container names for the two stacks on one network.
		chStackTag = confStackCH
	}
	ds, err = startDaemonStack(ctx, rt, rec, daemonStackOpts{
		network:      network,
		chIP:         chIP,
		binDir:       binDir,
		runID:        runID,
		apiPublish:   apiAddr,
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

	queryAddr := apiAddr
	body, err := queryAPI(ctx, queryAddr)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("api query: %w", err))
	}
	rec.logf("/api/query answered 200 on %s (%d bytes)", queryAddr, len(body))

	// Prove the api serves the built UI at /, not the placeholder.
	if withUI {
		uiBody, err := assertUIServed(ctx, queryAddr)
		if err != nil {
			return fail(rec, artifactsDir, rt, containers, fmt.Errorf("ui served: %w", err))
		}
		rec.logf("UI served on %s (GET / -> 200, built index.html %d bytes)", queryAddr, len(uiBody))
	}

	// Gate on a real agent→agg→api round-trip: the agent↔agg channel can be
	// dead while every TCP probe is green.
	if err := waitAggConveyor(ctx, queryAddr); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	rec.logf("agent↔agg conveyor live (recent %s point)", queryMetric)

	// Conformance: boot the duck stack over the shared metadata. Only the
	// aggregator differs, so this buildDaemons compiles just the duck agg.
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
			rpcKeyPath:   rpcKeyPath,
			apiStaticDir: apiStaticDir,
			staticMount:  apiMountTarget,
			backend:      backendDuck,
			stackTag:     confStackDuck,
			sharedMeta:   ds.metadata,
		})
		// Metadata is already tracked; append only the started duck services
		// (nil-safe on a partial start).
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

	// Under -v, stream daemon logs live. This defer runs before teardown's
	// (LIFO), so the tail never races the container Rm.
	var stopStreamer func()
	if verbose {
		stopStreamer = startLogStreamer(ctx, rt, containers, runID)
	}
	defer func() {
		if stopStreamer != nil {
			stopStreamer()
		}
	}()

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
		// The harness seeds both agents itself (one hostname, so _h/max_host
		// agree across backends) instead of running client drivers.
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
	if totalFail > 0 {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
		writeRunArtifacts(artifactsDir, rec)
		return 1
	}
	// A cut-short differential must not read as PASS.
	if cancelled {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
		writeRunArtifacts(artifactsDir, rec)
		summary := fmt.Sprintf("INTERRUPTED: conformance differential cancelled before finishing — %d passed, results incomplete, runtime=%s, runid=%s",
			totalPass, rt.Name(), runID)
		rec.logf("%s", summary)
		fmt.Println(summary)
		return 1
	}

	// On success service logs are captured only under -v.
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

// clientPhaseOpts configures runClientPhase; shared across clients.
type clientPhaseOpts struct {
	network          string
	agentIP          string
	apiAddr          string // published api address for assertions
	apiContainerAddr string // api <ip>:port on the run network (driver pre-warm polling)
	runID            string
	arch             string
	repoRoot         string
	artifactsDir     string
	cache            string
	skipClientBuild  bool
	prewarmRetries   int
}

// preWarmRetryBackoff lets a transient journal-longpoll stall clear while the
// default retries stay well inside the run timeout.
const preWarmRetryBackoff = 20 * time.Second

// runClientPhase drives one client: generate its stream, pre-create value_p
// metrics, build and run its driver, then assert. It waits for the driver to
// exit before asserting because rust/cpp flush only on destruction.
func runClientPhase(ctx context.Context, rt Runtime, rec *recorder, d clientDriver, o clientPhaseOpts) (passed, failed int) {
	spec, buildCache, err := clientBuildCacheFor(o.repoRoot, o.cache, d.name, d.tag, o.arch)
	if err != nil {
		rec.logf("FAIL %s: resolve build cache: %v", d.name, err)
		fmt.Printf("FAIL %s: resolve build cache: %v\n", d.tag, err)
		return 0, 1
	}

	// The realtime builtins (__src_ingestion_status, __src_client_write_err,
	// __agg_sampling_factor) are recorded at receive time, not the events'
	// historic ts; a --skip-client-build replay keeps an old stream.Base, so
	// their windows anchor here instead.
	statusAnchor := uint32(time.Now().Unix())

	// A cached driver binary embeds its stream's names and base, so a replay
	// regenerates the identical stream from the cached descriptor (runID+base).
	stream, cached, err := streamForClientPhase(d, o, buildCache)
	if err != nil {
		rec.logf("FAIL %s: %v", d.name, err)
		fmt.Printf("FAIL %s: %v\n", d.tag, err)
		return 0, 1
	}
	rec.logf("%s: stream %s: base=%d buckets=%d metrics=%d writes=%d",
		d.name, streamSourceLabel(cached), stream.Base, numBuckets, len(stream.Metrics), len(stream.Writes))

	// value_p never auto-creates (autocreate derives only counter/value/unique).
	if err := createValuePMetrics(ctx, rec, o.apiAddr, stream); err != nil {
		rec.logf("FAIL %s: pre-create value_p metrics: %v", d.name, err)
		fmt.Printf("FAIL %s: pre-create value_p metrics: %v\n", d.tag, err)
		return 0, len(stream.Metrics)
	}

	agentAddr := net.JoinHostPort(o.agentIP, strconv.Itoa(agentPort))
	clientContainer := e2ePrefix + o.runID + "-client-" + d.tag

	// Write the descriptor before the build so a new binary is never paired
	// with an old descriptor if the run later fails. A failed build/run deletes
	// it, since a compile failure leaves the old binary in place.
	if !cached {
		src, rerr := d.renderSource(o.repoRoot, stream)
		var srcHash string
		if rerr != nil {
			// buildRun will fail too, and then the descriptor is deleted.
			rec.logf("%s: could not hash driver source for cache descriptor: %v", d.name, rerr)
		} else {
			srcHash = sourceHash(src)
		}
		if err := saveStreamCacheMeta(buildCache, o.runID, stream.Base, d.tag, o.arch, srcHash, d.baseImage); err != nil {
			rec.logf("%s: could not write build descriptor: %v", d.name, err)
		}
	}

	// Only a pre-warm timeout (preWarmExit) is retried: it is a transient
	// apple/container journal-longpoll stall where the api serves a stale
	// metrics list. Rebuilds on retry hit the mounted compile caches.
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
			removeStreamCacheDescriptor(rec, d.name, buildCache)
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
		if exitCode == preWarmExit {
			rec.logf("FAIL %s: pre-warm timed out (metrics never created — agent/agg path down?)\n%s", d.name, indent(output))
			fmt.Printf("FAIL %s: pre-warm timed out (agent/agg path down?)\n", d.tag)
		} else {
			rec.logf("FAIL %s: driver exited %d\n%s", d.name, exitCode, indent(output))
			fmt.Printf("FAIL %s driver exited %d\n", d.tag, exitCode)
		}
		removeStreamCacheDescriptor(rec, d.name, buildCache)
		return 0, len(stream.Metrics)
	}
	rec.logf("%s: driver exited 0\n%s", d.name, indent(truncate(strings.TrimSpace(output), 1200)))
	// Not fatal: a later --skip-client-build fails with "no driver binary".
	if !cached && !fileExists(filepath.Join(buildCache, driverBinName)) {
		rec.logf("%s: build did not cache a driver binary at %s", d.name, filepath.Join(buildCache, driverBinName))
	}

	// Silent-loss tripwire, checked first so a TCP-backpressure drop reads as one
	// labelled failure rather than N "count too low" mismatches.
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

	// Rejection statuses, conservation ledger and the sampling tripwire. Only
	// their failures fold into the total, so passed stays the matrix count. One
	// shared __src_ingestion_status poll serves both rejections and ledger.
	ledgerBD, ledgerBody := pollIngestionLedger(ctx, o.apiAddr, stream, statusAnchor)
	rjPassed, rjFailed := assertRejections(rec, o.apiAddr, d.tag, stream, statusAnchor, ledgerBD, ledgerBody)
	ldPassed, ldFailed := assertConservationLedger(rec, o.apiAddr, d.tag, stream, statusAnchor, ledgerBD, ledgerBody)
	if ok, det := assertNoAggSampling(ctx, rec, o.apiAddr, d.tag, statusAnchor); !ok {
		failed++
		const samplingTripwire = "agg sampling tripwire (__agg_sampling_factor non-zero)"
		rec.logf("FAIL %s: %s\n%s", d.name, samplingTripwire, indent(det))
		fmt.Printf("FAIL %s: %s\n", d.tag, samplingTripwire)
	} else {
		// A silent tripwire is indistinguishable from one that never ran.
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

// resolveArch picks the daemons' GOARCH: --arch, else the host's.
func resolveArch(flagArch string) string {
	if flagArch != "" {
		return flagArch
	}
	return runtime.GOARCH
}

// pruneStale best-effort removes every e2e-* container and network. The prefix
// is shared by every run, so concurrent runs would delete each other's stacks.
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

// resolveAPIAddr turns --api-port into the api's published host address:
// "" is the default 127.0.0.1:10888, "auto" a free loopback port.
func resolveAPIAddr(flagVal string) (string, error) {
	port := apiPort
	switch flagVal {
	case "":
	case "auto":
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return "", fmt.Errorf("pick a free api port: %w", err)
		}
		port = ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
	default:
		n, err := strconv.Atoi(flagVal)
		if err != nil || n < 1 || n > 65535 {
			return "", fmt.Errorf("%q must be \"auto\" or a port number 1-65535", flagVal)
		}
		port = n
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
}

// checkPublishedPortFree fails when something already answers on the api's
// host address, so a concurrent run is caught before its assertions silently
// query the other run's api.
func checkPublishedPortFree(ctx context.Context, addr string) error {
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	if c, err := d.DialContext(ctx, "tcp", addr); err == nil {
		c.Close()
		return fmt.Errorf("published port %s already in use — another e2e run? a --keep stack? stop it, or pass --api-port", addr)
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

// recorder logs progress lines to stderr (keeping stdout clean for PASS/FAIL)
// and keeps them for summary.txt, plus the raw responses of failed queries.
type recorder struct {
	lines        []string
	verbose      bool
	artifactsDir string
	runID        string

	fqMu          sync.Mutex
	failedQueries []failedQuery
}

// failedQuery is one /api/query that failed an assertion, with the verbatim
// response, dumped to failed-queries.json.
type failedQuery struct {
	Label      string `json:"label"`
	Client     string `json:"client"`
	Metric     string `json:"metric"`
	Func       string `json:"func"`
	URL        string `json:"url"`
	HTTPStatus int    `json:"http_status"` // 0 when the request itself failed
	Body       string `json:"body"`
}

func (r *recorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	r.lines = append(r.lines, line)
	fmt.Fprintln(os.Stderr, "[e2e] "+line)
}

// recordFailedQuery appends one failed query; safe for concurrent use.
func (r *recorder) recordFailedQuery(fq failedQuery) {
	if r == nil {
		return
	}
	r.fqMu.Lock()
	r.failedQueries = append(r.failedQueries, fq)
	r.fqMu.Unlock()
}

// snapshotFailedQueries returns a copy of the recorded failed queries.
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
// queries/<client>__<metric>__<qw>.json (used under -v). Best-effort.
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

// writeFailedQueries writes failed-queries.json when there is at least one.
// Best-effort: a failure is logged and must not mask the run result.
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

// sanitizeFileName maps every rune outside [A-Za-z0-9_-] to "_".
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

// fail records the failure, dumps the started containers' logs and the
// artifacts, and returns the exit code. rt is nil before runtime selection.
func fail(rec *recorder, artifactsDir string, rt Runtime, containers []string, err error) int {
	msg := fmt.Sprintf("FAIL: %v", err)
	rec.lines = append(rec.lines, msg)
	fmt.Fprintln(os.Stderr, "[e2e] "+msg)
	dumpServiceLogs(rec, rt, containers, artifactsDir)
	writeRunArtifacts(artifactsDir, rec)
	fmt.Fprintln(os.Stderr, msg)
	return 1
}

// dumpServiceLogs writes each still-present container's logs to
// <artifacts>/<service>.log. Best-effort.
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

// serviceLogName strips e2e-<runid>- from a container name, keeping the whole
// remainder so conformance's ch-agg and duck-agg stay distinct; without a run
// id it keeps the last dash segment.
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
	_ = os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

// writeRunArtifacts writes summary.txt and, if any, failed-queries.json.
func writeRunArtifacts(artifactsDir string, rec *recorder) {
	writeFailedQueries(artifactsDir, rec.snapshotFailedQueries(), rec)
	writeSummary(artifactsDir, rec.lines)
}

// startLogStreamer tails container logs to stderr, each line prefixed
// [<service>]. apple/container's `logs` has no -f, so it polls and prints the
// bytes appended since the last fetch. Fetch errors are ignored.
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
