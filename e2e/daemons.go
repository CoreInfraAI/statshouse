package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// alpineBase is the minimal image each daemon binary is bind-mounted into.
	// No image builds anywhere. alpine (busybox) gives the static Go
	// binaries a Linux userland + /bin/sh for the entrypoint; pinned to the exact
	// minor tag present locally (3.24 on this machine), not the floating alpine:3
	// tag, so a rerun reproduces the same userland (pinned base image).
	alpineBase = "alpine:3.24"

	metaPort   = 2442  // metadata RPC
	aggPort    = 13336 // aggregator receive
	apiPort    = 10888 // api HTTP (the only published port by default)
	apiRPCPort = 10889 // api RPC
	agentPort  = 13337 // agent client: raw UDP + RPC TCP

	// duckStoreMount is the in-container directory the aggregator's duck-store
	// owns under the duck backend. The container's writable layer is enough —
	// the store lives and dies with the run, like ClickHouse's data dir.
	duckStoreMount = "/store"

	// rpcKeyMount is where the shared RPC crypto key is mounted inside every
	// daemon container. The agg/agent read it from this default path
	// (defaultPathToPwd = "/etc/engine/pass"); the metadata/api read it via
	// --rpc-crypto-path=/etc/engine/pass. All four must hold the SAME key so the
	// nonce-exchange handshake (which requires encryption whenever the two peers
	// are not on the same machine) succeeds for every cross-container link.
	rpcKeyMount = "/etc/engine/pass"

	// apiStaticMount is where the placeholder index.html is mounted into the api
	// (the default, UI-less run). The api parses index.html as a Go template at
	// startup (and refuses to start if it is missing); the harness ships a
	// placeholder page (e2e/api-static/index.html) unless --with-ui builds the real
	// npm UI, which is then mounted at apiUIMount instead. The actual in-container
	// mount target for a given run is daemonStackOpts.staticMount; the api reads it
	// via --static-dir=<mount>.
	apiStaticMount = "/static"

	// apiUIMount is where the built npm UI (statshouse-ui/build, with index.html at
	// its root) is mounted into the api when --with-ui is set, served via
	// --static-dir=/ui. Distinct from the placeholder /static so a UI run is
	// unambiguous in the api's flags and logs.
	apiUIMount = "/ui"

	// queryMetric is a builtin resolved in-process by the api (no metadata
	// mapping required), so /api/query returns 200 with empty data on a fresh
	// stack — proving the api answers end-to-end (it still has to reach metadata
	// + ClickHouse to build the reply). See format.BuiltinMetricByName.
	queryMetric = "__agg_bucket_receive_delay_sec"
)

// daemonStack is the set of running daemon services.
type daemonStack struct {
	metadata *service
	agg      *service
	api      *service
	agent    *service
}

// service is one running daemon container and its inspected IP.
type service struct {
	name string // container name
	ip   string
}

// containerNames returns the names of the services that were started (nil ones,
// from a partial start, are skipped) — for teardown and log capture.
func (ds *daemonStack) containerNames() []string {
	var names []string
	for _, s := range []*service{ds.metadata, ds.agg, ds.api, ds.agent} {
		if s != nil {
			names = append(names, s.name)
		}
	}
	return names
}

// daemonStackOpts configures startDaemonStack.
type daemonStackOpts struct {
	network      string
	chIP         string // ClickHouse IPv4 on the run network; "" under the duck backend (no ClickHouse exists)
	binDir       string // host dir holding the four compiled daemon binaries
	runID        string
	apiPublish   string         // host address the api is published on ("127.0.0.1:10888"); "" publishes nothing
	rpcKeyPath   string         // host path to the shared RPC crypto key (mounted into all four)
	apiStaticDir string         // host dir with index.html (mounted into the api at staticMount)
	staticMount  string         // in-container mount target = --static-dir value (apiStaticMount or apiUIMount)
	backend      storageBackend // clickhouse: the usual stack; duck: DuckDB in the aggregator, no ClickHouse
	stackTag     string         // "" default; else a tag between runID and role ("ch"/"duck") so two stacks coexist on one network
	sharedMeta   *service       // non-nil: skip starting metadata and reuse this already-running service (conformance's shared-metadata stacks)
}

// cname renders a container name. With a stackTag the role is prefixed
// "tag-role" (e2e-<runid>-ch-agg), keeping the e2ePrefix+runID prefix that
// pruneStale, teardown and the log streamer match on; without one it is the
// plain historical shape (e2e-<runid>-agg).
func (o daemonStackOpts) cname(role string) string {
	if o.stackTag != "" {
		role = o.stackTag + "-" + role
	}
	return e2ePrefix + o.runID + "-" + role
}

// keyVol is the read-only volume spec mounting the shared RPC crypto key into a
// daemon container at rpcKeyMount.
func (o daemonStackOpts) keyVol() string { return o.rpcKeyPath + ":" + rpcKeyMount + ":ro" }

// startDaemonStack brings up metadata, agg, api, and agent on the run network,
// wired entirely by inspected IP (never container DNS — apple/container in-
// container DNS does not resolve names), with the exact flags, and
// waits on each one's real readiness probe (TCP dial; no fixed sleeps).
//
// Startup order: metadata → agg → api + agent. Each daemon binary is
// bind-mounted read-only into alpineBase and run via a /bin/sh entrypoint that
// mkdirs its writable dirs then execs the binary (so the binary becomes PID 1).
func startDaemonStack(ctx context.Context, rt Runtime, rec *recorder, o daemonStackOpts) (*daemonStack, error) {
	ds := &daemonStack{}

	// --- metadata ---
	// Conformance runs TWO daemon stacks over ONE shared metadata (both aggs
	// auto-create into it; both apis journal from it) — the second stack skips
	// the start and reuses the first's service.
	if o.sharedMeta != nil {
		ds.metadata = o.sharedMeta
		rec.logf("reusing shared metadata %s at %s", ds.metadata.name, ds.metadata.ip)
	} else {
		// First boot only: --create-binlog initializes the binlog and EXITS, then the
		// server starts without it (verified in cmd/statshouse-metadata: the
		// create-binlog path returns nil immediately). Both run in ONE container
		// sharing its writable layer, so the init step's binlog is present for the
		// server. metadata is the root service, so all its flags are static literals.
		metaC := o.cname("metadata")
		// mkdir (child) -> create-binlog (child, exits 0) -> exec server (replaces
		// shell, becomes PID 1). Only the server is exec'd: exec'ing create-binlog
		// would make the (exiting) init step PID 1 and stop the container before the
		// server starts.
		metaScript := "mkdir -p /var/lib/meta/binlog && " +
			`/statshouse-metadata -p 2442 --db-path=/var/lib/meta/db --binlog-prefix=/var/lib/meta/binlog/bl --create-binlog "0,1"` +
			" && exec " +
			"/statshouse-metadata -p 2442 --db-path=/var/lib/meta/db --binlog-prefix=/var/lib/meta/binlog/bl" +
			" --rpc-crypto-path=" + rpcKeyMount
		if err := rt.Run(ctx, RunOpts{
			Name:    metaC,
			Image:   alpineBase,
			Network: o.network,
			Volumes: []string{
				filepath.Join(o.binDir, "statshouse-metadata") + ":/statshouse-metadata:ro",
				o.keyVol(),
			},
			Cmd:    []string{"/bin/sh", "-c", metaScript},
			Detach: true,
		}); err != nil {
			return ds, fmt.Errorf("start metadata: %w", err)
		}
		// Track+inspect+probe in one step (shared with agg/api/agent). The helper tracks
		// the container on the stack BEFORE the inspect so a mid-probe failure still
		// tears down the already-running container (an untracked service blocks
		// NetworkRemove while it stays attached). ds.metadata.ip is the canonical IP;
		// later blocks read it from there.
		if err := startServiceProbe(ctx, rt, rec, &ds.metadata, "metadata", metaC, o.network, metaPort, ""); err != nil {
			return ds, err
		}
	}

	// --- aggregator ---
	// --receive-budget-warming=0 is MANDATORY: the default 15m ramp
	// starves per-metric receive budgets and agents sample even tiny payloads.
	//
	// --disable-receive-sample-budget: stops the agg from advertising
	// per-metric receive budgets back to agents, so the big-unique bucket is sized
	// only by the agent's own (bumped) --sample-budget. The agg's receive-budget
	// path already skips historic writes (aggregator_handlers.go:
	// !args.IsSetHistoric()), so this is mostly belt-and-suspenders — but it
	// removes one variable from the sampling path.
	//
	// The agg must advertise its REAL run-network IP to agents, not the CH
	// cluster's host_name ("localhost"). selectShardReplica reads host_name from
	// system.clusters (config.xml remote_servers.statlogs2 -> <host>localhost</host>)
	// and uses it verbatim as the agg's own address; the agent then adopts that
	// topology and dials localhost:13336 for buckets + its metric journal — which
	// never resolves cross-container (apple/container in-container DNS does not
	// resolve names). localdebug sidesteps this by running everything on 127.0.0.1.
	//
	// --cluster-shards-addrs overrides the advertised list. The agg's own IP is
	// not known until the container starts, so the entrypoint discovers it from
	// its eth0 interface (scope global excludes loopback/link-local) and injects
	// it into the flag. Listening stays on 0.0.0.0 (robust); only the advertised
	// address is the reachable IP.
	//
	// `-u root -g root` keeps the agg as root: alpine has no 'kitten' user, and
	// ChangeUserGroup only no-ops for non-root, so it would fatally setuid to the
	// missing user otherwise (see the agent block for the full rationale).
	aggC := o.cname("agg")
	aggScript := aggRunScript(o, ds.metadata.ip)
	if err := rt.Run(ctx, RunOpts{
		Name:    aggC,
		Image:   alpineBase,
		Network: o.network,
		Volumes: []string{
			filepath.Join(o.binDir, aggBinName(o.backend)) + ":/statshouse-agg:ro",
			o.keyVol(),
		},
		Cmd:    []string{"/bin/sh", "-c", aggScript},
		Detach: true,
	}); err != nil {
		return ds, fmt.Errorf("start agg: %w", err)
	}
	if err := startServiceProbe(ctx, rt, rec, &ds.agg, "agg", aggC, o.network, aggPort, ""); err != nil {
		return ds, err
	}
	// Under duck the agg IS the storage: the stack is not ready until the
	// store-query RPC answers a real query (the replacement for "ClickHouse
	// schema finished loading" as the storage-readiness gate).
	if o.backend == backendDuck {
		if err := waitStoreQueryReady(ctx, rt, aggC, net.JoinHostPort(ds.agg.ip, strconv.Itoa(aggPort)), o.rpcKeyPath); err != nil {
			return ds, err
		}
		rec.logf("agg store-query rpc ready (real storeQuery round-trip on :%d)", aggPort)
	}

	// --- api (+ published port from config) ---
	apiC := o.cname("api")
	apiPortSpec, apiPublished := fmt.Sprintf("%s:%d", o.apiPublish, apiPort), o.apiPublish != "" // default 127.0.0.1:10888:10888
	apiStatic := filepath.Join(o.apiStaticDir, "index.html")
	if !fileExists(apiStatic) {
		return ds, fmt.Errorf("missing api static asset %q (the api needs index.html to parse at startup)", apiStatic)
	}
	apiCmd := joinSh("mkdir -p /cache", "/statshouse-api", apiDaemonFlags(o, ds.metadata.ip, ds.agg.ip)...)
	apiRun := RunOpts{
		Name:    apiC,
		Image:   alpineBase,
		Network: o.network,
		Volumes: []string{
			filepath.Join(o.binDir, "statshouse-api") + ":/statshouse-api:ro",
			o.keyVol(),
			o.apiStaticDir + ":" + o.staticMount + ":ro",
		},
		Cmd:    apiCmd,
		Detach: true,
	}
	if apiPublished {
		apiRun.Ports = []string{apiPortSpec}
	}
	if err := rt.Run(ctx, apiRun); err != nil {
		return ds, fmt.Errorf("start api: %w", err)
	}
	// The api logs its published-port spec (or "(not published)") via the probe's
	// extra log suffix; the other daemons pass "".
	apiExtra := " (not published)"
	if apiPublished {
		apiExtra = " publish=" + apiPortSpec
	}
	if err := startServiceProbe(ctx, rt, rec, &ds.api, "api", apiC, o.network, apiPort, apiExtra); err != nil {
		return ds, err
	}

	// --- agent ---
	agentC := o.cname("agent")
	agg3 := strings.TrimSuffix(strings.Repeat(net.JoinHostPort(ds.agg.ip, strconv.Itoa(aggPort))+",", 3), ",")
	agentCmd := joinSh(
		"mkdir -p /cache",
		"/statshouse",
		"-agent",
		"--cluster=statlogs2",
		"--hostname=agent1",
		"--agg-addr="+agg3,
		"--cache-dir=/cache",
		"--hardware-metric-scrape-disable",
		// neutralize agent sampling so the exact per-bucket assertions
		// are never distorted by a keep×SF multiplier. agent_shard_send.go
		// computes the per-shard sampler budget as
		//   remainingBudget = max(MinSampleBudget,
		//                         min(SampleBudget/shards, MaxUncompressedBucketSize/2) − budgetSum)
		// where budgetSum is the sum of agg-advertised per-metric budgets for the
		// built-in statshouse_* metrics, which eats the whole shard budget; our
		// freshly-created e2e metrics have NO advertised budget, so without
		// intervention they are squeezed into the default MinSampleBudget=2000 floor
		// and sampled (observed: counter values uniformly ×4.72, stag cardinality
		// 6→2..4).
		//
		// Two DISTINCT root causes masqueraded as "sampling" in early runs; do not
		// conflate them:
		//  (1) unique 100k collapsing to EXACTLY 1024 was NOT agent sampling — it
		//      was the go client's default per-bucket reservoir (defaultMaxBucketSize
		//      =1024, statshouse.go; appendUnique keeps only MaxBucketSize sampled
		//      values once a bucket overflows). Fixed DRIVER-SIDE by
		//      ConfigureArgs{MaxBucketSize:1<<18} in drivers/go/main.go.tmpl; the
		//      rust/cpp libraries have no such cap.
		//  (2) the counter×4.72 / stag 6→2..4 skew above IS agent sampling — fixed
		//      here by --min-sample-budget.
		//
		// Only --min-sample-budget is needed. The SampleBudget/shards term is capped
		// to MaxUncompressedBucketSize/2 (5 MB) BEFORE the max(), so even an
		// arbitrarily large --sample-budget yields ≤5 MB (minus budgetSum) — always
		// below a MinSampleBudget set above MaxUncompressedBucketSize (10 MB). The
		// cap is upstream of the min-floor, so --min-sample-budget is unbounded
		// there: 11 MB means sampler.Run() keeps every item with SF=1. --sample-budget
		// is therefore DEAD in this configuration (its term can never win the max)
		// and is omitted.
		// (The agg receive budget is already skipped twice over: historic writes
		// bypass it AND we pass --disable-receive-sample-budget.) Only LOOSENS
		// sampling → the small counter metrics stay green and the sampling-factor
		// tripwire stays 0.
		"--min-sample-budget=11000000",
		// Same 'kitten' setuid reason as the agg: the agent runs as root in the
		// container and would fatally fail to drop to the missing 'kitten' user.
		"-u", "root", "-g", "root",
	)
	if err := rt.Run(ctx, RunOpts{
		Name:    agentC,
		Image:   alpineBase,
		Network: o.network,
		Volumes: []string{
			filepath.Join(o.binDir, "statshouse") + ":/statshouse:ro",
			o.keyVol(),
		},
		Cmd:    agentCmd,
		Detach: true,
	}); err != nil {
		return ds, fmt.Errorf("start agent: %w", err)
	}
	if err := startServiceProbe(ctx, rt, rec, &ds.agent, "agent", agentC, o.network, agentPort, ""); err != nil {
		return ds, err
	}

	return ds, nil
}

// aggRunScript builds the aggregator container's /bin/sh entrypoint. metaIP is
// the metadata container's run-network IP. The script is extracted so its flags
// are unit-testable per backend without a container; the two backends differ
// only in the storage block:
//
//   - clickhouse: `--kh=<ch-ip>:8123` names the ClickHouse to write to.
//   - duck: no ClickHouse exists. `--storage-backend=duck` selects the embedded
//     DuckDB store (the binary mounted at /statshouse-agg is the duckdb-tagged
//     build), `--duck-store-dir` owns the store, and `--local-shard` names the
//     shard this single process is (the CH cluster autodetect the clickhouse
//     stack relies on has nothing to read under duck).
//
// The budget/sampling flags are shared verbatim: the duck write path rides the
// same insert-budget and receive-budget machinery, so the known e2e hazards
// (insert sampler, receive-budget warming) are neutralized identically.
func aggRunScript(o daemonStackOpts, metaIP string) string {
	metaAggAddr := net.JoinHostPort(metaIP, strconv.Itoa(metaPort))
	mkdirDirs := "/cache"
	var storageFlags string
	switch o.backend {
	case backendDuck:
		mkdirDirs += " " + duckStoreMount
		storageFlags = fmt.Sprintf(` \
  --storage-backend=duck \
  --duck-store-dir=%[1]s \
  --local-shard=1`, duckStoreMount)
	default:
		storageFlags = fmt.Sprintf(` \
  --kh=%[1]s:8123`, o.chIP)
	}
	return fmt.Sprintf(`set -e
mkdir -p %[4]s
AGG_IP=$(ip -4 -o addr show scope global 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -1)
[ -n "$AGG_IP" ] || { echo 'e2e: could not determine aggregator run-network IP' >&2; exit 1; }
exec /statshouse-agg \
  --agg-addr=0.0.0.0:%[1]d \
  --cluster=statlogs2 \
  --auto-create \
  --auto-create-default-namespace \
  --deny-old-agents=false \
  --metadata-addr=%[3]s \
  --cache-dir=/cache \
  --receive-budget-warming=0 \
  --disable-receive-sample-budget \
  --cluster-shards-addrs=${AGG_IP}:%[1]d,${AGG_IP}:%[1]d,${AGG_IP}:%[1]d \
  --insert-budget=100000000 \
  --min-insert-budget=100000000%[2]s \
  -u root -g root`,
		aggPort, storageFlags, metaAggAddr, mkdirDirs)
}

// apiDaemonFlags builds the api daemon's flag list (everything after the
// binary; joinSh turns it into the exec argv). metaIP and aggIP are the
// metadata/aggregator container IPs. The storage flags branch on the backend:
// under clickhouse the api reads ClickHouse directly (`--clickhouse-v2-addrs`),
// under duck it sends every query to the aggregator's RPC port
// (`--storage-backend=duck` + `--duck-shard-addrs`).
func apiDaemonFlags(o daemonStackOpts, metaIP, aggIP string) []string {
	flags := []string{
		"--local-mode",
		"--insecure-mode",
		"--listen-addr=0.0.0.0:" + strconv.Itoa(apiPort),
		"--listen-rpc-addr=0.0.0.0:" + strconv.Itoa(apiRPCPort),
		"--metadata-addr=" + net.JoinHostPort(metaIP, strconv.Itoa(metaPort)),
		"--available-shards=1",
		"--cache-dir=/cache",
		"--rpc-crypto-path=" + rpcKeyMount,
		// The api is built without the `embed` tag, so statshouseui.FS() is nil
		// and it loads index.html from --static-dir. The mount target is the
		// placeholder /static by default, or /ui when --with-ui built the npm UI;
		// either way index.html (a valid Go template) sits at its root.
		"--static-dir=" + o.staticMount,
	}
	if o.backend == backendDuck {
		flags = append(flags,
			"--storage-backend=duck",
			"--duck-shard-addrs="+net.JoinHostPort(aggIP, strconv.Itoa(aggPort)))
	} else {
		chV2 := strings.TrimSuffix(strings.Repeat(o.chIP+":9000,", 3), ",") // <ch-ip>:9000 three times (cluster config shape)
		flags = append(flags, "--clickhouse-v2-addrs="+chV2)
	}
	return flags
}

// startServiceProbe is the repeated tail of every daemon start: track the freshly-
// started container on the stack via slot, inspect its run-network IP, log it, and
// wait on the TCP readiness probe. The container is tracked (slot filled) BEFORE the
// inspect, so a mid-probe failure still tears down the already-running container —
// an untracked service is skipped by teardown and blocks NetworkRemove while it
// stays attached. label is the human-readable role for log lines and error context;
// port is the readiness-probe port. extra (the api's published-port spec) is
// appended to the IP log line.
func startServiceProbe(ctx context.Context, rt Runtime, rec *recorder, slot **service, label, container, network string, port int, extra string) error {
	*slot = &service{name: container}
	ip, err := rt.InspectIP(ctx, container, network)
	if err != nil {
		return fmt.Errorf("inspect %s IP: %w", label, err)
	}
	(*slot).ip = ip
	rec.logf("%s container=%s ip=%s%s", label, container, ip, extra)
	if err := waitTCP(ctx, rt, rec, label, container, ip, port); err != nil {
		return err
	}
	rec.logf("%s ready (tcp :%d)", label, port)
	return nil
}

// joinSh builds a /bin/sh -c argv that runs `prep` (e.g. "mkdir -p /cache") then
// execs the binary+flags passed as the remaining args. The flags are passed as
// separate argv elements after the "--" $0 placeholder, so `exec "$@"` runs them
// verbatim — no shell quoting of the (IP-laden) flags is needed.
func joinSh(prep string, bin string, flags ...string) []string {
	return append([]string{"/bin/sh", "-c", prep + `; exec "$@"`, "--", bin}, flags...)
}

// writeRPCKey writes a fresh 32-byte RPC crypto key to a host temp file and
// returns its path. It is mounted read-only into every daemon at rpcKeyMount so
// all four derive the same KeyID and their cross-container RPC handshakes
// succeed. localdebug avoids encryption by running every daemon on 127.0.0.1
// (sameMachine → encryption skipped); the container stack cannot, so a shared
// key is mandatory. 32 bytes satisfies MinCryptoKeyLen, and random bytes almost
// never begin with four zero bytes (the other rejection). The caller removes the
// file when done.
func writeRPCKey() (string, error) {
	key := make([]byte, 32)
	if _, err := crand.Read(key); err != nil {
		return "", fmt.Errorf("generate RPC crypto key: %w", err)
	}
	f, err := os.CreateTemp("", "statshouse-e2e-rpckey-*")
	if err != nil {
		return "", fmt.Errorf("create RPC crypto key file: %w", err)
	}
	name := f.Name()
	if _, err := f.Write(key); err != nil {
		f.Close()
		os.Remove(name) // don't leak the temp file on the error path
		return "", fmt.Errorf("write RPC crypto key: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(name) // don't leak the temp file on the error path
		return "", fmt.Errorf("close RPC crypto key file: %w", err)
	}
	return name, nil
}

// waitTCP polls a real TCP dial to addr (host→container IP; verified reachable
// on apple/container, docker-on-Linux, and the lima guest) until the port is
// accepting connections, surfacing the container logs on timeout. No fixed sleeps.
func waitTCP(ctx context.Context, rt Runtime, rec *recorder, label, container, ip string, port int) error {
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	const (
		timeout  = 3 * time.Minute
		interval = 2 * time.Second
	)
	var lastErr string
	if err := poll(ctx, timeout, interval, func() (bool, error) {
		c, derr := net.DialTimeout("tcp", addr, 2*time.Second)
		if derr == nil {
			_ = c.Close()
			return true, nil
		}
		lastErr = derr.Error()
		return false, nil
	}); err != nil {
		return fmt.Errorf("%s readiness (tcp %s) not reached within %s: %v\n%s",
			label, addr, timeout, err, diagnose(ctx, rt, container, lastErr))
	}
	return nil
}

// queryAPI polls GET /api/query on the api's host address until it answers HTTP
// 200 (empty data is fine). It proves the api serves end-to-end.
// apiAddr is the published host address ("127.0.0.1:10888") or, when the api is
// not published, the container IP:port.
func queryAPI(ctx context.Context, apiAddr string) (string, error) {
	now := time.Now()
	url := fmt.Sprintf("http://%s/api/query?s=%s&f=%d&t=%d&w=1&qw=count",
		apiAddr, queryMetric, now.Add(-5*time.Minute).Unix(), now.Unix())
	const timeout = 2 * time.Minute
	var (
		lastBody string
		lastCode int
	)
	if err := poll(ctx, timeout, 2*time.Second, func() (bool, error) {
		body, code, gerr := httpGet(ctx, url)
		if gerr != nil {
			lastBody = gerr.Error()
			return false, nil
		}
		lastCode, lastBody = code, body
		return code == http.StatusOK, nil
	}); err != nil {
		return "", fmt.Errorf("/api/query did not answer 200 within %s (last code=%d): %v\nlast body: %s",
			timeout, lastCode, err, truncate(lastBody, 1000))
	}
	return lastBody, nil
}

func httpGet(ctx context.Context, url string) (string, int, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, err
}

// waitAggConveyor gates "stack ready" on a REAL agent→agg→api round-trip, not
// just TCP dials. It polls /api/query for the agg's receive-delay builtin
// (__agg_bucket_receive_delay_sec — set every second per agent that delivered a
// bucket, and the api's OWN healthcheck metric, internal/api/handler.go) and
// waits for at least one recent non-zero point. The recurring flake this guards:
// the stack comes up with every TCP dial green, but the agent↔agg RPC channel is
// silently dead (no keep-alive, auto-create never fires, every client write then
// times out). A recent point here proves the agent is delivering buckets, the agg
// is inserting them, and the api reads them back — before any client starts. The
// ~24s historic conveyor plus agent cold-start skew is well inside the 90s budget.
// Reuses queryCounter/poll so it stays cheap.
func waitAggConveyor(ctx context.Context, apiAddr string) error {
	now := time.Now()
	qurl := fmt.Sprintf("http://%s/api/query?s=%s&f=%d&t=%d&w=1s&ac=1&qw=count",
		apiAddr, queryMetric, now.Add(-5*time.Minute).Unix(), now.Unix())
	// "Recent": a point in the last 3 minutes of the 5-minute query window — loose
	// enough to absorb clock skew between the host and the containers.
	cutoff := now.Add(-3 * time.Minute).Unix()
	const timeout = 90 * time.Second
	var lastErr string
	if err := poll(ctx, timeout, 3*time.Second, func() (bool, error) {
		resp, qerr := queryCounter(ctx, qurl)
		if qerr != nil {
			lastErr = qerr.Error()
			return false, nil
		}
		if hasRecentPoint(resp, cutoff) {
			return true, nil
		}
		lastErr = "no recent non-zero data point"
		return false, nil
	}); err != nil {
		return fmt.Errorf("agent↔agg round-trip: %s returned no recent point within %s — the agent→agg→api conveyor is likely down (TCP probes can be green while this channel is dead): %v\n%s",
			queryMetric, timeout, err, lastErr)
	}
	return nil
}

// hasRecentPoint reports whether resp carries at least one non-zero data point at
// a timestamp ≥ cutoff. The agg receive-delay metric's count is ≥1 whenever an
// agent delivered a bucket that second, so any non-zero recent point is proof the
// agent→agg→api conveyor is live. A null point unmarshals to 0.0, which is not a
// false positive (a real delivery's count is ≥1).
func hasRecentPoint(resp *apiSeriesResponse, cutoff int64) bool {
	for i := range resp.Data.Series.SeriesMeta {
		data := resp.Data.Series.SeriesData[i]
		for j, ts := range resp.Data.Series.Time {
			if j >= len(data) || ts < cutoff {
				continue
			}
			if data[j] != 0 {
				return true
			}
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
