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
	// alpineBase is the image each static daemon binary is bind-mounted into
	// (no image builds). Pinned to a minor tag so reruns get the same userland.
	alpineBase = "alpine:3.24"

	metaPort   = 2442  // metadata RPC
	aggPort    = 13336 // aggregator receive
	apiPort    = 10888 // api HTTP (the only published port by default)
	apiRPCPort = 10889 // api RPC
	agentPort  = 13337 // agent client: raw UDP + RPC TCP

	// duckStoreMount is the aggregator's duck-store dir, in the container's
	// writable layer: the store lives and dies with the run.
	duckStoreMount = "/store"

	// rpcKeyMount is where the shared RPC crypto key is mounted in every daemon
	// (the agg/agent default path; metadata/api get it via --rpc-crypto-path).
	// Cross-container links require encryption, so all four need the SAME key.
	rpcKeyMount = "/etc/engine/pass"

	// apiStaticMount holds the placeholder index.html (e2e/api-static) for
	// UI-less runs; the api refuses to start without one.
	apiStaticMount = "/static"

	// apiUIMount holds the built npm UI when --with-ui is set.
	apiUIMount = "/ui"

	// queryMetric is a builtin the api resolves without a metadata mapping, so
	// /api/query answers 200 on a fresh stack.
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

// containerNames returns the names of the started services (a partial start
// leaves nils), for teardown and log capture.
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
	chIP         string // ClickHouse IP on the run network; "" under duck
	binDir       string // host dir with the compiled daemon binaries
	runID        string
	apiPublish   string // host address to publish the api on; "" publishes nothing
	rpcKeyPath   string // host path to the shared RPC crypto key
	apiStaticDir string // host dir with index.html
	staticMount  string // apiStaticMount or apiUIMount
	backend      storageBackend
	stackTag     string   // optional container-name infix so two stacks share a network
	sharedMeta   *service // non-nil: reuse this metadata instead of starting one
}

// cname renders a container name: e2e-<runid>-[<stackTag>-]<role>. The
// e2ePrefix+runID prefix is what pruneStale, teardown and log capture match on.
func (o daemonStackOpts) cname(role string) string {
	if o.stackTag != "" {
		role = o.stackTag + "-" + role
	}
	return e2ePrefix + o.runID + "-" + role
}

func (o daemonStackOpts) keyVol() string { return o.rpcKeyPath + ":" + rpcKeyMount + ":ro" }

// startDaemonStack brings up metadata → agg → api + agent on the run network,
// wired by inspected IP (apple/container in-container DNS does not resolve
// names), waiting on each one's TCP readiness probe. Each binary is
// bind-mounted into alpineBase and exec'd from /bin/sh so it becomes PID 1.
func startDaemonStack(ctx context.Context, rt Runtime, rec *recorder, o daemonStackOpts) (*daemonStack, error) {
	ds := &daemonStack{}

	// --- metadata ---
	// Conformance's second stack reuses the first stack's metadata.
	if o.sharedMeta != nil {
		ds.metadata = o.sharedMeta
		rec.logf("reusing shared metadata %s at %s", ds.metadata.name, ds.metadata.ip)
	} else {
		// --create-binlog initializes the binlog and exits; the server then starts
		// in the same container.
		metaC := o.cname("metadata")
		// Only the server is exec'd: an exiting init step as PID 1 would stop the
		// container.
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

		if err := startServiceProbe(ctx, rt, rec, &ds.metadata, "metadata", metaC, o.network, metaPort, ""); err != nil {
			return ds, err
		}
	}

	// --- aggregator ---
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
	// Under duck the agg IS the storage: not ready until it answers a real
	// store query.
	if o.backend == backendDuck {
		if err := waitStoreQueryReady(ctx, rt, aggC, net.JoinHostPort(ds.agg.ip, strconv.Itoa(aggPort)), o.rpcKeyPath); err != nil {
			return ds, err
		}
		rec.logf("agg store-query rpc ready (real storeQuery round-trip on :%d)", aggPort)
	}

	// --- api ---
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
		// Disable agent sampling so exact per-bucket assertions hold. The
		// per-shard budget is max(MinSampleBudget, min(SampleBudget/shards,
		// MaxUncompressedBucketSize/2) − budgetSum), and the builtin metrics'
		// budgetSum eats the second term, leaving new metrics at the 2000-byte
		// floor. A floor above MaxUncompressedBucketSize keeps every item
		// (--sample-budget can never win the max, so it is omitted).
		"--min-sample-budget=11000000",
		// alpine has no 'kitten' user; dropping privileges to it would be fatal.
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

// aggRunScript builds the aggregator's /bin/sh entrypoint. The backends differ
// only in storage flags; duck needs --local-shard since there is no CH cluster
// to autodetect the shard from.
//
// Non-obvious flags:
//   - --receive-budget-warming=0: the default 15m ramp starves receive budgets
//     and agents sample even tiny payloads.
//   - --cluster-shards-addrs: otherwise the agg advertises the CH cluster's
//     host_name ("localhost"), which agents cannot dial cross-container. The
//     entrypoint discovers the agg's run-network IP at startup.
//   - -u root -g root: alpine has no 'kitten' user to drop privileges to.
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

// apiDaemonFlags builds the api's flags. Under duck the api queries the
// aggregator's RPC port instead of ClickHouse.
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
		// Built without the `embed` tag, the api loads index.html from here.
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

// startServiceProbe records a started container in slot, inspects its IP and
// waits for its TCP port. slot is filled BEFORE the inspect so a failure still
// tears the container down (an untracked one would block NetworkRemove).
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

// joinSh builds a /bin/sh -c argv that runs prep then execs bin with flags,
// passed as separate argv elements so they need no shell quoting.
func joinSh(prep string, bin string, flags ...string) []string {
	return append([]string{"/bin/sh", "-c", prep + `; exec "$@"`, "--", bin}, flags...)
}

// writeRPCKey writes a fresh 32-byte RPC crypto key (see rpcKeyMount) to a
// host temp file and returns its path; the caller removes it.
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
		os.Remove(name)
		return "", fmt.Errorf("write RPC crypto key: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", fmt.Errorf("close RPC crypto key file: %w", err)
	}
	return name, nil
}

// waitTCP polls a TCP dial to the container IP until the port accepts,
// surfacing the container logs on timeout.
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

// queryAPI polls GET /api/query at apiAddr until it answers 200 (empty data is
// fine).
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

// waitAggConveyor gates "stack ready" on a real agent→agg→api round-trip: a
// recent non-zero point of the agg's receive-delay builtin (written each second
// per agent that delivered a bucket). It catches a silently dead agent↔agg RPC
// channel that TCP probes miss.
func waitAggConveyor(ctx context.Context, apiAddr string) error {
	now := time.Now()
	qurl := fmt.Sprintf("http://%s/api/query?s=%s&f=%d&t=%d&w=1s&ac=1&qw=count",
		apiAddr, queryMetric, now.Add(-5*time.Minute).Unix(), now.Unix())
	// Loose enough to absorb host/container clock skew.
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

// hasRecentPoint reports whether resp has a non-zero point at ts ≥ cutoff (a
// null point decodes to 0, so it never counts).
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
