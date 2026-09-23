package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

// The rust-client path. The statshouse crate is a zero-dependency single file,
// so it is built with plain `rustc --crate-type rlib` and the driver with
// `rustc --extern`: offline cargo fails because the workspace's xtask member
// pulls crates.io deps the container cannot fetch.

const (
	// rustBaseImage is pinned to an exact minor so reruns get the same toolchain.
	rustBaseImage = "rust:1.83-bookworm"

	rustClientName  = "statshouse-rs"
	driverRustDir   = "drivers/rust"
	rustLibRel      = "statshouse/src/lib.rs"
	rustTargetMount = "/target" // host-mounted rlib cache
)

// rustByteStringLit renders the body of a Rust byte-string literal. Byte
// strings cannot hold raw non-ASCII bytes, so those become \xHH escapes.
func rustByteStringLit(s string) string {
	return escapeLitBody(s, `\x%02x`)
}

func rustFloatLit(f float64) string {
	return floatLit(f)
}

func rustDriverFuncs() template.FuncMap {
	return template.FuncMap{
		"rustBytes": rustByteStringLit,
		"rustFloat": rustFloatLit,
	}
}

func renderRustDriver(tmplPath string, stream metricStream, outDir string) error {
	return renderDriver(tmplPath, "rust-driver", rustDriverFuncs(), stream, outDir, "main.rs")
}

// buildAndRunRustClient clones, renders, builds offline and runs the rust
// driver. A non-zero driver exit is reported via the exit code, not err.
func buildAndRunRustClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached binary.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, rustBaseImage)
	}

	clonePath, err := o.spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverRustDir, "main.rs.tmpl")
	if err := renderRustDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered rust driver: %s (%d writes)", filepath.Join(o.workDir, "main.rs"), len(o.stream.Writes))

	// Bind-mount sources must exist before the run.
	targetCache := filepath.Join(o.cache, "rust-target")
	for _, d := range []string{targetCache, o.buildCache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return 0, "", fmt.Errorf("create cache %s: %w", d, err)
		}
	}

	libPath := clientMount + "/" + rustLibRel
	rlibPath := rustTargetMount + "/libstatshouse.rlib"
	driverPath := driverBinMount + "/" + driverBinName
	// Rebuild the rlib only when the pinned source is newer than the cached copy.
	buildRlib := fmt.Sprintf(
		`if [ ! -f %[1]s ] || [ %[2]s -nt %[1]s ]; then rustc --edition 2021 --crate-type rlib --crate-name statshouse %[2]s -o %[1]s && echo "e2e: built statshouse rlib"; else echo "e2e: reused cached statshouse rlib"; fi`,
		rlibPath, libPath,
	)
	buildRun := "set -e; " + buildRlib +
		`; rustc --edition 2021 --extern statshouse="` + rlibPath + `" ` + workMount + `/main.rs -o ` + driverPath +
		`; ` + driverPath

	opts := RunOpts{
		Name:    o.container,
		Image:   rustBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.workDir + ":" + workMount,
			clonePath + ":" + clientMount + ":ro",
			targetCache + ":" + rustTargetMount,
			o.buildCache + ":" + driverBinMount,
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true,
	}
	rec.logf("rust client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}
