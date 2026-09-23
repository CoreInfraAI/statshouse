package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

// The cpp-client path. The client is header-only, so the driver is a single
// translation unit compiled with `g++ -std=c++17 -pthread -I`; nothing is cached.

const (
	cppBaseImage = "gcc:13-bookworm"

	cppClientName = "statshouse-cpp"
	driverCppDir  = "drivers/cpp"
)

// cStringLit renders the body of a C/C++ string literal. Non-printable bytes
// become fixed-width octal \NNN escapes; \x is avoided because it is greedy
// over following hex digits. '?' is safe: C++17 removed trigraphs.
func cStringLit(s string) string {
	return escapeLitBody(s, `\%03o`)
}

func cFloatLit(f float64) string {
	return floatLit(f)
}

func cppDriverFuncs() template.FuncMap {
	return template.FuncMap{
		"cString": cStringLit,
		"cFloat":  cFloatLit,
	}
}

func renderCppDriver(tmplPath string, stream metricStream, outDir string) error {
	return renderDriver(tmplPath, "cpp-driver", cppDriverFuncs(), stream, outDir, "main.cpp")
}

// buildAndRunCppClient clones, renders, builds offline and runs the cpp
// driver. A non-zero driver exit is reported via the exit code, not err.
func buildAndRunCppClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached binary.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, cppBaseImage)
	}

	clonePath, err := o.spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverCppDir, "main.cpp.tmpl")
	if err := renderCppDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered cpp driver: %s (%d writes)", filepath.Join(o.workDir, "main.cpp"), len(o.stream.Writes))

	// Bind-mount sources must exist before the run.
	if err := os.MkdirAll(o.buildCache, 0o755); err != nil {
		return 0, "", fmt.Errorf("create build cache %s: %w", o.buildCache, err)
	}

	driverPath := driverBinMount + "/" + driverBinName
	// -pthread: TransportTCP spawns worker threads.
	buildRun := "set -e; g++ --version | head -1" +
		`; g++ -std=c++17 -pthread -I ` + clientMount + " " + workMount + `/main.cpp -o ` + driverPath +
		`; ` + driverPath

	opts := RunOpts{
		Name:    o.container,
		Image:   cppBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.workDir + ":" + workMount,
			clonePath + ":" + clientMount + ":ro",
			o.buildCache + ":" + driverBinMount,
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true,
	}
	rec.logf("cpp client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}
