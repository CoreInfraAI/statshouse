package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// Runtime abstracts the container CLI the harness shells out to:
// apple/container (default on macOS; it has no stable Go API) or docker.
type Runtime interface {
	Name() string

	// HasNetworkEgress reports whether containers can reach the public network
	// (docker yes, apple/container no); --with-ui picks its npm install mode by it.
	HasNetworkEgress() bool

	// EnsureSystem makes the runtime ready, auto-starting apple/container's
	// system services if needed.
	EnsureSystem(ctx context.Context) error

	// CheckVersion enforces the pinned apple/container CLI version; no-op on docker.
	CheckVersion(ctx context.Context) error

	NetworkCreate(ctx context.Context, name string) error
	NetworkRemove(ctx context.Context, name string) error
	NetworkList(ctx context.Context) ([]string, error)

	Run(ctx context.Context, opts RunOpts) error

	// Exec returns stdout and the exit code; err only when the command cannot launch.
	Exec(ctx context.Context, containerID string, cmd []string) (stdout string, exitCode int, err error)

	Logs(ctx context.Context, containerID string) (string, error)

	Stop(ctx context.Context, containerID string) error

	Rm(ctx context.Context, containerID string, force bool) error

	// ContainerList returns all containers, running or not.
	ContainerList(ctx context.Context) ([]string, error)

	// InspectIP returns the container's IPv4 on network, without the CIDR
	// prefix. Wiring is by IP: apple/container DNS does not resolve names.
	InspectIP(ctx context.Context, containerID, network string) (string, error)
}

// RunOpts configures Runtime.Run. Volumes and Ports use docker-style syntax,
// which both CLIs accept.
type RunOpts struct {
	Name    string
	Image   string
	Network string
	Env     []string // KEY=VAL
	Volumes []string // "src:dst[:ro]"
	Ports   []string // "[host-ip:]host:container[/proto]"
	Cmd     []string
	Detach  bool
	AutoRm  bool
}

// buildRunArgs renders RunOpts as `run` args; both CLIs accept the same flags.
func buildRunArgs(opts RunOpts) []string {
	args := []string{"run"}
	if opts.Detach {
		args = append(args, "-d")
	}
	if opts.AutoRm {
		args = append(args, "--rm")
	}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}
	if opts.Network != "" {
		args = append(args, "--network", opts.Network)
	}
	for _, e := range opts.Env {
		args = append(args, "-e", e)
	}
	for _, v := range opts.Volumes {
		args = append(args, "-v", v)
	}
	for _, p := range opts.Ports {
		args = append(args, "-p", p)
	}
	args = append(args, opts.Image)
	args = append(args, opts.Cmd...)
	return args
}

// selectRuntime returns the --runtime choice, else auto-detects by GOOS and PATH.
func selectRuntime(flag string) (Runtime, error) {
	name := flag
	if name == "" {
		name = autoDetectRuntime()
	}
	switch name {
	case "container":
		return &containerRuntime{}, nil
	case "docker":
		return &dockerRuntime{}, nil
	default:
		if flag != "" {
			return nil, fmt.Errorf("unknown runtime %q (want \"container\" or \"docker\")", flag)
		}
		return nil, fmt.Errorf("could not auto-detect a container runtime: install apple/container or docker, or pass --runtime=container|docker")
	}
}

func autoDetectRuntime() string {
	switch runtime.GOOS {
	case "darwin":
		if lookPath("container") {
			return "container"
		}
		if lookPath("docker") {
			return "docker"
		}
	case "linux":
		if lookPath("docker") {
			return "docker"
		}
		if lookPath("container") {
			return "container"
		}
	default:
		if lookPath("docker") {
			return "docker"
		}
		if lookPath("container") {
			return "container"
		}
	}
	return ""
}

func lookPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// resourceInList makes Rm/NetworkRemove idempotent without matching CLI error
// wording, which drifts between releases. A list error is returned, not treated
// as absent, so a resource is never silently leaked.
func resourceInList(ctx context.Context, list func(context.Context) ([]string, error), name string) (bool, error) {
	items, err := list(ctx)
	if err != nil {
		return false, err
	}
	for _, n := range items {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// execResult captures a finished command's output; run reports a non-zero exit
// via exitCode, not an error.
type execResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func run(ctx context.Context, name string, args ...string) (execResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	res := execResult{stdout: out.String(), stderr: errb.String()}
	if err != nil {
		// Otherwise a cancelled ctx surfaces as a confusing "signal: killed".
		if ctx.Err() != nil {
			return res, fmt.Errorf("%s: %w", cmdStr(name, args), ctx.Err())
		}
		if ee, ok := err.(*exec.ExitError); ok {
			res.exitCode = ee.ExitCode()
			return res, nil
		}
		return res, fmt.Errorf("%s: %w", cmdStr(name, args), err)
	}
	return res, nil
}

// runOK runs a command and returns its stdout, treating a non-zero exit as an error.
func runOK(ctx context.Context, name string, args ...string) (string, error) {
	res, err := run(ctx, name, args...)
	if err != nil {
		return res.stdout, err
	}
	if res.exitCode != 0 {
		return res.stdout, fmt.Errorf("%s failed (exit %d)\n%s", cmdStr(name, args), res.exitCode, indent(res.stderr))
	}
	return res.stdout, nil
}

func cmdStr(name string, args []string) string {
	return name + " " + strings.Join(args, " ")
}

func indent(s string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = "    " + lines[i]
	}
	return strings.Join(lines, "\n")
}
