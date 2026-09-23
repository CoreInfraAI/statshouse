package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// containerRuntime shells out to apple/container (macOS), whose CLI version is
// pinned because it drifts per release.
type containerRuntime struct{ cliRuntime }

const pinnedContainerVersion = "1.2.0"

func newContainerRuntime() *containerRuntime {
	return &containerRuntime{cliRuntime{
		bin:             "container",
		networkLsArgs:   []string{"network", "ls", "--quiet"},
		containerLsArgs: []string{"ls", "--all", "--format", "json"},
		parseContainers: func(out string) ([]string, error) {
			var arr []struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(out), &arr); err != nil {
				return nil, fmt.Errorf("parse container ls json: %w", err)
			}
			ids := make([]string, 0, len(arr))
			for _, c := range arr {
				ids = append(ids, c.ID)
			}
			return ids, nil
		},
	}}
}

func (r *containerRuntime) HasNetworkEgress() bool { return false }

func (r *containerRuntime) EnsureSystem(ctx context.Context) error {
	out, err := runOK(ctx, "container", "system", "status")
	if err != nil {
		return r.startAndVerify(ctx)
	}
	if !statusRunning(out) {
		return r.startAndVerify(ctx)
	}
	return nil
}

func (r *containerRuntime) startAndVerify(ctx context.Context) error {
	if _, err := runOK(ctx, "container", "system", "start"); err != nil {
		return fmt.Errorf("container system start failed: %w", err)
	}
	// `start` returns before services are fully live; poll status briefly.
	for range 30 {
		out, err := runOK(ctx, "container", "system", "status")
		if err == nil && statusRunning(out) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("container services did not reach 'running' after `container system start`; run `container system status` manually")
}

func statusRunning(statusOut string) bool {
	for _, line := range strings.Split(statusOut, "\n") {
		fs := strings.Fields(line)
		if len(fs) >= 2 && fs[0] == "status" {
			return fs[1] == "running"
		}
	}
	return false
}

func (r *containerRuntime) CheckVersion(ctx context.Context) error {
	out, err := runOK(ctx, "container", "--version")
	if err != nil {
		return fmt.Errorf("cannot determine container CLI version: %w", err)
	}
	ver := parseContainerVersion(out)
	if ver == "" {
		return fmt.Errorf("could not parse container CLI version from: %q", strings.TrimSpace(out))
	}
	return checkContainerVersion(ver)
}

// checkContainerVersion is split out so the mismatch path is testable.
func checkContainerVersion(parsed string) error {
	if parsed != pinnedContainerVersion {
		return fmt.Errorf("apple/container CLI version %q does not match pinned %q — CLI drift can break the harness; update the pin or install %s",
			parsed, pinnedContainerVersion, pinnedContainerVersion)
	}
	return nil
}

var containerVersionRe = regexp.MustCompile(`version\s+(\d+\.\d+\.\d+)`)

func parseContainerVersion(s string) string {
	m := containerVersionRe.FindStringSubmatch(s)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

func (r *containerRuntime) InspectIP(ctx context.Context, id, network string) (string, error) {
	out, err := runOK(ctx, "container", "inspect", id)
	if err != nil {
		return "", err
	}
	var arr []struct {
		Status struct {
			Networks []struct {
				Network     string `json:"network"`
				IPv4Address string `json:"ipv4Address"`
			} `json:"networks"`
		} `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &arr); err != nil {
		return "", fmt.Errorf("parse container inspect json: %w", err)
	}
	if len(arr) == 0 {
		return "", fmt.Errorf("inspect returned no entries for %q", id)
	}
	for _, n := range arr[0].Status.Networks {
		if network == "" || n.Network == network {
			return stripCIDR(n.IPv4Address), nil
		}
	}
	return "", fmt.Errorf("no IPv4 for container %q on network %q", id, network)
}

func stripCIDR(s string) string {
	// "192.168.64.2/24" -> "192.168.64.2"
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i]
	}
	return strings.TrimSpace(s)
}
