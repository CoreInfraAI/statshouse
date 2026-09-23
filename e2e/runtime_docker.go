package main

import (
	"context"
	"encoding/json"
	"fmt"
)

// dockerRuntime shells out to docker (default on Linux).
type dockerRuntime struct{}

func (r *dockerRuntime) Name() string { return "docker" }

func (r *dockerRuntime) HasNetworkEgress() bool { return true }

func (r *dockerRuntime) EnsureSystem(ctx context.Context) error {
	if _, err := runOK(ctx, "docker", "info"); err != nil {
		return fmt.Errorf("docker daemon not reachable (`docker info` failed): %w", err)
	}
	return nil
}

func (r *dockerRuntime) CheckVersion(_ context.Context) error {
	return nil
}

func (r *dockerRuntime) NetworkCreate(ctx context.Context, name string) error {
	_, err := runOK(ctx, "docker", "network", "create", name)
	return err
}

func (r *dockerRuntime) NetworkRemove(ctx context.Context, name string) error {
	present, err := resourceInList(ctx, r.NetworkList, name)
	if err != nil {
		return err
	}
	if !present {
		return nil // already gone — idempotent
	}
	_, err = runOK(ctx, "docker", "network", "rm", name)
	return err
}

func (r *dockerRuntime) NetworkList(ctx context.Context) ([]string, error) {
	out, err := runOK(ctx, "docker", "network", "ls", "--format", "{{.Name}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmpty(out), nil
}

func (r *dockerRuntime) ContainerList(ctx context.Context) ([]string, error) {
	out, err := runOK(ctx, "docker", "ps", "--all", "--format", "{{.Names}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmpty(out), nil
}

func (r *dockerRuntime) Run(ctx context.Context, opts RunOpts) error {
	_, err := runOK(ctx, "docker", buildRunArgs(opts)...)
	return err
}

func (r *dockerRuntime) Exec(ctx context.Context, id string, cmd []string) (string, int, error) {
	args := append([]string{"exec", id}, cmd...)
	res, err := run(ctx, "docker", args...)
	// clickhouse-client writes errors to stderr; keep them in probe output.
	return res.stdout + res.stderr, res.exitCode, err
}

func (r *dockerRuntime) Logs(ctx context.Context, id string) (string, error) {
	out, err := runOK(ctx, "docker", "logs", id)
	return out, err
}

func (r *dockerRuntime) Stop(ctx context.Context, id string) error {
	_, err := runOK(ctx, "docker", "stop", id)
	return err
}

func (r *dockerRuntime) Rm(ctx context.Context, id string, force bool) error {
	present, err := resourceInList(ctx, r.ContainerList, id)
	if err != nil {
		return err
	}
	if !present {
		return nil // already gone — idempotent
	}
	args := []string{"rm"}
	if force {
		args = append(args, "-f")
	}
	args = append(args, id)
	_, err = runOK(ctx, "docker", args...)
	return err
}

func (r *dockerRuntime) InspectIP(ctx context.Context, id, network string) (string, error) {
	out, err := runOK(ctx, "docker", "inspect", id)
	if err != nil {
		return "", err
	}
	var arr []struct {
		NetworkSettings struct {
			Networks map[string]struct {
				IPAddress string `json:"IPAddress"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal([]byte(out), &arr); err != nil {
		return "", fmt.Errorf("parse docker inspect json: %w", err)
	}
	if len(arr) == 0 {
		return "", fmt.Errorf("inspect returned no entries for %q", id)
	}
	nets := arr[0].NetworkSettings.Networks
	if network != "" {
		// Never fall back to another network's IP: that would silently miswire.
		n, ok := nets[network]
		if !ok {
			return "", fmt.Errorf("container %q is not attached to network %q", id, network)
		}
		if n.IPAddress == "" {
			return "", fmt.Errorf("no IPv4 for container %q on network %q", id, network)
		}
		return n.IPAddress, nil
	}
	for _, n := range nets { // no network specified: fall back to the first available address
		if n.IPAddress != "" {
			return n.IPAddress, nil
		}
	}
	return "", fmt.Errorf("no IPv4 for container %q on any network", id)
}
