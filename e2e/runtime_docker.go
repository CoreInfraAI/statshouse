package main

import (
	"context"
	"encoding/json"
	"fmt"
)

// dockerRuntime shells out to docker (default on Linux).
type dockerRuntime struct{ cliRuntime }

func newDockerRuntime() *dockerRuntime {
	return &dockerRuntime{cliRuntime{
		bin:             "docker",
		networkLsArgs:   []string{"network", "ls", "--format", "{{.Name}}"},
		containerLsArgs: []string{"ps", "--all", "--format", "{{.Names}}"},
		parseContainers: func(out string) ([]string, error) { return splitNonEmpty(out), nil },
	}}
}

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
