// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestConfigStorageBackend(t *testing.T) {
	parse := func(args ...string) (*Config, error) {
		cfg := DefaultConfig()
		f := flag.NewFlagSet("api", flag.ContinueOnError)
		cfg.Bind(f, cfg)
		require.NoError(t, f.Parse(args))
		return cfg, cfg.ValidateConfig()
	}
	cfg, err := parse()
	require.NoError(t, err)
	require.Equal(t, duckstore.BackendClickHouse, cfg.StorageBackend)

	_, err = parse("--storage-backend=duck")
	require.ErrorContains(t, err, "--duck-shard-addrs", "duck reads need the aggregators' addresses")

	// any API binary accepts duck: it only talks to the aggregators over RPC
	cfg, err = parse("--storage-backend=duck", "--duck-shard-addrs=agg1:13336,agg2:13336")
	require.NoError(t, err)
	require.Equal(t, []string{"agg1:13336", "agg2:13336"}, cfg.DuckShardAddrs)

	_, err = parse("--storage-backend=duck", "--duck-shard-addrs=agg1:13336,agg1:13336")
	require.ErrorContains(t, err, "twice")
}
