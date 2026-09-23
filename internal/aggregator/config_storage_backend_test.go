// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestValidateConfigAggregatorStorageBackend(t *testing.T) {
	c := DefaultConfigAggregator()
	require.Equal(t, duckstore.BackendClickHouse, c.StorageBackend, "clickhouse must be the default backend")
	require.NoError(t, ValidateConfigAggregator(&c))

	c.StorageBackend = duckstore.BackendDuck
	err := ValidateConfigAggregator(&c)
	require.Error(t, err)
	if !duckstore.Available {
		require.Contains(t, err.Error(), duckstore.BuildTag, "an untagged binary must refuse the duck backend")
		return
	}
	require.Contains(t, err.Error(), "--duck-store-dir")
	c.DuckStoreDir = t.TempDir()
	require.NoError(t, ValidateConfigAggregator(&c))

	for flag, set := range map[string]func(*ConfigAggregator){
		"--migration":         func(c *ConfigAggregator) { c.RemoteInitial.MigrationTimeRange = "1-2" },
		"--local-shard":       func(c *ConfigAggregator) { c.LocalShard = 0 },
		"--duck-retention-":   func(c *ConfigAggregator) { c.DuckRetention1m = -time.Second },
		"--duck-retention-1s": func(c *ConfigAggregator) { c.DuckRetention1s = time.Hour },
	} {
		bad := c
		set(&bad)
		err := ValidateConfigAggregator(&bad)
		require.Error(t, err)
		require.Contains(t, err.Error(), flag)
	}
}
