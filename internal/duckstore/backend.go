// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Package duckstore implements the DuckDB storage backend for StatsHouse: one
// DuckDB file embedded in each aggregator shard, written by the aggregator's
// insert conveyor and read by the API over the statshouse.storeQuery RPC.
// Everything that touches the DuckDB driver sits behind the "duckdb" build
// tag, so binaries built without it stay pure Go.
package duckstore

import (
	"fmt"
	"runtime"
	"time"

	"github.com/VKCOM/statshouse/internal/format"
)

// BuildTag is the Go build tag that compiles DuckDB support into a binary.
const BuildTag = "duckdb"

// Default retention per tier, mirroring ClickHouse's TTLs so switching
// backends does not change how long data lives. Zero keeps the tier forever.
const (
	DefaultRetention1s = 52 * time.Hour
	DefaultRetention1m = 33 * 24 * time.Hour

	DefaultRetention1h time.Duration = 0
)

// DefaultMemoryLimitBytes is DuckDB's memory_limit; the spill-to-disk bound
// follows it. Defaults target the smallest viable node, not the available
// envelope: an embedded DuckDB must not grow into the RAM the aggregator's own
// conveyor needs.
const DefaultMemoryLimitBytes int64 = 256 << 20

// DefaultQueryConcurrency is how many store queries execute at once.
var DefaultQueryConcurrency = max(2, runtime.GOMAXPROCS(0))

// StorageBackend selects which storage backend metric data is written to and
// read from. Parsed from --storage-backend by the aggregator and the API.
type StorageBackend int8

const (
	// BackendClickHouse is the default: all writes and reads go to the
	// ClickHouse cluster.
	BackendClickHouse StorageBackend = iota
	// BackendDuck stores metric data in the duck-store owned by each
	// aggregator shard.
	BackendDuck
)

// ParseStorageBackend parses a --storage-backend flag value. The empty string
// selects ClickHouse, the historical default.
func ParseStorageBackend(s string) (StorageBackend, error) {
	switch s {
	case "", "clickhouse":
		return BackendClickHouse, nil
	case "duck":
		return BackendDuck, nil
	default:
		return BackendClickHouse, fmt.Errorf("invalid --storage-backend value %q, must be %q or %q", s, "clickhouse", "duck")
	}
}

// String implements flag.Value.
func (b StorageBackend) String() string {
	if b == BackendDuck {
		return "duck"
	}
	return "clickhouse"
}

// Set implements flag.Value.
func (b *StorageBackend) Set(s string) error {
	v, err := ParseStorageBackend(s)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// Validate reports whether this binary can run backend b as the storage
// owner: a binary built without the "duckdb" build tag must refuse
// --storage-backend=duck at startup. The API, which only talks to the
// aggregators over RPC, must not gate on this.
func (b StorageBackend) Validate() error {
	if b == BackendDuck && !Available {
		return fmt.Errorf("--storage-backend=duck is not supported by this binary: it was built without the %q build tag that embeds DuckDB", BuildTag)
	}
	return nil
}

// Config configures a Store.
type Config struct {
	Dir      string
	ShardNum int // 1-based, what queries see as _shard_num
	// Per-tier retention; zero keeps the tier forever.
	Retention1s, Retention1m, Retention1h time.Duration
	MemoryLimitBytes                      int64 // DuckDB memory_limit; 0 means DefaultMemoryLimitBytes
	QueryConcurrency                      int   // queries executing at once; 0 means DefaultQueryConcurrency
	Metrics                               MetricsRecorder
	Logf                                  func(format string, args ...any)
}

// MetricsRecorder receives the store's builtin __duck_store_* metrics; the
// aggregator's built-in agent implements it.
type MetricsRecorder interface {
	AddValueCounter(t uint32, metricInfo *format.MetricMetaValue, tags []int32, value float64, counter float64)
}
