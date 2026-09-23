// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"flag"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseStorageBackend(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    StorageBackend
		wantErr bool
	}{
		{name: "empty means clickhouse", in: "", want: BackendClickHouse},
		{name: "clickhouse", in: "clickhouse", want: BackendClickHouse},
		{name: "duck", in: "duck", want: BackendDuck},
		{name: "case sensitive clickhouse", in: "ClickHouse", wantErr: true},
		{name: "case sensitive duck", in: "Duck", wantErr: true},
		{name: "build tag value is not a backend value", in: "duckdb", wantErr: true},
		{name: "unknown backend", in: "sqlite", wantErr: true},
		{name: "legacy alias", in: "ch", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseStorageBackend(tt.in)
			if tt.wantErr {
				require.Error(t, err, "ParseStorageBackend(%q)", tt.in)
				require.Contains(t, err.Error(), "--storage-backend", "error must name the flag for flag.Value users")
				return
			}
			require.NoError(t, err, "ParseStorageBackend(%q)", tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStorageBackendString(t *testing.T) {
	require.Equal(t, "clickhouse", BackendClickHouse.String())
	require.Equal(t, "duck", BackendDuck.String())
	// the zero value must be the historical default so a fresh config parses
	require.Equal(t, BackendClickHouse, StorageBackend(0))
}

func TestStorageBackendFlagValue(t *testing.T) {
	var b StorageBackend
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&b, "storage-backend", "usage")
	require.NoError(t, f.Parse([]string{"--storage-backend", "duck"}))
	require.Equal(t, BackendDuck, b)
	require.Equal(t, "duck", b.String())

	// omitted flag leaves the zero value in place
	var b2 StorageBackend
	f2 := flag.NewFlagSet("test", flag.ContinueOnError)
	f2.SetOutput(io.Discard)
	f2.Var(&b2, "storage-backend", "usage")
	require.NoError(t, f2.Parse(nil))
	require.Equal(t, BackendClickHouse, b2)

	// invalid value is rejected by the flag package
	var b3 StorageBackend
	f3 := flag.NewFlagSet("test", flag.ContinueOnError)
	f3.SetOutput(io.Discard)
	f3.Var(&b3, "storage-backend", "usage")
	require.Error(t, f3.Parse([]string{"--storage-backend", "postgres"}))
	require.Equal(t, BackendClickHouse, b3)
}

func TestStorageBackendValidate(t *testing.T) {
	require.NoError(t, BackendClickHouse.Validate(), "clickhouse must validate in any binary")

	if Available {
		t.Skip("built with the duckdb tag; the untagged rejection is covered by available_duckdb_test.go")
	}
	err := BackendDuck.Validate()
	require.Error(t, err, "an untagged binary must refuse the duck backend at startup")
	// the message must name the flag and the build tag, so the operator knows
	// both what to change and how to get a binary that supports it
	require.Contains(t, err.Error(), "--storage-backend=duck")
	require.Contains(t, err.Error(), `"`+BuildTag+`"`)
}

func TestDefaultRetentionMirrorsClickHouseTTLs(t *testing.T) {
	require.Equal(t, 52*time.Hour, DefaultRetention1s)
	require.Equal(t, 33*24*time.Hour, DefaultRetention1m)
	require.Equal(t, time.Duration(0), DefaultRetention1h, "the 1h tier is unbounded by default")
}
