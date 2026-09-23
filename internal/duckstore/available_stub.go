// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !duckdb

package duckstore

import (
	"context"
	"fmt"
)

// Available reports whether this binary embeds DuckDB (the "duckdb" build tag).
const Available = false

// Store is a placeholder in binaries built without DuckDB; Open always fails.
type Store struct{}

func Open(Config) (*Store, error) {
	return nil, fmt.Errorf("duck-store: this binary was built without the %q build tag", BuildTag)
}

func (*Store) Insert(context.Context, []byte) error                 { return nil }
func (*Store) Query(context.Context, string) (int, [][]byte, error) { return 0, nil, nil }
func (*Store) Close() error                                         { return nil }
