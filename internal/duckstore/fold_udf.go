// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

// foldUDF exposes one Go fold (fold.go) to SQL as LIST(BLOB) -> BLOB.
type foldUDF struct {
	name string
	fold func([][]byte) ([]byte, error)
}

func (f foldUDF) Config() duckdb.ScalarFuncConfig {
	blob, _ := duckdb.NewTypeInfo(duckdb.TYPE_BLOB) // cannot fail for a primitive type
	list, _ := duckdb.NewListInfo(blob)
	return duckdb.ScalarFuncConfig{InputTypeInfos: []duckdb.TypeInfo{list}, ResultTypeInfo: blob}
}

func (f foldUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: func(values []driver.Value) (any, error) {
		list, _ := values[0].([]any)
		blobs := make([][]byte, len(list))
		for i, v := range list {
			b, ok := v.([]byte)
			if !ok {
				return nil, fmt.Errorf("duck-store: %s: list element %d is %T, want BLOB", f.name, i, v)
			}
			blobs[i] = b
		}
		out, err := f.fold(blobs)
		if err != nil {
			return nil, fmt.Errorf("duck-store: %s: %w", f.name, err)
		}
		return out, nil
	}}
}

// registerFolds registers the folds with the database conn belongs to; every
// connection of that database sees them.
func registerFolds(conn *sql.Conn) error {
	for _, f := range []foldUDF{
		{udfMergePercentiles, foldPercentiles},
		{udfMergeUniq, foldUniques},
		{udfMergeArgMin, foldArgMin},
		{udfMergeArgMax, foldArgMax},
	} {
		if err := duckdb.RegisterScalarUDF(conn, f.name, f); err != nil {
			return fmt.Errorf("duck-store: register %s: %w", f.name, err)
		}
	}
	return nil
}
