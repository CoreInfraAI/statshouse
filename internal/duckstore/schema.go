// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"fmt"
	"strings"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

// SchemaVersion stamps the store file. A file stamped with another version is
// moved aside on open and a fresh one created: files are never upgraded in
// place and never read through a compatibility shim.
//
//	1: rows_1s/1m/1h tier tables in ClickHouse's statshouse_v6 column layout
const SchemaVersion = 1

// tier is one resolution table: rows are stored with time truncated to the
// tier, exactly like ClickHouse's statshouse_v6_1s/1m/1h.
type tier struct {
	table   string // physical table
	chTable string // the ClickHouse table the API's SQL names
	seconds int64
}

var tiers = []tier{
	{"rows_1s", data_model.LODTables[data_model.Version6][1], 1},
	{"rows_1m", data_model.LODTables[data_model.Version6][60], 60},
	{"rows_1h", data_model.LODTables[data_model.Version6][3600], 3600},
}

// The collapse aggregate of every non-key column, as ClickHouse's
// AggregatingMergeTree defines it for statshouse_v6. The aggregate-state
// columns hold ClickHouse's own state bytes, merged by the Go folds (fold.go)
// registered as UDFs.
var valueColumns = []struct{ name, collapse string }{
	{"count", "sum(count)"},
	{"max_count", "max(max_count)"},
	{"min", "min(min)"},
	{"max", "max(max)"},
	{"sum", "sum(sum)"},
	{"sumsquare", "sum(sumsquare)"},
	{"percentiles", udfMergePercentiles + "(list(percentiles))"},
	{"uniq_state", udfMergeUniq + "(list(uniq_state))"},
	{"min_host", udfMergeArgMin + "(list(min_host))"},
	{"max_host", udfMergeArgMax + "(list(max_host))"},
	{"max_count_host", udfMergeArgMax + "(list(max_count_host))"},
}

// Names of the aggregate-state folds (LIST(BLOB) -> BLOB).
const (
	udfMergePercentiles = "sh_merge_percentiles"
	udfMergeUniq        = "sh_merge_uniq"
	udfMergeArgMin      = "sh_merge_argmin"
	udfMergeArgMax      = "sh_merge_argmax"
)

// keyColumns are the columns rows collapse by.
var keyColumns = func() []string {
	cols := []string{"metric", "time"}
	for i := 0; i < format.MaxTags; i++ {
		cols = append(cols, fmt.Sprintf("tag%d", i), fmt.Sprintf("stag%d", i))
	}
	return cols
}()

func tierTableDDL(table string) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS " + table + " (metric INTEGER NOT NULL, time BIGINT NOT NULL")
	for i := 0; i < format.MaxTags; i++ {
		fmt.Fprintf(&b, ", tag%d INTEGER NOT NULL, stag%d VARCHAR NOT NULL", i, i)
	}
	for _, c := range valueColumns[:6] {
		b.WriteString(", " + c.name + " DOUBLE NOT NULL")
	}
	for _, c := range valueColumns[6:] {
		b.WriteString(", " + c.name + " BLOB NOT NULL")
	}
	b.WriteString(")")
	return b.String()
}

// collapseSQL folds the rows of the given buckets of one tier into one row per
// key, in place. It runs inside a transaction; rows appended concurrently are
// invisible to it and so survive untouched.
func collapseSQL(table string, buckets string) []string {
	sel := make([]string, 0, len(keyColumns)+len(valueColumns))
	sel = append(sel, keyColumns...)
	for _, c := range valueColumns {
		sel = append(sel, c.collapse+" AS "+c.name)
	}
	where := " WHERE time IN (" + buckets + ")"
	return []string{
		"CREATE OR REPLACE TEMP TABLE collapsed AS SELECT " + strings.Join(sel, ", ") + " FROM " + table + where + " GROUP BY ALL",
		"DELETE FROM " + table + where,
		"INSERT INTO " + table + " SELECT * FROM collapsed",
		"DROP TABLE collapsed",
	}
}

// compatSQL is the thin ClickHouse compatibility layer that lets the API send
// the SQL its ClickHouse query builder renders: views named after the
// ClickHouse tables (with the columns ClickHouse queries filter on but duck
// does not store, and _shard_num, the Distributed table's virtual column),
// and macros for the ClickHouse functions the builder calls. What cannot be
// expressed this way (parametric aggregates, INTERVAL arithmetic, string
// escaping) the builder renders differently for duck.
func compatSQL(shardNum int) []string {
	stmts := []string{
		"CREATE OR REPLACE MACRO toFloat64(x) AS CAST(x AS DOUBLE)",
		"CREATE OR REPLACE MACRO toInt64(x) AS CAST(x AS BIGINT)",
		"CREATE OR REPLACE MACRO match(s, re) AS regexp_matches(s, re)",
		"CREATE OR REPLACE MACRO uniqMergeState(x) AS " + udfMergeUniq + "(list(x))",
		"CREATE OR REPLACE MACRO argMinMergeState(x) AS " + udfMergeArgMin + "(list(x))",
		"CREATE OR REPLACE MACRO argMaxMergeState(x) AS " + udfMergeArgMax + "(list(x))",
	}
	for _, t := range tiers {
		sel := fmt.Sprintf("SELECT *, 0::UTINYINT AS index_type, 0 AS pre_tag, '' AS pre_stag, %d::UINTEGER AS _shard_num FROM %s", shardNum, t.table)
		for _, name := range []string{t.chTable, t.chTable + format.TableDistSuffix} {
			stmts = append(stmts, "CREATE OR REPLACE VIEW "+name+" AS "+sel)
		}
	}
	return stmts
}
