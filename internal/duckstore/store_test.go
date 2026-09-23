// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/chutil"
)

func openTestStore(t *testing.T, dir string, cfg Config) *Store {
	cfg.Dir, cfg.ShardNum = dir, 3
	s, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (s *Store) countRows(t *testing.T, table string) (n int) {
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM "+table).Scan(&n))
	return n
}

func (s *Store) runPass(t *testing.T, fn func(context.Context, *sql.Conn) error) {
	conn, err := s.db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, fn(context.Background(), conn))
}

// series is what the API's ClickHouse builder would select from the 1m tier,
// decoded with the API's own ClickHouse column types.
type series struct {
	time     proto.ColInt64
	tag1     proto.ColInt32
	stag2    proto.ColStr
	shard    proto.ColUInt32
	count    proto.ColFloat64
	min      proto.ColFloat64
	pct      chutil.ColTDigest
	uniq     chutil.ColUnique
	minHost  chutil.ColArgMinStringFloat32
	maxHost  chutil.ColArgMaxStringFloat32
	numRows  int
	colOrder []proto.ColResult
}

func querySeries(t *testing.T, s *Store, from, to int64) *series {
	sql := fmt.Sprintf("SELECT toInt64(time) AS _time,tag1,stag2,_shard_num,toFloat64(sum(count)) AS _val0,toFloat64(min(min)) AS _val1,"+
		"sh_merge_percentiles(list(percentiles)) AS _val2,uniqMergeState(uniq_state) AS _val3,argMinMergeState(min_host) AS _minHost,"+
		"argMaxMergeState(max_host) AS _maxHost FROM statshouse_v6_1m_dist WHERE time>=%d AND time<%d AND index_type=0 AND pre_tag=0 "+
		"AND pre_stag='' AND metric=5 AND (match(stag2,'^s')) GROUP BY _time,tag1,stag2,_shard_num LIMIT 100", from, to)
	rows, cols, err := s.Query(context.Background(), sql)
	require.NoError(t, err)
	r := &series{numRows: rows}
	r.colOrder = []proto.ColResult{&r.time, &r.tag1, &r.stag2, &r.shard, &r.count, &r.min, &r.pct, &r.uniq, &r.minHost, &r.maxHost}
	require.Len(t, cols, len(r.colOrder))
	for i, c := range r.colOrder {
		require.NoError(t, c.DecodeColumn(proto.NewReader(bytes.NewReader(cols[i])), rows), "column %d", i)
	}
	return r
}

func TestStoreInsertQueryCompact(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{})
	minute := time.Now().Add(-2*time.Hour).Unix() / 60 * 60 // every bucket closed
	ts := uint32(minute)
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, ts, 3), sampleRow(5, ts+1, 1), sampleRow(6, ts, 100))))
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, ts+2, 8))))
	require.Equal(t, 4, s.countRows(t, "rows_1m"))
	require.Equal(t, 4, s.countRows(t, "rows_1h"))

	check := func() {
		r := querySeries(t, s, minute, minute+60)
		require.Equal(t, 1, r.numRows)
		require.Equal(t, minute, r.time[0])
		require.Equal(t, int32(7), r.tag1[0])
		require.Equal(t, "s", r.stag2.Row(0))
		require.Equal(t, uint32(3), r.shard[0])
		require.Equal(t, 3.0, r.count[0])
		require.Equal(t, 1.0, r.min[0])
		require.InDelta(t, 3, r.pct[0].Quantile(0.5), 1e-9)
		require.Equal(t, uint64(3), r.uniq[0].Size(true))
		require.Equal(t, int32(1), r.minHost[0].AsInt32)
		require.Equal(t, float32(1), r.minHost[0].Val)
		require.Equal(t, "h", r.maxHost[0].AsString)
		require.Equal(t, float32(8), r.maxHost[0].Val)
	}
	check()
	s.runPass(t, s.compact)
	require.Equal(t, 2, s.countRows(t, "rows_1m"), "one row per key and minute")
	require.Equal(t, 2, s.countRows(t, "rows_1h"))
	require.Equal(t, 4, s.countRows(t, "rows_1s"), "every (metric, second) was distinct already")
	check()
}

func TestStoreRetention(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{Retention1s: time.Hour})
	old, recent := uint32(time.Now().Add(-2*time.Hour).Unix()), uint32(time.Now().Unix())
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, old, 1), sampleRow(5, recent, 1))))
	s.runPass(t, s.retain)
	require.Equal(t, 1, s.countRows(t, "rows_1s"))
	require.Equal(t, 2, s.countRows(t, "rows_1m"), "1m retention is unbounded here")
}

func TestStoreMovesAsideOtherSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Dir: dir})
	require.NoError(t, err)
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, uint32(time.Now().Unix()), 1))))
	_, err = s.db.Exec("UPDATE duck_store_version SET schema_version = 99")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s = openTestStore(t, dir, Config{})
	require.Zero(t, s.countRows(t, "rows_1s"), "a file of another schema version is never read")
	aside, err := filepath.Glob(filepath.Join(dir, fileName+".v99-*"))
	require.NoError(t, err)
	require.Len(t, aside, 1, "the old file is kept for the operator")
}

func TestStoreQueryAdmission(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{QueryConcurrency: 1})
	_, _, err := s.Query(context.Background(), "DELETE FROM rows_1s")
	require.Error(t, err, "only SELECT is served")

	s.sema <- struct{}{} // the only slot is busy
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _, err = s.Query(ctx, "SELECT 1")
	require.ErrorIs(t, err, errOverloaded)
	<-s.sema
	rows, _, err := s.Query(context.Background(), "SELECT 1::INTEGER")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
}
