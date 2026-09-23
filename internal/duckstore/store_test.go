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
	"database/sql/driver"
	"fmt"
	"os"
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

// A transaction whose COMMIT never ran (its context died first) must not leave
// the writer inside it: the next insert has to commit, and the abandoned rows
// must not appear.
func TestStoreInsertRecoversFromAbandonedCommit(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{})
	ctx, cancel := context.WithCancel(context.Background())
	err := inTx(ctx, s.writer, "BEGIN", func() error {
		_, err := s.writer.ExecContext(ctx, "INSERT INTO rows_1s SELECT * FROM rows_1s")
		cancel()
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
	now := uint32(time.Now().Unix())
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, now, 1))))
	for _, table := range []string{"rows_1s", "rows_1m", "rows_1h"} {
		require.Equal(t, 1, s.countRows(t, table))
	}

	// a writer the store had to discard is replaced on the next insert
	_ = s.writer.Raw(func(any) error { return driver.ErrBadConn })
	require.Error(t, s.Insert(context.Background(), testBody(sampleRow(5, now, 1))))
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, now, 1))))
	require.Equal(t, 2, s.countRows(t, "rows_1s"))
}

// Queries arrive over RPC, so only the shapes the API's builder renders run,
// read-only, with no file or setting reachable.
func TestStoreQueryRefusesAnythingButBuilderSelects(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Config{})
	secret := filepath.Join(dir, "secret.txt")
	require.NoError(t, os.WriteFile(secret, []byte("s3cret"), 0o600))
	for _, q := range []string{
		"SELECT 1; DROP TABLE rows_1s; SELECT 1::INTEGER",
		"SELECT 1::INTEGER; COMMIT; DELETE FROM rows_1s",
		"DELETE FROM rows_1s",
		"SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))",
		"SELECT content FROM read_text('" + secret + "')",
		"SELECT * FROM query('SELECT 1')",
		"SELECT count(*) FROM rows_1s",
		"SELECT metric FROM statshouse_v6_1s_dist WHERE metric IN (SELECT 1)",
		"SELECT metric FROM statshouse_v6_1s_dist UNION ALL SELECT metric FROM statshouse_v6_1m_dist",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT current_setting('memory_limit')",
		"SELECT metric FROM main.statshouse_v6_1s_dist",
	} {
		_, _, err := s.Query(context.Background(), q)
		require.Error(t, err, q)
	}
	var n int
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'rows_1s'").Scan(&n))
	require.Equal(t, 1, n)
	_, err := s.db.Exec("SET memory_limit='1GB'")
	require.Error(t, err, "the configuration is locked")
	_, err = s.db.Exec("SELECT * FROM read_text('" + secret + "')")
	require.Error(t, err, "external access is disabled")
}

func TestStoreQueryRefusesOversizedResult(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{})
	now := uint32(time.Now().Unix())
	require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, now, 1), sampleRow(5, now+1, 2))))
	defer func(v int) { maxResultBytes = v }(maxResultBytes)
	maxResultBytes = 20 // one row: 8 bytes of time + 8 of value
	_, _, err := s.Query(context.Background(), "SELECT toInt64(time) AS _time, toFloat64(sum(count)) AS _val0 FROM statshouse_v6_1s_dist GROUP BY _time")
	require.ErrorContains(t, err, "narrow the query")
}

// Rows left uncollapsed before a restart, however old, are found and
// collapsed: the dirty set does not survive a restart.
func TestStoreCollapsesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ts := uint32(time.Now().Add(-24 * time.Hour).Unix())
	s, err := Open(Config{Dir: dir})
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, ts, 1))))
	}
	require.NoError(t, s.Close())

	s = openTestStore(t, dir, Config{})
	for _, table := range []string{"rows_1s", "rows_1m", "rows_1h"} {
		require.Equal(t, 2, s.countRows(t, table))
	}
	s.runPass(t, s.compact)
	for _, table := range []string{"rows_1s", "rows_1m", "rows_1h"} {
		require.Equal(t, 1, s.countRows(t, table), table)
	}
}

// A bucket that cannot be collapsed (here: a corrupt aggregate state) stays
// dirty for the next pass without holding back the other buckets.
func TestStoreCollapseIsolatesFailingBucket(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Config{})
	good, bad := time.Now().Add(-2*time.Hour).Unix()/60*60, time.Now().Add(-3*time.Hour).Unix()/60*60
	for range 2 {
		require.NoError(t, s.Insert(context.Background(), testBody(sampleRow(5, uint32(good), 1), sampleRow(5, uint32(bad), 1))))
	}
	_, err := s.db.Exec("UPDATE rows_1m SET percentiles = '\\x05'::BLOB WHERE time = ?", bad)
	require.NoError(t, err)
	conn, err := s.db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	require.Error(t, s.compact(context.Background(), conn))
	var n int
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM rows_1m WHERE time = ?", good).Scan(&n))
	require.Equal(t, 1, n, "the good bucket collapsed")
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM rows_1m WHERE time = ?", bad).Scan(&n))
	require.Equal(t, 2, n, "the failing bucket is left as it was")
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, 2, s.dirty[1][bad], "and retried on the next pass")
}
