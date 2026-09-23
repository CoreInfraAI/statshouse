// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/VKCOM/statshouse/internal/format"
)

// The store is one DuckDB file per aggregator shard holding the three tier
// tables. Every insert round appends each row to all three tiers with its time
// truncated to the tier — the shape of ClickHouse's materialized views — in
// one transaction. Queries always GROUP BY, so rows sharing a key are correct
// as they are; compaction only folds them to save space: once a time bucket
// has closed and received more than one round, its rows are collapsed in
// place, the way AggregatingMergeTree merges parts. DuckDB's MVCC keeps
// queries, inserts and compaction out of each other's way.

const (
	fileName        = "statshouse.duckdb"
	compactInterval = 10 * time.Second
	retainInterval  = time.Minute
	sampleInterval  = 30 * time.Second
	// closeGrace is how long past its end a bucket may still receive the
	// conveyor's regular (non-historic) inserts; it is collapsed after that.
	closeGrace       = 10 * time.Second
	collapseBatchMax = 100
	// reconcileChunk is how many buckets of a tier one compaction pass checks
	// for rows left uncollapsed before the process started.
	reconcileChunk = 3600
	reconcileDone  = math.MaxInt64
)

var errOverloaded = errors.New("duck-store: overloaded, every query slot stayed busy until the deadline")

// maxResultBytes bounds a query's encoded columns: the answer travels in one
// RPC packet (at most 16 MiB), and building a bigger one would only fail later.
var maxResultBytes = 14 << 20

type Store struct {
	cfg  Config
	db   *sql.DB
	sema chan struct{}

	insertMu sync.Mutex
	writer   *sql.Conn

	mu       sync.Mutex
	dirty    [3]map[int64]int // per tier: bucket start -> insert rounds since its last collapse
	lastPass [2]time.Time     // last successful compaction, retention

	// The dirty set lives in memory: buckets before startUnix are checked
	// for uncollapsed rows by reconcile, reconcileNext[tier] at a time.
	startUnix     int64
	reconcileNext [3]int64 // owned by the maintenance goroutine; 0: not started

	ctx    context.Context // canceled by Close, stops maintenance
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open opens (creating if needed) the store in cfg.Dir and starts its
// background maintenance. A file stamped with another SchemaVersion is moved
// aside and replaced by an empty one.
func Open(cfg Config) (*Store, error) {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.MemoryLimitBytes <= 0 {
		cfg.MemoryLimitBytes = DefaultMemoryLimitBytes
	}
	if cfg.QueryConcurrency <= 0 {
		cfg.QueryConcurrency = DefaultQueryConcurrency
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(cfg.Dir, fileName)
	db, err := openDB(path, cfg)
	if err != nil {
		return nil, err
	}
	if version, ok := storedVersion(db); ok && version != SchemaVersion {
		_ = db.Close()
		aside := fmt.Sprintf("%s.v%d-%d", path, version, time.Now().Unix())
		cfg.Logf("duck-store: %s has schema version %d, want %d: moving it to %s and starting empty", path, version, SchemaVersion, aside)
		if err := os.Rename(path, aside); err != nil {
			return nil, err
		}
		_ = os.Rename(path+".wal", aside+".wal")
		if db, err = openDB(path, cfg); err != nil {
			return nil, err
		}
	}
	s := &Store{cfg: cfg, db: db, sema: make(chan struct{}, cfg.QueryConcurrency)}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	now := time.Now()
	s.lastPass = [2]time.Time{now, now}
	s.startUnix = now.Unix()
	for i := range s.dirty {
		s.dirty[i] = map[int64]int{}
	}
	s.wg.Add(2)
	go s.maintain()
	go s.sampleLoop()
	return s, nil
}

func openDB(path string, cfg Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s?threads=1&memory_limit=%dB&max_temp_directory_size=%dB&temp_directory=%s&autoinstall_known_extensions=false&autoload_known_extensions=false",
		path, cfg.MemoryLimitBytes, cfg.MemoryLimitBytes, filepath.Join(cfg.Dir, "tmp"))
	return sql.Open("duckdb", dsn)
}

// storedVersion reports the file's stamp; ok is false for a fresh file.
func storedVersion(db *sql.DB) (version int, ok bool) {
	var tables int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_tables()").Scan(&tables); err != nil || tables == 0 {
		return 0, false
	}
	if err := db.QueryRow("SELECT schema_version FROM duck_store_version").Scan(&version); err != nil {
		return -1, true // tables without a stamp: not ours to read
	}
	return version, true
}

func (s *Store) init() (err error) {
	ctx := context.Background()
	if s.writer, err = s.db.Conn(ctx); err != nil {
		return err
	}
	if err := registerFolds(s.writer); err != nil {
		return err
	}
	stmts := []string{"CREATE TABLE IF NOT EXISTS duck_store_version (schema_version INTEGER NOT NULL)"}
	for _, t := range tiers {
		stmts = append(stmts, tierTableDDL(t.table))
	}
	stmts = append(stmts, compatSQL(s.cfg.ShardNum)...)
	stmts = append(stmts, fmt.Sprintf("INSERT INTO duck_store_version SELECT %d WHERE NOT EXISTS (FROM duck_store_version)", SchemaVersion),
		// queries arrive over RPC: no files, extensions or settings are reachable through them
		"SET enable_external_access=false", "SET lock_configuration=true")
	for _, stmt := range stmts {
		if _, err := s.writer.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("duck-store: %s: %w", stmt, err)
		}
	}
	return nil
}

// Close stops maintenance and closes the file.
func (s *Store) Close() error {
	s.cancel()
	s.wg.Wait()
	if s.writer != nil {
		_ = s.writer.Close()
	}
	return s.db.Close()
}

// Insert durably stores one insert round: body is the RowBinary the
// aggregator would send to ClickHouse's statshouse_v3_incoming.
func (s *Store) Insert(ctx context.Context, body []byte) error {
	s.insertMu.Lock()
	defer s.insertMu.Unlock()
	var buckets [3]map[int64]struct{}
	for i := range buckets {
		buckets[i] = map[int64]struct{}{}
	}
	if s.writer == nil {
		var err error
		if s.writer, err = s.db.Conn(ctx); err != nil {
			return err
		}
	}
	err := inTx(ctx, s.writer, "BEGIN", func() error {
		return s.writer.Raw(func(dc any) error {
			var apps []*duckdb.Appender
			var err error
			for _, t := range tiers {
				var a *duckdb.Appender
				if a, err = duckdb.NewAppenderFromConn(dc.(driver.Conn), "", t.table); err != nil {
					break
				}
				apps = append(apps, a)
			}
			vals := make([]driver.Value, len(keyColumns)+len(valueColumns))
			if err == nil {
				err = decodeIncoming(body, func(r *incomingRow) error {
					vals[0] = r.metric
					for i := range r.tags {
						vals[2+2*i], vals[3+2*i] = r.tags[i], r.stags[i]
					}
					v := vals[len(keyColumns):]
					for i, x := range r.values {
						v[i] = x
					}
					for i, x := range r.states {
						v[len(r.values)+i] = x
					}
					for i, t := range tiers {
						ts := int64(r.time) - int64(r.time)%t.seconds
						vals[1] = ts
						if err := apps[i].AppendRow(vals...); err != nil {
							return err
						}
						buckets[i][ts] = struct{}{}
					}
					return nil
				})
			}
			for _, a := range apps { // closing flushes
				if cerr := a.Close(); err == nil {
					err = cerr
				}
			}
			return err
		})
	})
	if err != nil {
		if s.writer.PingContext(context.Background()) != nil { // discarded by inTx
			s.writer = nil
		}
		return err
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, t := range tiers {
		for b := range buckets[i] {
			if n, ok := s.dirty[i][b]; ok {
				s.dirty[i][b] = n + 1
			} else if closed(b, t, now) {
				s.dirty[i][b] = 2 // a late round into a bucket that already holds rows
			} else {
				s.dirty[i][b] = 1
			}
		}
	}
	return nil
}

func closed(bucket int64, t tier, now time.Time) bool {
	return bucket+t.seconds+int64(closeGrace/time.Second) <= now.Unix()
}

// inTx runs fn inside an explicit transaction on conn, begun with begin. It is
// issued through the connection rather than sql.Tx because a duckdb.Appender
// cannot take part in an sql.Tx. Every failure, a failed COMMIT included, rolls
// back; a connection whose transaction cannot be ended would fail every later
// BEGIN, so it is discarded instead of going back to the pool.
func inTx(ctx context.Context, conn *sql.Conn, begin string, fn func() error) error {
	if _, err := conn.ExecContext(ctx, begin); err != nil {
		return err
	}
	err := fn()
	if err == nil {
		if _, err = conn.ExecContext(ctx, "COMMIT"); err == nil {
			return nil
		}
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, rerr := conn.ExecContext(cleanup, "ROLLBACK"); rerr != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn }) // closes the physical connection
	}
	return err
}

// Query runs one SELECT of the shapes the API's query builder renders (see
// checkQueryAST) in a read-only transaction and returns its columns in
// ClickHouse Native encoding.
func (s *Store) Query(ctx context.Context, query string) (rows int, cols [][]byte, err error) {
	start := time.Now()
	select {
	case s.sema <- struct{}{}:
	case <-ctx.Done():
		s.record(format.BuiltinMetricMetaDuckQueryTime, time.Since(start).Seconds(), format.TagValueIDDuckQueryRefused)
		return 0, nil, errOverloaded
	}
	defer func() {
		<-s.sema
		s.record(format.BuiltinMetricMetaDuckQueryTime, time.Since(start).Seconds(), statusTag(err))
	}()
	if len(query) > maxQueryLen {
		return 0, nil, fmt.Errorf("duck-store: query is %d bytes, at most %d are served", len(query), maxQueryLen)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer conn.Close()
	var ast string
	if err := conn.QueryRowContext(ctx, "SELECT CAST(json_serialize_sql(?::VARCHAR) AS VARCHAR)", query).Scan(&ast); err != nil {
		return 0, nil, err
	}
	if err := checkQueryAST(ast); err != nil {
		return 0, nil, err
	}
	err = inTx(ctx, conn, "BEGIN TRANSACTION READ ONLY", func() (err error) {
		rows, cols, err = readNative(ctx, conn, query)
		return err
	})
	return rows, cols, err
}

func readNative(ctx context.Context, conn *sql.Conn, query string) (rows int, cols [][]byte, err error) {
	r, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, nil, err
	}
	defer r.Close()
	types, err := r.ColumnTypes()
	if err != nil {
		return 0, nil, err
	}
	cols = make([][]byte, len(types))
	vals := make([]any, len(types))
	ptrs := make([]any, len(types))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	size := 0
	for r.Next() {
		if err := r.Scan(ptrs...); err != nil {
			return 0, nil, err
		}
		for i, v := range vals {
			if size += nativeSize(v); size > maxResultBytes { // checked before copying the value
				return 0, nil, fmt.Errorf("duck-store: the result exceeds %d bytes after %d rows, narrow the query", maxResultBytes, rows)
			}
			if cols[i], err = appendNative(cols[i], v); err != nil {
				return 0, nil, fmt.Errorf("duck-store: column %s: %w", types[i].Name(), err)
			}
		}
		rows++
	}
	return rows, cols, r.Err()
}

// nativeSize is how many bytes appendNative adds for v.
func nativeSize(v any) int {
	switch v := v.(type) {
	case string:
		return binary.MaxVarintLen64 + len(v)
	case []byte:
		return len(v)
	default:
		return 8
	}
}

// appendNative appends one value in ClickHouse Native encoding; the SQL's
// column types line up with the ClickHouse columns the API decodes into.
// Aggregate states are stored in ClickHouse's own encoding already.
func appendNative(buf []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case int32:
		return binary.LittleEndian.AppendUint32(buf, uint32(v)), nil
	case uint32:
		return binary.LittleEndian.AppendUint32(buf, v), nil
	case int64:
		return binary.LittleEndian.AppendUint64(buf, uint64(v)), nil
	case float64:
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(v)), nil
	case string:
		return append(binary.AppendUvarint(buf, uint64(len(v))), v...), nil
	case []byte:
		return append(buf, v...), nil
	default:
		return buf, fmt.Errorf("unsupported value %T", v)
	}
}

func (s *Store) maintain() {
	defer s.wg.Done()
	compact := time.NewTicker(compactInterval)
	retain := time.NewTicker(retainInterval)
	defer compact.Stop()
	defer retain.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-compact.C:
			s.pass(0, format.TagValueIDDuckMaintenanceCompaction, s.compact)
		case <-retain.C:
			s.pass(1, format.TagValueIDDuckMaintenanceRetention, s.retain)
		}
	}
}

// sampleLoop reports the store's gauges independently of maintenance, so a
// stuck pass shows as a growing maintenance age.
func (s *Store) sampleLoop() {
	defer s.wg.Done()
	sample := time.NewTicker(sampleInterval)
	defer sample.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-sample.C:
			s.sample()
		}
	}
}

func (s *Store) pass(i int, kind int32, fn func(context.Context, *sql.Conn) error) {
	start := time.Now()
	conn, err := s.db.Conn(s.ctx)
	if err == nil {
		err = fn(s.ctx, conn)
		_ = conn.Close()
	}
	if err != nil {
		s.cfg.Logf("duck-store: maintenance pass failed: %v", err)
	} else {
		s.mu.Lock()
		s.lastPass[i] = time.Now()
		s.mu.Unlock()
	}
	s.record(format.BuiltinMetricMetaDuckMaintenanceTime, time.Since(start).Seconds(), kind, statusTag(err))
}

// compact collapses every closed bucket that received more than one round.
func (s *Store) compact(ctx context.Context, conn *sql.Conn) error {
	errs := []error{s.reconcile(ctx, conn)}
	now := time.Now()
	for i, t := range tiers {
		var due []int64
		s.mu.Lock()
		for b, n := range s.dirty[i] {
			if closed(b, t, now) {
				if n > 1 {
					due = append(due, b)
				}
				delete(s.dirty[i], b)
			}
		}
		s.mu.Unlock()
		for len(due) != 0 {
			batch := due[:min(len(due), collapseBatchMax)]
			errs = append(errs, s.collapse(ctx, conn, i, batch))
			due = due[len(batch):]
		}
	}
	return errors.Join(errs...)
}

// collapse folds buckets of tier i in one transaction. A failing batch is
// split until the failing buckets are isolated: they stay dirty for the next
// pass while the rest collapse.
func (s *Store) collapse(ctx context.Context, conn *sql.Conn, i int, buckets []int64) error {
	list := make([]string, len(buckets))
	for j, b := range buckets {
		list[j] = strconv.FormatInt(b, 10)
	}
	err := inTx(ctx, conn, "BEGIN", func() error {
		for _, stmt := range collapseSQL(tiers[i].table, strings.Join(list, ",")) {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if len(buckets) == 1 || ctx.Err() != nil || conn.PingContext(ctx) != nil {
		s.markDirty(i, buckets)
		return fmt.Errorf("collapse %s buckets %v: %w", tiers[i].table, buckets, err)
	}
	half := len(buckets) / 2
	return errors.Join(s.collapse(ctx, conn, i, buckets[:half]), s.collapse(ctx, conn, i, buckets[half:]))
}

func (s *Store) markDirty(i int, buckets []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range buckets {
		s.dirty[i][b] = max(s.dirty[i][b], 2)
	}
}

// reconcile checks, one chunk per tier and pass, the buckets written before
// this process started for rows sharing a key, and marks them dirty: the dirty
// set is kept in memory and a restart forgets it.
func (s *Store) reconcile(ctx context.Context, conn *sql.Conn) error {
	for i, t := range tiers {
		from := s.reconcileNext[i]
		if from == reconcileDone {
			continue
		}
		if from == 0 {
			var first sql.NullInt64
			if err := conn.QueryRowContext(ctx, "SELECT min(time) FROM "+t.table).Scan(&first); err != nil {
				return err
			}
			if !first.Valid {
				s.reconcileNext[i] = reconcileDone
				continue
			}
			from = first.Int64
		}
		to := min(from+reconcileChunk*t.seconds, s.startUnix+1)
		rows, err := conn.QueryContext(ctx, "SELECT time FROM "+t.table+" WHERE time >= ? AND time < ? GROUP BY time HAVING count(*) > count(DISTINCT hash("+strings.Join(keyColumns, ", ")+"))", from, to)
		if err != nil {
			return err
		}
		var found []int64
		for rows.Next() {
			var b int64
			if err := rows.Scan(&b); err != nil {
				rows.Close()
				return err
			}
			found = append(found, b)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		s.markDirty(i, found)
		s.reconcileNext[i] = to
		if to > s.startUnix {
			s.reconcileNext[i] = reconcileDone
		}
	}
	return nil
}

func (s *Store) retain(ctx context.Context, conn *sql.Conn) error {
	for i, retention := range []time.Duration{s.cfg.Retention1s, s.cfg.Retention1m, s.cfg.Retention1h} {
		if retention <= 0 {
			continue
		}
		cutoff := time.Now().Add(-retention).Unix()
		if _, err := conn.ExecContext(ctx, "DELETE FROM "+tiers[i].table+" WHERE time < ?", cutoff); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) sample() {
	var blockSize, used, free int64
	ctx, cancel := context.WithTimeout(s.ctx, sampleInterval)
	defer cancel()
	err := s.db.QueryRowContext(ctx, "SELECT block_size, used_blocks, free_blocks FROM pragma_database_size() WHERE database_name = current_database()").Scan(&blockSize, &used, &free)
	if err == nil {
		s.record(format.BuiltinMetricMetaDuckStoreSize, float64(blockSize*used), format.TagValueIDDuckSizeUsed)
		s.record(format.BuiltinMetricMetaDuckStoreSize, float64(blockSize*free), format.TagValueIDDuckSizeFree)
	}
	now := time.Now()
	s.mu.Lock()
	var backlog [3]int
	for i, t := range tiers {
		for b, n := range s.dirty[i] {
			if n > 1 && closed(b, t, now) {
				backlog[i]++
			}
		}
	}
	lastPass := s.lastPass
	s.mu.Unlock()
	for i, tag := range []int32{format.TagValueIDDuckTier1s, format.TagValueIDDuckTier1m, format.TagValueIDDuckTier1h} {
		s.record(format.BuiltinMetricMetaDuckBacklog, float64(backlog[i]), tag)
	}
	s.record(format.BuiltinMetricMetaDuckMaintenanceAge, now.Sub(lastPass[0]).Seconds(), format.TagValueIDDuckMaintenanceCompaction)
	s.record(format.BuiltinMetricMetaDuckMaintenanceAge, now.Sub(lastPass[1]).Seconds(), format.TagValueIDDuckMaintenanceRetention)
}

func (s *Store) record(meta *format.MetricMetaValue, value float64, tags ...int32) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.AddValueCounter(uint32(time.Now().Unix()), meta, append([]int32{0}, tags...), value, 1)
	}
}

func statusTag(err error) int32 {
	if err != nil {
		return format.TagValueIDStatusError
	}
	return format.TagValueIDStatusOK
}
