// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/VKCOM/tl/pkg/rpc"
	"golang.org/x/sync/errgroup"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// duckShards reads metric data from the aggregators' duck-stores (see
// docs/duck-store.md). The query builder renders the same SQL it renders for
// ClickHouse, in its duck dialect; every shard runs it over its own rows and
// answers with ClickHouse Native columns, which decode into the query's
// result columns exactly like a ClickHouse block. Each shard's answer reaches
// the caller as one block, so rows of one series that several shards hold
// come separately — mergeShardRows folds them, the job a ClickHouse
// Distributed table does inside the database.
type duckShards struct {
	clients []*tlstatshouse.Client
}

func newDuckShards(cfg *Config, client rpc.Client) *duckShards {
	if cfg.StorageBackend != duckstore.BackendDuck {
		return nil
	}
	d := &duckShards{}
	for _, addr := range cfg.DuckShardAddrs {
		d.clients = append(d.clients, &tlstatshouse.Client{Client: client, Network: "tcp4", Address: addr})
	}
	return d
}

func (d *duckShards) Select(ctx context.Context, query ch.Query) (time.Duration, error) {
	start := time.Now()
	results, ok := query.Result.(proto.Results)
	if !ok {
		return 0, fmt.Errorf("duck-store: unsupported query result %T", query.Result)
	}
	resps := make([]tlstatshouse.StoreQueryResponse, len(d.clients))
	g, gctx := errgroup.WithContext(ctx)
	for i, c := range d.clients {
		g.Go(func() error {
			if err := c.StoreQuery(gctx, tlstatshouse.StoreQuery{Sql: query.Body}, nil, &resps[i]); err != nil {
				return fmt.Errorf("duck shard %d (%s): %w", i+1, c.Address, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return time.Since(start), err
	}
	for _, resp := range resps {
		if len(resp.Columns) != len(results) {
			return time.Since(start), fmt.Errorf("duck-store: %d columns in the answer, %d in the query", len(resp.Columns), len(results))
		}
		for i, col := range results {
			col.Data.Reset()
			if resp.Rows == 0 {
				continue
			}
			if err := col.Data.DecodeColumn(proto.NewReader(bytes.NewReader([]byte(resp.Columns[i]))), int(resp.Rows)); err != nil {
				return time.Since(start), fmt.Errorf("duck-store: column %s: %w", col.Name, err)
			}
		}
		if err := query.OnResult(ctx, proto.Block{Rows: int(resp.Rows), Columns: len(results)}); err != nil {
			return time.Since(start), err
		}
	}
	return time.Since(start), nil
}

// shardMerge reports whether rows of one series may come from several duck
// shards and must be folded: the job a ClickHouse Distributed table does.
func (h *requestHandler) shardMerge() bool {
	return h.duck != nil && len(h.duck.clients) > 1
}

// mergeShardRows folds rows of the same series and time into one. times holds
// each row's time, or is nil when all rows share one.
func mergeShardRows[R any, P interface {
	*R
	parts() (*tsTags, *tsValues)
}](rows []R, times []int64) ([]R, []int64) {
	type key struct {
		time int64
		tags tsTags
	}
	index := make(map[key]int, len(rows))
	res, resTimes := rows[:0], times[:0]
	for i := range rows {
		tags, _ := P(&rows[i]).parts()
		k := key{tags: *tags}
		if times != nil {
			k.time = times[i]
		}
		if j, ok := index[k]; ok {
			_, dst := P(&res[j]).parts()
			_, src := P(&rows[i]).parts()
			dst.merge(*src)
			continue
		}
		index[k] = len(res)
		res = append(res, rows[i])
		if times != nil {
			resTimes = append(resTimes, k.time)
		}
	}
	return res, resTimes
}

// mergeShardPoints folds point rows of one series and time from several
// shards and orders the result by time, so the point consumer, which keeps
// the last row of a series, sees its latest bucket whatever the shard count.
func mergeShardPoints(rows []pSelectRow, times []int64) []pSelectRow {
	rows, times = mergeShardRows(rows, times)
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return times[order[a]] < times[order[b]] })
	res := make([]pSelectRow, len(rows))
	for i, j := range order {
		res[i] = rows[j]
	}
	return res
}

// sortLikeSQL orders one time bucket's rows like the series SQL's ORDER BY
// (grouped tags in order, the last column descending for sortDescending), which
// pagination relies on once several shards' rows are merged.
func sortLikeSQL(rows []tsSelectRow, by []int, desc bool) {
	var cols []func(l, r *tsSelectRow) int // the ORDER BY columns after _time
	for _, x := range by {
		switch x {
		case format.ShardTagIndex:
			cols = append(cols, func(l, r *tsSelectRow) int { return cmp.Compare(l.shardNum, r.shardNum) })
		default:
			if x == format.StringTopTagIndex {
				x = format.StringTopTagIndexV3
			}
			cols = append(cols,
				func(l, r *tsSelectRow) int { return cmp.Compare(l.tag[x], r.tag[x]) },
				func(l, r *tsSelectRow) int { return strings.Compare(l.stag[x], r.stag[x]) })
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		for i, compare := range cols {
			c := compare(&rows[a], &rows[b])
			if desc && i == len(cols)-1 { // the SQL's DESC applies to the last column only
				c = -c
			}
			if c != 0 {
				return c < 0
			}
		}
		return false
	})
}

func (r *tsSelectRow) parts() (*tsTags, *tsValues) { return &r.tsTags, &r.tsValues }
func (r *pSelectRow) parts() (*tsTags, *tsValues)  { return &r.tsTags, &r.tsValues }
