// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/VKCOM/tl/pkg/rpc"
	"golang.org/x/sync/errgroup"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
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

// mergeShardRows folds rows of the same series into one: every row passed
// shares the same time. A no-op unless several duck shards answered.
func mergeShardRows[R any, P interface {
	*R
	parts() (*tsTags, *tsValues)
}](h *requestHandler, rows []R) []R {
	if h.duck == nil || len(h.duck.clients) < 2 || len(rows) < 2 {
		return rows
	}
	index := make(map[tsTags]int, len(rows))
	res := rows[:0]
	for i := range rows {
		tags, values := P(&rows[i]).parts()
		if j, ok := index[*tags]; ok {
			_, dst := P(&res[j]).parts()
			dst.merge(*values)
			continue
		}
		index[*tags] = len(res)
		res = append(res, rows[i])
	}
	return res
}

func (r *tsSelectRow) parts() (*tsTags, *tsValues) { return &r.tsTags, &r.tsValues }
func (r *pSelectRow) parts() (*tsTags, *tsValues)  { return &r.tsTags, &r.tsValues }
