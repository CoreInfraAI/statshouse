// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func point(tag1 int64, count float64) pSelectRow {
	var p pSelectRow
	p.tag[1], p.count = tag1, count
	return p
}

// Points of one series in several time buckets must not be summed across the
// buckets, and the latest bucket must end up last whatever the shard order.
func TestMergeShardPoints(t *testing.T) {
	rows := []pSelectRow{point(7, 2), point(7, 1), point(8, 5), point(7, 3)} // shard 1: t101, t100, t100; shard 2: t100
	times := []int64{101, 100, 100, 100}
	var got [][2]float64 // (tag1, count)
	for _, p := range mergeShardPoints(rows, times) {
		got = append(got, [2]float64{float64(p.tag[1]), p.count})
	}
	require.Equal(t, [][2]float64{{7, 4}, {8, 5}, {7, 2}}, got)
}

func row(tag1 int64, stag1 string) tsSelectRow {
	var r tsSelectRow
	r.tag[1], r.stag[1] = tag1, stag1
	return r
}

// Rows of one bucket from two shards interleave like the SQL's ORDER BY, so
// pagination neither skips nor repeats rows.
func TestSortLikeSQL(t *testing.T) {
	rows := []tsSelectRow{row(1, ""), row(3, ""), row(2, "b"), row(2, "a"), row(4, "")}
	sortLikeSQL(rows, []int{1}, false)
	require.Equal(t, []tsSelectRow{row(1, ""), row(2, "a"), row(2, "b"), row(3, ""), row(4, "")}, rows)
	sortLikeSQL(rows, []int{1}, true) // ORDER BY ..., tag1, stag1 DESC: only stag1 descends
	require.Equal(t, []tsSelectRow{row(1, ""), row(2, "b"), row(2, "a"), row(3, ""), row(4, "")}, rows)
}
