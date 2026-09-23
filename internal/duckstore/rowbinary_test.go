// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// testRow is one statshouse_v3_incoming row, encoded like the aggregator does.
type testRow struct {
	metric         int32
	time           uint32
	tag1           int32
	stag2          string
	count, min     float64
	max, sum       float64
	pct, uniq      []byte
	minHost        []byte
	maxHost        []byte
	maxCountHostTo []byte
}

func (r testRow) append(buf []byte) []byte {
	buf = append(buf, 0) // index_type
	buf = binary.LittleEndian.AppendUint32(buf, uint32(r.metric))
	buf = binary.LittleEndian.AppendUint32(buf, r.time)
	for i := 0; i < format.MaxTags; i++ {
		var tag int32
		var stag string
		switch i {
		case 1:
			tag = r.tag1
		case 2:
			stag = r.stag2
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(tag))
		buf = rowbinary.AppendString(buf, stag)
	}
	for _, v := range []float64{r.count, r.count, r.min, r.max, r.sum, r.sum * r.sum} {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	}
	for _, s := range [][]byte{r.pct, r.uniq, r.minHost, r.maxHost, r.maxCountHostTo} {
		buf = append(buf, s...)
	}
	return buf
}

func testBody(rows ...testRow) []byte {
	var body []byte
	for _, r := range rows {
		body = r.append(body)
	}
	return body
}

func sampleRow(metric int32, ts uint32, value float64) testRow {
	return testRow{
		metric: metric, time: ts, tag1: 7, stag2: "s",
		count: 1, min: value, max: value, sum: value,
		pct: pctState(value), uniq: uniqState(uint64(value)),
		minHost: hostState(1, "", float32(value)), maxHost: hostState(0, "h", float32(value)),
		maxCountHostTo: rowbinary.AppendArgMinMaxStringEmpty(nil),
	}
}

func TestDecodeIncoming(t *testing.T) {
	a, b := sampleRow(5, 1000, 3), sampleRow(6, 1001, 4)
	var got []incomingRow
	require.NoError(t, decodeIncoming(testBody(a, b), func(r *incomingRow) error {
		got = append(got, *r)
		return nil
	}))
	require.Len(t, got, 2)
	require.Equal(t, int32(6), got[1].metric)
	require.Equal(t, uint32(1001), got[1].time)
	require.Equal(t, int32(7), got[1].tags[1])
	require.Equal(t, "s", got[1].stags[2])
	require.Equal(t, [6]float64{1, 1, 4, 4, 4, 16}, got[1].values)
	require.Equal(t, [5][]byte{b.pct, b.uniq, b.minHost, b.maxHost, b.maxCountHostTo}, got[1].states)

	body := testBody(a)
	require.Error(t, decodeIncoming(body[:len(body)-1], func(*incomingRow) error { return nil }), "a truncated body must fail")
}
