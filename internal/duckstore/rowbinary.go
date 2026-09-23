// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/VKCOM/statshouse/internal/format"
)

// incomingRow is one row of the RowBinary body the aggregator inserts into
// ClickHouse's statshouse_v3_incoming (the column list is getTableDesc in
// internal/aggregator): duck-store ingests exactly the bytes ClickHouse would.
type incomingRow struct {
	metric int32
	time   uint32
	tags   [format.MaxTags]int32
	stags  [format.MaxTags]string
	values [6]float64 // count, max_count, min, max, sum, sumsquare
	states [5][]byte  // percentiles, uniq_state, min_host, max_host, max_count_host: ClickHouse state bytes
}

// decodeIncoming calls fn for every row of body. State slices alias body.
func decodeIncoming(body []byte, fn func(*incomingRow) error) error {
	d := rowDecoder{buf: body}
	var row incomingRow
	for len(d.buf) != 0 && d.err == nil {
		d.fixed(1) // index_type, always 0
		row.metric = int32(binary.LittleEndian.Uint32(d.fixed(4)))
		row.time = binary.LittleEndian.Uint32(d.fixed(4))
		for i := range row.tags {
			row.tags[i] = int32(binary.LittleEndian.Uint32(d.fixed(4)))
			row.stags[i] = string(d.fixed(d.uvarint()))
		}
		for i := range row.values {
			row.values[i] = math.Float64frombits(binary.LittleEndian.Uint64(d.fixed(8)))
		}
		row.states[0] = d.state(func() { d.fixed(8 * d.uvarint()) })
		row.states[1] = d.state(func() { d.fixed(1); d.fixed(4 * d.uvarint()) })
		for i := 2; i < 5; i++ {
			row.states[i] = d.state(d.argMinMax)
		}
		if d.err != nil {
			break
		}
		if err := fn(&row); err != nil {
			return err
		}
	}
	return d.err
}

type rowDecoder struct {
	buf []byte
	err error
}

var zeros [8]byte

func (d *rowDecoder) fixed(n int) []byte {
	if d.err != nil || n > len(d.buf) {
		if d.err == nil {
			d.err = fmt.Errorf("duck-store: RowBinary body truncated")
		}
		return zeros[:]
	}
	b := d.buf[:n]
	d.buf = d.buf[n:]
	return b
}

func (d *rowDecoder) uvarint() int {
	v, n := binary.Uvarint(d.buf)
	if n <= 0 || v > math.MaxInt32 {
		if d.err == nil {
			d.err = fmt.Errorf("duck-store: RowBinary body has a bad length prefix")
		}
		return 0
	}
	d.buf = d.buf[n:]
	return int(v)
}

// state returns the bytes one aggregate-state reader consumed.
func (d *rowDecoder) state(read func()) []byte {
	start := d.buf
	read()
	return start[:len(start)-len(d.buf)]
}

// argMinMax skips one argMin/argMax(String, Float32) state: a uint32 length
// (0xffffffff when there is no argument) and that many argument bytes, then a
// has-value flag and the float32 value (see data_model.ArgMinMaxStringFloat32).
func (d *rowDecoder) argMinMax() {
	if n := binary.LittleEndian.Uint32(d.fixed(4)); n != math.MaxUint32 {
		d.fixed(int(n))
	}
	if d.fixed(1)[0] != 0 {
		d.fixed(4)
	}
}
