// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/hrissan/tdigest"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// The aggregate-state columns hold ClickHouse's own state bytes, which DuckDB
// can neither merge nor produce. These folds merge a list of states into one
// with the codecs the aggregator and the API already use; fold_udf.go exposes
// them to SQL as LIST(BLOB) -> BLOB functions, used both by compaction and by
// the ...MergeState macros queries call.

func foldPercentiles(blobs [][]byte) ([]byte, error) {
	if len(blobs) == 1 {
		return blobs[0], nil
	}
	var merged *tdigest.TDigest
	for _, b := range blobs {
		td, err := decodeTDigest(b)
		if err != nil {
			return nil, err
		}
		if merged == nil {
			merged = td
		} else if td != nil {
			merged.Merge(td)
		}
	}
	return rowbinary.AppendCentroids(nil, merged, 1), nil // nil digest encodes as empty
}

func foldUniques(blobs [][]byte) ([]byte, error) {
	if len(blobs) == 1 {
		return blobs[0], nil
	}
	var u data_model.ChUnique
	for _, b := range blobs {
		if err := u.MergeRead(bytes.NewBuffer(b)); err != nil {
			return nil, fmt.Errorf("duck-store: uniq state: %w", err)
		}
	}
	return u.MarshallAppend(nil), nil
}

func foldArgMin(blobs [][]byte) ([]byte, error) {
	var res data_model.ArgMinStringFloat32
	return foldArgMinMax(blobs, &res.ArgMinMaxStringFloat32, func(v data_model.ArgMinMaxStringFloat32) {
		res.Merge(data_model.ArgMinStringFloat32{ArgMinMaxStringFloat32: v})
	})
}

func foldArgMax(blobs [][]byte) ([]byte, error) {
	var res data_model.ArgMaxStringFloat32
	return foldArgMinMax(blobs, &res.ArgMinMaxStringFloat32, func(v data_model.ArgMinMaxStringFloat32) {
		res.Merge(data_model.ArgMaxStringFloat32{ArgMinMaxStringFloat32: v})
	})
}

func foldArgMinMax(blobs [][]byte, res *data_model.ArgMinMaxStringFloat32, merge func(data_model.ArgMinMaxStringFloat32)) ([]byte, error) {
	if len(blobs) == 1 {
		return blobs[0], nil
	}
	var buf []byte
	for _, b := range blobs {
		var v data_model.ArgMinMaxStringFloat32
		var err error
		if buf, err = v.ReadFrom(bytes.NewReader(b), buf); err != nil {
			return nil, fmt.Errorf("duck-store: argMin/argMax state: %w", err)
		}
		if !v.Empty() {
			merge(v)
		}
	}
	if res.Empty() {
		return rowbinary.AppendArgMinMaxStringEmpty(nil), nil
	}
	return res.MarshallAppend(nil), nil
}

// decodeTDigest decodes one quantilesTDigest state: a uvarint centroid count,
// then per centroid a float32 mean and a float32 weight. nil means no state.
func decodeTDigest(b []byte) (*tdigest.TDigest, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r := bytes.NewReader(b)
	n, err := binary.ReadUvarint(r)
	if err != nil || n == 0 {
		return nil, err
	}
	td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
	var c [8]byte
	for ; n > 0; n-- {
		if _, err := io.ReadFull(r, c[:]); err != nil {
			return nil, fmt.Errorf("duck-store: tdigest state: %w", err)
		}
		td.AddCentroid(tdigest.Centroid{
			Mean:   float64(math.Float32frombits(binary.LittleEndian.Uint32(c[:4]))),
			Weight: float64(math.Float32frombits(binary.LittleEndian.Uint32(c[4:]))),
		})
	}
	td.Normalize()
	return td, nil
}
