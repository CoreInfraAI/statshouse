// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"bytes"
	"testing"

	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

func pctState(values ...float64) []byte {
	td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
	for _, v := range values {
		td.Add(v, 1)
	}
	return rowbinary.AppendCentroids(nil, td, 1)
}

func uniqState(values ...uint64) []byte {
	var u data_model.ChUnique
	for _, v := range values {
		u.Insert(v)
	}
	return u.MarshallAppend(nil)
}

func hostState(host int32, shost string, val float32) []byte {
	arg := data_model.ArgMinMaxStringFloat32{AsInt32: host, AsString: shost, Val: val}
	return arg.MarshallAppend(nil)
}

func decodeHost(t *testing.T, b []byte) data_model.ArgMinMaxStringFloat32 {
	var arg data_model.ArgMinMaxStringFloat32
	_, err := arg.ReadFrom(bytes.NewReader(b), nil)
	require.NoError(t, err)
	return arg
}

func TestFoldPercentilesMatchesDirectMerge(t *testing.T) {
	groups := [][]float64{{1, 2, 3}, {4, 5, 6}, {}, {7, 8, 9, 10}}
	direct := tdigest.NewWithCompression(rowbinary.TDigestCompression)
	var states [][]byte
	for _, vs := range groups {
		states = append(states, pctState(vs...))
		for _, v := range vs {
			direct.Add(v, 1)
		}
	}
	folded, err := foldPercentiles(states)
	require.NoError(t, err)
	got, err := decodeTDigest(folded)
	require.NoError(t, err)
	for _, q := range []float64{0.01, 0.5, 0.99} {
		require.InDelta(t, direct.Quantile(q), got.Quantile(q), 1e-6)
	}

	empty, err := foldPercentiles([][]byte{rowbinary.AppendEmptyCentroids(nil), rowbinary.AppendEmptyCentroids(nil)})
	require.NoError(t, err)
	require.Equal(t, rowbinary.AppendEmptyCentroids(nil), empty)
	_, err = foldPercentiles([][]byte{states[0], {5}}) // five centroids announced, none present
	require.Error(t, err)
}

func TestFoldUniquesIsTheUnion(t *testing.T) {
	folded, err := foldUniques([][]byte{uniqState(1, 2, 3), uniqState(3, 4), rowbinary.AppendEmptyUnique(nil)})
	require.NoError(t, err)
	var u data_model.ChUnique
	require.NoError(t, u.ReadFrom(bytes.NewReader(folded)))
	require.Equal(t, uint64(4), u.Size(true))
}

func TestFoldArgMinMax(t *testing.T) {
	states := [][]byte{hostState(7, "", 5), rowbinary.AppendArgMinMaxStringEmpty(nil), hostState(0, "b", 1), hostState(9, "", 9)}
	folded, err := foldArgMin(states)
	require.NoError(t, err)
	require.Equal(t, data_model.ArgMinMaxStringFloat32{AsString: "b", Val: 1}, decodeHost(t, folded))
	folded, err = foldArgMax(states)
	require.NoError(t, err)
	require.Equal(t, data_model.ArgMinMaxStringFloat32{AsInt32: 9, Val: 9}, decodeHost(t, folded))

	folded, err = foldArgMax([][]byte{rowbinary.AppendArgMinMaxStringEmpty(nil), rowbinary.AppendArgMinMaxStringEmpty(nil)})
	require.NoError(t, err)
	require.Equal(t, rowbinary.AppendArgMinMaxStringEmpty(nil), folded)
}
