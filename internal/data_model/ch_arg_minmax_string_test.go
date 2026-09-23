// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArgMinMaxStringMergeKeepsHostOverEmpty(t *testing.T) {
	var empty ArgMinMaxStringFloat32 // a shard or LOD where no host was recorded

	mn := ArgMinStringFloat32{ArgMinMaxStringFloat32{AsString: "a", Val: 5}}
	mn.Merge(ArgMinStringFloat32{empty})
	require.Equal(t, "a", mn.AsString, "an empty state's zero value must not win the minimum")

	mx := ArgMaxStringFloat32{ArgMinMaxStringFloat32{AsInt32: 3, Val: -5}}
	mx.Merge(ArgMaxStringFloat32{empty})
	require.Equal(t, int32(3), mx.AsInt32, "an empty state's zero value must not win the maximum")
}

func TestArgMinMaxStringReadFromResetsReusedValue(t *testing.T) {
	arg := ArgMinMaxStringFloat32{AsString: "old", Val: 5}
	_, err := arg.ReadFrom(bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0}), nil) // empty state
	require.NoError(t, err)
	require.True(t, arg.Empty())
	require.Zero(t, arg.Val)

	full := ArgMinMaxStringFloat32{AsInt32: 7, Val: 2}
	arg = ArgMinMaxStringFloat32{AsString: "old"}
	_, err = arg.ReadFrom(bytes.NewReader(full.MarshallAppend(nil)), nil)
	require.NoError(t, err)
	require.Equal(t, full, arg, "a mapped host replaces a string one")
}
