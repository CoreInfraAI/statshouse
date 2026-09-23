// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package api

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

// A mapped tag column decodes as Int32 (only aliased expressions are Int64);
// rowAtPoint read the Int64 column unguarded and panicked the api.
func TestRowAtPointMappedTagRepro(t *testing.T) {
	var c seriesQuery
	c.tag = append(c.tag, &tagCol{dataInt32: proto.ColInt32{7}, tagX: 2})
	row := c.rowAtPoint(0)
	require.Equal(t, int64(7), row.tag[2])
}

// v6 keeps a string tag value in stagN with tagN=0; rowAtPoint must copy stag
// the way rowAt does, or the value renders as the mapped fallback.
func TestRowAtPointStringTagRepro(t *testing.T) {
	var c seriesQuery
	var stag stagCol
	stag.tagX = 3
	stag.data.Append("alpha")
	c.stag = append(c.stag, &stag)
	row := c.rowAtPoint(0)
	require.Equal(t, "alpha", row.stag[3])
}

// Grouping a point query by __shard__ must carry the shard into the row, or
// every shard collapses onto shard 0.
func TestRowAtPointShardNumRepro(t *testing.T) {
	var c seriesQuery
	c.shardNum = proto.ColUInt32{2}
	row := c.rowAtPoint(0)
	require.Equal(t, uint32(2), row.shardNum)
}
