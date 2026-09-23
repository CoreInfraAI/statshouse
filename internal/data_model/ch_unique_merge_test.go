// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fixed seeds: the assertions are tolerance-banded statistical checks, and a
// deterministic input keeps them reproducible run to run.

// fillUnique feeds u n distinct values drawn from rng. Collisions of a PRNG
// stream over uint64 are negligible (<<1 expected even at 1.6M draws).
func fillUnique(u *ChUnique, n int, rng *rand.Rand) {
	for i := 0; i < n; i++ {
		u.Insert(rng.Uint64())
	}
}

// requireRejectedKeepsState pins the rejection-without-mutation contract every
// malformed-blob test rests on: each blob's decode fails, and the accumulator
// — its marshalled state and its estimate — comes out exactly as it went in.
// The marshalled comparison is the codec's wire format, the bytes ClickHouse
// and duck-store store and exchange, not an internal representation.
// TestChUniqueMergeAboveMaxSize merges two halves of well over uniquesHashMaxSize
// (65536) distinct values and checks the merged estimate. Above that threshold
// shrinkIfNeed raises skipDegree mid-merge, so a source-screened Merge inserts
// items the target's filter should have dropped while Size() still extrapolates
// by 1 << skipDegree — measured +17% at 80K, +1289% at 400K before the fix.
func TestChUniqueMergeAboveMaxSize(t *testing.T) {
	for _, tt := range []struct {
		name string
		n    int
		seed int64
	}{
		{"80K", 80_000, 101},
		{"400K", 400_000, 102},
		{"800K", 800_000, 103},
		{"1.6M", 1_600_000, 104},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(tt.seed))
			var lhs, rhs ChUnique
			fillUnique(&lhs, tt.n/2, rng)
			fillUnique(&rhs, tt.n-tt.n/2, rng)
			// Input sanity: each half alone must already estimate its own count.
			require.InDelta(t, float64(tt.n/2), float64(lhs.Size(false)), 0.02*float64(tt.n/2))
			require.InDelta(t, float64(tt.n-tt.n/2), float64(rhs.Size(false)), 0.02*float64(tt.n-tt.n/2))

			lhs.Merge(rhs)
			size := lhs.Size(false)
			require.InDelta(t,
				float64(tt.n), float64(size), 0.02*float64(tt.n),
				"merged Size() must estimate distinct count within ±2%%")
		})
	}
}

// TestChUniqueMergeOrderInvariance merges the same four parts in two different
// orders (first part as target vs last part as target) and requires both
// estimates to stay within tolerance of the true count and of each other.
func TestChUniqueMergeOrderInvariance(t *testing.T) {
	const (
		n     = 1_600_000
		parts = 4
	)
	rng := rand.New(rand.NewSource(105))
	var ps [parts]ChUnique
	for i := 0; i < n; i++ {
		ps[i%parts].Insert(rng.Uint64())
	}

	forward := ps[0]
	for i := 1; i < parts; i++ {
		forward.Merge(ps[i])
	}
	backward := ps[parts-1]
	for i := parts - 2; i >= 0; i-- {
		backward.Merge(ps[i])
	}

	sizeF := forward.Size(false)
	sizeB := backward.Size(false)
	require.InDelta(t, float64(n), float64(sizeF), 0.02*float64(n), "forward order must estimate within ±2%%")
	require.InDelta(t, float64(n), float64(sizeB), 0.02*float64(n), "backward order must estimate within ±2%%")
	require.InDelta(t, float64(sizeF), float64(sizeB), 0.02*float64(n),
		"the two merge orders must agree within tolerance")
}

// TestChUniqueMergeReadSkipDegree merges the marshalled form of a set whose
// skipDegree was raised by shrinking into a target whose degree is still 0 —
// the direction every fold of stored uniq states takes when a large partial
// meets a small one. MergeRead must raise the target's degree exactly as
// Merge does; before the fix it rehashed at the old degree, so every item of
// the large blob entered the table while Size() still extrapolated by
// 1 << 0 — a 4x undercount at these sizes, and order-dependent (merging the
// other way around was correct).
func TestChUniqueMergeReadSkipDegree(t *testing.T) {
	const (
		bigN   = 200_000 // above uniquesHashMaxSize, so shrinkIfNeed raises the degree
		smallN = 1_000
		seed   = 106
	)
	rng := rand.New(rand.NewSource(seed))
	var big, small ChUnique
	fillUnique(&big, bigN, rng)
	fillUnique(&small, smallN, rng)
	require.GreaterOrEqual(t, big.skipDegree, uint32(1), "the big set must carry a raised skipDegree")
	require.Equal(t, uint32(0), small.skipDegree)

	bigBlob := big.MarshallAppend(nil)
	smallBlob := small.MarshallAppend(nil)
	n := bigN + smallN

	// the raising direction: small target, big blob
	var intoSmall ChUnique
	require.NoError(t, intoSmall.MergeRead(bytes.NewBuffer(smallBlob)))
	require.NoError(t, intoSmall.MergeRead(bytes.NewBuffer(bigBlob)))
	sizeSmallFirst := intoSmall.Size(false)

	// the already-correct direction: big target, small blob
	var intoBig ChUnique
	require.NoError(t, intoBig.MergeRead(bytes.NewBuffer(bigBlob)))
	require.NoError(t, intoBig.MergeRead(bytes.NewBuffer(smallBlob)))
	sizeBigFirst := intoBig.Size(false)

	require.InDelta(t, float64(n), float64(sizeSmallFirst), 0.02*float64(n),
		"merging a raised-degree blob into a degree-0 set must estimate within ±2%%")
	require.InDelta(t, float64(n), float64(sizeBigFirst), 0.02*float64(n),
		"the other merge order must estimate within ±2%%")
	require.InDelta(t, float64(sizeSmallFirst), float64(sizeBigFirst), 0.02*float64(n),
		"the two merge orders must agree within tolerance")
}

// TestChUniqueMergeReadMatchesMerge folds the same two states with MergeRead
// and with Merge and requires the same resulting estimate: the two merge
// entry points are two views of one operation, and a fold whose result
// depends on which one ran is wrong somewhere.
func TestChUniqueMergeReadMatchesMerge(t *testing.T) {
	const (
		bigN   = 200_000
		smallN = 1_000
	)
	rng := rand.New(rand.NewSource(107))
	var big, small ChUnique
	fillUnique(&big, bigN, rng)
	fillUnique(&small, smallN, rng)

	var viaRead ChUnique
	require.NoError(t, viaRead.MergeRead(bytes.NewBuffer(small.MarshallAppend(nil))))
	require.NoError(t, viaRead.MergeRead(bytes.NewBuffer(big.MarshallAppend(nil))))

	var viaMerge ChUnique
	viaMerge.Merge(small)
	viaMerge.Merge(big)

	require.Equal(t, viaMerge.skipDegree, viaRead.skipDegree,
		"both merge paths must settle on the same skipDegree")
	require.InDelta(t, float64(viaMerge.Size(false)), float64(viaRead.Size(false)), 0.02*float64(bigN),
		"MergeRead and Merge must estimate the same merged set within tolerance")
}

// TestChUniqueMergeReadMalformedHeaderKeepsState feeds MergeRead a blob whose
// header is malformed — truncated right after a skip-degree byte higher than
// the target's, or a bogus item count — and requires the error to leave the
// accumulator exactly as it was. The ingestion caller (MergeWithTL2) ignores
// this error, so mutating on the way to it would thin the live set to a
// coarser, noisier estimator with nothing logged; before the fix the degree
// was raised and the table rehashed before the item count was even read.
