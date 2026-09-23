// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDuckStoreMetricsAreRegistered pins the duck-store observability metrics
// in the builtin registry: their IDs resolve to the metas, the metas carry
// the delivery flags every aggregator-written builtin metric carries, and
// package init padded every tag layout to the fixed width with the
// aggregator-identity tags in place.
func TestDuckStoreMetricsAreRegistered(t *testing.T) {
	for id, m := range map[int32]*MetricMetaValue{
		-157: BuiltinMetricMetaDuckMaintenanceTime,
		-160: BuiltinMetricMetaDuckQueryTime,
		-161: BuiltinMetricMetaDuckStoreSize,
		-162: BuiltinMetricMetaDuckBacklog,
		-163: BuiltinMetricMetaDuckMaintenanceAge,
	} {
		require.Same(t, m, BuiltinMetrics[id], "id %d must be in the builtin registry", id)
		require.Contains(t, BuiltinMetricByName, m.Name)
		require.Equal(t, id, m.MetricID, "init stamps the metric id")
		require.True(t, m.NoSampleAgent, m.Name)
		require.True(t, m.WithAggregatorID, m.Name)
		require.Equal(t, "aggregator_host", m.Tags[AggHostTag].Description, m.Name)
	}
}
