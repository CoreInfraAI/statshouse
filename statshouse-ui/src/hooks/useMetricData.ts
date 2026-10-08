// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useEffect, useMemo } from 'react';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { PlotData, setPlotData, usePlotsDataStore } from '@/store2/plotDataStore';
import { normalizePlotData } from '@/store2/plotDataStore/normalizePlotData';
import { ProduceUpdate } from '@/store2/helpers';
import { useApiQuery } from '@/api/query';
import { produce } from 'immer';
import { StatsHouseStore, useStatsHouse, useStatsHouseShallow } from '@/store2';
import { emptyFunction } from '@/common/helpers';
import { queryStart } from '@/store2/plotQueryStore';
import { emptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';
import { ThemeStore, useThemeStore } from '@/store2/themeStore';

const selectorDark = ({ dark }: ThemeStore) => dark;

const selectorStore = ({ params: { timeRange, timeShifts, variables } }: StatsHouseStore) => ({
  timeRange,
  timeShifts,
  variables,
});

export function useMetricData(
  visible: boolean = true,
  priority?: number
): [PlotData, (next: ProduceUpdate<PlotData>) => void] {
  const { plot } = useWidgetPlotContext();
  const plotData = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id] ?? emptyPlotData, [plot.id]));
  const setPlotDataProduce = useMemo(() => setPlotData.bind(undefined, plot.id), [plot.id]);

  const { timeRange, timeShifts, variables } = useStatsHouseShallow(selectorStore);
  const queryData = useApiQuery(plot, timeRange, timeShifts, variables, undefined, visible, priority);
  const response = queryData.data?.data;
  const error = queryData.error;
  const isLoading = queryData.isLoading || queryData.isRefetching;
  // subscribe to the boolean only, not the whole heal history, and to the theme used for the total line color
  const healsStatus = useStatsHouse(useCallback(({ plotHeals }) => plotHeals[plot.id]?.status, [plot.id]));
  const dark = useThemeStore(selectorDark);
  useEffect(() => {
    let prepareEnd: () => void = emptyFunction;
    if (isLoading) {
      prepareEnd = queryStart(plot.id);
    }
    return prepareEnd;
  }, [isLoading, plot.id]);

  useEffect(() => {
    if (error) {
      if (error.status === 403) {
        setPlotDataProduce((d) => {
          d.error403 = error.toString();
        });
      } else {
        setPlotDataProduce((d) => {
          d.error = error.toString();
          d.lastHeals = false;
        });
      }
    } else if (response) {
      setPlotDataProduce((d) => {
        const next = produce(d, normalizePlotData(response, plot, timeShifts));
        next.lastHeals = true;
        if (healsStatus) {
          next.error = '';
        }
        return next;
      });
    }
    // timeRange is not a dependency: live mode changes it every second and the response is the same
  }, [error, plot, response, setPlotDataProduce, timeShifts, healsStatus, dark]);

  return useMemo(() => [plotData, setPlotDataProduce], [plotData, setPlotDataProduce]);
}
