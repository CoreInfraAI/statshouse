// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Queue } from '@/common/Queue';
import { createStore, StoreSlice } from '../createStore';
import { PlotKey } from '@/url2';
import { uPlotToImageData } from '@/common/canvasToImage';
import { skipTimeout } from '@/common/helpers';
import { usePlotVisibilityStore } from '@/store2/plotVisibilityStore';

const queuePreview = new Queue();

// live mode updates every plot every few seconds, previews don't need to follow each update
const previewMinInterval = 5000;
const previewThrottle: Partial<Record<PlotKey, { last: number; timer?: ReturnType<typeof setTimeout> }>> = {};

function clearPreviewThrottle(plotKey: PlotKey) {
  clearTimeout(previewThrottle[plotKey]?.timer);
  delete previewThrottle[plotKey];
}

export type PlotPreviewStore = {
  plotPreviewUrlList: Partial<Record<PlotKey, string>>;
  plotPreviewAbortController: Partial<Record<PlotKey, AbortController>>;
};
export const plotPreviewStore: StoreSlice<PlotPreviewStore, PlotPreviewStore> = () => ({
  plotPreviewUrlList: {},
  plotPreviewAbortController: {},
});

export const usePlotPreviewStore = createStore<PlotPreviewStore>(plotPreviewStore);

export async function createPlotPreview(plotKey: PlotKey, u: uPlot, width: number = 300) {
  const throttle = (previewThrottle[plotKey] ??= { last: 0 });
  clearTimeout(throttle.timer);
  throttle.timer = undefined;
  const wait = throttle.last + previewMinInterval - Date.now();
  if (wait > 0 && usePlotPreviewStore.getState().plotPreviewUrlList[plotKey]) {
    throttle.timer = setTimeout(() => {
      throttle.timer = undefined;
      if (u.root.isConnected) {
        createPlotPreview(plotKey, u, width);
      }
    }, wait);
    return;
  }
  throttle.last = Date.now();
  await renderPlotPreview(plotKey, u, width);
}

async function renderPlotPreview(plotKey: PlotKey, u: uPlot, width: number) {
  await skipTimeout();
  if (
    !usePlotVisibilityStore.getState().plotPreviewList[plotKey] &&
    !usePlotVisibilityStore.getState().plotVisibilityList[plotKey]
  ) {
    return;
  }
  const controller = new AbortController();
  usePlotPreviewStore.setState((state) => {
    state.plotPreviewAbortController[plotKey]?.abort();
    state.plotPreviewAbortController[plotKey] = controller;
  });
  try {
    const url = await queuePreview.add(
      () => uPlotToImageData(u, devicePixelRatio ? devicePixelRatio * width : width),
      controller.signal
    );
    if (url) {
      setPlotPreview(plotKey, url);
    } else if (previewThrottle[plotKey]) {
      previewThrottle[plotKey].last = 0;
    }
  } catch (_) {
    // abort task
  }
  usePlotPreviewStore.setState((state) => {
    delete state.plotPreviewAbortController[plotKey];
  });
}
export function setPlotPreview(plotKey: PlotKey, url: string) {
  usePlotPreviewStore.setState((state) => {
    const old = state.plotPreviewUrlList[plotKey];
    if (old && old.indexOf('blob:') === 0 && old !== url) {
      URL.revokeObjectURL(old);
    }
    state.plotPreviewUrlList[plotKey] = url;
  });
}

export function clearAllPlotPreview() {
  (Object.keys(previewThrottle) as PlotKey[]).forEach(clearPreviewThrottle);
  usePlotPreviewStore.setState((state) => {
    Object.values(state.plotPreviewUrlList).forEach((url) => {
      if (url) {
        URL.revokeObjectURL(url);
      }
    });
    state.plotPreviewUrlList = {};
    state.plotPreviewAbortController = {};
  });
}

export function clearPlotPreview(plotKey: PlotKey) {
  clearPreviewThrottle(plotKey);
  usePlotPreviewStore.setState((state) => {
    const url = state.plotPreviewUrlList[plotKey];
    if (url) {
      URL.revokeObjectURL(url);
    }
    delete state.plotPreviewUrlList[plotKey];
    delete state.plotPreviewAbortController[plotKey];
  });
}
