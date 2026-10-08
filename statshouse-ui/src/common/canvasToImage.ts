// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export function linkToImage(link: string): Promise<HTMLImageElement> {
  return new Promise((resolve) => {
    const img = new Image();
    img.onload = () => {
      resolve(img);
    };
    img.src = link;
  });
}

export async function toBlob(canvas: HTMLCanvasElement | OffscreenCanvas): Promise<Blob | null> {
  if (typeof window.OffscreenCanvas === 'function' && canvas instanceof window.OffscreenCanvas) {
    return canvas.convertToBlob({ type: 'image/png' });
  }
  return new Promise((resolve) => {
    (canvas as HTMLCanvasElement).toBlob?.(resolve, 'image/png');
  });
}
export function createCanvas(width: number, height: number): HTMLCanvasElement | OffscreenCanvas {
  if (typeof window.OffscreenCanvas === 'function' && width > 0 && height > 0) {
    return new window.OffscreenCanvas(width, height);
  }
  const ext = document.createElement('canvas');
  ext.width = width;
  ext.height = height;
  return ext;
}

type UPlotPathsStroke = Path2D | Map<CanvasRenderingContext2D['strokeStyle'], Path2D>;
type UPlotPathsFill = Path2D | Map<CanvasRenderingContext2D['fillStyle'], Path2D>;
type UPlotPaths = {
  stroke?: UPlotPathsStroke | null;
  fill?: UPlotPathsFill | null;
  clip?: Path2D | null;
  gaps?: number[][] | null;
  _stroke?: CanvasRenderingContext2D['strokeStyle'] | null;
  _fill?: CanvasRenderingContext2D['fillStyle'] | null;
  _width?: number;
};
// uPlot internal state cached on series after draw
type UPlotDrawn = {
  _paths?: UPlotPaths | null;
  _stroke?: CanvasRenderingContext2D['strokeStyle'] | null;
  _fill?: CanvasRenderingContext2D['fillStyle'] | null;
};
type UPlotDrawnSeries = uPlot.Series & UPlotDrawn & { points?: uPlot.Series.Points & UPlotDrawn };
type PreviewContext = CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D;

function eachPath<S>(
  paths: Path2D | Map<S, Path2D> | null | undefined,
  style: S | null | undefined,
  draw: (path: Path2D, style: S) => void
) {
  if (paths instanceof Map) {
    paths.forEach((path, pathStyle) => draw(path, pathStyle));
  } else if (paths && style) {
    draw(paths, style);
  }
}

// same order as uPlot strokeFill: clip, fill, stroke; band clips are not replayed, current plots don't use them
function fillStroke(
  ctx: PreviewContext,
  paths: UPlotPaths,
  strokeStyle: CanvasRenderingContext2D['strokeStyle'] | null | undefined,
  fillStyle: CanvasRenderingContext2D['fillStyle'] | null | undefined,
  lineWidth: number,
  dash: number[] | undefined,
  cap: CanvasLineCap | undefined
) {
  ctx.save();
  if (paths.clip) {
    ctx.clip(paths.clip);
  }
  eachPath(paths.fill, fillStyle, (path, style) => {
    ctx.fillStyle = style;
    ctx.fill(path);
  });
  if (lineWidth > 0) {
    ctx.lineWidth = lineWidth;
    ctx.lineJoin = 'round';
    ctx.setLineDash(dash ?? []);
    ctx.lineCap = cap ?? 'butt';
    eachPath(paths.stroke, strokeStyle, (path, style) => {
      ctx.strokeStyle = style;
      ctx.stroke(path);
    });
  }
  ctx.restore();
}

function drawSeriesPreview(ctx: PreviewContext, u: uPlot, series: UPlotDrawnSeries[], si: number) {
  const s = series[si];
  const paths = s._paths;
  if (!paths) {
    return;
  }
  fillStroke(
    ctx,
    paths,
    paths._stroke ?? s._stroke,
    paths._fill ?? s._fill,
    (paths._width ?? s.width ?? 1) * devicePixelRatio,
    s.dash,
    s.cap
  );

  const points = s.points;
  const pointsPaths = points?._paths;
  if (points && pointsPaths) {
    // uPlot keeps old point paths when points stop showing, so repeat its show/filter check
    const [i0, i1] = (series[0].idxs ?? [0, 0]) as number[];
    const show = typeof points.show === 'function' ? points.show(u, si, i0, i1, paths.gaps) : points.show;
    const filtered = typeof points.filter === 'function' ? points.filter(u, si, !!show, paths.gaps) : points.filter;
    if (show || filtered) {
      const width = (pointsPaths._width ?? points.width ?? 0) * devicePixelRatio;
      const strokeStyle = pointsPaths._stroke ?? points._stroke;
      const pointsFill = pointsPaths._fill ?? points._fill ?? (width > 0 ? '#fff' : strokeStyle);
      fillStroke(ctx, pointsPaths, strokeStyle, pointsFill, width, points.dash, points.cap);
    }
  }
}

// Redraws uPlot series paths into a software canvas instead of reading pixels from u.ctx.canvas:
// reading an accelerated canvas blocks the main thread until the GPU process flushes all pending canvas work.
export async function uPlotToImageData(u: uPlot, result_width?: number): Promise<string> {
  const { left, top, width, height } = u.bbox;
  const series = u.series as UPlotDrawnSeries[];
  // data loaded but series not drawn yet, an empty preview would replace the previous one
  const notDrawn = !!u.data[0]?.length && series.some((s, si) => si > 0 && s.show && !s._paths);
  if (!(width > 0 && height > 0) || notDrawn) {
    return '';
  }
  const ext_width = Math.round(result_width ?? width);
  const ext_height = Math.round((ext_width / width) * height);
  const ext = createCanvas(ext_width, ext_height);
  const ctx = ext.getContext('2d', { willReadFrequently: true }) as PreviewContext | null;
  if (ctx) {
    ctx.scale(ext_width / width, ext_height / height);
    ctx.translate(-left, -top);
    ctx.beginPath();
    ctx.rect(left, top, width, height);
    ctx.clip();
    series.forEach((s, si) => {
      if (si > 0 && s.show) {
        drawSeriesPreview(ctx, u, series, si);
      }
    });
  }
  const blobExt = await toBlob(ext);
  if (!blobExt) {
    return '';
  }
  return URL.createObjectURL(blobExt);
}

export async function setBackgroundColor(
  data: string,
  color: string,
  result_width?: number,
  result_height?: number
): Promise<string> {
  if (!data || !color) {
    return '';
  }
  const imgElement = await linkToImage(data);
  if (!imgElement) {
    return '';
  }
  const { width, height } = imgElement;
  const ext_width = result_width ?? width;
  const ext_height = result_height ?? (result_width ? Math.round((result_width / width) * height) : height);
  const ext = createCanvas(ext_width, ext_height);
  const ctx = ext.getContext('2d') as CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D | null;
  if (ctx) {
    ctx.globalAlpha = 1;
    ctx.fillStyle = color;
    ctx.fillRect(0, 0, ext_width, ext_height);
    ctx.drawImage(imgElement, 0, 0, width, height, 0, 0, ext_width, ext_height);
  }
  const blobExt = await toBlob(ext);
  if (!blobExt) {
    return '';
  }
  return URL.createObjectURL(blobExt);
}
