// The demo page.
//
// Everything below runs the same engine, oracle and checker as the CLI --
// tools/bundle.mjs walks the import graph from this file, so there is no
// second implementation to drift. The vendored real streams are embedded by
// the same bundler, so a number on this page and a number from
// `node src/cli.js stream usgs` come from the same bytes.
//
// The picture is the argument. Event time runs left to right, processing time
// runs top to bottom, and the watermark is a line through that plane. Every dot
// to the LEFT of the line arrived after the watermark had already passed its
// event time -- which is what "late" means, drawn rather than defined.

import { runStream } from '../src/run.js';
import { syntheticStream } from '../src/streams/synthetic.js';
import { demoStream, DEMO_CONFIG } from '../src/streams/demo.js';
import { parseWindowKey } from '../src/core/window.js';
import { LATE_POLICIES, TERMINATIONS } from '../src/core/engine.js';
import { STRATEGY_NAMES } from '../src/core/watermark.js';

const SVG_NS = 'http://www.w3.org/2000/svg';

/** @type {any} */
const EMBEDDED = (typeof window !== 'undefined' && /** @type {any} */ (window).FLUME_STREAMS) || {};

const SOURCES = [
  { id: 'demo', label: 'the demo log (19 events)' },
  { id: 'synthetic', label: 'generated, seeded' },
  { id: 'gharchive', label: 'GitHub events (real)' },
  { id: 'wikimedia', label: 'Wikimedia edits (real)' },
  { id: 'usgs', label: 'USGS earthquakes (real)' },
];

const DEFAULTS = {
  source: 'demo',
  seed: 4711,
  events: 300,
  policy: 'update',
  watermark: 'bounded',
  bound: 1000,
  window: 5000,
  lateness: 6000,
  termination: 'idle',
};

/** @param {number} ms */
function humanMs(ms) {
  const a = Math.abs(ms);
  if (a < 1000) return Math.round(ms) + 'ms';
  if (a < 60000) return (ms / 1000).toFixed(1) + 's';
  if (a < 3600000) return (ms / 60000).toFixed(1) + 'm';
  if (a < 86400000) return (ms / 3600000).toFixed(1) + 'h';
  return (ms / 86400000).toFixed(1) + 'd';
}

/** Read the whole configuration out of the URL, so any view is a link. */
function readState() {
  const q = new URLSearchParams(location.search);
  /** @type {any} */
  const s = { ...DEFAULTS };
  for (const key of Object.keys(DEFAULTS)) {
    const raw = q.get(key);
    if (raw === null) continue;
    s[key] = typeof DEFAULTS[/** @type {keyof typeof DEFAULTS} */ (key)] === 'number' ? Number(raw) : raw;
  }
  if (!SOURCES.some((x) => x.id === s.source)) s.source = DEFAULTS.source;
  if (!LATE_POLICIES.includes(s.policy)) s.policy = DEFAULTS.policy;
  if (!STRATEGY_NAMES.includes(s.watermark)) s.watermark = DEFAULTS.watermark;
  if (!TERMINATIONS.includes(s.termination)) s.termination = DEFAULTS.termination;
  for (const n of ['seed', 'events', 'bound', 'window', 'lateness']) {
    if (!Number.isFinite(s[n])) s[n] = DEFAULTS[/** @type {keyof typeof DEFAULTS} */ (n)];
  }
  s.window = Math.max(1, Math.round(s.window));
  s.lateness = Math.max(0, Math.round(s.lateness));
  s.bound = Math.max(0, Math.round(s.bound));
  s.events = Math.min(20000, Math.max(10, Math.round(s.events)));
  return s;
}

/** @param {any} state */
function writeState(state) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(state)) {
    if (String(v) !== String(DEFAULTS[/** @type {keyof typeof DEFAULTS} */ (k)])) q.set(k, String(v));
  }
  const url = location.pathname + (q.toString() ? '?' + q.toString() : '');
  history.replaceState(null, '', url);
}

/** @param {any} state */
function buildEvents(state) {
  if (state.source === 'demo') return { events: demoStream(), meta: null };
  if (state.source === 'synthetic') {
    return {
      events: syntheticStream({
        seed: state.seed, events: state.events, stepMs: 1000,
        lateProb: 0.2, maxDelaySlots: 20,
        burstAt: Math.floor(state.events * 0.4),
        burstCount: Math.floor(state.events * 0.27),
        burstSpanMs: 4000,
      }),
      meta: null,
    };
  }
  const s = EMBEDDED[state.source];
  if (!s) return { events: [], meta: null, error: 'stream "' + state.source + '" is not embedded in this page' };
  return { events: s.events, meta: s };
}

/** @param {string} tag @param {any} [attrs] @param {(Node|string)[]} [kids] */
function el(tag, attrs = {}, kids = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') node.className = /** @type {string} */ (v);
    else if (k === 'text') node.textContent = String(v);
    else node.setAttribute(k, String(v));
  }
  for (const kid of kids) node.append(kid);
  return node;
}

/** @param {string} tag @param {any} attrs */
function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  return node;
}

/**
 * The plot. x = event time, y = processing time (position in the log).
 *
 * The watermark is a polyline whose x is the watermark and whose y is the
 * arrival index at which it held that value. Because it only ever moves right
 * as y increases, the region to its left is "the engine has already declared
 * this event time finished". Every dot in that region is a late event, and no
 * further explanation of lateness is needed.
 */
function drawPlot(events, result, verdict) {
  const W = 900;
  const H = Math.min(560, Math.max(260, 40 + events.length * 14));
  const pad = { l: 54, r: 16, t: 26, b: 34 };
  const root = svg('svg', {
    viewBox: '0 0 ' + W + ' ' + H, width: '100%', height: H,
    role: 'img',
    'aria-label':
      'Event time on the horizontal axis, processing order on the vertical axis. ' +
      'The watermark is drawn as a line; every event plotted to its left arrived late.',
  });

  let lo = Infinity;
  let hi = -Infinity;
  for (const e of events) { if (e.t < lo) lo = e.t; if (e.t > hi) hi = e.t; }
  const span = Math.max(1, hi - lo);
  const x = (/** @type {number} */ t) => pad.l + ((t - lo) / span) * (W - pad.l - pad.r);
  const y = (/** @type {number} */ i) => pad.t + (i / Math.max(1, events.length - 1)) * (H - pad.t - pad.b);

  // Window bands, alternating, with the closing tick where the watermark
  // crossed each right edge.
  const first = Math.floor((lo - result.config.offsetMs) / result.config.windowMs) * result.config.windowMs;
  const bands = svg('g', {});
  let band = 0;
  const outcomeByWindow = new Map(verdict.windows.map((/** @type {any} */ w) => [w.window, w.outcome]));
  for (let b = first; b <= hi; b += result.config.windowMs) {
    const x0 = Math.max(pad.l, x(b));
    const x1 = Math.min(W - pad.r, x(b + result.config.windowMs));
    if (x1 <= x0) { band++; continue; }
    const key = b + '-' + (b + result.config.windowMs);
    const outcome = outcomeByWindow.get(key);
    bands.append(svg('rect', {
      x: x0, y: pad.t, width: x1 - x0, height: H - pad.t - pad.b,
      class: 'band band-' + (outcome || 'none') + (band % 2 ? ' band-odd' : ''),
    }));
    band++;
  }
  root.append(bands);

  // The watermark line. One point per trace step, clamped into the plot so an
  // infinite or below-range watermark does not distort the drawing.
  const pts = [];
  let wmHeld = null;
  const traceByIndex = new Map(result.trace.map((/** @type {any} */ s) => [s.index, s.watermark]));
  for (let i = 0; i < events.length; i++) {
    if (traceByIndex.has(i)) wmHeld = traceByIndex.get(i);
    if (wmHeld === null || !isFinite(wmHeld)) continue;
    const cx = Math.max(pad.l, Math.min(W - pad.r, x(wmHeld)));
    pts.push(cx + ',' + y(i));
  }
  if (pts.length > 1) {
    // The region to the LEFT of the watermark is the one the engine has already
    // declared finished. Shading it turns "what does late mean" from a sentence
    // into a place on the page: every dot inside it arrived after the engine had
    // promised nothing more would.
    const firstY = pts[0].split(',')[1];
    const lastY = pts[pts.length - 1].split(',')[1];
    root.append(svg('polygon', {
      points: pad.l + ',' + firstY + ' ' + pts.join(' ') + ' ' + pad.l + ',' + lastY,
      class: 'lateregion',
    }));
    root.append(svg('polyline', { points: pts.join(' '), class: 'watermark' }));
  }

  // Window closings: a tick at the row where each pane fired. With hundreds of
  // windows this is a hatch pattern rather than information, so it is drawn only
  // when the count is small enough to read.
  if (result.panes.length <= 60) {
    for (const pane of result.panes) {
      const w = parseWindowKey(pane.window);
      if (w.end < lo || w.end > hi + result.config.windowMs) continue;
      const cy = y(pane.atIndex);
      const cx = Math.max(pad.l, Math.min(W - pad.r, x(Math.min(w.end, hi))));
      root.append(svg('line', {
        x1: pad.l, y1: cy, x2: cx, y2: cy,
        class: pane.kind === 'revision' ? 'close close-revision' : 'close',
      }));
      if (events.length <= 60) {
        const label = svg('text', { x: Math.min(W - pad.r - 2, cx + 4), y: cy - 3, class: 'panelabel' });
        label.textContent = (pane.kind === 'revision' ? '→ ' : '') + pane.count +
          (pane.retracts !== null ? ' (was ' + pane.retracts + ')' : '');
        root.append(label);
      }
    }
  }

  // The events themselves.
  const sideIdx = new Set(result.sideOutput.map((/** @type {any} */ s) => s.index));
  const revisedIdx = new Set(result.panes.filter((/** @type {any} */ p) => p.kind === 'revision')
    .map((/** @type {any} */ p) => p.atIndex));
  const r = events.length > 800 ? 1.6 : events.length > 120 ? 2.4 : 4;
  for (let i = 0; i < events.length; i++) {
    const cls = sideIdx.has(i) ? 'ev ev-toolate' : revisedIdx.has(i) ? 'ev ev-late' : 'ev';
    const radius = sideIdx.has(i) || revisedIdx.has(i) ? r + 1.5 : r;
    const dot = svg('circle', { cx: x(events[i].t), cy: y(i), r: radius, class: cls });
    const title = svg('title', {});
    title.textContent =
      'row ' + i + ' (processing order), event time +' + humanMs(events[i].t - lo) +
      (sideIdx.has(i) ? ' -- past allowed lateness, sent to the side output'
        : revisedIdx.has(i) ? ' -- late, inside the bound: its window was revised' : '');
    dot.append(title);
    root.append(dot);
  }

  // Axes.
  root.append(svg('line', { x1: pad.l, y1: pad.t, x2: pad.l, y2: H - pad.b, class: 'axis' }));
  root.append(svg('line', { x1: pad.l, y1: H - pad.b, x2: W - pad.r, y2: H - pad.b, class: 'axis' }));
  const xl = svg('text', { x: W - pad.r, y: H - 10, class: 'axislabel', 'text-anchor': 'end' });
  xl.textContent = 'event time →  (0 .. +' + humanMs(span) + ')';
  root.append(xl);
  const yl = svg('text', {
    x: 14, y: pad.t + (H - pad.t - pad.b) / 2, class: 'axislabel',
    transform: 'rotate(-90 14 ' + (pad.t + (H - pad.t - pad.b) / 2) + ')',
    'text-anchor': 'middle',
  });
  yl.textContent = 'processing order ↓';
  root.append(yl);
  const note = svg('text', { x: pad.l + 8, y: H - pad.b - 8, class: 'regionlabel' });
  note.textContent = 'the watermark has already declared this side finished — every dot here is late';
  root.append(note);
  return root;
}

/** @param {any} verdict */
function drawVerdict(verdict) {
  const wrap = el('div', { class: 'verdict' });
  const row = el('div', { class: 'counts' });
  for (const [name, n] of /** @type {[string, number][]} */ ([
    ['complete', verdict.summary.complete],
    ['revised', verdict.summary.revised],
    ['unverifiable', verdict.summary.unverifiable],
  ])) {
    row.append(el('div', { class: 'count count-' + name }, [
      el('strong', { text: String(n) }),
      el('span', { text: name }),
    ]));
  }
  wrap.append(row);
  const reasons = Object.entries(verdict.reasons).sort();
  if (reasons.length) {
    wrap.append(el('p', {
      class: 'muted small',
      text: 'unverifiable because ' + reasons.map(([k, v]) => k + ': ' + v).join(', '),
    }));
  }
  if (verdict.findings.length) {
    const box = el('div', { class: 'findings' });
    box.append(el('strong', { text: verdict.findings.length + ' finding(s) against the engine' }));
    for (const f of verdict.findings.slice(0, 6)) {
      box.append(el('div', { class: 'small', text: f.code + ': ' + f.detail }));
    }
    wrap.append(box);
  } else {
    wrap.append(el('p', { class: 'muted small', text: 'No findings against the engine.' }));
  }
  return wrap;
}

/** @param {any} verdict */
function drawTable(verdict) {
  const table = el('table', { class: 'windows' });
  const head = el('tr', {}, [
    el('th', { text: 'window' }), el('th', { text: 'outcome' }),
    el('th', { text: 'emitted' }), el('th', { text: 'oracle' }),
    el('th', { text: 'panes' }), el('th', { text: 'why' }),
  ]);
  table.append(el('thead', {}, [head]));
  const body = el('tbody', {});
  for (const w of verdict.windows.slice(0, 40)) {
    body.append(el('tr', { class: 'row-' + w.outcome }, [
      el('td', { class: 'mono', text: w.window }),
      el('td', { text: w.outcome }),
      el('td', { class: 'mono', text: w.emitted === null ? '-' : String(w.emitted) }),
      el('td', { class: 'mono', text: String(w.expected) }),
      el('td', { class: 'mono', text: String(w.panes) }),
      el('td', { class: 'small', text: w.detail }),
    ]));
  }
  table.append(body);
  if (verdict.windows.length > 40) {
    table.append(el('caption', {
      class: 'small muted',
      text: 'first 40 of ' + verdict.windows.length + ' windows',
    }));
  }
  return table;
}

function render() {
  const state = readState();
  writeState(state);
  const built = buildEvents(state);
  const host = /** @type {HTMLElement} */ (document.getElementById('output'));
  host.textContent = '';
  if (built.error || built.events.length === 0) {
    host.append(el('p', { class: 'findings', text: built.error || 'no events' }));
    return;
  }
  const events = built.events;
  const config = state.source === 'demo'
    ? { ...DEMO_CONFIG }
    : {
      windowMs: built.meta ? built.meta.windowMs : state.window,
      allowedLatenessMs: built.meta ? built.meta.windowMs : state.lateness,
      latePolicy: state.policy,
      termination: state.termination,
      watermark: {
        name: state.watermark, boundMs: state.bound,
        percentile: 0.99, sampleSize: 512, floorMs: state.bound, msPerEvent: 1000,
      },
    };
  if (state.source === 'demo') {
    config.latePolicy = state.policy;
    config.termination = state.termination;
  }
  const run = runStream(events, config);

  host.append(drawVerdict(run.verdict));
  host.append(drawPlot(events, run.result, run.verdict));
  host.append(el('p', { class: 'legend small' }, [
    el('span', { class: 'k k-wm', text: '—' }), ' the watermark  ',
    el('span', { class: 'k k-ev', text: '●' }), ' on time  ',
    el('span', { class: 'k k-late', text: '●' }), ' late, inside the bound (its window was revised)  ',
    el('span', { class: 'k k-toolate', text: '●' }), ' past the bound (side output)',
  ]));
  host.append(el('p', { class: 'small muted', text: describe(run, built, state) }));
  host.append(drawTable(run.verdict));
  if (built.meta) {
    host.append(el('p', { class: 'small muted' }, [
      el('strong', { text: built.meta.title }), ' — event time is ' + built.meta.eventTime +
      '; processing time is ' + built.meta.processingTime + '. ' + built.meta.licence,
    ]));
  }
}

/** @param {any} run @param {any} built @param {any} state */
function describe(run, built, state) {
  const lp = run.lateness;
  return (
    built.events.length + ' events, ' + lp.outOfOrder + ' out of order (' + lp.outOfOrderPct.toFixed(2) + '%), ' +
    'lateness p50 ' + humanMs(lp.p50) + ' / p99 ' + humanMs(lp.p99) + ' / max ' + humanMs(lp.max) + '. ' +
    'Windows of ' + humanMs(run.result.config.windowMs) + ', allowed lateness ' +
    humanMs(run.result.config.allowedLatenessMs) + ', late policy "' + run.result.config.latePolicy +
    '", termination "' + run.result.config.termination + '". Watermark: ' + run.result.config.watermarkDescribe +
    (state.source === 'demo' ? ' The demo log fixes its window and lateness settings so the story stays the same.' : '')
  );
}

function wireControls() {
  const state = readState();
  const form = /** @type {HTMLFormElement} */ (document.getElementById('controls'));
  /** @type {NodeListOf<HTMLInputElement|HTMLSelectElement>} */
  const fields = form.querySelectorAll('[name]');
  for (const field of fields) {
    const name = field.name;
    if (name in state) field.value = String(state[name]);
    field.addEventListener('change', () => {
      const next = readState();
      /** @type {any} */ (next)[name] = field.value;
      const q = new URLSearchParams();
      for (const [k, v] of Object.entries(next)) {
        if (String(v) !== String(DEFAULTS[/** @type {keyof typeof DEFAULTS} */ (k)])) q.set(k, String(v));
      }
      history.replaceState(null, '', location.pathname + (q.toString() ? '?' + q.toString() : ''));
      render();
      // Keep the visible controls in step with what was actually applied.
      const applied = readState();
      for (const f of fields) if (f.name in applied) f.value = String(applied[f.name]);
    });
  }
}

if (typeof document !== 'undefined') {
  wireControls();
  render();
}
