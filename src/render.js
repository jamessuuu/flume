// The two-axis picture.
//
// Event time runs left to right. Processing time runs top to bottom -- one row
// per event, in the order the engine saw them. That is the whole idea of this
// project in one diagram: a stream has two clocks, they disagree, and the
// disagreement is what a watermark is for.
//
// The watermark is drawn as `|` on each row, at the event-time position it held
// after that event was processed. Watching it walk rightwards down the page,
// always behind the rightmost event seen, is the thing worth seeing.
//
// Pure string building. No colour, no cursor control, no terminal detection --
// the output is the same in a pipe, a file and a CI log.

import { parseWindowKey } from './core/window.js';

/** @param {number} ms */
export function humanMs(ms) {
  const a = Math.abs(ms);
  if (a < 1000) return ms + 'ms';
  if (a < 60000) return (ms / 1000).toFixed(1) + 's';
  if (a < 3600000) return (ms / 60000).toFixed(1) + 'm';
  if (a < 86400000) return (ms / 3600000).toFixed(1) + 'h';
  return (ms / 86400000).toFixed(1) + 'd';
}

/**
 * @param {{t: number}[]} events
 * @param {ReturnType<import('./core/engine.js').runEngine>} result
 * @param {{width?: number, maxRows?: number}} [opts]
 * @returns {string[]}
 */
export function renderTimeline(events, result, opts = {}) {
  const width = opts.width ?? 56;
  const maxRows = opts.maxRows ?? 40;
  if (events.length === 0) return ['(no events)'];

  const lo = Math.min(...events.map((e) => e.t));
  const hi = Math.max(...events.map((e) => e.t));
  const span = Math.max(1, hi - lo);
  /** @param {number} t */
  const col = (t) => Math.max(0, Math.min(width - 1, Math.round(((t - lo) / span) * (width - 1))));

  // Rebuild the per-event story from the result rather than re-simulating: the
  // side output and the pane list already say which events were late and when
  // each window closed, so the picture cannot drift away from the verdict.
  /** @type {Map<number, string>} */
  const sideByIndex = new Map();
  for (const s of result.sideOutput) sideByIndex.set(s.index, s.window);
  /** @type {Map<number, typeof result.panes>} */
  const panesByIndex = new Map();
  for (const p of result.panes) {
    const list = panesByIndex.get(p.atIndex) ?? [];
    list.push(p);
    panesByIndex.set(p.atIndex, list);
  }
  /** @type {Map<number, number>} */
  const wmByIndex = new Map();
  for (const s of result.trace) wmByIndex.set(s.index, s.watermark);

  const lines = [];
  // Ruler: window boundaries, so a reader can see which window a mark falls in.
  const ruler = new Array(width).fill('-');
  const windowMs = result.config.windowMs;
  const first = Math.floor((lo - result.config.offsetMs) / windowMs) * windowMs + result.config.offsetMs;
  for (let b = first; b <= hi + windowMs; b += windowMs) {
    if (b >= lo && b <= hi) ruler[col(b)] = ':';
  }
  lines.push('   arr  event time ' + humanMs(0) + ' .. +' + humanMs(span) + '  (: window boundary)');
  lines.push('        ' + ruler.join(''));

  let watermark = Number.NEGATIVE_INFINITY;
  const shown = Math.min(events.length, maxRows);
  for (let i = 0; i < shown; i++) {
    const ev = events[i];
    if (wmByIndex.has(i)) watermark = /** @type {number} */ (wmByIndex.get(i));
    const track = new Array(width).fill(' ');
    if (Number.isFinite(watermark) && watermark >= lo && watermark <= hi) track[col(watermark)] = '|';
    const isSide = sideByIndex.has(i);
    const panes = panesByIndex.get(i) ?? [];
    const revised = panes.some((p) => p.kind === 'revision');
    const mark = isSide ? '!' : revised ? '*' : 'o';
    track[col(ev.t)] = mark;
    /** @type {string[]} */
    const notes = [];
    notes.push('t=+' + humanMs(ev.t - lo));
    for (const p of panes) {
      const w = parseWindowKey(p.window);
      notes.push(
        (p.kind === 'revision' ? 'REVISED ' : 'closed ') +
        '[+' + humanMs(w.start - lo) + ',+' + humanMs(w.end - lo) + ') = ' + p.count +
        (p.retracts !== null ? ' (was ' + p.retracts + ')' : '')
      );
    }
    if (isSide) notes.push('TOO LATE -> side output');
    lines.push('   ' + String(i).padStart(3) + '  ' + track.join('') + '  ' + notes.join('  '));
  }
  if (events.length > shown) {
    lines.push('        ' + '... ' + (events.length - shown) + ' more events not shown');
  }
  lines.push('        ' + ruler.join(''));
  lines.push('   o on time   * arrived after its window closed, inside allowed lateness');
  lines.push('   ! arrived past allowed lateness   | the watermark, after this event');
  return lines;
}

/**
 * The one-screen verdict block. Three counts, then the reasons, then the
 * findings -- in that order, because the counts are the answer and the findings
 * are the accusation.
 *
 * @param {ReturnType<import('./run.js').runStream>} run
 * @returns {string[]}
 */
export function renderVerdict(run) {
  const { verdict, result } = run;
  const lines = [];
  const s = verdict.summary;
  lines.push(
    s.complete + ' complete   ' + s.revised + ' revised   ' + s.unverifiable + ' unverifiable' +
    '   (' + verdict.total + ' windows of ' + humanMs(result.config.windowMs) + ')'
  );
  const reasons = Object.entries(verdict.reasons).sort(([a], [b]) => a.localeCompare(b));
  for (const [reason, n] of reasons) {
    lines.push('  unverifiable because ' + reason + ': ' + n);
  }
  if (verdict.findings.length === 0) {
    lines.push('  no findings against the engine');
  } else {
    lines.push('  ' + verdict.findings.length + ' FINDING(S) against the engine:');
    for (const f of verdict.findings.slice(0, 8)) {
      lines.push('    ' + f.code + (f.window ? ' [' + f.window + ']' : '') + ': ' + f.detail);
    }
    if (verdict.findings.length > 8) {
      lines.push('    ... ' + (verdict.findings.length - 8) + ' more');
    }
  }
  return lines;
}
