// The ground truth.
//
// Once every event has arrived, "how many events fell in each window" is not a
// hard question -- group by event time and count. That is what this file does,
// in one pass, with no watermark, no lateness and no policy.
//
// The oracle exists so that the engine's answer can be compared against a
// number rather than against an opinion. It shares no code with the engine
// beyond window assignment, which is the one thing both must agree on for the
// comparison to mean anything. If assignWindow were wrong, both would be wrong
// together and the comparison would pass -- so test/window.test.js checks
// assignment against hand-computed boundaries, independently of both.

import { assignWindow, windowKey } from './window.js';

/**
 * @param {{t: number}[]} events
 * @param {{windowMs: number, offsetMs?: number}} opts
 * @returns {{windows: {window: string, start: number, end: number, count: number}[],
 *            byKey: Map<string, number>, total: number}}
 */
export function batchWindows(events, { windowMs, offsetMs = 0 }) {
  /** @type {Map<string, {window: string, start: number, end: number, count: number}>} */
  const byWindow = new Map();
  for (const ev of events) {
    const w = assignWindow(ev.t, windowMs, offsetMs);
    const k = windowKey(w);
    const row = byWindow.get(k);
    if (row) row.count++;
    else byWindow.set(k, { window: k, start: w.start, end: w.end, count: 1 });
  }
  const windows = [...byWindow.values()].sort((a, b) => (a.start - b.start) || (a.end - b.end));
  /** @type {Map<string, number>} */
  const byKey = new Map();
  for (const w of windows) byKey.set(w.window, w.count);
  return { windows, byKey, total: events.length };
}

/**
 * Lateness statistics for a log, measured the way the engine would see it:
 * lateness of an event is how far behind the maximum event time seen SO FAR it
 * arrived. An event that arrives in order has lateness 0.
 *
 * This is the number that decides whether a bound is sane for a stream, and it
 * is the number every claim in the README about a vendored stream comes from.
 *
 * @param {{t: number}[]} events
 */
export function latenessProfile(events) {
  let maxSeen = Number.NEGATIVE_INFINITY;
  /** @type {number[]} */
  const lateness = [];
  let outOfOrder = 0;
  for (const ev of events) {
    if (maxSeen !== Number.NEGATIVE_INFINITY && ev.t < maxSeen) {
      outOfOrder++;
      lateness.push(maxSeen - ev.t);
    } else {
      lateness.push(0);
    }
    if (ev.t > maxSeen) maxSeen = ev.t;
  }
  const sorted = lateness.slice().sort((a, b) => a - b);
  /** @param {number} p */
  const q = (p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : 0);
  return {
    events: events.length,
    outOfOrder,
    outOfOrderPct: events.length ? (100 * outOfOrder) / events.length : 0,
    p50: q(0.5),
    p90: q(0.9),
    p99: q(0.99),
    p999: q(0.999),
    max: sorted.length ? sorted[sorted.length - 1] : 0,
  };
}
