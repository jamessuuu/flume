// Synthetic streams, for the fixtures and the negative control.
//
// Real streams live in vendor/ and are where the README's headline numbers come
// from. These exist for the one thing the real ones cannot do: produce a stream
// whose lateness is exactly known, including exactly zero, so a fixture can
// assert a floor and the negative control can assert a zero.
//
// Two properties are deliberate, and the first version of this file had neither
// of them. Both were added because a fixture failed to fire against a stream
// that was too tidy to be a stream:
//
//   1. Event-time gaps are exponential, not a regular grid. On a grid the
//      arrival index and the event time advance in lockstep, which makes a
//      PROCESSING-time watermark accidentally correct -- the `processing-time-
//      watermark` fixture could not fire against it. Real sources are bursty;
//      a grid is a shape no stream has.
//   2. An optional burst: `burstCount` events crammed into `burstSpanMs` of
//      event time. This is the source that buffered during an outage and then
//      replayed, and it is the exact condition under which a processing-time
//      watermark overshoots and closes windows whose data has not arrived.
//
// Generation is a pure function of the seed.

import { sfc32, heavyTail } from '../core/rng.js';

/**
 * @typedef {object} SyntheticSpec
 * @property {number} seed
 * @property {number} events
 * @property {number} [startMs] first event time, default 0
 * @property {number} [stepMs] MEAN event-time gap, default 1000
 * @property {boolean} [regular] use an exact grid instead of exponential gaps
 * @property {number} [lateProb] fraction of events displaced in arrival order
 * @property {number} [maxDelaySlots] largest arrival displacement, in slots
 * @property {number} [keys] distinct partition keys, default 1
 * @property {number} [burstAt] event index at which a burst begins
 * @property {number} [burstCount] events in the burst
 * @property {number} [burstSpanMs] event-time span the burst is crammed into
 */

/**
 * @param {SyntheticSpec} spec
 * @returns {{t: number, p: number, key: string, id: number}[]}
 */
export function syntheticStream(spec) {
  const {
    seed, events, startMs = 0, stepMs = 1000, regular = false,
    lateProb = 0.15, maxDelaySlots = 8, keys = 1,
    burstAt = -1, burstCount = 0, burstSpanMs = 0,
  } = spec;
  if (!Number.isInteger(events) || events <= 0) {
    throw new RangeError('events must be a positive integer, got ' + events);
  }
  const rand = sfc32(seed);
  const burstEnd = burstAt >= 0 ? burstAt + burstCount : -1;
  const burstStep = burstCount > 0 ? Math.max(1, Math.round(burstSpanMs / burstCount)) : 0;

  /** @type {{t: number, id: number, key: string, arrival: number}[]} */
  const rows = [];
  let t = startMs;
  for (let i = 0; i < events; i++) {
    if (i > 0) {
      if (i > burstAt && i < burstEnd) {
        t += burstStep;
      } else if (regular) {
        t += stepMs;
      } else {
        // Exponential inter-event gap, mean stepMs, floored at 1 so event times
        // stay strictly non-decreasing and distinct enough to window on.
        const u = Math.min(0.999999, Math.max(1e-9, rand()));
        t += Math.max(1, Math.round(-Math.log(1 - u) * stepMs));
      }
    }
    const delayed = lateProb > 0 && rand() < lateProb;
    const slots = delayed ? 1 + heavyTail(rand, maxDelaySlots) : 0;
    rows.push({ t, id: i, key: 'k' + (keys === 1 ? 0 : i % keys), arrival: i + slots });
  }
  // Ties broken by original index, so the permutation is total and stable.
  rows.sort((a, b) => (a.arrival - b.arrival) || (a.id - b.id));
  return rows.map((r, rank) => ({
    t: r.t,
    // Processing time, for display on the demo's second axis only. The engine
    // never reads it; it uses the position in this array, which is the same
    // information with none of the ambiguity.
    p: startMs + rank * stepMs,
    key: r.key,
    id: r.id,
  }));
}

/**
 * The negative control's input: a perfectly ordered stream with zero lateness.
 * Not "lateProb near zero" -- exactly zero, by construction, so the assertion it
 * supports is an equality and not a threshold.
 *
 * @param {{seed: number, events: number, startMs?: number, stepMs?: number, keys?: number,
 *          regular?: boolean}} spec
 */
export function orderedStream(spec) {
  return syntheticStream({ ...spec, lateProb: 0, maxDelaySlots: 0 });
}
