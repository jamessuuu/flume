// Watermark strategies.
//
// A watermark is an assertion about event time: "no event with a timestamp at
// or below W will arrive from here on." It is a heuristic, and every strategy
// here is wrong sometimes -- what differs is how, and whether the engine can
// tell afterwards.
//
// The contract every strategy must honour:
//
//   1. It reads ONLY the event times it has been shown. Never a clock, never
//      the arrival index, never a random number. `processing-time` violates
//      this on purpose; it is a planted fixture, not a strategy anyone should
//      choose.
//   2. The EMITTED watermark is monotonic non-decreasing. A watermark that goes
//      backwards retracts an assertion the engine has already acted on, so
//      `emit()` clamps. Whether a clamp is a bug depends on the strategy: for a
//      fixed bound the raw value can never fall, so a clamp means something is
//      wrong; for an adaptive bound the raw value falls whenever the sample
//      forgets an outlier, and the clamp is the design. Each strategy declares
//      which it is in `monotonicByConstruction`, and the checker only accuses a
//      strategy that claims monotonicity and then breaks it. Getting this wrong
//      was one of the three false positives real data found.
//   3. It is a pure function of the sequence of event times, so replaying a
//      log produces the same watermark trace byte for byte.

/**
 * @typedef {object} WatermarkStrategy
 * @property {string} name
 * @property {string} describe human-readable, printed in every report
 * @property {(eventTime: number, index: number) => void} observe
 * @property {() => number} emit current watermark, monotonic
 * @property {() => number} rawEmit pre-clamp value, for the regression check
 * @property {boolean} monotonicByConstruction whether this strategy's RAW output
 *   can never fall. Only a strategy that claims this and then falls is a defect;
 *   for an adaptive strategy a falling raw value is the design, and the clamp is
 *   the mechanism, not a repair. Real data taught this distinction -- see the
 *   README on false positives.
 */

/** Below every possible event time; the value a watermark starts at. */
export const WATERMARK_MIN = Number.NEGATIVE_INFINITY;

/** Above every possible event time; only a graceful end-of-stream emits it. */
export const WATERMARK_MAX = Number.POSITIVE_INFINITY;

export const STRATEGY_NAMES = [
  'bounded',
  'bounded-naive',
  'percentile',
  'punctuated',
  'processing-time',
];

/**
 * Wrap a raw strategy so monotonicity is enforced in exactly one place and
 * every attempted regression is recorded rather than quietly smoothed away.
 *
 * @param {{name: string, describe: string, monotonicByConstruction: boolean,
 *          observe: (t: number, i: number) => void, raw: () => number}} impl
 * @returns {WatermarkStrategy & {regressions: number, maxRegressionMs: number}}
 */
function monotonic(impl) {
  let held = WATERMARK_MIN;
  let regressions = 0;
  let maxRegressionMs = 0;
  const wrapped = {
    name: impl.name,
    describe: impl.describe,
    monotonicByConstruction: impl.monotonicByConstruction,
    /** @param {number} t @param {number} i */
    observe(t, i) {
      impl.observe(t, i);
    },
    rawEmit: impl.raw,
    emit() {
      const raw = impl.raw();
      if (raw > held) {
        held = raw;
      } else if (raw < held && held !== WATERMARK_MIN && Number.isFinite(raw)) {
        wrapped.regressions++;
        const delta = held - raw;
        if (delta > wrapped.maxRegressionMs) wrapped.maxRegressionMs = delta;
      }
      return held;
    },
    regressions,
    maxRegressionMs,
  };
  return wrapped;
}

/**
 * Bounded out-of-orderness, the strategy nearly every real job uses.
 *
 * watermark = maxSeenEventTime - bound - 1
 *
 * The `- 1` is not a rounding flourish. Event times are integers and many
 * events can share one. If the watermark were `maxSeen - bound`, then with a
 * bound of 0 the arrival of an event at time T would immediately assert that
 * nothing at time T will arrive again -- but simultaneous events are exactly
 * what a millisecond or second timestamp produces in bulk. Apache Flink's
 * BoundedOutOfOrdernessWatermarks subtracts the same 1 for the same reason.
 * `bounded-naive` omits it, and the real streams in vendor/ are what proved
 * the difference matters (see README, "the off-by-one real data found").
 *
 * @param {{boundMs: number}} opts
 */
function boundedStrategy({ boundMs }) {
  let maxSeen = WATERMARK_MIN;
  return monotonic({
    name: 'bounded',
    monotonicByConstruction: true,
    describe: 'bounded out-of-orderness, bound ' + boundMs + 'ms (watermark = maxSeen - bound - 1)',
    observe(t) {
      if (t > maxSeen) maxSeen = t;
    },
    raw() {
      return maxSeen === WATERMARK_MIN ? WATERMARK_MIN : maxSeen - boundMs - 1;
    },
  });
}

/**
 * The same, without the inclusive-boundary correction. Kept selectable so the
 * README's claim about it can be reproduced rather than taken on faith.
 * @param {{boundMs: number}} opts
 */
function boundedNaiveStrategy({ boundMs }) {
  let maxSeen = WATERMARK_MIN;
  return monotonic({
    name: 'bounded-naive',
    monotonicByConstruction: true,
    describe: 'bounded out-of-orderness WITHOUT the inclusive-boundary -1, bound ' + boundMs + 'ms',
    observe(t) {
      if (t > maxSeen) maxSeen = t;
    },
    raw() {
      return maxSeen === WATERMARK_MIN ? WATERMARK_MIN : maxSeen - boundMs;
    },
  });
}

/**
 * Adaptive: the bound is the p-th percentile of the lateness actually
 * observed in a sliding sample, instead of a constant somebody guessed.
 *
 * This exists because of what the three vendored streams do to a constant. A
 * bound tuned on GitHub Archive (seconds) discards essentially all of USGS
 * (hours to weeks). The percentile strategy is not magic -- it still misses
 * the tail by construction, which is the point of choosing the percentile --
 * but it fails proportionally instead of catastrophically.
 *
 * The sample is a fixed-size ring buffer, sorted on demand. That is O(n log n)
 * per emit in the worst case; `sampleSize` is small (default 512) and the
 * engine emits once per event, so this is the deliberate cost of not needing
 * an approximate-quantile dependency.
 *
 * @param {{percentile: number, sampleSize: number, floorMs: number}} opts
 */
function percentileStrategy({ percentile, sampleSize, floorMs }) {
  let maxSeen = WATERMARK_MIN;
  /** @type {number[]} */
  const ring = new Array(sampleSize).fill(0);
  let filled = 0;
  let cursor = 0;
  return monotonic({
    name: 'percentile',
    // The bound is re-derived from a sliding sample, so maxSeen - bound - 1 can
    // fall when the sample forgets an old outlier. That is the strategy working.
    monotonicByConstruction: false,
    describe:
      'adaptive: bound = p' + Math.round(percentile * 100) + ' of the last ' + sampleSize +
      ' observed lateness values, floor ' + floorMs + 'ms',
    observe(t) {
      if (maxSeen !== WATERMARK_MIN) {
        const lateness = maxSeen - t;
        ring[cursor] = lateness > 0 ? lateness : 0;
        cursor = (cursor + 1) % sampleSize;
        if (filled < sampleSize) filled++;
      }
      if (t > maxSeen) maxSeen = t;
    },
    raw() {
      if (maxSeen === WATERMARK_MIN) return WATERMARK_MIN;
      let bound = floorMs;
      if (filled > 0) {
        const sample = ring.slice(0, filled).sort((a, b) => a - b);
        const idx = Math.min(filled - 1, Math.floor(percentile * filled));
        bound = Math.max(floorMs, sample[idx]);
      }
      return maxSeen - bound - 1;
    },
  });
}

/**
 * Punctuated: the watermark only advances when the source says it may, every
 * `everyN` events, and then only to the minimum event time seen since the last
 * punctuation. Slower and safer than `bounded`; included because a source that
 * knows its own completeness (a partitioned log with per-partition high water
 * marks) is a real and common case.
 *
 * @param {{everyN: number}} opts
 */
function punctuatedStrategy({ everyN }) {
  let maxSeen = WATERMARK_MIN;
  let minSinceMark = WATERMARK_MAX;
  let held = WATERMARK_MIN;
  let sinceMark = 0;
  return monotonic({
    name: 'punctuated',
    // The minimum of a later block can be below the minimum of an earlier one.
    monotonicByConstruction: false,
    describe: 'punctuated: advance to min(event time) of each block of ' + everyN + ' events',
    observe(t) {
      if (t > maxSeen) maxSeen = t;
      if (t < minSinceMark) minSinceMark = t;
      sinceMark++;
      if (sinceMark >= everyN) {
        // The block is complete, so nothing below its minimum is still coming
        // from within it. Minus one, again, for the inclusive boundary.
        held = minSinceMark - 1;
        minSinceMark = WATERMARK_MAX;
        sinceMark = 0;
      }
    },
    raw() {
      return held;
    },
  });
}

/**
 * PLANTED FIXTURE. The watermark advances on PROCESSING time -- here, the
 * arrival index -- and never looks at an event's timestamp at all.
 *
 * This is the classic bug, and it is classic because it works. On a stream
 * whose events arrive roughly in order at a roughly steady rate, a
 * processing-time watermark tracks the event-time watermark closely enough to
 * pass a demo, a staging soak, and often production, until the day the source
 * stalls and then catches up. Then it closes windows for data that has not
 * arrived, and the numbers are quietly wrong forever.
 *
 * `msPerEvent` is the assumed rate. Getting it wrong is not a separate bug; it
 * IS the bug, because a rate assumption is what a processing-time watermark is
 * made of.
 *
 * `originMs: null` anchors on the first event time observed and then never
 * reads an event time again -- which is precisely how this survives review. A
 * reader sees the anchor, concludes the strategy is event-time aware, and moves
 * on.
 *
 * @param {{msPerEvent: number, originMs: number|null}} opts
 */
function processingTimeStrategy({ msPerEvent, originMs }) {
  let index = -1;
  let origin = originMs;
  return monotonic({
    name: 'processing-time',
    monotonicByConstruction: true,
    describe:
      'PLANTED BUG: watermark = origin + arrivalIndex * ' + msPerEvent +
      'ms; after the anchor, event timestamps are never read again',
    observe(t, i) {
      if (origin === null) origin = t;
      index = i;
    },
    raw() {
      return index < 0 || origin === null ? WATERMARK_MIN : origin + index * msPerEvent;
    },
  });
}

/**
 * @param {object} spec
 * @param {string} spec.name
 * @param {number} [spec.boundMs]
 * @param {number} [spec.percentile]
 * @param {number} [spec.sampleSize]
 * @param {number} [spec.floorMs]
 * @param {number} [spec.everyN]
 * @param {number} [spec.msPerEvent]
 * @param {number|null} [spec.originMs]
 * @returns {WatermarkStrategy & {regressions: number, maxRegressionMs: number}}
 */
export function createWatermarkStrategy(spec) {
  switch (spec.name) {
    case 'bounded':
      return boundedStrategy({ boundMs: spec.boundMs ?? 0 });
    case 'bounded-naive':
      return boundedNaiveStrategy({ boundMs: spec.boundMs ?? 0 });
    case 'percentile':
      return percentileStrategy({
        percentile: spec.percentile ?? 0.99,
        sampleSize: spec.sampleSize ?? 512,
        floorMs: spec.floorMs ?? 0,
      });
    case 'punctuated':
      return punctuatedStrategy({ everyN: spec.everyN ?? 64 });
    case 'processing-time':
      return processingTimeStrategy({
        msPerEvent: spec.msPerEvent ?? 1,
        originMs: spec.originMs === undefined ? null : spec.originMs,
      });
    default:
      throw new RangeError(
        'unknown watermark strategy "' + spec.name + '"; known: ' + STRATEGY_NAMES.join(', ')
      );
  }
}
