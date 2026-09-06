// The streaming engine.
//
// One pass over an event log. Every event carries an event time; the engine's
// only notion of processing time is the event's position in the log. That is a
// design decision, not a shortcut: an engine that reads a clock cannot be
// replayed, and an engine that cannot be replayed cannot be checked. There is
// no Date.now, no Math.random and no setTimeout anywhere under src/core --
// test/determinism.test.js fails the build if one appears.
//
// The aggregate is a count per window. Not because counting is interesting,
// but because the ground truth for a count is one line of code (src/core/
// oracle.js), so a disagreement between the streamed result and the batch
// result can only be the windowing, never the arithmetic.
//
// The order of operations per event matters and is the usual source of
// off-by-one arguments, so it is written out here:
//
//   1. judge the event against the CURRENT watermark (which does not yet
//      include this event -- a watermark emitted after seeing e cannot make e
//      late retroactively)
//   2. apply it: on time, late-in-bound, or too late
//   3. show it to the watermark strategy
//   4. emit the new watermark
//   5. fire every window the watermark has now closed, then garbage-collect
//      every window whose allowed lateness has now elapsed

import { assignWindow, windowKey } from './window.js';
import { createWatermarkStrategy, WATERMARK_MIN, WATERMARK_MAX } from './watermark.js';

/**
 * @typedef {object} FlumeEvent
 * @property {number} t event time, integer ms
 * @property {string} [key] optional partition key, carried through, not aggregated on
 * @property {number|null} [p] optional real processing timestamp, for display only
 *   -- the engine never reads it
 * @property {string} [k] optional label carried by the vendored streams
 */

/**
 * @typedef {object} Pane
 * @property {string} window
 * @property {number} seq 1 for the initial pane, 2+ for revisions
 * @property {'initial'|'revision'} kind
 * @property {number} count the value emitted
 * @property {number|null} retracts the count this pane supersedes, null for the initial pane
 * @property {number} watermark the watermark that caused this pane
 * @property {number} atIndex the log position at which it fired
 */

/** The three late-data policies. Exactly these; there is no fourth. */
export const LATE_POLICIES = ['drop', 'update', 'side-output'];

/** How the run ends. See the README on why this is a choice and not a default. */
export const TERMINATIONS = ['idle', 'flush'];

export const DEFAULTS = {
  windowMs: 60000,
  offsetMs: 0,
  allowedLatenessMs: 0,
  latePolicy: /** @type {'drop'|'update'|'side-output'} */ ('update'),
  termination: /** @type {'idle'|'flush'} */ ('idle'),
};

/**
 * @typedef {object} EngineOptions
 * @property {number} [windowMs]
 * @property {number} [offsetMs]
 * @property {number} [allowedLatenessMs]
 * @property {'drop'|'update'|'side-output'} [latePolicy]
 * @property {'idle'|'flush'} [termination]
 * @property {object} [watermark] strategy spec, see createWatermarkStrategy
 * @property {object} [flags] planted-bug build flags, see src/bugs.js
 */

/**
 * @param {FlumeEvent[]} events in arrival order; index IS processing order
 * @param {EngineOptions} [options]
 */
export function runEngine(events, options = {}) {
  const windowMs = options.windowMs ?? DEFAULTS.windowMs;
  const offsetMs = options.offsetMs ?? DEFAULTS.offsetMs;
  const allowedLatenessMs = options.allowedLatenessMs ?? DEFAULTS.allowedLatenessMs;
  const latePolicy = options.latePolicy ?? DEFAULTS.latePolicy;
  const termination = options.termination ?? DEFAULTS.termination;
  const flags = options.flags ?? {};

  if (!LATE_POLICIES.includes(latePolicy)) {
    throw new RangeError('unknown late policy "' + latePolicy + '"; known: ' + LATE_POLICIES.join(', '));
  }
  if (!TERMINATIONS.includes(termination)) {
    throw new RangeError('unknown termination "' + termination + '"; known: ' + TERMINATIONS.join(', '));
  }
  if (!Number.isInteger(allowedLatenessMs) || allowedLatenessMs < 0) {
    throw new RangeError('allowed lateness must be a non-negative integer, got ' + allowedLatenessMs);
  }

  // PLANTED FIXTURE `processing-time-watermark`: whatever strategy was asked
  // for, use the one that never reads an event time. The rate is taken from the
  // request so the fixture can be tuned to the stream it is planted in, which
  // is what makes it realistic -- a rate assumption that is roughly right is
  // exactly what lets this bug survive.
  const requested = options.watermark ?? { name: 'bounded', boundMs: 0 };
  const strategy = createWatermarkStrategy(
    flags.processingTimeWatermark
      ? { name: 'processing-time', msPerEvent: requested.msPerEvent ?? 1, originMs: requested.originMs }
      : requested
  );

  /**
   * @typedef {object} WindowState
   * @property {number} start
   * @property {number} end
   * @property {string} key
   * @property {number} count
   * @property {number} lastEmitted the count of the most recent pane, -1 before any
   * @property {number} paneCount
   * @property {boolean} fired
   * @property {boolean} collected
   * @property {number} onTime
   * @property {number} lateInBound
   * @property {number} tooLate
   * @property {number|null} firedAtWatermark
   * @property {number|null} collectedAtWatermark
   * @property {number} firstIndex
   * @property {number} lastIndex
   * @property {boolean} openedAfterClose
   */

  /** @type {Map<string, WindowState>} */
  const windows = new Map();
  /** @type {Pane[]} */
  const panes = [];
  /** @type {{index:number, t:number, window:string, reason:string, latenessMs:number}[]} */
  const sideOutput = [];
  /** @type {{index:number, watermark:number}[]} */
  const trace = [];

  const counters = {
    total: 0,
    onTime: 0,
    lateInBound: 0,
    tooLate: 0,
    /** in-bound late events discarded because the policy said to; counted, not silent */
    droppedInBound: 0,
    /** events the engine threw away with neither a counter nor a receipt. A
     *  correct build can never increment this; the `silent-drop` fixture can. */
    discardedWithoutReceipt: 0,
    revisionsEmitted: 0,
    /** windows whose first event arrived after the watermark had passed their end */
    openedAfterClose: 0,
  };

  let watermark = WATERMARK_MIN;
  let maxObservedLateness = 0;
  // A watermark derived from observed event times can never exceed the highest
  // event time seen so far: every honest strategy computes maxSeen minus a
  // non-negative bound. A watermark that overshoots is therefore not a function
  // of the data at all, whatever its author believed. This is the structural
  // signature of the processing-time bug, and it is exact rather than
  // statistical -- which matters, because the statistical signature (a high
  // rate of events arriving below the watermark) is also what a correctly
  // implemented bound that is simply too small for the stream produces, and
  // those two must never be confused.
  let watermarkOvershoots = 0;
  let maxOvershootMs = 0;
  let observedMaxEventTime = WATERMARK_MIN;
  let observedMinEventTime = WATERMARK_MAX;

  /** @param {number} start @param {number} end @param {number} index */
  function ensureWindow(start, end, index) {
    const k = windowKey({ start, end });
    let w = windows.get(k);
    if (!w) {
      w = {
        start, end, key: k,
        count: 0, lastEmitted: -1, paneCount: 0,
        fired: false, collected: false,
        onTime: 0, lateInBound: 0, tooLate: 0,
        firedAtWatermark: null, collectedAtWatermark: null,
        firstIndex: index, lastIndex: index, openedAfterClose: false,
      };
      windows.set(k, w);
    }
    return w;
  }

  /** @param {WindowState} w @param {'initial'|'revision'} kind @param {number} index */
  function emitPane(w, kind, index) {
    w.paneCount++;
    panes.push({
      window: w.key,
      seq: w.paneCount,
      kind,
      count: w.count,
      retracts: kind === 'revision' ? w.lastEmitted : null,
      watermark,
      atIndex: index,
    });
    w.lastEmitted = w.count;
    if (kind === 'revision') counters.revisionsEmitted++;
  }

  /**
   * Close every window the watermark has passed, then release every window
   * whose allowed lateness has elapsed. Both loops walk the live windows, so
   * this is O(live windows) per event; `windows` only holds windows that have
   * not been collected yet, which for a well-behaved stream is a handful.
   *
   * @param {number} index
   */
  function advance(index) {
    for (const w of windows.values()) {
      if (!w.fired && watermark >= w.end) {
        w.fired = true;
        w.firedAtWatermark = watermark;
        emitPane(w, 'initial', index);
      }
    }
    for (const w of windows.values()) {
      if (!w.fired || w.collected) continue;
      // PLANTED FIXTURE `close-without-lateness`: collect the moment the
      // window fires, so an in-bound late event finds no state to update and
      // the allowed-lateness setting is decoration.
      const releaseAt = flags.closeWithoutLateness ? w.end : w.end + allowedLatenessMs;
      if (watermark >= releaseAt) {
        w.collected = true;
        w.collectedAtWatermark = watermark;
      }
    }
  }

  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    const t = ev.t;
    if (!Number.isFinite(t)) {
      throw new RangeError('event ' + i + ' has a non-finite event time: ' + t);
    }
    counters.total++;
    if (t > observedMaxEventTime) observedMaxEventTime = t;
    if (t < observedMinEventTime) observedMinEventTime = t;

    const win = assignWindow(t, windowMs, offsetMs);
    const k = windowKey(win);
    const existing = windows.get(k);
    const latenessMs = observedMaxEventTime === WATERMARK_MIN ? 0 : Math.max(0, observedMaxEventTime - t);
    if (latenessMs > maxObservedLateness) maxObservedLateness = latenessMs;

    // Step 1 + 2: judge against the CURRENT watermark, then apply.
    const windowIsClosed = watermark >= win.end;
    const stateIsGone = existing ? existing.collected : watermark >= win.end + allowedLatenessMs;

    if (windowIsClosed && stateIsGone) {
      counters.tooLate++;
      if (flags.silentDrop) {
        // PLANTED FIXTURE `silent-drop`: no receipt, no counter. Undo the
        // counter increment so the event leaves no trace at all, which is what
        // makes it undetectable from the output -- and what the checker has to
        // catch by comparing against the oracle instead.
        counters.tooLate--;
        counters.discardedWithoutReceipt++;
      } else {
        if (existing) existing.tooLate++;
        sideOutput.push({
          index: i,
          t,
          window: k,
          reason: 'beyond allowed lateness: watermark ' + watermark + ' >= windowEnd ' + win.end +
            ' + allowedLateness ' + allowedLatenessMs,
          latenessMs: watermark - win.end,
        });
      }
    } else if (windowIsClosed) {
      const w = ensureWindow(win.start, win.end, i);
      w.lastIndex = i;
      w.lateInBound++;
      counters.lateInBound++;
      if (latePolicy === 'update') {
        w.count++;
        // A window can receive its FIRST event after the watermark has already
        // passed its end -- the watermark overran a window that had no data yet.
        // The pane that results is that window's initial output, not a revision
        // of something never emitted, and calling it a revision would break the
        // retraction chain. It is recorded as opened-after-close so the report
        // can say the watermark overran rather than pretending it did not.
        if (w.paneCount === 0) {
          w.fired = true;
          w.firedAtWatermark = watermark;
          w.openedAfterClose = true;
          counters.openedAfterClose++;
          emitPane(w, 'initial', i);
        } else {
          emitPane(w, 'revision', i);
        }
      } else if (latePolicy === 'side-output') {
        sideOutput.push({
          index: i,
          t,
          window: k,
          reason: 'late but within allowed lateness; policy is side-output',
          latenessMs: watermark - win.end,
        });
      } else {
        counters.droppedInBound++;
      }
    } else {
      const w = ensureWindow(win.start, win.end, i);
      w.lastIndex = i;
      w.count++;
      w.onTime++;
      counters.onTime++;
    }

    // Steps 3-5.
    strategy.observe(t, i);
    const next = strategy.emit();
    if (next !== watermark) {
      watermark = next;
      trace.push({ index: i, watermark });
    }
    if (Number.isFinite(watermark) && watermark > observedMaxEventTime) {
      watermarkOvershoots++;
      const over = watermark - observedMaxEventTime;
      if (over > maxOvershootMs) maxOvershootMs = over;
    }
    advance(i);
  }

  // End of stream. `idle` means we stopped watching a stream that continues:
  // the watermark stays where it stopped and anything it never reached is
  // honestly unknown. `flush` means the source is exhausted and said so, which
  // licenses a final watermark of +infinity.
  if (termination === 'flush') {
    watermark = WATERMARK_MAX;
    trace.push({ index: events.length, watermark });
    advance(events.length);
  }

  /** @type {any[]} */
  const windowList = [...windows.values()]
    .sort((a, b) => (a.start - b.start) || (a.end - b.end))
    .map((w) => ({
      window: w.key,
      start: w.start,
      end: w.end,
      count: w.count,
      emitted: w.paneCount > 0 ? w.lastEmitted : null,
      panes: w.paneCount,
      fired: w.fired,
      collected: w.collected,
      openedAfterClose: w.openedAfterClose,
      onTime: w.onTime,
      lateInBound: w.lateInBound,
      tooLate: w.tooLate,
      firedAtWatermark: w.firedAtWatermark,
      collectedAtWatermark: w.collectedAtWatermark,
    }));

  return {
    config: {
      windowMs, offsetMs, allowedLatenessMs, latePolicy, termination,
      watermark: strategy.name,
      watermarkDescribe: strategy.describe,
      flags: Object.keys(flags).filter((f) => flags[f]).sort(),
    },
    windows: windowList,
    panes,
    sideOutput,
    trace,
    counters,
    finalWatermark: watermark,
    watermarkMonotonicByConstruction: strategy.monotonicByConstruction,
    watermarkRegressions: strategy.regressions,
    watermarkOvershoots,
    maxWatermarkOvershootMs: maxOvershootMs,
    maxWatermarkRegressionMs: strategy.maxRegressionMs,
    observed: {
      events: events.length,
      minEventTime: events.length ? observedMinEventTime : null,
      maxEventTime: events.length ? observedMaxEventTime : null,
      maxLatenessMs: maxObservedLateness,
    },
  };
}

/** @typedef {ReturnType<typeof runEngine>} EngineResult */
