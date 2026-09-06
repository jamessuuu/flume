// The engine: when a window closes, what a late event does to it, and what the
// end of a stream means.

import test from 'node:test';
import assert from 'node:assert/strict';
import { runEngine, LATE_POLICIES, TERMINATIONS } from '../src/core/engine.js';

/** A log where one event, at `lateT`, arrives last. */
function logWithOneLateEvent(lateT = 500) {
  return [
    { t: 0 }, { t: 1000 }, { t: 2000 }, { t: 3000 },
    { t: 4000 }, { t: 5000 }, { t: 6000 }, { t: 7000 },
    { t: lateT },
  ];
}

const BASE = {
  windowMs: 4000,
  allowedLatenessMs: 4000,
  watermark: { name: 'bounded', boundMs: 0 },
};

test('a window fires exactly once when the watermark passes its end', () => {
  const r = runEngine([{ t: 0 }, { t: 1000 }, { t: 4000 }, { t: 5000 }], {
    ...BASE, latePolicy: 'update',
  });
  const first = r.windows.find((w) => w.window === '0-4000');
  assert.ok(first);
  assert.equal(first.panes, 1);
  assert.equal(first.emitted, 2);
  // maxSeen is 5000 when it fires, so the watermark is 4999.
  assert.equal(first.firedAtWatermark, 4999);
});

test('under `update`, a late event inside the bound retracts and re-emits', () => {
  const r = runEngine(logWithOneLateEvent(500), { ...BASE, latePolicy: 'update' });
  const w = r.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.panes, 2);
  assert.equal(w.emitted, 5);
  const panes = r.panes.filter((p) => p.window === '0-4000');
  assert.equal(panes[0].kind, 'initial');
  assert.equal(panes[0].count, 4);
  assert.equal(panes[1].kind, 'revision');
  assert.equal(panes[1].retracts, 4, 'the revision must say what it replaces');
  assert.equal(panes[1].count, 5);
  assert.equal(r.counters.revisionsEmitted, 1);
});

test('under `drop`, the same event is counted and discarded, never silently', () => {
  const r = runEngine(logWithOneLateEvent(500), { ...BASE, latePolicy: 'drop' });
  const w = r.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.panes, 1, 'no revision under the drop policy');
  assert.equal(r.counters.droppedInBound, 1);
  assert.equal(r.counters.discardedWithoutReceipt, 0, 'a correct build never discards without a receipt');
});

test('under `side-output`, the event is routed out with a reason attached', () => {
  const r = runEngine(logWithOneLateEvent(500), { ...BASE, latePolicy: 'side-output' });
  assert.equal(r.sideOutput.length, 1);
  assert.equal(r.sideOutput[0].t, 500);
  assert.match(r.sideOutput[0].reason, /within allowed lateness/);
});

test('past the allowed lateness an event is receipted under every policy', () => {
  for (const latePolicy of LATE_POLICIES) {
    const events = [
      { t: 0 }, { t: 1000 }, { t: 2000 }, { t: 3000 },
      { t: 20000 }, { t: 21000 },
      { t: 500 },
    ];
    const r = runEngine(events, { ...BASE, latePolicy });
    assert.equal(r.counters.tooLate, 1, latePolicy);
    assert.equal(r.sideOutput.length, 1, latePolicy + ': a too-late event always gets a receipt');
    assert.match(r.sideOutput[0].reason, /beyond allowed lateness/, latePolicy);
    assert.equal(r.counters.discardedWithoutReceipt, 0, latePolicy);
  }
});

test('`idle` leaves the watermark where it stopped; `flush` runs it to +infinity', () => {
  const events = [{ t: 0 }, { t: 1000 }, { t: 2000 }];
  const idle = runEngine(events, { ...BASE, latePolicy: 'update', termination: 'idle' });
  assert.equal(idle.finalWatermark, 1999);
  assert.equal(idle.windows[0].fired, false, 'the only window never closed');
  assert.equal(idle.panes.length, 0);

  const flushed = runEngine(events, { ...BASE, latePolicy: 'update', termination: 'flush' });
  assert.equal(flushed.finalWatermark, Number.POSITIVE_INFINITY);
  assert.equal(flushed.windows[0].fired, true);
  assert.equal(flushed.windows[0].emitted, 3);
});

test('a window whose first event arrives after its close emits an initial pane, not a revision', () => {
  // The watermark ran past [0,4000) before anything landed in it. The pane that
  // results is that window's first output; calling it a revision would break the
  // retraction chain, because there is nothing to retract.
  const r = runEngine([{ t: 8000 }, { t: 9000 }, { t: 1000 }], {
    ...BASE, allowedLatenessMs: 8000, latePolicy: 'update',
  });
  const w = r.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.openedAfterClose, true);
  assert.equal(w.panes, 1);
  assert.equal(r.panes.find((p) => p.window === '0-4000')?.kind, 'initial');
  assert.equal(r.counters.openedAfterClose, 1);
});

test('the engine counts every event exactly once', () => {
  const events = logWithOneLateEvent(500);
  for (const latePolicy of LATE_POLICIES) {
    const r = runEngine(events, { ...BASE, latePolicy });
    const accounted = r.counters.onTime + r.counters.lateInBound + r.counters.tooLate +
      r.counters.discardedWithoutReceipt;
    assert.equal(accounted, events.length, latePolicy);
  }
});

test('bad configuration is refused before anything runs', () => {
  assert.throws(() => runEngine([], { latePolicy: 'ignore' }), /unknown late policy/);
  assert.throws(() => runEngine([], { termination: 'someday' }), /unknown termination/);
  assert.throws(() => runEngine([], { allowedLatenessMs: -1 }), /non-negative integer/);
  assert.throws(() => runEngine([{ t: NaN }], BASE), /non-finite event time/);
});

test('an empty log produces an empty, well-formed result', () => {
  const r = runEngine([], { ...BASE, latePolicy: 'update' });
  assert.deepEqual(r.windows, []);
  assert.deepEqual(r.panes, []);
  assert.equal(r.observed.events, 0);
  assert.equal(r.observed.minEventTime, null);
  assert.equal(TERMINATIONS.includes(r.config.termination), true);
});
