// The batch oracle and the lateness profile.
//
// The oracle is deliberately trivial. That is its value: a ground truth complex
// enough to be wrong would not be a ground truth.

import test from 'node:test';
import assert from 'node:assert/strict';
import { batchWindows, latenessProfile } from '../src/core/oracle.js';

test('the oracle groups by event time and ignores arrival order entirely', () => {
  const inOrder = [{ t: 0 }, { t: 1000 }, { t: 2000 }, { t: 5000 }];
  const shuffled = [{ t: 5000 }, { t: 2000 }, { t: 0 }, { t: 1000 }];
  const a = batchWindows(inOrder, { windowMs: 4000 });
  const b = batchWindows(shuffled, { windowMs: 4000 });
  assert.deepEqual(a.windows, b.windows);
  assert.deepEqual(a.windows, [
    { window: '0-4000', start: 0, end: 4000, count: 3 },
    { window: '4000-8000', start: 4000, end: 8000, count: 1 },
  ]);
});

test('an empty log has no windows and a total of zero', () => {
  const o = batchWindows([], { windowMs: 1000 });
  assert.deepEqual(o.windows, []);
  assert.equal(o.total, 0);
});

test('lateness is measured against the maximum seen SO FAR, not the maximum overall', () => {
  // The third event is 4000ms behind the highest event time seen when it
  // arrived. The fourth is in order and therefore not late at all, even though
  // it is below the eventual maximum.
  const lp = latenessProfile([{ t: 0 }, { t: 5000 }, { t: 1000 }, { t: 6000 }]);
  assert.equal(lp.events, 4);
  assert.equal(lp.outOfOrder, 1);
  assert.equal(lp.max, 4000);
});

test('a perfectly ordered log has exactly zero lateness', () => {
  const lp = latenessProfile([{ t: 0 }, { t: 1 }, { t: 2 }, { t: 3 }]);
  assert.equal(lp.outOfOrder, 0);
  assert.equal(lp.outOfOrderPct, 0);
  assert.equal(lp.max, 0);
  assert.equal(lp.p99, 0);
});

test('equal timestamps are not out of order', () => {
  const lp = latenessProfile([{ t: 100 }, { t: 100 }, { t: 100 }]);
  assert.equal(lp.outOfOrder, 0, 'simultaneous is not late');
});

test('percentiles are ordered and bounded by the maximum', () => {
  const events = [];
  for (let i = 0; i < 1000; i++) events.push({ t: i % 7 === 0 ? i - 500 : i });
  const lp = latenessProfile(events);
  assert.ok(lp.p50 <= lp.p90);
  assert.ok(lp.p90 <= lp.p99);
  assert.ok(lp.p99 <= lp.p999);
  assert.ok(lp.p999 <= lp.max);
});
