// Window assignment, checked against boundaries computed by hand.
//
// This test matters more than it looks. The engine and the oracle share exactly
// one thing -- assignWindow -- so if assignment were wrong they would be wrong
// together and every comparison between them would pass. These assertions are
// the independent check that keeps the oracle worth having.

import test from 'node:test';
import assert from 'node:assert/strict';
import { assignWindow, windowKey, parseWindowKey, compareWindows } from '../src/core/window.js';

test('boundaries are half-open: start is in, end is not', () => {
  assert.deepEqual(assignWindow(0, 1000), { start: 0, end: 1000 });
  assert.deepEqual(assignWindow(999, 1000), { start: 0, end: 1000 });
  assert.deepEqual(assignWindow(1000, 1000), { start: 1000, end: 2000 });
});

test('negative event times land in the window below, not collapsed toward zero', () => {
  // A truncating division would put -1 into [0,1000). Math.floor puts it where
  // it belongs. Pre-epoch timestamps are unusual, not invalid.
  assert.deepEqual(assignWindow(-1, 1000), { start: -1000, end: 0 });
  assert.deepEqual(assignWindow(-1000, 1000), { start: -1000, end: 0 });
  assert.deepEqual(assignWindow(-1001, 1000), { start: -2000, end: -1000 });
});

test('the offset shifts every boundary', () => {
  assert.deepEqual(assignWindow(0, 1000, 250), { start: -750, end: 250 });
  assert.deepEqual(assignWindow(250, 1000, 250), { start: 250, end: 1250 });
  assert.deepEqual(assignWindow(1249, 1000, 250), { start: 250, end: 1250 });
});

test('a real epoch timestamp lands on a real hour boundary', () => {
  // 2026-01-15T03:17:42Z inside the 03:00 hour.
  const t = Date.parse('2026-01-15T03:17:42Z');
  const w = assignWindow(t, 3600000);
  assert.equal(new Date(w.start).toISOString(), '2026-01-15T03:00:00.000Z');
  assert.equal(new Date(w.end).toISOString(), '2026-01-15T04:00:00.000Z');
});

test('window keys round-trip, including negative starts', () => {
  for (const w of [{ start: 0, end: 1000 }, { start: -60000, end: 0 }, { start: -120000, end: -60000 }]) {
    assert.deepEqual(parseWindowKey(windowKey(w)), w);
  }
});

test('a malformed window key is refused rather than guessed at', () => {
  assert.throws(() => parseWindowKey('not-a-key'), /not a window key/);
  assert.throws(() => parseWindowKey(''), /not a window key/);
});

test('bad window sizes are refused', () => {
  assert.throws(() => assignWindow(0, 0), /positive integer/);
  assert.throws(() => assignWindow(0, -1000), /positive integer/);
  assert.throws(() => assignWindow(0, 1000.5), /positive integer/);
  assert.throws(() => assignWindow(NaN, 1000), /must be finite/);
});

test('windows sort by start then end', () => {
  const list = [{ start: 10, end: 20 }, { start: 0, end: 10 }, { start: 0, end: 5 }];
  list.sort(compareWindows);
  assert.deepEqual(list, [{ start: 0, end: 5 }, { start: 0, end: 10 }, { start: 10, end: 20 }]);
});
