// The watermark strategies, one contract at a time.

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createWatermarkStrategy, STRATEGY_NAMES, WATERMARK_MIN,
} from '../src/core/watermark.js';

/** @param {any} spec @param {number[]} times */
function drive(spec, times) {
  const s = createWatermarkStrategy(spec);
  const out = [];
  for (let i = 0; i < times.length; i++) {
    s.observe(times[i], i);
    out.push(s.emit());
  }
  return { out, strategy: s };
}

test('before any event the watermark is below every possible event time', () => {
  const s = createWatermarkStrategy({ name: 'bounded', boundMs: 0 });
  assert.equal(s.emit(), WATERMARK_MIN);
});

test('bounded subtracts the bound AND the inclusive-boundary 1', () => {
  const { out } = drive({ name: 'bounded', boundMs: 100 }, [1000, 1200, 1100]);
  assert.deepEqual(out, [899, 1099, 1099]);
});

test('the inclusive-boundary 1 is what keeps simultaneous events on time', () => {
  // Three events sharing one timestamp, bound 0. With `maxSeen - bound` the
  // second and third would be at or below the watermark and therefore late,
  // which is absurd: they are not out of order, they are simultaneous.
  const correct = drive({ name: 'bounded', boundMs: 0 }, [5000, 5000, 5000]).out;
  const naive = drive({ name: 'bounded-naive', boundMs: 0 }, [5000, 5000, 5000]).out;
  assert.deepEqual(correct, [4999, 4999, 4999], 'the watermark stays strictly below the shared timestamp');
  assert.deepEqual(naive, [5000, 5000, 5000], 'the naive form claims the timestamp is finished');
});

test('every strategy emits a non-decreasing watermark', () => {
  const times = [1000, 5000, 2000, 9000, 3000, 9500, 4000, 20000, 6000];
  for (const name of STRATEGY_NAMES) {
    const { out } = drive(
      { name, boundMs: 500, percentile: 0.9, sampleSize: 8, floorMs: 0, everyN: 3, msPerEvent: 700 },
      times
    );
    for (let i = 1; i < out.length; i++) {
      assert.ok(out[i] >= out[i - 1], name + ' went backwards at ' + i + ': ' + out[i - 1] + ' -> ' + out[i]);
    }
  }
});

test('a fixed bound cannot regress; an adaptive one can, and says so', () => {
  const times = [1000, 50000, 2000, 51000, 3000, 52000, 4000];
  const fixed = drive({ name: 'bounded', boundMs: 100 }, times).strategy;
  assert.equal(fixed.regressions, 0);
  assert.equal(fixed.monotonicByConstruction, true);

  const adaptive = drive(
    { name: 'percentile', percentile: 0.5, sampleSize: 4, floorMs: 0 }, times
  ).strategy;
  assert.equal(adaptive.monotonicByConstruction, false);
  assert.ok(adaptive.regressions > 0, 'a sliding percentile bound must be able to shrink');
});

test('an event-time strategy never rises above the highest event time it has seen', () => {
  // This is the invariant the checker uses to catch a processing-time
  // watermark. Assert it directly here, for every honest strategy.
  const times = [1000, 1200, 1100, 9000, 2000, 9100, 30000];
  for (const name of ['bounded', 'bounded-naive', 'percentile', 'punctuated']) {
    const s = createWatermarkStrategy({ name, boundMs: 0, percentile: 0.99, sampleSize: 8, floorMs: 0, everyN: 2 });
    let maxSeen = -Infinity;
    for (let i = 0; i < times.length; i++) {
      s.observe(times[i], i);
      if (times[i] > maxSeen) maxSeen = times[i];
      assert.ok(s.emit() <= maxSeen, name + ' overshot the highest event time at ' + i);
    }
  }
});

test('processing-time DOES overshoot, which is the whole planted bug', () => {
  // Event times crawl; arrivals do not. After the anchor this strategy never
  // reads an event time again, so it walks straight past them.
  const times = [1000, 1050, 1100, 1150, 1200, 1250];
  const s = createWatermarkStrategy({ name: 'processing-time', msPerEvent: 1000 });
  let overshoots = 0;
  let maxSeen = -Infinity;
  for (let i = 0; i < times.length; i++) {
    s.observe(times[i], i);
    if (times[i] > maxSeen) maxSeen = times[i];
    if (s.emit() > maxSeen) overshoots++;
  }
  assert.ok(overshoots > 0, 'the planted bug must overshoot on a slow stream');
});

test('punctuated only advances on a block boundary', () => {
  const { out } = drive({ name: 'punctuated', everyN: 3 }, [5000, 3000, 4000, 9000, 8000, 7000]);
  // Block 1 = {5000,3000,4000}, minimum 3000, so the watermark becomes 2999 and
  // stays there until block 2 completes at {9000,8000,7000}, minimum 7000.
  assert.deepEqual(out, [WATERMARK_MIN, WATERMARK_MIN, 2999, 2999, 2999, 6999]);
});

test('an unknown strategy is refused with the list of known ones', () => {
  assert.throws(
    () => createWatermarkStrategy({ name: 'vibes' }),
    /unknown watermark strategy "vibes".*bounded/s
  );
});
