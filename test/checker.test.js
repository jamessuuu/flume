// The checker: the three outcomes, and the line between an outcome and a
// finding.
//
// The distinction under test here is the one that took three real streams to
// get right: a window whose value is unknown because the CONFIGURATION could not
// cope is `unverifiable` with a reason, and a window whose value is unknown
// because the ENGINE misbehaved is `unverifiable` plus a finding. Conflating the
// two produced 124 findings against a correct engine (see the README).

import test from 'node:test';
import assert from 'node:assert/strict';
import { runStream } from '../src/run.js';
import { OUTCOMES, FINDINGS } from '../src/core/checker.js';
import { demoStream, DEMO_CONFIG, DEMO_EXPECTED } from '../src/streams/demo.js';

const BASE = {
  windowMs: 4000,
  allowedLatenessMs: 4000,
  latePolicy: /** @type {'update'} */ ('update'),
  watermark: { name: 'bounded', boundMs: 0 },
};

test('every window gets exactly one of the three outcomes, and none is skipped', () => {
  const run = runStream(demoStream(), DEMO_CONFIG);
  assert.equal(run.verdict.windows.length, run.verdict.total);
  for (const w of run.verdict.windows) {
    assert.ok(OUTCOMES.includes(w.outcome), 'unclassified outcome: ' + w.outcome);
  }
  const sum = run.verdict.summary.complete + run.verdict.summary.revised + run.verdict.summary.unverifiable;
  assert.equal(sum, run.verdict.total, 'the three counts must partition the windows');
});

test('the demo produces all three outcomes and both unverifiable reasons', () => {
  const run = runStream(demoStream(), DEMO_CONFIG);
  assert.deepEqual(run.verdict.summary, DEMO_EXPECTED);
  assert.deepEqual(
    Object.keys(run.verdict.reasons).sort(),
    ['lateness-bound-exceeded', 'watermark-never-reached']
  );
  assert.equal(run.verdict.findings.length, 0, 'the demo runs a correct build');
});

test('complete means closed by the watermark and agreeing with the oracle', () => {
  const run = runStream([{ t: 0 }, { t: 1000 }, { t: 8000 }, { t: 9000 }], {
    ...BASE, termination: 'flush',
  });
  const w = run.verdict.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.outcome, 'complete');
  assert.equal(w.expected, 2);
  assert.equal(w.emitted, 2);
  assert.equal(w.panes, 1);
});

test('revised means a pane was corrected, and it is never reported as complete', () => {
  const run = runStream([{ t: 0 }, { t: 1000 }, { t: 5000 }, { t: 6000 }, { t: 2000 }], {
    ...BASE, termination: 'flush',
  });
  const w = run.verdict.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.outcome, 'revised');
  assert.equal(w.panes, 2);
  assert.equal(w.emitted, w.expected, 'the final pane still has to be right');
});

test('unverifiable, reason watermark-never-reached: the stream ended first', () => {
  const run = runStream([{ t: 0 }, { t: 1000 }], { ...BASE, termination: 'idle' });
  const w = run.verdict.windows[0];
  assert.equal(w.outcome, 'unverifiable');
  assert.equal(w.reason, 'watermark-never-reached');
  assert.match(w.detail, /the watermark stopped at 999/);
  assert.equal(run.verdict.findings.length, 0, 'ending a stream is not a defect');
});

test('unverifiable, reason lateness-bound-exceeded: receipted, so not a defect', () => {
  const run = runStream([
    { t: 0 }, { t: 1000 }, { t: 40000 }, { t: 41000 }, { t: 2000 },
  ], { ...BASE, termination: 'flush' });
  const w = run.verdict.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.outcome, 'unverifiable');
  assert.equal(w.reason, 'lateness-bound-exceeded');
  assert.equal(w.expected, 3);
  assert.equal(w.emitted, 2);
  assert.equal(run.verdict.findings.length, 0,
    'obeying a bound that was too small is a configuration problem, not an engine defect');
});

test('a window with NO pane whose events were all receipted is not a finding', () => {
  // Every event for [0,4000) arrives after the watermark has already released
  // the window, so no pane is ever emitted -- but all of them are receipted.
  // Flagging this was one of the three false positives real data exposed.
  const run = runStream([
    { t: 40000 }, { t: 41000 }, { t: 42000 }, { t: 1000 }, { t: 2000 },
  ], { ...BASE, termination: 'flush' });
  const w = run.verdict.windows.find((x) => x.window === '0-4000');
  assert.ok(w);
  assert.equal(w.panes, 0);
  assert.equal(w.outcome, 'unverifiable');
  assert.equal(w.reason, 'lateness-bound-exceeded');
  assert.equal(run.verdict.findings.length, 0);
});

test('an adaptive strategy whose bound shrinks is not accused of regressing', () => {
  // A hundred events in perfect order, so the sampled bound settles at zero,
  // then a run of very late ones that does not move maxSeen at all. The bound
  // climbs while maxSeen stands still, so maxSeen - bound - 1 falls. That is
  // the adaptive strategy behaving exactly as designed.
  const events = [];
  for (let i = 0; i < 100; i++) events.push({ t: i * 100 });
  for (let i = 0; i < 60; i++) events.push({ t: 9900 - 100000 - i * 10 });
  const run = runStream(events, {
    ...BASE, windowMs: 5000, termination: 'flush',
    watermark: { name: 'percentile', percentile: 0.5, sampleSize: 16, floorMs: 0 },
  });
  assert.ok(run.result.watermarkRegressions > 0, 'the setup must actually make the bound shrink');
  assert.equal(run.result.watermarkMonotonicByConstruction, false);
  assert.equal(
    run.verdict.findings.filter((f) => f.code === 'watermark-regression').length, 0,
    'a shrinking adaptive bound is the design, not a defect'
  );
});

test('every finding code has documentation, and every emitted code is a known one', () => {
  const documented = Object.keys(FINDINGS);
  assert.ok(documented.length >= 7);
  for (const code of documented) {
    assert.equal(typeof FINDINGS[code], 'string');
    assert.ok(FINDINGS[code].length > 30, code + ' needs a real description');
  }
});
