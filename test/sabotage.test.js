// The fixture that carries the project.
//
// Three of flume's planted bugs break the engine, and the checker catches them.
// That proves the checker FIRES. It says nothing about the complete/revised
// distinction being real, because a checker that called every window `complete`
// would still have caught all three -- they show up as findings, not as
// outcomes.
//
// This fixture breaks the checker itself, at exactly the point where it decides
// whether a corrected window gets to keep the word "complete". If these tests
// ever pass with the flag OFF, then `revised` is decoration and every number
// this project prints is worth less than it looks.

import test from 'node:test';
import assert from 'node:assert/strict';
import { runStream } from '../src/run.js';
import { fixtureStream, FIXTURE_CONFIG, runSabotageFixture } from '../src/fixtures.js';
import { demoStream, DEMO_CONFIG } from '../src/streams/demo.js';
import { loadVendored } from '../src/streams/vendored.js';
import { parseBuildFlags } from '../src/bugs.js';

const SABOTAGE = parseBuildFlags('checker-revised-as-complete');

test('the sabotaged checker reports a window it knows was revised as complete', () => {
  const events = demoStream();
  const honest = runStream(events, DEMO_CONFIG);
  assert.equal(honest.verdict.summary.revised, 1, 'the demo must contain a real revision to hide');
  assert.equal(honest.verdict.summary.complete, 1);

  const lying = runStream(events, { ...DEMO_CONFIG, flags: SABOTAGE });
  assert.equal(lying.verdict.summary.revised, 0, 'the sabotage did not hide the revision');
  assert.equal(lying.verdict.summary.complete, 2, 'the hidden revision must reappear as complete');
  assert.equal(
    lying.verdict.summary.unverifiable, honest.verdict.summary.unverifiable,
    'the sabotage must not touch the third outcome'
  );
});

test('the engine is untouched: only the label changed, not the panes', () => {
  // If the sabotage changed engine behaviour it would be a different bug, and
  // "the checker is lying" would not be what this fixture demonstrates.
  const events = demoStream();
  const honest = runStream(events, DEMO_CONFIG);
  const lying = runStream(events, { ...DEMO_CONFIG, flags: SABOTAGE });
  assert.deepEqual(lying.result.panes, honest.result.panes);
  assert.deepEqual(lying.result.counters, honest.result.counters);
  const revisedWindow = honest.verdict.windows.find((w) => w.outcome === 'revised');
  const sameWindow = lying.verdict.windows.find((w) => w.window === revisedWindow?.window);
  assert.ok(revisedWindow && sameWindow);
  assert.equal(sameWindow.panes, 2, 'the pane count still says two; only the outcome lies');
  assert.equal(sameWindow.outcome, 'complete');
});

test('across 40 generated streams the lie is always an exact relabel', () => {
  const r = runSabotageFixture(Array.from({ length: 40 }, (_v, i) => i + 1));
  assert.equal(r.seedsWithRevisions, 40, 'every seed must produce revisions for this to test anything');
  assert.ok(r.revisedHidden > 100, 'only ' + r.revisedHidden + ' revised windows were hidden');
  assert.equal(r.allExactRelabels, true, 'the sabotage moved something other than revised -> complete');
});

test('it lies about real streams too, not only generated ones', () => {
  const events = loadVendored('wikimedia');
  const config = {
    windowMs: 10000, allowedLatenessMs: 10000,
    latePolicy: /** @type {'update'} */ ('update'),
    watermark: { name: 'bounded', boundMs: 1000 },
  };
  const honest = runStream(events, config);
  assert.ok(honest.verdict.summary.revised > 0, 'the real stream must contain revisions to hide');
  const lying = runStream(events, { ...config, flags: SABOTAGE });
  assert.equal(lying.verdict.summary.revised, 0);
  assert.equal(
    lying.verdict.summary.complete,
    honest.verdict.summary.complete + honest.verdict.summary.revised
  );
});

test('the sabotage cannot promote a window that was never right', () => {
  // It only relabels windows whose final value already matches the oracle. A
  // blanket "return complete" would be a cruder lie and a weaker fixture, so
  // this pins the fixture to the one branch it is aimed at.
  const events = fixtureStream(5);
  const honest = runStream(events, FIXTURE_CONFIG);
  const lying = runStream(events, { ...FIXTURE_CONFIG, flags: SABOTAGE });
  assert.equal(
    lying.verdict.summary.unverifiable, honest.verdict.summary.unverifiable,
    'unverifiable windows must be untouched'
  );
  for (const w of honest.verdict.windows.filter((x) => x.outcome === 'unverifiable')) {
    const after = lying.verdict.windows.find((x) => x.window === w.window);
    assert.equal(after?.outcome, 'unverifiable', w.window + ' was promoted and should not have been');
  }
});
