// The three vendored real streams.
//
// Two claims are under test. The first is that the numbers the README quotes
// are the numbers the code produces: vendor/MEASURED.json is regenerated here
// and compared field by field against the committed copy, so a claim in the
// README can only be wrong if this test is also failing.
//
// The second is the one that took the longest to earn: a CORRECT engine
// produces ZERO defect findings against all three real streams, under every
// configuration measured -- including the configurations whose bounds are
// hopelessly wrong for the stream. A bound that is four orders of magnitude too
// small must make windows unverifiable, not make the engine look broken.

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  loadVendored, parseNdjsonStream, STREAMS, STREAM_IDS, streamMeta, VENDOR_DIR,
} from '../src/streams/vendored.js';
import { measure, CONFIGS } from '../tools/measure-streams.mjs';
import { latenessProfile } from '../src/core/oracle.js';
import { runStream } from '../src/run.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

test('all three streams load, in arrival order, with real timestamps', () => {
  assert.equal(STREAM_IDS.length, 3);
  for (const id of STREAM_IDS) {
    const events = loadVendored(id);
    assert.ok(events.length >= 1000, id + ' has only ' + events.length + ' events');
    for (const e of events) {
      assert.ok(Number.isFinite(e.t), id + ': every event needs a finite event time');
      assert.ok(e.t > Date.parse('2000-01-01'), id + ': event times must be real epoch milliseconds');
    }
  }
});

test('each stream ships a SOURCE.md that states its origin and its licence', () => {
  for (const meta of STREAMS) {
    const file = path.join(VENDOR_DIR, meta.id, 'SOURCE.md');
    const text = fs.readFileSync(file, 'utf8');
    assert.match(text, /\*\*Source\.\*\*/, meta.id + ': SOURCE.md must state the source');
    assert.match(text, /\*\*Licence\.\*\*/, meta.id + ': SOURCE.md must state the licence position');
    assert.match(text, /\*\*What is vendored\.\*\*/, meta.id + ': SOURCE.md must say what was taken');
    assert.ok(text.includes('http'), meta.id + ': SOURCE.md must carry the source URL');
  }
});

test('all three carry real lateness, and three different shapes of it', () => {
  const profiles = STREAM_IDS.map((id) => ({ id, lp: latenessProfile(loadVendored(id)) }));
  for (const { id, lp } of profiles) {
    assert.ok(lp.outOfOrder > 0, id + ' has no out-of-order events, so it proves nothing');
    assert.ok(lp.max > 0, id + ' has zero maximum lateness');
  }
  // The point of having three is that a bound tuned on one is wrong for
  // another. Assert the spread is real rather than describing it in prose.
  const maxima = profiles.map((p) => p.lp.max).sort((a, b) => a - b);
  assert.ok(
    maxima[2] / Math.max(1, maxima[0]) > 1000,
    'the three streams should differ in maximum lateness by orders of magnitude; got ' + JSON.stringify(maxima)
  );
});

test('vendor/MEASURED.json is what the code currently measures', () => {
  const committed = JSON.parse(fs.readFileSync(path.join(ROOT, 'vendor', 'MEASURED.json'), 'utf8'));
  const live = measure();
  assert.deepEqual(live, committed, 'run `npm run measure` and commit vendor/MEASURED.json');
});

test('a correct engine produces ZERO defect findings on every real stream and configuration', () => {
  let runs = 0;
  for (const meta of STREAMS) {
    const events = loadVendored(meta.id);
    for (const cfg of CONFIGS) {
      const run = runStream(events, {
        windowMs: meta.suggestedWindowMs,
        allowedLatenessMs: meta.suggestedWindowMs,
        latePolicy: 'update',
        termination: 'idle',
        watermark: cfg.watermark,
      });
      runs++;
      assert.equal(
        run.verdict.findings.length, 0,
        meta.id + ' / ' + cfg.id + ' produced findings against a correct engine: ' +
          JSON.stringify(run.verdict.findingCounts)
      );
    }
  }
  assert.equal(runs, 9);
});

test('a hopeless bound makes windows unverifiable, and every missing event is receipted', () => {
  // USGS lateness runs to weeks. A one-second bound cannot cope, and the honest
  // result is a pile of unverifiable windows with reasons -- not a pile of
  // accusations against the engine, and not a silent loss.
  const events = loadVendored('usgs');
  const run = runStream(events, {
    windowMs: 3600000, allowedLatenessMs: 3600000,
    latePolicy: 'update', watermark: { name: 'bounded', boundMs: 1000 },
  });
  assert.equal(run.verdict.findings.length, 0);
  assert.ok(run.verdict.summary.unverifiable > 100);
  assert.equal(run.verdict.reasons['lateness-bound-exceeded'] > 0, true);
  const accounted = run.result.counters.onTime + run.result.counters.lateInBound + run.result.counters.tooLate;
  assert.equal(accounted, events.length, 'every event must be accounted for');
  assert.equal(run.result.counters.discardedWithoutReceipt, 0);
  assert.equal(run.result.sideOutput.length, run.result.counters.tooLate);
});

test('the 124 retired false positives are reconstructible, and all 124 are still not defects', () => {
  // The README says an earlier version of the checker produced 124 findings
  // against a correct engine on these three streams. That number would be a
  // memory rather than a measurement if the code that produced it were gone, so
  // this test rebuilds the three retired conditions from the shipped result and
  // counts them. Each condition still HAPPENS -- what changed is the verdict
  // drawn from it. If this total ever moves, the README moves with it.
  const retired = { noPaneAfterClose: 0, unrevisedLate: 0, watermarkRegression: 0 };
  for (const meta of STREAMS) {
    const events = loadVendored(meta.id);
    for (const cfg of CONFIGS) {
      const run = runStream(events, {
        windowMs: meta.suggestedWindowMs,
        allowedLatenessMs: meta.suggestedWindowMs,
        latePolicy: 'update',
        termination: 'idle',
        watermark: cfg.watermark,
      });
      // 1. A window the watermark closed with no pane: the old checker flagged
      //    it without asking whether the missing events were receipted.
      retired.noPaneAfterClose += run.verdict.windows.filter(
        (w) => w.panes === 0 && w.reason === 'lateness-bound-exceeded'
      ).length;
      // 2. "late data but only one pane": true whenever a window's FIRST event
      //    arrived after its close, where one pane is the correct answer.
      retired.unrevisedLate += run.result.windows.filter(
        (w) => w.lateInBound > 0 && w.panes === 1
      ).length;
      // 3. A raw watermark that fell, from a strategy that never promised it
      //    would not.
      if (run.result.watermarkRegressions > 0 && !run.result.watermarkMonotonicByConstruction) {
        retired.watermarkRegression += 1;
      }
      assert.equal(run.verdict.findings.length, 0, meta.id + '/' + cfg.id);
    }
  }
  const total = retired.noPaneAfterClose + retired.unrevisedLate + retired.watermarkRegression;
  assert.deepEqual(retired, { noPaneAfterClose: 81, unrevisedLate: 40, watermarkRegression: 3 });
  assert.equal(total, 124, 'the README quotes 124; it must stay the number these conditions produce');
});

test('the inclusive-boundary 1 matters exactly where timestamps collide', () => {
  const committed = JSON.parse(fs.readFileSync(path.join(ROOT, 'vendor', 'MEASURED.json'), 'utf8'));
  for (const s of committed.streams) {
    if (s.eventsPerDistinctTime === 1) {
      assert.equal(
        s.naiveExtraLate, 0,
        s.id + ' has no colliding timestamps, so the -1 cannot change anything'
      );
    } else {
      assert.ok(
        s.naiveExtraLate > 0,
        s.id + ' has ' + s.eventsPerDistinctTime + ' events per timestamp; dropping the -1 must ' +
          'misclassify some of them'
      );
    }
  }
});

test('a corrupt vendored file is refused with the line number, not a stack trace', () => {
  assert.throws(() => parseNdjsonStream('{"t":1}\nnot json\n', 'x.ndjson'), /x\.ndjson:2: not valid JSON/);
  assert.throws(() => parseNdjsonStream('{"nope":1}\n', 'x.ndjson'), /x\.ndjson:1: every line needs/);
  assert.throws(() => parseNdjsonStream('\n\n', 'x.ndjson'), /no events found/);
});

test('an unknown stream id is refused with the list of known ones', () => {
  assert.throws(() => streamMeta('kafka'), /unknown stream "kafka".*gharchive/s);
});
