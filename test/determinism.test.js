// Determinism, asserted rather than promised.
//
// Two halves. The first replays real and generated logs and compares the whole
// serialised run byte for byte -- panes, side output, watermark trace, verdict,
// everything. The second reads the source of every module under src/core and
// fails if a clock, a random number or a timer has appeared in it, because a
// replay test can only catch nondeterminism that happens to differ on the run
// it was given, and a grep catches the class.

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runStream, serialiseRun } from '../src/run.js';
import { syntheticStream } from '../src/streams/synthetic.js';
import { loadVendored, STREAM_IDS } from '../src/streams/vendored.js';
import { sfc32 } from '../src/core/rng.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

const CONFIG = {
  windowMs: 10000,
  allowedLatenessMs: 5000,
  latePolicy: /** @type {'update'} */ ('update'),
  watermark: { name: 'bounded', boundMs: 2000 },
};

test('replaying a generated log produces byte-identical output', () => {
  const events = syntheticStream({
    seed: 4711, events: 500, stepMs: 1000, lateProb: 0.25, maxDelaySlots: 25,
    burstAt: 200, burstCount: 100, burstSpanMs: 5000,
  });
  const a = serialiseRun(runStream(events, CONFIG));
  const b = serialiseRun(runStream(events, CONFIG));
  assert.equal(a, b);
  assert.ok(a.length > 10000, 'the comparison has to be over a substantial output to mean anything');
});

test('replaying every vendored real stream produces byte-identical output', () => {
  for (const id of STREAM_IDS) {
    const events = loadVendored(id);
    const a = serialiseRun(runStream(events, CONFIG));
    const b = serialiseRun(runStream(events, CONFIG));
    assert.equal(a, b, id + ' did not replay identically');
  }
});

test('the same seed regenerates the same stream; a different seed does not', () => {
  const spec = { events: 200, stepMs: 1000, lateProb: 0.3, maxDelaySlots: 10 };
  assert.deepEqual(syntheticStream({ ...spec, seed: 9 }), syntheticStream({ ...spec, seed: 9 }));
  assert.notDeepEqual(syntheticStream({ ...spec, seed: 9 }), syntheticStream({ ...spec, seed: 10 }));
});

test('adjacent seeds produce unrelated sequences', () => {
  // A weakly seeded generator makes seeds 1 and 2 nearly the same stream, which
  // would quietly turn a 40-seed fixture sweep into one seed run 40 times.
  const a = Array.from({ length: 50 }, sfc32(1));
  const b = Array.from({ length: 50 }, sfc32(2));
  let same = 0;
  for (let i = 0; i < 50; i++) if (Math.abs(a[i] - b[i]) < 1e-6) same++;
  assert.equal(same, 0, 'seeds 1 and 2 produced overlapping output');
});

test('the watermark trace itself is part of what must match', () => {
  // Two runs could agree on every window and still have advanced the watermark
  // differently. That is a different execution, and the serialisation includes
  // the trace so it cannot hide.
  const events = syntheticStream({ seed: 1, events: 200, lateProb: 0.3 });
  const run = runStream(events, CONFIG);
  assert.ok(run.result.trace.length > 20);
  assert.ok(serialiseRun(run).includes('"trace"'));
});

test('no clock, no random number and no timer anywhere under src/core', () => {
  const dir = path.join(ROOT, 'src', 'core');
  const files = fs.readdirSync(dir).filter((f) => f.endsWith('.js'));
  assert.ok(files.length >= 5, 'expected the core to have several modules');
  const forbidden = [
    /\bDate\s*\.\s*now\b/,
    /\bnew\s+Date\b/,
    /\bMath\s*\.\s*random\b/,
    /\bsetTimeout\b/,
    /\bsetInterval\b/,
    /\bperformance\s*\.\s*now\b/,
    /\bprocess\s*\.\s*hrtime\b/,
    /\bcrypto\s*\.\s*randomUUID\b/,
  ];
  for (const file of files) {
    const text = fs.readFileSync(path.join(dir, file), 'utf8');
    // Strip comments first: this file's own prose says "no Date.now", and a
    // linter that cannot describe its own rule is not a useful linter.
    const code = text.replace(/\/\*[\s\S]*?\*\//g, '').split('\n')
      .map((l) => l.replace(/\/\/.*$/, '')).join('\n');
    for (const re of forbidden) {
      assert.ok(!re.test(code), 'src/core/' + file + ' contains ' + re);
    }
  }
});

test('the engine sees only the event log, never a processing timestamp', () => {
  // Every event carries an optional `p` for the demo's second axis. Stripping it
  // must not change a single byte of the result, which is the strongest form of
  // "the engine never reads it".
  const events = syntheticStream({ seed: 77, events: 300, lateProb: 0.25 });
  const stripped = events.map((e) => ({ t: e.t, key: e.key }));
  assert.equal(serialiseRun(runStream(events, CONFIG)), serialiseRun(runStream(stripped, CONFIG)));
});
