// Reproduction recipes for the planted fixtures, and the negative control.
//
// One source of truth shared by `node src/cli.js fixtures`, the test suite and
// the README, so the numbers in the README cannot drift away from the numbers
// the tests assert.
//
// `minFires` is a FLOOR that CI asserts, deliberately set at or below the rate
// measured on the day recorded in MEASURED_ON. `measured` is what was actually
// observed. Nothing here is a guess; `node src/cli.js fixtures` reproduces
// every number in one command.

import { syntheticStream, orderedStream } from './streams/synthetic.js';
import { runStream } from './run.js';
import { parseBuildFlags } from './bugs.js';

export const MEASURED_ON = '2026-09-06';

/** @param {number} from @param {number} to @returns {number[]} */
function range(from, to) {
  const out = [];
  for (let i = from; i <= to; i++) out.push(i);
  return out;
}

/**
 * The stream every engine fixture runs against: skewed arrival plus a burst.
 * The burst is not decoration. Without it the arrival index and the event time
 * advance in lockstep and a PROCESSING-time watermark is accidentally correct,
 * so the fixture that matters most cannot fire. A stream that never bursts is a
 * stream that never had an upstream outage, which is to say not a stream.
 *
 * @param {number} seed
 */
export function fixtureStream(seed) {
  return syntheticStream({
    seed,
    events: 300,
    stepMs: 1000,
    lateProb: 0.2,
    maxDelaySlots: 20,
    burstAt: 120,
    burstCount: 80,
    burstSpanMs: 4000,
  });
}

/** Engine configuration shared by every fixture, so only the bug flag varies. */
export const FIXTURE_CONFIG = {
  windowMs: 10000,
  allowedLatenessMs: 5000,
  latePolicy: /** @type {'update'} */ ('update'),
  termination: /** @type {'idle'} */ ('idle'),
  watermark: { name: 'bounded', boundMs: 2000, msPerEvent: 1000 },
};

/**
 * @typedef {object} Fixture
 * @property {string} id build flag id, see src/bugs.js
 * @property {'engine'|'checker'} target
 * @property {string} expect the finding code that must appear (engine fixtures)
 * @property {number[]} seeds
 * @property {number} minFires floor CI asserts
 * @property {number} measured seeds on which it actually fired
 * @property {string} note
 */

/** @type {Fixture[]} */
export const FIXTURES = [
  {
    id: 'processing-time-watermark',
    target: 'engine',
    expect: 'watermark-overshoot',
    seeds: range(1, 40),
    minFires: 40,
    measured: 40,
    note:
      'Caught structurally, not statistically. A watermark derived from observed event times is ' +
      'maxSeen minus a non-negative bound, so it can never rise above the highest event time yet ' +
      'seen. One that does is not a function of the data. A rate-of-late-events threshold would ' +
      'also have fired here -- and would have fired on a correctly implemented bound that is simply ' +
      'too small for its stream, which is why it is not the test.',
  },
  {
    id: 'close-without-lateness',
    target: 'engine',
    expect: 'premature-close',
    seeds: range(1, 40),
    minFires: 40,
    measured: 40,
    note:
      'Allowed lateness becomes a setting that appears in the config, prints in the report and does ' +
      'nothing. The give-away is not the missing revisions -- "no revisions" reads like "no late ' +
      'data" -- it is that state was released before the watermark had passed end + allowedLateness.',
  },
  {
    id: 'silent-drop',
    target: 'engine',
    expect: 'silent-drop',
    seeds: range(1, 40),
    minFires: 40,
    measured: 40,
    note:
      'The output of a silently dropping engine is indistinguishable from a healthy one: no counter ' +
      'moves, no receipt appears, the panes look fine. Only the batch oracle finds it, which is the ' +
      'entire argument for having an oracle rather than trusting the engine\'s own accounting.',
  },
];

/**
 * The sabotaged checker is not scored by "did a finding fire" -- it produces no
 * findings at all, which is exactly what makes it dangerous. It is scored by
 * whether it changes verdicts it has no business changing.
 * @param {number[]} seeds
 */
export function runSabotageFixture(seeds) {
  const rows = [];
  for (const seed of seeds) {
    const events = fixtureStream(seed);
    const honest = runStream(events, { ...FIXTURE_CONFIG });
    const lying = runStream(events, {
      ...FIXTURE_CONFIG,
      flags: parseBuildFlags('checker-revised-as-complete'),
    });
    rows.push({
      seed,
      revisedHonest: honest.verdict.summary.revised,
      revisedLying: lying.verdict.summary.revised,
      completeHonest: honest.verdict.summary.complete,
      completeLying: lying.verdict.summary.complete,
      // The lie must be exactly a relabelling: every revised window becomes
      // complete, and nothing else moves.
      exactRelabel:
        lying.verdict.summary.revised === 0 &&
        lying.verdict.summary.complete === honest.verdict.summary.complete + honest.verdict.summary.revised &&
        lying.verdict.summary.unverifiable === honest.verdict.summary.unverifiable,
    });
  }
  const withRevisions = rows.filter((r) => r.revisedHonest > 0);
  return {
    seeds: rows.length,
    seedsWithRevisions: withRevisions.length,
    revisedHidden: withRevisions.reduce((a, r) => a + r.revisedHonest, 0),
    allExactRelabels: rows.every((r) => r.exactRelabel),
    rows,
  };
}

/**
 * @param {Fixture} fixture
 * @returns {{fires: number, controlFires: number, seeds: number}}
 */
export function runFixture(fixture) {
  let fires = 0;
  let controlFires = 0;
  for (const seed of fixture.seeds) {
    const events = fixtureStream(seed);
    const buggy = runStream(events, { ...FIXTURE_CONFIG, flags: parseBuildFlags(fixture.id) });
    if (buggy.verdict.findings.some((f) => f.code === fixture.expect)) fires++;
    const control = runStream(events, { ...FIXTURE_CONFIG });
    if (control.verdict.findings.length > 0) controlFires++;
  }
  return { fires, controlFires, seeds: fixture.seeds.length };
}

export const NEGATIVE_CONTROL = {
  seeds: range(1, 200),
  eventCounts: [120, 400],
  note:
    'A perfectly ordered stream with exactly zero lateness, ended with a flush -- the source is ' +
    'exhausted and says so, which is what licenses a final watermark of +infinity. Every window must ' +
    'come out complete: zero revised, zero unverifiable, zero findings. Without this, "the checker ' +
    'found bugs" is indistinguishable from "the checker fires at random".',
};

/**
 * `idle` is the other truth about the end of a stream: we stopped watching one
 * that continues. The trailing windows the watermark never reached then
 * genuinely cannot be verified, and saying so is the third outcome doing its
 * job rather than a bug. How MANY trail depends on where the last event fell
 * inside its window, so the claim asserted is not a count -- it is that on a
 * zero-lateness stream every unverifiable window is unverifiable for the one
 * legitimate reason, and never for any other.
 */
export function runNegativeControl() {
  let runs = 0;
  let revised = 0;
  let unverifiable = 0;
  let findings = 0;
  let idleRuns = 0;
  let idleTailOnly = 0;
  let idleFindings = 0;
  for (const seed of NEGATIVE_CONTROL.seeds) {
    for (const events of NEGATIVE_CONTROL.eventCounts) {
      const stream = orderedStream({ seed, events, stepMs: 700 });
      const flushed = runStream(stream, { ...FIXTURE_CONFIG, termination: 'flush' });
      runs++;
      revised += flushed.verdict.summary.revised;
      unverifiable += flushed.verdict.summary.unverifiable;
      findings += flushed.verdict.findings.length;

      const idle = runStream(stream, { ...FIXTURE_CONFIG, termination: 'idle' });
      idleRuns++;
      idleFindings += idle.verdict.findings.length;
      const reasons = Object.keys(idle.verdict.reasons);
      const tailOnly =
        idle.verdict.summary.revised === 0 &&
        idle.verdict.summary.unverifiable >= 1 &&
        reasons.length === 1 && reasons[0] === 'watermark-never-reached';
      if (tailOnly) idleTailOnly++;
    }
  }
  return { runs, revised, unverifiable, findings, idleRuns, idleTailOnly, idleFindings };
}
