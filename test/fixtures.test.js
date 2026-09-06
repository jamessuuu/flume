// The planted fixtures and the negative control, asserted from the same recipes
// the CLI and the README use.

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  FIXTURES, FIXTURE_CONFIG, fixtureStream, runFixture, runNegativeControl,
} from '../src/fixtures.js';
import { runStream } from '../src/run.js';
import { BUGS, parseBuildFlags, formatBuildFlags, BUG_IDS } from '../src/bugs.js';
import { FINDINGS } from '../src/core/checker.js';

for (const fixture of FIXTURES) {
  test('fixture ' + fixture.id + ' fires "' + fixture.expect + '" and the control does not', () => {
    const r = runFixture(fixture);
    assert.ok(
      r.fires >= fixture.minFires,
      fixture.id + ' fired on ' + r.fires + '/' + r.seeds + ' seeds, floor is ' + fixture.minFires
    );
    assert.equal(r.controlFires, 0, 'the correct build produced findings on ' + r.controlFires + ' seeds');
  });
}

test('every fixture expects a documented finding code', () => {
  for (const fixture of FIXTURES) {
    assert.ok(FINDINGS[fixture.expect], fixture.id + ' expects an undocumented code: ' + fixture.expect);
  }
});

test('every build flag in src/bugs.js is reachable and changes something', () => {
  const events = fixtureStream(1);
  const control = runStream(events, FIXTURE_CONFIG);
  for (const bug of BUGS) {
    const run = runStream(events, { ...FIXTURE_CONFIG, flags: parseBuildFlags(bug.id) });
    const changed =
      JSON.stringify(run.verdict.summary) !== JSON.stringify(control.verdict.summary) ||
      run.verdict.findings.length !== control.verdict.findings.length;
    assert.ok(changed, bug.id + ' had no observable effect, so it is not a fixture');
  }
});

test('an unknown build flag is refused and lists the known ones', () => {
  assert.throws(() => parseBuildFlags('not-a-bug'), /unknown build flag "not-a-bug"/);
  assert.throws(() => parseBuildFlags('not-a-bug'), new RegExp(BUG_IDS[0]));
});

test('flags round-trip through their string form', () => {
  assert.equal(formatBuildFlags(parseBuildFlags(undefined)), 'correct');
  assert.equal(formatBuildFlags(parseBuildFlags('silent-drop')), 'silent-drop');
  // Formatting is in declaration order, not input order, so the string form of
  // a build is canonical and two equivalent builds compare equal.
  assert.equal(
    formatBuildFlags(parseBuildFlags('silent-drop,close-without-lateness')),
    formatBuildFlags(parseBuildFlags('close-without-lateness,silent-drop'))
  );
  assert.equal(
    formatBuildFlags(parseBuildFlags('silent-drop,close-without-lateness')),
    'close-without-lateness,silent-drop'
  );
});

test('the engine never sees the checker flag and the checker never sees the engine flags', () => {
  const events = fixtureStream(3);
  const sabotaged = runStream(events, {
    ...FIXTURE_CONFIG, flags: parseBuildFlags('checker-revised-as-complete'),
  });
  const honest = runStream(events, FIXTURE_CONFIG);
  // The engine result must be untouched by a checker-only flag: same panes,
  // same side output, same counters.
  assert.deepEqual(sabotaged.result.counters, honest.result.counters);
  assert.equal(sabotaged.result.panes.length, honest.result.panes.length);
});

test('negative control: a perfectly ordered stream produces no revisions and no unknowns', () => {
  const nc = runNegativeControl();
  assert.ok(nc.runs >= 400, 'the control has to be large enough to mean something');
  assert.equal(nc.revised, 0, 'a zero-lateness stream cannot produce a revision');
  assert.equal(nc.unverifiable, 0, 'a flushed zero-lateness stream cannot produce an unknown');
  assert.equal(nc.findings, 0, 'a correct engine on a tidy stream must be clean');
});

test('negative control, idle: the only unverifiable windows are the tail the watermark never reached', () => {
  const nc = runNegativeControl();
  assert.equal(nc.idleFindings, 0);
  assert.equal(
    nc.idleTailOnly, nc.idleRuns,
    nc.idleRuns - nc.idleTailOnly + ' idle runs went unverifiable for a reason other than the tail'
  );
});
