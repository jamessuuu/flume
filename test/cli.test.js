// Release-standard row R6, proven behaviourally.
//
// The entry point must not throw a raw stack trace on a missing file, a
// malformed file, an empty file, a file far larger than expected, or a
// wrong-type argument. Each must produce a stated error and a non-zero exit.
// These run the real CLI in a child process, because that is what a stranger
// does.

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const CLI = path.join(ROOT, 'src', 'cli.js');
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'flume-cli-'));

/** @param {string[]} args */
function run(args) {
  const r = spawnSync(process.execPath, [CLI, ...args], { encoding: 'utf8', cwd: ROOT });
  return { code: r.status, out: r.stdout || '', err: r.stderr || '' };
}

/** @param {{err: string, out: string}} r */
function assertNoStackTrace(r) {
  const text = r.err + r.out;
  assert.ok(!/\n\s+at\s.+:\d+:\d+/.test(text), 'leaked a stack trace:\n' + text);
  assert.ok(!text.includes('node:internal'), 'leaked node internals:\n' + text);
}

test('R6: a missing file states the error and exits non-zero', () => {
  const r = run(['check', path.join(TMP, 'does-not-exist.ndjson')]);
  assert.equal(r.code, 1);
  assert.match(r.err, /no such file/);
  assertNoStackTrace(r);
});

test('R6: an empty file states the error and exits non-zero', () => {
  const f = path.join(TMP, 'empty.ndjson');
  fs.writeFileSync(f, '');
  const r = run(['check', f]);
  assert.equal(r.code, 1);
  assert.match(r.err, /file is empty/);
  assertNoStackTrace(r);
});

test('R6: a malformed file states the line that broke', () => {
  const f = path.join(TMP, 'prose.ndjson');
  fs.writeFileSync(f, '{"t":1000}\nDear reader, this is not an event log.\n');
  const r = run(['check', f]);
  assert.equal(r.code, 1);
  assert.match(r.err, /prose\.ndjson:2: not valid JSON/);
  assertNoStackTrace(r);
});

test('R6: valid JSON that is not an event log is still refused', () => {
  const f = path.join(TMP, 'wrong-shape.ndjson');
  fs.writeFileSync(f, '{"timestamp":"2026-01-01T00:00:00Z"}\n');
  const r = run(['check', f]);
  assert.equal(r.code, 1);
  assert.match(r.err, /every line needs a finite numeric "t"/);
  assertNoStackTrace(r);
});

test('R6: a file far larger than expected is refused before it is read', () => {
  const f = path.join(TMP, 'huge.ndjson');
  const fd = fs.openSync(f, 'w');
  fs.ftruncateSync(fd, 33 * 1024 * 1024);
  fs.closeSync(fd);
  const r = run(['check', f]);
  assert.equal(r.code, 1);
  assert.match(r.err, /the limit is/);
  assertNoStackTrace(r);
  fs.unlinkSync(f);
});

test('R6: a directory where a file was expected', () => {
  const r = run(['check', TMP]);
  assert.equal(r.code, 1);
  assert.match(r.err, /is a directory/);
  assertNoStackTrace(r);
});

test('R6: wrong-type arguments are refused with the type that was wanted', () => {
  for (const args of [
    ['run', '--seed', 'banana'],
    ['run', '--events', '3.5'],
    ['run', '--window', '0'],
    ['run', '--lateness', '-1'],
    ['run', '--late-prob', '17'],
  ]) {
    const r = run(args);
    assert.equal(r.code, 1, args.join(' ') + ' should have failed');
    assert.match(r.err, /must be (an integer|a positive integer|zero or more|a number between)/, args.join(' '));
    assertNoStackTrace(r);
  }
});

test('R6: unknown enums list the acceptable values', () => {
  for (const [args, re] of /** @type {[string[], RegExp][]} */ ([
    [['run', '--policy', 'ignore'], /must be one of: drop, update, side-output/],
    [['run', '--watermark', 'vibes'], /must be one of: bounded, bounded-naive/],
    [['run', '--termination', 'someday'], /must be one of: idle, flush/],
    [['run', '--build', 'not-a-bug'], /unknown build flag "not-a-bug"/],
    [['stream', 'kafka'], /unknown stream "kafka"/],
  ])) {
    const r = run(args);
    assert.equal(r.code, 1, args.join(' '));
    assert.match(r.err, re, args.join(' '));
    assertNoStackTrace(r);
  }
});

test('R6: an unknown option and an unknown command are both refused with usage', () => {
  const opt = run(['run', '--turbo']);
  assert.equal(opt.code, 1);
  assert.match(opt.err, /unknown option "--turbo"/);
  const cmd = run(['frobnicate']);
  assert.equal(cmd.code, 1);
  assert.match(cmd.err, /unknown command "frobnicate"/);
  assertNoStackTrace(opt);
  assertNoStackTrace(cmd);
});

test('R6: an option missing its value does not silently take the next flag', () => {
  const r = run(['run', '--seed']);
  assert.equal(r.code, 1);
  assert.match(r.err, /needs a value/);
  assertNoStackTrace(r);
});

test('no arguments prints usage and exits non-zero', () => {
  const r = run([]);
  assert.equal(r.code, 1);
  assert.match(r.out, /flume -- event-time windowing/);
});

test('a valid event log is accepted and checked', () => {
  const f = path.join(TMP, 'good.ndjson');
  const lines = [];
  for (let i = 0; i < 50; i++) lines.push(JSON.stringify({ t: i * 1000, p: i * 1000, k: 'a' }));
  lines.push(JSON.stringify({ t: 2000, p: 51000, k: 'a' }));
  fs.writeFileSync(f, lines.join('\n') + '\n');
  const r = run(['check', f, '--json']);
  assert.equal(r.code, 0);
  const out = JSON.parse(r.out);
  assert.equal(out.events, 51);
  assert.equal(out.findings.length, 0);
});

test('demo, streams, fixtures and bugs all exit zero and say something', () => {
  for (const [args, re] of /** @type {[string[], RegExp][]} */ ([
    [['demo'], /1 complete   1 revised   2 unverifiable/],
    [['streams'], /gharchive/],
    [['bugs'], /processing-time-watermark/],
    [['fixtures'], /Negative control/],
  ])) {
    const r = run(args);
    assert.equal(r.code, 0, args.join(' ') + ' exited ' + r.code + '\n' + r.err);
    assert.match(r.out, re, args.join(' '));
  }
});

test('running a planted build exits non-zero, because findings are failures', () => {
  const r = run(['run', '--build', 'silent-drop', '--json']);
  assert.equal(r.code, 1);
  const out = JSON.parse(r.out);
  assert.ok(out.findings.some((/** @type {any} */ f) => f.code === 'silent-drop'));
});
