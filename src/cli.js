#!/usr/bin/env node
// flume command line.
//
// Every command exits non-zero on failure and prints a stated error rather than
// a stack trace. That is release-standard row R6, and it is the row a technical
// visitor discovers fastest -- usually by pointing the tool at the wrong file.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { runStream, serialiseRun } from './run.js';
import { STRATEGY_NAMES } from './core/watermark.js';
import { LATE_POLICIES, TERMINATIONS } from './core/engine.js';
import { latenessProfile } from './core/oracle.js';
import { syntheticStream } from './streams/synthetic.js';
import { demoStream, DEMO_CONFIG, DEMO_EXPECTED } from './streams/demo.js';
import { loadVendored, parseNdjsonStream, STREAMS, STREAM_IDS, streamMeta } from './streams/vendored.js';
import { BUGS, BUG_IDS, parseBuildFlags, formatBuildFlags } from './bugs.js';
import {
  FIXTURES, FIXTURE_CONFIG, MEASURED_ON, NEGATIVE_CONTROL,
  runFixture, runNegativeControl, runSabotageFixture,
} from './fixtures.js';
import { renderTimeline, renderVerdict, humanMs } from './render.js';

/** Thrown for every condition a user can cause; never surfaces a stack trace. */
export class UserError extends Error {}

/** Refuse absurd inputs before reading them, not after. */
export const MAX_INPUT_BYTES = 32 * 1024 * 1024;

const USAGE = `flume -- event-time windowing with watermarks, and a checker for the result

  flume demo                     one late event, one revision, explained
  flume run [options]            a generated stream with known lateness
  flume stream <id> [options]    one of the vendored real streams
  flume check <file.ndjson>      an event log from a file: {"t":<ms>,"p":<ms|null>,"k":"<key>"}
  flume streams                  the vendored real streams and their measured lateness
  flume fixtures                 the planted fixtures and the negative control
  flume bugs                     list the build-flag fixtures

options for run / stream / check
  --window <ms>          tumbling window size, default 10000
  --lateness <ms>        allowed lateness, default 5000
  --policy <p>           ${LATE_POLICIES.join(' | ')}, default update
  --termination <t>      ${TERMINATIONS.join(' | ')}, default idle
  --watermark <name>     ${STRATEGY_NAMES.join(' | ')}, default bounded
  --bound <ms>           lateness bound for the bounded strategies, default 2000
  --percentile <0..1>    for the percentile strategy, default 0.99
  --build <flags>        comma separated, see \`flume bugs\`; default: correct
  --json                 machine-readable output

options for run only
  --seed <int>           default 4711
  --events <int>         default 300
  --late-prob <0..1>     fraction of events displaced in arrival order, default 0.2
  --timeline             draw the two-axis picture

exit codes
  0  ran, and nothing the command asserts was violated
  1  a bad argument, an unreadable or malformed input, or a failed assertion
`;

/**
 * @param {string[]} argv
 * @returns {{_: string[], [k: string]: any}}
 */
export function parseArgs(argv) {
  /** @type {any} */
  const out = { _: [] };
  const wantsValue = new Set([
    'window', 'lateness', 'policy', 'termination', 'watermark', 'bound',
    'percentile', 'build', 'seed', 'events', 'late-prob',
  ]);
  const bare = new Set(['json', 'help', 'timeline']);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) {
      out._.push(a);
      continue;
    }
    const name = a.slice(2);
    if (bare.has(name)) {
      out[name] = true;
      continue;
    }
    if (!wantsValue.has(name)) {
      throw new UserError('unknown option "' + a + '"\n\n' + USAGE);
    }
    const value = argv[++i];
    if (value === undefined) throw new UserError('option "' + a + '" needs a value');
    out[name] = value;
  }
  return out;
}

/** @param {any} args @param {string} name @param {number} fallback */
function intOpt(args, name, fallback) {
  if (args[name] === undefined) return fallback;
  const raw = String(args[name]);
  if (!/^-?\d+$/.test(raw)) {
    throw new UserError('--' + name + ' must be an integer, got "' + raw + '"');
  }
  return Number(raw);
}

/** @param {any} args @param {string} name @param {number} fallback */
function nonNegIntOpt(args, name, fallback) {
  const v = intOpt(args, name, fallback);
  if (v < 0) throw new UserError('--' + name + ' must be zero or more, got ' + v);
  return v;
}

/** @param {any} args @param {string} name @param {number} fallback */
function positiveIntOpt(args, name, fallback) {
  const v = intOpt(args, name, fallback);
  if (v <= 0) throw new UserError('--' + name + ' must be a positive integer, got ' + v);
  return v;
}

/** @param {any} args @param {string} name @param {number} fallback */
function fractionOpt(args, name, fallback) {
  if (args[name] === undefined) return fallback;
  const raw = String(args[name]);
  const v = Number(raw);
  if (!Number.isFinite(v) || v < 0 || v > 1) {
    throw new UserError('--' + name + ' must be a number between 0 and 1, got "' + raw + '"');
  }
  return v;
}

/** @param {any} args @param {string} name @param {string[]} allowed @param {string} fallback */
function enumOpt(args, name, allowed, fallback) {
  if (args[name] === undefined) return fallback;
  const v = String(args[name]);
  if (!allowed.includes(v)) {
    throw new UserError('--' + name + ' must be one of: ' + allowed.join(', ') + '; got "' + v + '"');
  }
  return v;
}

/** @param {any} args */
export function engineOptionsFrom(args, defaults = {}) {
  const name = enumOpt(args, 'watermark', STRATEGY_NAMES, 'bounded');
  /** @type {any} */
  const wm = { name, boundMs: nonNegIntOpt(args, 'bound', /** @type {any} */ (defaults).boundMs ?? 2000) };
  if (name === 'percentile') {
    wm.percentile = fractionOpt(args, 'percentile', 0.99);
    wm.sampleSize = 512;
    wm.floorMs = wm.boundMs;
  }
  if (name === 'processing-time') wm.msPerEvent = 1000;
  wm.msPerEvent = wm.msPerEvent ?? 1000;
  let flags;
  try {
    flags = parseBuildFlags(args.build);
  } catch (err) {
    throw new UserError(/** @type {Error} */ (err).message);
  }
  return {
    windowMs: positiveIntOpt(args, 'window', /** @type {any} */ (defaults).windowMs ?? 10000),
    allowedLatenessMs: nonNegIntOpt(args, 'lateness', /** @type {any} */ (defaults).allowedLatenessMs ?? 5000),
    latePolicy: /** @type {any} */ (enumOpt(args, 'policy', LATE_POLICIES, 'update')),
    termination: /** @type {any} */ (enumOpt(args, 'termination', TERMINATIONS, 'idle')),
    watermark: wm,
    flags,
  };
}

/**
 * Reads a file the way a user will get it wrong: missing, a directory, empty,
 * enormous, or full of something that is not an event log.
 * @param {string} file
 * @returns {string}
 */
export function readEventFile(file) {
  let stat;
  try {
    stat = fs.statSync(file);
  } catch (err) {
    const e = /** @type {NodeJS.ErrnoException} */ (err);
    if (e.code === 'ENOENT') throw new UserError('no such file: ' + file);
    if (e.code === 'EACCES') throw new UserError('cannot read (permission denied): ' + file);
    throw new UserError('cannot read ' + file + ': ' + e.message);
  }
  if (stat.isDirectory()) throw new UserError(file + ' is a directory, not an event log');
  if (!stat.isFile()) throw new UserError(file + ' is not a regular file');
  if (stat.size === 0) throw new UserError(file + ': file is empty');
  if (stat.size > MAX_INPUT_BYTES) {
    throw new UserError(
      file + ' is ' + stat.size + ' bytes; the limit is ' + MAX_INPUT_BYTES +
      ' bytes. flume holds the whole log in memory on purpose (see the README scope cut), so it ' +
      'refuses a file it cannot hold rather than dying part way through one.'
    );
  }
  try {
    return fs.readFileSync(file, 'utf8');
  } catch (err) {
    throw new UserError('cannot read ' + file + ': ' + /** @type {Error} */ (err).message);
  }
}

/** @param {string[]} lines */
function say(lines) {
  process.stdout.write(lines.join('\n') + '\n');
}

/** @param {any} obj */
function sayJson(obj) {
  process.stdout.write(JSON.stringify(obj, null, 2) + '\n');
}

/**
 * The demo. A tiny hand-built log, small enough to read every row, whose whole
 * point is the one event on row 14.
 */
function cmdDemo(args) {
  const events = demoStream();
  const options = { ...DEMO_CONFIG, flags: parseBuildFlags(args.build) };
  const run = runStream(events, options);
  if (args.json) {
    sayJson({ config: run.result.config, verdict: run.verdict.summary, windows: run.verdict.windows });
    return 0;
  }
  say([
    'flume demo -- nineteen events, two clocks, and all three outcomes.',
    '',
    'Event time runs left to right. Processing time runs top to bottom, one row per',
    'event, in the order the engine saw them. The watermark is the |, and it is',
    'always behind the rightmost event seen -- that is what makes it a watermark and',
    'not a clock.',
    '',
  ]);
  say(renderTimeline(events, run.result, { width: 40 }));
  say(['']);
  say(renderVerdict(run));
  const byOutcome = {
    complete: run.verdict.windows.filter((w) => w.outcome === 'complete'),
    revised: run.verdict.windows.filter((w) => w.outcome === 'revised'),
    unverifiable: run.verdict.windows.filter((w) => w.outcome === 'unverifiable'),
  };
  say([
    '',
    'Row 9 carries an event time of +3.0s. Its window had already closed and reported',
    '4. Allowed lateness is 6s and the state was still there, so the engine retracted',
    '4 and emitted 5. That window is REVISED, not complete -- the distinction the',
    'checker exists to keep.',
    '',
    'Row 18 carries +6.0s, and by then the state of its window had been released. It',
    'is not dropped: it goes to the side output carrying the reason, and its window is',
    'UNVERIFIABLE, because the emitted panes cannot be shown to be right.',
    '',
    'The last window is unverifiable for a different reason: the log ended before the',
    'watermark reached it. Two reasons, one outcome, and the report says which.',
    '',
  ]);
  for (const [outcome, list] of Object.entries(byOutcome)) {
    for (const w of list) say(['  ' + outcome.padEnd(13) + w.window + '  ' + w.detail]);
  }
  say([
    '',
    'Same log, same watermark strategy, run again -- byte for byte the same output.',
    'Nothing under src/core reads a clock; test/determinism.test.js asserts it.',
  ]);
  const got = run.verdict.summary;
  const want = DEMO_EXPECTED;
  if (got.complete !== want.complete || got.revised !== want.revised || got.unverifiable !== want.unverifiable) {
    process.stderr.write(
      'flume: the demo must produce ' + JSON.stringify(want) + '; got ' + JSON.stringify(got) + '\n'
    );
    return 1;
  }
  return 0;
}

function cmdRun(args) {
  const seed = intOpt(args, 'seed', 4711);
  const events = positiveIntOpt(args, 'events', 300);
  const lateProb = fractionOpt(args, 'late-prob', 0.2);
  const options = engineOptionsFrom(args);
  const stream = syntheticStream({
    seed, events, stepMs: 1000, lateProb, maxDelaySlots: 20,
    burstAt: Math.floor(events * 0.4), burstCount: Math.floor(events * 0.27), burstSpanMs: 4000,
  });
  const run = runStream(stream, options);
  if (args.json) {
    sayJson({
      seed, events, config: run.result.config, lateness: run.lateness,
      summary: run.verdict.summary, reasons: run.verdict.reasons, findings: run.verdict.findings,
    });
    return run.verdict.findings.length > 0 ? 1 : 0;
  }
  say([
    'seed ' + seed + '  build ' + formatBuildFlags(options.flags) + '  ' + events + ' events',
    'watermark: ' + run.result.config.watermarkDescribe,
    'windows ' + humanMs(options.windowMs) + ', allowed lateness ' + humanMs(options.allowedLatenessMs) +
      ', late policy ' + options.latePolicy + ', termination ' + options.termination,
    'measured lateness: ' + fmtLateness(run.lateness),
    '',
  ]);
  if (args.timeline) {
    say(renderTimeline(stream, run.result, { width: 52, maxRows: 40 }));
    say(['']);
  }
  say(renderVerdict(run));
  return run.verdict.findings.length > 0 ? 1 : 0;
}

function cmdStream(args) {
  const id = args._[1];
  if (!id) {
    throw new UserError('which stream? one of: ' + STREAM_IDS.join(', ') + '\n\n' + USAGE);
  }
  let meta;
  try {
    meta = streamMeta(id);
  } catch (err) {
    throw new UserError(/** @type {Error} */ (err).message);
  }
  let events;
  try {
    events = loadVendored(id);
  } catch (err) {
    throw new UserError(/** @type {Error} */ (err).message);
  }
  const options = engineOptionsFrom(args, {
    windowMs: meta.suggestedWindowMs,
    allowedLatenessMs: meta.suggestedWindowMs,
  });
  const run = runStream(events, options);
  if (args.json) {
    sayJson({
      stream: id, source: meta.source, config: run.result.config, lateness: run.lateness,
      summary: run.verdict.summary, reasons: run.verdict.reasons, findings: run.verdict.findings,
    });
    return run.verdict.findings.length > 0 ? 1 : 0;
  }
  say([
    meta.title,
    '  source     ' + meta.source,
    '  event time ' + meta.eventTime,
    '  processing ' + meta.processingTime,
    '  licence    ' + meta.licence,
    '',
    'watermark: ' + run.result.config.watermarkDescribe,
    'windows ' + humanMs(options.windowMs) + ', allowed lateness ' + humanMs(options.allowedLatenessMs) +
      ', late policy ' + options.latePolicy,
    'measured lateness: ' + fmtLateness(run.lateness),
    '',
  ]);
  say(renderVerdict(run));
  return run.verdict.findings.length > 0 ? 1 : 0;
}

function cmdCheck(args) {
  const file = args._[1];
  if (!file) throw new UserError('which file? usage: flume check <file.ndjson>');
  const text = readEventFile(file);
  let events;
  try {
    events = parseNdjsonStream(text, path.basename(file));
  } catch (err) {
    throw new UserError(/** @type {Error} */ (err).message);
  }
  const options = engineOptionsFrom(args);
  const run = runStream(events, options);
  if (args.json) {
    sayJson({
      file, events: events.length, config: run.result.config, lateness: run.lateness,
      summary: run.verdict.summary, reasons: run.verdict.reasons, findings: run.verdict.findings,
    });
    return run.verdict.findings.length > 0 ? 1 : 0;
  }
  say([
    file + ': ' + events.length + ' events',
    'watermark: ' + run.result.config.watermarkDescribe,
    'measured lateness: ' + fmtLateness(run.lateness),
    '',
  ]);
  say(renderVerdict(run));
  return run.verdict.findings.length > 0 ? 1 : 0;
}

function cmdStreams(args) {
  const rows = STREAMS.map((meta) => {
    const events = loadVendored(meta.id);
    const lp = latenessProfile(events);
    const span = Math.max(...events.map((e) => e.t)) - Math.min(...events.map((e) => e.t));
    return { meta, lp, span, events: events.length };
  });
  if (args.json) {
    sayJson(rows.map((r) => ({
      id: r.meta.id, source: r.meta.source, licence: r.meta.licence,
      events: r.events, spanMs: r.span, lateness: r.lp,
    })));
    return 0;
  }
  say([
    'The three vendored real streams. Slices only; see vendor/<id>/SOURCE.md for',
    'the derivation and the licence position of each.',
    '',
  ]);
  for (const r of rows) {
    say([
      r.meta.id + ' -- ' + r.meta.title,
      '  ' + r.events + ' events over ' + humanMs(r.span) + ' of event time',
      '  out of order: ' + r.lp.outOfOrder + ' (' + r.lp.outOfOrderPct.toFixed(2) + '%)',
      '  lateness p50 ' + humanMs(r.lp.p50) + '  p90 ' + humanMs(r.lp.p90) +
        '  p99 ' + humanMs(r.lp.p99) + '  max ' + humanMs(r.lp.max),
      '  ' + r.meta.licence,
      '',
    ]);
  }
  return 0;
}

function cmdFixtures(args) {
  /** @type {any[]} */
  const results = [];
  let ok = true;
  for (const fixture of FIXTURES) {
    const r = runFixture(fixture);
    const pass = r.fires >= fixture.minFires && r.controlFires === 0;
    if (!pass) ok = false;
    results.push({ id: fixture.id, expect: fixture.expect, ...r, minFires: fixture.minFires, pass });
  }
  const sab = runSabotageFixture(FIXTURES[0].seeds.slice(0, 20));
  const sabPass = sab.allExactRelabels && sab.revisedHidden > 0;
  if (!sabPass) ok = false;
  const nc = runNegativeControl();
  const ncPass = nc.revised === 0 && nc.unverifiable === 0 && nc.findings === 0 &&
    nc.idleFindings === 0 && nc.idleTailOnly === nc.idleRuns;
  if (!ncPass) ok = false;

  if (args.json) {
    sayJson({
      measuredOn: MEASURED_ON,
      fixtures: results,
      sabotage: { ...sab, rows: undefined, pass: sabPass },
      negativeControl: { ...nc, pass: ncPass },
      ok,
    });
    return ok ? 0 : 1;
  }
  say(['Planted fixtures (recipes in src/fixtures.js, measured ' + MEASURED_ON + ')']);
  for (const r of results) {
    say(['  ' + (r.pass ? 'OK  ' : 'FAIL') + ' ' + r.id.padEnd(26) +
      r.fires + '/' + r.seeds + ' seeds fired "' + r.expect + '" (floor ' + r.minFires + '), ' +
      'control ' + r.controlFires]);
  }
  say([
    '  ' + (sabPass ? 'OK  ' : 'FAIL') + ' checker-revised-as-complete  hid ' + sab.revisedHidden +
      ' revised windows across ' + sab.seedsWithRevisions + '/' + sab.seeds +
      ' seeds; every change was an exact relabel of revised to complete',
    '',
    'Negative control: ' + nc.runs + ' executions of a perfectly ordered, zero-lateness stream',
    '  flush termination: ' + nc.revised + ' revised, ' + nc.unverifiable + ' unverifiable, ' +
      nc.findings + ' findings',
    '  idle termination:  ' + nc.idleTailOnly + '/' + nc.idleRuns +
      ' runs unverifiable ONLY at the tail the watermark never reached, ' + nc.idleFindings + ' findings',
    '',
    NEGATIVE_CONTROL.note,
    '',
    'Fixture stream: ' + JSON.stringify(FIXTURE_CONFIG),
  ]);
  return ok ? 0 : 1;
}

function cmdBugs(args) {
  if (args.json) {
    sayJson(BUGS);
    return 0;
  }
  say(['Build-flag fixtures (all of them live in src/bugs.js):', '']);
  for (const b of BUGS) {
    say([
      '  ' + b.id + '  [' + b.target + ']',
      '    ' + b.title,
      '    ' + wrap(b.why, 72, '    '),
      '',
    ]);
  }
  say(['Use: flume run --build ' + BUG_IDS[0]]);
  return 0;
}

/** @param {ReturnType<typeof latenessProfile>} lp */
function fmtLateness(lp) {
  return lp.outOfOrder + '/' + lp.events + ' out of order (' + lp.outOfOrderPct.toFixed(2) + '%), ' +
    'p50 ' + humanMs(lp.p50) + ', p99 ' + humanMs(lp.p99) + ', max ' + humanMs(lp.max);
}

/** @param {string} text @param {number} width @param {string} indent */
function wrap(text, width, indent) {
  const words = text.split(/\s+/);
  const lines = [];
  let line = '';
  for (const w of words) {
    if (line.length + w.length + 1 > width) { lines.push(line); line = w; }
    else line = line ? line + ' ' + w : w;
  }
  if (line) lines.push(line);
  return lines.join('\n' + indent);
}

/** @param {string[]} argv @returns {number} */
export function main(argv) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (err) {
    if (err instanceof UserError) {
      process.stderr.write('flume: ' + err.message + '\n');
      return 1;
    }
    throw err;
  }
  const cmd = args._[0];
  if (args.help || !cmd) {
    process.stdout.write(USAGE);
    return cmd ? 0 : 1;
  }
  const table = {
    demo: cmdDemo, run: cmdRun, stream: cmdStream, check: cmdCheck,
    streams: cmdStreams, fixtures: cmdFixtures, bugs: cmdBugs,
  };
  const fn = /** @type {any} */ (table)[cmd];
  if (!fn) {
    process.stderr.write('flume: unknown command "' + cmd + '"\n\n' + USAGE);
    return 1;
  }
  try {
    return fn(args);
  } catch (err) {
    if (err instanceof UserError) {
      process.stderr.write('flume: ' + err.message + '\n');
      return 1;
    }
    // Anything reaching here is a flume bug, not a user error. Say so plainly
    // and still refuse to print a stack trace: a stack trace tells a visitor
    // nothing they can act on, and it is the R6 failure a stranger finds first.
    process.stderr.write(
      'flume: internal error: ' + /** @type {Error} */ (err).message +
      '\n       This is a bug in flume, not in your input. Please open an issue with the command you ran.\n'
    );
    return 1;
  }
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  process.exitCode = main(process.argv.slice(2));
}

export { USAGE };
export const HERE = path.dirname(fileURLToPath(import.meta.url));
