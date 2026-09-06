#!/usr/bin/env node
// Writes vendor/MEASURED.json: the lateness profile of each vendored stream and
// what the checker says about it under a fixed set of configurations.
//
// The point of committing this file is that the README quotes it and
// test/streams.test.js asserts it. A number in the README can therefore only be
// wrong if the test suite is also failing -- there is no third place for a claim
// to live.
//
//   node tools/measure-streams.mjs           rewrite vendor/MEASURED.json
//   node tools/measure-streams.mjs --check   fail if it is stale
//
// Unlike tools/fetch-streams.mjs this needs no network: it reads the vendored
// slices already in the repository, so CI runs it.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { STREAMS, loadVendored } from '../src/streams/vendored.js';
import { latenessProfile } from '../src/core/oracle.js';
import { runStream } from '../src/run.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const OUT = path.join(ROOT, 'vendor', 'MEASURED.json');

/**
 * The configurations every stream is run under. Two fixed bounds, one adaptive.
 * The 1s bound is the one a developer picks after looking at a tidy stream; the
 * 60s bound is what they pick after being burned once; the percentile strategy
 * is what happens when the bound is measured instead of guessed. Running all
 * three against all three streams is what makes the mismatch visible.
 */
export const CONFIGS = [
  { id: 'bounded-1s', watermark: { name: 'bounded', boundMs: 1000 } },
  { id: 'bounded-60s', watermark: { name: 'bounded', boundMs: 60000 } },
  { id: 'percentile-p99', watermark: { name: 'percentile', percentile: 0.99, sampleSize: 512, floorMs: 1000 } },
];

export function measure() {
  const streams = STREAMS.map((meta) => {
    const events = loadVendored(meta.id);
    const lp = latenessProfile(events);
    let minT = Infinity;
    let maxT = -Infinity;
    for (const e of events) { if (e.t < minT) minT = e.t; if (e.t > maxT) maxT = e.t; }
    const distinctEventTimes = new Set(events.map((e) => e.t)).size;
    const runs = CONFIGS.map((cfg) => {
      const run = runStream(events, {
        windowMs: meta.suggestedWindowMs,
        allowedLatenessMs: meta.suggestedWindowMs,
        latePolicy: 'update',
        termination: 'idle',
        watermark: cfg.watermark,
      });
      return {
        config: cfg.id,
        summary: run.verdict.summary,
        reasons: run.verdict.reasons,
        findings: run.verdict.findings.length,
        findingCounts: run.verdict.findingCounts,
        watermarkOvershoots: run.result.watermarkOvershoots,
        rawWatermarkRegressions: run.result.watermarkRegressions,
        sideOutput: run.result.sideOutput.length,
        revisionsEmitted: run.result.counters.revisionsEmitted,
        // The three conditions an earlier version of the checker mistook for
        // engine defects. They are recorded, not hidden, because each is a real
        // and interesting property of the run -- what changed is the verdict
        // drawn from them, not whether they happen. See the README section on
        // false positives.
        openedAfterClose: run.result.counters.openedAfterClose,
        windowsNoPaneAllReceipted: run.verdict.windows.filter(
          (w) => w.panes === 0 && w.reason === 'lateness-bound-exceeded'
        ).length,
      };
    });
    // The inclusive-boundary experiment: the same bound of 0, with and without
    // the -1. On a stream whose event times are all distinct the two must agree
    // exactly; on one with many events per timestamp they cannot.
    const boundary = ['bounded', 'bounded-naive'].map((name) => {
      const run = runStream(events, {
        windowMs: meta.suggestedWindowMs,
        allowedLatenessMs: meta.suggestedWindowMs,
        latePolicy: 'update',
        termination: 'idle',
        watermark: { name, boundMs: 0 },
      });
      return {
        strategy: name,
        lateEvents: run.result.counters.lateInBound + run.result.counters.tooLate,
        lateInBound: run.result.counters.lateInBound,
        tooLate: run.result.counters.tooLate,
      };
    });
    return {
      id: meta.id,
      title: meta.title,
      source: meta.source,
      events: events.length,
      eventTimeSpanMs: maxT - minT,
      distinctEventTimes,
      eventsPerDistinctTime: Number((events.length / distinctEventTimes).toFixed(2)),
      windowMs: meta.suggestedWindowMs,
      lateness: lp,
      runs,
      boundary,
      naiveExtraLate: boundary[1].lateEvents - boundary[0].lateEvents,
    };
  });
  const totalFindings = streams.reduce((a, s) => a + s.runs.reduce((b, r) => b + r.findings, 0), 0);
  return { generatedBy: 'tools/measure-streams.mjs', configs: CONFIGS.map((c) => c.id), streams, totalFindings };
}

function main() {
  const checkOnly = process.argv.includes('--check');
  const text = JSON.stringify(measure(), null, 2) + '\n';
  if (checkOnly) {
    let current = null;
    try {
      current = fs.readFileSync(OUT, 'utf8');
    } catch {
      process.stderr.write('measure-streams: vendor/MEASURED.json is missing; run `npm run measure`\n');
      return 1;
    }
    if (current !== text) {
      process.stderr.write('measure-streams: vendor/MEASURED.json is stale; run `npm run measure` and commit it\n');
      return 1;
    }
    process.stdout.write('measure-streams: vendor/MEASURED.json is up to date\n');
    return 0;
  }
  fs.writeFileSync(OUT, text);
  process.stdout.write('measure-streams: wrote vendor/MEASURED.json (' + text.length + ' bytes)\n');
  return 0;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  process.exitCode = main();
}
