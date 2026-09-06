// One run: engine, oracle, checker, in that order, over one event log.
//
// This is the only place the three are wired together, so there is exactly one
// answer to "what does flume do with a stream" and every caller -- the CLI, the
// tests, the fixtures, the demo page -- gets the same one.

import { runEngine } from './core/engine.js';
import { batchWindows, latenessProfile } from './core/oracle.js';
import { checkRun } from './core/checker.js';
import { splitFlags } from './bugs.js';

/**
 * @param {{t: number, p?: number|null, k?: string, key?: string}[]} events arrival order
 * @param {import('./core/engine.js').EngineOptions & {flags?: Record<string, boolean>}} [options]
 */
export function runStream(events, options = {}) {
  const flags = options.flags ?? {};
  const split = splitFlags(flags);
  const result = runEngine(events, { ...options, flags: split.engine });
  const oracle = batchWindows(events, {
    windowMs: result.config.windowMs,
    offsetMs: result.config.offsetMs,
  });
  const verdict = checkRun(result, oracle, { sabotage: split.sabotage });
  return { result, oracle, verdict, lateness: latenessProfile(events) };
}

/**
 * The stable, byte-comparable form of a run. Two runs of the same log with the
 * same configuration must produce identical strings from this -- that is the
 * determinism claim, and test/determinism.test.js asserts exactly it.
 *
 * The trace is included: a watermark that advanced differently is a different
 * run even if the windows came out the same, and hiding that would make the
 * determinism claim weaker than it sounds.
 *
 * @param {ReturnType<typeof runStream>} run
 * @returns {string}
 */
export function serialiseRun(run) {
  return JSON.stringify(
    {
      config: run.result.config,
      counters: run.result.counters,
      finalWatermark: fmt(run.result.finalWatermark),
      windows: run.result.windows.map((w) => ({
        ...w,
        firedAtWatermark: fmt(w.firedAtWatermark),
        collectedAtWatermark: fmt(w.collectedAtWatermark),
      })),
      panes: run.result.panes.map((p) => ({ ...p, watermark: fmt(p.watermark) })),
      sideOutput: run.result.sideOutput,
      trace: run.result.trace.map((s) => ({ ...s, watermark: fmt(s.watermark) })),
      verdict: {
        summary: run.verdict.summary,
        reasons: run.verdict.reasons,
        findings: run.verdict.findings,
        windows: run.verdict.windows,
      },
      lateness: run.lateness,
    },
    null,
    1
  );
}

/**
 * JSON cannot hold Infinity, and a watermark of +/-infinity is a real and
 * meaningful value here, so it is rendered as a string rather than silently
 * becoming null.
 * @param {number|null} v
 */
function fmt(v) {
  if (v === null) return null;
  if (v === Number.POSITIVE_INFINITY) return '+inf';
  if (v === Number.NEGATIVE_INFINITY) return '-inf';
  return v;
}
