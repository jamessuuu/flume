// The checker: three outcomes per window, and findings against the engine.
//
// Every window gets exactly one of:
//
//   complete     -- closed by the watermark, and the value it emitted is the
//                   value the oracle computes over the whole log. Nothing that
//                   arrived later changed it.
//   revised      -- late data arrived within allowed lateness and changed an
//                   already-emitted pane. The retraction chain is recorded, and
//                   the final pane agrees with the oracle.
//   unverifiable -- the emitted panes cannot be shown to be right. Two ways to
//                   get here, and they are different enough to keep separate in
//                   the `reason`: the watermark never reached the window (the
//                   stream ended first), or data arrived past the allowed
//                   lateness so the window's true value is not recoverable from
//                   what was emitted.
//
// A finding is stronger than an outcome: it is an accusation against the
// ENGINE, not a statement about a window. The distinction matters, and getting
// it wrong is what the three real streams in vendor/ caught -- see the README
// section on false positives. A window whose value is unknown because the
// configured lateness bound is four orders of magnitude too small for the
// stream is `unverifiable` with a reason. It is NOT a defect finding, because
// the engine did exactly what it was told. Only findings mean "this code is
// wrong".

import { parseWindowKey } from './window.js';

/** The three outcomes. There is no fourth, and no window may go unclassified. */
export const OUTCOMES = ['complete', 'revised', 'unverifiable'];

/** Why a window ended up unverifiable. */
export const UNVERIFIABLE_REASONS = [
  'watermark-never-reached',
  'lateness-bound-exceeded',
  'policy-discard',
  'unexplained-shortfall',
  'no-pane-after-close',
];

/**
 * Finding codes, each an accusation against the engine with a fixed meaning.
 * `severity` is always 'defect'; a condition that is not a defect does not get
 * a finding at all, it gets an outcome and a reason.
 */
export const FINDINGS = {
  'watermark-overshoot':
    'the watermark rose above the highest event time observed so far, which no strategy ' +
    'derived from observed event times can do',
  'watermark-regression': 'a strategy that claims its raw watermark can never fall produced one that did',
  'silent-drop':
    'events were discarded with neither a counter nor a side-output receipt, so their loss ' +
    'is invisible in the output',
  'premature-close': 'window state was released before the configured allowed lateness had elapsed in watermark time',
  'pane-count-mismatch':
    'under the update policy the number of panes does not equal the number of firings that ' +
    'were owed; the usual cause is late data inside the bound that produced no revision',
  'pane-chain-broken': 'a revision pane retracts a value that is not the value the previous pane emitted',
  'unexplained-shortfall':
    'the final pane disagrees with the oracle by more than the receipted discards can ' +
    'account for',
  'no-pane-after-close':
    'the watermark passed the window end, the window had events, and not one of them was ' +
    'either emitted or receipted',
};

/**
 * @param {ReturnType<import('./engine.js').runEngine>} result
 * @param {ReturnType<import('./oracle.js').batchWindows>} oracle
 * @param {{sabotage?: {revisedAsComplete?: boolean}}} [opts]
 */
export function checkRun(result, oracle, opts = {}) {
  const sabotage = opts.sabotage ?? {};
  /** @type {{code: string, severity: 'defect', window: string|null, detail: string}[]} */
  const findings = [];
  /** @param {string} code @param {string|null} window @param {string} detail */
  const flag = (code, window, detail) => {
    findings.push({ code, severity: 'defect', window, detail });
  };

  // A falling raw watermark only accuses the engine when the strategy promised
  // it could not happen. An adaptive bound falls every time its sample forgets
  // an outlier; calling that a defect produced a finding on every real stream
  // and was the first of the three false positives vendor/ found.
  if (result.watermarkRegressions > 0 && result.watermarkMonotonicByConstruction) {
    flag('watermark-regression', null,
      result.watermarkRegressions + ' regression(s) from a strategy whose raw watermark cannot fall, largest ' +
      result.maxWatermarkRegressionMs + 'ms');
  }
  // The structural test for a watermark that is not a function of the data.
  // Every honest strategy emits maxSeen minus a non-negative bound, so it is
  // bounded above by maxSeen at all times. Overshooting is impossible for such
  // a strategy and unavoidable for one driven by arrival rate, which makes this
  // exact where a violation-rate threshold would only have been suggestive.
  if (result.watermarkOvershoots > 0) {
    flag('watermark-overshoot', null,
      result.watermarkOvershoots + ' event(s) processed while the watermark stood above the highest ' +
      'event time yet seen; largest overshoot ' + result.maxWatermarkOvershootMs + 'ms');
  }
  if (result.counters.discardedWithoutReceipt > 0) {
    flag('silent-drop', null,
      result.counters.discardedWithoutReceipt + ' event(s) discarded with no counter and no receipt');
  }

  // Allowed lateness is a promise about watermark time: state survives until
  // the watermark passes end + allowedLateness. A window released earlier than
  // that had its promise broken, whatever the output looks like afterwards.
  for (const w of result.windows) {
    if (!w.collected || w.collectedAtWatermark === null) continue;
    const owedUntil = w.end + result.config.allowedLatenessMs;
    if (w.collectedAtWatermark < owedUntil) {
      flag('premature-close', w.window,
        'released at watermark ' + w.collectedAtWatermark + ', ' + (owedUntil - w.collectedAtWatermark) +
        'ms before the ' + result.config.allowedLatenessMs + 'ms of allowed lateness had elapsed');
    }
  }

  // Pane chains must be internally consistent: a revision says what it
  // replaces, and that has to be what the previous pane emitted.
  /** @type {Map<string, number>} */
  const lastPaneValue = new Map();
  for (const pane of result.panes) {
    if (pane.kind === 'revision') {
      const prev = lastPaneValue.get(pane.window);
      if (prev === undefined || pane.retracts !== prev) {
        flag('pane-chain-broken', pane.window,
          'pane ' + pane.seq + ' retracts ' + pane.retracts + ' but the previous pane emitted ' + prev);
      }
    }
    lastPaneValue.set(pane.window, pane.count);
  }

  /** @type {Map<string, any>} */
  const engineByWindow = new Map(result.windows.map((w) => [w.window, w]));
  /** @type {Map<string, number>} */
  const sideOutByWindow = new Map();
  for (const s of result.sideOutput) {
    sideOutByWindow.set(s.window, (sideOutByWindow.get(s.window) ?? 0) + 1);
  }

  const allWindows = new Set([...oracle.byKey.keys(), ...engineByWindow.keys()]);
  /** @type {{window: string, start: number, end: number, outcome: string, reason: string|null,
   *          expected: number, emitted: number|null, panes: number, detail: string}[]} */
  const windows = [];

  for (const key of allWindows) {
    const expected = oracle.byKey.get(key) ?? 0;
    const eng = engineByWindow.get(key);
    // Window keys can hold negative starts ("-60000-0"), so they are parsed by
    // the module that formats them, never by splitting on the dash here.
    const bounds = parseWindowKey(key);
    const start = eng ? eng.start : bounds.start;
    const end = eng ? eng.end : bounds.end;
    const emitted = eng ? eng.emitted : null;
    const panes = eng ? eng.panes : 0;
    const sideOut = sideOutByWindow.get(key) ?? 0;
    // Under a discard policy the engine counts what it threw away but does not
    // list it per window, so attribute the policy drops proportionally: the
    // per-window in-bound late count IS the per-window discard count when the
    // policy is drop or side-output.
    const policyDiscarded = eng && result.config.latePolicy !== 'update' ? eng.lateInBound : 0;
    const receipted = sideOut + policyDiscarded;

    /** @param {string} outcome @param {string|null} reason @param {string} detail */
    const push = (outcome, reason, detail) => {
      windows.push({ window: key, start, end, outcome, reason, expected, emitted, panes, detail });
    };

    if (panes === 0) {
      if (result.finalWatermark < end) {
        push('unverifiable', 'watermark-never-reached',
          'the watermark stopped at ' + fmtWatermark(result.finalWatermark) + ', ' +
          (end - result.finalWatermark) + 'ms short of this window\'s end (' + end + ')');
      } else if (receipted >= expected && expected > 0) {
        // The watermark ran past this window before ANY of its events arrived,
        // and every one of them was receipted to the side output. That is the
        // configured lateness bound being too small for the stream, not a
        // defect: nothing was lost silently and the reason is on the record.
        // Flagging it was the second false positive vendor/ found -- 38 of 38
        // such windows on the USGS stream were fully receipted.
        push('unverifiable', 'lateness-bound-exceeded',
          'the watermark passed this window before any of its ' + expected +
          ' events arrived; all ' + receipted + ' are receipted in the side output');
      } else {
        flag('no-pane-after-close', key,
          'watermark ' + fmtWatermark(result.finalWatermark) + ' >= end ' + end + ', oracle has ' +
          expected + ' event(s), only ' + receipted + ' receipted, and no pane fired');
        push('unverifiable', 'no-pane-after-close',
          'the watermark closed this window, nothing was emitted, and ' + (expected - receipted) +
          ' event(s) are unaccounted for');
      }
      continue;
    }

    // Under the update policy the pane count is an identity, not a heuristic:
    // one pane for the close, plus one for every in-bound late event -- unless
    // the window's first event arrived after the watermark had already passed
    // its end, in which case that event's pane IS the close. Checking
    // `lateInBound > 0 && panes === 1` instead was the third false positive
    // vendor/ found: it fired on all 20 opened-after-close windows in the USGS
    // stream, every one of which was behaving correctly.
    if (eng && result.config.latePolicy === 'update' && eng.fired) {
      const owed = (eng.openedAfterClose ? 0 : 1) + eng.lateInBound;
      if (panes !== owed) {
        flag('pane-count-mismatch', key,
          panes + ' pane(s) for ' + eng.lateInBound + ' in-bound late event(s)' +
          (eng.openedAfterClose ? ' on a window opened after its close' : '') + '; expected ' + owed);
      }
    }

    if (emitted === expected) {
      if (panes > 1) {
        // The sabotaged checker reports a window it KNOWS was revised as
        // complete. That is the single most dangerous lie this program could
        // tell, because it turns "we corrected it" into "it was right the
        // first time", and test/sabotage.test.js asserts it fires.
        if (sabotage.revisedAsComplete) {
          push('complete', null, 'closed by the watermark at ' + fmtWatermark(eng.firedAtWatermark));
        } else {
          push('revised', null,
            panes + ' panes; late data changed the value under the ' + result.config.latePolicy +
            ' policy, final pane agrees with the batch oracle');
        }
      } else {
        push('complete', null,
          'closed by the watermark at ' + fmtWatermark(eng.firedAtWatermark) +
          ', value agrees with the batch oracle');
      }
      continue;
    }

    // The emitted value and the oracle disagree. The only question that
    // matters is whether the difference is receipted.
    const shortfall = expected - (emitted ?? 0);
    if (shortfall === receipted && receipted > 0) {
      const reason = sideOut > 0 && policyDiscarded === 0 ? 'lateness-bound-exceeded' : 'policy-discard';
      push('unverifiable', reason,
        'emitted ' + emitted + ', oracle ' + expected + '; the ' + shortfall +
        ' missing event(s) are receipted (' + sideOut + ' side-output, ' + policyDiscarded +
        ' policy-discarded). The engine obeyed its configuration; the window\'s true value is ' +
        'not recoverable from what was emitted.');
    } else {
      flag('unexplained-shortfall', key,
        'emitted ' + emitted + ', oracle ' + expected + ', receipted discards ' + receipted +
        ' (difference of ' + (shortfall - receipted) + ' unaccounted for)');
      push('unverifiable', 'unexplained-shortfall',
        'emitted ' + emitted + ', oracle ' + expected + ', only ' + receipted + ' receipted');
    }
  }

  windows.sort((a, b) => (a.start - b.start) || (a.end - b.end));

  const summary = { complete: 0, revised: 0, unverifiable: 0 };
  /** @type {Record<string, number>} */
  const reasons = {};
  for (const w of windows) {
    summary[/** @type {'complete'|'revised'|'unverifiable'} */ (w.outcome)]++;
    if (w.reason) reasons[w.reason] = (reasons[w.reason] ?? 0) + 1;
  }

  /** @type {Record<string, number>} */
  const findingCounts = {};
  for (const f of findings) findingCounts[f.code] = (findingCounts[f.code] ?? 0) + 1;

  return {
    windows,
    summary,
    reasons,
    findings,
    findingCounts,
    clean: findings.length === 0,
    total: windows.length,
  };
}

/** @param {number|null} w */
function fmtWatermark(w) {
  if (w === null) return 'never';
  if (w === Number.POSITIVE_INFINITY) return '+inf (end of stream)';
  if (w === Number.NEGATIVE_INFINITY) return '-inf (never advanced)';
  return String(w);
}
