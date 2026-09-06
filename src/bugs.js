// The planted fixtures, all of them, in one file.
//
// A checker whose failure path has never been exercised is decoration. These
// are the four ways this project is deliberately broken so that the checker can
// be watched to catch them. Every one is a build flag read somewhere real --
// tools/lint.mjs fails if a flag is declared here and never read, because a
// flag nobody checks is a fixture that cannot fire.
//
// Three break the engine. The fourth breaks the CHECKER, which is the one that
// matters: catching engine bugs proves the checker fires, not that it is right.

/**
 * @typedef {object} Bug
 * @property {string} id
 * @property {string} flag camelCase name the code reads
 * @property {'engine'|'checker'} target
 * @property {string} title
 * @property {string} why what makes this the realistic version of the mistake
 */

/** @type {Bug[]} */
export const BUGS = [
  {
    id: 'processing-time-watermark',
    flag: 'processingTimeWatermark',
    target: 'engine',
    title: 'the watermark advances on processing time, not event time',
    why:
      'The classic one. On a stream that arrives roughly in order at a roughly steady rate a ' +
      'processing-time watermark tracks the real one closely enough to pass a demo and a staging ' +
      'soak. It fails the first time the source stalls and catches up, and the numbers are quietly ' +
      'wrong from then on.',
  },
  {
    id: 'close-without-lateness',
    flag: 'closeWithoutLateness',
    target: 'engine',
    title: 'window state is released the moment the window fires',
    why:
      'Allowed lateness becomes a setting that appears in the config, is printed in the report, and ' +
      'does nothing. Late data inside the bound finds no state to update, so a stream that should ' +
      'produce revisions produces none -- and "no revisions" reads like "no lateness".',
  },
  {
    id: 'silent-drop',
    flag: 'silentDrop',
    target: 'engine',
    title: 'events past the allowed lateness are dropped with no counter and no receipt',
    why:
      'The most dangerous engine bug of the three, because the output looks identical to a healthy ' +
      'run. Nothing in the panes, the counters or the side output records the loss. Only a ' +
      'comparison against the batch oracle can find it, which is the argument for having an oracle.',
  },
  {
    id: 'checker-revised-as-complete',
    flag: 'checkerRevisedAsComplete',
    target: 'checker',
    title: 'the checker reports a window it knows was revised as complete',
    why:
      'This is the fixture that carries the project. The other three prove the checker fires; this ' +
      'one proves the complete/revised distinction is load-bearing. With it on, every window that ' +
      'late data corrected is reported as having been right the first time -- the exact lie a ' +
      'streaming report is most likely to tell, and the one nobody downstream can detect.',
  },
];

export const BUG_IDS = BUGS.map((b) => b.id).sort();

/** A build with nothing planted. */
export function correctBuild() {
  return /** @type {Record<string, boolean>} */ ({});
}

/**
 * @param {string|undefined} spec comma-separated bug ids, or undefined
 * @returns {Record<string, boolean>}
 */
export function parseBuildFlags(spec) {
  const flags = correctBuild();
  if (!spec) return flags;
  for (const raw of String(spec).split(',')) {
    const id = raw.trim();
    if (!id) continue;
    const bug = BUGS.find((b) => b.id === id);
    if (!bug) {
      throw new Error('unknown build flag "' + id + '"; known flags: ' + BUG_IDS.join(', '));
    }
    flags[bug.flag] = true;
  }
  return flags;
}

/** @param {Record<string, boolean>} flags */
export function formatBuildFlags(flags) {
  const on = BUGS.filter((b) => flags[b.flag]).map((b) => b.id);
  return on.length ? on.join(',') : 'correct';
}

/**
 * Split the flags into the two places they are consumed. The engine must never
 * see the checker's sabotage flag and the checker must never see the engine's,
 * or a fixture could "fire" through the wrong door.
 *
 * @param {Record<string, boolean>} flags
 */
export function splitFlags(flags) {
  return {
    engine: {
      processingTimeWatermark: !!flags.processingTimeWatermark,
      closeWithoutLateness: !!flags.closeWithoutLateness,
      silentDrop: !!flags.silentDrop,
    },
    sabotage: {
      revisedAsComplete: !!flags.checkerRevisedAsComplete,
    },
  };
}
