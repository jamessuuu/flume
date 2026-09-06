// The library surface.
//
// Everything a caller needs to run a log through the engine and get a verdict,
// and nothing that depends on the filesystem -- src/streams/vendored.js and
// src/cli.js are the only modules that read files, and neither is re-exported
// here, so this entry point works unchanged in a browser.

export { runStream, serialiseRun } from './run.js';
export { runEngine, LATE_POLICIES, TERMINATIONS, DEFAULTS } from './core/engine.js';
export { batchWindows, latenessProfile } from './core/oracle.js';
export { checkRun, OUTCOMES, UNVERIFIABLE_REASONS, FINDINGS } from './core/checker.js';
export { createWatermarkStrategy, STRATEGY_NAMES, WATERMARK_MIN, WATERMARK_MAX } from './core/watermark.js';
export { assignWindow, windowKey, parseWindowKey, compareWindows } from './core/window.js';
export { syntheticStream, orderedStream } from './streams/synthetic.js';
export { demoStream, DEMO_CONFIG, DEMO_EXPECTED } from './streams/demo.js';
export { BUGS, BUG_IDS, parseBuildFlags, formatBuildFlags, correctBuild, splitFlags } from './bugs.js';
