// The demo page.
//
// The page is the artefact most likely to rot, because it is the one nobody
// runs a test against by habit. These assertions cover the three ways it can be
// quietly wrong: the bundle drifting from the source it was built from, the
// page losing the credit it owes, and the default view no longer showing the
// thing the whole project is about.

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { build } from '../tools/bundle.mjs';
import { runStream } from '../src/run.js';
import { demoStream, DEMO_CONFIG, DEMO_EXPECTED } from '../src/streams/demo.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const WEB = path.join(ROOT, 'web');

test('the committed bundle matches what the sources build', () => {
  const committed = fs.readFileSync(path.join(WEB, 'flume.bundle.js'), 'utf8');
  const built = build();
  // Compared with assert.ok rather than assert.equal on purpose: the bundle is
  // most of a megabyte, and a failing assert.equal would print a diff of it.
  // The first differing offset is what a developer actually needs.
  if (committed !== built) {
    let at = 0;
    while (at < committed.length && at < built.length && committed[at] === built[at]) at++;
    assert.fail(
      'web/flume.bundle.js is stale: run `npm run build:web` and commit it. ' +
      'Committed ' + committed.length + ' bytes, freshly built ' + built.length +
      ' bytes, first difference at offset ' + at + ': committed ' +
      JSON.stringify(committed.slice(at, at + 60)) + ' vs built ' +
      JSON.stringify(built.slice(at, at + 60))
    );
  }
});

test('the page loads exactly one script, and it is the bundle', () => {
  const html = fs.readFileSync(path.join(WEB, 'index.html'), 'utf8');
  const scripts = [...html.matchAll(/<script[^>]*src="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(scripts, ['flume.bundle.js']);
  // No server, no CDN, no font host, no analytics: the page must work from a
  // file:// URL on a machine with no network.
  assert.ok(!/https?:\/\/[^"']*\.(js|css)/.test(html), 'the page must not fetch remote code or styles');
  assert.ok(!html.includes('<script>') || html.indexOf('<script>') === -1, 'no inline script');
});

test('the Dataflow credit is on the first screen, above the fold, before any claim', () => {
  const html = fs.readFileSync(path.join(WEB, 'index.html'), 'utf8');
  const credit = html.indexOf('Akidau');
  assert.ok(credit > 0, 'the page must credit the Dataflow model by name');
  assert.match(html, /Dataflow Model.{0,40}VLDB 2015/s);
  const controls = html.indexOf('id="controls"');
  assert.ok(credit < controls, 'the credit must appear before the interactive part, not after it');
});

test('the page states what it is not', () => {
  const html = fs.readFileSync(path.join(WEB, 'index.html'), 'utf8');
  assert.match(html, /not Flink, not Beam, and not Kafka Streams/);
  assert.match(html, /no checkpointing/);
  assert.match(html, /no exactly-once/);
});

test('the default view shows a late event and all three outcomes', () => {
  // The page defaults to the demo log with DEMO_CONFIG, so what a first-time
  // visitor sees is exactly this run. If it ever stops containing a revision,
  // the front page stops making the argument.
  const run = runStream(demoStream(), DEMO_CONFIG);
  assert.deepEqual(run.verdict.summary, DEMO_EXPECTED);
  assert.ok(run.verdict.summary.revised >= 1, 'the default view must contain a late event that revised a window');
  assert.ok(run.result.sideOutput.length >= 1, 'and one that arrived too late to be taken');
});

test('the page defaults agree with the demo configuration the CLI uses', () => {
  const app = fs.readFileSync(path.join(WEB, 'app.js'), 'utf8');
  assert.match(app, /source: 'demo'/, 'the page must default to the demo log');
  assert.match(app, /policy: 'update'/, 'the default policy must be the one that produces a revision');
});

test('every control the page offers is reflected in the URL', () => {
  const app = fs.readFileSync(path.join(WEB, 'app.js'), 'utf8');
  const html = fs.readFileSync(path.join(WEB, 'index.html'), 'utf8');
  const named = [...html.matchAll(/<(?:select|input) name="([a-z]+)"/g)].map((m) => m[1]);
  assert.ok(named.length >= 8, 'expected the page to expose the configuration');
  const defaults = /const DEFAULTS = \{([\s\S]*?)\};/.exec(app);
  assert.ok(defaults);
  for (const name of named) {
    assert.ok(
      new RegExp('^\\s*' + name + ':', 'm').test(defaults[1]),
      'control "' + name + '" has no default, so it cannot round-trip through the URL'
    );
  }
});

test('the bundle embeds the three real streams, so the page and the CLI agree', () => {
  const bundle = fs.readFileSync(path.join(WEB, 'flume.bundle.js'), 'utf8');
  for (const id of ['gharchive', 'wikimedia', 'usgs']) {
    assert.ok(bundle.includes('"' + id + '":'), 'the bundle is missing the ' + id + ' stream');
  }
  assert.ok(bundle.includes('window.FLUME_STREAMS'));
});
