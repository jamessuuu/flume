#!/usr/bin/env node
// A small bundler, and the reason it exists rather than a dependency.
//
// The demo page has to be one static file a stranger can open from disk. ES
// modules do not load over file://, so the page needs a single classic
// <script>. Rather than add a build toolchain to a project whose whole argument
// is that it is verifiable, this walks the import graph and rewrites a
// deliberately tiny subset of module syntax:
//
//   import { a, b } from './x.js';   ->  const { a, b } = __req('x.js');
//   export function f                ->  function f          (+ registered)
//   export const C                   ->  const C             (+ registered)
//   export class K                   ->  class K             (+ registered)
//
// Anything else -- default exports, namespace imports, re-exports, dynamic
// import, a bare specifier -- throws. A bundler that silently mis-handles a
// form it does not understand would be worse than no bundler, so this one
// refuses instead.
//
// It also embeds the three vendored real streams, so the page runs the same
// engine over the same data as the CLI and the two cannot disagree.
//
//   node tools/bundle.mjs           write web/flume.bundle.js
//   node tools/bundle.mjs --check   fail if the committed bundle is stale

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { STREAMS } from '../src/streams/vendored.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const ENTRY = path.join(ROOT, 'web', 'app.js');
const OUT = path.join(ROOT, 'web', 'flume.bundle.js');

const IMPORT_RE = /^import\s*\{([^}]*)\}\s*from\s*['"]([^'"]+)['"];?\s*$/;
const EXPORT_FN_RE = /^export\s+(?:async\s+)?function\s+([A-Za-z_$][\w$]*)/;
const EXPORT_CONST_RE = /^export\s+(?:const|let|var)\s+([A-Za-z_$][\w$]*)/;
const EXPORT_CLASS_RE = /^export\s+class\s+([A-Za-z_$][\w$]*)/;

/**
 * @param {string} file absolute path
 * @param {Map<string, {id: string, code: string}>} modules
 * @returns {string} module id
 */
function load(file, modules) {
  const id = path.relative(ROOT, file).split(path.sep).join('/');
  if (modules.has(id)) return id;
  modules.set(id, { id, code: '' }); // placeholder, breaks import cycles
  const src = fs.readFileSync(file, 'utf8');
  const lines = src.split('\n');
  /** @type {string[]} */
  const exports = [];
  /** @type {string[]} */
  const out = [];
  let inBlockComment = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    // Only skip statements, never rewrite inside a block comment: several
    // modules describe their own export forms in prose.
    if (inBlockComment) {
      if (trimmed.includes('*/')) inBlockComment = false;
      out.push(line);
      continue;
    }
    if (trimmed.startsWith('/*')) {
      if (!trimmed.includes('*/')) inBlockComment = true;
      out.push(line);
      continue;
    }

    if (trimmed.startsWith('import')) {
      const m = IMPORT_RE.exec(trimmed);
      if (!m) {
        throw new Error(
          id + ':' + (i + 1) + ': unsupported import form for this bundler:\n  ' + trimmed +
            "\nOnly `import { a, b } from './rel.js';` is supported."
        );
      }
      const spec = m[2];
      if (!spec.startsWith('.')) {
        throw new Error(
          id + ':' + (i + 1) + ': bare import "' + spec + '". The demo page runs in a browser, so ' +
            'nothing it reaches may depend on a Node builtin or a package.'
        );
      }
      const depId = load(path.resolve(path.dirname(file), spec), modules);
      out.push('const {' + m[1] + '} = __req(' + JSON.stringify(depId) + ');');
      continue;
    }

    if (trimmed.startsWith('export')) {
      let m;
      if ((m = EXPORT_FN_RE.exec(trimmed))) exports.push(m[1]);
      else if ((m = EXPORT_CLASS_RE.exec(trimmed))) exports.push(m[1]);
      else if ((m = EXPORT_CONST_RE.exec(trimmed))) exports.push(m[1]);
      else throw new Error(id + ':' + (i + 1) + ': unsupported export form for this bundler:\n  ' + trimmed);
      out.push(line.replace(/^(\s*)export\s+/, '$1'));
      continue;
    }
    out.push(line);
  }

  for (const name of exports) out.push('__exports.' + name + ' = ' + name + ';');
  modules.set(id, { id, code: out.join('\n') });
  return id;
}

/**
 * The vendored slices, embedded so the page computes over the same bytes the
 * CLI does. Normalised to LF before embedding: without this the bundle's bytes
 * depend on how the checkout translated line endings, and `--check` reports a
 * stale bundle on a clean clone on the other platform.
 */
function embedStreams() {
  /** @type {Record<string, any>} */
  const out = {};
  for (const meta of STREAMS) {
    const file = path.join(ROOT, 'vendor', meta.id, 'events.ndjson');
    const text = fs.readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
    const events = text.split('\n').filter(Boolean).map((l) => JSON.parse(l));
    out[meta.id] = {
      title: meta.title,
      source: meta.source,
      licence: meta.licence,
      eventTime: meta.eventTime,
      processingTime: meta.processingTime,
      windowMs: meta.suggestedWindowMs,
      events,
    };
  }
  return out;
}

export function build() {
  /** @type {Map<string, {id: string, code: string}>} */
  const modules = new Map();
  const entryId = load(ENTRY, modules);
  const parts = [];
  parts.push('// GENERATED by tools/bundle.mjs -- edit web/app.js or src/, then run `npm run build:web`.');
  parts.push('(function () {');
  parts.push('"use strict";');
  parts.push('var __defs = {};');
  parts.push('var __cache = {};');
  parts.push('function __req(id) {');
  parts.push('  if (__cache[id]) return __cache[id];');
  parts.push('  var m = { exports: {} };');
  parts.push('  __cache[id] = m.exports;');
  parts.push('  __defs[id](m.exports, __req);');
  parts.push('  return m.exports;');
  parts.push('}');
  parts.push('var FLUME_STREAMS = ' + JSON.stringify(embedStreams()) + ';');
  parts.push('if (typeof window !== "undefined") window.FLUME_STREAMS = FLUME_STREAMS;');
  for (const mod of modules.values()) {
    parts.push('__defs[' + JSON.stringify(mod.id) + '] = function (__exports, __req) {');
    parts.push(mod.code);
    parts.push('};');
  }
  parts.push('__req(' + JSON.stringify(entryId) + ');');
  parts.push('})();');
  return parts.join('\n') + '\n';
}

function main() {
  const checkOnly = process.argv.includes('--check');
  let bundle;
  try {
    bundle = build();
  } catch (err) {
    process.stderr.write('bundle: ' + /** @type {Error} */ (err).message + '\n');
    return 1;
  }
  if (checkOnly) {
    let current = null;
    try {
      current = fs.readFileSync(OUT, 'utf8');
    } catch {
      process.stderr.write('bundle: web/flume.bundle.js is missing; run `npm run build:web`\n');
      return 1;
    }
    if (current !== bundle) {
      process.stderr.write('bundle: web/flume.bundle.js is stale; run `npm run build:web` and commit the result\n');
      return 1;
    }
    process.stdout.write('bundle: up to date (' + bundle.length + ' bytes)\n');
    return 0;
  }
  fs.writeFileSync(OUT, bundle);
  process.stdout.write('bundle: wrote web/flume.bundle.js (' + bundle.length + ' bytes)\n');
  return 0;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  process.exitCode = main();
}
