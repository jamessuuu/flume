#!/usr/bin/env node
// A small project-specific gate, not a style opinion engine.
//
// Every rule here is one that would actually cost something if broken: a stray
// console.log in a library, a TODO shipped as documentation, a source file with
// no explanation at the top, a build flag nobody reads, a clock inside the
// deterministic core. Formatting arguments are deliberately absent.
//
//   node tools/lint.mjs

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const MAX_LINE = 120;

/** @param {string} dir @param {string[]} [acc] @returns {string[]} */
function walk(dir, acc = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (['node_modules', '.git', 'vendor', '.playwright-mcp', '.vercel'].includes(entry.name)) continue;
      walk(full, acc);
    } else if (/\.(js|mjs)$/.test(entry.name)) {
      if (entry.name === 'flume.bundle.js') continue; // generated
      acc.push(full);
    }
  }
  return acc;
}

function main() {
  /** @type {string[]} */
  const problems = [];
  const files = walk(ROOT);

  for (const file of files) {
    const rel = path.relative(ROOT, file).split(path.sep).join('/');
    const text = fs.readFileSync(file, 'utf8');
    const lines = text.split('\n');
    const isLibrary = rel.startsWith('src/') && rel !== 'src/cli.js';
    // This file necessarily contains the strings it forbids. A linter that
    // cannot describe its own rules is not a useful linter.
    const isSelf = rel === 'tools/lint.mjs';

    if (!/^(\/\/|\/\*|#!)/.test(text)) {
      problems.push(rel + ':1 - file does not start with a comment explaining what it is');
    }

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const at = rel + ':' + (i + 1);
      if (line.includes('\t')) problems.push(at + ' - literal tab');
      if (/\s+$/.test(line)) problems.push(at + ' - trailing whitespace');
      if (line.length > MAX_LINE) problems.push(at + ' - line is ' + line.length + ' chars (max ' + MAX_LINE + ')');
      if (!isSelf && /\bTODO\b|\bFIXME\b|\bXXX\b/.test(line)) {
        problems.push(at + ' - unresolved marker; either do it or write down why it is not done');
      }
      if (isLibrary && /\bconsole\s*\.\s*(log|debug|info)\s*\(/.test(line)) {
        problems.push(at + ' - console output from a library module; return the value instead');
      }
      if (!isSelf && /\bdebugger\b/.test(line)) problems.push(at + ' - debugger statement');
      if (line.indexOf(String.fromCharCode(0)) !== -1) problems.push(at + ' - NUL byte in source');
    }
    if (text.length > 0 && !text.endsWith('\n')) problems.push(rel + ' - no trailing newline');
  }

  // Every planted bug must be findable from src/bugs.js, and every flag must be
  // read somewhere real. A flag nobody checks is a fixture that cannot fire.
  const bugsSrc = fs.readFileSync(path.join(ROOT, 'src', 'bugs.js'), 'utf8');
  const flags = Array.from(bugsSrc.matchAll(/^\s*flag: '([A-Za-z]+)',$/gm)).map((m) => m[1]);
  if (flags.length === 0) problems.push('src/bugs.js - no build flags found; the parser above must have drifted');
  const elsewhere = files
    .filter((f) => !f.endsWith('bugs.js'))
    .map((f) => fs.readFileSync(f, 'utf8'))
    .join('\n');
  for (const flag of flags) {
    const readAsEngineFlag = new RegExp('flags\\.' + flag + '\\b').test(elsewhere);
    const readAsSabotage = flag === 'checkerRevisedAsComplete' && /revisedAsComplete/.test(elsewhere);
    if (!readAsEngineFlag && !readAsSabotage) {
      problems.push('src/bugs.js - build flag "' + flag + '" is declared but never read; it cannot fire');
    }
  }

  // Every finding code the checker can emit must be documented in FINDINGS, and
  // every documented code must be reachable. An undocumented accusation is not
  // an accusation anyone can act on.
  const checkerSrc = fs.readFileSync(path.join(ROOT, 'src', 'core', 'checker.js'), 'utf8');
  const documented = new Set(
    Array.from(checkerSrc.matchAll(/^\s*'([a-z-]+)':\s*'/gm)).map((m) => m[1])
  );
  const emitted = new Set(Array.from(checkerSrc.matchAll(/flag\(\s*'([a-z-]+)'/g)).map((m) => m[1]));
  for (const code of emitted) {
    if (!documented.has(code)) {
      problems.push('src/core/checker.js - finding "' + code + '" is emitted but not documented');
    }
  }
  for (const code of documented) {
    if (!emitted.has(code)) {
      problems.push('src/core/checker.js - finding "' + code + '" is documented but never emitted');
    }
  }

  if (problems.length > 0) {
    for (const p of problems) process.stderr.write('lint: ' + p + '\n');
    process.stderr.write('lint: ' + problems.length + ' problem(s)\n');
    return 1;
  }
  process.stdout.write('lint: ' + files.length + ' files, no problems\n');
  return 0;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  process.exitCode = main();
}
