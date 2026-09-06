#!/usr/bin/env node
// Builds vendor/ from three public sources. Committed for provenance: the
// vendored slices are in the repository, but the exact derivation that produced
// them should be readable rather than taken on trust.
//
// This is NOT run in CI and not run by `npm test`. It needs the network, and the
// Wikimedia source is a live stream, so a second run produces a different (but
// equally valid) capture. The repository ships the capture that every number in
// the README was measured against.
//
//   node tools/fetch-streams.mjs               all three
//   node tools/fetch-streams.mjs gharchive     one of them
//
// What is vendored, and why so little: each output line is
// {"t": eventTimeMs, "p": processingTimeMs|null, "k": key}. No titles, no user
// names, no repository names, no comments, no payloads, no coordinates. Two
// timestamps and a low-cardinality label are all flume needs, and vendoring
// anything more would import a licensing question this project has no reason to
// take on. The licence position for each source is written into its SOURCE.md.

import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const VENDOR = path.join(ROOT, 'vendor');
const UA = 'flume/0.1 (portfolio project; https://github.com/jamessuuu/flume)';

/** GH Archive hour file to slice. Fixed so the vendored slice is reproducible. */
const GHARCHIVE_HOUR = '2026-01-15-3';
const SLICE = 5000;

/** @param {string} name @param {{t:number,p:number|null,k:string}[]} rows @param {string} sourceMd */
function writeStream(name, rows, sourceMd) {
  const dir = path.join(VENDOR, name);
  fs.mkdirSync(dir, { recursive: true });
  const body = rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
  fs.writeFileSync(path.join(dir, 'events.ndjson'), body, 'utf8');
  fs.writeFileSync(path.join(dir, 'SOURCE.md'), sourceMd, 'utf8');
  process.stdout.write(
    'wrote vendor/' + name + '/events.ndjson (' + rows.length + ' events, ' + body.length + ' bytes)\n'
  );
}

async function fetchGharchive() {
  const url = 'https://data.gharchive.org/' + GHARCHIVE_HOUR + '.json.gz';
  process.stdout.write('GET ' + url + '\n');
  const res = await fetch(url, { headers: { 'user-agent': UA } });
  if (!res.ok) throw new Error('gharchive: HTTP ' + res.status);
  const gz = Buffer.from(await res.arrayBuffer());
  const text = zlib.gunzipSync(gz).toString('utf8');
  const lines = text.split('\n').filter(Boolean);
  // A contiguous slice, not a sample. Sampling would destroy the arrival order,
  // and arrival order is the entire signal.
  const rows = lines.slice(0, SLICE).map((line) => {
    const o = JSON.parse(line);
    return { t: Date.parse(o.created_at), p: null, k: String(o.type) };
  });
  writeStream('gharchive', rows, `# GH Archive — 5,000 GitHub public events

**Source.** \`https://data.gharchive.org/${GHARCHIVE_HOUR}.json.gz\` — the GH
Archive hourly file for ${GHARCHIVE_HOUR.slice(0, 10)} 03:00 UTC, downloaded
2026-09-06. The full hour holds 141,671 events; this is the first 5,000 lines of
the file, a contiguous slice in the file's own order.

**Why the file's order is the arrival order.** GH Archive writes each hourly
file in the order GitHub's public events API returned the events. That order is
the processing order; \`created_at\` is the event time. The file carries no
per-event arrival timestamp, so \`p\` is null and flume uses the line's position,
which is the same information.

**What is vendored.** Per event: \`t\` (\`created_at\` parsed to epoch ms),
\`p\` (null), and \`k\` (the GitHub event type, e.g. \`PushEvent\`). Nothing else.
No repository names, no actor names, no payloads, no organisation names.

**Licence.** The GH Archive *project* is MIT licensed (Copyright 2012-2016 Ilya
Grigorik; \`https://github.com/igrigorik/gharchive.org/blob/master/LICENSE.md\`,
retrieved 2026-09-06). The archive republishes GitHub's public events timeline;
neither gharchive.org nor its repository states a separate licence for the data
itself. flume therefore vendors only two derived timestamps and a schema enum
per event — facts about when things happened, not the content of anything
anybody wrote.
`);
}

async function fetchUsgs() {
  const url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson';
  process.stdout.write('GET ' + url + '\n');
  const res = await fetch(url, { headers: { 'user-agent': UA } });
  if (!res.ok) throw new Error('usgs: HTTP ' + res.status);
  const geo = await res.json();
  const all = geo.features
    .map((/** @type {any} */ f) => ({
      t: f.properties.time,
      p: f.properties.updated,
      k: String(f.properties.net),
    }))
    .filter((/** @type {any} */ r) => Number.isFinite(r.t) && Number.isFinite(r.p));
  // Arrival order IS `updated` order: that is when the record last reached the
  // feed. Sorting by it reconstructs the stream a consumer would have seen.
  all.sort((/** @type {any} */ a, /** @type {any} */ b) => (a.p - b.p) || (a.t - b.t));
  const rows = all.slice(0, SLICE);
  writeStream('usgs', rows, `# USGS earthquakes — 5,000 catalogue records

**Source.** \`https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson\`
— the USGS "All Earthquakes, Past Month" GeoJSON summary feed, downloaded
2026-09-06 (${geo.features.length} features in that snapshot). This is the first
5,000 records in \`updated\` order.

**Why \`updated\` is the processing time.** Every record carries \`time\` (when
the earthquake happened — the event time) and \`updated\` (when the record was
last modified in the catalogue — when a consumer of the feed would have seen this
version). Sorting by \`updated\` reconstructs the order a feed consumer observed.
Both timestamps are the source's own; neither is synthesised here. This is the
one stream of the three where event time and processing time are separately
measured facts rather than one measured and one inferred from position.

**What is vendored.** Per record: \`t\` (\`time\`), \`p\` (\`updated\`), and
\`k\` (\`net\`, the contributing seismic network code, e.g. \`ak\`, \`ci\`, \`us\`).
No magnitudes, no coordinates, no place names, no event ids.

**Licence.** Produced by the U.S. Geological Survey, an agency of the U.S.
federal government. Works of the United States Government are not subject to
copyright protection in the United States (17 U.S.C. § 105). The feed's own
documentation page carries no separate licence statement, and USGS's
copyright-and-credits page returned HTTP 403 to a scripted request on
2026-09-06, so this file cites the statutory position rather than quoting a page
it could not retrieve.
`);
}

/**
 * Wikimedia EventStreams is a live SSE endpoint, so this reads until it has
 * `SLICE` events and then aborts. A second run captures a different minute of
 * the world's edits; the repository ships the capture the README was measured
 * against.
 */
async function fetchWikimedia() {
  const url = 'https://stream.wikimedia.org/v2/stream/recentchange';
  process.stdout.write('GET ' + url + ' (live stream, reading ' + SLICE + ' events)\n');
  const controller = new AbortController();
  const res = await fetch(url, {
    headers: { 'user-agent': UA, accept: 'text/event-stream' },
    signal: controller.signal,
  });
  if (!res.ok || !res.body) throw new Error('wikimedia: HTTP ' + res.status);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  /** @type {{t:number,p:number|null,k:string}[]} */
  const rows = [];
  let buffer = '';
  let firstDt = null;
  let lastDt = null;
  while (rows.length < SLICE) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl;
    while ((nl = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (!line.startsWith('data: ')) continue;
      let o;
      try { o = JSON.parse(line.slice(6)); } catch { continue; }
      if (!o || typeof o.timestamp !== 'number' || !o.meta || !o.meta.dt) continue;
      const p = Date.parse(o.meta.dt);
      if (!Number.isFinite(p)) continue;
      if (firstDt === null) firstDt = o.meta.dt;
      lastDt = o.meta.dt;
      rows.push({ t: o.timestamp * 1000, p, k: String(o.server_name ?? 'unknown') });
      if (rows.length >= SLICE) break;
    }
  }
  controller.abort();
  writeStream('wikimedia', rows, `# Wikimedia EventStreams — 5,000 recent changes

**Source.** \`https://stream.wikimedia.org/v2/stream/recentchange\` — the
Wikimedia Foundation's public \`mediawiki.recentchange\` event stream, captured
2026-09-06 from ${firstDt} to ${lastDt} UTC. This is a live stream, so re-running
\`tools/fetch-streams.mjs\` captures a different (equally valid) window; the
capture in this repository is the one every number in the README was measured
against.

**Why \`meta.dt\` is the processing time.** Each event carries \`timestamp\`
(when the edit was recorded on the wiki, one-second resolution — the event time)
and \`meta.dt\` (when the event was emitted onto the stream, millisecond
resolution — when a consumer saw it). Arrival order is capture order, which is
the order the stream delivered them.

**What is vendored.** Per event: \`t\` (\`timestamp\` x 1000), \`p\`
(\`meta.dt\` parsed to epoch ms), and \`k\` (\`server_name\`, e.g.
\`en.wikipedia.org\`). No titles, no user names, no comments, no revision ids, no
edit summaries — nothing from the edits themselves.

**Licence.** Wikimedia project content is licensed CC BY-SA 4.0 (and in places
GFDL) per the Wikimedia Foundation Terms of Use. flume vendors no content: only
per-event timestamps and the wiki's domain name. The stream itself is public and
requires no credentials.
`);
}

const TASKS = { gharchive: fetchGharchive, usgs: fetchUsgs, wikimedia: fetchWikimedia };

async function main() {
  const want = process.argv.slice(2).filter((a) => !a.startsWith('-'));
  const names = want.length ? want : Object.keys(TASKS);
  for (const name of names) {
    const fn = /** @type {any} */ (TASKS)[name];
    if (!fn) {
      process.stderr.write(
        'fetch-streams: unknown stream "' + name + '"; known: ' + Object.keys(TASKS).join(', ') + '\n'
      );
      return 1;
    }
    await fn();
  }
  return 0;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main().then((code) => { process.exitCode = code; }).catch((err) => {
    process.stderr.write('fetch-streams: ' + err.message + '\n');
    process.exitCode = 1;
  });
}
