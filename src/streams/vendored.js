// Loading the three real streams in vendor/.
//
// Each is NDJSON, one event per line, in ARRIVAL order:
//   {"t": eventTimeMs, "p": processingTimeMs|null, "k": "key"}
//
// The order of the lines is load-bearing. It is the processing order, and
// re-sorting the file would destroy the only signal these streams carry that a
// generated one cannot: real out-of-order arrival, produced by real systems
// under no obligation to be tidy.
//
// The parser is strict on purpose. A vendored file with a bad line is a
// corrupted repository, not a runtime condition to route around, and the R6
// tests point the CLI at malformed files to prove the error is stated rather
// than thrown.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..', '..');
export const VENDOR_DIR = path.join(ROOT, 'vendor');

/**
 * @typedef {object} StreamMeta
 * @property {string} id
 * @property {string} title
 * @property {string} source URL the slice came from
 * @property {string} eventTime what `t` is
 * @property {string} processingTime what `p` is, or how arrival order is known
 * @property {string} licence
 * @property {number} suggestedWindowMs a window size that suits this stream's rate
 */

/** @type {StreamMeta[]} */
export const STREAMS = [
  {
    id: 'gharchive',
    title: 'GitHub public events (GH Archive)',
    source: 'https://data.gharchive.org/2026-01-15-3.json.gz',
    eventTime: 'the event\'s own created_at, one-second resolution',
    processingTime: 'not recorded by the archive; arrival order is the file\'s line order',
    licence: 'GH Archive project MIT; data is GitHub\'s public events timeline. See vendor/gharchive/SOURCE.md',
    suggestedWindowMs: 10000,
  },
  {
    id: 'wikimedia',
    title: 'Wikimedia recent changes (EventStreams)',
    source: 'https://stream.wikimedia.org/v2/stream/recentchange',
    eventTime: 'the edit\'s timestamp, one-second resolution',
    processingTime: 'meta.dt, when the event was emitted onto the stream',
    licence: 'Wikimedia content is CC BY-SA 4.0; no content is vendored. See vendor/wikimedia/SOURCE.md',
    suggestedWindowMs: 10000,
  },
  {
    id: 'usgs',
    title: 'USGS earthquake catalogue',
    source: 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson',
    eventTime: 'the origin time of the earthquake',
    processingTime: 'the catalogue record\'s updated time, sorted ascending to reconstruct arrival',
    licence: 'US Government work, not subject to copyright in the US (17 U.S.C. 105). See vendor/usgs/SOURCE.md',
    suggestedWindowMs: 3600000,
  },
];

export const STREAM_IDS = STREAMS.map((s) => s.id);

/** @param {string} id */
export function streamMeta(id) {
  const meta = STREAMS.find((s) => s.id === id);
  if (!meta) throw new Error('unknown stream "' + id + '"; known: ' + STREAM_IDS.join(', '));
  return meta;
}

/**
 * @param {string} text NDJSON
 * @param {string} label for error messages
 * @returns {{t: number, p: number|null, k: string}[]}
 */
export function parseNdjsonStream(text, label) {
  const lines = text.split('\n');
  /** @type {{t: number, p: number|null, k: string}[]} */
  const out = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      throw new Error(label + ':' + (i + 1) + ': not valid JSON');
    }
    if (!obj || typeof obj !== 'object' || !Number.isFinite(obj.t)) {
      throw new Error(label + ':' + (i + 1) + ': every line needs a finite numeric "t" (event time in ms)');
    }
    out.push({
      t: obj.t,
      p: Number.isFinite(obj.p) ? obj.p : null,
      k: typeof obj.k === 'string' ? obj.k : '',
    });
  }
  if (out.length === 0) throw new Error(label + ': no events found');
  return out;
}

/** @param {string} id @returns {{t: number, p: number|null, k: string}[]} */
export function loadVendored(id) {
  const meta = streamMeta(id);
  const file = path.join(VENDOR_DIR, meta.id, 'events.ndjson');
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch {
    throw new Error(
      'vendored stream "' + id + '" is missing at ' + file +
      '; run `node tools/fetch-streams.mjs ' + id + '` to rebuild it'
    );
  }
  return parseNdjsonStream(text, 'vendor/' + meta.id + '/events.ndjson');
}
