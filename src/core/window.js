// Window assignment. Tumbling event-time windows, and nothing else.
//
// A window is a half-open interval [start, end) over EVENT time. Assignment is
// a pure function of the event's own timestamp: it does not depend on when the
// event arrived, which is the entire distinction this project exists to make.
//
// Sliding and session windows are deliberately absent -- see the scope cut in
// the README. Tumbling windows are enough to show the watermark contract, and
// every extra window type is another place for a bug the fixtures do not cover.

/**
 * @typedef {object} Window
 * @property {number} start inclusive, event-time ms
 * @property {number} end exclusive, event-time ms
 */

/**
 * Assign an event time to its tumbling window.
 *
 * Uses Math.floor, not a truncating division, so negative event times land in
 * the window below rather than collapsing toward zero. Event times before the
 * epoch are unusual but they are not an error, and silently misassigning them
 * would be worse than refusing them.
 *
 * @param {number} eventTime ms
 * @param {number} sizeMs window size, must be a positive integer
 * @param {number} [offsetMs] shifts every boundary, default 0
 * @returns {Window}
 */
export function assignWindow(eventTime, sizeMs, offsetMs = 0) {
  if (!Number.isFinite(eventTime)) {
    throw new RangeError('event time must be finite, got ' + eventTime);
  }
  if (!Number.isInteger(sizeMs) || sizeMs <= 0) {
    throw new RangeError('window size must be a positive integer number of ms, got ' + sizeMs);
  }
  const start = Math.floor((eventTime - offsetMs) / sizeMs) * sizeMs + offsetMs;
  return { start, end: start + sizeMs };
}

/**
 * The canonical string form of a window. Used as a Map key and as the id in
 * every emitted record, so it must be stable across runs and platforms.
 *
 * @param {Window} w
 * @returns {string}
 */
export function windowKey(w) {
  return w.start + '-' + w.end;
}

/**
 * Parse a window key back into a window. The inverse of {@link windowKey}.
 *
 * @param {string} key
 * @returns {Window}
 */
export function parseWindowKey(key) {
  const m = /^(-?\d+)-(-?\d+)$/.exec(key);
  if (!m) throw new RangeError('not a window key: ' + JSON.stringify(key));
  return { start: Number(m[1]), end: Number(m[2]) };
}

/**
 * Order two windows by start then end. Every list of windows this project
 * emits is sorted with this, so two runs produce the same byte sequence.
 *
 * @param {Window} a
 * @param {Window} b
 * @returns {number}
 */
export function compareWindows(a, b) {
  if (a.start !== b.start) return a.start - b.start;
  return a.end - b.end;
}
