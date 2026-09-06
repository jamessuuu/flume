// A seeded generator, used only to BUILD synthetic streams -- never inside the
// engine, the oracle or the checker. Those three see an event log and nothing
// else, which is what makes a replay byte-identical.
//
// sfc32 (Chris Doty-Humphrey's Small Fast Counting generator, public domain).
// 32-bit integer arithmetic only, so the sequence is identical on every
// platform with `>>> 0` -- that is, all of them. The seed is expanded with a
// splitmix32 round so that adjacent seeds produce unrelated streams instead of
// nearly identical ones, and the first twelve outputs are discarded so a small
// seed does not show through at the start.

/** @param {number} x @returns {number} */
function splitmix32(x) {
  let z = (x + 0x9e3779b9) | 0;
  z = Math.imul(z ^ (z >>> 16), 0x21f0aaad);
  z = Math.imul(z ^ (z >>> 15), 0x735a2d97);
  return (z ^ (z >>> 15)) >>> 0;
}

/**
 * @param {number} seed any integer
 * @returns {() => number} uniform in [0, 1)
 */
export function sfc32(seed) {
  let a = splitmix32(seed);
  let b = splitmix32(a);
  let c = splitmix32(b);
  let d = splitmix32(c);
  const next = function () {
    a >>>= 0; b >>>= 0; c >>>= 0; d >>>= 0;
    let t = (a + b) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    d = (d + 1) | 0;
    t = (t + d) | 0;
    c = (c + t) | 0;
    return (t >>> 0) / 4294967296;
  };
  for (let i = 0; i < 12; i++) next();
  return next;
}

/**
 * An exponential draw truncated to [0, max]. Used for lateness, because real
 * lateness is heavy-tailed: most events are on time and a handful are very
 * late. A uniform draw would produce a distribution no real stream has, and a
 * fixture tuned against it would prove nothing about a real one.
 *
 * @param {() => number} rand
 * @param {number} max
 * @returns {number} integer in [0, max]
 */
export function heavyTail(rand, max) {
  const u = Math.min(0.999999, Math.max(1e-9, rand()));
  const x = -Math.log(1 - u) / 3;
  return Math.min(max, Math.round(x * max));
}
