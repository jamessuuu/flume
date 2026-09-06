// The demo log: nineteen events, hand-placed, small enough to read every row.
//
// Shared by `flume demo` and the demo page so the two cannot drift apart. It is
// built to put all three outcomes and both reasons for the third on one screen:
//
//   [0s,5s)    four events arrive, the window closes and reports 4, then an
//              event with an event time of +3s arrives on row 9 -- inside the
//              allowed lateness, so the window is corrected to 5.  REVISED
//   [5s,10s)   closes and reports 5; an event with an event time of +6s arrives
//              on row 18, after the state was released.  UNVERIFIABLE, because
//              the lateness bound was exceeded -- and the event is receipted in
//              the side output rather than dropped.
//   [10s,15s)  nothing late, closed cleanly.  COMPLETE
//   [15s,20s)  the log ends before the watermark reaches its end.  UNVERIFIABLE,
//              because the watermark never got there.
//
// Those two unverifiable reasons are not the same thing and the demo shows both:
// one window is unknowable because of a policy bound, the other because the
// stream simply stopped.

/** Event times in ARRIVAL order. The two interesting rows are 9 and 18. */
export const DEMO_ORDER = [
  0, 1000, 2000, 4000, 5000, 6000, 7000, 8000, 9000,
  3000,
  10000, 11000, 12000, 13000, 14000, 15000, 17000, 18000,
  6000,
];

/** Configuration the demo is designed around; changing it changes the story. */
export const DEMO_CONFIG = {
  windowMs: 5000,
  allowedLatenessMs: 6000,
  latePolicy: /** @type {'update'} */ ('update'),
  termination: /** @type {'idle'} */ ('idle'),
  watermark: { name: 'bounded', boundMs: 1000 },
};

/** What the demo must produce. Asserted by the CLI and by test/demo.test.js. */
export const DEMO_EXPECTED = { complete: 1, revised: 1, unverifiable: 2 };

/** @returns {{t: number, p: number, k: string}[]} */
export function demoStream() {
  return DEMO_ORDER.map((t, rank) => ({ t, p: rank * 1000, k: 'demo' }));
}
