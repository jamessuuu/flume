# flume

A small event-time stream processor. Every event carries the time it *happened*,
distinct from the time it *arrived*. Windows close when a watermark over event
time passes them - never on a wall clock - and late data is handled by a policy
you choose and the checker can audit.

The watermark, the trigger, the allowed-lateness bound and the retracted pane are
the Dataflow model's: **Akidau et al., *The Dataflow Model*, VLDB 2015**. flume is
a single-process reading of those semantics small enough to watch and check. It
is not Flink, not Beam, and not Kafka Streams - see the scope cut.

**Every window comes back as one of three things, never two:** `complete`,
`revised`, or `unverifiable` with the reason printed.

```
$ node src/cli.js stream usgs
USGS earthquake catalogue
  source     https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson
  event time the origin time of the earthquake
  processing the catalogue record's updated time, sorted ascending to reconstruct arrival
  licence    US Government work, not subject to copyright in the US (17 U.S.C. 105). See vendor/usgs/SOURCE.md

watermark: bounded out-of-orderness, bound 2000ms (watermark = maxSeen - bound - 1)
windows 1.0h, allowed lateness 1.0h, late policy update
measured lateness: 4288/5000 out of order (85.76%), p50 10.7h, p99 12.8d, max 15.3d

5 complete   6 revised   387 unverifiable   (398 windows of 1.0h)
  unverifiable because lateness-bound-exceeded: 386
  unverifiable because watermark-never-reached: 1
  no findings against the engine
```

That is real USGS data with real lateness, and it is the honest answer: a
one-hour allowed-lateness bound cannot cope with a catalogue that revises records
for two weeks. The engine did exactly what it was told, every discarded event has
a receipt, and 387 windows are unknowable rather than quietly wrong.

---

## Install and run it in 60 seconds

```sh
git clone https://github.com/jamessuuu/flume && cd flume
npm ci        # devDeps are TypeScript and node types; flume has no runtime deps
npm run demo  # nineteen events, two clocks, all three outcomes
```

Or open `web/index.html` in a browser. It is a static page - no server, no build,
no dataset to download, no credentials. The default view shows a late event, so
the idea is on the first screen.

## The worked example

`npm run demo` is nineteen hand-placed events. Event time runs left to right,
processing order runs top to bottom, and the watermark is the `|`.

```
$ node src/cli.js demo
   arr  event time 0ms .. +18.0s  (: window boundary)
        :----------:----------:----------:------
     0  o                                         t=+0ms
     1    o                                       t=+1.0s
     2    | o                                     t=+2.0s
     3        |  o                                t=+4.0s
     4           | o                              t=+5.0s
     5             | o                            t=+6.0s
     6               | o                          t=+7.0s  closed [+0ms,+5.0s) = 4
     7                 | o                        t=+8.0s
     8                   |  o                     t=+9.0s
     9         *         |                        t=+3.0s  REVISED [+0ms,+5.0s) = 5 (was 4)
    10                     |  o                   t=+10.0s
    11                        | o                 t=+11.0s
    12                          | o               t=+12.0s  closed [+5.0s,+10.0s) = 5
    13                            | o             t=+13.0s
    14                              | o           t=+14.0s
    15                                |  o        t=+15.0s
    16                                     | o    t=+17.0s  closed [+10.0s,+15.0s) = 5
    17                                       | o  t=+18.0s
    18               !                       |    t=+6.0s  TOO LATE -> side output
        :----------:----------:----------:------
   o on time   * arrived after its window closed, inside allowed lateness
   ! arrived past allowed lateness   | the watermark, after this event

1 complete   1 revised   2 unverifiable   (4 windows of 5.0s)
  unverifiable because lateness-bound-exceeded: 1
  unverifiable because watermark-never-reached: 1
  no findings against the engine
```

Row 9 carries an event time of +3.0s. Its window had already closed and reported
4. Allowed lateness is 6s and the state was still there, so the engine retracted
4 and emitted 5. **That window is `revised`, not `complete`** - the distinction
the checker exists to keep.

Row 18 carries +6.0s, and by then the state of its window had been released. It
is not dropped: it goes to the side output carrying its reason, and its window is
`unverifiable`. The last window is `unverifiable` too, for the *other* reason  - 
the log ended before the watermark reached it. One outcome, two reasons, and the
report says which.

## Three outcomes, and why the third is not a hedge

| outcome | means |
|---|---|
| `complete` | the watermark closed it, and the value it emitted equals what a batch recomputation over the whole log says. Nothing arriving later changed it. |
| `revised` | late data inside the allowed lateness corrected an already-emitted pane. The final value is right; it was not right the first time, and the report refuses to blur those two. |
| `unverifiable` | the emitted panes cannot be shown to be right. Two stated reasons: **`watermark-never-reached`** (the log ended first) or **`lateness-bound-exceeded`** (data arrived past the bound, and is receipted in the side output). |

The ground truth is computable, which is what makes the first two rows claims
rather than hopes: `src/core/oracle.js` groups every event by event time and
counts, with no watermark, no lateness and no policy. The streamed answer is
compared against that number.

A **finding** is a different thing from an outcome. An outcome describes a
window; a finding is an accusation against the *engine*. A window whose value is
unknown because the configured bound was four orders of magnitude too small for
the stream is `unverifiable` with a reason - the engine did what it was told.
Keeping those two apart is not a stylistic choice; it is what the three real
streams below forced.

## The three real streams, and the 124 false positives they found

flume vendors a slice of three public streams with genuinely different lateness.
Nothing here is synthesised: both timestamps come from the source, or arrival
order is the source's own file order. Full derivation and licence position for
each is in `vendor/<id>/SOURCE.md`.

```
$ node src/cli.js streams
gharchive -- GitHub public events (GH Archive)
  5000 events over 2.1m of event time
  out of order: 133 (2.66%)
  lateness p50 0ms  p90 0ms  p99 1.0s  max 1.8m

wikimedia -- Wikimedia recent changes (EventStreams)
  5000 events over 1.8m of event time
  out of order: 3217 (64.34%)
  lateness p50 1.0s  p90 2.0s  p99 8.0s  max 22.0s

usgs -- USGS earthquake catalogue
  5000 events over 16.6d of event time
  out of order: 4288 (85.76%)
  lateness p50 10.7h  p90 3.6d  p99 12.8d  max 15.3d
```

Maximum lateness spans **1.8 minutes to 15.3 days** across the three - four
orders of magnitude. That spread is the point. A watermark tuned on one of these
misfires on another, and the misfire is the finding.

### What the checker flagged, before and after

Three watermark configurations × three streams = nine runs of a **correct**
engine. The first version of the checker produced **124 findings** across those
nine runs. Every one was a false positive, and each had a distinct mechanism:

| retired finding | count | why it was wrong |
|---|---:|---|
| `no-pane-after-close` | 81 | Fires when the watermark passed a window's end but no pane was emitted. On USGS with an hour bound, 38 windows had *every one* of their events arrive after the state was released - so no state was ever created, and all of them were receipted to the side output. Nothing was lost and nothing was silent. The bound was too small; the engine was fine. |
| `unrevised-late` | 40 | Fires when a window has in-bound late data under the `update` policy but only one pane. USGS has 36 windows whose *first* event arrived after the watermark had already passed their end; on the 20 of those that received exactly one late event, one pane is the correct answer, because that pane **is** the close. 20 windows × 2 fixed-bound configurations = 40. The check was wrong, not the engine. |
| `watermark-regression` | 3 | Fires when the raw watermark falls. For a fixed bound that is impossible, so it means a bug. For the **adaptive** percentile strategy the raw value falls every time the sliding sample forgets an outlier - 4,295 times on USGS alone. That is the strategy working as designed. |
| **total** | **124** | |

After the fix - reclassify a fully receipted window as `unverifiable /
lateness-bound-exceeded`, replace the pane heuristic with an exact identity
(`panes === 1 + lateInBound`, or `=== lateInBound` for a window opened after its
close), and only accuse a strategy that *declared* `monotonicByConstruction`  - 
the same nine runs produce nothing at all:

```
$ node --test "test/streams.test.js"
✔ a correct engine produces ZERO defect findings on every real stream and configuration (547ms)
✔ the 124 retired false positives are reconstructible, and all 124 are still not defects (540ms)
```

**The 124 is not a memory.** `test/streams.test.js` reconstructs all three
retired conditions from the shipped results and asserts they still total exactly
81 + 40 + 3. What changed is the verdict drawn from them, not whether they
happen; the underlying counts are in `vendor/MEASURED.json`, regenerated and
compared by the same test.

Removing those three false positives also silenced a **true** positive - the
`processing-time-watermark` fixture had been caught by `no-pane-after-close`, and
after the fix it produced nothing. The replacement is structural rather than
statistical:

> A watermark derived from observed event times is `maxSeen` minus a
> non-negative bound, so it can never rise above the highest event time yet seen.
> One that does is not a function of the data.

That is `watermark-overshoot`, and it is exact. The statistical alternative - a
threshold on the rate of late events - would have fired here *and* on a correctly
implemented bound that is simply too small for its stream, which is precisely the
confusion the other 124 findings were made of.

### The off-by-one that only real data finds

`bounded` emits `maxSeen - bound - 1`, not `maxSeen - bound`. The `-1` is for the
inclusive boundary: with a bound of 0, an event at time T would otherwise assert
that nothing at time T can arrive again - but simultaneous events are exactly
what a second-resolution timestamp produces in bulk. Apache Flink's
`BoundedOutOfOrdernessWatermarks` subtracts the same 1.

Both forms ship (`--watermark bounded-naive`), so the difference is measurable
rather than asserted. At bound 0:

| stream | events per distinct timestamp | extra events the naive form calls late |
|---|---:|---:|
| gharchive | 41.67 | **7** |
| wikimedia | 48.08 | **284** |
| usgs | 1.00 | **0** |

USGS timestamps are unique to the millisecond, so the two forms agree exactly  - 
which is the control that shows the effect is the collision and not something
else. On Wikimedia, 284 events out of 5,000 are misclassified as late by a
watermark that is otherwise correct.

## The planted fixtures and the negative control

A checker whose failure path has never been exercised is decoration. flume ships
four build-flag bugs (`node src/cli.js bugs` lists them; every one lives in
`src/bugs.js`, nothing is hidden elsewhere). Three break the engine. The fourth
breaks the checker.

```
$ node src/cli.js fixtures
Planted fixtures (recipes in src/fixtures.js, measured 2026-09-06)
  OK   processing-time-watermark 40/40 seeds fired "watermark-overshoot" (floor 40), control 0
  OK   close-without-lateness    40/40 seeds fired "premature-close" (floor 40), control 0
  OK   silent-drop               40/40 seeds fired "silent-drop" (floor 40), control 0
  OK   checker-revised-as-complete  hid 98 revised windows across 20/20 seeds; every change was an exact relabel of revised to complete

Negative control: 400 executions of a perfectly ordered, zero-lateness stream
  flush termination: 0 revised, 0 unverifiable, 0 findings
  idle termination:  400/400 runs unverifiable ONLY at the tail the watermark never reached, 0 findings
```

**The sabotaged checker is the one that matters.** The other three prove the
checker *fires*; they say nothing about the `complete`/`revised` distinction
being real, because a checker that called every window `complete` would still
have caught all three - they appear as findings, not as outcomes.
`checker-revised-as-complete` reports a window it knows was corrected as having
been right the first time. With it on, every revision disappears into the
`complete` column, including on real Wikimedia data. That is the exact lie a
streaming report is most likely to tell and the one nobody downstream can detect
(`test/sabotage.test.js`).

**The negative control** is 400 executions of a perfectly ordered stream with
*exactly* zero lateness - by construction, not by a small probability. Zero
revisions, zero unknowns, zero findings. Without it, "the checker found bugs" is
indistinguishable from "the checker fires at random".

The same runs under `idle` termination produce exactly one kind of unverifiable
window: the tail the watermark never reached. That is not a bug being tolerated,
it is the difference between two truths about the end of a stream. `flush` means
the source is exhausted and said so, which licenses a final watermark of
+infinity. `idle` means we stopped watching a stream that continues, so the last
windows are genuinely unknown. Which one you pick changes the answer, so flume
makes you pick.

### Two bugs the fixtures caught in flume itself

- **A window opened after its own close emitted a "revision" of nothing.** When
  the watermark ran past a window before any of its events arrived, the first
  pane was labelled a revision and claimed to retract a value that had never been
  emitted, breaking the retraction chain. Found by `pane-chain-broken` on the
  burst stream. Now such a pane is `initial`, and the window is marked
  `openedAfterClose` so the report says the watermark overran rather than
  pretending it did not.
- **The fixture stream was too tidy to break anything.** The first synthetic
  generator laid event times on an exact grid. On a grid the arrival index and
  the event time advance in lockstep, which makes a *processing-time* watermark
  accidentally correct - the most important fixture could not fire against it.
  Event-time gaps are now exponential and the generator takes a burst, because a
  stream that never bursts is a stream that never had an upstream outage.

## What is actually in here

**`src/core/engine.js`** - one pass over an event log. Windows are assigned by
event time; the engine's only notion of processing time is the position in the
log. There is no `Date.now`, no `Math.random` and no `setTimeout` anywhere under
`src/core`, and `test/determinism.test.js` fails the build if one appears. The
same log replayed produces byte-identical output including the watermark trace.

**`src/core/watermark.js`** - five strategies: `bounded`, `bounded-naive`,
`percentile` (the bound is the p99 of observed lateness rather than a constant
somebody guessed), `punctuated`, and `processing-time`, which is a planted
fixture and not a strategy anyone should choose. Each declares whether its raw
output is monotonic by construction, because that is what decides whether a
falling watermark is a bug or a design.

**`src/core/oracle.js`** - the batch ground truth, deliberately trivial. A ground
truth complex enough to be wrong would not be one. It shares exactly one function
with the engine (`assignWindow`), and `test/window.test.js` checks that function
against hand-computed boundaries so the two cannot be wrong together.

**`src/core/checker.js`** - the three outcomes, the reasons, and seven finding
codes. `tools/lint.mjs` fails the build if a finding is emitted without
documentation or documented without being reachable.

**`vendor/`** - the three real slices, each with a `SOURCE.md` stating the exact
URL, what was taken, why the arrival order is what it is, and the licence
position. Only two timestamps and a low-cardinality label are vendored per event:
no titles, usernames, repository names, comments, payloads or coordinates.

## What it does NOT do

- **Not a stream processor to run anything on.** One process, in-memory state, no
  checkpointing, no exactly-once across a restart, no recovery, no backpressure,
  no distribution. Restart it and the state is gone. For the durability side of
  this problem see [sluice](https://github.com/jamessuuu/sluice) and
  [cofferdam](https://github.com/jamessuuu/cofferdam); this project deliberately
  does not overlap them.
- **Not Flink, Beam or Kafka Streams.** Those implement the same published
  semantics at production scale, with state backends, savepoints and a cluster.
  flume implements a subset of the semantics so they can be watched. No benchmark
  against them appears here, because none was run.
- **Tumbling windows only.** No sliding windows, no session windows, no custom
  window functions. Every extra window type is another place for a bug the
  fixtures do not cover.
- **One aggregate: a count.** Chosen because its ground truth is one line of
  code, so a disagreement between the streamed and batch results can only be the
  windowing, never the arithmetic.
- **The whole log is held in memory**, and `check` refuses a file over 32 MB
  rather than dying part way through one.
- **The vendored slices are 5,000 events each**, not the whole source. The
  GH Archive hour they come from holds 141,671 events; every number quoted here
  is measured on the committed slice, never extrapolated to the hour.
- **The Wikimedia slice is a live capture.** Re-running `tools/fetch-streams.mjs`
  produces a different, equally valid minute of the world's edits. The repository
  ships the capture every number above was measured against.
- **Cut from v1:** sliding and session windows, multiple aggregates, per-key
  window state, a persistent state backend, npm packaging.

### Known failure modes

- A `bounded` watermark with a bound smaller than the stream's real lateness
  turns most windows `unverifiable`. That is the contract working, but it does
  mean flume cannot give you a number for every window of every stream.
- The `percentile` strategy still misses the tail by construction - that is what
  choosing a percentile means. On USGS at p99 it leaves 354 of 398 windows
  unverifiable, most of them because the watermark never got there at all.
- Both second-resolution sources (GH Archive, Wikimedia) put ~40-48 events on
  each distinct timestamp. Any watermark with a bound of 0 is therefore making a
  much stronger claim than it looks, and the `-1` is load-bearing.
- The demo page plots one dot per event. At 5,000 events (the real streams) it is
  a density picture, not a readable scatter, and the per-window close ticks are
  suppressed above 60 panes because they become a hatch pattern.
- Opened from a `file://` URL the page works, but it cannot write the
  configuration into the address bar: a browser refuses `history.replaceState`
  on an opaque origin. The controls still apply - the state is held in the page,
  not re-read from the URL - but a view is only a shareable link when the page is
  served over http(s). Verified served; the `file://` behaviour is the documented
  browser rule, not something measured here.
- The demo page's default log pins its window size and allowed lateness. The
  policy, termination and watermark controls apply to it, but changing the window
  would break the nineteen-row story, so those two fields are ignored on that one
  source and honoured everywhere else.
- `flume run --build <flag>` exits 1 when findings appear. That is intended - a
  finding is a failure - but it means the planted builds cannot be used in a
  pipeline that treats exit 0 as "ran".

## Commands

```
flume demo                     nineteen events, two clocks, all three outcomes
flume run [options]            a generated stream with known lateness
flume stream <id> [options]    gharchive | wikimedia | usgs
flume check <file.ndjson>      an event log from a file
flume streams                  the vendored streams and their measured lateness
flume fixtures                 the planted fixtures and the negative control
flume bugs                     list the build-flag fixtures
```

`run`, `stream` and `check` take `--window`, `--lateness`, `--policy`,
`--termination`, `--watermark`, `--bound`, `--percentile`, `--build` and
`--json`. `run` additionally takes `--seed`, `--events`, `--late-prob` and
`--timeline`.

The input format for `check` is NDJSON, one event per line, **in arrival order**:

```
{"t": 1768446000000, "p": null, "k": "PushEvent"}
```

`t` is the event time in epoch milliseconds and is the only required field. `p`
is a display-only processing timestamp the engine never reads. Re-sorting the
file destroys the only signal it carries.

Bad input produces a stated error and a non-zero exit, never a stack trace:

```
$ node src/cli.js check nope.ndjson
flume: no such file: nope.ndjson

$ node src/cli.js check prose.ndjson
flume: prose.ndjson:2: not valid JSON

$ node src/cli.js run --policy ignore
flume: --policy must be one of: drop, update, side-output; got "ignore"

$ node src/cli.js run --seed banana
flume: --seed must be an integer, got "banana"
```

## Reproducing every number in this README

| claim | command |
|---|---|
| the three streams' lateness | `node src/cli.js streams` |
| the USGS run at the top | `node src/cli.js stream usgs` |
| zero findings across nine runs | `node --test "test/streams.test.js"` |
| the 124 retired false positives | `node --test "test/streams.test.js"` (reconstructed and asserted) |
| the naive-boundary table | `npm run measure` then read `vendor/MEASURED.json` |
| fixture fire rates, negative control | `node src/cli.js fixtures` |
| the sabotaged checker | `node --test "test/sabotage.test.js"` |
| determinism | `node --test "test/determinism.test.js"` |
| hostile input | `node --test "test/cli.test.js"` |
| everything | `npm test` (96 tests, ~2 s) |

Measured on Node v24.15.0, Windows 11, 2026-09-06. `engines.node` is `>=22.0.0`:
the library runs on older Node, but `npm test` uses the test runner's glob
support, and 22 is the oldest version this has actually been exercised on.

**On CI, honestly:** `.github/workflows/ci.yml` installs from the lockfile on a
pinned Node 22.14.0 and runs lint, typecheck, bundle freshness, measurement
freshness, the full suite, the fixtures, the streams and the demo on Linux, then
the suite again on Windows (flume is developed on Windows and its module
resolution depends on `pathToFileURL`). It first ran on 2026-09-06, on the push of `650abeb`, and passed (GitHub
Actions run 34035769216). The run is the badge; this README implies nothing
beyond it. What has been verified is the equivalent locally, from a fresh
`git clone` into an empty directory followed by `npm ci`: lint clean, typecheck
clean, bundle fresh, measurements fresh, 96/96 tests, all fixtures firing,
negative control clean. That clean-clone run is what proves the `.gitattributes`
line-ending rules hold - the vendored NDJSON and the generated bundle are both
compared byte for byte, and a checkout that translated line endings would fail
both.

## Development

```sh
npm test           # 96 tests
npm run lint       # a project-specific gate, not a style opinion engine
npm run typecheck  # tsc over JSDoc types
npm run measure    # regenerate vendor/MEASURED.json after touching the core
npm run build:web  # regenerate web/flume.bundle.js after editing src/ or web/app.js
npm run check:web  # fail if the committed bundle is stale
```

`tools/fetch-streams.mjs` rebuilds `vendor/` from the network. It is committed
for provenance and is deliberately *not* run by CI or by `npm test`: the vendored
slices in this repository are the ones every number above was measured against.

## Credits and licence

flume is MIT licensed - see [LICENSE](LICENSE).

- **The Dataflow model** - Akidau, Bradshaw, Chambers, Chernyak, Fernández-Moctezuma,
  Lax, McVeety, Mills, Perry, Schmidt, Whittle, *The Dataflow Model: A Practical
  Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded,
  Out-of-Order Data Processing*, PVLDB 8(12), 2015. Watermarks, triggers, allowed
  lateness and accumulation modes are theirs; the implementation and its mistakes
  are mine.
- **Apache Flink** - the inclusive-boundary `-1` in `bounded` follows
  `BoundedOutOfOrdernessWatermarks`.
- **GH Archive** - `vendor/gharchive/`. The GH Archive project is MIT licensed
  (© 2012-2016 Ilya Grigorik); it republishes GitHub's public events timeline,
  for which no separate data licence is stated. Only derived timestamps and the
  event-type enum are vendored. See `vendor/gharchive/SOURCE.md`.
- **Wikimedia Foundation EventStreams** - `vendor/wikimedia/`. Wikimedia project
  content is CC BY-SA 4.0; **no content is vendored**, only per-event timestamps
  and the wiki's domain name. See `vendor/wikimedia/SOURCE.md`.
- **U.S. Geological Survey earthquake feed** - `vendor/usgs/`. A work of the U.S.
  federal government, not subject to copyright in the United States
  (17 U.S.C. § 105). The feed's documentation states no separate licence, and the
  USGS copyright page returned HTTP 403 to a scripted request on 2026-09-06, so
  this repository cites the statutory position rather than quoting a page it could
  not retrieve. See `vendor/usgs/SOURCE.md`.
