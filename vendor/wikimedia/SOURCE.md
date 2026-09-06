# Wikimedia EventStreams — 5,000 recent changes

**Source.** `https://stream.wikimedia.org/v2/stream/recentchange` — the
Wikimedia Foundation's public `mediawiki.recentchange` event stream, captured
2026-09-06 from 2026-09-06T12:43:52.687Z to 2026-09-06T12:45:28.273Z UTC. This is a live stream, so re-running
`tools/fetch-streams.mjs` captures a different (equally valid) window; the
capture in this repository is the one every number in the README was measured
against.

**Why `meta.dt` is the processing time.** Each event carries `timestamp`
(when the edit was recorded on the wiki, one-second resolution — the event time)
and `meta.dt` (when the event was emitted onto the stream, millisecond
resolution — when a consumer saw it). Arrival order is capture order, which is
the order the stream delivered them.

**What is vendored.** Per event: `t` (`timestamp` x 1000), `p`
(`meta.dt` parsed to epoch ms), and `k` (`server_name`, e.g.
`en.wikipedia.org`). No titles, no user names, no comments, no revision ids, no
edit summaries — nothing from the edits themselves.

**Licence.** Wikimedia project content is licensed CC BY-SA 4.0 (and in places
GFDL) per the Wikimedia Foundation Terms of Use. flume vendors no content: only
per-event timestamps and the wiki's domain name. The stream itself is public and
requires no credentials.
