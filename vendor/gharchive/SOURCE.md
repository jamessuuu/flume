# GH Archive — 5,000 GitHub public events

**Source.** `https://data.gharchive.org/2026-01-15-3.json.gz` — the GH
Archive hourly file for 2026-01-15 03:00 UTC, downloaded
2026-09-06. The full hour holds 141,671 events; this is the first 5,000 lines of
the file, a contiguous slice in the file's own order.

**Why the file's order is the arrival order.** GH Archive writes each hourly
file in the order GitHub's public events API returned the events. That order is
the processing order; `created_at` is the event time. The file carries no
per-event arrival timestamp, so `p` is null and flume uses the line's position,
which is the same information.

**What is vendored.** Per event: `t` (`created_at` parsed to epoch ms),
`p` (null), and `k` (the GitHub event type, e.g. `PushEvent`). Nothing else.
No repository names, no actor names, no payloads, no organisation names.

**Licence.** The GH Archive *project* is MIT licensed (Copyright 2012-2016 Ilya
Grigorik; `https://github.com/igrigorik/gharchive.org/blob/master/LICENSE.md`,
retrieved 2026-09-06). The archive republishes GitHub's public events timeline;
neither gharchive.org nor its repository states a separate licence for the data
itself. flume therefore vendors only two derived timestamps and a schema enum
per event — facts about when things happened, not the content of anything
anybody wrote.
