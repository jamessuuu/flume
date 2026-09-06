# USGS earthquakes - 5,000 catalogue records

**Source.** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson`
 -  the USGS "All Earthquakes, Past Month" GeoJSON summary feed, downloaded
2026-09-06 (11142 features in that snapshot). This is the first
5,000 records in `updated` order.

**Why `updated` is the processing time.** Every record carries `time` (when
the earthquake happened - the event time) and `updated` (when the record was
last modified in the catalogue - when a consumer of the feed would have seen this
version). Sorting by `updated` reconstructs the order a feed consumer observed.
Both timestamps are the source's own; neither is synthesised here. This is the
one stream of the three where event time and processing time are separately
measured facts rather than one measured and one inferred from position.

**What is vendored.** Per record: `t` (`time`), `p` (`updated`), and
`k` (`net`, the contributing seismic network code, e.g. `ak`, `ci`, `us`).
No magnitudes, no coordinates, no place names, no event ids.

**Licence.** Produced by the U.S. Geological Survey, an agency of the U.S.
federal government. Works of the United States Government are not subject to
copyright protection in the United States (17 U.S.C. § 105). The feed's own
documentation page carries no separate licence statement, and USGS's
copyright-and-credits page returned HTTP 403 to a scripted request on
2026-09-06, so this file cites the statutory position rather than quoting a page
it could not retrieve.
