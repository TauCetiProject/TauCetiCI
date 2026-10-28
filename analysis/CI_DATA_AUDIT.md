# CI data collection audit

Audit date: 28 October 2026, UTC.

The useful core is job state, runner identity, timestamps, outcomes, and a few
merge facts. These answer whether the fleet is busy, where jobs wait, how runners
compare, how much Namespace capacity we use, and how quickly staging clears the
merge backlog. Collecting these facts does not require exhaustive historical log,
artifact, or auxiliary-workflow recovery.

There are two overlapping collection systems. The Cloudflare service supplies
live counts and eleven dashboard charts. The older scheduled collector supplies
the SQLite release, static fallback charts, and a modelling report. A field
appearing in an archive or downloadable database is not, by itself, evidence of
a useful consumer. The recommendations below distinguish displayed data,
analysis inputs, operational bookkeeping, and fields with no identified consumer.

## Data and consumers

| Data collected or derived | Actual consumer and purpose | Collection cost | Recommendation |
| --- | --- | --- | --- |
| Run/job IDs, attempts, parent relation, workflow and trigger | Join events without mixing retries; distinguish PR, staging, main and control jobs throughout the reports | Included in webhook payloads; API reads when parents or jobs are missing | Keep |
| Job status and conclusion | Live running/waiting counts; PR failure rate; merge outcomes; stop completed jobs contributing running time | Webhooks; targeted repairs for missing or stale state | Keep; prioritize current state |
| Job created/start/completion times | Both fleet timelines, build-duration and wait percentiles, runner minutes, cancelled-work totals | Webhooks; job-list reads only when observations are insufficient | Keep |
| Runner labels, assignment and name | GitHub/Namespace/eastanglia/leanfro split; avoid counting unassigned jobs as executing; runner-size comparisons in the report | Included in job observations | Keep the evidence needed to identify runner and size |
| Auxiliary workflows across fleet repositories | The “Other GitHub jobs” series, queue peaks and other-job waits show competition for GitHub runner slots | Webhooks are useful; live inventory also queues job-list recovery for every completed run with no stored jobs. The legacy collector samples one in four auxiliary runs | Keep basic webhook observations; stop exhaustive historical recovery. Mark coverage gaps rather than claiming zero activity |
| Build step names, boundaries and outcomes | Coarse phase chart; failed-step classification; outside-sandbox overhead estimates in the report | Included in job payloads/listings; no separate step endpoint needed | Keep for build jobs; do not enrich every control job |
| Sandbox phase timings from `ci-telemetry` | Split the combined build/audit/lint step in the phase chart; batching-overhead analysis | Artifact listing and ZIP download; local telemetry generation | Optional, bounded to recent builds. Retain coarse API step timing when absent |
| Failed-job logs and short excerpts | Failure-cause chart and failure-rate report. The class is consumed; archived excerpts also permit classifier corrections | Download job logs, extract relevant lines; live repairs retain a class, while legacy records retain excerpts | Recent failed build jobs only, with a fixed request budget and expiry. Leave unknown causes explicit |
| Built/unpacked/replayed module counts | Cache-use table in `REPORT.md`; work calibration and replay features in `analysis/replay.py` | Parse the sandbox build log; included in an already-downloaded telemetry artifact | Keep small counters only if these analyses remain wanted; no separate recovery backlog |
| Up to 500 slowest module names and timings per build, total module/job count and summed module build time | Stored inside telemetry/build JSON; no chart, modelling-report or replay consumer identified for `slowest_built`, `jobs_total` or `built_seconds_total` | Log parsing and repeated artifact/archive/SQLite storage | Stop producing these unused fields by default |
| PR lookup, merge bases, behind/ahead counts, diff statistics, paths and patch fingerprints | Rebase-risk and failure-by-change-size sections of `REPORT.md`; dependency cohorts and optional scheduling replay. These do not feed the live charts | Additional PR, commit and compare API requests | Make detailed collection an explicit analysis mode or bounded sample; keep minimal build/PR identity separately |
| Main commits and proof of arrival on main | PR-arrivals line; actual merged-PR denominator in backend cost analysis | Live push webhooks; legacy commit-history reads | Keep minimal PR/SHA/arrival evidence; do not confuse workflow-source SHA with tested staging head |
| Bors readiness observations | Ready-to-merge count and queue-size chart; legacy report/replay additionally use eligible PR/head details and experiment context | Read the bot's existing observation stream; no new CI builds or changes to admission | Keep count/time/completeness for charts. Detailed head histories belong to explicit replay analysis |
| Staging batch ID, tested head, members, time and result | Batch-size scatter plot; merged-PR cost analysis; verify that membership belongs to the tested head | Run title/metadata plus read-only batch-membership lookups | Keep; bound retries for old unavailable membership |
| Host CPU/RSS, memory, pressure and device-I/O counters | Resource tables in `REPORT.md`; no dashboard chart or production dispatcher consumer identified | Host sampling during jobs; artifact download through both normal enrichment and a separate hourly resource refresh | Use bounded experiments or an explicit sample, rather than an always-on completeness target |
| Cache publication counts, staged bytes/objects and confirmation | Publication/resource section of `REPORT.md`; these reporting copies do not authorize cache publication or merging | Publication artifact creation and download | Keep compact outcomes if used for operational diagnosis; detailed historical collection belongs with resource experiments |
| Repository ruleset snapshots | Written to `records/settings` and the SQLite `settings` table; no chart, report or replay reader identified | Ruleset list plus one request per ruleset, even when no change is ultimately stored | Stop scheduled snapshot collection unless a named audit needs it. The policy tooling's separate current-ruleset checks are unrelated |
| Actor login and surplus GitHub run/job response fields | Legacy actor is carried through archive/index reconstruction; live normalized facts discard most response fields. No actor-based chart or report identified | Mostly extra archive bytes, rather than extra GitHub requests | Keep only fields needed for identity/provenance, routing and metrics; avoid archiving whole API objects indefinitely |
| Inventory intervals, gaps, verification markers and comparison samples | Coverage notes; stale-count detection; sampled correctness audit and admin diagnostics | Listing reads plus at most three sampled job-list reads per hourly comparison, subject to the shared budget | Keep bounded coverage and sparse audits. Coverage does not justify exhaustive enrichment |
| Prior metric contributions, outbox entries and applied receipts | Correct charts after late events, and prevent double counting across retries | Internal storage and delivery; no GitHub request needed | Keep bounded bookkeeping while corrections are supported. Millions of repeated metric updates were a bug, not millions of independent facts |

The consumer inventory covers the live service, site browser/rendering code,
scheduled collector/publisher, modelling report and replay module. Unknown
external users of the downloadable database cannot be inferred from repository
code. Compatibility of that public export should be handled separately from
whether every field deserves continued scheduled collection.

The published `REPORT.md` has populated resource and replay sections, so those
consumers are active calculations. For example, it reports 922 publication
artifacts and 919 public-map confirmations. That establishes software use; it
does not establish that anyone needs those tables refreshed continuously.

## Overlap and unnecessary work

The live page uses the manifest's running/queued counts, ready-to-merge count,
coverage timestamps, chart status and eleven SVG URLs. It does not use the
exported `current.builds`, `current.staging`, `github`, `pending_repairs`,
`coverage` interval arrays or `series_url` directly. Some of these remain useful
diagnostics or public exports, but publishing them is not evidence of a browser
consumer. In particular, the service emits a fresh complete series JSON file
every chart generation without an identified application reader.

The two reporting pipelines fetch overlapping job lists, telemetry artifacts
and failed-job logs, and independently calculate the same chart families.
The separate resource refresh performs full `run_record` enrichment, so a
request to gather resource artifacts can also fetch jobs, comparisons,
telemetry and failed-job logs. It is bounded, but broader than its name suggests.

There is still a real dependency on the legacy pipeline: the Pages workflow
downloads `ci.sqlite.gz` and regenerates fallback charts. The compressed release
was 355,528,740 bytes at the audit snapshot. Pages is scheduled every three
hours, with additional push/manual triggers. Replace this chart-generation
dependency with cached published report assets before retiring routine legacy
collection. SQLite can then be a deliberate export rather than a prerequisite
for serving fallback images.

The legacy collector is scheduled every twenty minutes with a ceiling of 4,500
GitHub requests per invocation; the live service has its own 500-request rolling
hourly ceiling. These are ceilings rather than measured usage, and should not
be added together as though they were one shared request counter.

The live summary workflow is another reporting job triggered on completed build
workflows. It runs on GitHub and reserves six shared API requests before doing
any reads, even if it needs fewer. Completed workflow status does not prove it
published: run 40605478624 succeeded while logging “Summary deferred: shared
GitHub budget unavailable.” Avoid making this helper a mandatory CI job for
every completion; first determine which missing data requires it beyond the
normal webhook payloads.

## Repair backlog findings

At 00:56:49 UTC, protected health reported 26,076 job checks, 9,539 detail checks,
94 run checks and one membership check. Earlier snapshots show the dominant
job/detail queues growing by approximately 130 entries per hour. These are
reporting tasks, not build jobs waiting for a runner.

All twenty oldest sampled job checks had completed run metadata but no stored
jobs. Their workflows were PR status, label, auto-merge, recovery and publishing
helpers. All twenty oldest sampled detail checks had four verified, completed
jobs for a PR build and no phase telemetry. The oldest sampled job check was due
on 19 October. These oldest-entry samples demonstrate dispensable recovery
work; they are not a random sample and cannot establish a percentage for the
whole backlog.

Several code paths sustain the backlog:

1. Inventory passes completed runs through normal ingestion, which queues job
   and detail work for completed PR builds even when stored facts already
   satisfy that work. The later selective inventory condition does not undo it.
2. A summary with no artifact omits telemetry rather than recording a completed
   search with an unavailable result. A detail repair can repeat the search.
3. Optional repairs have no age limit. Fresh/archive priority is stored at
   enqueue time, so old entries can retain their original fresh priority.
4. The routine for pruning satisfied repairs is an admin operation rather than
   an automatic admission/retirement guarantee.

The proposed policy is to repair current/missing build state first, give optional
phase/log enrichment a small separate allowance and a 24-hour lifetime, and
abandon old auxiliary and optional detail recovery. Record terminal reasons so
the next inventory does not recreate abandoned work. Retain a small independent
correctness sample. Increasing the budget is not the first remedy.

## Retention findings

The live service's 30-day general and 90-day build limits move facts out of
SQLite into compressed R2 archives; they do not delete them. Old detailed metric
distributions and receipts likewise move to R2 after 90 days. Compact summaries
and fact archives have no configured expiry. The run directory also retains its
mapping indefinitely.

Setup configures 14-day expiry for processed deliveries and rendered assets.
Failed deliveries and validation JSON have no configured lifecycle expiry.
SQLite comparison records expire after 30 days, but their R2 copies remain.
The legacy collector stores append-only records in Git with no automatic expiry;
removing files later would not remove their historical copies from Git.

A smaller default would retain compact daily summaries long term, retain raw
build facts only for a defined correction/debugging window, retain auxiliary
facts for the 30-day fleet view, and bound failure/validation evidence. Deleting
old source facts requires retiring their repairs and defining the supported
correction window first. Existing historical records need not be rewritten to
stop future overcollection.

## Recommended implementation order

1. Stop ruleset snapshots and unused per-module detail fields. Keep identity,
   runner, timing and outcome evidence.
2. Fix repair admission, expiry and retirement; discard the old optional and
   auxiliary recovery backlog under that policy. Preserve observed facts and
   label incomplete historical coverage.
3. Make resource collection and detailed compare/replay enrichment explicit
   analysis modes. Keep small, purposeful samples where an ongoing report needs
   them; do not create another completeness queue.
4. Serve static fallbacks from already-published report assets, then remove
   overlapping routine chart collection and publishing. Keep SQLite export as
   an intentional analysis capability.
5. Enforce archive retention consistently, including failed deliveries,
   validation copies, raw facts and directory mappings.

These are recommendations. This audit changes no running collectors, retained
production data, dispatcher settings, worker capacity or Bors behavior.

## Evidence

- [Displayed charts and their explanations](https://github.com/TauCetiProject/TauCeti/blob/da2116658e712c681fdddb6d36af07a70f8865fd/web/Site/CI.lean)
  and [browser consumers](https://github.com/TauCetiProject/TauCeti/blob/da2116658e712c681fdddb6d36af07a70f8865fd/web/static_files/ci-live.js).
- [Live metric inputs](https://github.com/TauCetiProject/TauCetiCI/blob/a9a48b77a735a41db1fb4004175d3350bece1fd5/services/live-ci/src/model.ts),
  [chart consumers](https://github.com/TauCetiProject/TauCetiCI/blob/a9a48b77a735a41db1fb4004175d3350bece1fd5/services/live-ci/src/charts.ts),
  [repair and publication paths](https://github.com/TauCetiProject/TauCetiCI/blob/a9a48b77a735a41db1fb4004175d3350bece1fd5/services/live-ci/src/control.ts)
  and [fact storage](https://github.com/TauCetiProject/TauCetiCI/blob/a9a48b77a735a41db1fb4004175d3350bece1fd5/services/live-ci/src/storage.ts).
- [Legacy collector workflow](https://github.com/TauCetiProject/TauCetiCI/blob/52adccc5a863abb4fbe38681c755c80e0e9b2ead/.github/workflows/collect.yml),
  [resource refresh](https://github.com/TauCetiProject/TauCetiCI/blob/52adccc5a863abb4fbe38681c755c80e0e9b2ead/.github/workflows/resources.yml),
  [SQLite and modelling publication](https://github.com/TauCetiProject/TauCetiCI/blob/52adccc5a863abb4fbe38681c755c80e0e9b2ead/.github/workflows/publish.yml),
  [report consumers](https://github.com/TauCetiProject/TauCetiCI/blob/52adccc5a863abb4fbe38681c755c80e0e9b2ead/analysis/report.py)
  and [optional replay consumers](https://github.com/TauCetiProject/TauCetiCI/blob/52adccc5a863abb4fbe38681c755c80e0e9b2ead/analysis/replay.py).
- [Pinned collector inputs](https://github.com/TauCetiProject/TauCetiTools/blob/4c7381d3a47fd9b9f7aa786dd2a28c375e36fee6/tauceti_tools/collector/collect.py),
  [bounded resource refresh implementation](https://github.com/TauCetiProject/TauCetiTools/blob/4c7381d3a47fd9b9f7aa786dd2a28c375e36fee6/tauceti_tools/collector/refresh_resources.py)
  and [published database schema](https://github.com/TauCetiProject/TauCetiTools/blob/76a770e94c027cd3b8c9e1244b401fe9975cce96/tauceti_tools/collector/build_db.py).
- [Live summary producer](https://github.com/TauCetiProject/TauCetiTools/blob/ad4e955b398d1419b0931e3c825a925f1c6a6e9b/tauceti_tools/assets/scripts/ci_live_summary.py),
  [telemetry producer](https://github.com/TauCetiProject/TauCetiTools/blob/ad4e955b398d1419b0931e3c825a925f1c6a6e9b/tauceti_tools/assets/scripts/ci_telemetry.py),
  [Pages fallback generation](https://github.com/TauCetiProject/TauCeti/blob/da2116658e712c681fdddb6d36af07a70f8865fd/.github/workflows/pages.yml)
  and [configured retention](https://github.com/TauCetiProject/TauCetiCI/blob/a9a48b77a735a41db1fb4004175d3350bece1fd5/services/live-ci/scripts/setup.py).

Production HTML, manifest and health snapshots are saved locally in
`~/.local/state/tauceti-fleet-analysis/ci-data-audit-20261028/`. The forty stored-fact
samples are in `../ci-reconciliation-20261028/oldest-samples.json` relative to that
snapshot directory. Protected health identified deployment
`03800d6c-e519-4044-b8dc-7df616e807f3`; its recorded source is `a9a48b77`.
