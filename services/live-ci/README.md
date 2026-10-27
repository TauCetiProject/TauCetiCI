# Live CI reports

Cloudflare hosts the CI event store, repair scheduler and report API. The public
website remains on GitHub Pages. Visiting it never initiates a GitHub read.

Production is `https://ci-data.taucetiproject.org`. It starts in **shadow** mode:
the page retains its existing charts until validation and explicit activation.
The current collector and downloadable SQLite release remain available during
that transition.

## Processing and storage

Signed GitHub deliveries are archived before they enter the queue.
Up to 16 consumer invocations process at most four messages concurrently each;
this capacity does not change the separate GitHub read budget.
A sharded run directory routes job events to their repository/UTC-day fact object. Each
run attempt and job has its own identity. Terminal observations cannot be
reopened by older deliveries. Later summaries preserve metadata already enriched
by an earlier summary.

Every fact replacement commits its previous/new metric contribution and an
outbox together. Day objects apply outbox deltas with persistent receipts.
Retries across either object boundary cannot double-count.
Outbox delivery groups at most 40 updates for the same day into one request,
retaining each update's individual receipt. Overlapping batches are safe to
replay, and deployment skew falls back to individual updates.
Fact activity and dirty-day notifications return to the scheduler through durable
alarms, starting
new request chains instead of reentering ingestion. Notifications coalesce by
revision, survive failures and do not postpone an earlier scheduled alarm when
more events arrive. Daily duration/wait
distributions retain exact nearest-rank percentiles; sums and counts are
reversible. Fleet bins retain running-time integrals, queue interval boundaries,
wait distributions and Namespace picker ticks. Matching queue departures are
applied before arrivals. A trailing-wait window includes jobs completed before
the chart begins.

Active intervals advance in bounded alarm batches. Drain-only wakes never
refresh intervals: existing outbox receipts drain before the next refresh page,
and a persisted deadline starts the next sweep a minute after one finishes.
This prevents a delivery backlog from generating more updates on every retry.
Only dirty time bins are recalculated. SQLite computes queue peaks and exact
daily quantiles, returning compact results without loading a day's interval boundaries or timing samples
into the Worker heap. Publishing reads at most 90 materialized daily summaries,
rather than raw jobs or all historical records. Current activity publishes each
minute; changed chart generations publish every ten minutes. The current UTC day
is provisional. An unavailable or unfinished input remains explicit.
Publication runs independently of repair batches, so initial reconciliation
cannot hold up snapshots of incoming webhook activity.

Self-hosted jobs have their own fleet and runner-minute series. Their build
outcomes, durations and waits contribute to the same PR and merge statistics as
GitHub-hosted and Namespace jobs.
Bors membership repairs run in a separate bounded lane, independent of GitHub
quota. Staging coverage counts missing membership and pending repairs; completed
runs with unknown batch sizes remain visible as diamonds below the scatter plot.

General facts move to compressed R2 archives after 30 days; build facts retain
90 days. Archival operates in bounded run batches, retaining the old contribution
with each archived fact. A correction restores those facts before calculating
its delta. After 90 days, exact aggregate distributions and receipts also move
to compressed, resumable archive pages; compact daily summaries remain in R2.
Historical corrections restore just their affected days. Raw deliveries and
retired rendered assets expire after 14 days. Fact archives and summary history
are retained. SQLite objects support Cloudflare point-in-time recovery.

The request scheduler and publisher use a short persisted lease. Normal read
work, completed-attempt verification and missing-artifact/log recovery share a
rolling ceiling of 500 request attempts per hour, including retries and token
exchange. Completed resources retire; 404/410 responses become explicit gaps.
Secondary limits pause reads and publisher admissions. Historical import never
spends GitHub quota. Bootstrap is deliberately separate from the freshness path.

The trusted `ci-live-summary.yml` workflow in TauCeti reserves six requests before
reading run metadata, jobs, artifacts or failed-job logs. Its OIDC token must
identify that exact main-branch workflow and repository. Reservations count the
full allowance conservatively even when fewer requests are used.
At the default budget, publisher reservations use at most 360 of the 500 slots
per rolling hour, leaving 140 for reconciliation and token exchanges. Deferred
summaries remain repairable from webhooks and the attempt-specific repair queue.
The publisher reports
quota rejections to the shared scheduler. A fresh runner parses bounded JSON
artifacts; it neither checks out nor executes candidate code. Candidate phase
measurements remain statistics and cannot establish producer or merge identity.

## Public contract

`GET /v1/ci/manifest.json` returns `tauceti-ci.manifest/v1`:

- `mode`: `shadow` or `live`.
- `generated_at`, `observed_at`: publication and event-receipt times.
- `current`: observed running/queued counts by runner, active builds, staging
  batches, inventory confirmation time, computation time and truncation state.
- `groups`: independent fleet, daily, detail, queue and staging publication times,
  source cutoffs, coverage description, pending work and known gaps.
- `assets`: SVG filenames mapped to immutable revisioned URLs.
- `series_url`: the bounded materialized JSON series behind those assets.
- `coverage`: enumerated inventory intervals per repository.
- `github`: rolling request/reservation accounting, budget and cooldown.

Webhook observation is **not exhaustive enumeration**: `complete` remains false.
Inventory coverage, detail completeness and rendering timestamps are distinct.
An empty observation is not proof of zero activity. The browser labels stale
coverage, preserves working images after failures and pauses polling when hidden.

Ready-to-merge history uses the last completed candidate snapshot while bors
scans for changes, matching the legacy report. A partial startup scan without a
completed snapshot remains a gap. Explicit partial observations break the line,
as do missing observations more than three minutes apart; isolated valid samples
remain visible. The time axis starts at the first recorded day, within the
thirty-day window. Queue snapshots were first recorded on 2026-10-04.

Assets live at `/v1/ci/assets/<revision>/<filename>`. They are uploaded before the
manifest pointer changes. Public reads use R2 only, support ETags and CORS, and
carry no GitHub credentials. `/healthz` exposes service identity/version only.
Protected `/admin/health` exposes scheduler and processing diagnostics.
`GET /admin/repairs?kind=run` returns at most 20 pending repair records.
`POST /admin/prune-repairs` checks up to 100 repairs against stored facts and
removes only satisfied work; follow its returned cursor to scan the backlog.
`POST /admin/replay-failed` requeues up to 100 retained failed deliveries;
follow its returned cursor for additional pages. These operations require the
admin bearer token and make no GitHub requests. Successful replay moves the body
back to the normal delivery archive; duplicate messages remain idempotent.

`POST /admin/repair-members` processes up to eight due bors membership repairs.
`POST /admin/recompute` with `{"repo":"TauCetiProject/TauCeti","day":"2026-10-17"}`
recalculates at most forty stored self-hosted jobs using reversible metric deltas.
`changed` counts reclassified jobs and `unidentified` counts jobs still lacking host evidence.
Pass its returned `cursor` until null; retries cannot duplicate totals. These
authenticated maintenance operations do not read GitHub. After a historical
repair, a bootstrap completion checkpoint requests a fresh chart generation.

Fleet hosts are reported as `eastanglia` (Chris) and `leanfro` (tauchonki).
Aliases come from permanent machine labels, broker host labels, or the runner
name; they do not change dispatch names or policy. Unidentified self-hosted jobs
remain in `self-hosted`. Older stored jobs without retained identity evidence
need a replay of archived job records to split their historical totals.
Rebuild `ci.sqlite` with the updated Tools version to retain archived runner names,
then rerun `scripts/bootstrap.py` with the original `--since` and
`--sampling-cutover` values. The added runner names change the content hashes of
those imports, so new deliveries replace their previous contributions. Existing
receipts for unchanged records remain valid. The raw database `runner_kind`
category stays `self-hosted`; only reporting derives the public alias.

After correcting historical jobs, `POST /admin/refresh-day` with
`{"day":"2026-10-22"}` materializes that day's latest aggregate and refreshes
published charts immediately. It returns the day's daily metrics for verification,
requires the admin bearer token and makes no GitHub requests.

`GET /admin/backlog` reports at most ten fact partitions, including each bounded
outbox count, receipt sequence, oldest receipt, active-job count, last drain and
refresh times, and refresh deadline/cursor. Follow the returned `cursor` with
`?cursor=...` for the next page. This admin-only read makes no GitHub requests
and does not flush or refresh facts. Counts above 10,000 are marked inexact;
`outbox_upper_bound` uses the receipt-id span without scanning the whole queue.
Inactive partitions remain included because they may still have pending receipts;
an unavailable partition returns its own error without hiding the rest of the page.
Interval refresh pauses during a delivery outage, so last-drain/refresh times
should be checked when aggregate charts become stale.
`next_alarm` reports the scheduled wake time in Unix seconds, or null if no
alarm is scheduled. To restart a stranded outbox, `POST /admin/resume-drain`
with `{"repo":"TauCetiProject/TauCeti","day":"2026-10-23"}` schedules its
normal drain alarm within one second. This requires the admin bearer token and
an allowed repository. It preserves an earlier future alarm, re-arms an elapsed
alarm, and does nothing when the outbox is empty; it never rewrites facts or receipts.

`POST /admin/discard-outbox` accepts the same repository/day and permanently
abandons undelivered metric corrections for an inactive historical partition.
It refuses current/future days and partitions with active jobs or runs. Raw
facts, saved contributions, already-applied aggregates and receipt sequences
remain intact; drain alarms stop. Historical aggregates stay partial rather
than replaying the discarded work. The discard time, highest receipt ID and
exact deletion count appear in `/admin/backlog` for that partition.

## Setup and bootstrap

```sh
cd services/live-ci
npm ci
npm test
python3 scripts/setup.py deploy
```

Alternatively, `python3 scripts/setup.py watch` waits for registration and
installation and performs these two finishing steps once. It checks at most
every five minutes and exits after connecting the App. This is an optional
one-time setup helper; serving reports has no host dependency.

The setup tool explicitly selects the TauCeti Cloudflare account, creates its
queues/bucket, applies retention, deploys the Worker and uploads secrets through
stdin. Private files live under `~/.config/tauceti-live-ci`, with mode 0600.

Open `https://ci-data.taucetiproject.org/setup/register` in an authenticated
GitHub browser. Create the dedicated read-only App and install it on the
organisation's fleet repositories. The callback saves credentials privately.
Then finish verification and redeploy:

```sh
python3 scripts/setup.py finish
python3 scripts/setup.py deploy
```

The App needs Actions, contents and metadata read permissions; subscriptions are
`workflow_run`, `workflow_job` and `push`. The service uses its installation
quota. The existing worker-state installation remains separate.

Build the legacy database once from an immutable archive checkout, then import:

```sh
python3 -m collector.build_db --out /path/to/bootstrap.sqlite
python3 services/live-ci/scripts/bootstrap.py --db /path/to/bootstrap.sqlite
```

Use a snapshot preceding webhook cutover. The importer preserves historical
sampling weights and running-only estimates. It streams records in bounded
batches and maintains disposable local receipts so interruptions are resumable.
Four HTTP batches run concurrently by default (`--concurrency 1` serializes
them). The completion checkpoint waits for every batch acknowledgement.
Records interleave four independent UTC days to avoid concentrating every
request on the same day object; `--interleave-days 1` restores strict recent-first
ordering. Lower `--concurrency` or pause/resume the importer if live queue age
starts growing during catch-up.
The original archives and SQLite release remain the historical export. Do not
rebuild that input database between retries of the same bootstrap operation.

To close an archive-to-webhook gap with a newer immutable database, use a new
receipt file and `--sampling-cutover 2026-10-19T01:00:00Z` (the first complete
live hour), with `--since` before the missing period. This mode imports older
runs updated since that point as well. Sample weights apply before cutover;
each observed job counts once afterwards. Unsampled run metadata records a zero
historical weight, so overlapping webhook jobs do not duplicate the sampled
cohort. Actual jobs and their timestamps remain stored, and later events retain
the cutover metadata. Completion republishes charts after every batch is saved.

Deploy the TauCeti page and summary-workflow changes through their normal PR.
No page credentials or API tokens are needed. The page remains on its fallbacks
while the service is in shadow mode.

## Validation and activation

Each hour, the service independently reads three settled run attempts through
the quota owner, after snapshotting their stored jobs. It compares job inventory,
state, runner and timestamps without repairing the reference into the mirror
first. Results are retained privately under `validation/`. Publication freshness
is required as well as agreement.

`GET /admin/activation-check` reports current readiness and comparison results.
There is no mandatory comparison waiting period. Live activation requires the
dedicated App to be configured and a report published within five minutes.
Comparisons continue after launch. To activate:

```sh
printf 'live\n' > ~/.config/tauceti-live-ci/publication-mode
python3 scripts/setup.py deploy
```

Rollback changes that file to `shadow` and redeploys. It preserves facts, repair
state and archives. Live activity can launch while historical import continues:
the browser keeps the site's complete static charts until bootstrap sends its
completion checkpoint. Import prioritizes recent runs and overlaps four event
deliveries at a time; it consumes no GitHub quota. Current inventory and missing
run metadata take priority over older job and detail repairs within the same
500-request rolling budget.
Current inventory and stale active runs also precede old inventory pages, so
initial catch-up cannot starve activity reconciliation.

Keep existing collection/publication schedules during rollout. Retire overlapping
collection schedules only after live
freshness and archival/export continuity have been verified; manual backfills
and SQLite export must remain available.

Tests cover transactional replay, reordered deliveries, attempts, exact
percentiles, phase corrections, synthetic queued cancellations, quota pauses,
HMAC authentication, cold restore and public-read isolation. A production
archive cohort is checked against the existing Python daily/fleet calculations.
The 1x/10x fixture checks contribution growth and independence from retained
history. Its local timings are not evidence of Cloudflare network capacity;
observe production queue age, request usage, storage and processing costs during
rollout before asserting a production throughput limit.
