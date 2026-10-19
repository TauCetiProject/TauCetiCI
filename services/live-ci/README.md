# Live CI reports

Cloudflare hosts the CI event store, repair scheduler and report API. The public
website remains on GitHub Pages. Visiting it never initiates a GitHub read.

Production is `https://ci-data.taucetiproject.org`. It starts in **shadow** mode:
the page retains its existing charts until validation and explicit activation.
The current collector and downloadable SQLite release remain available during
that transition.

## Processing and storage

Signed GitHub deliveries are archived before they enter the queue. A sharded
run directory routes job events to their repository/UTC-day fact object. Each
run attempt and job has its own identity. Terminal observations cannot be
reopened by older deliveries. Later summaries preserve metadata already enriched
by an earlier summary.

Every fact replacement commits its previous/new metric contribution and an
outbox together. Day objects apply outbox deltas with persistent receipts.
Retries across either object boundary cannot double-count. Daily duration/wait
distributions retain exact nearest-rank percentiles; sums and counts are
reversible. Fleet bins retain running-time integrals, queue interval boundaries,
wait distributions and Namespace picker ticks. Matching queue departures are
applied before arrivals. A trailing-wait window includes jobs completed before
the chart begins.

Active intervals advance in bounded alarm batches. Only dirty time bins are
recalculated. SQLite computes queue peaks and exact daily quantiles, returning
compact results without loading a day's interval boundaries or timing samples
into the Worker heap. Publishing reads at most 90 materialized daily summaries,
rather than raw jobs or all historical records. Current activity publishes each
minute; changed chart generations publish every ten minutes. The current UTC day
is provisional. An unavailable or unfinished input remains explicit.

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
full allowance conservatively even when fewer requests are used. It reports
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

Assets live at `/v1/ci/assets/<revision>/<filename>`. They are uploaded before the
manifest pointer changes. Public reads use R2 only, support ETags and CORS, and
carry no GitHub credentials. `/healthz` exposes service identity/version only.
Protected `/admin/health` exposes scheduler and processing diagnostics.

## Setup and bootstrap

```sh
cd services/live-ci
npm ci
npm test
python3 scripts/setup.py deploy
```

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
The original archives and SQLite release remain the historical export. Do not
rebuild that input database between retries of the same bootstrap operation.

Deploy the TauCeti page and summary-workflow changes through their normal PR.
No page credentials or API tokens are needed. The page remains on its fallbacks
while the service is in shadow mode.

## Validation and activation

Each hour, the service independently reads three settled run attempts through
the quota owner, after snapshotting their stored jobs. It compares job inventory,
state, runner and timestamps without repairing the reference into the mirror
first. Results are retained privately under `validation/`. Publication freshness
is required as well as agreement.

`GET /admin/activation-check` requires a continuous seven-day passing comparison
window, no gap longer than two hours, and a recent comparison. Setup refuses a
live deployment until this check passes. To activate after reviewing results:

```sh
printf 'live\n' > ~/.config/tauceti-live-ci/publication-mode
python3 scripts/setup.py deploy
```

Rollback changes that file to `shadow` and redeploys. It preserves facts, repair
state and archives. Keep existing collection/publication schedules during the
comparison period. Retire overlapping collection schedules only after live
freshness and archival/export continuity have been verified; manual backfills
and SQLite export must remain available.

Tests cover transactional replay, reordered deliveries, attempts, exact
percentiles, phase corrections, synthetic queued cancellations, quota pauses,
HMAC authentication, cold restore and public-read isolation. A production
archive cohort is checked against the existing Python daily/fleet calculations.
The 1x/10x fixture checks contribution growth and independence from retained
history. Its local timings are not evidence of Cloudflare network capacity;
observe production queue age, request usage, storage and processing costs during
the shadow window before asserting a production throughput limit.
