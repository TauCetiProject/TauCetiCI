# TauCetiCI

Durable, queryable records of every GitHub Actions run in the
[TauCetiProject](https://github.com/TauCetiProject) organisation, and the analyses built on them.

The records answer, for each CI run:

- **What was tested:** the head commit; for PR builds, the PR, its merge base, and how far it was
  behind `main`; for merge-queue builds, which PRs were grouped and in what order.
- **Where it ran:** GitHub-hosted or Namespace, and the machine size.
- **How long it waited:** job created to job started.
- **How long it took:** per job and, for TauCeti's build workflows, per step.
- **What failed:** the first failing step, a failure class, and the error lines of that step.

## Layout

```
records/runs/<repo>/<yyyy>/<mm>/<dd>/<collected-at>.ndjson.gz   one line per completed run
records/main/<repo>/<yyyy>/<mm>/<dd>/<collected-at>.ndjson.gz   main's history (what landed when)
records/settings/<repo>/<observed-at>.json                     rulesets (merge queue) when changed
collector/                                                     collection and database code
```

Record files are dated by the run's `created_at`, written once, and never rewritten. A run appears
once per attempt; a re-run adds a record for the new attempt.

Every run carries its jobs. Runs of TauCeti's build workflows (`pr-build`, `ci`, `nightly-verify`,
`pr-profile`, `lint-full`, `pages`) also carry steps, `tested` (the commits), and a `failure`
object on each failed job. The high-volume label, notification and merge-bot workflows are
recorded with jobs only, and only for one run in four (chosen by run id); the rest, and all of them
in long backfills, are recorded at run level (`jobs_fetched: false`). The sample keeps their queue
waits unbiased at a quarter of the API cost.

## Querying

A SQLite database is rebuilt every three hours from the records and published as `ci.sqlite.gz` on the
[`db` release](https://github.com/TauCetiProject/TauCetiCI/releases/tag/db):

```bash
gh release download db -R TauCetiProject/TauCetiCI -p ci.sqlite.gz && gunzip ci.sqlite.gz
sqlite3 ci.sqlite "SELECT runner_kind, COUNT(*), AVG(wait_s), AVG(run_s)
                   FROM jobs WHERE name = 'sandboxed-build' GROUP BY 1"
```

Or build it locally from a checkout with `python3 -m collector.build_db` (writes `db/ci.sqlite`).
The tables are described in [`collector/build_db.py`](collector/build_db.py). DuckDB can also read
the records directly: `SELECT * FROM read_ndjson('records/runs/TauCeti/**/*.ndjson.gz')`.

## Collection

[`collect.yml`](.github/workflows/collect.yml) runs hourly. It resumes from `state/cursor.json`
with six hours of overlap for long runs, and skips runs already recorded. It sizes its API budget
from the token's live rate limit and spends it on build workflows first, leaving anything it cannot
afford for the next hour. If `github.token` proves too small for the organisation's volume (about
1,500 runs an hour reach a runner), set `COLLECTOR_APP_ID` and `COLLECTOR_APP_PRIVATE_KEY` for an
App installed on the organisation.

Backfill a period locally with, for example:

```bash
python3 -m collector.collect --since 2026-09-01 --until 2026-09-08 --jobs-for build
```

Job logs are kept by GitHub for 90 days, so failure excerpts can be backfilled only that far.
Run and job metadata do not expire.

## Analysis

[`analysis/report.py`](analysis/report.py) models failure rates, rebase and merge-queue risk,
GitHub-hosted runner capacity, merge-queue settings (by simulation) and the economics of batching
PR builds, from the database. The publish workflow regenerates it with the database, as the `db`
release's `REPORT.md`.

## Merge backend experiments

The collector also reads the minute observations archived by the hosted bors Worker.
Engine identity comes from merge_group or trusted staging telemetry, never MERGE_BACKEND
at collection time. A bors repository_dispatch run's head_sha names the workflow source;
merge_builds stores its actual tested head/base, batch ID, approved members, and whether
that tested commit reached main. Older dispatches without this metadata stay unattributed.

The modelling report includes total validation job minutes per actually merged PR, all
recorded attempts and failures, ordinary PR CI separately, runner sizes, cache counters,
observed arrival/backlog and switches/overlaps. The records are observational and never
authorize admission. Missing minute samples/artifacts are gaps. Eligibility is the existing
ready-to-merge label and latency begins when a head is first observed eligible; the trusted
review sweep independently revalidates every admission.

Deploy the bors endpoint and trusted TauCeti workflow telemetry before interpreting these
comparisons. Keep manual switching until several normal batches have drained in both directions.
