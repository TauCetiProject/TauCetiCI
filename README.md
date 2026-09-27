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
recorded with jobs only. Long backfills may record those at run level only (`jobs_fetched: false`).

## Querying

A SQLite database is rebuilt daily from the records and published as `ci.sqlite.gz` on the
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
