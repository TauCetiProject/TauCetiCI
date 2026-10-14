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
that tested commit reached main. Older dispatches still contribute bors CI cost, with unknown tested heads explicitly excluded from merge counts.

The modelling report includes total validation job minutes per actually merged PR, all
recorded attempts and failures, ordinary PR CI separately, runner sizes, cache counters,
observed arrival/backlog and switches/overlaps. The records are observational and never
authorize admission. Missing minute samples/artifacts are gaps. Eligibility is the existing
ready-to-merge label and latency begins when a head is first observed eligible; the trusted
review sweep independently revalidates every admission.

Deploy the bors endpoint and trusted TauCeti workflow telemetry before interpreting these
comparisons. Keep manual switching until several normal batches have drained in both directions.

### Merge-backend comparison accounting

The backend report includes recorded merge-validation jobs and all main-push jobs,
including publication, failed/cancelled jobs and reruns. Ordinary PR validation
is shared cost and is shown separately. Post-merge runs are attributed only when
their actual main head matches a tested head with an unambiguous engine; unknown
or ambiguous pushes remain separate. Runner-size breakdowns identify validation
cost; phase totals also include post-merge and shared costs.

Landing timing uses the earliest collected main-push workflow creation time
proving a tested commit reached main, including recorded parent ancestry for
coalesced pushes. This is an upper bound, subject to notification and collection
gaps. Staging commit creation dates are never used as landing times. Landed
heads without push evidence remain in aggregate merge counts but are excluded
from timed denominators and latency calculations.

Timed experiment snapshots in minute observations produce separate handoff,
bors and queue phases, splitting jobs that cross phase boundaries. The report
shows arrivals of newly observed eligible heads, pending backlog, sample gaps,
throughput, and shared/all-recorded CI costs. Collection does not fetch every
auxiliary workflow's jobs; missing-job counts explicitly flag incomplete totals.
Compare closed windows with similar arrival rates and workload/runner settings;
first-observed arrivals include backlog when observation coverage begins.

### Trusted resource observations

`pr-build` can publish `ci-host-resources-<attempt>` from a workflow-pinned
sampler running **outside** bwrap; `ci-publication-<attempt>` comes from the
secret-isolated publisher after the key-bearing step. TauCetiCI reads these as
bounded JSON data. Attempt naming and run/attempt validation prevent reruns from
borrowing earlier measurements. Missing, expired, partial and invalid telemetry
is nonfatal and visible. Deploy the TauCeti producer first to populate new metrics;
historical cohorts are available immediately without a 24-hour baseline gate.

Artifact names and self-reported trust labels do not establish producer trust.
The collector independently requires GitHub API provenance for TauCeti main-source
`pr-build` dispatches or `ci` main pushes/dispatches. For `pull_request_target`,
it requires the base repository and consistent pinned main references to known
reusable workflows from the API; the run's head fields describe the candidate.
Host observations must also match the independently established workflow SHA.
Candidate PR workflows, branch dispatches and contexts
without independently verified source provenance produce visible
`unverified-producer` gaps. Historical target runs without those API references
also remain unverified. Database rebuilds apply the same
rule to historical records, and unverified dependency flags cannot set cohorts.

The hourly `collect resources` workflow independently samples recent completed
`pr-build`/`ci` runs, so a slow full-history collection cannot stall new resource
data. It lists at most 100 runs per workflow within six hours, attempts at most
20 new trusted runs, caps API requests at 150 and starts no requests after five
minutes. It defers quota waits; the job timeout is 15 minutes. This recent sample
is neither exhaustive nor random. Each record carries `collection_scope`, and
`records/resource-refresh/` receipts expose list truncation, run/API caps, missing
artifacts and deferred work. No general completeness cursor advances. Manual
dispatch can name up to 20 completed runs for reproducible verification.

The derived tables are `run_attempts` (cohort, CI conclusion, workflow source),
`resource_observations` (coverage and payload per attempt), and `phase_boundaries`
(trusted API step start/end, including failure/cancel/partial jobs). `jobs.api_status`
preserves queued/in_progress/completed from the jobs API; jobs are fetched
through attempt-specific API paths so earlier execution cannot be mislabeled
as a retry. Historical missing attempt identity is excluded from retry attribution.
Earlier attempts without a collected snapshot have explicit coverage gaps; a queued job does not
contribute execution time. Historical unknown API states remain unknown.

The report separates dependency bumps (cache warmth unknown), ordinary changes
and unknown/truncated diffs, plus trigger, runner size and conclusion. Command
CPU is wait4 user+system time, including reaped descendants. Peak process RSS is
the largest process, **not** simultaneous tree RSS. Host utilization, sampled
used-memory peak, pressure and per-device I/O include other machine activity;
stacked devices can overlap and must not be summed as unique transfer volume.
Sampling reads at most 64 KiB per proc file every five seconds, capped at 2880
samples, retaining only endpoints and peaks. Sampler CPU time is reported.
The host wrapper must subreap bubblewrap's orphaned PID-namespace init to account
for sandbox CPU/RSS. Only `command_accounting_scope: reaped-descendants` contributes
command totals. Older/direct-child artifacts have explicit incomplete command
coverage; their host counters remain usable.
A SIGTERM/SIGINT flushes partial observations when the host survives; hard kills
and lost runners leave visible gaps. Producer setup and artifact upload are
best-effort; build/audit exit codes and publication gates remain authoritative.

Publication bytes/objects describe the staged payload including its revision
map, **not** actual network transfer or deduplicated object writes. Public-map
confirmation, CI success and tested-head landing are recorded separately.
Accepted PR throughput requires main-push evidence and deduplicates overlapping
batches/reruns. Retry cost and failed/cancelled cost overlap; success without
landing is not necessarily waste. Speculation attribution remains explicitly
unknown until a trusted producer designates speculative attempts. Raw resource
and pressure/device payloads remain queryable for matching workload cohorts.
Candidate-written fine-grained phase/module telemetry remains **untrusted** and
separate. No metrics feed correctness, admission, merge or cache-trust decisions.

### Advisory batching replay

The published `REPORT.md` includes deterministic replay of the latest captured
24 hours of eligibility snapshots. It compares caps 8/16/32 with a FIFO cap-128
reference for the [configured production cap](https://github.com/TauCetiProject/TauCeti/blob/main/bors.toml).
The count-only reference uses the same serial model as the alternatives; it is
not an exact reproduction of bors' parallelism, priorities, speculative train or
promotion process. Observed validation cost and actual main-push acceptance are
reported separately, with their collection gaps.

```bash
python3 -m analysis.replay --db db/ci.sqlite --out replay.json --trace
```

This writes versioned inputs, model parameters, coverage, input/trace hashes and
the complete sensitivity matrix. Repeating it against the same database produces
identical bytes. `--hours` changes the captured window; it does not generate a
new arrival process. Initial backlog age is left censored. New exact heads arrive
between their preceding and first eligibility observations; both endpoints are
replayed. Missing snapshots are gaps, and missing metadata is explicitly counted
and imputed. A new observed head or explicit loss of readiness expires the old
one. Disappearance from an incomplete snapshot does not prove withdrawal.

`replay_features` retains completed PR snapshots with exact head, diff paths and
staleness; only features available before the **earlier** arrival bound are used.
The build model fits successful ordinary staging jobs completed before the window
to candidate-written built-module counts. Insufficient or constant inputs use
explicit fallback parameters. Dependency/unknown cohorts and cancelled/failed
jobs do not train successful duration. Mixed runner sizes and module statistics
limit the fit; these are advisory hints, never trusted proof or cache evidence.
Per-head work uses prior module counts when available, otherwise a median work
estimate (at least the changed-module count). Changed paths are only an overlap
proxy: downstream dependency rebuilds and cache warmth are unknown.

Work/risk formation uses a 20-minute estimated work budget, changed-path overlap,
and an unvalidated dependency/staleness risk ordering. Every batch starts with
its oldest eligible atomic unit; units waiting an hour bypass the work budget.
The cap counts PR heads. Oversized bundles run alone and retries never split a
bundle or admit new work. Finite backlogs cannot be bypassed indefinitely; elapsed
starvation bounds require bounded service times and arrival rates. Unknown
historical bundles are counted separately from the synthetic atomic-pair stress
case. No bundle metadata is inferred from overlapping staging batches.

Low/high proof-failure scenarios use stable SHA-256 draws per exact head. These
are injected outcomes, not attribution of observed batch failures to members.
Infrastructure retries keep frozen membership and stop after two retries with
an unresolved infrastructure result; they never reject a head as a proof failure.
Isolation, infrastructure and stale-work costs are shown separately (these can
overlap). Dependency bumps, stale bases, unknown telemetry, work scale and overlap
also have explicit stress comparisons. Runner minutes mean wall time, not CPU,
dollars or measured network cost. Head versions are separate replay arrivals,
so simulated head throughput is not a count of unique accepted PRs.

The advisory recommendation is to retain the current production policy while
improving coverage, then require matched replay and canary benefit before any
adaptive scheduling. `adaptive_scheduling_enabled` is always false: this module
has no live scheduling integration or write capability. No optimum, automatic
enablement, 24-hour baseline gate, admission decision, cache producer change or
publication credential access is introduced. The older independent-arrival
simulator is retained and labelled as a historical illustration.
