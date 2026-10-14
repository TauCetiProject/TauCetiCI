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

## Controlled runner experiment (#16245)

`runner-benchmark.yml` is a separate, manual, read-only experiment in **TauCetiCI**.
It does not dispatch `pr-build`, post production statuses, record approvals, save
Actions caches, attach persistent cache volumes, or invoke a publisher. Its token
has only contents/actions read permission; checkout credentials are removed.
Candidate compilation runs offline under the staging bwrap policy with only
`.lake` writable. The same policy first checks that credentials, the host Mathlib
download store, network, nested user namespaces and writes to trusted tooling are
unavailable. Full trusted build, lint and axiom audits remain enabled. Uploaded
artifacts contain statistics/logs only, never compiled output or publishable maps.

The immutable plan is `benchmarks/plan.json`: one source/tooling commit, pinned
Lean and manifest digest, three workloads, three shapes, two repetitions (18
slots). Warm/light restores the exact parent of one landed PR; warm/heavy restores
the exact parent of a 12-PR landed interval (36 changed files). Both compile the
same head. Cold/bump starts with no root-package artifacts on that same post-bump
pin; it models full root recompilation rather than an additional dependency bump.
All cells independently install the same official toolchain and fetch the same
complete official Mathlib cache into fresh job-local directories. There is no
compressed-download or toolchain snapshot restore, so hidden cache hits cannot
bias setup. Root cache restoration uses one exact published revision, with no
ancestor fallback; a missing/partial warm cache fails that slot. SHA-256 inventories
of downloaded root mappings/archive bytes must agree across all successful cells
of a workload. These hashes establish comparability and byte integrity; the
existing audited-main publisher supplies producer trust.

Only the **earliest dispatch of this workflow** on `main`, attempt 1, may execute.
Every worker independently rejects reruns, including rerun-failed reusing gate
outputs. Later dispatches fail the budget gate. **Do not delete workflow run history
or rename/copy this workflow to restart the experiment**: failed, cancelled,
missing, timed-out and mismatched slots consume the original budget; they are not
replaced. Concurrency is at most three jobs at ordinary priority, without bors
priority or cancellation. The experiment has no production/default-runner effect.

After review and landing, dispatch once:

```sh
gh workflow run runner-benchmark.yml --repo TauCetiProject/TauCetiCI --ref main
gh run watch RUN_ID --repo TauCetiProject/TauCetiCI --exit-status --interval 120
gh api repos/TauCetiProject/TauCetiCI/actions/runs/RUN_ID/jobs --paginate --slurp > jobs.json
gh run download RUN_ID --repo TauCetiProject/TauCetiCI --dir benchmark-results
python3 -m benchmarks.experiment report --directory benchmark-results --jobs jobs.json
```

The final report enumerates all 18 slots even if a worker never started or uploaded
an artifact. Cache drift invalidates that workload comparison. Host resource
samplers cover setup, Mathlib, root restore, trusted drivers and sandbox separately;
they report CPU, largest-process RSS (not simultaneous process-tree memory), host
memory/CPU pressure and device I/O. Incomplete descendant accounting stays visibly
partial. Candidate phase timestamps/build logs are separately labelled untrusted
statistics. Raw samples, CPU model, actual memory/CPU count, cgroup limits and
lineage remain in each result. Setup/command costs are estimated unit-minutes,
with prepaid/overage USD ranges using the dated public Namespace rates in the plan.
They exclude checkout/provisioning/artifact overhead; use the final jobs API start
and completion times for whole-job cost, and the provider invoice for actual
billing. Neither cost model includes shared plan allocation or storage charges.

Recommendations require both repetitions on baseline and candidate, at least 10%
less median elapsed and at most 10% greater compute cost. The two-run sample is a
bounded experiment, not a statistical guarantee or whole-pipeline throughput claim.
Review per-workload resource/phase data and whole-job costs before proposing any
default change. Keeping 8CPU/16GB is a valid result. A justified production change
must be a separate human-reviewed TauCeti infrastructure PR with the previous labels
recorded for immediate revert, followed by a **new** workflow-pinned production
dispatch and verification. PR #15568's GitHub fallback is separate.
