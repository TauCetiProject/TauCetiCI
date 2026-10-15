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
records/inventory/<repo>/<yyyy>/<mm>/<dd>/*.ndjson.gz  listing metadata, all statuses
records/jobs/<repo>/<yyyy>/<mm>/<dd>/*.ndjson.gz       fetched jobs for one attempt
records/runs/<repo>/<yyyy>/<mm>/<dd>/*.ndjson.gz       optional enriched run snapshots
records/inventory-coverage/<repo>/**/*.ndjson.gz      completed discovery intervals
records/merge-members/<repo>/**/*.ndjson.gz           independently fetched bors batch membership
records/observations/**/*.ndjson.gz                  bors readiness snapshots
records/main/<repo>/**/*.ndjson.gz                   main's history (what landed when)
records/settings/<repo>/<observed-at>.json           changed rulesets
collector/                                           collection and database code
```

Record files are dated by the run's `created_at`, written atomically, and never rewritten.
Metadata, jobs and optional details are separate snapshots of the same run attempt; a retry
has its own attempt identity. The derived database combines them without allowing a basic
snapshot to erase enriched data. Active runs have no terminal result until a later scan.

All TauCeti build workflows (`pr-build`, `ci`, `nightly-verify`, `pr-profile`, `lint-full`,
`pages`) have their jobs collected. The high-volume label, notification and merge-bot
workflows have their jobs sampled one run in four, chosen by run id. Optional build details
include steps, tested commits, failure excerpts and trusted resource artifacts. Pending job
work is explicit; missing optional artifacts do not pin job coverage.

## Querying

A SQLite database is rebuilt after collection, with a three-hour fallback schedule, and published as `ci.sqlite.gz` on the
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

[`collect.yml`](.github/workflows/collect.yml) is scheduled every twenty minutes. GitHub may
delay scheduled runs. Each invocation starts no requests after thirty minutes, checkpoints
completed records to git about every two minutes, and performs a final push before the
55-minute runner timeout. API exhaustion ends a phase without waiting for a quota reset.
Before each checkpoint, uncommitted page/job files are packed by kind, repository and day;
committed archives stay immutable. Interrupted discovery windows are replayed; already written pages and enrichment are retained.

Discovery records metadata before fetching jobs, logs or artifacts. Recent six-hour windows
are scanned for each repository before older backlog. The separate chronological inventory
cursor advances only through fully enumerated windows. Listing pages reuse their count request
and recursively split dense windows to respect GitHub's 1,000-result cap. A burst that cannot
be enumerated leaves coverage open rather than being silently truncated.

The disposable `.cache/collector.sqlite` index remembers known files, pending attempts and
batch membership; a cache miss rebuilds it from git. Warm invocations read only new record
files. Job fetching uses attempt-specific endpoints, persists small completed batches, retries
individual failures later, and reserves work for both current builds and sampled auxiliary
jobs before optional enrichment. Missing earlier attempts get their own metadata and job work
items; already archived attempt jobs are not fetched again. Known workflows still active after
six hours are polled by run id with an hourly backoff. Completed artifact listings and immutable
comparisons are shared within an invocation. Bors observations and batch membership have independent budgets
and do not require GitHub API quota.

`state/inventory-cursor.json` tracks contiguous discovery, `state/build-cursor.json` tracks
build-job completeness, `state/details-cursor.json` tracks optional detail work, and
`state/cursor.json` retains all sampled-job completeness. Main history has its own
`state/main-cursor.json` with overlap for commits created before they reached main. The active
repository catalog keeps archived repositories from pinning current fleet coverage.
Inventory coverage receipts also expose newer discovery islands across an older backlog.
The observation cursor advances only after every page of an archive day was read. A separate
page position resumes interrupted archives and replays the current tail page to discover new
observations, instead of downloading the entire day again. External reads and inventory also
have phase time limits, leaving time for job collection. Recent active workflows hold job coverage open; historical stuck runs remain visible in inventory
but stop pinning the watermark after the six-hour overlap. Reruns of runs created outside
that overlap require an explicit backfill; no claim of continuous historical rerun monitoring
is made. Permanently unavailable attempts and jobs are explicit gaps. A 404/410 ends that work item;
other run-specific errors retain durable retry receipts and become explicit gaps after three failures,
instead of pinning every later chart indefinitely. Shared network and API outages remain retryable. The published database retains their
reasons, and chart freshness metadata and visible notes report these gaps. Resource records
awaiting enrichment are labelled pending, separately from missing historical artifacts.

If the installation token is too small for the organisation's volume, configure
`COLLECTOR_APP_ID` and `COLLECTOR_APP_PRIVATE_KEY` for an App installed on the organisation.
The normal GitHub-hosted invocation caps requests at 4,500 including retries. Local backfills
write records and keep existing coverage cursors conservative; add `--push` only in a trusted
main checkout where completed records may be published:

```bash
python3 -m collector.incremental --since 2026-09-01 --until 2026-09-08 --jobs-for build
```

The manual `backfill` workflow uses this same collector and checkpoint path. Re-dispatching
the same range and job mode resumes its separate inventory cursor, rather than restarting
the range. Backfills have their own serialized queue; their append-only records and separate
range cursors allow normal collection to continue. Future range endpoints are capped at the
current time, and backfills never advance the observation cursor.

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
reference for the [production cap captured on Oct 14](https://github.com/TauCetiProject/TauCeti/blob/4f1a681d10b8371c56bc49158b0481d18bd949f5/bors.toml).
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
has no live scheduling or service-write integration. No optimum, automatic
enablement, 24-hour baseline gate, admission decision, cache producer change or
publication credential access is introduced. The older independent-arrival
simulator is retained and labelled as a historical illustration.

## Controlled runner experiment (#16245)

`runner-benchmark.yml` is a separate, manual, read-only experiment in **TauCetiCI**.
The bounded experiment is complete: [results and limitations](benchmarks/RESULTS.md)
recommend retaining 8CPU/16GB. All 18 replacement slots built and passed audits,
then failed the same full-library lint finding; no successful whole-pipeline
performance claim or production runner change is made. The consumed budget
does not permit another workload dispatch.

It does not dispatch `pr-build`, post production statuses, record approvals, save
Actions caches, attach persistent cache volumes, or invoke a publisher. Its token
has only contents/actions read permission; checkout credentials are removed.
Candidate compilation runs offline under the staging bwrap policy with only
`.lake` writable. The same policy first checks that credentials, the host Mathlib
download store, network, nested user namespaces and writes to trusted tooling are
unavailable. Full trusted build, lint and axiom audits remain enabled. Uploaded
artifacts contain statistics/logs only, never compiled output or publishable maps.

The existing GitHub-hosted collector tests remain enabled. A supplemental
Namespace validation job runs the same complete suite; the two experimental
control jobs also use Namespace to avoid the GitHub-hosted queue. These jobs do
not execute the representative TauCeti workloads or consume benchmark slots.

The immutable plan is `benchmarks/plan.json`: fixed landed source commits and one tooling commit, pinned
Lean and manifest digest, three workloads, three shapes, two repetitions (18
slots). Warm/light restores published batch 730 and compiles the next landed PR (6 changed files); warm/heavy
restores the same parent and compiles the entire landed batch 735 (27 PRs, 76 changed
files). The parent revision map is SHA-256 pinned and every downloaded mapping
must match it before compilation. Cold/bump starts with no root-package artifacts on that same post-bump
pin; it models full root recompilation rather than an additional dependency bump.
All cells independently install the same official toolchain and fetch the same
complete official Mathlib cache into fresh job-local directories. There is no
compressed-download or toolchain snapshot restore, so hidden cache hits cannot
bias setup. Root cache restoration uses one exact published revision, with no
ancestor fallback; a missing/partial warm cache fails that slot. SHA-256 inventories
of downloaded root mappings/archive bytes must agree across all successful cells
of a workload. These hashes establish comparability and byte integrity; the
existing audited-main publisher supplies producer trust.

The user approved exactly 18 replacement slots on 2026-10-15, for a total of
36 scheduled slots including the original failed experiment. The fixed plan
records that authorization and retires only original dispatch `38652074029`;
its evidence remains unchanged. Only the **earliest non-retired dispatch** on
`main`, attempt 1, may execute. Every worker independently rejects reruns,
including rerun-failed reusing gate outputs. Later dispatches fail the budget gate.
Do not delete workflow history or rename/copy the workflow. Failed, cancelled,
missing and timed-out replacement slots consume this budget and are not replaced.
Concurrency remains three at ordinary priority, without persistent caches or
production authority.

The approved replacement was dispatched once after landing as run `38665598590`.
Do not repeat it. To reproduce its report from retained artifacts:

```sh
gh api repos/TauCetiProject/TauCetiCI/actions/runs/38665598590/attempts/1/jobs --paginate --slurp > jobs.json
gh run download 38665598590 --repo TauCetiProject/TauCetiCI --dir benchmark-results
python3 -m benchmarks.experiment report --directory benchmark-results --jobs jobs.json
```

The final report enumerates all 18 slots even if a worker never started or uploaded
an artifact. Cache drift invalidates that workload comparison. Host resource
samplers cover setup, Mathlib, root restore, trusted drivers and sandbox separately;
they report CPU, largest-process RSS (not simultaneous process-tree memory), host
memory/CPU pressure and device I/O. Incomplete descendant accounting stays visibly
partial. Candidate phase timestamps/build logs are separately labelled untrusted
statistics. Sample counts, CPU model, actual memory/CPU count, cgroup limits and
lineage remain in each result. Setup/command costs are estimated unit-minutes,
with prepaid/overage USD ranges using the dated public Namespace rates in the plan.
They exclude checkout/provisioning/artifact overhead; use the final jobs API start
and completion times for whole-job cost, and the provider invoice for actual
billing. Neither cost model includes shared plan allocation or storage charges.

There is no approved cost ceiling or automatic runner choice. Compare elapsed
latency, compute cost and queue capacity together. At fixed job concurrency,
capacity scales inversely with service time; at a saturated vCPU quota, larger
runners may reduce concurrent jobs. Include assembly, publication, failures and
arrival rates before claiming bors can keep up. Two repetitions are a bounded
experiment, not a statistical guarantee or whole-pipeline throughput measurement.
Successful comparable validations are needed to validate a production promotion.
A production runner change follows the human-reviewed TauCeti infrastructure
process, records the previous labels for immediate revert, and uses a **new**
workflow-pinned production dispatch for verification. PR #15568's GitHub fallback
is separate.

The user chose 16CPU/32GB for bors staging after reviewing the measured
latency/cost tradeoffs. The normal Bors release and subsequent hardware checks
are recorded in [the production rollout](benchmarks/ROLLOUT.md). The experiment
cap and its failed-validation accounting remain unchanged.

The [first dispatch](https://github.com/TauCetiProject/TauCetiCI/actions/runs/38652074029)
failed during package setup because the host PATH omitted system sbin directories.
It produced 14 failed setup records and four missing records; the jobs API accounts
for 13 failed and five cancelled slots. No TauCeti build phase ran. The complete
receipt, plan, API lineage, artifact metadata and raw resource/job observations are
retained in `benchmarks/evidence/38652074029.json.gz`. These are setup/failure
observations, not runner performance evidence. Replacement performance observations
and their failed-lint limitation are retained in
`benchmarks/evidence/38665598590.json.gz` and [RESULTS.md](benchmarks/RESULTS.md).
PR #12 fixes the host PATH and adds actual Ubuntu package setup to CI.
The readiness test additionally verifies full dependency/root-cache preparation,
trusted drivers and the sandbox boundary, stopping before TauCeti compilation.

Cancelled jobs without an assigned runner and execution steps have no execution
cost estimate. GitHub can populate `started_at` for these queued jobs; using that
value would incorrectly bill queue time. Assigned-job estimates start at the first
execution step. Provider startup, billing rounding and invoice allocation remain
outside this estimate. The original 18-slot budget remains exhausted and retained. The user approved
one additional 18-slot replacement on 2026-10-15; the total scheduled cap is 36.
Further attempts require new explicit authorization.
