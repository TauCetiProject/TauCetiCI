# Runner experiment 16245

All 18 approved replacement slots are accounted. The earlier unconditional
recommendation to retain 8CPU/16GB is withdrawn; it used an unapproved cost ceiling.
All workload validations failed the same full-library lint finding. After reviewing
the measured tradeoffs, the user separately authorized 16CPU/32GB for bors staging;
the production change landed as described in [ROLLOUT.md](ROLLOUT.md).

## Protocol and budget

The original experiment scheduled 18 slots in
[run 38652074029](https://github.com/TauCetiProject/TauCetiCI/actions/runs/38652074029).
A setup PATH defect stopped it before any TauCeti build. Its permanent
[receipt](evidence/38652074029.json.gz) retains 14 failed setup records and four
missing records; job conclusions were 13 failed and five cancelled. Known
allocated execution cost was 114.933 unit-minutes; four slots have unavailable
execution timing. Queue time is excluded from execution-cost estimates.

The user explicitly approved exactly 18 replacement slots on 2026-10-15,
bringing the total authorized scheduled budget to 36. The
[approval ledger](https://github.com/TauCetiProject/TauCeti/issues/16245#issuecomment-6181946188)
and [PR #15](https://github.com/TauCetiProject/TauCetiCI/pull/15) retire only that
original dispatch. Later dispatches and reruns cannot obtain more slots.

The replacement is
[run 38665598590](https://github.com/TauCetiProject/TauCetiCI/actions/runs/38665598590),
dispatched once from merged harness commit
`8914073349ef3ba7fa01dfdbe2587f70467bffff`. It schedules three workloads across
8CPU/16GB, 8CPU/32GB and 16CPU/32GB, two repetitions each, at most three concurrent
benchmark jobs. The complete input contract is [plan.json](plan.json).

| Input | Immutable value |
|---|---|
| Trusted TauCeti tooling and warm/heavy source | `4f1a681d10b8371c56bc49158b0481d18bd949f5` |
| Warm/light source | `99f5b2a9f6eb5c925917e64124960cf9a442751b` |
| Published warm parent | `720bf8c135bd2c354a15d55a73f2fc2c3d7aa358` |
| Toolchain | `leanprover/lean4:v4.35.0-rc3` |
| Manifest SHA-256 | `d2cc624cfe9ec5ac6f21940c79dca2c55413ac6cd2ea0ce3666ce348c4e4cee5` |
| Public parent map SHA-256 | `8163a900a187830756f0a066f20bf3d5b723a665ba4ee7b3cea261b73babdec6` |

Warm/light changes six source files in one landed PR. Warm/heavy contains batch
735, 27 PRs and 76 changed source files. Both start from the exact published
batch-730 map and archive inventory. The cold workload uses the heavy source
with empty root build/cache directories and the same official warm Mathlib
dependencies. It measures a root-cache miss on fixed post-bump pins; it does not
change dependencies or measure downloading an uncached Mathlib build.

## Scope and interpretation

Every slot invokes the fixed trusted sandbox script with full duplicate, axiom,
module-system and environment-lint checks. The benchmark leaves environment
lint unscoped. Production's
[lint-scope.sh](https://github.com/TauCetiProject/TauCeti/blob/4f1a681d10b8371c56bc49158b0481d18bd949f5/scripts/lint-scope.sh)
can instead select changed modules, with full-library exceptions/fallbacks.
Whole-job measurements here describe full-library validation. Compile/restore
and audit measurements must be distinguished from ordinary scoped-production
end-to-end latency. Production publication, approval/status orchestration and
experiment queueing are excluded by design. Executor speedups do not establish
whole-production-pipeline speedups; the phase mix also differs from the historical
ordinary-run sample.

The first completed warm/light repetitions built successfully and passed the
duplicate/axiom/module audits, then failed full environment lint on the unchanged
`simpNF HomotopyGroup.ofMul_const` declaration. Failed slots remain failed;
timings through that gate do not establish successful whole-pipeline performance.
No input, gate, lint baseline or workload is changed during this experiment.

Host resource records come from the workflow-pinned sampler outside the
sandbox. CPU totals cover reaped descendants. Used memory is the host estimate
`MemTotal - MemAvailable`, sampled every five seconds, and can miss short peaks.
Largest-process RSS includes mapped files and is not simultaneous tree RSS.
Host CPU, memory, pressure and I/O counters can include other machine
activity. Candidate phase/log output is separate, untrusted statistical data.
Two repetitions provide limited evidence about variance.

Costs use assigned-runner execution from the first actual job step to completion,
excluding queue time. Requested shapes cost 8, 16 and 16 compute units/minute,
respectively. The pinned public rates are $0.001 prepaid or $0.0015 overage per
unit-minute ([Namespace pricing](https://namespace.so/pricing), checked
2026-10-14). Estimates exclude control/readiness jobs and storage; provider
invoices remain authoritative. Experiment matrix scheduling and provider
capacity waits are separate from execution-cost comparisons.

## Isolation and deployment evidence

Merged PRs [#10](https://github.com/TauCetiProject/TauCetiCI/pull/10),
[#12](https://github.com/TauCetiProject/TauCetiCI/pull/12),
[#13](https://github.com/TauCetiProject/TauCetiCI/pull/13) and
[#15](https://github.com/TauCetiProject/TauCetiCI/pull/15) deploy the isolated
harness, setup correction, readiness/evidence retention and bounded replacement
authorization. Post-merge
[readiness run 38665599400](https://github.com/TauCetiProject/TauCetiCI/actions/runs/38665599400)
passed 81 tests, compileall, shell syntax, full pinned input/cache preparation,
trusted native drivers and actual sandbox canaries in 1m59s.

The live
[16CPU warm/light job](https://github.com/TauCetiProject/TauCetiCI/actions/runs/38665598590/job/116196828930)
confirms Actions/Contents/Metadata read-only token permissions and denied writes
to the toolchain/trusted checkout, nested user namespaces and network access.
Candidate processes receive a fresh environment containing only listed build
settings. There are no
cache-save actions, persistent-cache mounts, production status/approval calls,
publication credentials or publisher path. Only statistics are uploaded;
compiled outputs remain in the disposable job. Integrity hashes identify bytes
and establish comparability; they do not establish proof correctness.

Production's
[runner selection](https://github.com/TauCetiProject/TauCeti/blob/346965ee5b080ab881c80e16054c36f5ac19af97/.github/workflows/pr-build.yml#L108)
now selects 16CPU/32GB for bors staging. The user authorized this separate
production change after reviewing latency and cost; the benchmark has no
automatic promotion path. See [ROLLOUT.md](ROLLOUT.md) for release verification.

## Measurements and recommendation

**16CPU/32GB is the strongest measured latency candidate, especially for cold builds.**
The previous 10% cost-increase limit was introduced by the implementation session;
it was absent from the issue and user requirements. It is not an approved policy.
The controlled experiment did not produce a successful full-validation pair:
all 18 attempts passed build and
specified audits, then failed the same unscoped full-library lint finding.
These measurements show execution tradeoffs through that common failed gate,
not successful bors service times.

The tables below compare two repetitions through that common failed gate. They describe the fixed full-validation workload and retain 0/2 successful validations for every cell.

| Workload | Shape | Successful / executed | Median assigned-job seconds | Median compute unit-minutes | Elapsed reduction | Compute-cost increase |
|---|---|---:|---:|---:|---:|---:|
| Warm/light | 8CPU/16GB | 0 / 2 | 1218.5 | 162.467 | 0.0% | 0.0% |
| Warm/light | 8CPU/32GB | 0 / 2 | 1097.0 | 292.533 | 10.0% | 80.1% |
| Warm/light | 16CPU/32GB | 0 / 2 | 877.5 | 234.000 | 28.0% | 44.0% |
| Warm/heavy | 8CPU/16GB | 0 / 2 | 1585.5 | 211.400 | 0.0% | 0.0% |
| Warm/heavy | 8CPU/32GB | 0 / 2 | 1581.5 | 421.733 | 0.3% | 99.5% |
| Warm/heavy | 16CPU/32GB | 0 / 2 | 1149.5 | 306.533 | 27.5% | 45.0% |
| Cold/root | 8CPU/16GB | 0 / 2 | 7579.5 | 1010.600 | 0.0% | 0.0% |
| Cold/root | 8CPU/32GB | 0 / 2 | 6139.0 | 1637.067 | 19.0% | 62.0% |
| Cold/root | 16CPU/32GB | 0 / 2 | 4506.5 | 1201.733 | 40.5% | 18.9% |

Fine phase boundaries below are untrusted log statistics; host resource accounting and final job states remain separate. Environment lint failed before source-style lint; publication was disabled.

| Workload | Shape | Median build/restore s | Median audits s | Median full environment-lint s |
|---|---|---:|---:|---:|
| Warm/light | 8CPU/16GB | 426.6 | 96.3 | 568.7 |
| Warm/light | 8CPU/32GB | 418.1 | 60.8 | 497.4 |
| Warm/light | 16CPU/32GB | 289.2 | 51.2 | 422.8 |
| Warm/heavy | 8CPU/16GB | 808.4 | 103.9 | 561.0 |
| Warm/heavy | 8CPU/32GB | 834.9 | 70.9 | 542.7 |
| Warm/heavy | 16CPU/32GB | 527.3 | 67.3 | 444.6 |
| Cold/root | 8CPU/16GB | 6752.7 | 96.3 | 600.3 |
| Cold/root | 8CPU/32GB | 5368.9 | 69.7 | 597.6 |
| Cold/root | 16CPU/32GB | 3904.3 | 70.1 | 441.5 |

| Workload | Shape | Median sandbox CPU cores | Peak sampled used GiB | Largest-process RSS GiB | Median memory PSI some / full s |
|---|---|---:|---:|---:|---:|
| Warm/light | 8CPU/16GB | 5.92 | 5.45 | 11.64 | 8.38 / 7.38 |
| Warm/light | 8CPU/32GB | 6.15 | 6.26 | 14.91 | 0.00 / 0.00 |
| Warm/light | 16CPU/32GB | 10.49 | 7.04 | 15.44 | 0.00 / 0.00 |
| Warm/heavy | 8CPU/16GB | 6.84 | 5.16 | 11.63 | 9.11 / 7.61 |
| Warm/heavy | 8CPU/32GB | 7.02 | 6.02 | 14.86 | 0.00 / 0.00 |
| Warm/heavy | 16CPU/32GB | 12.27 | 7.57 | 15.53 | 0.12 / 0.09 |
| Cold/root | 8CPU/16GB | 7.63 | 10.89 | 11.80 | 17.45 / 13.52 |
| Cold/root | 8CPU/32GB | 7.69 | 11.15 | 14.56 | 3.04 / 2.16 |
| Cold/root | 16CPU/32GB | 14.44 | 13.54 | 15.15 | 2.01 / 1.36 |

Known replacement worker execution: **10956.133 compute unit-minutes**, approximately **$10.956 prepaid / $16.434 overage** at the pinned rates. Add 114.933 known worker unit-minutes from the original setup-only dispatch; its four unallocated/timing-unavailable slots remain explicit. Control/readiness/validation jobs, storage and provider startup/billing differences are excluded.

The cold baseline uses a median 7.63 of eight CPU cores; its median memory
pressure is 17.45 seconds across roughly two hours of sandbox execution. No
trial failed due to OOM or timeout.

### Cost versus latency and queue capacity

The following comparisons use the same observed medians above. Dollar estimates
use the pinned prepaid-to-overage rates, not the provider invoice. Capacity is
an inference at unchanged job concurrency with a saturated executor; it excludes
assembly, publication, failures/retries and provider waiting. It is not measured
accepted PRs per hour.

| Workload | 8CPU16GB → 16CPU32GB minutes | Minutes saved | Extra USD per attempt | Compute cost increase | Fixed-concurrency attempt capacity increase |
|---|---:|---:|---:|---:|---:|
| Warm/light | 20.31 → 14.63 | 5.68 | $0.072–0.107 | 44.0% | 38.9% |
| Warm/heavy | 26.43 → 19.16 | 7.27 | $0.095–0.143 | 45.0% | 37.9% |
| Cold/root | 126.33 → 75.11 | 51.22 | $0.191–0.287 | 18.9% | 68.2% |

8CPU/32GB has the same per-minute compute price as 16CPU/32GB and is slower
in every workload median. It has no measured advantage here.

A queue near saturation can benefit substantially from a service-time reduction.
For illustration, a 28% reduction moves executor utilization from 90% to 64.8%
at unchanged arrival rate and job concurrency. This is a scenario, not an observed
bors utilization estimate. At a saturated fixed vCPU quota, doubling CPUs per
job can instead halve concurrency; the warm measurements would then imply about
31% fewer attempts per hour. Check actual quota headroom before rollout.

An actual keep-up assessment needs the current arrival rate and workload mix,
success/retry rate, batch sizes, allowed concurrent batches, provider vCPU quota
and full production service times. This experiment does not establish those
values. A successful production canary should assess queue age/backlog and
accepted PRs per hour alongside spend, without an invented cost veto.

The [permanent receipt](evidence/38665598590.json.gz) retains the plan, all 18 raw worker/resource records, final jobs/run/artifact API data, observed failed-gate summaries and separately labelled untrusted phase/error excerpts. `protocol_completed` and `accounting_complete` are true; `complete` remains false because there were no successful full validations. Failed runs are not relabelled as successes.

A separately authorized bors runner-default change is released through production PR #16875. Reverting that PR restores staging to 8CPU/16GB; its routing test records the previous label. The benchmark has no automatic configuration/publication path, and its consumed budget prevents further workload dispatches. New experiments need new explicit authorization. A later hardware promotion requires independent successful validation and the human-reviewed production change/rollback process described in README.md. Historical receipts retain the original recommendation fields as provenance; the unsupported cost-policy conclusion in them is superseded by this correction.
