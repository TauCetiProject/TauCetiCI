# Advisory batching evidence

Issue: [TauCeti#16244](https://github.com/TauCetiProject/TauCeti/issues/16244).
Source records: TauCetiCI `1cb4d18e438fac159b8424bb91883f383363a7b1`.
Production [bors configuration](https://github.com/TauCetiProject/TauCeti/blob/4f1a681d10b8371c56bc49158b0481d18bd949f5/bors.toml)
sets max_batch_size=128. The advisory module does not change it.

Retain the current production policy. The work/risk heuristic does not consistently
improve on FIFO within the same cap, and the apparent best cap changes with injected
failure and work assumptions. This is evidence for further matched canaries,
not an optimal cap or measured production improvement. Adaptive scheduling remains
disabled; there is no live integration or automatic enablement.

## Captured workload and limits

The immediate validation database uses TauCeti runs from Oct 10 onward, all
captured eligibility observations and main history. Older per-head features are
therefore missing in this focused validation; the normal publisher uses full
history. The captured window ends at 2026-10-14 08:16:49 UTC, not the current clock.
No new 24-hour baseline was required.

The window has 1,437 snapshots, 909 exact head versions, 325 initially present
heads with unknown prior age, and seven gaps longer than two minutes. Excluding
initial backlog, first-observed eligibility is 24.34 heads/hour. This differs from
actual admissions and accepted PRs. There are 70 missing feature snapshots, 71
imputed build-work estimates, 70 unknown dependency/staleness values and zero
known bundle IDs. Synthetic bundle stress is explicit, not historical inference.

Main-push evidence within the window records 440 distinct accepted PRs, about
18.33/hour. Attempt-start accounting records 49 bors runs and 1,004.7 runner
minutes across their jobs; all 49 have jobs, but collection completeness is not
established. Attempt and acceptance windows differ, so dividing these quantities
would not establish a causal cost per PR. These observed PRs are different from
simulated head versions.

The build model uses nine completed successful ordinary staging attempts before
the window, all Namespace 8x16: fixed 563.300307403 seconds plus 0.661996149 seconds
per estimated work unit. Median work per member is 21.322580645. Unknown/dependency
cohorts, failed/cancelled jobs and future observations do not train successful
duration. Module counters remain untrusted statistical hints. The small sample,
unknown cache warmth and absent transitive rebuild graph limit calibration.

## Conditional results

One serial validation lane, no fill delay, upper eligibility bound, initial
backlog included. Proof rates are injected at 1% or 20%, with infrastructure
rates 1% or 5%. Other scenarios default to 2% each. Head failures use common
SHA-256 draws across policies. Unknown heads use explicit imputation. Retry
membership is frozen and bisection preserves atomic units. Staleness and changed
paths are proxies, not measured transitive build work. FIFO128 is a count-only
reference, not an exact bors implementation or measured production baseline.
The workload drains after its captured window, explaining long stress latencies.

| Failure scenario | Formation | Cap | Simulated heads/hour | Runner min/head landed | p90 latency min |
| --- | --- | --- | --- | --- | --- |
| low-failure | fifo | 128 | 16.45 | 3.65 | 1617.9 |
| low-failure | fifo | 8 | 23.30 | 2.58 | 769.8 |
| low-failure | work-risk | 8 | 22.90 | 2.62 | 799.2 |
| low-failure | fifo | 16 | 25.47 | 2.36 | 634.9 |
| low-failure | work-risk | 16 | 25.29 | 2.37 | 652.0 |
| low-failure | fifo | 32 | 25.41 | 2.36 | 695.3 |
| low-failure | work-risk | 32 | 24.28 | 2.47 | 765.7 |
| high-failure | fifo | 128 | 2.77 | 21.66 | 11266.6 |
| high-failure | fifo | 8 | 3.68 | 16.28 | 8040.1 |
| high-failure | work-risk | 8 | 3.71 | 16.16 | 7948.9 |
| high-failure | fifo | 16 | 3.37 | 17.83 | 8980.5 |
| high-failure | work-risk | 16 | 3.41 | 17.57 | 8913.9 |
| high-failure | fifo | 32 | 3.20 | 18.76 | 9619.9 |
| high-failure | work-risk | 32 | 3.15 | 19.02 | 9631.9 |

The complete output includes both eligibility bounds, low/high failures,
infrastructure outage, zero/full changed-path overlap, half/double work,
dependency-bump, stale-base, atomic-pair and missing-telemetry stress cohorts.
Costs distinguish total, isolation, infrastructure and stale work; categories
can overlap. Outage exhaustion is deferred infrastructure, never proof rejection.
Every batch anchors its oldest eligible unit; oversized bundles run alone, and
overdue units bypass the work budget. This prevents indefinite bypass in finite
backlogs; it cannot promise elapsed starvation bounds during overload or outages.

## Reproduction and validation

Build the full database with `python3 -m collector.build_db`, then run
`python3 -m analysis.replay --db db/ci.sqlite --out replay.json --trace`.
The normal `analysis.report` publication includes the complete sensitivity table
and coverage, so this advisory reaches the existing production report without
changing trusted build workflows, merge gates or cache publishers.

Focused input SHA-256: `cacc82922652d3ecc3f713d678e72184cc48d613c4fed6549cca34c9a06b02c7`.
Focused full JSON SHA-256 (including traces):
`a94f13345c9b0f733d6bd2edbfd14850a769769364fb8e78837af16681f3552c`.
The Python 3.11 and 3.14 runs produced byte-identical JSON. The focused database
and complete report build passed. Local validation passes 56 Python 3.11 tests
plus compileall and diff checks. Tests cover deterministic replay, cohort/time
filtering, unknown telemetry, atomic bundles, dependency/stale sensitivity,
frozen retries, infrastructure exhaustion and oldest-unit fairness.

Full-history and production publication verification are recorded on the issue;
this document reports the focused captured-workload experiment, not a canary.
