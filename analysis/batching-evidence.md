# Advisory batching evidence

Issue: [TauCeti#16244](https://github.com/TauCetiProject/TauCeti/issues/16244).
Source records: TauCetiCI `1cb4d18e438fac159b8424bb91883f383363a7b1` plus the committed
854-snapshot archive through Oct 14 22:42 UTC. Its receipt is
`records/replay-captures/2026/10/14/20261014T224333Z.json`: 14 read requests,
no collection cursor movement.
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
history. The captured window ends at 2026-10-14 22:42:38 UTC.
No new 24-hour baseline was required.

The window has 1,428 snapshots, 891 exact head versions, 240 initially present
heads with unknown prior age, and four gaps longer than two minutes. Excluding
initial backlog, first-observed eligibility is 27.14 heads/hour. This differs from
actual admissions and accepted PRs. There are 364 missing feature snapshots, 364
imputed build-work estimates, 364 unknown dependency/staleness values and zero
known bundle IDs. Synthetic bundle stress is explicit, not historical inference.

Main-push evidence within the window records 126 distinct accepted PRs (a partial recorded
rate of 5.25/hour). Attempt-start accounting records 24 bors runs and 382.53 runner
minutes across their jobs; all 24 have jobs, but collection completeness is not
established. The general collection cursor is only Oct 12 11:02 UTC; recent
resource sampling does not advance it. These incomplete counts do not establish
a current throughput decline. Attempt and acceptance windows differ, so dividing these quantities
would not establish a causal cost per PR. These observed PRs are different from
simulated head versions.

The build model uses 22 completed successful ordinary staging attempts before
the window, all Namespace 8x16: fixed 575.777151696 seconds plus 0.790400342 seconds
per estimated work unit. Median work per member is 45.514957265. Unknown/dependency
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
| low-failure | fifo | 128 | 13.45 | 4.46 | 2116.1 |
| low-failure | fifo | 8 | 21.75 | 2.76 | 848.2 |
| low-failure | work-risk | 8 | 21.73 | 2.76 | 852.6 |
| low-failure | fifo | 16 | 23.36 | 2.57 | 804.5 |
| low-failure | work-risk | 16 | 23.46 | 2.56 | 779.9 |
| low-failure | fifo | 32 | 21.60 | 2.78 | 956.7 |
| low-failure | work-risk | 32 | 22.88 | 2.62 | 851.0 |
| high-failure | fifo | 128 | 2.63 | 22.83 | 11182.2 |
| high-failure | fifo | 8 | 3.60 | 16.65 | 7859.0 |
| high-failure | work-risk | 8 | 3.62 | 16.56 | 7705.8 |
| high-failure | fifo | 16 | 3.25 | 18.44 | 8827.1 |
| high-failure | work-risk | 16 | 3.23 | 18.58 | 8929.5 |
| high-failure | fifo | 32 | 3.03 | 19.83 | 9590.9 |
| high-failure | work-risk | 32 | 3.01 | 19.96 | 9667.6 |

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

Focused input SHA-256: `6c02ad2ea45d18b1c68c338a388d9784f8cd52f4deadec7b979f1f37413f2c64`.
Focused full JSON SHA-256 including traces:
`7a102c2a4e9476a47d0ff08f7e3ce59d8a296e7c98151afc1ee77399b2cd52d3`.
The Python 3.11 and 3.14 runs produced byte-identical JSON. The focused database
and complete report build passed. Local validation passes 58 Python 3.11 tests
plus compileall and diff checks. Tests cover deterministic replay, cohort/time
filtering, unknown telemetry, atomic bundles, dependency/stale sensitivity,
frozen retries, infrastructure exhaustion, stale proof/infra retries and oldest-unit fairness.

Full-history and production publication verification are recorded on the issue;
this document reports the focused captured-workload experiment, not a canary.
