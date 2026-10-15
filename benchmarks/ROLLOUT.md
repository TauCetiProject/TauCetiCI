# Bors staging runner rollout

The user chose 16CPU/32GB for bors staging to improve latency and queue capacity.
The 10% cost ceiling formerly used by the benchmark reporter was not authorized;
[PR #18](https://github.com/TauCetiProject/TauCetiCI/pull/18) removed it.
The original bounded observations and failed-lint limits remain in [RESULTS.md](RESULTS.md).
This rollout is an operational validation, not a new controlled speed comparison.

## Release

[Production PR #16875](https://github.com/TauCetiProject/TauCeti/pull/16875)
landed in main at
[`346965ee5b080ab881c80e16054c36f5ac19af97`](https://github.com/TauCetiProject/TauCeti/commit/346965ee5b080ab881c80e16054c36f5ac19af97).
Only bors-staging dispatches select 16CPU/32GB. Staging priority and per-run
assignment remain in place; other event routing retains its existing policy.
No build/audit/lint, cache-producer trust, credential or publisher boundary changed.

The user explicitly authorized the normal
[`bors merge` command](https://github.com/TauCetiProject/TauCeti/pull/16875#issuecomment-6187415555).
Bors recorded exact-head approval, all ten
[AI rubrics approved](https://github.com/TauCetiProject/TauCeti/pull/16875#issuecomment-6187629280),
and fresh PR-level build/bump/scope checks passed in
[run 38708357233](https://github.com/TauCetiProject/TauCeti/actions/runs/38708357233).
The same approved head was isolated into singleton batch 793 and temporarily
prioritized, without cancelling the running predecessor. The
[landing run 38710469186](https://github.com/TauCetiProject/TauCeti/actions/runs/38710469186)
passed sandboxed build/audits, isolated publication and finalization. Its trusted
workflow predates the release and used 8CPU; it is not 16CPU hardware evidence.

## Post-release hardware verification

The first new production dispatch is
[run 38712306645](https://github.com/TauCetiProject/TauCeti/actions/runs/38712306645),
batch 794, testing `1536436942648fbd37b1cb87d5765da9d9710ed5` over the landed
`346965ee5b080ab881c80e16054c36f5ac19af97` workflow. Its allocated compiler runner
is `nsc-runner-tgcp4oj7fegvm` with the 16x32 priority-one label and its exact run-ID
feature assignment.

This sample upgrades Lean from `v4.35.0-rc3` to `v4.35.0` and Mathlib from
`3b531c35df33d6bbe64b4e87dec056211d3a5676` to
`074063781836922cb02701cd12943c9f51731afa`. It is a full toolchain rebuild;
these pins differ from the controlled benchmark. No speedup estimate is inferred
from comparing this run with the earlier benchmark.

The compiler job was cancelled when the bump PR was pushed to a new head:
[Bors cancellation](https://github.com/TauCetiProject/TauCeti/pull/16333#issuecomment-6189089920).
Its overall run failed terminal finalization and skipped publication. No host
resource artifact was retained; the actual CPU-count field remains unavailable.
The retained provider startup log also reports `Machine Type: 16x32` before
candidate checkout. This confirms actual machine type; the independent host
CPU-count sample remains unavailable. The run is retained as cancelled, with
missing telemetry explicitly accounted.

A second new dispatch,
[run 38714981429](https://github.com/TauCetiProject/TauCeti/actions/runs/38714981429),
batch 768, has also been allocated the 16x32 staging profile on
`nsc-runner-pdo3puc1svep8`, with exact run-ID assignment and staging priority.
The host telemetry confirms **16 CPUs**. The sandbox command succeeded in
927.04 seconds and consumed 11,690.84 CPU-seconds (12.61 cores on average); host
CPU busy fraction was 79.5%. Build/audits, isolated publication with public revision
map confirmation, and terminal finalization all passed. This is a live 25-PR
batch, not a controlled comparison against an 8CPU run.

The bounded [production receipt](evidence/16245-production-rollout.json.gz)
retains run/job API responses, workflow source, host telemetry, allocation data
and cancellation accounting. The bump cancellation above came from the existing
interactive session repairing three locally reproduced integration errors, not
from an observed normal updater/fleet policy. The replacement head merges the
exact staging base and changes two Lean files; no general cancellation-rate
inference follows from this event.

## Rollback and budget

Revert PR #16875 to restore staging's 8x16 shape expression and routing-test
expectation, then verify a new trusted staging dispatch. No keys, shared cache
producer rules or publisher isolation need changing.

The 18 original setup-failed slots and 18 explicitly approved replacement slots
remain the complete benchmark budget. No benchmark was dispatched for this
rollout. The live staging jobs above are the existing production queue, not
additional controlled trials.
