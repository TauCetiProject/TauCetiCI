import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSync } from "esbuild";
const code = buildSync({
  entryPoints: ["src/operations-model.ts"],
  bundle: true,
  platform: "node",
  format: "esm",
  write: false,
}).outputFiles[0].text;
const { progressFrame, lakeJobEstimates, withLakeEstimate } = await import(
  "data:text/javascript;base64," + Buffer.from(code).toString("base64")
);

const frame = {
  schema: "tauceti.operations.progress/v1",
  session: "f28ca028-01b2-4c5c-9567-5b51b0ae179a",
  sequence: 470,
  observed_at: "2026-11-19T04:21:06.524274Z",
  source_revision: "91942aa9ff8cea04d0d9905b80f75c07ce2379c4",
  active_compilers: 0,
  cpu_seconds: 459484.13,
  phase: "build",
  completed_modules: 25433,
  failed_modules: 0,
  last_module: "TauCeti.RingTheory.Perfectoid.UniformTilt.Basic",
  last_progress_at: "2026-11-19T04:21:06.554254Z",
};

test("reporters that predate Lake task counters are still accepted", () => {
  assert.deepEqual(progressFrame({ ...frame }), frame);
});

test("Lake task counters, their rate and the finer sandbox phases are accepted", () => {
  const value = {
    ...frame,
    completed_tasks: 113917,
    total_tasks: 113955,
    tasks_per_minute: 850.5,
  };
  assert.equal(progressFrame({ ...value }).total_tasks, 113955);
  for (const phase of ["cache-unpack", "audit-import", "audit-duplicates", "audit-axioms"])
    assert.equal(progressFrame({ ...value, phase }).phase, phase);
  assert.equal(
    progressFrame({ ...frame, completed_tasks: null, total_tasks: null, tasks_per_minute: null })
      .completed_tasks,
    null,
  );
});

test("invalid task counters and rates are rejected", () => {
  for (const patch of [
    { completed_tasks: 5, total_tasks: 4 },
    { completed_tasks: 5, total_tasks: null },
    { completed_tasks: 5 },
    { completed_tasks: -1, total_tasks: 4 },
    { completed_tasks: 1.5, total_tasks: 4 },
    { completed_tasks: 1, total_tasks: 10000001 },
    { completed_tasks: "1", total_tasks: 4 },
    { tasks_per_minute: -1 },
    { tasks_per_minute: Infinity },
    { tasks_per_minute: "850" },
    { tasks_per_minute: 1e8 },
    { phase: "audit-everything" },
    { log_tail: "[1/2] Built" },
  ])
    assert.throws(() => progressFrame({ ...frame, ...patch }));
});

const repo = "TauCetiProject/TauCeti";
const finished = (run_id, total, observed_at, extra = {}) => ({
  repository: repo,
  run_id,
  job_class: "pr_build",
  frame: { phase: "lint-env", completed_tasks: total, total_tasks: total, observed_at, ...extra },
});
const running = (run_id, progress, extra = {}) => ({
  repository: repo,
  run_id,
  active: true,
  job_class: "staging_build",
  progress: { coverage: "untrusted_counters_trusted_reporter", phase: "build", ...progress },
  ...extra,
});

test("an active build's estimate is its repository's latest finished build's final job count", () => {
  const estimates = lakeJobEstimates(
    [
      finished("1", 83094, "2026-11-19T07:57:00Z"),
      { ...finished("2", 81818, "2026-11-19T05:00:00Z"), job_class: null },
      // Not finished: still building, or a later phase that never reached Lake's total.
      finished("4", 99999, "2026-11-19T08:00:00Z", { phase: "build" }),
      finished("5", 99999, "2026-11-19T08:00:00Z", { completed_tasks: 7 }),
      { ...finished("6", 1, "2026-11-19T09:00:00Z"), frame: null },
      { ...finished("7", 5, "2026-11-19T09:00:00Z"), repository: "Private/Hidden" },
    ],
    undefined,
    [repo],
  );
  assert.deepEqual(estimates, {
    [repo]: { total_tasks: 83094, run_id: "1", job_class: "pr_build", observed_at: "2026-11-19T07:57:00Z" },
  });
  const staging = withLakeEstimate(running("8", { completed_tasks: 13529, total_tasks: 15021 }), estimates);
  assert.equal(staging.progress.estimated_total_tasks, 83094);
  assert.equal(staging.progress.estimate_source.run_id, "1");
});

test("no estimate for builds that outgrew it, inactive or finished jobs, or the estimate's own run", () => {
  const estimates = lakeJobEstimates([finished("1", 83094, "2026-11-19T07:57:00Z")], undefined, [repo]);
  for (const job of [
    running("9", { completed_tasks: 85000, total_tasks: 86000 }),
    running("9", { completed_tasks: 83094, total_tasks: 83094 }),
    running("9", { completed_tasks: 100, total_tasks: 200 }, { active: false }),
    running("9", { phase: "lint-env", completed_tasks: 80000, total_tasks: 80000 }),
    running("1", { completed_tasks: 100, total_tasks: 200 }),
    running("9", { completed_tasks: 100, total_tasks: 200 }, { repository: "Other/Repo" }),
    { ...running("9", {}), progress: { coverage: "steps_only" } },
  ])
    assert.equal(withLakeEstimate(job, estimates), job);
});

test("the estimate outlives the reports and leaves with its repository's public scope", () => {
  const previous = lakeJobEstimates([finished("2", 81818, "2026-11-19T05:00:00Z")], undefined, [repo]);
  assert.equal(lakeJobEstimates([], previous, [repo])[repo].total_tasks, 81818);
  assert.equal(
    lakeJobEstimates([finished("1", 70000, "2026-11-18T05:00:00Z")], previous, [repo])[repo].total_tasks,
    81818,
  );
  assert.deepEqual(lakeJobEstimates([], previous, ["Other/Repo"]), {});
});
