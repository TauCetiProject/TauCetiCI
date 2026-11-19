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
const { progressFrame } = await import(
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
