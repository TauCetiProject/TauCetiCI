import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSync } from "esbuild";

const code = buildSync({
  stdin: {
    contents: `export { compactEvent } from "./policy";
      export { normalizeJob, WORKFLOW } from "./model";
      export { publicJob } from "./operations-jobs";`,
    resolveDir: new URL("../src/", import.meta.url).pathname,
  },
  bundle: true,
  platform: "node",
  format: "esm",
  write: false,
}).outputFiles[0].text;
const { compactEvent, normalizeJob, publicJob, WORKFLOW } = await import(
  "data:text/javascript;base64," + Buffer.from(code).toString("base64")
);

test("GitHub check-run identity survives ingress compaction and public job projection", () => {
  const repo = "TauCetiProject/TauCeti";
  const event = compactEvent({
    repo,
    delivery: "identity-regression",
    jobs: [{
      id: 123,
      run_id: 456,
      run_attempt: 2,
      name: "sandboxed-build",
      status: "in_progress",
      created_at: "2026-11-06T17:00:00Z",
      started_at: "2026-11-06T17:00:01Z",
      check_run_url: `https://api.github.com/repos/${repo}/check-runs/789`,
      check_run_id: "forged-body-identity",
      private_environment: "must not be archived",
    }],
  });
  assert.equal(event.jobs[0].check_run_id, undefined);
  assert.equal(event.jobs[0].private_environment, undefined);
  const job = normalizeJob(event.jobs[0]);
  const projected = publicJob({
    repo, id: "456", attempt: 2, workflow: WORKFLOW, trigger: "pr",
    status: "in_progress",
  }, job);
  assert.equal(projected.id, "123");
  assert.equal(projected.run_id, "456");
  assert.equal(projected.run_attempt, 2);
  assert.equal(projected.check_run_id, "789",
    "the signed reporter must join the GitHub check run, not the job or body ID");
});
