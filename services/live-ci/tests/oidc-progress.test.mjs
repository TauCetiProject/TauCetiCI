import { test } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync, createSign } from "node:crypto";
import { buildSync } from "esbuild";
const code = buildSync({
  entryPoints: ["src/oidc.ts"],
  bundle: true,
  platform: "node",
  format: "esm",
  write: false,
}).outputFiles[0].text;
const { verifyProgress } = await import(
  "data:text/javascript;base64," + Buffer.from(code).toString("base64")
);
const repo = "TauCetiProject/TauCeti",
  clock = Math.floor(Date.now() / 1000);
const { privateKey, publicKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
});
const jwk = { ...publicKey.export({ format: "jwk" }), kid: "test" };
const base = {
  iss: "https://token.actions.githubusercontent.com",
  aud: "tauceti-operations-progress",
  exp: clock + 300,
  iat: clock,
  nbf: clock - 5,
  repository: repo,
  repository_visibility: "public",
  workflow_ref: repo + "/.github/workflows/pr-build.yml@refs/heads/main",
  event_name: "pull_request_target",
  run_id: "100",
  run_attempt: "2",
  check_run_id: "555",
  workflow_sha: "a".repeat(40),
};
function token(claims) {
  const data =
    Buffer.from(JSON.stringify({ alg: "RS256", kid: "test" })).toString(
      "base64url",
    ) +
    "." +
    Buffer.from(JSON.stringify(claims)).toString("base64url");
  return (
    data +
    "." +
    createSign("RSA-SHA256").update(data).sign(privateKey).toString("base64url")
  );
}
const env = {
  OPERATIONS_PUBLIC_REPOSITORIES: JSON.stringify([repo]),
  ARCHIVE: {
    get: async () => ({ json: async () => ({ fetched: clock, keys: [jwk] }) }),
  },
};
test("progress identity is signed repository/run/attempt/check-run, with no caller job-id grant", async () => {
  const claims = await verifyProgress(token(base), env);
  assert.equal(claims.check_run_id, "555");
  assert.equal(claims.run_attempt, "2");
  for (const patch of [
    { aud: "tauceti-live-ci" },
    {
      workflow_ref:
        repo + "/.github/workflows/pr-build.yml@refs/heads/untrusted",
    },
    { repository: "Private/Hidden" },
    { repository_visibility: "private" },
    { check_run_id: undefined },
    { run_attempt: "0" },
    { exp: clock - 1 },
    { iat: "not-a-number" },
    { event_name: "pull_request" },
    { iss: "https://evil.example" },
  ])
    await assert.rejects(verifyProgress(token({ ...base, ...patch }), env));
});

test("native merge-group progress is restricted to its signed main-queue ref", async () => {
  const ref = "refs/heads/gh-readonly-queue/main/pr-10-abcdef";
  const claims = {
    ...base,
    event_name: "merge_group",
    ref,
    workflow_ref: repo + "/.github/workflows/pr-build.yml@" + ref,
  };
  assert.equal((await verifyProgress(token(claims), env)).ref, ref);
  await assert.rejects(
    verifyProgress(token({ ...claims, event_name: "pull_request" }), env),
  );
  const other = "refs/heads/gh-readonly-queue/private/pr-10";
  await assert.rejects(
    verifyProgress(
      token({
        ...claims,
        ref: other,
        workflow_ref: repo + "/.github/workflows/pr-build.yml@" + other,
      }),
      env,
    ),
  );
});
