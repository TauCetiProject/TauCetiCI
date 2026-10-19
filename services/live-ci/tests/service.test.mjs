import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHmac, generateKeyPairSync } from "node:crypto";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { build } from "esbuild";
const repo = "TauCetiProject/TauCeti";
const clock = Math.floor(Date.now() / 1000),
  date = new Date((clock - 300) * 1000).toISOString().slice(0, 10);
const iso = (t) => new Date(t * 1000).toISOString();
const run = (id = "100", attempt = 1) => ({
  id,
  run_attempt: attempt,
  path: ".github/workflows/pr-build.yml",
  event: "pull_request_target",
  status: "completed",
  conclusion: "success",
  created_at: iso(clock - 300),
  updated_at: iso(clock),
  head_sha: "a".repeat(40),
  head_branch: "main",
});
const job = (id = "200", run_id = "100") => ({
  id,
  run_id,
  run_attempt: 1,
  name: "sandboxed-build",
  status: "completed",
  conclusion: "success",
  created_at: iso(clock - 300),
  started_at: iso(clock - 240),
  completed_at: iso(clock - 60),
  labels: ["ubuntu-latest"],
  runner_id: 42,
  steps: [
    {
      name: "Build exact candidate",
      started_at: iso(clock - 240),
      completed_at: iso(clock - 60),
      conclusion: "success",
    },
  ],
});
async function runtime(t, bindings = {}, upstream) {
  let github = 0;
  const options = convertV4MiniflareOptions({
    modules: true,
    script: readFileSync("dist/index.js", "utf8"),
    compatibilityDate: "2026-10-15",
    durableObjects: {
      DIRECTORIES: { className: "RunDirectory", useSQLite: true },
      FACTS: { className: "FactPartition", useSQLite: true },
      DAYS: { className: "DayAggregate", useSQLite: true },
      CONTROL: { className: "Control", useSQLite: true },
    },
    r2Buckets: ["ARCHIVE"],
    queueProducers: { EVENTS: "events" },
    bindings: {
      WEBHOOK_SECRET: "webhook",
      ADMIN_TOKEN: "admin",
      INGEST_TOKEN: "ingest",
      ALLOWED_REPOSITORIES: JSON.stringify([repo]),
      GITHUB_HOURLY_BUDGET: "500",
      PUBLIC_ORIGIN: "https://taucetiproject.org",
      PUBLICATION_MODE: "shadow",
      ...bindings,
    },
    outboundService: async (req) => {
      if (new URL(req.url).hostname === "api.github.com") {
        github++;
        if (upstream) return upstream(req);
      }
      return Response.json({ observations: [], truncated: false });
    },
  });
  const mf = new Miniflare(options);
  t.after(() => mf.dispose());
  let ns = await mf.getDurableObjectNamespace("CONTROL");
  const post = async (path, b = {}) => {
    const r = await ns
      .get(ns.idFromName("control"))
      .fetch("https://internal" + path, {
        method: "POST",
        body: JSON.stringify(b),
      });
    assert.equal(r.status, 200, await r.clone().text());
    return r.json();
  };
  const ingest = async (b) =>
    post("/event", { repo, delivery: crypto.randomUUID(), ...b });
  const summary = async () => {
    const ns = await mf.getDurableObjectNamespace("DAYS");
    const r = await ns
      .get(ns.idFromName(date))
      .fetch("https://internal/summary", { method: "POST", body: "{}" });
    assert.equal(r.status, 200, await r.clone().text());
    return r.json();
  };
  const updateBindings = async (values) => {
    for (const [key, value] of Object.entries(values))
      options.workers[0].config.env[key] = { type: "text", value };
    await mf.setOptions(options);
    ns = await mf.getDurableObjectNamespace("CONTROL");
  };
  const notify = async () => {
    for (const [binding, name] of [
      ["FACTS", repo + "/" + date],
      ["DAYS", date],
    ]) {
      const ns = await mf.getDurableObjectNamespace(binding);
      const response = await ns
        .get(ns.idFromName(name))
        .fetch("https://internal/tick", {
          method: "POST",
          body: "{}",
        });
      assert.equal(response.status, 200, await response.clone().text());
    }
  };
  return {
    mf,
    post,
    ingest,
    summary,
    notify,
    updateBindings,
    github: () => github,
  };
}
test("duplicate delivery, late queued job and telemetry corrections preserve totals", async (t) => {
  const r = await runtime(t),
    j = job();
  const event = { repo, delivery: "same", run: run(), jobs: [j] };
  await r.post("/event", event);
  await r.post("/event", event);
  let s = await r.summary();
  assert.equal(s.daily["pr-total"], 1);
  assert.equal(s.daily["duration/github"].median, 3);
  await r.ingest({
    jobs: [
      {
        ...j,
        status: "queued",
        conclusion: null,
        started_at: null,
        completed_at: null,
        steps: [],
      },
    ],
  });
  s = await r.summary();
  assert.equal(s.daily["pr-total"], 1);
  assert.equal(s.daily["duration/github"].median, 3);
  await r.ingest({
    run_id: "100",
    attempt: 1,
    telemetry: {
      phases: [
        { phase: "build", seconds: 120 },
        { phase: "audit-axioms", seconds: 60 },
      ],
    },
  });
  s = await r.summary();
  assert.equal(s.daily["phase/build"], 2);
  assert.equal(s.daily["phase/audits"], 1);
  assert.equal(s.daily["phase/build, audits, lints"] || 0, 0);
});
test("reruns retain every build attempt and exact nearest-rank percentiles", async (t) => {
  const r = await runtime(t);
  await r.ingest({ run: run(), jobs: [job()] });
  const next = run("100", 2);
  await r.ingest({
    run: next,
    jobs: [{ ...job("201"), run_attempt: 2, started_at: iso(clock - 180) }],
  });
  const s = await r.summary();
  assert.equal(s.daily["pr-total"], 2);
  assert.deepEqual(s.daily["duration/github"], { median: 2, p90: 3 });
});
test("outbox retry cannot apply the same delta twice", async (t) => {
  const r = await runtime(t),
    ns = await r.mf.getDurableObjectNamespace("DAYS"),
    stub = ns.get(ns.idFromName(date));
  const body = JSON.stringify({
    id: "delta",
    day: date,
    rows: [{ bucket: 0, metric: "pr-total", value: 0, amount: 1 }],
  });
  for (let i = 0; i < 2; i++)
    assert.equal(
      (await stub.fetch("https://internal/delta", { method: "POST", body }))
        .status,
      200,
    );
  assert.equal((await r.summary()).daily["pr-total"], 1);
});
test("manifest and immutable charts support conditional public reads without GitHub", async (t) => {
  const r = await runtime(t);
  await r.ingest({ run: run(), jobs: [job()] });
  await r.notify();
  await r.post("/tick");
  const response = await r.mf.dispatchFetch(
    "https://example.com/v1/ci/manifest.json",
  );
  assert.equal(response.status, 200);
  const manifest = await response.json();
  assert.equal(manifest.mode, "shadow");
  assert.equal(manifest.groups.daily.complete, false);
  assert.ok(manifest.assets["ci-build-duration.svg"]);
  assert.equal(
    (
      await r.mf.dispatchFetch("https://example.com/v1/ci/manifest.json", {
        headers: { "if-none-match": response.headers.get("etag") },
      })
    ).status,
    304,
  );
  const asset = await r.mf.dispatchFetch(
    "https://example.com" + manifest.assets["ci-build-duration.svg"].url,
  );
  assert.equal(asset.status, 200);
  assert.match(await asset.text(), /<svg/);
  for (let i = 0; i < 20; i++)
    await r.mf.dispatchFetch("https://example.com/v1/ci/manifest.json");
  assert.equal(r.github(), 0);
});
test("alarm notifications publish active state and late completion corrections", async (t) => {
  const r = await runtime(t);
  await r.ingest({
    run: { ...run(), status: "in_progress", conclusion: null },
    jobs: [
      { ...job(), status: "in_progress", conclusion: null, completed_at: null },
    ],
  });
  await r.notify();
  await r.post("/tick");
  const bucket = await r.mf.getR2Bucket("ARCHIVE");
  let manifest = await (await bucket.get("public/v1/ci/manifest.json")).json();
  assert.equal(manifest.current.running.github, 1);
  assert.equal((await r.post("/health")).active_partitions, 1);
  await r.ingest({ run: run(), jobs: [job()] });
  await r.notify();
  await r.post("/tick");
  manifest = await (await bucket.get("public/v1/ci/manifest.json")).json();
  assert.deepEqual(manifest.current.running, {});
  assert.equal((await r.post("/health")).active_partitions, 0);
  assert.equal((await r.summary()).daily["pr-total"], 1);
  assert.equal((await r.summary()).daily["duration/github"].median, 3);
});
test("terminal parents retire unobserved children without inventing job results", async (t) => {
  const r = await runtime(t);
  await r.ingest({
    run: {
      ...run(),
      status: "in_progress",
      conclusion: null,
      updated_at: iso(clock - 180),
    },
    jobs: [
      {
        ...job(),
        status: "in_progress",
        conclusion: null,
        completed_at: null,
        steps: [],
      },
    ],
  });
  await r.notify();
  const facts = await r.mf.getDurableObjectNamespace("FACTS"),
    partition = facts.get(facts.idFromName(repo + "/" + date));
  const current = async () =>
    (
      await partition.fetch("https://internal/current", {
        method: "POST",
        body: "{}",
      })
    ).json();
  assert.equal((await current()).counts[0].count, 1);
  await r.ingest({ run: { ...run(), updated_at: iso(clock - 120) } });
  assert.deepEqual((await current()).counts, []);
  const bounded = await r.summary();
  assert.equal(bounded.daily["duration/github"], undefined);
  // An unchanged payload still stays inactive on subsequent alarm refreshes.
  await r.notify();
  assert.deepEqual((await current()).counts, []);
  await r.ingest({ jobs: [job()] });
  const settled = await r.summary();
  assert.equal(settled.daily["duration/github"].median, 3);
  assert.equal(settled.daily["pr-total"], 1);
  assert.deepEqual((await current()).counts, []);
  assert.equal(r.github(), 0);
});
test("trusted metadata and summaries cancel satisfied repairs for the same attempt", async (t) => {
  const r = await runtime(t);
  for (const [kind, attempt] of [
    ["run", 1],
    ["jobs", 1],
    ["details", 1],
    ["run", 2],
  ])
    await r.post("/repair", { repo, kind, args: { id: "100", attempt } });
  await r.ingest({
    run: run(),
    jobs: [job()],
    verified_jobs: true,
    telemetry: { phases: [{ phase: "build", seconds: 180 }] },
  });
  assert.deepEqual(
    (await r.post("/health")).pending.map(({ kind, count }) => ({
      kind,
      count,
    })),
    [{ kind: "run", count: 1 }],
  );
  assert.equal(r.github(), 0);
});
test("repair recovery prunes settled work without reading GitHub or dropping unresolved attempts", async (t) => {
  const r = await runtime(t);
  await r.ingest({
    run: run(),
    jobs: [job()],
    verified_jobs: true,
    telemetry: { phases: [{ phase: "build", seconds: 180 }] },
  });
  // Simulate the pre-recovery backlog, created before summaries arrived.
  for (const [kind, attempt] of [
    ["run", 1],
    ["run", 2],
    ["jobs", 1],
    ["jobs", 2],
    ["details", 1],
  ])
    await r.post("/repair", { repo, kind, args: { id: "100", attempt } });
  const result = await r.post("/prune-repairs");
  assert.deepEqual(result, { scanned: 5, removed: 3, cursor: null });
  const remaining = await r.post("/repairs");
  assert.equal(remaining.length, 2);
  assert.ok(remaining.every((row) => row.args.attempt === 2));
  assert.equal(r.github(), 0);
});
test("publication refreshes observed activity while an inventory request is blocked", async (t) => {
  const key = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  }).privateKey.export({ format: "pem", type: "pkcs8" });
  let entered, release;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const blocked = new Promise((resolve) => {
    release = resolve;
  });
  const r = await runtime(
    t,
    {
      GITHUB_APP_ID: "1",
      GITHUB_INSTALLATION_ID: "2",
      GITHUB_PRIVATE_KEY: String(key),
    },
    async (req) => {
      if (new URL(req.url).pathname.endsWith("/access_tokens"))
        return Response.json({ token: "test", expires_at: iso(clock + 3600) });
      entered();
      await blocked;
      return Response.json({ total_count: 0, workflow_runs: [] });
    },
  );
  await r.ingest({
    run: { ...run(), status: "in_progress", conclusion: null },
    jobs: [
      { ...job(), status: "in_progress", conclusion: null, completed_at: null },
    ],
  });
  await r.notify();
  const repairing = r.post("/tick");
  try {
    await started;
    const reads = r.github();
    await r.post("/publish");
    const bucket = await r.mf.getR2Bucket("ARCHIVE");
    const manifest = await (
      await bucket.get("public/v1/ci/manifest.json")
    ).json();
    assert.equal(manifest.current.running.github, 1);
    assert.equal(r.github(), reads);
  } finally {
    release();
    await repairing;
  }
});
test("webhooks verify HMAC and persist the delivery before queueing", async (t) => {
  const r = await runtime(t),
    body = JSON.stringify({
      repository: { full_name: repo },
      workflow_run: run(),
    });
  const headers = {
    "x-github-event": "workflow_run",
    "x-github-delivery": "delivery-1",
    "x-hub-signature-256":
      "sha256=" + createHmac("sha256", "webhook").update(body).digest("hex"),
  };
  assert.equal(
    (
      await r.mf.dispatchFetch("https://example.com/webhooks/github", {
        method: "POST",
        headers: { ...headers, "x-hub-signature-256": "bad" },
        body,
      })
    ).status,
    401,
  );
  assert.equal(
    (
      await r.mf.dispatchFetch("https://example.com/webhooks/github", {
        method: "POST",
        headers,
        body,
      })
    ).status,
    202,
  );
  assert.equal(
    (await (await r.mf.getR2Bucket("ARCHIVE")).list({ prefix: "inbox/" }))
      .objects.length,
    1,
  );
});
test("queued cancellations with synthetic started_at do not accrue runner minutes", async (t) => {
  const r = await runtime(t);
  await r.ingest({
    run: { ...run(), conclusion: "cancelled" },
    jobs: [{ ...job(), conclusion: "cancelled", runner_id: null, steps: [] }],
  });
  const s = await r.summary();
  assert.equal(s.daily["minutes/github"] || 0, 0);
  assert.equal(s.daily["duration/github"], undefined);
});
test("publisher read leases share the rolling budget and cannot be multiplied by retry", async (t) => {
  const r = await runtime(t);
  const first = await r.post("/lease", {
    publisher: "1/1",
    run_id: "100",
    attempt: 1,
  });
  assert.equal(first.granted, true);
  await r.post("/lease", { publisher: "1/1", run_id: "100", attempt: 1 });
  let health = await r.post("/health");
  assert.equal(
    health.requests.reduce((n, x) => n + x.count, 0),
    6,
  );
  for (let i = 2; i <= 83; i++)
    assert.equal(
      (
        await r.post("/lease", {
          publisher: `${i}/1`,
          run_id: "100",
          attempt: 1,
        })
      ).granted,
      true,
    );
  assert.equal(
    (await r.post("/lease", { publisher: "84/1", run_id: "100", attempt: 1 }))
      .granted,
    false,
  );
});
test("cold fact archive restores prior contributions before a historical correction", async (t) => {
  const r = await runtime(t),
    then = clock - 60 * 86400,
    oldDate = iso(then).slice(0, 10),
    partition = repo + "/" + oldDate;
  const raw = {
    ...run(),
    path: ".github/workflows/label.yml",
    created_at: iso(then),
    updated_at: iso(then + 300),
  };
  const j = {
    ...job(),
    name: "label",
    created_at: iso(then),
    started_at: iso(then + 60),
    completed_at: iso(then + 120),
  };
  await r.ingest({ run: raw, jobs: [j], historical: true });
  const ns = await r.mf.getDurableObjectNamespace("FACTS"),
    fact = ns.get(ns.idFromName(partition));
  let response = await fact.fetch("https://internal/archive", {
    method: "POST",
    body: JSON.stringify({ builds: false }),
  });
  assert.equal(response.status, 200, await response.clone().text());
  response = await fact.fetch("https://internal/status", {
    method: "POST",
    body: JSON.stringify({ id: "100", attempt: 1 }),
  });
  assert.equal((await response.json()).jobs, 0);
  const bucket = await r.mf.getR2Bucket("ARCHIVE");
  assert.equal((await bucket.list({ prefix: "facts/" })).objects.length, 1);
  await r.ingest({
    jobs: [
      { ...j, started_at: iso(then + 120), completed_at: iso(then + 180) },
    ],
  });
  const days = await r.mf.getDurableObjectNamespace("DAYS");
  response = await days
    .get(days.idFromName(oldDate))
    .fetch("https://internal/summary", { method: "POST", body: "{}" });
  assert.deepEqual((await response.json()).daily["wait/other"], {
    median: 2,
    p90: 2,
  });
});
test("secondary rate limits pause all repair reads and preserve pending work", async (t) => {
  const key = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  }).privateKey.export({ format: "pem", type: "pkcs8" });
  const r = await runtime(
    t,
    {
      GITHUB_APP_ID: "1",
      GITHUB_INSTALLATION_ID: "2",
      GITHUB_PRIVATE_KEY: String(key),
    },
    (req) =>
      new URL(req.url).pathname.endsWith("/access_tokens")
        ? Response.json({ token: "test", expires_at: iso(clock + 3600) })
        : Response.json(
            { message: "secondary rate limit" },
            { status: 429, headers: { "retry-after": "300" } },
          ),
  );
  await r.post("/tick");
  const calls = r.github();
  assert.equal(calls, 2);
  await r.post("/tick");
  assert.equal(r.github(), calls);
  const health = await r.post("/health");
  assert.ok(health.pending.length);
  assert.ok(health.requests.some((x) => x.status === 429));
});
test("connecting the App updates bindings without losing observed facts", async (t) => {
  const r = await runtime(t, {}, (req) =>
    new URL(req.url).pathname.endsWith("/access_tokens")
      ? Response.json({ token: "test", expires_at: iso(clock + 3600) })
      : Response.json({ total_count: 0, workflow_runs: [] }),
  );
  await r.ingest({ run: run(), jobs: [job()] });
  assert.equal((await r.post("/health")).github_configured, false);
  const key = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  }).privateKey.export({ format: "pem", type: "pkcs8" });
  await r.updateBindings({
    GITHUB_APP_ID: "1",
    GITHUB_INSTALLATION_ID: "2",
    GITHUB_PRIVATE_KEY: String(key),
  });
  assert.equal((await r.post("/health")).github_configured, true);
  assert.equal((await r.summary()).daily["pr-total"], 1);
  await r.post("/tick");
  assert.ok((await r.post("/health")).requests.some((x) => x.status === 200));
  assert.equal((await r.post("/health")).repair_error, null);
});
test("activation requires a configured App and a recent publication", async (t) => {
  const r = await runtime(t);
  await r.post("/comparison", { passed: true, samples: 3 });
  assert.equal((await r.post("/activation-check")).ready, false);
});
test("activation can proceed immediately without a comparison waiting period", async (t) => {
  const key = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  }).privateKey.export({ format: "pem", type: "pkcs8" });
  const r = await runtime(
    t,
    {
      GITHUB_APP_ID: "1",
      GITHUB_INSTALLATION_ID: "2",
      GITHUB_PRIVATE_KEY: String(key),
    },
    (req) =>
      new URL(req.url).pathname.endsWith("/access_tokens")
        ? Response.json({ token: "test", expires_at: iso(clock + 3600) })
        : Response.json({ total_count: 0, workflow_runs: [] }),
  );
  assert.equal((await r.post("/activation-check")).ready, false);
  await r.post("/tick");
  const check = await r.post("/activation-check");
  assert.equal(check.ready, true);
  assert.equal(check.comparison_count, 0);
});
test("live activity launches while static charts cover unfinished historical import", async (t) => {
  const r = await runtime(t, { PUBLICATION_MODE: "live" });
  await r.ingest({
    run: { ...run(), status: "in_progress", conclusion: null },
    jobs: [
      { ...job(), status: "in_progress", conclusion: null, completed_at: null },
    ],
  });
  await r.notify();
  await r.post("/tick");
  const bucket = await r.mf.getR2Bucket("ARCHIVE");
  let manifest = await (await bucket.get("public/v1/ci/manifest.json")).json();
  assert.equal(manifest.mode, "live");
  assert.equal(manifest.current.running.github, 1);
  assert.equal(manifest.history.status, "importing");
  assert.deepEqual(manifest.assets, {});
  await r.ingest({
    historical: true,
    bootstrap: { since: iso(clock - 90 * 86400) },
  });
  await r.post("/tick");
  manifest = await (await bucket.get("public/v1/ci/manifest.json")).json();
  assert.equal(manifest.history.status, "ready");
  assert.equal(Object.keys(manifest.assets).length, 11);
});
test("parallel historical import preserves duplicate receipts and exact totals", async (t) => {
  const r = await runtime(t);
  const events = ["100", "100", "101", "102"].map((id) => ({
    repo,
    delivery: "import/" + id,
    historical: true,
    run: run(id),
    jobs: [job(String(Number(id) + 100), id)],
  }));
  const response = await r.mf.dispatchFetch(
    "https://example.com/admin/import",
    {
      method: "POST",
      headers: { Authorization: "Bearer admin" },
      body: JSON.stringify(events),
    },
  );
  assert.equal(response.status, 200, await response.clone().text());
  assert.equal((await response.json()).imported, 4);
  const summary = await r.summary();
  assert.equal(summary.daily["pr-total"], 3);
  assert.equal(summary.daily["duration/github"].median, 3);
  assert.equal(r.github(), 0);
});
test("cold aggregate pages restore exact distributions and idempotency receipts", async (t) => {
  const r = await runtime(t),
    date = iso(clock - 120 * 86400).slice(0, 10),
    ns = await r.mf.getDurableObjectNamespace("DAYS"),
    d = ns.get(ns.idFromName(date));
  const send = async (path, b) => {
    const response = await d.fetch("https://internal" + path, {
      method: "POST",
      body: JSON.stringify(b),
    });
    assert.equal(response.status, 200, await response.clone().text());
    return response.json();
  };
  const delta = {
    id: "original",
    day: date,
    rows: [
      { bucket: 0, metric: "duration/github", value: 2, amount: 1 },
      { bucket: 0, metric: "duration/github", value: 4, amount: 1 },
    ],
  };
  await send("/delta", delta);
  await send("/tick", {});
  let result;
  for (let i = 0; i < 10; i++) {
    result = await send("/compact", { day: date });
    if (result.done) break;
  }
  assert.equal(result.done, true);
  assert.deepEqual(
    (await send("/summary", { day: date })).daily["duration/github"],
    { median: 2, p90: 4 },
  );
  await send("/delta", {
    id: "correction",
    day: date,
    rows: [
      { bucket: 0, metric: "duration/github", value: 2, amount: -1 },
      { bucket: 0, metric: "duration/github", value: 6, amount: 1 },
    ],
  });
  await send("/delta", delta);
  assert.deepEqual(
    (await send("/summary", { day: date })).daily["duration/github"],
    { median: 4, p90: 6 },
  );
});
test("concurrent landing corrections leave a PR only on its earliest observed day", async (t) => {
  const r = await runtime(t),
    later = clock - 86400,
    earlier = clock - 2 * 86400;
  await Promise.all([
    r.ingest({ landing: { pr: 42, at: later } }),
    r.ingest({ landing: { pr: 42, at: earlier } }),
  ]);
  const ns = await r.mf.getDurableObjectNamespace("DAYS");
  async function landings(at) {
    const date = iso(at).slice(0, 10),
      response = await ns
        .get(ns.idFromName(date))
        .fetch("https://internal/summary", {
          method: "POST",
          body: JSON.stringify({ day: date }),
        });
    return (await response.json()).landings;
  }
  assert.equal((await landings(later)).length, 0);
  assert.equal((await landings(earlier)).length, 1);
});
test("older staging and partition notifications cannot regress current state", async (t) => {
  const r = await runtime(t),
    ns = await r.mf.getDurableObjectNamespace("DAYS"),
    d = ns.get(ns.idFromName(date));
  for (const b of [
    {
      status: "completed",
      conclusion: "success",
      updated: clock,
      members: [1, 2],
    },
    { status: "queued", conclusion: null, updated: clock - 60 },
  ]) {
    const response = await d.fetch("https://internal/staging", {
      method: "POST",
      body: JSON.stringify({
        id: "100",
        attempt: 1,
        created: clock - 300,
        batch: 1,
        testedHead: "a",
        day: date,
        ...b,
      }),
    });
    assert.equal(response.status, 200);
  }
  const response = await d.fetch("https://internal/summary", {
    method: "POST",
    body: "{}",
  });
  assert.equal((await response.json()).staging[0].conclusion, "success");
  await r.post("/partition", {
    partition: repo + "/" + date,
    active: 1,
    revision: 2,
  });
  await r.post("/partition", {
    partition: repo + "/" + date,
    active: 0,
    revision: 1,
  });
  assert.equal((await r.post("/health")).active_partitions, 1);
});
test("orphaned inbox writes recover and duplicate queue messages find the archived body", async (t) => {
  const r = await runtime(t),
    bucket = await r.mf.getR2Bucket("ARCHIVE"),
    control = await r.mf.getDurableObjectNamespace("CONTROL");
  const code = (
    await build({
      entryPoints: ["src/index.ts"],
      bundle: true,
      format: "esm",
      platform: "node",
      write: false,
      plugins: [
        {
          name: "unused-durable-object-bindings",
          setup(build) {
            // This test calls only entrypoint methods in Node; actual Durable
            // Objects and their binding handling run in the Miniflare instance.
            build.onResolve({ filter: /^cloudflare:workers$/ }, () => ({
              path: "bindings",
              namespace: "runtime-stub",
            }));
            build.onLoad({ filter: /.*/, namespace: "runtime-stub" }, () => ({
              contents: "export const env = {};",
              loader: "js",
            }));
          },
        },
      ],
    })
  ).outputFiles[0].text;
  const worker = (
    await import(
      "data:text/javascript;base64," + Buffer.from(code).toString("base64")
    )
  ).default;
  const key = "inbox/" + date + "/orphan.json";
  await bucket.put(
    key,
    JSON.stringify({ repo, delivery: "orphan", run: run() }),
    { customMetadata: { queued_at: String(clock - 300), dispatches: "1" } },
  );
  const queued = [],
    tasks = [],
    env = {
      ARCHIVE: bucket,
      CONTROL: control,
      EVENTS: { send: async (b) => queued.push(b) },
    };
  await worker.scheduled({}, env, { waitUntil: (p) => tasks.push(p) });
  await Promise.all(tasks);
  assert.ok(queued.some((b) => b.key === key));
  let ack = 0;
  const message = {
    body: { key },
    attempts: 1,
    ack: () => ack++,
    retry: () => assert.fail("delivery should not retry"),
  };
  await worker.queue({ messages: [message] }, env);
  await worker.queue({ messages: [message] }, env);
  assert.equal(ack, 2);
  assert.equal(await bucket.get(key), null);
  assert.ok(await bucket.get(key.replace("inbox/", "deliveries/")));
  const failed = key.replace("inbox/", "failed/");
  await bucket.put(
    failed,
    JSON.stringify({ repo, delivery: "orphan", run: run() }),
  );
  assert.equal(
    (
      await r.mf.dispatchFetch("https://example.com/admin/replay-failed", {
        method: "POST",
        body: "{}",
      })
    ).status,
    401,
  );
  const replay = await r.mf.dispatchFetch(
    "https://example.com/admin/replay-failed",
    {
      method: "POST",
      headers: { Authorization: "Bearer admin" },
      body: "{}",
    },
  );
  assert.equal(replay.status, 202);
  assert.deepEqual(await replay.json(), { enqueued: 1, cursor: null });
  await worker.queue(
    { messages: [{ ...message, body: { key: failed } }] },
    env,
  );
  assert.equal(await bucket.get(failed), null);
  await worker.queue(
    { messages: [{ ...message, body: { key: failed } }] },
    env,
  );
  assert.equal(ack, 4);
  assert.equal(r.github(), 0);
});
test("SQL projections match archived production metrics and timeline bins", async (t) => {
  const r = await runtime(t),
    f = JSON.parse(readFileSync("tests/reference.json", "utf8"));
  for (const [i, event] of f.events.entries())
    await r.ingest({ ...event, delivery: "fixture/" + i });
  const ns = await r.mf.getDurableObjectNamespace("DAYS"),
    response = await ns
      .get(ns.idFromName(f.day))
      .fetch("https://internal/summary", {
        method: "POST",
        body: JSON.stringify({ day: f.day }),
      });
  assert.equal(response.status, 200, await response.clone().text());
  const data = await response.json();
  for (const [key, value] of Object.entries(f.expected)) {
    if (typeof value === "number")
      assert.ok(Math.abs((data.daily[key] || 0) - value) < 1e-7, key);
    else assert.deepEqual(data.daily[key], value, key);
  }
  const bins = new Map(
    data.fleet.filter((b) => b.step === 600).map((b) => [b.at, b]),
  );
  for (let i = 0; i < 144; i++)
    for (const [type, expected] of [
      ["running", f.fleet.running],
      ["queued", f.fleet.queued],
      ["wait", f.fleet.wait_min],
    ])
      for (const [key, values] of Object.entries(expected))
        assert.ok(
          Math.abs(
            (bins.get(f.fleet.since + i * 600)?.[type]?.[key] || 0) - values[i],
          ) < 1e-7,
          `${type}/${key}/${i}`,
        );
});
