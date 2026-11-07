import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readFileSync } from "node:fs";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const repo = "TauCetiProject/TauCeti";
const seed = JSON.parse(
  readFileSync("tests/fixtures/operations-queue.frames.json", "utf8"),
)[0];
const code = await build({
  entryPoints: ["src/operations.ts"],
  bundle: true,
  platform: "node",
  format: "esm",
  write: false,
  plugins: [
    {
      name: "bindings",
      setup(builder) {
        builder.onResolve({ filter: /^cloudflare:workers$/ }, () => ({
          path: "bindings",
          namespace: "test",
        }));
        builder.onLoad({ filter: /.*/, namespace: "test" }, () => ({
          contents:
            "export const env = new Proxy({}, { get(_target, key) { return globalThis.__operationsTestEnv?.[key]; } });",
        }));
      },
    },
  ],
});
const { Operations } = await import(
  "data:text/javascript;base64," +
    Buffer.from(code.outputFiles[0].text).toString("base64")
);

function runtime(t) {
  const database = new DatabaseSync(":memory:"),
    objects = new Map(),
    writes = [],
    background = new Set();
  let alarm = null,
    failPointer = false,
    beforeWrite,
    clock = Date.parse(seed.metadata.observed_at);
  t.mock.method(Date, "now", () => clock);
  const ctx = {
    waitUntil(promise) {
      background.add(promise);
      promise.then(
        () => background.delete(promise),
        () => background.delete(promise),
      );
    },
    storage: {
      sql: {
        exec(sql, ...args) {
          if (
            args.some(
              (v) => typeof v === "string" && Buffer.byteLength(v) > 2_000_000,
            )
          )
            throw new Error("Durable Object SQLite row/value limit");
          let rows = [],
            rowsWritten = 0;
          if (!args.length && sql.split(";").filter((s) => s.trim()).length > 1)
            database.exec(sql);
          else {
            const statement = database.prepare(sql);
            if (statement.columns().length) rows = statement.all(...args);
            else rowsWritten = statement.run(...args).changes;
          }
          return {
            rowsWritten,
            toArray: () => rows,
            one: () => {
              assert.equal(rows.length, 1);
              return rows[0];
            },
          };
        },
      },
      transactionSync(fn) {
        database.exec("BEGIN");
        try {
          const result = fn();
          database.exec("COMMIT");
          return result;
        } catch (error) {
          database.exec("ROLLBACK");
          throw error;
        }
      },
      getAlarm: async () => alarm,
      setAlarm: async (value) => {
        alarm = value;
      },
      deleteAlarm: async () => {
        alarm = null;
      },
    },
  };
  globalThis.__operationsTestEnv = {
    OPERATIONS_PUBLIC_REPOSITORIES: JSON.stringify([repo]),
    ARCHIVE: {
      async put(key, value) {
        if (beforeWrite) {
          const callback = beforeWrite;
          beforeWrite = undefined;
          await callback(key, value);
        }
        if (key.endsWith("/manifest.json") && failPointer) {
          failPointer = false;
          throw new Error("R2 unavailable");
        }
        writes.push(key);
        objects.set(key, value);
      },
      async delete(key) {
        objects.delete(key);
      },
    },
  };
  const operations = new Operations(ctx);
  t.after(async () => {
    while (background.size) await Promise.all([...background]);
    database.close();
    delete globalThis.__operationsTestEnv;
  });
  const request = (path, value = {}) =>
    operations.fetch(
      new Request("https://internal" + path, {
        method: "POST",
        body: JSON.stringify(value),
      }),
    );
  const send = async (value, status = 200) => {
    const response = await request("/queue", value);
    assert.equal(response.status, status, await response.clone().text());
    return response.json();
  };
  const manifest = () =>
    JSON.parse(objects.get("public/v1/operations/manifest.json"));
  const object = (ref) => JSON.parse(objects.get("public" + ref.url));
  const next = (sequence = 2, epoch = 1) => {
    const value = structuredClone(seed);
    value.source.sequence = sequence;
    value.source.epoch = epoch;
    value.snapshot = crypto.randomUUID();
    value.metadata.observed_at = new Date(clock).toISOString();
    return value;
  };
  return {
    database,
    objects,
    writes,
    ctx,
    operations,
    background,
    async fire() {
      alarm = null;
      await operations.alarm();
    },
    async drain() {
      while (background.size) await Promise.all([...background]);
    },
    request,
    send,
    manifest,
    object,
    next,
    advance: (ms) => {
      clock += ms;
    },
    failPointer: () => {
      failPointer = true;
    },
    onWrite: (callback) => {
      beforeWrite = callback;
    },
    alarm: () => alarm,
    // Drive the cohort started by this alarm, then its coalesced pointer.
    // Do not start newer queue captures ingested during this cohort's upload.
    async flush() {
      clock = Math.max(
        clock,
        ...["queue", "jobs", "hosts", "host_history", "history"].map(
          (kind) => (operations.get("domain_started/" + kind) || 0) + 10000,
        ),
      );
      alarm = null;
      await operations.alarm();
      while (background.size) await Promise.all([...background]);
      clock = Math.max(clock, (operations.pointerStarted || 0) + 10000);
      operations.startPointer();
      while (background.size) await Promise.all([...background]);
      await operations.rearm();
    },
  };
}

test("backlog counts distinct waiting PRs and current active members, including staging", async (t) => {
  const r = runtime(t);
  const backlog = async () => (await (await r.request("/backlog")).json()).observation;
  assert.equal(await backlog(), null);
  const frame = r.next(1);
  await r.send(frame);
  assert.equal(await backlog(), null); // Unknown/shadow queues cannot replace production history.
  frame.source.sequence = 2;
  frame.snapshot = crypto.randomUUID();
  frame.metadata.service.mode = "production";
  const input = frame.changes.find((c) => c.collection === "inputs").value;
  for (const [id, pr, state] of [["3", "101", "waiting"], ["4", "103", "reserved"], ["5", "104", "withdrawn"], ["6", "105", "landed"]]) {
    frame.changes.push({ collection: "inputs", id, value: { ...input, id, pr, state, batch_id: null } });
  }
  const member = frame.changes.find((c) => c.collection === "members").value;
  for (const [id, pr, extra] of [
    ["3", "106", { batch_version: 1 }],
    ["4", "107", { removed: true }],
    ["5", "108", { withdrawn: true }],
    ["6", "109", { batch_id: "2", batch_version: 0 }],
  ]) frame.changes.push({ collection: "members", id, value: {
    ...member, id, pr, pr_url: `https://github.com/${repo}/pull/${pr}`, ...extra,
  } });
  await r.send(frame);
  const observation = await backlog();
  assert.equal(observation.eligible_count, 4); // 101, 102, 103, and open-batch 109.
  assert.equal(observation.candidate_scan_complete, true);
  assert.equal(observation.candidate_snapshot_at, frame.metadata.observed_at);
  r.advance(300_000);
  assert.deepEqual(await backlog(), observation); // Polling cannot freshen a stopped producer.
  const delta = r.next(3);
  delta.base = { epoch: 1, sequence: 2 };
  delta.metadata.service.mode = "production";
  delta.changes = [{ collection: "batches", id: "1", value: {
    ...frame.changes.find((c) => c.collection === "batches").value, state: "landed",
  } }];
  await r.send(delta);
  assert.equal((await backlog()).eligible_count, 3); // Terminal batch members no longer count.
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
  assert.equal(await backlog(), null);
});

test("backlog ignores partial uploads and marks incomplete committed collections as gaps", async (t) => {
  const r = runtime(t);
  const frame = r.next(1);
  frame.metadata.service.mode = "production";
  await r.send(frame);
  const initial = await (await r.request("/backlog")).json();
  const next = r.next(2);
  next.metadata.service.mode = "production";
  next.metadata.collection_complete.members = false;
  const parts = split(next);
  for (const part of parts.slice(0, -1)) await r.send(part);
  assert.deepEqual(await (await r.request("/backlog")).json(), initial);
  await r.send(parts.at(-1));
  const { observation } = await (await r.request("/backlog")).json();
  assert.equal(observation.candidate_scan_complete, false);
  assert.equal(observation.candidate_snapshot_at, null);
});

function split(frame, size = 3) {
  const chunks = [];
  for (let i = 0; i < frame.changes.length; i += size)
    chunks.push(frame.changes.slice(i, i + size));
  return chunks.map((changes, part) => ({
    schema: frame.schema,
    source: frame.source,
    snapshot: frame.snapshot,
    base: frame.base,
    part,
    parts: chunks.length,
    changes,
    ...(part === chunks.length - 1 ? { metadata: frame.metadata } : {}),
  }));
}

function lookupFrame(r, count = 256, large = false) {
  const frame = r.next(1),
    template = frame.changes.find((c) => c.collection === "members").value;
  const batch = frame.changes.find(
    (c) => c.collection === "batches" && c.id === template.batch_id,
  ).value;
  batch.size = count;
  frame.changes = frame.changes.filter((c) => c.collection !== "members");
  for (let i = 0; i < count; i++) {
    const pr = String(large ? Math.floor(i / 4) * 64 + (i % 4) + 64 : i + 1);
    frame.changes.push({
      collection: "members",
      id: String(i + 1),
      value: {
        ...template,
        id: String(i + 1),
        pr,
        position: i + 1,
        pr_url: `https://github.com/${repo}/pull/${pr}`,
        ...(large
          ? { title: "🧮".repeat(300), explanation: "🧮".repeat(300) }
          : {}),
      },
    });
  }
  frame.metadata.collection_counts.members = count;
  return frame;
}

test("a full membership version update shares bounded lookup objects with the existing reader", async (t) => {
  const r = runtime(t), frame = lookupFrame(r);
  for (const part of split(frame, 100)) await r.send(part);
  await r.flush();
  const before = r.writes.length;
  frame.source.sequence++;
  frame.snapshot = crypto.randomUUID();
  for (const c of frame.changes) {
    if (c.collection === "batches") c.value.version++;
    if (c.collection === "members") c.value.batch_version++;
  }
  for (const part of split(frame, 100)) await r.send(part);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue),
    puts = r.writes.slice(before).filter((key) =>
      JSON.parse(r.objects.get(key)).schema === "tauceti.operations.prs/v1",
    );
  assert.equal(queue.pr_shards.length, 64);
  assert.equal(puts.length, 16, "64 changed buckets need only 16 writes");
  for (const c of frame.changes.filter((c) => c.collection === "members")) {
    // This is the existing browser's lookup, with no new schema or fallback.
    const record = r.object(queue.pr_shards[Number(BigInt(c.value.pr) % 64n)])
      .prs[`${repo}#${c.value.pr}`];
    assert.deepEqual(record.members, [c.value]);
  }
});

test("a changed or removed lookup member preserves unchanged neighboring buckets", async (t) => {
  const r = runtime(t), frame = lookupFrame(r);
  for (const part of split(frame, 100)) await r.send(part);
  await r.flush();
  const previous = r.object(r.manifest().domains.queue);
  frame.source.sequence++;
  frame.snapshot = crypto.randomUUID();
  frame.changes.find((c) => c.collection === "members" && c.id === "1")
    .value.explanation = "Updated explanation";
  frame.changes.find((c) => c.collection === "members" && c.id === "2")
    .value = null;
  frame.metadata.collection_counts.members--;
  frame.changes.find((c) => c.collection === "batches" && c.id === "1")
    .value.size--;
  for (const part of split(frame, 100)) await r.send(part);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue);
  assert.equal(r.object(queue.pr_shards[1]).prs[`${repo}#1`].members[0]
    .explanation, "Updated explanation");
  assert.equal(r.object(queue.pr_shards[2]).prs[`${repo}#2`]?.members.length || 0, 0);
  assert.deepEqual(r.object(queue.pr_shards[3]).prs[`${repo}#3`],
    r.object(previous.pr_shards[3]).prs[`${repo}#3`]);
  assert.deepEqual(queue.pr_shards[4], previous.pr_shards[4],
    "an unrelated group keeps its immutable reference");
  assert.equal(queue.counts.waiting, previous.counts.waiting);
});

test("lookup grouping splits by UTF-8 bytes before uploading oversized groups", async (t) => {
  const r = runtime(t), frame = lookupFrame(r, 256, true);
  for (const part of split(frame, 50)) await r.send(part);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue);
  assert.equal(new Set(queue.pr_shards.slice(0, 4).map((r) => r.url)).size, 4);
  for (const ref of queue.pr_shards)
    assert.ok(Buffer.byteLength(r.objects.get("public" + ref.url)) <= 262144);
  for (const c of frame.changes.filter((c) => c.collection === "members"))
    assert.deepEqual(r.object(queue.pr_shards[Number(BigInt(c.value.pr) % 64n)])
      .prs[`${repo}#${c.value.pr}`].members, [c.value]);
});

test("independent queue children and PR lookups advance while a member upload is held", async (t) => {
  const r = runtime(t),
    archive = globalThis.__operationsTestEnv.ARCHIVE;
  const original = archive.put.bind(archive),
    seen = new Set();
  let release,
    held = false,
    active = 0,
    peak = 0;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  archive.put = async (key, value) => {
    const schema = JSON.parse(value).schema;
    seen.add(schema);
    active++;
    peak = Math.max(peak, active);
    try {
      if (!held && schema === "tauceti.operations.members/v1") {
        held = true;
        await gate;
      }
      return await original(key, value);
    } finally {
      active--;
    }
  };
  try {
    await r.send(seed);
    await pumpUntil(
      r,
      () =>
        held &&
        seen.has("tauceti.operations.attempts/v1") &&
        seen.has("tauceti.operations.events/v1") &&
        seen.has("tauceti.operations.prs/v1"),
    );
    assert.ok(
      ![...r.objects.values()].some(
        (v) => JSON.parse(v).schema === "tauceti.operations.queue/v1",
      ),
      "a root cannot reference the held child",
    );
    assert.ok(
      peak <= 4,
      "independent graphs share the four underlying upload permits",
    );
    r.advance(123);
  } finally {
    release();
  }
  await r.drain();
  await r.flush();
  const queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.source_sequence, 1);
  const timing = (await (await r.request("/health")).json()).queue_preparation;
  assert.equal(timing.scope, r.manifest().scope);
  assert.equal(timing.source_sequence, queue.source_sequence);
  assert.equal(timing.observed_at, queue.observed_at);
  assert.ok(
    timing.started_at <= timing.capture_finished_at &&
      timing.capture_finished_at <= timing.graph_finished_at &&
      timing.graph_finished_at <= timing.ready_recorded_at,
  );
  assert.ok(
    timing.object_requests >= timing.puts &&
      timing.puts > 0 &&
      timing.shared_hits > 0,
  );
  assert.ok(timing.put_ms > 0 && timing.bytes_written > 0);
  for (const value of r.objects.values())
    for (const ref of value.matchAll(
      /\/v1\/operations\/objects\/[a-f0-9]{64}\/[a-f0-9]{64}\.json/g,
    ))
      assert.ok(r.objects.has("public" + ref[0]));
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
  await r.fire();
  await r.drain();
  assert.equal(
    (await (await r.request("/health")).json()).queue_preparation,
    null,
    "withdrawn source timing cannot masquerade as current-scope work",
  );
});

test("a failed queue sibling retains its preparation while another underlying put is held", async (t) => {
  const r = runtime(t),
    archive = globalThis.__operationsTestEnv.ARCHIVE;
  const original = archive.put.bind(archive);
  let release,
    held = false,
    failed = false;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  archive.put = async (key, value) => {
    const schema = JSON.parse(value).schema;
    if (!held && schema === "tauceti.operations.members/v1") {
      held = true;
      await gate;
    }
    if (!failed && schema === "tauceti.operations.prs/v1") {
      failed = true;
      throw Error("storage failure");
    }
    return original(key, value);
  };
  try {
    await r.send(seed);
    await pumpUntil(r, () => held && failed);
    const originalPreparation = r.operations.preparing.get("queue");
    assert.ok(originalPreparation);
    r.advance(15000);
    await r.send(r.next(2));
    await r.fire();
    assert.equal(
      r.operations.preparing.get("queue"),
      originalPreparation,
      "failure cannot release ownership before the held sibling settles",
    );
  } finally {
    release();
  }
  await r.drain();
});

test("completed preparation timings survive sampling gaps, restart, and the single-summary format", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const first = (await (await r.request("/health")).json()).queue_preparation;
  r.operations.set("queue_preparation", first); // live-update predecessor representation
  for (let sequence = 2; sequence <= 12; sequence++) {
    r.advance(10000);
    await r.send(r.next(sequence));
    await r.flush();
  }
  const restarted = new Operations(r.ctx);
  const health = await (
    await restarted.fetch(new Request("https://internal/health"))
  ).json();
  assert.deepEqual(
    health.queue_preparations.map((v) => v.source_sequence),
    [5, 6, 7, 8, 9, 10, 11, 12],
  );
  assert.deepEqual(health.queue_preparation, health.queue_preparations.at(-1));
  assert.ok(
    health.queue_preparations.every(
      (v) =>
        v.scope === r.manifest().scope &&
        v.ready_recorded_at >= v.graph_finished_at,
    ),
  );
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
  await restarted.alarm();
  await r.drain();
  const withdrawn = await (
    await restarted.fetch(new Request("https://internal/health"))
  ).json();
  assert.deepEqual(withdrawn.queue_preparations, []);
  assert.equal(withdrawn.queue_preparation, null);
});

test("actual Python export is accepted and produces linked, paginated queue and PR views", async (t) => {
  const r = runtime(t);
  const result = await r.send(seed);
  assert.equal(result.committed, true);
  assert.equal(
    r.objects.size,
    0,
    "ingest schedules publication rather than doing R2 work in the producer request",
  );
  await r.flush();
  const manifest = r.manifest(),
    queue = r.object(manifest.domains.queue);
  assert.equal(queue.schema, "tauceti.operations.queue/v1");
  assert.equal(queue.counts.members, 2);
  const head = queue.batches.find((b) => b.state === "head");
  const index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
  const detail = r.object(index.batches.find((b) => b.id === head.id).detail);
  assert.equal(detail.member_details_complete, true);
  const page = r.object(detail.member_pages[0]);
  assert.equal(page.records.length, 2);
  const pr = r.object(queue.pr_shards[101 % 64]).prs[repo + "#101"];
  assert.equal(
    pr.members[0].accepted_head,
    page.records.find((m) => m.pr === "101").accepted_head,
  );
  const attempts = r.object(detail.attempt_pages[0]).records;
  assert.equal(attempts[0].candidate, head.tip);
  assert.equal(attempts[0].base, head.base);
});

test("repair build observations retain numeric progress separately from phase age and reject private log fields", async (t) => {
  const r = runtime(t),
    frame = structuredClone(seed);
  const head = frame.changes.find((c) => c.collection === "batches").value;
  const build = {
    stage: "preparation",
    completed_tasks: 58742,
    total_tasks: 60357,
    sampled_at: seed.metadata.observed_at,
    log_updated_at: seed.metadata.observed_at,
    last_progress_at: null,
  };
  const task = {
    id: "21",
    kind: "rebase_conflict",
    batch_id: head.id,
    batch_version: head.version,
    state: "open",
    resolution: "",
    ci_attempt_id: null,
    created_at: seed.metadata.observed_at,
    resolved_at: null,
    progress: {
      coverage: "observed",
      state: "sources_ready",
      observed_at: "2026-01-01T00:00:00Z",
      seconds: null,
      providers: [],
      models: [],
      cost: {
        recorded_usd: 0,
        recorded_attempts: 0,
        unknown_attempts: 0,
        coverage: "recorded_subtotal_only",
      },
      build,
    },
  };
  frame.changes.push({ collection: "tasks", id: task.id, value: task });
  task.progress.models_truncated = false;
  const invalidCoverage = structuredClone(frame);
  invalidCoverage.changes.at(-1).value.progress.models_truncated = "PRIVATE";
  await r.send(invalidCoverage, 400);
  for (const change of [
    (b) => {
      b.private_log = "PRIVATE";
    },
    (b) => {
      b.completed_tasks = 60358;
    },
    (b) => {
      b.total_tasks = 0;
    },
    (b) => {
      b.completed_tasks = 1.5;
    },
    (b) => {
      b.stage = "PRIVATE_PATH";
    },
    (b) => {
      delete b.last_progress_at;
    },
    (b) => {
      b.sampled_at = null;
    },
  ]) {
    const invalid = structuredClone(frame);
    change(invalid.changes.at(-1).value.progress.build);
    await r.send(invalid, 400);
  }
  const unavailable = structuredClone(frame);
  unavailable.changes.at(-1).value.progress.coverage = "unavailable";
  await r.send(unavailable, 400);
  await r.send(frame);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue);
  assert.deepEqual(
    queue.batches.find((b) => b.id === head.id).active_tasks[0].progress.build,
    build,
  );
  assert.equal(
    queue.batches.find((b) => b.id === head.id).active_tasks[0].progress
      .observed_at,
    task.progress.observed_at,
  );
});

test("out-of-order frames stay invisible until every part commits, including across object eviction", async (t) => {
  const r = runtime(t),
    frames = split(seed);
  assert.equal((await r.send(frames.at(-1))).committed, false);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_entities").get().count,
    0,
  );
  r.operations = new Operations(r.ctx);
  for (let i = 0; i < frames.length - 1; i++) {
    const response = await r.operations.fetch(
      new Request("https://internal/queue", {
        method: "POST",
        body: JSON.stringify(frames[i]),
      }),
    );
    assert.equal(response.status, 200, await response.clone().text());
    const result = await response.json();
    assert.equal(result.committed, i === frames.length - 2);
  }
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_entities").get().count,
    seed.changes.length,
  );
});

test("duplicate receipts cannot acknowledge different source data", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  assert.equal((await r.send(seed)).committed, true);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_entities").get().count,
    seed.changes.length,
  );
  const changed = structuredClone(seed);
  changed.changes[0].value.state = "landed";
  await r.send(changed, 409);
  const health = await (await r.request("/health")).json();
  assert.equal(health.revision, 1);
});

test("a newer process fences delayed old-process uploads without discarding the last published snapshot", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const oldManifest = r.objects.get("public/v1/operations/manifest.json");
  const next = split(r.next(1, 2));
  await r.send(next[0]);
  await r.send(r.next(3, 1), 409);
  assert.equal(
    r.objects.get("public/v1/operations/manifest.json"),
    oldManifest,
  );
  for (const frame of next.slice(1)) await r.send(frame);
  await r.flush();
  assert.equal(r.object(r.manifest().domains.queue).source_epoch, 2);
});

test("one sequence cannot mix different snapshots or duplicate changed identities", async (t) => {
  const r = runtime(t),
    parts = split(seed);
  await r.send(parts[0]);
  const conflicting = structuredClone(parts[1]);
  conflicting.snapshot = crypto.randomUUID();
  await r.send(conflicting, 409);
  const newer = r.next();
  newer.changes.push(structuredClone(newer.changes[0]));
  await r.send(newer, 400);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_entities").get().count,
    0,
  );
});

test("unknown private fields, malformed identities and arbitrary links are rejected before persistence", async (t) => {
  const r = runtime(t),
    mutations = [
      (f) => {
        f.changes[0].value.token = "PRIVATE_TOKEN";
      },
      (f) => {
        f.metadata.workers.push({
          id: "w",
          coverage: "unavailable",
          observed_at: null,
          root: "/private/home",
        });
      },
      (f) => {
        f.metadata.service.environment = { SECRET: "PRIVATE" };
      },
      (f) => {
        f.changes.find((c) => c.collection === "members").value.pr_url =
          "https://evil.example/";
      },
      (f) => {
        f.changes.find((c) => c.collection === "members").value.repository =
          "PrivateOrg/Hidden";
      },
      (f) => {
        f.changes[0].id = 9007199254740993;
      },
      (f) => {
        delete f.changes.find((c) => c.collection === "members").value.batch_id;
      },
    ];
  for (const mutate of mutations) {
    const value = structuredClone(seed);
    mutate(value);
    await r.send(value, 400);
  }
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_frames").get().count,
    0,
  );
  assert.equal(
    r.database.prepare("SELECT COUNT(*) count FROM ops_sources").get().count,
    0,
  );
  assert.equal(r.objects.size, 0);
});

test("failed manifest publication retains the last complete generation and retries durably", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const previous = r.objects.get("public/v1/operations/manifest.json");
  r.advance(15_000);
  r.failPointer();
  await r.send(r.next());
  await r.drain();
  assert.equal(r.objects.get("public/v1/operations/manifest.json"), previous);
  assert.equal(
    (await (await r.request("/health")).json()).publication_error,
    true,
  );
  assert.ok(r.alarm() > Date.now());
  r.advance(15_000);
  await r.flush();
  assert.equal(
    (await (await r.request("/health")).json()).publication_error,
    false,
  );
  assert.equal(r.object(r.manifest().domains.queue).source_sequence, 2);
});

test("incoming heartbeats do not postpone an earlier publication alarm", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  const first = r.alarm();
  r.advance(5000);
  await r.send(r.next());
  assert.equal(r.alarm(), first);
  // Delivery can lag the deadline while other requests arrive. An expired
  // deadline is still pending work, and must not be moved into the future.
  r.advance(6000);
  await r.send(r.next(3));
  assert.equal(r.alarm(), first);
});

test("new observations use the next publication slot without adding an idle interval", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  r.advance(15000);
  await r.flush();
  const published = Date.now();
  r.advance(8000);
  await r.send(r.next());
  assert.equal(
    r.alarm(),
    published + 10000,
    "preserve the existing minimum write interval",
  );
  r.advance(3000);
  await r.flush();
  r.advance(15000);
  await r.send(r.next(3));
  assert.equal(
    r.alarm(),
    Date.now() + 1000,
    "idle arrivals need not wait another full interval",
  );
});

test("ingestion during an R2 write cannot publish mixed member and candidate generations", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const previous = r.objects.get("public/v1/operations/manifest.json");
  const second = r.next(2);
  second.changes.find(
    (c) => c.collection === "batches" && c.value.state === "head",
  ).value.version = 2;
  const third = r.next(3);
  third.changes.find(
    (c) => c.collection === "batches" && c.value.state === "head",
  ).value.version = 3;
  third.changes
    .filter((c) => c.collection === "members")
    .forEach((c) => {
      c.value.batch_version = 3;
    });
  third.changes.find((c) => c.collection === "members").value.explanation =
    "The newer complete generation.";
  r.onWrite(async () => {
    await r.send(third);
  });
  r.advance(15000);
  await r.send(second);
  await r.drain();
  assert.notEqual(
    r.objects.get("public/v1/operations/manifest.json"),
    previous,
  );
  assert.equal(
    (await (await r.request("/health")).json()).published_revision,
    1, // Newer uncommitted ingest keeps the conservative global watermark behind.
  );
  assert.equal(
    (await (await r.request("/health")).json()).domain_publication.queue
      .committed,
    "3/" + second.snapshot,
  );
  const captured = r.object(r.manifest().domains.queue);
  assert.equal(captured.batches.find((b) => b.state === "head").version, 2);
  r.advance(10000);
  await r.fire();
  await r.drain();
  const queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.source_sequence, 3);
  const head = queue.batches.find((b) => b.state === "head");
  assert.equal(head.version, 3);
  const index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
  const detail = r.object(index.batches.find((b) => b.id === head.id).detail);
  const members = r.object(detail.member_pages[0]).records;
  assert.ok(
    members.some((m) => m.explanation === "The newer complete generation."),
  );
});

test("1200 members have complete stable pagination and only changed detail objects are uploaded again", async (t) => {
  const r = runtime(t),
    value = structuredClone(seed),
    template = value.changes.find((c) => c.collection === "members").value;
  value.changes = value.changes.filter((c) => c.collection !== "members");
  const batch = value.changes.find(
    (c) => c.collection === "batches" && c.value.state === "head",
  );
  batch.value.size = 1200;
  for (let i = 1; i <= 1200; i++) {
    const member = {
      ...template,
      id: String(i),
      pr: String(i),
      position: i,
      pr_url: `https://github.com/${repo}/pull/${i}`,
    };
    value.changes.push({ collection: "members", id: member.id, value: member });
  }
  value.metadata.collection_counts.members = 1200;
  let active = 0,
    peak = 0,
    allActive = 0,
    allPeak = 0,
    ready,
    release;
  const fourPages = new Promise((resolve) => {
    ready = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const watch = async (_key, body) => {
    r.onWrite(watch);
    allActive++;
    allPeak = Math.max(allPeak, allActive);
    try {
      if (JSON.parse(body).schema !== "tauceti.operations.members/v1") return;
      active++;
      peak = Math.max(peak, active);
      if (active === 4) ready(true);
      try {
        await gate;
      } finally {
        active--;
      }
    } finally {
      allActive--;
    }
  };
  r.onWrite(watch);
  for (const frame of split(value, 400)) await r.send(frame);
  const publication = r.flush();
  try {
    assert.equal(
      await Promise.race([
        fourPages,
        new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
      ]),
      true,
      "independent member pages should upload concurrently",
    );
    assert.equal(
      r.objects.has("public/v1/operations/manifest.json")
        ? r.manifest().domains.queue.url
        : null,
      null,
      "the pointer must wait for every dependent object",
    );
  } finally {
    release();
    await publication;
  }
  assert.equal(peak, 4, "independent member pages overlap");
  assert.ok(allPeak <= 4, "R2 writes remain globally bounded across domains");
  assert.equal(
    new Set(
      r.writes
        .filter((key) => !key.endsWith("/manifest.json"))
        .map((key) => r.objects.get(key)),
    ).size,
    r.writes.filter((key) => !key.endsWith("/manifest.json")).length,
    "identical immutable payloads upload once, including under physical incarnations",
  );
  const queue = r.object(r.manifest().domains.queue),
    index = r.object(queue.batch_shards[Number(BigInt(batch.id) % 64n)]);
  const detail = r.object(index.batches.find((b) => b.id === batch.id).detail);
  assert.equal(detail.member_pages.length, 12);
  assert.equal(
    detail.member_pages.reduce((n, p) => n + r.object(p).records.length, 0),
    1200,
  );
  const writes = r.writes.length;
  const heartbeat = r.next();
  heartbeat.base = { epoch: 1, sequence: 1 };
  heartbeat.changes = [];
  heartbeat.metadata.collection_counts.members = 1200;
  r.advance(15_000);
  await r.send(heartbeat);
  await r.flush();
  assert.ok(
    r.writes.length - writes <= 4,
    "queue, jobs and hosts current objects plus manifest, with immutable details reused",
  );
});

test("waiting inputs have bounded pages and leave the prepared list atomically on insertion", async (t) => {
  const r = runtime(t),
    frame = structuredClone(seed);
  const template = frame.changes.find((c) => c.collection === "inputs").value;
  const waiting = Array.from({ length: 1200 }, (_, i) => ({
    ...template,
    id: String(10000 + i),
    pr: String(20000 + i),
    state: "waiting",
    batch_id: null,
  }));
  frame.changes.push(
    ...waiting.map((value) => ({ collection: "inputs", id: value.id, value })),
  );
  frame.metadata.collection_counts.inputs += waiting.length;
  for (const part of split(frame, 400)) await r.send(part);
  await r.flush();
  let queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.counts.waiting, 1200);
  assert.equal(queue.waiting_pages.length, 12);
  assert.deepEqual(
    queue.waiting_pages
      .flatMap((p) => r.object(p).records)
      .map((i) => i.id)
      .sort(),
    waiting.map((i) => i.id).sort(),
  );
  const delta = r.next();
  delta.base = { epoch: frame.source.epoch, sequence: frame.source.sequence };
  delta.changes = [
    {
      collection: "inputs",
      id: waiting[0].id,
      value: { ...waiting[0], state: "inserted", batch_id: "1" },
    },
  ];
  delta.metadata.collection_counts.inputs =
    frame.metadata.collection_counts.inputs;
  await r.send(delta);
  assert.equal(r.object(r.manifest().domains.queue).counts.waiting, 1200);
  await r.flush();
  queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.counts.waiting, 1199);
  assert.ok(
    !queue.waiting_pages
      .flatMap((p) => r.object(p).records)
      .some((i) => i.id === waiting[0].id),
  );
});

test("public reads use R2 and cache only, with scoped ingestion, ETags and zero upstream requests", async (t) => {
  let upstream = 0;
  const options = convertV4MiniflareOptions({
    modules: true,
    script: readFileSync("dist/index.js", "utf8"),
    compatibilityDate: "2026-10-15",
    durableObjects: {
      OPERATIONS: { className: "Operations", useSQLite: true },
    },
    r2Buckets: ["ARCHIVE"],
    bindings: {
      QUEUE_MONITOR_TOKEN: "publisher",
      OPERATIONS_PUBLIC_REPOSITORIES: JSON.stringify([repo]),
      PUBLIC_ORIGIN: "https://taucetiproject.org",
      ALLOWED_REPOSITORIES: JSON.stringify([repo]),
    },
    outboundService: async () => {
      upstream++;
      throw new Error("A browser must not collect upstream data");
    },
  });
  const mf = new Miniflare(options);
  t.after(() => mf.dispose());
  const url = "https://ci.example",
    headers = {
      Authorization: "Bearer publisher",
      "Content-Type": "application/json",
    };
  assert.equal(
    (
      await mf.dispatchFetch(url + "/ingest/operations/queue", {
        method: "POST",
        body: JSON.stringify(seed),
      })
    ).status,
    401,
  );
  const response = await mf.dispatchFetch(url + "/ingest/operations/queue", {
    method: "POST",
    headers,
    body: JSON.stringify(seed),
  });
  assert.equal(response.status, 200, await response.clone().text());
  const ns = await mf.getDurableObjectNamespace("OPERATIONS");
  await ns
    .get(ns.idFromName("operations"))
    .fetch("https://internal/tick", { method: "POST", body: "{}" });
  // The alarm returns after scheduling independent background preparations.
  // Wait for the prepared pointer, without making a browser GET collect data.
  const bucket = await mf.getR2Bucket("ARCHIVE");
  for (let i = 0; i < 600; i++) {
    const pointer = await bucket.get("public/v1/operations/manifest.json");
    if (pointer && (await pointer.json()).domains.queue.url) break;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const manifest = await mf.dispatchFetch(url + "/v1/operations/manifest.json");
  assert.equal(manifest.status, 200);
  assert.equal(
    manifest.headers.get("access-control-allow-origin"),
    "https://taucetiproject.org",
  );
  assert.equal(manifest.headers.get("cache-control"), "no-store");
  assert.equal(
    (
      await mf.dispatchFetch(url + "/v1/operations/manifest.json", {
        headers: { "If-None-Match": manifest.headers.get("etag") },
      })
    ).status,
    304,
  );
  const queue = (await manifest.json()).domains.queue;
  const cachedManifest = await mf.dispatchFetch(
    url + "/v1/operations/manifest.json",
  );
  assert.equal(cachedManifest.headers.get("cache-control"), "no-store");
  const reads = await Promise.all(
    Array.from({ length: 100 }, () => mf.dispatchFetch(url + queue.url)),
  );
  assert.ok(reads.every((r) => r.status === 200));
  assert.equal(upstream, 0);
  options.workers[0].config.env.OPERATIONS_PUBLIC_REPOSITORIES = {
    type: "text",
    value: "[]",
  };
  await mf.setOptions(options);
  assert.equal(
    (await mf.dispatchFetch(url + queue.url)).status,
    404,
    "scope withdrawal blocks a formerly cached immutable URL before cache lookup",
  );
  assert.equal(
    (await mf.dispatchFetch(url + "/v1/operations/objects/not-a-digest.json"))
      .status,
    404,
  );
  assert.equal(
    (await mf.dispatchFetch(url + "/v1/operations/private/registration.json"))
      .status,
    404,
  );
});

test("busy ingestion publishes coherent captured generations without starvation and idle alarms stop", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  for (let n = 2; n <= 7; n++) {
    r.advance(10000);
    await r.send(r.next(n));
    await r.drain();
    assert.equal(queueSequence(r), n);
    const queue = r.object(r.manifest().domains.queue),
      head = queue.batches.find((b) => b.state === "head");
    const index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
    assert.equal(
      r.object(index.batches.find((b) => b.id === head.id).detail).batch
        .version,
      head.version,
    );
  }
  assert.equal(r.alarm(), null);
  const writes = r.writes.length;
  await r.request("/tick");
  await r.drain();
  assert.equal(r.writes.length, writes);
});

test("a missing delta base requests resync and deleted dispatch attempts disappear atomically", async (t) => {
  const r = runtime(t),
    delta = r.next();
  delta.base = { epoch: 1, sequence: 1 };
  delta.changes = [];
  assert.equal((await r.send(delta, 409)).code, "resync");
  await r.send(seed);
  await r.flush();
  const attempt = seed.changes.find((c) => c.collection === "attempts");
  const deleted = r.next();
  deleted.base = { epoch: 1, sequence: 1 };
  deleted.changes = [{ collection: "attempts", id: attempt.id, value: null }];
  await r.send(deleted);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue),
    head = queue.batches.find((b) => b.state === "head");
  const index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
  assert.equal(
    r.object(index.batches.find((b) => b.id === head.id).detail).attempt_pages
      .length,
    0,
  );
});

test("removed members are retained as evidence without corrupting active membership completeness", async (t) => {
  const r = runtime(t),
    value = structuredClone(seed);
  value.changes.find((c) => c.collection === "members").value.removed = true;
  value.changes.find(
    (c) => c.collection === "batches" && c.value.state === "head",
  ).value.size = 1;
  await r.send(value);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue),
    head = queue.batches.find((b) => b.state === "head");
  const index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
  const detail = r.object(index.batches.find((b) => b.id === head.id).detail);
  assert.equal(detail.member_details_complete, true);
  assert.equal(detail.removed_count, 1);
  assert.equal(queue.counts.members, 1);
});

test("1000 jobs use existing facts and paginated operations views with distinct workflow queues", async (t) => {
  const r = runtime(t);
  const partition = repo + "/2026-11-06";
  let reads = 0;
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () => {
        reads++;
        return Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision: 1,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs: Array.from({ length: 1000 }, (_, i) => ({
            repository: repo,
            id: String(i),
            run_id: "100",
            run_attempt: 1,
            active: true,
            status: i < 600 ? "in_progress" : "queued",
            assigned: i < 600,
            observed_at: new Date(Date.now()).toISOString(),
          })),
          pending_workflows: [
            { repository: repo, run_id: "101", run_attempt: 1 },
          ],
        });
      },
    }),
  };
  const response = await r.request("/partition", { partition, revision: 1 });
  assert.equal(response.status, 200);
  assert.equal(reads, 0, "notifications schedule, not read Facts reentrantly");
  await r.flush();
  const jobs = r.object(r.manifest().domains.jobs);
  assert.equal(jobs.job_pages.length, 10);
  assert.deepEqual(jobs.counts, {
    running: 600,
    queued_jobs: 400,
    pending_workflows: 1,
    active: 1000,
  });
  assert.equal(reads, 1);
});

test("host scopes, permanent identities and stale-source evidence reject arbitrary private diagnostics", async (t) => {
  const r = runtime(t),
    machine = "a".repeat(32);
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = JSON.stringify([
    { machine_id: machine, alias: "yael" },
  ]);
  const host = {
    machine_id: machine,
    alias: "yael",
    observed_at: new Date(Date.now() - 120000).toISOString(),
    transport_at: new Date(Date.now()).toISOString(),
    connected: true,
    coverage: "observed",
    source_revision: "a".repeat(40),
    controller_error: false,
    ci_enabled: false,
    workers_enabled: true,
    ci_max_jobs: 0,
    worker_memory: {
      working_set: 123,
      charged: 456,
      maximum: 1000,
      high: 800,
      swap: 0,
      oom_kill: 2,
      stall_some: 0.1,
      stall_full: null,
    },
    worker_admission: { state: "paused", reason: "memory_reserve" },
    ci_cooldown: { until: new Date(Date.now() + 30000).toISOString() },
    worker_limit: 2,
    capacity_context: {
      observed_at: new Date(Date.now() - 120000).toISOString(),
      scope: "organization_all_workflow_jobs",
      coverage: "observed",
      observer_error: false,
      running_jobs: { github: 3, namespace: 4, "self-hosted": null },
    },
    admission: {
      ci_kinds: ["pr", "staging"],
      staging_slots: 1,
      memory_budget_gib: 64,
      reservation_error: false,
    },
    activity: {
      observed_at: new Date(Date.now() - 60000).toISOString(),
      window_hours: 24,
      finished_rounds: 8,
      failed_rounds: 1,
      unknown_rounds: 2,
    },
    resources: { cpus: 16, memory_total: 1e10, memory_available: 1e9 },
    cpu_idle_fraction: 0.2,
    memory_pressure: true,
    compiler: {
      observed_at: null,
      waiting_guests: null,
      guests: null,
      healthy: false,
    },
    slots: [],
    workers: [],
    slots_truncated: false,
    workers_truncated: false,
  };
  const frame = {
    schema: "tauceti.operations.hosts-update/v1",
    source: seed.source,
    observed_at: new Date(Date.now()).toISOString(),
    hosts: [host],
  };
  const bad = structuredClone(frame);
  bad.hosts[0].worker_memory.cgroup = "/private/cgroup";
  assert.equal((await r.request("/hosts", bad)).status, 400);
  delete bad.hosts[0].worker_memory.cgroup;
  bad.hosts[0].environment = { TOKEN: "private" };
  assert.equal((await r.request("/hosts", bad)).status, 400);
  const privateAdmission = structuredClone(frame);
  privateAdmission.hosts[0].admission.private_command = "PRIVATE";
  assert.equal((await r.request("/hosts", privateAdmission)).status, 400);
  assert.equal((await r.request("/hosts", frame)).status, 200);
  await r.flush();
  const hosts = r.object(r.manifest().domains.hosts);
  assert.equal(hosts.hosts[0].ci_enabled, false);
  assert.equal(hosts.hosts[0].connected, true);
  assert.equal(hosts.hosts[0].worker_memory.oom_kill, 2);
  assert.equal(hosts.hosts[0].worker_admission.reason, "memory_reserve");
  assert.notEqual(hosts.hosts[0].observed_at, hosts.hosts[0].transport_at);
  assert.equal((await r.request("/hosts", frame)).status, 409);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_host_samples").get().n,
    3,
  );
  // Both transports initially report no prepared host state after a restart.
  // Keep the real facts, their original time and history instead of publishing
  // an empty host until its next successful observation arrives.
  const empty = {
    ...structuredClone(host),
    observed_at: null,
    transport_at: null,
    connected: false,
    coverage: "unavailable",
    ci_enabled: null,
    resources: { cpus: null, memory_total: null, memory_available: null },
  };
  const restarted = {
    ...frame,
    source: { ...frame.source, epoch: frame.source.epoch + 1, sequence: 1 },
    hosts: [empty],
  };
  assert.equal((await r.request("/hosts", restarted)).status, 200);
  await r.flush();
  let retained = r.object(r.manifest().domains.hosts).hosts[0];
  assert.equal(retained.observed_at, host.observed_at);
  assert.equal(retained.resources.cpus, 16);
  assert.equal(retained.connected, false);
  assert.equal((await r.request("/host", frame)).status, 200);
  assert.equal((await r.request("/host", restarted)).status, 200);
  await r.flush();
  retained = r.object(r.manifest().domains.hosts).hosts[0];
  assert.equal(retained.observed_at, host.observed_at);
  assert.equal(retained.resources.cpus, 16);
  assert.equal(retained.connected, false);
  assert.equal(
    r.database
      .prepare(
        "SELECT COUNT(*) n FROM ops_host_samples WHERE resolution=15 AND json_extract(data,'$.fresh')=1",
      )
      .get().n,
    0,
    "a restart must not manufacture a fresh historical host observation",
  );
});

test("shared pilot report exposes bounded public metrics without private snapshot or outcome paths", async (t) => {
  const r = runtime(t),
    report = JSON.parse(
      readFileSync("tests/fixtures/operations-history.json", "utf8"),
    );
  const response = await r.request("/history", report);
  assert.equal(response.status, 200, await response.clone().text());
  await r.flush();
  const history = r.object(r.manifest().domains.history);
  assert.equal(history.windows.length, 3);
  assert.equal(history.windows[0].summary.cost.recorded_usd, 2.5);
  const bad = structuredClone(report);
  bad.windows[0].summary.cost.private_path = "/private/token";
  assert.equal((await r.request("/history", bad)).status, 400);
  const corrected = structuredClone(report);
  corrected.windows[0].summary.cost.recorded_usd = 9.5;
  assert.equal((await r.request("/history", corrected)).status, 200);
  r.advance(15000);
  await r.flush();
  assert.equal(
    r.object(r.manifest().domains.history).windows[0].summary.cost.recorded_usd,
    9.5,
    "an accepted corrected report with the same observation time must become public",
  );
});

test("combined large queue, job and host fixture binds exact candidate/run/attempt/runner identities", async (t) => {
  const r = runtime(t),
    frame = structuredClone(seed),
    batch = frame.changes.find(
      (c) => c.collection === "batches" && c.value.state === "head",
    ),
    template = frame.changes.find((c) => c.collection === "members").value;
  const attempt = frame.changes.find((c) => c.collection === "attempts").value;
  attempt.run_id = "100";
  attempt.run_attempt = 2;
  batch.value.size = 1200;
  frame.changes = frame.changes.filter((c) => c.collection !== "members");
  for (let i = 1; i <= 1200; i++)
    frame.changes.push({
      collection: "members",
      id: String(i),
      value: {
        ...template,
        id: String(i),
        pr: String(i),
        position: i,
        title: i === 1 ? "<img src=x onerror=alert(1)>" : "Contribution " + i,
        pr_url: `https://github.com/${repo}/pull/${i}`,
        disposition: i % 3 ? "clean_replay" : "agent_edited",
      },
    });
  const inputTemplate = frame.changes.find(
    (c) => c.collection === "inputs",
  ).value;
  for (let i = 0; i < 120; i++) {
    const value = {
      ...inputTemplate,
      id: String(10000 + i),
      pr: String(20000 + i),
      state: "waiting",
      batch_id: null,
      no_repair: i === 0,
    };
    frame.changes.push({ collection: "inputs", id: value.id, value });
  }
  frame.metadata.collection_counts.inputs += 120;
  frame.metadata.collection_counts.members = 1200;
  for (const part of split(frame, 400)) await r.send(part);
  const partition = repo + "/2026-11-06",
    machine = "a26dbb02840844f1a72450e8cc29ea28";
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () =>
        Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision: 1,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs: Array.from({ length: 1000 }, (_, i) => ({
            repository: repo,
            id: String(i + 1),
            run_id: "100",
            run_attempt: 2,
            active: true,
            status: i < 600 ? "in_progress" : "queued",
            conclusion: null,
            assigned: i < 600,
            observed_at: new Date(Date.now()).toISOString(),
            check_run_id: String(i + 10000),
            runner_id: i === 0 ? "99" : null,
            runner_name: "test-runner",
            name: "sandboxed-build",
            pool: i === 0 ? "yael" : i % 2 ? "github" : "namespace",
            job_class: "staging_build",
            labels: i === 0 ? ["fleet-" + machine] : [],
            steps: [
              {
                name: "Build exact candidate",
                started_at: new Date(Date.now() - 30000).toISOString(),
                completed_at: null,
              },
            ],
            url: `https://github.com/${repo}/actions/runs/100/job/${i + 1}`,
            progress: { coverage: "steps_only", completed_modules: null },
          })),
          pending_workflows: [],
        }),
    }),
  };
  await r.request("/partition", { partition, revision: 1 });
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = JSON.stringify([
    { machine_id: machine, alias: "yael" },
  ]);
  const host = {
    machine_id: machine,
    alias: "yael",
    observed_at: new Date(Date.now()).toISOString(),
    transport_at: new Date(Date.now()).toISOString(),
    connected: true,
    coverage: "observed",
    source_revision: "a".repeat(40),
    controller_error: false,
    ci_enabled: true,
    workers_enabled: true,
    ci_max_jobs: 1,
    worker_limit: 2,
    capacity_context: {
      observed_at: new Date(Date.now() - 120000).toISOString(),
      scope: "organization_all_workflow_jobs",
      coverage: "observed",
      observer_error: false,
      running_jobs: { github: 3, namespace: 4, "self-hosted": null },
    },
    admission: {
      ci_kinds: ["pr", "staging"],
      staging_slots: 1,
      memory_budget_gib: 64,
      reservation_error: false,
    },
    activity: {
      observed_at: new Date(Date.now() - 60000).toISOString(),
      window_hours: 24,
      finished_rounds: 8,
      failed_rounds: 1,
      unknown_rounds: 2,
    },
    resources: { cpus: 16, memory_total: 1e10, memory_available: 1e9 },
    cpu_idle_fraction: 0.2,
    memory_pressure: true,
    compiler: {
      observed_at: new Date(Date.now()).toISOString(),
      waiting_guests: 1,
      guests: 1,
      healthy: true,
    },
    slots: [
      {
        slot: 1,
        instance_id: "f".repeat(16),
        runner_id: "99",
        runner_name: "test-runner",
        phase: "running",
        busy: true,
        started_at: new Date(Date.now() - 30000).toISOString(),
        vcpus: 16,
        memory_gib: 16,
      },
    ],
    workers: [],
    slots_truncated: false,
    workers_truncated: false,
  };
  assert.equal(
    (
      await r.request("/hosts", {
        schema: "tauceti.operations.hosts-update/v1",
        source: seed.source,
        observed_at: new Date(Date.now()).toISOString(),
        hosts: [host],
      })
    ).status,
    200,
  );
  await r.request(
    "/history",
    JSON.parse(readFileSync("tests/fixtures/operations-history.json", "utf8")),
  );
  await r.flush();
  const jobs = r.object(r.manifest().domains.jobs),
    first = r.object(jobs.job_pages[0]).records[0];
  assert.equal(first.queue_binding.candidate, attempt.candidate);
  assert.equal(first.queue_binding.batch_id, batch.id);
  assert.equal(first.runner_id, "99");
  assert.equal(first.host_binding.machine_id, machine);
  assert.equal(first.host_binding.instance_id, "f".repeat(16));
  if (process.env.OPERATIONS_BROWSER_FIXTURE) {
    const { writeFileSync } = await import("node:fs");
    writeFileSync(
      process.env.OPERATIONS_BROWSER_FIXTURE,
      JSON.stringify({
        clock: Date.now(),
        objects: Object.fromEntries(r.objects),
      }),
    );
  }
});

test("mixed-repository redaction accepts null outbox and scope removal purges all retained public facts", async (t) => {
  const r = runtime(t),
    value = structuredClone(seed);
  value.metadata.outbox = null;
  await r.send(value);
  await r.flush();
  const old = r.manifest();
  assert.ok(r.object(old.domains.queue).batches.length);
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
  await r.flush();
  assert.notEqual(r.manifest().scope, old.scope);
  assert.equal(r.manifest().domains.queue.url, null);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_entities").get().n,
    0,
  );
});

test("a facts response over two megabytes is stored as individually bounded job rows", async (t) => {
  const r = runtime(t),
    partition = repo + "/2026-11-06";
  const jobs = Array.from({ length: 1000 }, (_, i) => ({
    repository: repo,
    id: String(i + 1),
    run_id: String(i + 1),
    run_attempt: 1,
    active: true,
    status: "in_progress",
    assigned: true,
    observed_at: new Date(Date.now()).toISOString(),
    labels: [],
    steps: Array.from({ length: 12 }, () => ({
      name: "x".repeat(300),
      started_at: null,
      completed_at: null,
      conclusion: null,
    })),
  }));
  const value = {
    schema: "tauceti.operations.partition/v1",
    partition,
    revision: 1,
    received_at: new Date(Date.now()).toISOString(),
    jobs,
    pending_workflows: [],
    truncated: false,
  };
  assert.ok(Buffer.byteLength(JSON.stringify(value)) > 2_000_000);
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({ fetch: async () => Response.json(value) }),
  };
  await r.request("/partition", { partition, revision: 1 });
  await r.flush();
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_job_facts").get().n,
    1000,
  );
  assert.ok(
    r.database.prepare("SELECT MAX(length(data)) n FROM ops_job_facts").get()
      .n < 10000,
  );
  assert.equal(r.object(r.manifest().domains.jobs).counts.running, 1000);
  const before = r.database.prepare("SELECT total_changes() n").get().n;
  r.advance(15000);
  value.revision = 2;
  value.received_at = new Date(Date.now()).toISOString();
  value.jobs[0] = {
    ...value.jobs[0],
    active: false,
    status: "completed",
    conclusion: "success",
  };
  value.jobs.splice(1, 1);
  await r.request("/partition", { partition, revision: 2 });
  await r.flush();
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_job_facts").get().n,
    999,
  );
  assert.equal(
    r.database.prepare("SELECT active FROM ops_job_facts WHERE id='1'").get()
      .active,
    0,
  );
  assert.equal(
    r.database
      .prepare("SELECT COUNT(*) n FROM ops_job_facts WHERE id='2'")
      .get().n,
    0,
  );
  assert.equal(r.object(r.manifest().domains.jobs).counts.running, 998);
  const writes =
    r.database.prepare("SELECT total_changes() n").get().n - before;
  assert.ok(
    writes < 100,
    `one changed and one removed job must not rewrite 1000 jobs (${writes} rows changed)`,
  );
});

test("partial member updates cannot borrow old commits to claim current-generation completeness", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const frame = r.next(),
    head = frame.changes.find(
      (c) => c.collection === "batches" && c.value.state === "head",
    );
  head.value.version++;
  const member = frame.changes.find((c) => c.collection === "members");
  member.value.batch_version = head.value.version;
  frame.changes = [head, member];
  frame.base = { epoch: 1, sequence: 1 };
  frame.metadata.collection_complete.members = false;
  frame.metadata.truncated = true;
  frame.metadata.coverage = "partial";
  await r.send(frame);
  await r.flush();
  const queue = r.object(r.manifest().domains.queue),
    index = r.object(queue.batch_shards[Number(BigInt(head.id) % 64n)]);
  const detail = r.object(index.batches.find((b) => b.id === head.id).detail);
  assert.equal(detail.member_details_complete, false);
  assert.equal(detail.member_count, 1);
  assert.equal(
    r.object(detail.member_pages[0]).records[0].batch_version,
    head.value.version,
  );
});

test("an invalid host configuration leaves queue and GitHub publication working", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = "invalid JSON";
  r.advance(15000);
  await r.send(r.next());
  await r.flush();
  assert.equal(r.object(r.manifest().domains.queue).source_sequence, 2);
  assert.equal(r.manifest().domains.hosts.error, "projection_unavailable");
  assert.equal(
    (await (await r.request("/health")).json()).publication_error,
    false,
  );
});

test("quiet heartbeats do not rewrite every reachable object timestamp", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const before = r.database
    .prepare("SELECT hash,referenced FROM ops_objects")
    .all();
  r.advance(10000);
  const heartbeat = r.next();
  heartbeat.changes = [];
  heartbeat.base = { epoch: seed.source.epoch, sequence: seed.source.sequence };
  await r.send(heartbeat);
  await r.flush();
  for (const row of before) {
    const after = r.database
      .prepare("SELECT referenced FROM ops_objects WHERE hash=?")
      .get(row.hash);
    assert.equal(after.referenced, row.referenced);
  }
});

test("canonical SHA and recorded token coverage accept only their public contracts", async (t) => {
  const r = runtime(t),
    frame = structuredClone(seed);
  frame.metadata.canonical_sha = "a".repeat(40);
  frame.metadata.dispatch_config = {
    repository: repo,
    revision: frame.metadata.canonical_sha,
    namespace_build_cap: 20,
    pr_order: ["github", "chris", "namespace"],
    staging_order: ["chris", "namespace", "github"],
  };
  const wrongRevision = structuredClone(frame);
  wrongRevision.metadata.dispatch_config.revision = "b".repeat(40);
  assert.equal((await r.request("/queue", wrongRevision)).status, 400);
  const privatePolicy = structuredClone(frame);
  privatePolicy.metadata.dispatch_config.environment = { TOKEN: "PRIVATE" };
  assert.equal((await r.request("/queue", privatePolicy)).status, 400);
  const task = {
    id: "99",
    kind: "repair",
    batch_id: "1",
    batch_version: 1,
    state: "open",
    resolution: "",
    ci_attempt_id: null,
    created_at: seed.metadata.observed_at,
    resolved_at: null,
    progress: {
      coverage: "observed",
      state: "agent_running",
      observed_at: seed.metadata.observed_at,
      seconds: 10,
      providers: ["codex"],
      models: ["gpt-6.1"],
      cost: {
        recorded_usd: 1,
        recorded_attempts: 1,
        unknown_attempts: 0,
        coverage: "recorded_subtotal_only",
      },
      usage: {
        input_tokens: {
          recorded_tokens: 100,
          recorded_attempts: 1,
          unknown_attempts: 0,
        },
        output_tokens: {
          recorded_tokens: 0,
          recorded_attempts: 0,
          unknown_attempts: 1,
        },
      },
    },
  };
  frame.changes.push({ collection: "tasks", id: "99", value: task });
  const bad = structuredClone(frame);
  bad.metadata.canonical_sha = "PRIVATE_PATH";
  await r.send(bad, 400);
  const secret = structuredClone(frame);
  secret.changes.at(-1).value.progress.usage.private_prompt = "PRIVATE";
  await r.send(secret, 400);
  await r.send(frame);
  await r.flush();
  assert.equal(
    r.object(r.manifest().domains.queue).canonical_sha,
    "a".repeat(40),
  );
});

test("GitHub event time stays separate from fact receipt time", async (t) => {
  const r = runtime(t),
    partition = repo + "/2027-01-15";
  const event = new Date(Date.now() - 120000).toISOString(),
    receipt = new Date(Date.now()).toISOString();
  globalThis.__operationsTestEnv.CONTROL = {
    getByName: () => ({
      fetch: async () => Response.json({ partitions: [], inventory: [] }),
    }),
  };
  r.database.prepare("INSERT INTO ops_partitions VALUES (?,?,?,?,?)").run(
    partition,
    1,
    0,
    Math.floor(Date.now() / 1000),
    JSON.stringify({
      received_at: receipt,
      pending_workflows: [
        {
          repository: repo,
          run_id: "1",
          run_attempt: 1,
          status: "queued",
          workflow: ".github/workflows/pr-build.yml",
          observed_at: event,
        },
      ],
      pending_workflows_count: 1,
    }),
  );
  await r.flush();
  const root = r.object(r.manifest().domains.jobs);
  assert.equal(root.observed_at, event);
  assert.equal(root.received_at, receipt);
  assert.equal(root.observer.last_event_at, event);
});

test("build, control and maintenance are separate monitoring classes", async () => {
  const code = await build({
    entryPoints: ["src/operations-jobs.ts"],
    bundle: true,
    format: "esm",
    platform: "node",
    write: false,
  });
  const { publicJob } = await import(
    "data:text/javascript;base64," +
      Buffer.from(code.outputFiles[0].text).toString("base64")
  );
  const run = {
    id: "1",
    repo,
    attempt: 1,
    workflow: ".github/workflows/pr-build.yml",
    trigger: "pr",
    status: "in_progress",
    head: "a".repeat(40),
  };
  const job = {
    id: "2",
    attempt: 1,
    name: "sandboxed-build",
    kind: "github",
    status: "in_progress",
    steps: [],
    labels: [],
    created: 1,
    updated: 2,
  };
  assert.equal(publicJob(run, job).job_class, "pr_build");
  assert.equal(
    publicJob(run, { ...job, name: "pick-runner" }).job_class,
    "control",
  );
  assert.equal(
    publicJob({ ...run, workflow: ".github/workflows/live-ci-tests.yml" }, job)
      .job_class,
    "maintenance",
  );
  assert.equal(
    publicJob({ ...run, workflow: "unknown.yml" }, job).job_class,
    "other",
  );
});

// Pump only observation time and the DO alarm; never settle a deliberately held
// unrelated operation to make an assertion pass.
async function pumpUntil(r, predicate) {
  for (let i = 0; i < 80; i++) {
    if (predicate()) return;
    r.advance(1000);
    await r.fire();
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.ok(
    predicate(),
    "expected public state did not advance while unrelated work was held",
  );
}
const queueSequence = (r) =>
  r.object(r.manifest().domains.queue).source_sequence;
for (const heldLane of ["facts", "jobs-root"])
  test(`queue advances twice while ${heldLane} stays pending`, async (t) => {
    const r = runtime(t);
    await r.send(seed);
    await r.flush();
    const oldJobs = r.manifest().domains.jobs.url,
      partition = repo + "/2026-11-06";
    let release,
      enter,
      active = 0,
      peak = 0,
      pointers = 0,
      pointerPeak = 0;
    const gate = new Promise((resolve) => {
        release = resolve;
      }),
      entered = new Promise((resolve) => {
        enter = resolve;
      });
    globalThis.__operationsTestEnv.FACTS = {
      idFromName: (id) => id,
      get: () => ({
        fetch: async () => {
          if (heldLane === "facts") {
            enter();
            await gate;
          }
          return Response.json({
            schema: "tauceti.operations.partition/v1",
            partition,
            revision: 1,
            received_at: new Date(Date.now()).toISOString(),
            truncated: false,
            pending_workflows: [],
            jobs: [
              {
                repository: repo,
                id: "1",
                run_id: "100",
                run_attempt: 1,
                active: true,
                status: "in_progress",
                assigned: true,
                observed_at: new Date(Date.now()).toISOString(),
              },
            ],
          });
        },
      }),
    };
    const archive = globalThis.__operationsTestEnv.ARCHIVE,
      put = archive.put;
    archive.put = async (key, value) => {
      const body = JSON.parse(value),
        pointer = key.endsWith("/manifest.json");
      active++;
      peak = Math.max(peak, active);
      if (pointer) {
        pointers++;
        pointerPeak = Math.max(pointerPeak, pointers);
      }
      try {
        if (
          heldLane === "jobs-root" &&
          body.schema === "tauceti.operations.jobs/v1"
        ) {
          enter();
          await gate;
        }
        await new Promise((resolve) => setImmediate(resolve));
        return await put(key, value);
      } finally {
        active--;
        if (pointer) pointers--;
      }
    };
    r.advance(15000);
    await r.send(r.next(2));
    await r.request("/partition", { partition, revision: 1 });
    await r.fire();
    try {
      await entered;
      await pumpUntil(r, () => queueSequence(r) === 2);
      assert.equal(r.manifest().domains.jobs.url, oldJobs);
      await r.send(r.next(3));
      await pumpUntil(r, () => queueSequence(r) === 3);
      assert.equal(r.manifest().domains.jobs.url, oldJobs);
      assert.ok(
        peak <= 4,
        `all puts including pointer stay within four: ${peak}`,
      );
      assert.equal(pointerPeak, 1);
    } finally {
      release();
      await r.drain();
    }
    await pumpUntil(
      r,
      () => r.object(r.manifest().domains.jobs).counts.running === 1,
    );
    assert.equal(queueSequence(r), 3);
  });

test("withdrawing scope during a queue upload cannot relabel old payloads or restore old prepared caches", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const oldScope = r.manifest().scope;
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  archive.put = async (key, value) => {
    const body = JSON.parse(value);
    if (
      body.schema === "tauceti.operations.queue/v1" &&
      body.source_sequence === 2
    ) {
      enter();
      await gate;
    }
    return put(key, value);
  };
  r.advance(15000);
  await r.send(r.next(2));
  await r.fire();
  try {
    await entered;
    globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
    await r.fire();
  } finally {
    release();
    await r.drain();
  }
  await pumpUntil(
    r,
    () =>
      r.manifest().scope !== oldScope &&
      r.manifest().domains.queue.url === null,
  );
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_views").get().n,
    0,
  );
  const scope = r.manifest().scope;
  for (const [key, value] of r.objects)
    if (key.includes("/objects/" + scope + "/"))
      assert.ok(
        !value.includes(repo),
        "withdrawn repository never appears in the new object namespace",
      );
});

test("ready queue survives pointer failure and publisher restart without rebuilding unchanged objects", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  r.advance(15000);
  r.failPointer();
  await r.send(r.next(2));
  await r.drain();
  const puts = r.writes.filter((key) => !key.endsWith("/manifest.json")).length;
  assert.equal(queueSequence(r), 1);
  const restored = new Operations(r.ctx);
  r.advance(15000);
  await restored.alarm();
  await r.drain();
  assert.equal(queueSequence(r), 2);
  assert.equal(
    r.writes.filter((key) => !key.endsWith("/manifest.json")).length,
    puts,
  );
});

test("a held pointer acknowledges its captured queue and leaves the newer ready generation pending", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  let enter,
    release,
    pointers = 0,
    peak = 0;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  let held = false;
  archive.put = async (key, value) => {
    if (!key.endsWith("/manifest.json")) return put(key, value);
    pointers++;
    peak = Math.max(peak, pointers);
    try {
      if (!held) {
        held = true;
        enter();
        await gate;
      }
      return await put(key, value);
    } finally {
      pointers--;
    }
  };
  r.advance(15000);
  const second = r.next(2),
    third = r.next(3);
  await r.send(second);
  await r.fire();
  try {
    await pumpUntil(r, () => held);
    await entered;
    await r.send(third);
    await pumpUntil(
      r,
      () => r.operations.state("queue").version === "3/" + third.snapshot,
    );
    assert.equal(
      queueSequence(r),
      1,
      "a pending pointer write has not acknowledged anything",
    );
    assert.equal(peak, 1, "mutable-pointer puts never overlap");
  } finally {
    release();
    await r.drain();
  }
  assert.equal(
    queueSequence(r),
    3,
    "the completion kick consumes the newer ready generation without another event",
  );
  assert.equal(r.operations.state("queue").version, "3/" + third.snapshot);
  await pumpUntil(r, () => queueSequence(r) === 3);
  assert.equal(peak, 1);
});

test("scope withdrawal fences a delayed Facts response instead of repopulating withdrawn jobs", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const partition = repo + "/2026-11-06";
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () => {
        enter();
        await gate;
        return Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision: 1,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs: [
            {
              id: "1",
              repository: repo,
              run_id: "100",
              run_attempt: 1,
              active: true,
              observed_at: new Date(Date.now()).toISOString(),
            },
          ],
          pending_workflows: [],
        });
      },
    }),
  };
  r.advance(15000);
  await r.request("/partition", { partition, revision: 1 });
  await r.fire();
  try {
    await entered;
    globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
    await r.fire();
  } finally {
    release();
    await r.drain();
  }
  await pumpUntil(
    r,
    () =>
      r.manifest().domains.jobs.url &&
      r.object(r.manifest().domains.jobs).scope.repositories.length === 0,
  );
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_job_facts").get().n,
    0,
  );
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_partitions").get().n,
    0,
  );
});

test("withdrawn host history cannot restore its cleared cache after its upload finishes", async (t) => {
  const r = runtime(t),
    machine = "a".repeat(32);
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = JSON.stringify([
    { machine_id: machine, alias: "yael" },
  ]);
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  archive.put = async (key, value) => {
    const body = JSON.parse(value);
    if (
      body.schema === "tauceti.operations.host-history/v1" &&
      body.hosts.length
    ) {
      enter();
      await gate;
    }
    return put(key, value);
  };
  await r.send(seed);
  await r.fire();
  try {
    await entered;
    globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = "[]";
    await r.fire();
  } finally {
    release();
    await r.drain();
  }
  await pumpUntil(r, () => {
    if (!r.objects.has("public/v1/operations/manifest.json")) return false;
    const ref = r.manifest().domains.host_history;
    return ref.url && r.object(ref).hosts.length === 0;
  });
  assert.equal(r.operations.get("host_history"), null);
  const scope = r.manifest().scope;
  for (const [key, value] of r.objects)
    if (key.includes("/objects/" + scope + "/"))
      assert.ok(
        !value.includes(machine),
        "old host facts stay out of the new namespace",
      );
});

for (const deliverAlarms of [true, false])
  test(`workerd publishes successive generations with jobs held and alarm delivery ${deliverAlarms ? "enabled" : "disabled"}`, async (t) => {
    const bundled = await build({
      stdin: {
        resolveDir: process.cwd(),
        contents: `
    import { Operations as BaseOperations } from "./src/operations.ts";
    export class Operations extends BaseOperations {async alarm() {${deliverAlarms ? "await super.alarm();" : ""}}}
    export default { fetch(request, env) {
      return env.OPERATIONS.get(env.OPERATIONS.idFromName("operations")).fetch(request);
    }};`,
        loader: "js",
      },
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      plugins: [
        {
          name: "hold-one-observer-upload",
          setup(builder) {
            builder.onResolve({ filter: /^cloudflare:workers$/ }, (args) =>
              args.namespace === "held-bindings"
                ? { path: "cloudflare:workers", external: true }
                : { path: "env", namespace: "held-bindings" },
            );
            builder.onLoad(
              { filter: /.*/, namespace: "held-bindings" },
              () => ({
                contents: `
        import { env as actual } from "cloudflare:workers";
        export const env = new Proxy(actual, {get(target, key) {
          if (key !== "ARCHIVE") return target[key];
          return { async put(key, value, options) {
            if (JSON.parse(value).schema === "tauceti.operations.jobs/v1") await fetch("https://hold.example/jobs");
            return target.ARCHIVE.put(key, value, options);
          }, delete(key) {return target.ARCHIVE.delete(key);} };
        }});`,
              }),
            );
          },
        },
      ],
    });
    let release,
      entered = false;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    const mf = new Miniflare(
      convertV4MiniflareOptions({
        modules: true,
        script: bundled.outputFiles[0].text,
        compatibilityDate: "2026-10-15",
        durableObjects: {
          OPERATIONS: { className: "Operations", useSQLite: true },
        },
        r2Buckets: ["ARCHIVE"],
        bindings: { OPERATIONS_PUBLIC_REPOSITORIES: JSON.stringify([repo]) },
        outboundService: async (request) => {
          assert.equal(new URL(request.url).hostname, "hold.example");
          entered = true;
          await gate;
          return new Response("released");
        },
      }),
    );
    t.after(async () => {
      release();
      await mf.dispose();
    });
    const bucket = await mf.getR2Bucket("ARCHIVE"),
      seen = [];
    async function waitQueue(sequence) {
      for (let i = 0; i < 600; i++) {
        const object = await bucket.get("public/v1/operations/manifest.json");
        if (object) {
          const manifest = await object.json(),
            ref = manifest.domains.queue;
          if (ref?.url) {
            const queue = await (await bucket.get("public" + ref.url)).json();
            if (queue.source_sequence === sequence) {
              seen.push(sequence);
              return;
            }
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      assert.fail(
        `real alarm did not publish queue ${sequence} while jobs put was held`,
      );
    }
    const first = structuredClone(seed);
    assert.equal(
      (
        await mf.dispatchFetch("https://internal/queue", {
          method: "POST",
          body: JSON.stringify(first),
        })
      ).status,
      200,
    );
    await waitQueue(1);
    assert.equal(entered, true);
    if (!deliverAlarms)
      await new Promise((resolve) => setTimeout(resolve, 11000));
    const next = structuredClone(seed);
    next.source.sequence = 2;
    next.snapshot = crypto.randomUUID();
    next.metadata.observed_at = new Date().toISOString();
    assert.equal(
      (
        await mf.dispatchFetch("https://internal/queue", {
          method: "POST",
          body: JSON.stringify(next),
        })
      ).status,
      200,
    );
    await waitQueue(2);
    assert.deepEqual(
      seen,
      [1, 2],
      "actual background lifetime and future alarms remain independent of jobs",
    );
    assert.equal(
      (await mf.dispatchFetch("https://internal/health", { method: "POST" }))
        .status,
      200,
    );
  });

test("an ambiguous pointer write retries the latest durable union instead of restoring the old committed graph", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  let fail = true;
  archive.put = async (key, value) => {
    await put(key, value);
    if (fail && key.endsWith("/manifest.json")) {
      fail = false;
      throw new Error("lost acknowledgment after storage accepted pointer");
    }
  };
  r.advance(15000);
  await r.send(r.next(2));
  await r.drain();
  assert.equal(
    queueSequence(r),
    2,
    "ambiguous put may have reached public storage",
  );
  assert.equal(
    r.object(r.operations.state("queue", false).value).source_sequence,
    1,
    "no acknowledgment was invented",
  );
  const restored = new Operations(r.ctx),
    third = r.next(3);
  r.advance(15000);
  await r.send(third);
  await restored.alarm();
  await r.drain();
  r.advance(10000);
  await restored.alarm();
  await r.drain();
  assert.equal(queueSequence(r), 3);
  assert.equal(restored.state("queue", false).version, "3/" + third.snapshot);
});

test("an old source epoch finishing its upload cannot restore its prepared queue cache", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  archive.put = async (key, value) => {
    const body = JSON.parse(value);
    if (
      body.schema === "tauceti.operations.queue/v1" &&
      body.source_epoch === 1 &&
      body.source_sequence === 2
    ) {
      enter();
      await gate;
    }
    return put(key, value);
  };
  r.advance(15000);
  const old = r.next(2);
  await r.send(old);
  await r.fire();
  const newer = r.next(1, 2);
  try {
    await entered;
    await r.send(newer);
  } finally {
    release();
    await r.drain();
  }
  assert.notEqual(r.operations.state("queue").version, "3/" + old.snapshot);
  await pumpUntil(
    r,
    () => r.object(r.manifest().domains.queue).source_epoch === 2,
  );
  assert.equal(queueSequence(r), 1);
});

test("background GC rechecks stale candidates reused while an earlier delete was pending", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const report = JSON.parse(
      readFileSync("tests/fixtures/operations-history.json", "utf8"),
    ),
    choices = [];
  for (const cost of [8.5, 9.5]) {
    const value = structuredClone(report);
    value.windows[0].summary.cost.recorded_usd = cost;
    const ref = await r.operations.object(
      value,
      r.operations.projection("history"),
    );
    choices.push({ value, ref });
    r.database
      .prepare("UPDATE ops_objects SET referenced=? WHERE hash=?")
      .run(
        Math.floor(Date.now() / 1000) - 8 * 86400,
        ref.url.slice("/v1/operations/objects/".length, -5),
      );
  }
  let enter, release, deleting;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    remove = archive.delete;
  archive.delete = async (key) => {
    if (!deleting) {
      deleting = key;
      enter();
      await gate;
    }
    return remove(key);
  };
  r.operations.set("maintenance_at", 0);
  r.operations.startMaintenance();
  let target;
  try {
    await entered;
    target = choices.find((c) => "public" + c.ref.url !== deleting);
    assert.ok(target);
    await r.request("/history", target.value);
    await pumpUntil(
      r,
      () => r.manifest().domains.history.url === target.ref.url,
    );
    assert.ok(r.objects.has("public" + target.ref.url));
  } finally {
    release();
    await r.drain();
  }
  assert.ok(
    r.objects.has("public" + target.ref.url),
    "GC cannot delete the newly public root from its stale candidate list",
  );
});

test("a large retained runner map cannot exceed a publication version row or block queue updates", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const insert = r.database.prepare(
    "INSERT INTO ops_runner_hosts VALUES (?,?,?,?,?)",
  );
  r.ctx.storage.transactionSync(() => {
    for (let i = 0; i < 20000; i++)
      insert.run(
        "runner-" + i,
        "a".repeat(32),
        "b".repeat(32),
        i % 4,
        Math.floor(Date.now() / 1000),
      );
    r.operations.set("jobs_revision", 20000);
  });
  r.advance(15000);
  await r.send(r.next(2));
  await r.flush();
  assert.equal(queueSequence(r), 2);
  assert.ok(
    Buffer.byteLength(r.operations.get("domain_target/jobs").version) < 100,
  );
});

test("runner mapping slot changes and pruning dirty job bindings without dirtying on a repeated observation", async (t) => {
  const r = runtime(t),
    host = {
      machine_id: "a".repeat(32),
      connected: false,
      observed_at: null,
      slots: [{ runner_id: "1", instance_id: "b".repeat(32), slot: 0 }],
    };
  r.operations.recordHost(host);
  const inserted = r.operations.targets().jobs;
  r.operations.recordHost(host);
  assert.equal(r.operations.targets().jobs, inserted);
  host.slots[0].slot = 1;
  r.operations.recordHost(host);
  const changed = r.operations.targets().jobs;
  assert.notEqual(changed, inserted);
  r.advance(31 * 86400 * 1000);
  host.slots = [];
  r.operations.recordHost(host);
  assert.notEqual(r.operations.targets().jobs, changed);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_runner_hosts").get().n,
    0,
  );
});

test("a retired physical key cannot delete a recreated history object after publisher restart", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const report = JSON.parse(
    readFileSync("tests/fixtures/operations-history.json", "utf8"),
  );
  const old = await r.operations.object(
    report,
    r.operations.projection("history"),
  );
  r.database
    .prepare("UPDATE ops_objects SET referenced=? WHERE hash=?")
    .run(
      Math.floor(Date.now() / 1000) - 8 * 86400,
      old.url.slice("/v1/operations/objects/".length, -5),
    );
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    remove = archive.delete;
  archive.delete = async (key) => {
    if (key === "public" + old.url) {
      enter();
      await gate;
    }
    return remove(key);
  };
  r.operations.set("maintenance_at", 0);
  r.operations.startMaintenance();
  let recreated;
  try {
    await entered;
    const restored = new Operations(r.ctx);
    assert.equal(
      (
        await restored.fetch(
          new Request("https://internal/history", {
            method: "POST",
            body: JSON.stringify(report),
          }),
        )
      ).status,
      200,
    );
    for (let i = 0; i < 80; i++) {
      r.advance(1000);
      await restored.alarm();
      await new Promise((resolve) => setImmediate(resolve));
      const ref = r.manifest().domains.history;
      if (ref.url) {
        recreated = ref;
        break;
      }
    }
    assert.ok(recreated);
    assert.notEqual(
      recreated.url,
      old.url,
      "reconstruction uses a new immutable incarnation",
    );
    assert.ok(r.objects.has("public" + recreated.url));
  } finally {
    release();
    await r.drain();
  }
  assert.ok(
    r.objects.has("public" + recreated.url),
    "completion of the old external delete cannot touch the new root",
  );
  assert.equal(r.objects.has("public" + old.url), false);
});

test("an ambiguous immutable put retries its durable pending allocation after restart", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const report = JSON.parse(
      readFileSync("tests/fixtures/operations-history.json", "utf8"),
    ),
    keys = [];
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  let first = true;
  archive.put = async (key, value) => {
    if (JSON.parse(value).schema !== report.schema) return put(key, value);
    keys.push(key);
    await put(key, value);
    if (first) {
      first = false;
      throw new Error("put acknowledgment lost");
    }
  };
  await assert.rejects(
    r.operations.object(report, r.operations.projection("history")),
  );
  const restored = new Operations(r.ctx),
    ref = await restored.object(report, restored.projection("history"));
  assert.equal(keys.length, 2);
  assert.equal(keys[0], keys[1], "retry does not allocate another orphan");
  assert.equal(keys[1], "public" + ref.url);
});

test("live legacy content-hash receipts are reused without rewriting deployed objects", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const report = JSON.parse(
      readFileSync("tests/fixtures/operations-history.json", "utf8"),
    ),
    data = JSON.stringify(report);
  const hash =
    r.operations.get("public_scope") +
    "/" +
    Buffer.from(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(data)),
    ).toString("hex");
  const ref = { url: "/v1/operations/objects/" + hash + ".json" };
  r.objects.set("public" + ref.url, data);
  r.database
    .prepare("INSERT INTO ops_objects VALUES (?,?)")
    .run(hash, Math.floor(Date.now() / 1000));
  const writes = r.writes.length;
  assert.deepEqual(
    await r.operations.object(report, r.operations.projection("history")),
    ref,
  );
  assert.equal(r.writes.length, writes);
});

test("abandoned pending allocations and ambiguous orphan puts follow bounded retention cleanup", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const report = JSON.parse(
    readFileSync("tests/fixtures/operations-history.json", "utf8"),
  );
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  let orphan;
  archive.put = async (key, value) => {
    await put(key, value);
    if (JSON.parse(value).schema === report.schema) {
      orphan = key;
      throw new Error("lost put acknowledgment");
    }
  };
  await assert.rejects(
    r.operations.object(report, r.operations.projection("history")),
  );
  assert.ok(r.objects.has(orphan));
  r.database
    .prepare("UPDATE ops_object_keys SET allocated=? WHERE hash=?")
    .run(
      Math.floor(Date.now() / 1000) - 8 * 86400,
      orphan.slice("public/v1/operations/objects/".length, -5),
    );
  r.operations.set("maintenance_at", 0);
  r.operations.startMaintenance();
  await r.drain();
  assert.equal(r.objects.has(orphan), false);
  assert.equal(
    r.database
      .prepare("SELECT COUNT(*) n FROM ops_object_keys WHERE hash=?")
      .get(orphan.slice("public/v1/operations/objects/".length, -5)).n,
    0,
  );
});

test("completed build history survives a skipped-control burst with honest bounded coverage", async (t) => {
  const r = runtime(t),
    partition = repo + "/2026-11-06";
  const job = (id, kind, seconds, conclusion) => ({
    repository: repo,
    id: String(id),
    run_id: String(id),
    run_attempt: 1,
    active: false,
    status: "completed",
    conclusion,
    job_class: kind,
    observed_at: new Date(Date.now() - seconds * 1000).toISOString(),
    completed_at: new Date(Date.now() - seconds * 1000).toISOString(),
  });
  const jobs = [
    job(1, "pr_build", 600, "success"),
    ...Array.from({ length: 250 }, (_, i) =>
      job(1000 + i, "control", i, "skipped"),
    ),
  ];
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () =>
        Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision: 1,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs,
          pending_workflows: [],
        }),
    }),
  };
  await r.request("/partition", { partition, revision: 1 });
  await r.flush();
  const root = r.object(r.manifest().domains.jobs),
    published = root.job_pages.flatMap((ref) => r.object(ref).records);
  assert.equal(published.filter((j) => j.job_class === "pr_build").length, 1);
  assert.equal(root.truncated, true, "terminal overflow must be declared");
});

test("active saturation and thousands of controls keep bounded build history and repository privacy", async (t) => {
  const r = runtime(t),
    partition = repo + "/2026-11-06";
  const job = (id, active, kind, seconds, repository = repo) => ({
    repository,
    id: String(id),
    run_id: String(id),
    run_attempt: 1,
    active,
    status: active ? "in_progress" : "completed",
    assigned: active,
    conclusion: active ? null : "success",
    job_class: kind,
    observed_at: new Date(Date.now() - seconds * 1000).toISOString(),
  });
  const jobs = [
    ...Array.from({ length: 2001 }, (_, i) => job(i, true, "pr_build", i)),
    ...Array.from({ length: 3000 }, (_, i) =>
      job(10000 + i, false, "control", i),
    ),
    job(5000, false, "pr_build", 4000),
    job(99999, true, "pr_build", 0, "PrivateOrg/Hidden"),
    job(99998, false, "pr_build", 0, "PrivateOrg/Hidden"),
  ];
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () =>
        Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision: 1,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs,
          pending_workflows: [],
        }),
    }),
  };
  await r.request("/partition", { partition, revision: 1 });
  await r.flush();
  const root = r.object(r.manifest().domains.jobs),
    published = root.job_pages.flatMap((ref) => r.object(ref).records);
  assert.equal(root.counts.active, 2001);
  assert.equal(published.filter((j) => j.active).length, 2000);
  assert.ok(published.some((j) => j.id === "5000"));
  assert.ok(published.every((j) => j.repository === repo));
  assert.ok(published.length <= 2200);
  assert.ok(root.job_pages.length <= 22);
  assert.deepEqual(root.recent_completed, {
    builds: { limit: 150, returned: 1, truncated: false },
    other: { limit: 50, returned: 50, truncated: true },
  });
  assert.equal(root.truncated, true);
  assert.equal(root.active_truncated, true);
  assert.equal(root.source_truncated, false);
});

test("completed cohort exact boundaries and legacy classes preserve truthful truncation", async (t) => {
  const r = runtime(t),
    partition = repo + "/2026-11-06";
  let revision = 1;
  const job = (id, kind) => ({
    repository: repo,
    id: String(id),
    run_id: String(id),
    run_attempt: 1,
    active: false,
    status: "completed",
    conclusion: "success",
    job_class: kind,
    observed_at: new Date(Date.now() - id * 1000).toISOString(),
  });
  const jobs = [
    ...Array.from({ length: 150 }, (_, i) => job(i, "staging_build")),
    ...Array.from({ length: 50 }, (_, i) =>
      job(1000 + i, i < 20 ? null : i < 40 ? undefined : "legacy_unknown"),
    ),
  ];
  globalThis.__operationsTestEnv.FACTS = {
    idFromName: (id) => id,
    get: () => ({
      fetch: async () =>
        Response.json({
          schema: "tauceti.operations.partition/v1",
          partition,
          revision,
          received_at: new Date(Date.now()).toISOString(),
          truncated: false,
          jobs,
          pending_workflows: [],
        }),
    }),
  };
  await r.request("/partition", { partition, revision });
  await r.flush();
  let root = r.object(r.manifest().domains.jobs);
  assert.equal(root.truncated, false);
  assert.deepEqual(root.recent_completed, {
    builds: { limit: 150, returned: 150, truncated: false },
    other: { limit: 50, returned: 50, truncated: false },
  });
  jobs.push(job(5000, "pr_build"), job(5001, "control"));
  revision++;
  r.advance(15000);
  await r.request("/partition", { partition, revision });
  await r.flush();
  root = r.object(r.manifest().domains.jobs);
  assert.equal(root.truncated, true);
  assert.deepEqual(root.recent_completed, {
    builds: { limit: 150, returned: 150, truncated: true },
    other: { limit: 50, returned: 50, truncated: true },
  });
  assert.equal(
    root.job_pages.flatMap((ref) => r.object(ref).records).length,
    200,
  );
});

test("warm arrivals publish without waiting for a delayed alarm", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  r.advance(15000);
  await r.send(r.next(2));
  await r.drain();
  assert.equal(
    queueSequence(r),
    2,
    "no alarm was delivered for the arriving frame",
  );
});

test(
  "a newer queue target arriving during held preparation is consumed by completion kicks",
  { timeout: 5000 },
  async (t) => {
    let releaseQueue,
      releasePointer,
      queueEntered,
      pointerEntered,
      thirdUploaded;
    const queueGate = new Promise((resolve) => {
        releaseQueue = resolve;
      }),
      pointerGate = new Promise((resolve) => {
        releasePointer = resolve;
      });
    const queueHeld = new Promise((resolve) => {
        queueEntered = resolve;
      }),
      pointerHeld = new Promise((resolve) => {
        pointerEntered = resolve;
      }),
      thirdPut = new Promise((resolve) => {
        thirdUploaded = resolve;
      });
    t.signal.addEventListener("abort", () => {
      releaseQueue();
      releasePointer();
    });
    const r = runtime(t);
    await r.send(seed);
    await r.flush();
    const archive = globalThis.__operationsTestEnv.ARCHIVE,
      put = archive.put;
    archive.put = async (key, value) => {
      const body = JSON.parse(value);
      if (
        body.schema === "tauceti.operations.queue/v1" &&
        body.source_sequence === 2
      ) {
        queueEntered();
        await queueGate;
      }
      if (key.endsWith("/manifest.json") && body.domains.queue?.url) {
        const root = r.object(body.domains.queue);
        if (root.source_sequence === 2) {
          pointerEntered();
          await pointerGate;
        }
      }
      const result = await put(key, value);
      if (
        body.schema === "tauceti.operations.queue/v1" &&
        body.source_sequence === 3
      )
        thirdUploaded();
      return result;
    };
    try {
      r.advance(15000);
      await r.send(r.next(2));
      await queueHeld;
      r.advance(15000);
      const third = r.next(3);
      await r.send(third);
      r.advance(15000);
      releaseQueue();
      await pointerHeld;
      await thirdPut;
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(r.operations.state("queue").version, "3/" + third.snapshot);
      r.advance(15000);
      releasePointer();
      await r.drain();
      assert.equal(
        queueSequence(r),
        3,
        "no further source arrival or alarm was needed",
      );
    } finally {
      releaseQueue();
      releasePointer();
      await r.drain();
    }
  },
);

test("cold cheap domains wait for queue readiness while an explicit alarm can bootstrap a partial view", async (t) => {
  const r = runtime(t);
  let enter, release;
  const entered = new Promise((resolve) => {
      enter = resolve;
    }),
    gate = new Promise((resolve) => {
      release = resolve;
    });
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put;
  archive.put = async (key, value) => {
    if (JSON.parse(value).schema === "tauceti.operations.queue/v1") {
      enter();
      await gate;
    }
    return put(key, value);
  };
  try {
    await r.send(seed);
    await entered;
    for (let i = 0; i < 20; i++)
      await new Promise((resolve) => setImmediate(resolve));
    assert.equal(r.objects.has("public/v1/operations/manifest.json"), false);
    await r.fire();
    for (
      let i = 0;
      i < 20 && !r.objects.has("public/v1/operations/manifest.json");
      i++
    )
      await new Promise((resolve) => setImmediate(resolve));
    assert.equal(
      r.manifest().domains.queue.url,
      null,
      "explicit alarm permission allows honest partial bootstrap",
    );
  } finally {
    release();
    await r.drain();
  }
  r.advance(10000);
  await r.fire();
  await r.drain();
  assert.equal(queueSequence(r), 1);
});

test("an event pump after restart does not mistake a deferred queue for prepared state", async (t) => {
  const r = runtime(t),
    kick = r.operations.nudge;
  r.operations.nudge = () => {};
  await r.send(seed);
  r.operations.nudge = kick;
  r.operations.set("domain_started/queue", Date.now());
  const restored = new Operations(r.ctx);
  await restored.publish();
  await r.drain();
  assert.equal(restored.preparing.has("queue"), false);
  assert.equal(
    r.objects.has("public/v1/operations/manifest.json"),
    false,
    "there is no current-scope committed or ready queue",
  );
  await restored.alarm();
  await r.drain();
  assert.equal(r.manifest().domains.queue.url, null);
  r.advance(10000);
  await restored.publish();
  await r.drain();
  assert.equal(queueSequence(r), 1);
});

test(
  "an alarm bootstrap request survives coalescing with event-only kicks",
  { timeout: 5000 },
  async (t) => {
    let releaseScope, releaseQueue, scopeEntered, queueEntered;
    const scopeGate = new Promise((resolve) => {
        releaseScope = resolve;
      }),
      queueGate = new Promise((resolve) => {
        releaseQueue = resolve;
      });
    const scopeHeld = new Promise((resolve) => {
        scopeEntered = resolve;
      }),
      queueHeld = new Promise((resolve) => {
        queueEntered = resolve;
      });
    t.signal.addEventListener("abort", () => {
      releaseScope();
      releaseQueue();
    });
    const r = runtime(t),
      sync = r.operations.syncScope.bind(r.operations);
    let once = true;
    r.operations.syncScope = async () => {
      await sync();
      if (once) {
        once = false;
        scopeEntered();
        await scopeGate;
      }
    };
    const archive = globalThis.__operationsTestEnv.ARCHIVE,
      put = archive.put;
    archive.put = async (key, value) => {
      if (JSON.parse(value).schema === "tauceti.operations.queue/v1") {
        queueEntered();
        await queueGate;
      }
      return put(key, value);
    };
    let alarm;
    try {
      await r.send(seed);
      await scopeHeld;
      alarm = r.fire();
      r.operations.nudge();
      releaseScope();
      await alarm;
      await queueHeld;
      for (
        let i = 0;
        i < 40 && !r.objects.has("public/v1/operations/manifest.json");
        i++
      )
        await new Promise((resolve) => setImmediate(resolve));
      assert.ok(r.objects.has("public/v1/operations/manifest.json"));
      assert.equal(r.manifest().domains.queue.url, null);
    } finally {
      releaseScope();
      releaseQueue();
      if (alarm) await alarm;
      await r.drain();
    }
  },
);

test(
  "a large committed snapshot is acknowledged while publication uploads remain held",
  { timeout: 5000 },
  async (t) => {
    let release, enter;
    const gate = new Promise((resolve) => {
        release = resolve;
      }),
      entered = new Promise((resolve) => {
        enter = resolve;
      });
    t.signal.addEventListener("abort", release);
    const r = runtime(t),
      frame = structuredClone(seed),
      template = frame.changes.find((c) => c.collection === "members").value;
    frame.changes = frame.changes.filter((c) => c.collection !== "members");
    frame.changes.find(
      (c) => c.collection === "batches" && c.value.state === "head",
    ).value.size = 1200;
    for (let i = 1; i <= 1200; i++)
      frame.changes.push({
        collection: "members",
        id: String(i),
        value: {
          ...template,
          id: String(i),
          pr: String(i),
          position: i,
          pr_url: `https://github.com/${repo}/pull/${i}`,
        },
      });
    frame.metadata.collection_counts.members = 1200;
    const parts = split(frame, 400),
      archive = globalThis.__operationsTestEnv.ARCHIVE,
      put = archive.put;
    let released = false;
    archive.put = async (key, value) => {
      enter();
      await gate;
      return put(key, value);
    };
    try {
      for (const part of parts.slice(0, -1))
        assert.equal((await r.send(part)).committed, false);
      const before = performance.now(),
        result = await r.send(parts.at(-1));
      t.diagnostic(
        "large snapshot acknowledgment " +
          Math.round(performance.now() - before) +
          "ms; storage gate unreleased",
      );
      assert.equal(result.committed, true);
      await entered;
      assert.equal(released, false);
    } finally {
      released = true;
      release();
      await r.drain();
    }
  },
);

test("duplicate arrivals do not upload again and event-driven heartbeats preserve pointer cadence", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.flush();
  const writes = r.writes.length;
  await Promise.all(Array.from({ length: 100 }, () => r.send(seed)));
  await r.drain();
  assert.equal(
    r.writes.length,
    writes,
    "no-op retries do not multiply graph uploads",
  );
  const archive = globalThis.__operationsTestEnv.ARCHIVE,
    put = archive.put,
    times = [];
  archive.put = async (key, value) => {
    if (key.endsWith("/manifest.json")) times.push(Date.now());
    return put(key, value);
  };
  let sequence = 1;
  for (let tick = 1; tick <= 60; tick++) {
    r.advance(1000);
    if (tick % 5 === 0) {
      await r.send(r.next(++sequence));
      await r.drain();
    }
  }
  assert.ok(
    times.length >= 5 && times.length <= 6,
    "events advance data without alarm delivery at the existing cadence",
  );
  for (let i = 1; i < times.length; i++)
    assert.ok(
      times[i] - times[i - 1] >= 10000,
      "the minimum pointer interval is preserved",
    );
});

test("pool activity is independent of list cohorts, advances cached windows and obeys repository withdrawal", async t => {
  const r=runtime(t);
  await r.send(seed); await r.flush();
  const clock=Date.now()/1000, minute=Math.floor(clock/60)*60;
  const scope={running:12,waiting:3,latest_completion:null,oldest_running:null,oldest_waiting:null};
  const activity={complete:true,computed_at:new Date(Date.now()).toISOString(),pools:[{pool:'brett',all:scope,builds:scope,bins:[[minute-3600,1700,2,3,0,0,1600,1,1,0,0]]}]};
  r.database.prepare('INSERT INTO ops_partitions VALUES (?,?,0,?,?)').run(repo+'/2026-11-06',1,clock,JSON.stringify({jobs:[],activity,pending_workflows:[],received_at:new Date(Date.now()).toISOString()}));
  r.operations.set('jobs_revision',99); await r.request('/tick'); await r.flush();
  let jobs=r.object(r.manifest().domains.jobs);
  assert.equal(jobs.pool_activity.pools[0].builds.windows[0].finished,1602);
  assert.equal(jobs.counts.running,0, 'the separately bounded list is not the source for pool counts');
  r.advance(61000); await r.request('/tick'); await r.flush();
  jobs=r.object(r.manifest().domains.jobs);
  assert.equal(jobs.pool_activity.pools[0].builds.windows[0].finished,0);
  assert.equal(jobs.pool_activity.pools[0].builds.windows[1].finished,1602);
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES='[]';
  await r.request('/tick'); await r.flush();
  assert.deepEqual(r.object(r.manifest().domains.jobs).pool_activity.pools,[]);
});

test("upgrade schedules quiet legacy Facts partitions without another GitHub event", async t => {
  const r=runtime(t);await r.send(seed);await r.flush();
  const clock=Date.now()/1000;
  for(let day=1;day<=3;day++) r.database.prepare('INSERT INTO ops_partitions VALUES (?,?,0,?,?)').run(repo+'/2026-11-0'+day,1,clock-86400*day,JSON.stringify({jobs:[],pending_workflows:[],received_at:new Date((clock-86400*day)*1000).toISOString()}));
  const scope={running:0,waiting:0,latest_completion:null,oldest_running:null,oldest_waiting:null};
  let calls=0;
  globalThis.__operationsTestEnv.FACTS={idFromName:id=>id,get:partition=>({fetch:async()=>{calls++;return Response.json({schema:'tauceti.operations.partition/v1',partition,revision:1,received_at:new Date(Date.now()).toISOString(),jobs:[],pending_workflows:[],activity:{complete:true,computed_at:new Date(Date.now()).toISOString(),pools:[{pool:'github',all:scope,builds:scope,bins:[]}]}});}})};
  const oldRevision=r.operations.get('jobs_revision')||0;
  new Operations(r.ctx);
  assert.equal(r.database.prepare('SELECT count(*) n FROM ops_partitions WHERE pending=1').get().n,3);
  assert.equal(r.operations.get('jobs_revision'),oldRevision+1);
  new Operations(r.ctx);
  assert.equal(r.operations.get('jobs_revision'),oldRevision+1,'restart does not requeue already pending work');
  await r.request('/tick');await r.flush();
  assert.equal(calls,3);
  assert.equal(r.object(r.manifest().domains.jobs).pool_activity.coverage,'retained_observations');
  assert.equal(r.database.prepare('SELECT count(*) n FROM ops_partitions WHERE pending=1').get().n,0);
  new Operations(r.ctx);
  assert.equal(r.database.prepare('SELECT count(*) n FROM ops_partitions WHERE pending=1').get().n,0,'upgraded cached facts stay quiet');
});

test("historical activity migration leaves two live source lanes and still advances old work", async t => {
 const r=runtime(t);await r.send(seed);await r.flush();
 const clock=Date.now()/1000, day=new Date(Date.now()).toISOString().slice(0,10), calls=[];
 const insert=r.database.prepare('INSERT INTO ops_partitions VALUES (?,?,1,?,?)');
 for(let n=1;n<=12;n++)insert.run(repo+'/2026-10-'+String(n).padStart(2,'0'),1,clock-10000+n,JSON.stringify({computed_at:new Date((clock-20000)*1000).toISOString(),pending_workflows:[],activity:{complete:false,pools:[]}}));
 const hot=[repo+'/'+day,repo+'/'+new Date((clock-86400)*1000).toISOString().slice(0,10)];
 for(const partition of hot)insert.run(partition,1,clock,JSON.stringify({computed_at:new Date((clock-100)*1000).toISOString(),pending_workflows:[],activity:{complete:false,pools:[]}}));
 globalThis.__operationsTestEnv.FACTS={idFromName:id=>id,get:partition=>({fetch:async()=>{calls.push(partition);return Response.json({schema:'tauceti.operations.partition/v1',partition,revision:1,jobs:[],pending_workflows:[],computed_at:new Date(Date.now()).toISOString(),activity:{complete:true,pools:[]}});}})};
 await r.request('/tick');await r.drain();
 assert.equal(calls.length,3);assert.ok(hot.every(p=>calls.includes(p)),'both live sources advance before the historical backlog empties');
 assert.ok(calls.some(p=>p.endsWith('/2026-10-01')),'historical work retains a lane');
 for(const p of hot)assert.equal(r.database.prepare('SELECT pending FROM ops_partitions WHERE name=?').get(p).pending,0);
});

test("pool activity includes cached source metadata beyond the 200-partition job display limit", async t => {
 const r=runtime(t);await r.send(seed);await r.flush();
 const clock=Date.now()/1000, minute=Math.floor(clock/60)*60;
 const scope={running:0,waiting:0,latest_completion:null,oldest_running:null,oldest_waiting:null};
 for(let n=0;n<205;n++)r.database.prepare('INSERT INTO ops_partitions VALUES (?,?,0,?,?)').run(repo+'/'+new Date((clock-n*86400)*1000).toISOString().slice(0,10),1,clock-n,JSON.stringify({jobs:[],pending_workflows:[],received_at:new Date(Date.now()).toISOString(),activity:{complete:true,pools:[{pool:'github',all:scope,builds:scope,bins:[[minute-120,1,0,0,0,0,1,0,0,0,0]]}]}}));
 r.operations.set('jobs_revision',99);await r.request('/tick');await r.flush();
 const value=r.object(r.manifest().domains.jobs);
 assert.equal(value.source_truncated,true);
 assert.equal(value.pool_activity.coverage,'retained_observations');
 assert.equal(value.pool_activity.pools[0].builds.windows[0].finished,205);
});

test("native internal Facts reads may finish after three seconds without a false observer failure", async t => {
 const bundled=await build({stdin:{resolveDir:process.cwd(),loader:'js',contents:`
 import {Operations} from './src/operations.ts'; export {Operations};
 export class Facts {async fetch(){
   await new Promise(resolve=>setTimeout(resolve,3500));
   const scope={running:1,waiting:0,latest_completion:null,oldest_running:null,oldest_waiting:null};
   return Response.json({schema:'tauceti.operations.partition/v1',partition:'${repo}/2026-11-06',revision:1,computed_at:new Date().toISOString(),received_at:new Date().toISOString(),jobs:[],pending_workflows:[],activity:{complete:true,pools:[{pool:'github',all:scope,builds:scope,bins:[]}]}});
 }}
 export default{fetch(request,env){return env.OPERATIONS.get(env.OPERATIONS.idFromName('test')).fetch(request)}};
 `},bundle:true,platform:'browser',format:'esm',external:['cloudflare:workers'],write:false});
 const mf=new Miniflare(convertV4MiniflareOptions({modules:true,script:bundled.outputFiles[0].text,compatibilityDate:'2026-10-15',durableObjects:{OPERATIONS:{className:'Operations',useSQLite:true},FACTS:{className:'Facts',useSQLite:true}},r2Buckets:['ARCHIVE'],bindings:{OPERATIONS_PUBLIC_REPOSITORIES:JSON.stringify([repo])}}));
 t.after(()=>mf.dispose());
 const bucket=await mf.getR2Bucket('ARCHIVE');
 assert.equal((await mf.dispatchFetch('https://internal/partition',{method:'POST',body:JSON.stringify({partition:repo+'/2026-11-06',revision:1})})).status,200);
 for(let i=0;i<400;i++){
  const object=await bucket.get('public/v1/operations/manifest.json');
  if(object){const manifest=await object.json(),ref=manifest.domains.jobs;if(ref?.url){const value=await (await bucket.get('public'+ref.url)).json();if(value.pool_activity?.pools[0]?.builds.running===1){assert.equal(value.pool_activity.coverage,'retained_observations');return;}}}
  await new Promise(resolve=>setTimeout(resolve,50));
 }
 assert.fail('A valid slow internal source never became visible');
});
