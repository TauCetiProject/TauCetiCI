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
    writes = [];
  let alarm = null,
    failPointer = false,
    beforeWrite,
    clock = Date.parse(seed.metadata.observed_at);
  t.mock.method(Date, "now", () => clock);
  const ctx = {
    storage: {
      sql: {
        exec(sql, ...args) {
          if (
            args.some(
              (v) => typeof v === "string" && Buffer.byteLength(v) > 2_000_000,
            )
          )
            throw new Error("Durable Object SQLite row/value limit");
          let rows = [];
          if (!args.length && sql.split(";").filter((s) => s.trim()).length > 1)
            database.exec(sql);
          else {
            const statement = database.prepare(sql);
            if (statement.columns().length) rows = statement.all(...args);
            else statement.run(...args);
          }
          return {
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
  t.after(() => {
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
  };
}

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

test("actual Python export is accepted and produces linked, paginated queue and PR views", async (t) => {
  const r = runtime(t);
  const result = await r.send(seed);
  assert.equal(result.committed, true);
  assert.equal(
    r.objects.size,
    0,
    "ingest schedules publication rather than doing R2 work in the producer request",
  );
  await r.operations.alarm();
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
  await r.operations.alarm();
  const oldManifest = r.objects.get("public/v1/operations/manifest.json");
  const next = split(r.next(1, 2));
  await r.send(next[0]);
  await r.send(r.next(3, 1), 409);
  assert.equal(
    r.objects.get("public/v1/operations/manifest.json"),
    oldManifest,
  );
  for (const frame of next.slice(1)) await r.send(frame);
  await r.operations.alarm();
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
  await r.operations.alarm();
  const previous = r.objects.get("public/v1/operations/manifest.json");
  r.advance(15_000);
  await r.send(r.next());
  r.failPointer();
  await r.operations.alarm();
  assert.equal(r.objects.get("public/v1/operations/manifest.json"), previous);
  assert.equal(
    (await (await r.request("/health")).json()).publication_error,
    true,
  );
  assert.ok(r.alarm() > Date.now());
  r.advance(15_000);
  await r.operations.alarm();
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
  await r.operations.alarm();
  const published = Date.now();
  r.advance(8000);
  await r.send(r.next());
  assert.equal(r.alarm(), published + 10000, "preserve the existing minimum write interval");
  r.advance(3000);
  await r.operations.alarm();
  r.advance(15000);
  await r.send(r.next(3));
  assert.equal(r.alarm(), Date.now() + 1000, "idle arrivals need not wait another full interval");
});

test("ingestion during an R2 write cannot publish mixed member and candidate generations", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.operations.alarm();
  const previous = r.objects.get("public/v1/operations/manifest.json");
  const second = r.next(2);
  second.changes.find(
    (c) => c.collection === "batches" && c.value.state === "head",
  ).value.version = 2;
  await r.send(second);
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
  await r.operations.alarm();
  assert.notEqual(
    r.objects.get("public/v1/operations/manifest.json"),
    previous,
  );
  assert.equal(
    (await (await r.request("/health")).json()).published_revision,
    2,
  );
  const captured = r.object(r.manifest().domains.queue);
  assert.equal(captured.batches.find((b) => b.state === "head").version, 2);
  await r.operations.alarm();
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
  for (const frame of split(value, 400)) await r.send(frame);
  let active = 0, peak = 0, allActive = 0, allPeak = 0, ready, release;
  const fourPages = new Promise((resolve) => { ready = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  const watch = async (_key, body) => {
    r.onWrite(watch);
    allActive++; allPeak = Math.max(allPeak, allActive);
    try {
      if (JSON.parse(body).schema !== "tauceti.operations.members/v1") return;
      active++; peak = Math.max(peak, active);
      if (active === 4) ready(true);
      try { await gate; } finally { active--; }
    } finally { allActive--; }
  };
  r.onWrite(watch);
  const publication = r.operations.alarm();
  try {
    assert.equal(await Promise.race([fourPages, new Promise((resolve) => setTimeout(() => resolve(false), 1000))]), true,
      "independent member pages should upload concurrently");
    assert.equal(r.objects.has("public/v1/operations/manifest.json"), false,
      "the pointer must wait for every dependent object");
  } finally { release(); await publication; }
  assert.equal(peak, 4, "independent member pages overlap");
  assert.ok(allPeak <= 4, "R2 writes remain globally bounded across domains");
  assert.equal(new Set(r.writes).size, r.writes.length, "identical shard objects upload once");
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
  await r.operations.alarm();
  assert.ok(
    r.writes.length - writes <= 4,
    "queue, jobs and hosts current objects plus manifest, with immutable details reused",
  );
});

test("waiting inputs have bounded pages and leave the prepared list atomically on insertion", async (t) => {
  const r = runtime(t), frame = structuredClone(seed);
  const template = frame.changes.find(c => c.collection === "inputs").value;
  const waiting = Array.from({length:1200}, (_, i) => ({
    ...template, id: String(10000+i), pr: String(20000+i), state: "waiting", batch_id: null,
  }));
  frame.changes.push(...waiting.map(value=>({collection:"inputs",id:value.id,value})));
  frame.metadata.collection_counts.inputs += waiting.length;
  for (const part of split(frame,400)) await r.send(part);
  await r.operations.alarm();
  let queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.counts.waiting,1200);
  assert.equal(queue.waiting_pages.length,12);
  assert.deepEqual(queue.waiting_pages.flatMap(p=>r.object(p).records).map(i=>i.id).sort(),waiting.map(i=>i.id).sort());
  const delta = r.next();
  delta.base = {epoch:frame.source.epoch,sequence:frame.source.sequence};
  delta.changes = [{collection:"inputs",id:waiting[0].id,value:{...waiting[0],state:"inserted",batch_id:"1"}}];
  delta.metadata.collection_counts.inputs = frame.metadata.collection_counts.inputs;
  await r.send(delta);
  assert.equal(r.object(r.manifest().domains.queue).counts.waiting,1200);
  await r.operations.alarm();
  queue = r.object(r.manifest().domains.queue);
  assert.equal(queue.counts.waiting,1199);
  assert.ok(!queue.waiting_pages.flatMap(p=>r.object(p).records).some(i=>i.id===waiting[0].id));
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
  const cachedManifest = await mf.dispatchFetch(url + "/v1/operations/manifest.json");
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
  for (let n = 1; n <= 6; n++) {
    r.onWrite(async () => {
      r.advance(10_000);
      await r.send(r.next(n + 1));
    });
    await r.operations.alarm();
    assert.equal(r.object(r.manifest().domains.queue).source_sequence, n);
  }
  await r.operations.alarm();
  assert.equal(r.object(r.manifest().domains.queue).source_sequence, 7);
  assert.equal(r.alarm(), null);
  const writes = r.writes.length;
  await r.request("/tick");
  assert.equal(r.writes.length, writes);
});

test("a missing delta base requests resync and deleted dispatch attempts disappear atomically", async (t) => {
  const r = runtime(t),
    delta = r.next();
  delta.base = { epoch: 1, sequence: 1 };
  delta.changes = [];
  assert.equal((await r.send(delta, 409)).code, "resync");
  await r.send(seed);
  await r.operations.alarm();
  const attempt = seed.changes.find((c) => c.collection === "attempts");
  const deleted = r.next();
  deleted.base = { epoch: 1, sequence: 1 };
  deleted.changes = [{ collection: "attempts", id: attempt.id, value: null }];
  await r.send(deleted);
  await r.operations.alarm();
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
  await r.operations.alarm();
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
  await r.operations.alarm();
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
    worker_memory: {working_set:123,charged:456,maximum:1000,high:800,swap:0,oom_kill:2,stall_some:0.1,stall_full:null},
    worker_admission: {state:"paused",reason:"memory_reserve"},
    ci_cooldown: {until:new Date(Date.now()+30000).toISOString()},
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
  await r.operations.alarm();
  const hosts = r.object(r.manifest().domains.hosts);
  assert.equal(hosts.hosts[0].ci_enabled, false);
  assert.equal(hosts.hosts[0].connected, true);
  assert.equal(hosts.hosts[0].worker_memory.oom_kill,2);
  assert.equal(hosts.hosts[0].worker_admission.reason,"memory_reserve");
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
  await r.operations.alarm();
  let retained = r.object(r.manifest().domains.hosts).hosts[0];
  assert.equal(retained.observed_at, host.observed_at);
  assert.equal(retained.resources.cpus, 16);
  assert.equal(retained.connected, false);
  assert.equal((await r.request("/host", frame)).status, 200);
  assert.equal((await r.request("/host", restarted)).status, 200);
  await r.operations.alarm();
  retained = r.object(r.manifest().domains.hosts).hosts[0];
  assert.equal(retained.observed_at, host.observed_at);
  assert.equal(retained.resources.cpus, 16);
  assert.equal(retained.connected, false);
  assert.equal(
    r.database.prepare("SELECT COUNT(*) n FROM ops_host_samples").get().n,
    3,
    "a restart must not manufacture another historical host sample",
  );
});

test("shared pilot report exposes bounded public metrics without private snapshot or outcome paths", async (t) => {
  const r = runtime(t),
    report = JSON.parse(
      readFileSync("tests/fixtures/operations-history.json", "utf8"),
    );
  const response = await r.request("/history", report);
  assert.equal(response.status, 200, await response.clone().text());
  await r.operations.alarm();
  const history = r.object(r.manifest().domains.history);
  assert.equal(history.windows.length, 3);
  assert.equal(history.windows[0].summary.cost.recorded_usd, 2.5);
  const bad = structuredClone(report);
  bad.windows[0].summary.cost.private_path = "/private/token";
  assert.equal((await r.request("/history", bad)).status, 400);
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
  const inputTemplate = frame.changes.find(c => c.collection === "inputs").value;
  for (let i=0;i<120;i++) {
    const value = {...inputTemplate,id:String(10000+i),pr:String(20000+i),state:"waiting",batch_id:null,no_repair:i===0};
    frame.changes.push({collection:"inputs",id:value.id,value});
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
  await r.operations.alarm();
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
  await r.operations.alarm();
  const old = r.manifest();
  assert.ok(r.object(old.domains.queue).batches.length);
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_REPOSITORIES = "[]";
  await r.operations.alarm();
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
  await r.operations.alarm();
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
  value.jobs[0] = {...value.jobs[0], active:false, status:"completed", conclusion:"success"};
  value.jobs.splice(1,1);
  await r.request("/partition",{partition,revision:2});
  await r.operations.alarm();
  assert.equal(r.database.prepare("SELECT COUNT(*) n FROM ops_job_facts").get().n,999);
  assert.equal(r.database.prepare("SELECT active FROM ops_job_facts WHERE id='1'").get().active,0);
  assert.equal(r.database.prepare("SELECT COUNT(*) n FROM ops_job_facts WHERE id='2'").get().n,0);
  assert.equal(r.object(r.manifest().domains.jobs).counts.running,998);
  const writes = r.database.prepare("SELECT total_changes() n").get().n - before;
  assert.ok(writes < 100, `one changed and one removed job must not rewrite 1000 jobs (${writes} rows changed)`);
});

test("partial member updates cannot borrow old commits to claim current-generation completeness", async (t) => {
  const r = runtime(t);
  await r.send(seed);
  await r.operations.alarm();
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
  await r.operations.alarm();
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
  await r.operations.alarm();
  globalThis.__operationsTestEnv.OPERATIONS_PUBLIC_HOSTS = "invalid JSON";
  r.advance(15000);
  await r.send(r.next());
  await r.operations.alarm();
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
  await r.operations.alarm();
  const before = r.database
    .prepare("SELECT hash,referenced FROM ops_objects")
    .all();
  r.advance(10000);
  const heartbeat = r.next();
  heartbeat.changes = [];
  heartbeat.base = { epoch: seed.source.epoch, sequence: seed.source.sequence };
  await r.send(heartbeat);
  await r.operations.alarm();
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
  frame.metadata.dispatch_config = {repository:repo,revision:frame.metadata.canonical_sha,namespace_build_cap:20,pr_order:["github","chris","namespace"],staging_order:["chris","namespace","github"]};
  const wrongRevision = structuredClone(frame);
  wrongRevision.metadata.dispatch_config.revision = "b".repeat(40);
  assert.equal((await r.request("/queue",wrongRevision)).status,400);
  const privatePolicy = structuredClone(frame);
  privatePolicy.metadata.dispatch_config.environment = {TOKEN:"PRIVATE"};
  assert.equal((await r.request("/queue",privatePolicy)).status,400);
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
  await r.operations.alarm();
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
  await r.operations.alarm();
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
