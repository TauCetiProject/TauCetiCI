import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { build, buildSync } from "esbuild";

const code = buildSync({
  entryPoints: ["src/storage.ts"], bundle: true, platform: "node", format: "esm", write: false,
}).outputFiles[0].text;
const { FactPartition, DayAggregate, RunDirectory } = await import(
  "data:text/javascript;base64," + Buffer.from(code).toString("base64")
);

// Run the actual storage classes and SQL, with deterministic alarm deadlines
// and transport. Automatic real-time alarms would make burst/retry tests race.
function context(t) {
  const db = new DatabaseSync(":memory:");
  t.after(() => db.close());
  let alarm = null;
  return { db, storage: {
    sql: { exec(query, ...values) {
      let rows = [];
      if (!values.length && query.split(";").filter((s) => s.trim()).length > 1)
        db.exec(query);
      else {
        const statement = db.prepare(query);
        if (statement.columns().length) rows = statement.all(...values);
        else statement.run(...values);
      }
      return { toArray: () => rows, one: () => {
        assert.equal(rows.length, 1); return rows[0];
      } };
    } },
    transactionSync(fn) {
      db.exec("BEGIN");
      try { const result = fn(); db.exec("COMMIT"); return result; }
      catch (error) { db.exec("ROLLBACK"); throw error; }
    },
    getAlarm: async () => alarm,
    setAlarm: async (at) => { alarm = at; },
    deleteAlarm: async () => { alarm = null; },
  } };
}

test("legacy and retried orphan repairs expire without discarding fresh metadata work", async (t) => {
  let clock = Date.parse('2026-10-28T12:00:00Z');
  t.mock.method(Date, 'now', () => clock);
  const namespace = (value) => ({ idFromName: (id) => id,
    get: () => ({ fetch: async () => Response.json(value) }) });
  globalThis.__orphanTestEnv = {
    DIRECTORIES: namespace({ partition: 'TauCetiProject/TauCeti/2026-10-26' }),
    FACTS: namespace({ jobs: 0, job_rows: [], verified: false }),
  };
  t.after(() => { delete globalThis.__orphanTestEnv; });
  const bundled = await build({ entryPoints: ['src/control.ts'], bundle: true,
    platform: 'node', format: 'esm', write: false, plugins: [{ name: 'test-bindings', setup(builder) {
      builder.onResolve({ filter: /^cloudflare:workers$/ }, () => ({ path: 'bindings', namespace: 'test' }));
      builder.onLoad({ filter: /.*/, namespace: 'test' }, () => ({ contents: 'export const env = globalThis.__orphanTestEnv;' }));
    } }] });
  const { Control } = await import('data:text/javascript;base64,' + Buffer.from(bundled.outputFiles[0].text).toString('base64'));
  const ctx = context(t), repo = 'TauCetiProject/TauCeti';
  ctx.storage.sql.exec('CREATE TABLE repairs(key TEXT PRIMARY KEY,repo TEXT,kind TEXT,args TEXT,due INTEGER,failures INTEGER DEFAULT 0)');
  ctx.storage.sql.exec('INSERT INTO repairs VALUES (?,?,?,?,?,0)', 'legacy', repo,
    'details', JSON.stringify({ id: '100', attempt: 1 }), clock / 1000 - 2 * 86400);
  const control = new Control(ctx);
  assert.equal((await control.pruneRepairs()).retired, 1);
  control.enqueue(repo, 'jobs', { id: '200', attempt: 1 });
  const original = ctx.storage.sql.exec('SELECT enqueued FROM repairs').one().enqueued;
  clock += 2 * 86400 * 1000;
  ctx.storage.sql.exec('UPDATE repairs SET due=?', clock / 1000 + 60);
  control.enqueue(repo, 'jobs', { id: '200', attempt: 1, urgent: true });
  assert.equal(ctx.storage.sql.exec('SELECT enqueued FROM repairs').one().enqueued, original);
  control.enqueue(repo, 'run', { id: '300', attempt: 1 });
  const result = await control.pruneRepairs();
  assert.equal(result.retired, 1);
  assert.equal(ctx.storage.sql.exec('SELECT kind FROM repairs').one().kind, 'run');
});

function fixture(t, count) {
  let clock = Date.parse("2026-10-27T12:00:00Z"), loseAcknowledgement = false;
  let holdDelivery;
  t.mock.method(Date, "now", () => clock);
  const ctx = context(t), days = new Map(), notifications = [];
  let failControl = false, holdControl;
  const env = {
    DAYS: { idFromName: (id) => id, get: (id) => ({ fetch: async (url, options) => {
      if (!days.has(id)) days.set(id, new DayAggregate(context(t), env));
      const response = await days.get(id).fetch(new Request(url, options));
      if (holdDelivery) {
        const hold = holdDelivery;
        holdDelivery = undefined;
        await hold();
      }
      if (loseAcknowledgement) {
        loseAcknowledgement = false;
        throw new Error("lost acknowledgement after applying receipts");
      }
      return response;
    } }) },
    CONTROL: { idFromName: (id) => id, get: () => ({ fetch: async (_url, options) => {
      notifications.push(JSON.parse(options.body));
      if (failControl) { failControl = false; throw new Error("Control unavailable"); }
      if (holdControl) { const hold = holdControl; holdControl = undefined; await hold(); }
      return Response.json({});
    } }) },
    ARCHIVE: { get: async () => null, put: async () => {} },
  };
  const fact = new FactPartition(ctx, env), seconds = clock / 1000;
  fact.set("partition", "TauCetiProject/TauCeti/2026-10-27");
  const run = {
    repo: "TauCetiProject/TauCeti", id: "100", attempt: 1,
    workflow: ".github/workflows/pr-build.yml", event: "pull_request_target", trigger: "pr",
    status: "in_progress", conclusion: null, created: seconds - 300,
    started: seconds - 300, updated: seconds, head: "a".repeat(40), branch: "main", url: "",
  };
  ctx.storage.sql.exec("INSERT INTO runs VALUES (?,?,?)", run.id, run.attempt, JSON.stringify(run));
  for (let i = 0; i < count; i++) fact.replaceJob(run, {
    id: String(100000 + i), run: run.id, attempt: 1, name: "sandboxed-build",
    status: "queued", conclusion: null, created: seconds - 300, started: null,
    completed: null, kind: "github", assigned: false, steps: [], weight: 1,
    estimated: false, updated: seconds,
  }, seconds);
  const backlog = async () => (await fact.fetch(new Request("https://internal/backlog", {
    method: "POST", body: "{}",
  }))).json();
  return {
    fact, ctx, run, days, backlog, env, notifications,
    advance: (seconds) => { clock += seconds * 1000; },
    loseAcknowledgement: () => { loseAcknowledgement = true; },
    holdDelivery: (hold) => { holdDelivery = hold; },
    failControl: () => { failControl = true; },
    holdControl: (hold) => { holdControl = hold; },
    async wake() {
      const alarm = await ctx.storage.getAlarm();
      assert.notEqual(alarm, null);
      assert.ok(alarm > clock, "alarm must yield before the next wake");
      clock = alarm;
      await fact.alarm();
    },
    async catchUp() {
      for (let i = 0; (await backlog()).outbox; i++) {
        assert.ok(i < 100, "fixture outbox must drain");
        await fact.flush(true);
      }
    },
  };
}

async function post(object, path, body = {}) {
  const response = await object.fetch(new Request("https://internal" + path, {
    method: "POST", body: JSON.stringify(body),
  }));
  assert.equal(response.status, 200);
  return response.json();
}
const changes = (ctx) => Number(ctx.db.prepare("SELECT total_changes() n").get().n);
const rawRun = (run, extra = {}) => ({
  id: run.id, run_attempt: run.attempt, path: run.workflow, event: run.event,
  trigger: run.trigger, status: run.status, conclusion: run.conclusion,
  created_at: new Date(run.created * 1000).toISOString(),
  run_started_at: new Date(run.started * 1000).toISOString(),
  updated_at: new Date(run.updated * 1000).toISOString(),
  head_sha: run.head, head_branch: run.branch, html_url: run.url, ...extra,
});

test("a settled duplicate writes no rows and does not notify Control", async (t) => {
  const f = fixture(t, 0);
  const event = { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "same", run: rawRun(f.run, { status: "completed", conclusion: "success" }) };
  await post(f.fact, "/event", event);
  await f.fact.flush(true);
  const before = changes(f.ctx), revision = f.fact.meta("partition_revision"), sends = f.notifications.length;
  await post(f.fact, "/event", event);
  await f.fact.flush(true);
  assert.equal(changes(f.ctx), before);
  assert.equal(f.fact.meta("partition_revision"), revision);
  assert.equal(f.notifications.length, sends);
  assert.equal(await f.ctx.storage.getAlarm(), null);
  // A new delivery of the same complete run adds only its deduplication row.
  await post(f.fact, "/event", { ...event, delivery: "same-facts" });
  assert.equal(changes(f.ctx) - before, 1);
});

test("clock refreshes update only job contributions and do not repeat partition notifications", async (t) => {
  const f = fixture(t, 1);
  await f.catchUp();
  const queries = [], exec = f.ctx.storage.sql.exec;
  f.ctx.storage.sql.exec = (sql, ...args) => { queries.push(sql); return exec(sql, ...args); };
  const sends = f.notifications.length;
  await f.wake();
  assert.deepEqual(queries.filter((sql) => /^(INSERT|UPDATE).*jobs/.test(sql)),
    ["UPDATE jobs SET contribution=? WHERE id=?"]);
  assert.equal(f.notifications.length, sends);
  const revision = f.fact.meta("partition_revision");
  await f.fact.flush(true);
  assert.equal(f.fact.meta("partition_revision"), revision);
  f.advance(86401);
  await f.fact.flush(true);
  assert.equal(f.notifications.length, sends + 1, "active partitions retain a daily heartbeat");
});

for (const unknown of [false, true]) test(`activity transitions notify without chart deltas (${unknown ? "unknown runner" : "no jobs"})`, async (t) => {
  const f = fixture(t, 0);
  if (unknown) f.fact.replaceJob(f.run, {
    id: "200", run: f.run.id, attempt: 1, name: "helper", status: "queued",
    conclusion: null, created: f.run.created, started: null, completed: null,
    kind: "unknown", assigned: false, steps: [], weight: 1, estimated: false, updated: f.run.updated,
  }, Date.now() / 1000);
  assert.equal((await f.backlog()).outbox, 0);
  await f.fact.flush(true);
  const revision = f.fact.meta("partition_revision");
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "terminal", run: rawRun(f.run, { status: "completed", conclusion: "success" }) });
  assert.equal((await f.backlog()).outbox, 0);
  assert.equal(f.fact.meta("partition_pending"), revision + 1);
  assert.equal(await f.ctx.storage.getAlarm(), Date.now() + 1000);
  await f.fact.flush(true);
  assert.equal(f.notifications.at(-1).active, 0);
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "rerun", run: rawRun(f.run, { run_attempt: 2 }) });
  await f.fact.flush(true);
  assert.equal(f.notifications.at(-1).active, 1);
  assert.ok(f.notifications.at(-1).revision > revision + 1);
});

test("partition notification failure survives eviction and legacy revisions keep increasing", async (t) => {
  const f = fixture(t, 0);
  f.fact.set("partition_revision", 500);
  f.fact.set("partition_pending", 500);
  f.fact.set("alarm_refresh_due", Date.now() / 1000 + 60);
  f.advance(60); // Honor the already-persisted legacy deadline once.
  f.failControl();
  await assert.rejects(f.fact.alarm(), /Control unavailable/);
  assert.equal(f.fact.meta("partition_pending"), 501);
  assert.equal(await f.ctx.storage.getAlarm(), Date.now() + 60000);
  const restarted = new FactPartition(f.ctx, f.env);
  await restarted.alarm();
  assert.equal(f.notifications.at(-1).revision, 501);
  assert.equal(restarted.meta("partition_pending"), null);
  assert.equal(restarted.meta("alarm_refresh_due"), Date.now() / 1000 + 300);
});

test("a rollback advancing the legacy counter invalidates a stale activity marker", async (t) => {
  const f = fixture(t, 0);
  await f.fact.flush(true);
  const reported = f.fact.meta("partition_reported");
  // Older code maintains the legacy counter/pending keys, but knows nothing
  // about the new marker. Its latest acknowledgement may describe other state.
  f.fact.set("partition_revision", reported.revision + 100);
  f.fact.set("partition_pending", null);
  const restarted = new FactPartition(f.ctx, f.env);
  await restarted.flush(true);
  assert.equal(f.notifications.at(-1).revision, reported.revision + 101);
  assert.equal(f.notifications.at(-1).active, 1);
  assert.equal(restarted.meta("partition_pending"), null);
});

test("a terminal event during a partition notification retains its newer generation", async (t) => {
  const f = fixture(t, 0);
  let entered, release;
  const paused = new Promise((resolve) => { entered = resolve; });
  const resume = new Promise((resolve) => { release = resolve; });
  f.holdControl(() => { entered(); return resume; });
  const sending = f.fact.flush(true);
  await paused;
  const oldRevision = f.fact.meta("partition_pending");
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "concurrent-terminal", run: rawRun(f.run, { status: "completed" }) });
  release();
  await sending;
  assert.ok(f.fact.meta("partition_pending") > oldRevision);
  assert.notEqual(await f.ctx.storage.getAlarm(), null);
  await f.fact.flush(true);
  assert.equal(f.notifications.at(-1).active, 0);
  assert.equal(f.fact.meta("partition_pending"), null);
});

test("an event during the alarm lookup cannot have its recovery alarm deleted", async (t) => {
  const f = fixture(t, 0);
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "finished", run: rawRun(f.run, { status: "completed" }) });
  await f.fact.flush(true);
  assert.equal(await f.ctx.storage.getAlarm(), null);
  let entered, release, first = true;
  const paused = new Promise((resolve) => { entered = resolve; });
  const resume = new Promise((resolve) => { release = resolve; });
  const getAlarm = f.ctx.storage.getAlarm;
  f.ctx.storage.getAlarm = async () => {
    if (first) { first = false; entered(); await resume; }
    return getAlarm();
  };
  const flushing = f.fact.flush(true);
  await paused;
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "restarted", run: rawRun(f.run, { run_attempt: 2 }) });
  release();
  await flushing;
  assert.ok(f.fact.meta("partition_pending"));
  assert.notEqual(await getAlarm(), null);
  await f.fact.flush(true);
  assert.equal(f.notifications.at(-1).active, 1);
});

test("same-timestamp telemetry changes still correct failure metrics", async (t) => {
  const f = fixture(t, 0), run = rawRun(f.run, { status: "completed", conclusion: "failure" });
  const event = { partition: f.fact.meta("partition"), repo: f.run.repo, delivery: "failure", run,
    jobs: [{ id: "200", run_id: run.id, run_attempt: 1, name: "sandboxed-build",
      status: "completed", conclusion: "failure", labels: ["ubuntu-latest"],
      created_at: run.created_at, started_at: run.created_at, completed_at: run.updated_at }] };
  await post(f.fact, "/event", event);
  assert.equal(f.days.values().next().value.summary().daily["failure/pending"], 1);
  await post(f.fact, "/event", { ...event, delivery: "classification", jobs: [], telemetry: { failure: "lint" } });
  const daily = f.days.values().next().value.summary().daily;
  assert.equal(daily["failure/lint"], 1);
  assert.equal(daily["failure/pending"], undefined);
});

test("changed run metadata recalculates active sibling contributions at the same timestamp", async (t) => {
  const f = fixture(t, 1);
  const job = JSON.parse(f.ctx.storage.sql.exec("SELECT data FROM jobs").one().data);
  f.fact.replaceJob(f.run, { ...job, kind: "namespace" }, Date.now() / 1000);
  await f.catchUp();
  const aggregate = f.days.values().next().value;
  assert.equal(aggregate.summary().daily["picker/namespace"], 1);
  await post(f.fact, "/event", { partition: f.fact.meta("partition"), repo: f.run.repo,
    delivery: "corrected-trigger", run: rawRun(f.run, { trigger: "bors_staging" }) });
  assert.equal(f.fact.run(f.run.id, 1).trigger, "bors_staging");
  assert.equal(aggregate.summary().daily["picker/namespace"], undefined);
  assert.equal((await f.backlog()).active_jobs, 1);
});

test("unchanged run-directory locations do not rewrite their indexed row", async (t) => {
  const ctx = context(t), directory = new RunDirectory(ctx, {});
  const b = { id: "1", partition: "TauCetiProject/TauCeti/2026-10-27" };
  await post(directory, "/put", b);
  const before = changes(ctx);
  assert.equal((await post(directory, "/put", b)).partition, b.partition);
  assert.equal(changes(ctx), before);
  assert.equal((await post(directory, "/put", { ...b, partition: "corrected" })).partition, "corrected");
});

function dayFixture(t) {
  let clock = Date.parse("2026-10-27T12:00:00Z"), fail = false, hold;
  t.mock.method(Date, "now", () => clock);
  const ctx = context(t), notifications = [];
  const env = { CONTROL: { idFromName: (id) => id, get: () => ({ fetch: async (_url, options) => {
    notifications.push(JSON.parse(options.body));
    if (fail) { fail = false; throw new Error("Control unavailable"); }
    if (hold) { const waiting = hold; hold = undefined; await waiting(); }
    return Response.json({});
  } }) } };
  const aggregate = new DayAggregate(ctx, env);
  return { aggregate, ctx, env, notifications, advance: (seconds) => { clock += seconds * 1000; },
    fail: () => { fail = true; }, hold: (fn) => { hold = fn; },
    delta: (id) => post(aggregate, "/delta", { id, day: "2026-10-27",
      rows: [{ bucket: 0, metric: "pr-total", value: 0, amount: 1 }] }) };
}

test("day changes coalesce for one minute; duplicate receipts write and notify nothing", async (t) => {
  const f = dayFixture(t);
  for (let i = 0; i < 50; i++) { await f.delta(String(i)); f.advance(0.5); }
  assert.equal(await f.ctx.storage.getAlarm(), Date.parse("2026-10-27T12:01:00Z"));
  assert.equal(f.notifications.length, 0);
  const before = changes(f.ctx);
  await f.delta("49");
  assert.equal(changes(f.ctx), before);
  assert.equal(f.aggregate.meta("revision"), 50);
  await f.aggregate.alarm();
  assert.equal(f.notifications.length, 1);
  await f.delta("49");
  assert.equal(await f.ctx.storage.getAlarm(), null);
  assert.equal(f.aggregate.meta("notification"), null);
});

test("day notifications survive send failure, eviction and a concurrent correction", async (t) => {
  const f = dayFixture(t);
  await f.delta("first");
  f.fail();
  await assert.rejects(f.aggregate.alarm(), /Control unavailable/);
  assert.equal(await f.ctx.storage.getAlarm(), Date.now() + 60000);
  const restarted = new DayAggregate(f.ctx, f.env);
  let entered, release;
  const paused = new Promise((resolve) => { entered = resolve; });
  const resume = new Promise((resolve) => { release = resolve; });
  f.hold(() => { entered(); return resume; });
  const sending = restarted.alarm();
  await paused;
  await f.delta("second");
  release();
  await sending;
  assert.equal(restarted.meta("notification").revision, 2);
  assert.notEqual(await f.ctx.storage.getAlarm(), null);
  await restarted.alarm();
  assert.equal(restarted.meta("notification"), null);
  assert.equal(f.notifications.length, 3);
});

for (const expired of [false, true]) test(`retention re-arms a legacy pending day notification (${expired ? "elapsed" : "missing"} alarm)`, async (t) => {
  const f = dayFixture(t);
  await f.delta("old-pending");
  if (expired) await f.ctx.storage.setAlarm(Date.now() - 86400000);
  else await f.ctx.storage.deleteAlarm();
  const restarted = new DayAggregate(f.ctx, f.env);
  const result = await restarted.compact("2026-01-01");
  assert.equal(result.reason, "notification pending");
  assert.equal(await f.ctx.storage.getAlarm(), Date.now() + 60000);
  assert.equal(restarted.meta("notification").revision, 1);
  await restarted.alarm();
  assert.equal(f.notifications.length, 1);
  assert.equal(restarted.meta("notification"), null);
});

test("unchanged and stale day observations preserve revisions and earlier legacy alarms", async (t) => {
  const f = dayFixture(t), stage = { id: "1", attempt: 1, day: "2026-10-27",
    status: "completed", updated: Date.now() / 1000, batch: 1, testedHead: "a", members: [1] };
  await f.ctx.storage.setAlarm(Date.now() + 1000);
  await post(f.aggregate, "/staging", stage);
  assert.equal(await f.ctx.storage.getAlarm(), Date.now() + 1000);
  await f.aggregate.alarm();
  const revision = f.aggregate.meta("revision"), before = changes(f.ctx);
  await post(f.aggregate, "/staging", stage);
  await post(f.aggregate, "/staging", { ...stage, status: "queued", updated: stage.updated - 60 });
  assert.equal(changes(f.ctx), before);
  assert.equal(f.aggregate.meta("revision"), revision);
  assert.equal(await f.ctx.storage.getAlarm(), null);
  const observation = { day: "2026-10-27", at: Date.now() / 1000, count: 2, complete: true };
  await post(f.aggregate, "/observation", observation);
  const observed = changes(f.ctx), nextRevision = f.aggregate.meta("revision");
  await post(f.aggregate, "/observation", observation);
  await post(f.aggregate, "/landing", { day: "2026-10-27", pr: 1, at: observation.at, revision: 2 });
  const landed = f.aggregate.meta("revision");
  await post(f.aggregate, "/unlanding", { day: "2026-10-27", pr: 1, revision: 1 });
  assert.equal(f.aggregate.meta("revision"), landed);
  assert.equal(landed, nextRevision + 1);
  assert.ok(changes(f.ctx) > observed);
  assert.equal(f.aggregate.sql.exec("SELECT COUNT(*) n FROM landings").one().n, 1);
});

test("raw fact retirement preserves applied metrics without creating another archive", async (t) => {
  const f = fixture(t, 1);
  f.run.status = 'completed';
  f.run.conclusion = 'success';
  f.run.updated = f.run.created + 240;
  f.ctx.storage.sql.exec('UPDATE runs SET data=?', JSON.stringify(f.run));
  const old = JSON.parse(f.ctx.storage.sql.exec('SELECT data FROM jobs').one().data);
  f.fact.replaceJob(f.run, { ...old, status: 'completed', conclusion: 'success',
    started: f.run.created + 60, completed: f.run.created + 240, assigned: true }, Date.now() / 1000);
  await f.catchUp();
  const before = [...f.days.values()].map((day) => day.summary().daily);
  assert.ok(before.some((daily) => daily['minutes/github'] === 3));
  f.advance(91 * 86400);
  const result = await (await f.fact.fetch(new Request('https://internal/archive', {
    method: 'POST', body: JSON.stringify({ builds: true }),
  }))).json();
  assert.equal(result.done, true);
  assert.equal(f.ctx.storage.sql.exec('SELECT COUNT(*) n FROM jobs').one().n, 0);
  assert.equal(f.ctx.storage.sql.exec('SELECT COUNT(*) n FROM runs').one().n, 0);
  assert.deepEqual([...f.days.values()].map((day) => day.summary().daily), before);
});

test("legacy cold days acknowledge late writes after their detail archives expire", async (t) => {
  const reads = [], writes = new Map(), date = '2026-01-01';
  const summary = { daily: { 'pr-total': 10 }, landings: [{ pr: 1 }], fleet: [], observations: [], staging: [] };
  const env = { ARCHIVE: {
    get: async (key) => {
      reads.push(key);
      return { body: new Response(writes.get(key) || JSON.stringify(summary)).body,
        json: async () => JSON.parse(writes.get(key) || JSON.stringify(summary)) };
    },
    put: async (key, data) => writes.set(key, data),
  } };
  const day = new DayAggregate(context(t), env);
  day.set('cold', { pages: ['expired-detail-page'] });
  const response = await day.fetch(new Request('https://internal/delta', {
    method: 'POST', body: JSON.stringify({ day: date, id: 'late', rows: [] }),
  }));
  assert.equal((await response.json()).closed, true);
  for (let i = 0; i < 5; i++) if ((await day.compact(date)).done) break;
  assert.ok(reads.every((key) => key.startsWith('summaries/')));
  const saved = JSON.parse(writes.get(`summaries/${date}.json`));
  assert.equal(saved.daily['pr-total'], 10);
  assert.equal(saved.daily['landed-prs'], 1);
  assert.deepEqual(saved.landings, []);
});

test("a correction during summary upload delays closure and remains in the saved total", async (t) => {
  let release, entered;
  const paused = new Promise((resolve) => { entered = resolve; });
  const resume = new Promise((resolve) => { release = resolve; });
  let first = true, saved;
  const env = {
    ARCHIVE: { put: async (_key, data) => {
      if (first) { first = false; entered(); await resume; }
      saved = JSON.parse(data);
    } },
    CONTROL: { idFromName: (id) => id, get: () => ({ fetch: async () => Response.json({}) }) },
  };
  const aggregate = new DayAggregate(context(t), env), date = '2026-01-01';
  const delta = async (id, amount) => aggregate.fetch(new Request('https://internal/delta', {
    method: 'POST', body: JSON.stringify({ id, day: date,
      rows: [{ bucket: 0, metric: 'pr-total', value: 0, amount }] }),
  }));
  await delta('initial', 10);
  await aggregate.alarm();
  const closing = aggregate.compact(date);
  await paused;
  await delta('racing', 1);
  release();
  assert.equal((await closing).reason, 'concurrent correction');
  assert.ok(!aggregate.meta('closed'));
  await aggregate.alarm();
  for (let i = 0; i < 5; i++) if ((await aggregate.compact(date)).done) break;
  assert.equal(saved.daily['pr-total'], 11);
});

test("a burst over drain capacity settles without regenerating interval updates", async (t) => {
  const f = fixture(t, 100);
  await f.catchUp();
  await f.wake();
  assert.equal((await f.backlog()).outbox, 60);
  const sequence = (await f.backlog()).outbox_sequence;
  await f.wake();
  assert.equal((await f.backlog()).outbox, 20);
  await f.wake();
  assert.equal((await f.backlog()).outbox, 0);
  assert.equal((await f.backlog()).outbox_sequence, sequence);
  const refreshed = (await f.backlog()).last_refreshed;
  await f.wake();
  assert.equal((await f.backlog()).last_refreshed, refreshed + 300);
  assert.equal((await f.backlog()).outbox, 60);
});

for (const operationsOnly of [false, true]) test(
  `continued observations preserve an overdue ${operationsOnly ? "operations" : "control"} notification`, async (t) => {
    const f = fixture(t, 0), notices = [];
    if (operationsOnly) {
      await f.fact.flush(true); // Existing Control eligibility is already acknowledged.
      f.env.OPERATIONS = { idFromName: id => id, get: () => ({ fetch: async (_url, options) => {
        notices.push(JSON.parse(options.body));
        return Response.json({ ok: true });
      } }) };
    }
    const observe = (delivery) => post(f.fact, "/event", {
      repo: f.run.repo, partition: f.fact.meta("partition"), delivery,
      run: rawRun(f.run, {head_branch: delivery, updated_at: new Date(Date.now()).toISOString()}),
    });
    await observe("first");
    const deadline = await f.ctx.storage.getAlarm();
    assert.equal(deadline, Date.now() + 1000);
    f.advance(2); // Alarm delivery lags while another real observation arrives.
    await observe("second");
    assert.equal(await f.ctx.storage.getAlarm(), deadline);
    assert.equal(notices.length, 0, "ingestion must not enter a reentrant notification chain");
    await f.fact.alarm();
    if (operationsOnly) {
      assert.equal(notices.length, 1);
      assert.equal(notices[0].revision, f.fact.meta("operations_revision"));
    } else assert.equal(f.notifications.length, 1);
  },
);

test("frequent notifications do not postpone the persisted refresh deadline", async (t) => {
  const f = fixture(t, 1);
  await f.catchUp();
  const deadline = (await f.backlog()).refresh_due;
  for (let i = 0; i < 20; i++) {
    f.advance(2);
    // A pending retry still wakes promptly without advancing the refresh.
    f.fact.set("partition_pending", f.fact.meta("partition_revision"));
    await f.fact.flush();
    await f.wake();
  }
  assert.equal((await f.backlog()).last_refreshed, null);
  await f.wake();
  assert.equal((await f.backlog()).last_refreshed, deadline);
  assert.equal((await f.backlog()).refresh_due, deadline + 300);
});

test("a persisted full-page cursor resumes after draining and reaches later jobs", async (t) => {
  const f = fixture(t, 1001);
  await f.catchUp();
  await f.wake();
  assert.equal((await f.backlog()).outbox, 960);
  assert.equal((await f.backlog()).refresh_cursor, "100999");
  const sequence = (await f.backlog()).outbox_sequence;
  for (let i = 0; i < 24; i++) await f.wake();
  assert.equal((await f.backlog()).outbox, 0);
  assert.equal((await f.backlog()).outbox_sequence, sequence);
  assert.equal((await f.backlog()).refresh_cursor, "100999");
  await f.wake();
  assert.equal((await f.backlog()).refresh_cursor, null);
  assert.equal((await f.backlog()).outbox_sequence, sequence + 1);
  assert.equal((await f.backlog()).refresh_due, Date.now() / 1000 + 300);
});

test("a lost acknowledgement retries original receipts without double-counting", async (t) => {
  const f = fixture(t, 100);
  f.loseAcknowledgement();
  await assert.rejects(f.fact.alarm(), /lost acknowledgement/);
  assert.equal((await f.backlog()).outbox, 100);
  assert.equal((await f.backlog()).outbox_sequence, 100);
  await f.wake();
  assert.equal((await f.backlog()).outbox, 60);
  // Even an expired refresh deadline must not produce more work while draining.
  f.advance(120);
  await f.fact.alarm();
  assert.equal((await f.backlog()).outbox, 20);
  while ((await f.backlog()).outbox) await f.wake();
  assert.equal((await f.backlog()).outbox_sequence, 100);
  const aggregate = f.days.get("2026-10-27");
  assert.equal(aggregate.sql.exec("SELECT COUNT(*) n FROM receipts").one().n, 100);
  const peaks = aggregate.summary().fleet.filter((b) => b.step === 600).map((b) => b.queued.github || 0);
  assert.equal(Math.max(...peaks), 100);
});

test("an old deployed cursor and backlog drain without requiring a schema migration", async (t) => {
  const f = fixture(t, 1001);
  f.fact.set("alarm_cursor", "100999");
  await f.fact.alarm();
  assert.equal((await f.backlog()).outbox_sequence, 1001);
  assert.equal((await f.backlog()).refresh_cursor, "100999");
  while ((await f.backlog()).outbox) await f.wake();
  await f.wake();
  assert.equal((await f.backlog()).refresh_cursor, null);
  assert.equal((await f.backlog()).outbox_sequence, 1002);
});

test("a concurrent event can replay in-flight receipts without losing its new delta", async (t) => {
  const f = fixture(t, 100);
  await f.catchUp();
  let applied, release;
  const received = new Promise((resolve) => { applied = resolve; });
  const held = new Promise((resolve) => { release = resolve; });
  f.holdDelivery(() => { applied(); return held; });
  const alarm = f.wake();
  await received;
  const response = await f.fact.fetch(new Request("https://internal/event", {
    method: "POST",
    body: JSON.stringify({
      partition: "TauCetiProject/TauCeti/2026-10-27", delivery: "concurrent-event",
      jobs: [{ id: "200000", run_id: "100", run_attempt: 1, name: "sandboxed-build",
        status: "queued", created_at: new Date(Date.now() - 10000).toISOString(),
        labels: ["ubuntu-latest"], steps: [] }],
    }),
  }));
  assert.equal(response.status, 200);
  release();
  await alarm;
  while ((await f.backlog()).outbox) await f.wake();
  assert.equal((await f.backlog()).outbox_sequence, 201);
  const aggregate = f.days.get("2026-10-27");
  assert.equal(aggregate.sql.exec("SELECT COUNT(*) n FROM receipts").one().n, 201);
  const peaks = aggregate.summary().fleet.filter((b) => b.step === 600).map((b) => b.queued.github || 0);
  assert.equal(Math.max(...peaks), 101);
});

test("backlog diagnostics cap counts while retaining a receipt-span upper bound", async (t) => {
  const f = fixture(t, 0);
  f.ctx.storage.transactionSync(() => {
    for (let i = 0; i < 10020; i++) f.ctx.storage.sql.exec(
      "INSERT INTO outbox(day,data) VALUES (?,?)", "2026-10-27", "[]",
    );
    f.ctx.storage.sql.exec("DELETE FROM outbox WHERE id=2");
  });
  const status = await f.backlog();
  assert.equal(status.outbox, 10000);
  assert.equal(status.outbox_count_exact, false);
  assert.equal(status.outbox_upper_bound, 10020);
  assert.equal(status.oldest.id, 1);
  assert.equal(status.outbox_sequence, 10020);
});

test("an orphaned outbox resumes through its normal alarm without adding receipts", async (t) => {
  const f = fixture(t, 100);
  assert.equal((await f.backlog()).next_alarm, null);
  // Production retained a failed alarm timestamp more than three days old.
  await f.ctx.storage.setAlarm(Date.now() - 3 * 86400000);
  const resume = async () => (await f.fact.fetch(new Request("https://internal/resume-drain", {
    method: "POST", body: "{}",
  }))).json();
  assert.deepEqual(await resume(), { scheduled: true });
  const scheduled = (await f.backlog()).next_alarm;
  assert.equal(scheduled, Date.now() / 1000 + 1);
  assert.deepEqual(await resume(), { scheduled: true });
  assert.equal((await f.backlog()).next_alarm, scheduled);
  while ((await f.backlog()).outbox) await f.wake();
  assert.equal((await f.backlog()).outbox_sequence, 100);
  assert.deepEqual(await resume(), { scheduled: false });
});

test("discard removes only pending historical metrics and never reuses their receipt IDs", async (t) => {
  const f = fixture(t, 100);
  const request = () => f.fact.fetch(new Request("https://internal/discard-outbox-batch", {
    method: "POST", body: "{}",
  }));
  assert.equal((await request()).status, 409);
  assert.equal((await f.backlog()).outbox, 100);
  f.ctx.storage.sql.exec("UPDATE jobs SET active=0");
  assert.equal((await request()).status, 409); // Active parent alone also blocks deletion.
  f.run.status = "completed";
  f.ctx.storage.sql.exec("UPDATE runs SET data=?", JSON.stringify(f.run));
  const factsBefore = f.ctx.storage.sql.exec("SELECT * FROM jobs ORDER BY id").toArray();
  await f.ctx.storage.setAlarm(Date.now() + 1000);
  assert.deepEqual(await (await request()).json(), { deleted: 100, through: 100, remaining: false });
  const status = await f.backlog();
  assert.equal(status.outbox, 0);
  assert.equal(status.next_alarm, null);
  assert.equal(status.outbox_sequence, 100);
  assert.equal(status.discarded.deleted, 100);
  assert.deepEqual(f.ctx.storage.sql.exec("SELECT * FROM jobs ORDER BY id").toArray(), factsBefore);
  assert.deepEqual(await (await request()).json(), { deleted: 0, through: 100, remaining: false });
  assert.deepEqual((await f.backlog()).discarded, status.discarded);
  // An unchanged later observation does not replay the dropped contributions.
  const raw = JSON.parse(factsBefore[0].data);
  f.fact.replaceJob(f.run, raw, Date.now() / 1000);
  assert.equal((await f.backlog()).outbox, 0);
  f.fact.replaceJob(f.run, { ...raw, id: "900000" }, Date.now() / 1000);
  assert.ok((await f.backlog()).oldest.id > 100);
});

test("large discards make bounded progress and retain cumulative deletion accounting", async (t) => {
  const f = fixture(t, 0);
  f.run.status = "completed";
  f.ctx.storage.sql.exec("UPDATE runs SET data=?", JSON.stringify(f.run));
  f.ctx.storage.transactionSync(() => {
    for (let i = 0; i < 100003; i++) f.ctx.storage.sql.exec(
      "INSERT INTO outbox(day,data) VALUES (?,?)", "2026-10-27", "[]",
    );
  });
  const request = async () => (await f.fact.fetch(new Request("https://internal/discard-outbox-batch", {
    method: "POST", body: "{}",
  }))).json();
  assert.deepEqual(await request(), { deleted: 100000, through: 100003, remaining: true });
  assert.equal((await f.backlog()).outbox, 3);
  assert.deepEqual(await request(), { deleted: 3, through: 100003, remaining: false });
  assert.equal((await f.backlog()).discarded.deleted, 100003);
  assert.equal((await f.backlog()).outbox_sequence, 100003);
  assert.equal((await f.backlog()).next_alarm, null);
});

test("operations notifications carry job-only changes from alarms and cannot block metric drains", async t => {
  const f = fixture(t, 1), sent = [];
  f.env.OPERATIONS = { idFromName: id => id, get: () => ({ fetch: async (url, options) => {
    sent.push({ path: new URL(url).pathname, body: JSON.parse(options.body) });
    return Response.json({ ok: true });
  } }) };
  const row = f.ctx.storage.sql.exec("SELECT data FROM jobs LIMIT 1").one();
  const job = JSON.parse(row.data), run = f.fact.run(job.run, job.attempt);
  f.fact.replaceJob(run, { ...job, runnerId: "99", checkRunId: "77", runnerName: "assigned-runner" }, Date.now()/1000);
  assert.equal(sent.length, 0, "no reentrant Facts→Operations call during ingestion");
  await f.fact.alarm();
  assert.equal(sent[0].path, "/partition"); assert.ok(sent[0].body.revision > 0);
  const observation = await (await f.fact.fetch(new Request("https://internal/operations", { method: "POST", body: "{}" }))).json();
  assert.equal(observation.jobs[0].runner_id, "99"); assert.equal(observation.jobs[0].check_run_id, "77");
  f.env.OPERATIONS.get = () => ({ fetch: async () => { throw new Error("observer unavailable"); } });
  f.fact.replaceJob(run, { ...job, runnerId: "100" }, Date.now()/1000);
  await f.fact.alarm();
  assert.ok(f.fact.meta("operations_pending"), "failed observer notification remains retryable");
});

test("operations pool activity counts retained facts beyond the public list and follows source corrections", async t => {
  const f = fixture(t, 1201), stamp = Date.now() / 1000;
  const originals = f.ctx.storage.sql.exec("SELECT data FROM jobs").toArray().map(r => JSON.parse(r.data));
  for (const j of originals) f.fact.replaceJob(f.run, { ...j, status: "completed", conclusion: "success", completed: stamp - 120 }, stamp);
  const read = async () => (await f.fact.fetch(new Request("https://internal/operations", { method: "POST", body: "{}" }))).json();
  let result = await read();
  assert.equal(result.jobs.length, 1100);
  assert.equal(result.truncated, true);
  assert.equal(result.activity.complete, false);
  for(let i=0;i<12;i++) result = await read();
  assert.equal(result.activity.complete, true);
  const all = result.activity.pools.find(p => p.pool === "github");
  assert.equal(all.bins.reduce((n,b) => n + b[1], 0),1201);
  assert.equal(all.all.running,0); assert.equal(all.all.waiting,0);
  f.fact.replaceJob(f.run, { ...originals[0], status: "completed", conclusion: "failure", completed: stamp - 120, updated: stamp + 1 }, stamp + 1);
  f.fact.replaceJob(f.run, { ...originals[0], id: "900000", status: "in_progress", assigned: true, started: stamp - 60, completed: null, updated: stamp + 1 }, stamp + 1);
  result = await read();
  const corrected = result.activity.pools.find(p => p.pool === "github");
  assert.equal(corrected.bins.reduce((n,b) => n + b[1],0),1200);
  assert.equal(corrected.builds.running,1);
  assert.equal(result.backlog.running_jobs,1);
  assert.equal(result.backlog.pr_builds.in_progress,1);
});

test('an empty job enumeration is invalidated when a live workflow advances',async t=>{
 const f=fixture(t,0);
 assert.equal((await post(f.fact,'/verified',{id:'100',attempt:1})).verified,true);
 assert.equal((await post(f.fact,'/status',{id:'100',attempt:1})).verified,true);
 await post(f.fact,'/event',{repo:f.run.repo,partition:f.fact.meta('partition'),delivery:'pending-next-job',
  run:rawRun(f.run,{status:'pending',updated_at:new Date((f.run.updated+1)*1000).toISOString()})});
 assert.equal((await post(f.fact,'/status',{id:'100',attempt:1})).verified,false);
});
