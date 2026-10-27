import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { buildSync } from "esbuild";

const code = buildSync({
  entryPoints: ["src/storage.ts"], bundle: true, platform: "node", format: "esm", write: false,
}).outputFiles[0].text;
const { FactPartition, DayAggregate } = await import(
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

function fixture(t, count) {
  let clock = Date.parse("2026-10-27T12:00:00Z"), loseAcknowledgement = false;
  let holdDelivery;
  t.mock.method(Date, "now", () => clock);
  const ctx = context(t), days = new Map();
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
    CONTROL: { idFromName: (id) => id, get: () => ({ fetch: async () => Response.json({}) }) },
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
    fact, ctx, run, days, backlog,
    advance: (seconds) => { clock += seconds * 1000; },
    loseAcknowledgement: () => { loseAcknowledgement = true; },
    holdDelivery: (hold) => { holdDelivery = hold; },
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
  assert.equal((await f.backlog()).last_refreshed, refreshed + 60);
  assert.equal((await f.backlog()).outbox, 60);
});

test("frequent notifications do not postpone the persisted refresh deadline", async (t) => {
  const f = fixture(t, 1);
  await f.catchUp();
  const deadline = (await f.backlog()).refresh_due;
  for (let i = 0; i < 20; i++) {
    f.advance(2);
    await f.fact.flush(); // An ordinary event schedules its notification.
    await f.wake();
  }
  assert.equal((await f.backlog()).last_refreshed, deadline);
  assert.equal((await f.backlog()).refresh_due, deadline + 60);
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
  assert.equal((await f.backlog()).refresh_due, Date.now() / 1000 + 60);
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
