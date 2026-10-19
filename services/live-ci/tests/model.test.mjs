import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSync } from "esbuild";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
const output = buildSync({
  entryPoints: ["src/model.ts"],
  bundle: true,
  format: "esm",
  platform: "node",
  write: false,
}).outputFiles[0].text;
const m = await import(
  "data:text/javascript;base64," + Buffer.from(output).toString("base64")
);
const t = Date.parse("2026-10-18T12:00:00Z") / 1000;
const run = {
  repo: "TauCetiProject/TauCeti",
  id: "1",
  attempt: 1,
  workflow: m.WORKFLOW,
  event: "pull_request",
  trigger: "pr",
  status: "completed",
  conclusion: "success",
  created: t,
  started: t,
  updated: t + 300,
  head: "a",
  branch: "main",
  url: "",
};
const job = {
  id: "1",
  run: "1",
  attempt: 1,
  name: "sandboxed-build",
  status: "completed",
  conclusion: "success",
  created: t,
  started: t + 60,
  completed: t + 300,
  kind: "github",
  assigned: true,
  steps: [],
  weight: 1,
  estimated: false,
  updated: t + 300,
};
function apply(state, rows) {
  for (const r of rows) {
    const key = JSON.stringify([r.day, r.bucket, r.metric, r.value]);
    const v = state.get(key) || { ...r, amount: 0 };
    v.amount += r.amount;
    if (Math.abs(v.amount) < 1e-8) state.delete(key);
    else state.set(key, v);
  }
}
test("incremental corrections equal a complete recomputation across midnight and reruns", () => {
  const facts = new Map(),
    incremental = new Map();
  for (let i = 0; i < 200; i++) {
    const j = {
      ...job,
      id: String(i),
      attempt: 1 + (i % 3),
      created: t - 60000 + i * 30,
      started: t - 59900 + i * 30,
      completed: t - 59500 + i * 30,
    };
    facts.set(j.id, j);
    apply(incremental, m.contributions(run, j, t + 600));
  }
  for (let i = 0; i < 200; i += 3) {
    const before = facts.get(String(i)),
      after = {
        ...before,
        completed: before.completed + 90,
        conclusion: i % 2 ? "failure" : "cancelled",
        failure: "lint-env",
      };
    apply(
      incremental,
      m.difference(
        m.contributions(run, before, t + 600),
        m.contributions(run, after, t + 600),
      ),
    );
    facts.set(after.id, after);
  }
  const recomputed = new Map();
  for (const j of facts.values())
    apply(recomputed, m.contributions(run, j, t + 600));
  assert.deepEqual(
    [...incremental.keys()].sort(),
    [...recomputed.keys()].sort(),
  );
  for (const [key, value] of incremental)
    assert.ok(Math.abs(value.amount - recomputed.get(key).amount) < 1e-8, key);
});
test("queue peaks apply departures before arrivals at matching boundaries", () => {
  const rows = [];
  for (const j of [
    { ...job, created: t, started: t + 60, completed: t + 120 },
    { ...job, id: "2", created: t + 60, started: t + 120, completed: t + 180 },
  ])
    rows.push(...m.contributions(run, j, t + 600));
  const combined = new Map();
  apply(combined, rows);
  const result = m.summarize([...combined.values()]);
  const bin = result.fleet.find((b) => b.at === t && b.step === 600);
  assert.equal(bin.queued.github, 1);
  assert.ok(Math.abs(bin.running["gh-build-pr"] - 0.2) < 1e-8);
  assert.equal(bin.wait.build, 1);
});
test("active intervals extend, then retract correctly after a late completion", () => {
  const active = {
    ...job,
    status: "in_progress",
    conclusion: null,
    completed: null,
  };
  const running = { ...run, status: "in_progress", conclusion: null };
  const initial = m.contributions(running, active, t + 600),
    later = m.contributions(running, active, t + 1200);
  const state = new Map();
  apply(state, initial);
  apply(state, m.difference(initial, later));
  const terminal = { ...job, completed: t + 900 };
  apply(state, m.difference(later, m.contributions(run, terminal, t + 1500)));
  const expected = new Map();
  apply(expected, m.contributions(run, terminal, t + 1500));
  assert.deepEqual([...state].sort(), [...expected].sort());
});
test("a terminal parent bounds an unobserved job until its exact completion arrives", () => {
  const active = {
    ...job,
    status: "in_progress",
    conclusion: null,
    completed: null,
  };
  const bounded = m.contributions(run, active, t + 600);
  assert.deepEqual(bounded, m.contributions(run, active, t + 1200));
  assert.equal(m.summarize(bounded).daily["duration/github"], undefined);
  const actual = { ...job, completed: t + 240 };
  const state = new Map();
  apply(state, bounded);
  apply(state, m.difference(bounded, m.contributions(run, actual, t + 1200)));
  const expected = new Map();
  apply(expected, m.contributions(run, actual, t + 1200));
  assert.deepEqual([...state].sort(), [...expected].sort());
  assert.equal(
    m.summarize([...state.values()]).daily["duration/github"].median,
    3,
  );
});
test("failure denominator excludes cancelled builds and includes timeouts", () => {
  const rows = [];
  for (const conclusion of ["success", "failure", "timed_out", "cancelled"])
    rows.push(
      ...m.contributions(
        run,
        { ...job, conclusion, failure: "lint-env" },
        t + 600,
      ),
    );
  const state = new Map();
  apply(state, rows);
  const d = m.summarize([...state.values()]).daily;
  assert.equal(d["pr-total"], 3);
  assert.equal(d["failure/timeout"], 1);
  assert.equal(d["failure/lint-env"], 1);
  assert.equal(d["picker/github"], 4);
});
test("unobserved historical estimates contribute running time without queue or wait claims", () => {
  const result = m.summarize(
    m.contributions(
      { ...run, repo: "TauCetiProject/TauCetiWorker" },
      { ...job, name: "estimated auxiliary job", estimated: true },
      t + 600,
    ),
  );
  assert.equal(result.daily["wait/other"], undefined);
  assert.equal(
    result.fleet.some((b) => b.queued.github > 0),
    false,
  );
  assert.ok(result.fleet.some((b) => b.running["gh-other"] > 0));
});
test("failure categories match the existing Python classifier", async () => {
  const failure = buildSync({
    entryPoints: ["src/failure.ts"],
    bundle: true,
    format: "esm",
    platform: "node",
    write: false,
  }).outputFiles[0].text;
  const { classify } = await import(
    "data:text/javascript;base64," + Buffer.from(failure).toString("base64")
  );
  const cases = [
    ["failure", "Build exact candidate", ["LINT-ENV: FAIL"]],
    ["failure", "Build exact candidate", ["Lean exited with code 137"]],
    ["failure", "Mathlib cache", null],
    ["failure", "Build exact candidate", null],
    ["timed_out", "Build", []],
    ["failure", "Build", ["✔ Built KilledByRank"]],
  ];
  const reference = spawnSync(
    "python3",
    [
      "-c",
      'import json,sys;sys.path.insert(0,"../..");from collector.classify import classify;print(json.dumps([classify(*x) for x in json.load(sys.stdin)]))',
    ],
    { input: JSON.stringify(cases), encoding: "utf8" },
  );
  assert.equal(reference.status, 0, reference.stderr);
  assert.deepEqual(
    cases.map((c) => classify(...c)),
    JSON.parse(reference.stdout),
  );
});
test("10x event throughput changes work linearly, independently of retained history", () => {
  const measure = (n, history) => {
    const state = new Map();
    // Old daily counters stand for an arbitrary amount of already processed data.
    for (let i = 0; i < history; i++) state.set("old/" + i, { amount: 1 });
    let touched = 0;
    const start = performance.now();
    for (let i = 0; i < n; i++) {
      const rows = m.contributions(run, { ...job, id: String(i) }, t + 600);
      touched += rows.length;
      apply(state, rows);
    }
    return {
      events: n,
      retained: history,
      touched,
      elapsed_ms: Math.round(performance.now() - start),
    };
  };
  const one = measure(1000, 1000),
    ten = measure(10000, 1000),
    history = measure(1000, 100000);
  assert.equal(ten.touched, one.touched * 10);
  assert.equal(history.touched, one.touched);
  console.log("incremental benchmark", JSON.stringify({ one, ten, history }));
});
test("archived production cohort matches the existing daily report metrics", () => {
  const fixture = JSON.parse(readFileSync("tests/reference.json", "utf8")),
    state = new Map();
  for (const event of fixture.events) {
    const r = m.normalizeRun(event.repo, event.run);
    r.telemetry = event.telemetry;
    for (const raw of event.jobs)
      apply(
        state,
        m.contributions(
          r,
          m.normalizeJob(raw),
          Date.parse("2026-10-19T00:00:00Z") / 1000,
        ),
      );
  }
  const actual = m.summarize(
    [...state.values()].filter((r) => r.day === fixture.day),
  ).daily;
  for (const [key, value] of Object.entries(fixture.expected)) {
    if (typeof value === "number")
      assert.ok(
        Math.abs((actual[key] || 0) - value) < 1e-7,
        `${key}: ${actual[key]} vs ${value}`,
      );
    else assert.deepEqual(actual[key], value, key);
  }
});
test("archived production fleet bins match the existing sweep-line report", () => {
  const f = JSON.parse(readFileSync("tests/reference.json", "utf8")),
    state = new Map();
  for (const event of f.events) {
    const r = m.normalizeRun(event.repo, event.run);
    for (const raw of event.jobs)
      apply(
        state,
        m.contributions(
          r,
          m.normalizeJob(raw),
          Date.parse("2026-10-19T00:00:00Z") / 1000,
        ),
      );
  }
  const actual = new Map(
    m
      .summarize([...state.values()])
      .fleet.filter((b) => b.step === 600)
      .map((b) => [b.at, b]),
  );
  for (let i = 0; i < 144; i++) {
    const b = actual.get(f.fleet.since + i * 600) || {};
    for (const [type, expected] of [
      ["running", f.fleet.running],
      ["queued", f.fleet.queued],
      ["wait", f.fleet.wait_min],
    ])
      for (const [key, values] of Object.entries(expected)) {
        assert.ok(
          Math.abs((b[type]?.[key] || 0) - values[i]) < 1e-7,
          `${type}/${key} bin ${i}: ${b[type]?.[key]} vs ${values[i]}`,
        );
      }
  }
});
test("report SVGs parse and irregular observations keep their actual time positions", async () => {
  const code = buildSync({
    entryPoints: ["src/charts.ts"],
    bundle: true,
    format: "esm",
    platform: "node",
    write: false,
  }).outputFiles[0].text;
  const { renderCharts } = await import(
    "data:text/javascript;base64," + Buffer.from(code).toString("base64")
  );
  const data = [
    {
      day: m.day(t),
      data: {
        daily: {},
        fleet: [],
        observations: [
          { at: t - 3 * m.DAY, count: 1 },
          { at: t - m.DAY, count: 2 },
        ],
        staging: [],
        landings: [],
      },
    },
  ];
  const assets = renderCharts(data, t, [], {}, [
    { since: t - 30 * m.DAY, until: t },
  ]);
  assert.equal(Object.keys(assets).length, 11);
  const positions = [
    ...assets["ci-queue-size.svg"].matchAll(/<circle cx="([\d.]+)"/g),
  ].map((m) => Number(m[1]));
  assert.ok(Math.abs(positions[1] - positions[0] - (864 * 2) / 30) < 1e-7);
  assert.equal(assets["ci-bors-staging.svg"].includes('opacity=".16"'), false);
  const parsed = spawnSync(
    "python3",
    [
      "-c",
      "import json,sys,xml.etree.ElementTree as E;[E.fromstring(s) for s in json.load(sys.stdin).values()]",
    ],
    { input: JSON.stringify(assets), encoding: "utf8" },
  );
  assert.equal(parsed.status, 0, parsed.stderr);
});
