/** Scoped host observations: explicit fields, aliases and permanent identities. */
import { object, integer, source, string } from "./operations-model";
export type PublicHost = { machine_id: string; alias: string };
export function publicHosts(raw: string | undefined): PublicHost[] {
  const rows = JSON.parse(raw || "[]");
  if (!Array.isArray(rows) || rows.length > 100)
    throw new Error("Invalid public host scope");
  const ids = new Set(),
    names = new Set();
  for (const row of rows) {
    object(row, "machine_id alias");
    if (
      !/^[a-f0-9]{32}$/.test(row.machine_id) ||
      !/^[A-Za-z0-9_.-]{1,60}$/.test(row.alias) ||
      ids.has(row.machine_id) ||
      names.has(row.alias)
    )
      throw new Error("Invalid public registry");
    ids.add(row.machine_id);
    names.add(row.alias);
  }
  return rows;
}
function stamp(v: unknown) {
  if (
    v !== null &&
    (typeof v !== "string" || v.length > 40 || !Number.isFinite(Date.parse(v)))
  )
    throw new Error("Invalid time");
}
function num(v: unknown) {
  if (
    v !== null &&
    (typeof v !== "number" || !Number.isFinite(v) || v < 0 || v > 1e18)
  )
    throw new Error("Invalid resource");
}
function flag(v: unknown, nullable = false) {
  if (!(nullable && v === null) && typeof v !== "boolean")
    throw new Error("Invalid flag");
}
function rows(v: unknown): asserts v is Record<string, any>[] {
  if (!Array.isArray(v) || v.length > 128) throw new Error("Invalid list");
}
export function hostFrame(value: any, registry: PublicHost[]) {
  object(value, "schema source observed_at hosts");
  if (value.schema !== "tauceti.operations.hosts-update/v1")
    throw new Error("Invalid host schema");
  source(value.source);
  stamp(value.observed_at);
  rows(value.hosts);
  if (value.hosts.length !== registry.length)
    throw new Error("Registry coverage differs");
  const seen = new Set();
  for (const host of value.hosts) {
    object(
      host,
      "machine_id alias observed_at transport_at connected coverage source_revision controller_error ci_enabled workers_enabled ci_max_jobs worker_limit resources cpu_idle_fraction memory_pressure compiler slots workers slots_truncated workers_truncated admission activity capacity_context",
    );
    if (
      !registry.some(
        (r) => r.machine_id === host.machine_id && r.alias === host.alias,
      ) ||
      seen.has(host.machine_id)
    )
      throw new Error("Host is not public");
    seen.add(host.machine_id);
    stamp(host.observed_at);
    stamp(host.transport_at);
    string(host.source_revision, 64);
    if (host.source_revision && !/^[a-f0-9]{40,64}$/.test(host.source_revision))
      throw new Error("Invalid public revision");
    if (!["observed", "unavailable"].includes(host.coverage))
      throw new Error("Invalid host coverage");
    for (const key of [
      "connected",
      "controller_error",
      "slots_truncated",
      "workers_truncated",
    ])
      flag(host[key]);
    for (const key of ["ci_enabled", "workers_enabled", "memory_pressure"])
      flag(host[key], true);
    if (host.capacity_context !== undefined) {
      const c = host.capacity_context;
      object(c, "observed_at scope coverage observer_error running_jobs");
      stamp(c.observed_at);
      if (
        c.scope !== "organization_all_workflow_jobs" ||
        !["observed", "unavailable"].includes(c.coverage)
      )
        throw new Error("Invalid capacity scope");
      flag(c.observer_error);
      object(c.running_jobs, "github namespace self-hosted");
      for (const v of Object.values(c.running_jobs)) if (v !== null) integer(v);
    }
    if (host.admission !== undefined) {
      object(
        host.admission,
        "ci_kinds staging_slots memory_budget_gib reservation_error",
      );
      const a = host.admission;
      if (
        a.ci_kinds !== null &&
        !['["pr"]', '["staging"]', '["pr","staging"]'].includes(
          JSON.stringify(a.ci_kinds),
        )
      )
        throw new Error("Invalid CI classes");
      num(a.staging_slots);
      num(a.memory_budget_gib);
      flag(a.reservation_error);
    }
    if (host.activity !== undefined) {
      object(
        host.activity,
        "observed_at window_hours finished_rounds failed_rounds unknown_rounds",
      );
      stamp(host.activity.observed_at);
      if (host.activity.window_hours !== 24)
        throw new Error("Invalid activity window");
      for (const k of ["finished_rounds", "failed_rounds", "unknown_rounds"])
        num(host.activity[k]);
    }
    num(host.ci_max_jobs);
    num(host.worker_limit);
    num(host.cpu_idle_fraction);
    object(host.resources, "cpus memory_total memory_available");
    for (const key of ["cpus", "memory_total", "memory_available"])
      num(host.resources[key]);
    object(host.compiler, "observed_at waiting_guests guests healthy");
    stamp(host.compiler.observed_at);
    num(host.compiler.waiting_guests);
    num(host.compiler.guests);
    flag(host.compiler.healthy);
    rows(host.slots);
    rows(host.workers);
    for (const slot of host.slots) {
      object(
        slot,
        "slot instance_id runner_id runner_name phase busy started_at vcpus memory_gib compiler",
      );
      num(slot.slot);
      string(slot.runner_name, 100);
      string(slot.phase, 40);
      flag(slot.busy);
      stamp(slot.started_at);
      num(slot.vcpus);
      num(slot.memory_gib);
      if (slot.compiler !== undefined && slot.compiler !== null) {
        object(
          slot.compiler,
          "observed_at waiting_now running_now completed oldest_wait wait_seconds run_seconds lost",
        );
        stamp(slot.compiler.observed_at);
        for (const k of [
          "waiting_now",
          "running_now",
          "completed",
          "oldest_wait",
          "wait_seconds",
          "run_seconds",
          "lost",
        ])
          num(slot.compiler[k]);
      }
      if (slot.instance_id !== null && !/^[a-f0-9]{16}$/.test(slot.instance_id))
        throw new Error("Invalid VM identity");
      if (
        slot.runner_id !== null &&
        (typeof slot.runner_id !== "string" ||
          !/^\d{1,20}$/.test(slot.runner_id))
      )
        throw new Error("Invalid runner identity");
    }
    for (const worker of host.workers) {
      object(worker, "id state alive observed_at");
      string(worker.id, 100);
      string(worker.state, 40);
      flag(worker.alive);
      stamp(worker.observed_at);
    }
  }
  return value;
}
