/** Public operations contracts. Unknown fields are rejected at ingestion, not hidden by the UI. */
export const QUEUE_SCHEMA = "tauceti.operations.queue/v1";
export const UPDATE_SCHEMA = "tauceti.operations.queue-update/v1";
export const COLLECTIONS = [
  "batches",
  "inputs",
  "members",
  "attempts",
  "tasks",
  "events",
] as const;
export type Collection = (typeof COLLECTIONS)[number];
export type Source = { id: string; epoch: number; sequence: number };
export type Change = {
  collection: Collection;
  id: string;
  value: Record<string, any> | null;
};
export type QueueFrame = {
  schema: typeof UPDATE_SCHEMA;
  source: Source;
  snapshot: string;
  base: { epoch: number; sequence: number } | null;
  part: number;
  parts: number;
  changes: Change[];
  metadata?: Record<string, any>;
};
export function canonical(value: any): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value && typeof value === "object")
    return (
      "{" +
      Object.keys(value)
        .sort()
        .map((k) => JSON.stringify(k) + ":" + canonical(value[k]))
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const decimal = /^\d{1,20}$/;
const sha = /^[0-9a-f]{40}$/;
const fields: Record<Collection, string> = {
  batches:
    "id repository version base tip state size created_at sealed_at landed_at first_dispatch_at pre_rebase_state pre_rebase_tip pre_rebase_onto prepared_tip",
  inputs:
    "id repository pr accepted_head state batch_id title approved_at landed_at landed_commit no_repair",
  members:
    "id repository pr batch_id batch_version position accepted_head commit title author approved_at inserted_at landed_at provenance disposition empty removed no_repair withdrawn explanation pr_url commit_url",
  attempts:
    "id batch_id version candidate base run_id run_attempt status conclusion outcome dispatched_at started_at completed_at cancelled_at last_progress_at observed_at runner labels url verdicts phases",
  tasks:
    "id kind batch_id batch_version state resolution ci_attempt_id created_at resolved_at progress",
  events: "id at kind batch_id",
};
const idFields = new Set(["id", "pr", "batch_id", "run_id", "ci_attempt_id"]);
const shaFields = new Set([
  "base",
  "tip",
  "accepted_head",
  "commit",
  "candidate",
  "pre_rebase_tip",
  "pre_rebase_onto",
  "prepared_tip",
  "landed_commit",
]);
const numberFields = new Set([
  "version",
  "batch_version",
  "size",
  "position",
  "run_attempt",
]);
const booleanFields = new Set(["empty", "removed", "no_repair", "withdrawn", "sealed"]);

export function object(
  value: any,
  allowed: string,
): asserts value is Record<string, any> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected object");
  const names = new Set(allowed.split(" "));
  if (Object.keys(value).some((k) => !names.has(k)))
    throw new Error("Unknown public field");
}
export function integer(value: any, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new Error("Invalid integer");
}
export function string(value: any, max = 300, nullable = false): void {
  if (nullable && value === null) return;
  if (
    typeof value !== "string" ||
    Array.from(value).length > max ||
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value)
  )
    throw new Error("Invalid public text");
}
function stamp(value: any) {
  if (
    value !== null &&
    (typeof value !== "string" ||
      value.length > 40 ||
      !Number.isFinite(Date.parse(value)))
  )
    throw new Error("Invalid timestamp");
}
function identity(value: any, nullable = false) {
  if (nullable && value === null) return;
  if (typeof value !== "string" || !decimal.test(value))
    throw new Error("Invalid identity");
}
function hash(value: any) {
  if (value !== null && (typeof value !== "string" || !sha.test(value)))
    throw new Error("Invalid commit");
}
export function source(value: any): asserts value is Source {
  object(value, "id epoch sequence");
  if (typeof value.id !== "string" || !uuid.test(value.id))
    throw new Error("Invalid source");
  integer(value.epoch, 1);
  integer(value.sequence, 1);
}
function list(value: any, max: number): asserts value is any[] {
  if (!Array.isArray(value) || value.length > max)
    throw new Error("Invalid list");
}
function repository(value: any, repositories: string[]) {
  if (typeof value !== "string" || !repositories.includes(value))
    throw new Error("Repository is not public");
}
function link(value: any, repositories: string[]) {
  if (value === null) return;
  string(value, 300);
  const url = new URL(value);
  if (
    url.origin !== "https://github.com" ||
    url.search ||
    url.hash ||
    url.username ||
    url.password
  )
    throw new Error("Invalid public link");
  if (
    !repositories.some((repo) =>
      new RegExp(
        `^/${repo.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/(?:pull/\\d+|commit/[a-f0-9]{40}|actions/runs/\\d+)$`,
      ).test(url.pathname),
    )
  )
    throw new Error("Invalid public link identity");
}
export function record(
  collection: Collection,
  value: any,
  repositories: string[],
) {
  // Optional while older queue publishers still omit the membership seal.
  object(value, fields[collection] + (collection === "batches" ? " sealed" : ""));
  if (fields[collection].split(" ").some((key) => !Object.hasOwn(value, key)))
    throw new Error("Missing public field");
  identity(value.id);
  for (const [key, item] of Object.entries(value)) {
    if (idFields.has(key))
      identity(
        item,
        key === "run_id" ||
          key === "ci_attempt_id" ||
          (key === "batch_id" && ["inputs", "events"].includes(collection)),
      );
    else if (shaFields.has(key)) {
      if (
        item === null &&
        ["base", "tip", "candidate", "accepted_head"].includes(key)
      )
        throw new Error("Missing commit identity");
      hash(item);
    } else if (numberFields.has(key)) {
      if (!(key === "run_attempt" && item === null)) integer(item);
    } else if (booleanFields.has(key)) {
      if (!(key === "empty" && item === null) && typeof item !== "boolean")
        throw new Error("Invalid flag");
    } else if (key === "progress") {
      object(
        item,
        "coverage state observed_at seconds providers models models_truncated cost usage build",
      );
      if (!["observed", "unavailable"].includes(item.coverage))
        throw new Error("Invalid task progress");
      if (item.coverage !== "observed" && item.build !== undefined)
        throw new Error("Build requires an observed task");
      stamp(item.observed_at);
      if (item.models_truncated !== undefined && typeof item.models_truncated !== "boolean")
        throw new Error("Invalid model coverage");
      if (item.coverage === "observed") {
        string(item.state, 40);
        if (item.build !== undefined) {
          const b = item.build;
          object(b, "stage completed_tasks total_tasks sampled_at log_updated_at last_progress_at");
          if (!["preparation", "verification", "streaming"].includes(b.stage))
            throw new Error("Invalid task build stage");
          integer(b.completed_tasks, 0, 999999999);
          integer(b.total_tasks, 1, 999999999);
          if (b.completed_tasks > b.total_tasks) throw new Error("Invalid task build counts");
          stamp(b.sampled_at);
          stamp(b.log_updated_at);
          stamp(b.last_progress_at);
          if (b.sampled_at === null || b.log_updated_at === null)
            throw new Error("Missing task build observation time");
        }
        if (
          item.seconds !== null &&
          (!Number.isFinite(item.seconds) || item.seconds < 0)
        )
          throw new Error("Invalid duration");
        list(item.providers, 10);
        item.providers.forEach((v: any) => string(v, 100));
        list(item.models, 100);
        item.models.forEach((v: any) => string(v, 100));
        if (item.usage !== undefined) {
          object(
            item.usage,
            "input_tokens cached_input_tokens output_tokens reasoning_output_tokens total_tokens",
          );
          for (const raw of Object.values(item.usage)) {
            const row = raw as any;
            object(row, "recorded_tokens recorded_attempts unknown_attempts");
            integer(row.recorded_tokens);
            integer(row.recorded_attempts);
            integer(row.unknown_attempts);
          }
        }
        object(
          item.cost,
          "recorded_usd recorded_attempts unknown_attempts coverage",
        );
        if (item.cost.coverage !== "recorded_subtotal_only")
          throw new Error("Invalid cost coverage");
        for (const k of [
          "recorded_usd",
          "recorded_attempts",
          "unknown_attempts",
        ])
          if (!Number.isFinite(item.cost[k]) || item.cost[k] < 0)
            throw new Error("Invalid recorded cost");
      }
    } else if (key.endsWith("_at") || key === "at") stamp(item);
    else if (key === "repository") repository(item, repositories);
    else if (key.endsWith("_url") || key === "url") link(item, repositories);
    else if (key === "labels") {
      list(item, 30);
      item.forEach((v) => string(v, 100));
    } else if (key === "verdicts") {
      object(item, "build scope bump-guard");
      for (const verdict of Object.values(item)) {
        object(verdict, "state bound");
        string(verdict.state, 30);
        if (typeof verdict.bound !== "boolean")
          throw new Error("Invalid verdict binding");
      }
    } else if (key === "phases") {
      list(item, 100);
      for (const phase of item) {
        object(phase, "name status started_at completed_at");
        string(phase.name);
        string(phase.status, 30);
        stamp(phase.started_at);
        stamp(phase.completed_at);
      }
    } else
      string(item, key === "explanation" ? 2000 : 300, key === "conclusion");
  }
  if (collection === "members") {
    if (
      value.pr_url !==
        `https://github.com/${value.repository}/pull/${value.pr}` ||
      value.commit_url !==
        (value.commit
          ? `https://github.com/${value.repository}/commit/${value.commit}`
          : null)
    )
      throw new Error("Member link does not match source identity");
  }
}

export function metadata(value: any, repositories: string[]) {
  object(
    value,
    "schema observed_at source_revision coverage truncated scope service status_observed_at conditions outbox settings claims workers event_history_truncated claim_history_truncated collection_counts collection_complete active_batch_ids retained_batch_ids history canonical_sha dispatch_config",
  );
  if (
    value.schema !== QUEUE_SCHEMA ||
    !["observed", "partial", "unavailable"].includes(value.coverage)
  )
    throw new Error("Invalid queue schema");
  stamp(value.observed_at);
  stamp(value.status_observed_at);
  string(value.source_revision, 40);
  if (
    value.canonical_sha !== undefined &&
    value.canonical_sha !== null &&
    (typeof value.canonical_sha !== "string" || !sha.test(value.canonical_sha))
  )
    throw new Error("Invalid canonical commit");
  for (const key of [
    "truncated",
    "event_history_truncated",
    "claim_history_truncated",
  ])
    if (typeof value[key] !== "boolean")
      throw new Error("Invalid coverage flag");
  object(value.scope, "repositories");
  list(value.scope.repositories, 50);
  value.scope.repositories.forEach((r: any) => repository(r, repositories));
  if (value.dispatch_config !== undefined && value.dispatch_config !== null) {
    const c = value.dispatch_config;
    object(c, "repository revision namespace_build_cap pr_order staging_order");
    repository(c.repository, value.scope.repositories);
    if (typeof c.revision !== "string" || !sha.test(c.revision) || c.revision !== value.canonical_sha)
      throw new Error("Dispatch config must identify the observed canonical commit");
    integer(c.namespace_build_cap, 0, 10000);
    for (const order of [c.pr_order, c.staging_order]) {
      list(order, 20);
      if (!order.length || new Set(order).size !== order.length || order.some((name: any) => typeof name !== "string" || !/^[a-z][a-z0-9_-]{0,59}$/.test(name)))
        throw new Error("Invalid dispatch order");
    }
  }
  object(
    value.service,
    "revision started_at mode controller publication_enabled",
  );
  string(value.service.revision, 40);
  string(value.service.started_at, 40);
  if (
    !["production", "shadow", "unknown"].includes(value.service.mode) ||
    !["running", "unavailable", "unknown"].includes(value.service.controller)
  )
    throw new Error("Invalid service status");
  if (
    value.service.publication_enabled !== null &&
    typeof value.service.publication_enabled !== "boolean"
  )
    throw new Error("Invalid publication state");
  object(value.collection_counts, COLLECTIONS.join(" "));
  for (const key of COLLECTIONS)
    integer(value.collection_counts[key], 0, 20_000);
  object(value.collection_complete, COLLECTIONS.join(" "));
  for (const key of COLLECTIONS)
    if (typeof value.collection_complete[key] !== "boolean")
      throw new Error("Invalid collection coverage");
  list(value.retained_batch_ids, 20000);
  value.retained_batch_ids.forEach((id: any) => identity(id));
  list(value.active_batch_ids, 128);
  value.active_batch_ids.forEach((id: any) => identity(id));
  list(value.conditions, 100);
  for (const condition of value.conditions) {
    object(condition, "kind since batch_id seconds");
    string(condition.kind, 80);
    if (condition.since !== undefined) stamp(condition.since);
    if (condition.batch_id !== undefined) identity(condition.batch_id);
    if (
      condition.seconds !== undefined &&
      condition.seconds !== null &&
      (!Number.isFinite(condition.seconds) || condition.seconds < 0)
    )
      throw new Error("Invalid condition duration");
  }
  if (value.outbox !== null) list(value.outbox, 100);
  for (const row of value.outbox || []) {
    object(row, "kind state count");
    string(row.kind, 60);
    string(row.state, 30);
    integer(row.count);
  }
  object(
    value.settings,
    "repair_concurrency stall_interval_seconds open_batch_limit sealed_successor_min_members prepare_min_members prepare_quiet_seconds prepare_open_batches pre_rebase_enabled prepare_pre_rebased",
  );
  if (Object.hasOwn(value.settings, "sealed_successor_min_members"))
    integer(value.settings.sealed_successor_min_members);
  for (const setting of Object.values(value.settings))
    if (typeof setting !== "boolean") integer(setting);
  list(value.claims, 1000);
  for (const claim of value.claims) {
    object(claim, "batch_id task_id operation worker created_at expires_at");
    identity(claim.batch_id);
    identity(claim.task_id, true);
    string(claim.operation, 40);
    string(claim.worker);
    if (!/^queue-worker-[a-f0-9]{12}$/.test(claim.worker))
      throw new Error("Invalid public worker alias");
    stamp(claim.created_at);
    stamp(claim.expires_at);
  }
  list(value.workers, 100);
  for (const worker of value.workers) {
    object(worker, "id state observed_at started_at tasks errors coverage");
    string(worker.id);
    if (!/^(queue-worker-[a-f0-9]{12}|worker-\d+)$/.test(worker.id))
      throw new Error("Invalid public worker alias");
    if (!["observed", "unavailable"].includes(worker.coverage))
      throw new Error("Invalid worker coverage");
    stamp(worker.observed_at);
    if (worker.coverage === "observed") {
      string(worker.state, 40);
      stamp(worker.started_at);
      if (worker.tasks !== null) integer(worker.tasks);
      if (worker.errors !== null) integer(worker.errors);
    }
  }
  // History is a separately versioned public projection, implemented with its
  // own allowlist rather than accepting raw pilot snapshots or outcome files.
  if (value.history !== undefined)
    throw new Error("History contract not enabled");
}

export function queueFrame(value: any, repositories: string[]): QueueFrame {
  object(value, "schema source snapshot base part parts changes metadata");
  if (
    value.schema !== UPDATE_SCHEMA ||
    typeof value.snapshot !== "string" ||
    !uuid.test(value.snapshot)
  )
    throw new Error("Invalid frame schema");
  if (value.base !== null) {
    object(value.base, "epoch sequence");
    integer(value.base.epoch, 1);
    integer(value.base.sequence, 1);
  }
  source(value.source);
  integer(value.parts, 1, 2000);
  integer(value.part, 0, value.parts - 1);
  list(value.changes, 500);
  for (const change of value.changes) {
    object(change, "collection id value");
    if (!COLLECTIONS.includes(change.collection))
      throw new Error("Invalid collection");
    identity(change.id);
    if (change.value !== null)
      record(change.collection, change.value, repositories);
    if (change.value !== null && change.id !== change.value.id)
      throw new Error("Changed identity differs");
  }
  if (value.part === value.parts - 1) metadata(value.metadata, repositories);
  else if (value.metadata !== undefined)
    throw new Error("Only the last frame commits metadata");
  return value as QueueFrame;
}

/** Candidate-originated counters are bounded observations, never queue verdicts. */
export function progressFrame(value: any) {
  object(
    value,
    "schema session sequence observed_at phase completed_modules failed_modules last_module last_progress_at active_compilers cpu_seconds source_revision",
  );
  if (
    value.schema !== "tauceti.operations.progress/v1" ||
    !uuid.test(value.session)
  )
    throw new Error("Invalid progress schema");
  integer(value.sequence, 1);
  stamp(value.observed_at);
  stamp(value.last_progress_at);
  if (
    ![
      "starting",
      "build",
      "audit-setup",
      "audit-module-system",
      "lint-env",
      "lint-style",
      "declaration-names",
      "stage-cache",
      "end",
      "unknown",
    ].includes(value.phase)
  )
    throw new Error("Invalid phase");
  for (const k of ["completed_modules", "failed_modules", "active_compilers"])
    if (value[k] !== null) integer(value[k], 0, 10000000);
  if (
    value.cpu_seconds !== null &&
    (!Number.isFinite(value.cpu_seconds) ||
      value.cpu_seconds < 0 ||
      value.cpu_seconds > 1e12)
  )
    throw new Error("Invalid CPU duration");
  if (
    value.last_module !== null &&
    (typeof value.last_module !== "string" ||
      !/^TauCeti(?:\.[A-Za-z0-9_']+){0,30}$/.test(value.last_module) ||
      value.last_module.length > 300)
  )
    throw new Error("Invalid module");
  string(value.source_revision, 40);
  if (
    value.canonical_sha !== undefined &&
    value.canonical_sha !== null &&
    (typeof value.canonical_sha !== "string" || !sha.test(value.canonical_sha))
  )
    throw new Error("Invalid canonical commit");
  if (value.source_revision && !sha.test(value.source_revision))
    throw new Error("Invalid progress revision");
  return value;
}
