import { object, string } from "./operations-model";
const METRICS =
  "landed_heads landed_prs landed_prs_per_hour landed_membership_occurrences nonempty_unique_prs_per_hour nonempty_unique_prs empty_member_occurrences already_landed_empty_occurrences emptiness_unknown baseline_ratio ci_attempts singleton_runs failed_rounds_per_large_landing claim_collisions claim_expiries opt_outs anticipated_withdrawals needs_rebase readmissions edit_summaries shrink_to_empty inference_hold_minutes";
function numeric(v: any) {
  if (v !== null && (typeof v !== "number" || !Number.isFinite(v) || v < 0))
    throw new Error("Invalid public measurement");
}
function stamp(v: any) {
  if (typeof v !== "string" || v.length > 40 || !Number.isFinite(Date.parse(v)))
    throw new Error("Invalid measurement time");
}
export function historyFrame(v: any, repositories: string[]) {
  object(v, "schema observed_at scope coverage windows baseline limits");
  if (v.schema !== "tauceti.operations.history/v1" || v.coverage !== "observed")
    throw new Error("Invalid history schema");
  stamp(v.observed_at);
  object(v.scope, "repositories");
  if (
    !Array.isArray(v.scope.repositories) ||
    v.scope.repositories.some((r: any) => !repositories.includes(r))
  )
    throw new Error("Invalid history scope");
  if (!Array.isArray(v.windows) || v.windows.length > 3)
    throw new Error("Invalid windows");
  for (const w of v.windows) {
    object(
      w,
      "key label from until hours summary heads heads_truncated criteria",
    );
    if (!["24h", "7d", "pilot"].includes(w.key))
      throw new Error("Invalid window");
    string(w.label, 40);
    stamp(w.from);
    stamp(w.until);
    numeric(w.hours);
    if (w.criteria !== undefined) {
      object(w.criteria, "scenario week");
      const { scenario, week } = w.criteria;
      object(
        scenario,
        "status eligible_count qualifying_count qualifying_batches",
      );
      if (
        ![
          "timing_met_verification_required",
          "not_met_in_observed_data",
          "not_observed",
        ].includes(scenario.status)
      )
        throw new Error("Invalid scenario status");
      numeric(scenario.eligible_count);
      numeric(scenario.qualifying_count);
      if (
        !Array.isArray(scenario.qualifying_batches) ||
        scenario.qualifying_batches.length > 100 ||
        scenario.qualifying_batches.some(
          (b: any) => typeof b !== "string" || !/^\d{1,20}$/.test(b),
        )
      )
        throw new Error("Invalid qualifying batches");
      object(week, "status observed_days rate_above_baseline");
      if (
        ![
          "not_yet_observable",
          "requires_attribution_and_comment_audit",
        ].includes(week.status) ||
        typeof week.rate_above_baseline !== "boolean"
      )
        throw new Error("Invalid week criterion");
      numeric(week.observed_days);
    }
    object(
      w.summary,
      METRICS +
        " head_dispatch_minutes head_seal_minutes repair_task_minutes approval_to_insertion_minutes currently_waiting_age_minutes cost",
    );
    for (const k of METRICS.split(" ")) numeric(w.summary[k]);
    for (const k of [
      "head_dispatch_minutes",
      "head_seal_minutes",
      "repair_task_minutes",
      "approval_to_insertion_minutes",
      "currently_waiting_age_minutes",
    ]) {
      object(w.summary[k], "count min median p95 max");
      Object.values(w.summary[k]).forEach(numeric);
    }
    object(
      w.summary.cost,
      "recorded_usd recorded_attempts unknown_attempts coverage",
    );
    if (w.summary.cost.coverage !== "recorded_subtotal_only")
      throw new Error("Invalid cost coverage");
    for (const k of ["recorded_usd", "recorded_attempts", "unknown_attempts"])
      numeric(w.summary.cost[k]);
    if (
      !Array.isArray(w.heads) ||
      w.heads.length > 500 ||
      typeof w.heads_truncated !== "boolean"
    )
      throw new Error("Invalid head summary");
    for (const h of w.heads) {
      object(
        h,
        "batch_id size ci_rounds source_failure_rounds first_dispatch_to_landing_minutes seal_to_landing_minutes ci_observed_minutes claimed_task_minutes ci_task_overlap_minutes other_or_unobserved_minutes",
      );
      if (typeof h.batch_id !== "string" || !/^\d{1,20}$/.test(h.batch_id))
        throw new Error("Invalid head identity");
      for (const k of Object.keys(h)) if (k !== "batch_id") numeric(h[k]);
    }
  }
  object(
    v.baseline,
    "hours landed_prs landed_prs_per_hour singleton_runs failed_rounds_per_large_landing",
  );
  Object.values(v.baseline).forEach(numeric);
  if (!Array.isArray(v.limits) || v.limits.length > 10)
    throw new Error("Invalid limitations");
  v.limits.forEach((s: any) => string(s, 300));
  return v;
}
