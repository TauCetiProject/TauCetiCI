/** Same ordered v2 categories as collector/classify.py; excerpt text is statistics only. */
export function classify(
  conclusion: string | null,
  step: string,
  lines: string[] | null,
): string | undefined {
  if (!conclusion || ["success", "skipped", "neutral"].includes(conclusion))
    return undefined;
  if (conclusion === "cancelled") return "cancelled";
  if (conclusion === "timed_out") return "timeout";
  if (/Build exact candidate|lake build|^Build$/.test(step)) {
    const rules: [string, RegExp][] = [
      [
        "process-killed",
        /(?:^|\s)Killed(?:\s+timeout\b|\s*$)|\bLean exited with code 137\b/,
      ],
      [
        "timeout",
        /\btimed out\b|\bexceeded .* (?:deadline|wall-clock limit)\b/,
      ],
      ["infra-cache", /failed to download artifact|Transferred a partial file/],
      ["lint-env", /LINT-ENV: FAIL/],
      ["audit-axioms", /[Aa]xiom/],
      ["audit-duplicates", /[Dd]uplicate declaration/],
      ["audit-module-system", /module system|isModule/],
      ["lint-style", /ERR_[A-Z]+|style lint|lint-style/],
      ["lean-error", /\.lean:\d+:\d+: error|^error: .*\.lean/],
    ];
    for (const [name, pattern] of rules)
      if (
        lines?.some(
          (l) => !/^(?:✔(?:\s|$)|\+\+?\s)/.test(l) && pattern.test(l),
        )
      )
        return name;
    return lines === null ? "build-unknown" : "lean-error";
  }
  for (const [name, pattern] of [
    ["infra-report", /^Report|^Post /],
    ["policy", /guard|Scope|shim|Attest|Validate the Lake-pin|Normalise|bump/],
    ["dot-notation", /Dot-notation/],
    [
      "toolchain-incompat",
      /Build the trusted environment-lint driver|watchdog toolchain/,
    ],
    ["infra-cache", /Mathlib|Lake cache|oleans|Lake artifact|cache/],
    ["infra-setup", /Install|Raise vm|Checkout|Set up|Resolve PR head/],
  ] as [string, RegExp][])
    if (pattern.test(step)) return name;
  return "other";
}
