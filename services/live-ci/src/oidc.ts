import { type Env } from "./types";
const decode = (s: string) =>
  Uint8Array.from(
    atob(
      s
        .replaceAll("-", "+")
        .replaceAll("_", "/")
        .padEnd(Math.ceil(s.length / 4) * 4, "="),
    ),
    (c) => c.charCodeAt(0),
  );
/** Only the trusted, main-branch summary workflow may reserve reads or submit summaries. */
export async function verifyPublisher(token: string, env: Env): Promise<any> {
  return verify(
    token,
    env,
    (claims) =>
      claims.repository === "TauCetiProject/TauCeti" &&
      claims.ref === "refs/heads/main" &&
      claims.event_name === "workflow_run" &&
      claims.workflow_ref ===
        "TauCetiProject/TauCeti/.github/workflows/ci-live-summary.yml@refs/heads/main",
    "tauceti-live-ci",
  );
}
/** A build token has authority only over its own run/attempt/check-run telemetry.
 * check_run_id is signed by GitHub; neither a body job ID nor runner name is an identity grant. */
export async function verifyProgress(token: string, env: Env): Promise<any> {
  const repositories = JSON.parse(env.OPERATIONS_PUBLIC_REPOSITORIES || "[]");
  return verify(
    token,
    env,
    (c) =>
      repositories.includes(c.repository) &&
      c.repository_visibility === "public" &&
      (c.workflow_ref ===
        `${c.repository}/.github/workflows/pr-build.yml@refs/heads/main` ||
        (c.event_name === "merge_group" &&
          typeof c.ref === "string" &&
          /^refs\/heads\/gh-readonly-queue\/main\/[A-Za-z0-9_./-]{1,200}$/.test(
            c.ref,
          ) &&
          c.workflow_ref ===
            `${c.repository}/.github/workflows/pr-build.yml@${c.ref}`)) &&
      [
        "pull_request_target",
        "repository_dispatch",
        "workflow_dispatch",
        "merge_group",
      ].includes(c.event_name) &&
      /^\d{1,20}$/.test(String(c.run_id)) &&
      /^\d{1,20}$/.test(String(c.check_run_id)) &&
      Number.isSafeInteger(Number(c.run_attempt)) &&
      Number(c.run_attempt) > 0,
    "tauceti-operations-progress",
  );
}
async function verify(
  token: string,
  env: Env,
  policy: (claims: any) => boolean,
  audience: string,
): Promise<any> {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("Invalid publisher token");
  const head = JSON.parse(new TextDecoder().decode(decode(parts[0]))),
    claims = JSON.parse(new TextDecoder().decode(decode(parts[1])));
  const clock = Date.now() / 1000;
  if (
    head.alg !== "RS256" ||
    claims.iss !== "https://token.actions.githubusercontent.com" ||
    claims.aud !== audience ||
    ![claims.exp, claims.nbf, claims.iat].every(Number.isFinite) ||
    claims.exp < clock ||
    claims.nbf > clock + 30 ||
    claims.iat > clock + 30 ||
    clock - claims.iat > 600 ||
    claims.exp - claims.iat > 3600 ||
    !policy(claims)
  )
    throw new Error("Publisher claims rejected");
  const object = await env.ARCHIVE.get("private/oidc-jwks.json");
  let keys: any = object ? await object.json() : null;
  if (!keys || keys.fetched < clock - 21600) {
    const r = await fetch(
      "https://token.actions.githubusercontent.com/.well-known/jwks",
      { signal: AbortSignal.timeout(10000) },
    );
    if (!r.ok) throw new Error("Publisher key fetch failed");
    keys = { ...((await r.json()) as object), fetched: clock };
    await env.ARCHIVE.put("private/oidc-jwks.json", JSON.stringify(keys));
  }
  const jwk = keys.keys.find((k: any) => k.kid === head.kid);
  if (!jwk) throw new Error("Unknown publisher key");
  const key = await crypto.subtle.importKey(
    "jwk",
    jwk,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["verify"],
  );
  if (
    !(await crypto.subtle.verify(
      "RSASSA-PKCS1-v1_5",
      key,
      decode(parts[2]),
      new TextEncoder().encode(parts[0] + "." + parts[1]),
    ))
  )
    throw new Error("Publisher signature rejected");
  return claims;
}
