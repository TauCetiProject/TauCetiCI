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
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("Invalid publisher token");
  const head = JSON.parse(new TextDecoder().decode(decode(parts[0]))),
    claims = JSON.parse(new TextDecoder().decode(decode(parts[1])));
  const clock = Date.now() / 1000;
  if (
    head.alg !== "RS256" ||
    claims.iss !== "https://token.actions.githubusercontent.com" ||
    claims.aud !== "tauceti-live-ci" ||
    claims.exp < clock ||
    claims.nbf > clock + 30 ||
    claims.iat > clock + 30 ||
    clock - claims.iat > 600 ||
    claims.repository !== "TauCetiProject/TauCeti" ||
    claims.ref !== "refs/heads/main" ||
    claims.event_name !== "workflow_run" ||
    claims.workflow_ref !==
      "TauCetiProject/TauCeti/.github/workflows/ci-live-summary.yml@refs/heads/main"
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
