import type { Env } from "./types";
const enc = (v: any) =>
  btoa(JSON.stringify(v))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
export async function installationToken(
  env: Env,
): Promise<{ token: string; expires_at: string }> {
  if (
    !env.GITHUB_APP_ID ||
    !env.GITHUB_INSTALLATION_ID ||
    !env.GITHUB_PRIVATE_KEY
  )
    throw new Error("Dedicated GitHub App is not configured");
  const key = Uint8Array.from(
    atob(env.GITHUB_PRIVATE_KEY.replace(/-----[^-]+-----|\s/g, "")),
    (c) => c.charCodeAt(0),
  );
  const imported = await crypto.subtle.importKey(
    "pkcs8",
    key,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const now = Math.floor(Date.now() / 1000),
    data =
      enc({ alg: "RS256", typ: "JWT" }) +
      "." +
      enc({ iat: now - 60, exp: now + 540, iss: env.GITHUB_APP_ID });
  const signature = new Uint8Array(
    await crypto.subtle.sign(
      "RSASSA-PKCS1-v1_5",
      imported,
      new TextEncoder().encode(data),
    ),
  );
  const jwt =
    data +
    "." +
    btoa(String.fromCharCode(...signature))
      .replaceAll("+", "-")
      .replaceAll("/", "_")
      .replaceAll("=", "");
  const r = await fetch(
    `https://api.github.com/app/installations/${env.GITHUB_INSTALLATION_ID}/access_tokens`,
    {
      method: "POST",
      headers: headers(jwt),
      body: JSON.stringify({
        permissions: { actions: "read", contents: "read", metadata: "read" },
      }),
      signal: AbortSignal.timeout(15000),
    },
  );
  if (!r.ok) throw new Error(`Installation authentication HTTP ${r.status}`);
  return r.json();
}
export const headers = (token: string) => ({
  Authorization: `Bearer ${token}`,
  Accept: "application/vnd.github+json",
  "X-GitHub-Api-Version": "2022-11-28",
  "User-Agent": "TauCetiCI-live/1.0",
  "Content-Type": "application/json",
});
export async function boundedBytes(
  response: Response,
  limit: number,
): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let n = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    n += value.length;
    if (n > limit) {
      await reader.cancel();
      throw new Error("Response size limit exceeded");
    }
    chunks.push(value);
  }
  const data = new Uint8Array(n);
  let i = 0;
  for (const chunk of chunks) {
    data.set(chunk, i);
    i += chunk.length;
  }
  return data;
}
