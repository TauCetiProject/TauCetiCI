export interface Env {
  FACTS: DurableObjectNamespace;
  DAYS: DurableObjectNamespace;
  DIRECTORIES: DurableObjectNamespace;
  CONTROL: DurableObjectNamespace;
  ARCHIVE: R2Bucket;
  EVENTS: Queue<{ key: string }>;
  WEBHOOK_SECRET: string;
  INGEST_TOKEN: string;
  ADMIN_TOKEN: string;
  GITHUB_APP_ID?: string;
  GITHUB_INSTALLATION_ID?: string;
  GITHUB_PRIVATE_KEY?: string;
  GITHUB_HOURLY_BUDGET: string;
  ALLOWED_REPOSITORIES: string;
  PUBLIC_ORIGIN: string;
  PUBLICATION_MODE: string;
  CF_VERSION?: { id: string };
}
export const stub = (ns: DurableObjectNamespace, name: string) =>
  ns.get(ns.idFromName(name));
export async function call(
  ns: DurableObjectNamespace,
  name: string,
  path: string,
  body: any = {},
): Promise<any> {
  const response = await stub(ns, name).fetch("https://internal" + path, {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`Internal ${path}: ${response.status}`);
  return response.json();
}
export const json = (data: any, status = 200) =>
  Response.json(data, { status });
export const now = () => Math.floor(Date.now() / 1000);
export const allowed = (env: Env): string[] =>
  JSON.parse(env.ALLOWED_REPOSITORIES || "[]");
export const historicalRepo = (repo: any) =>
  typeof repo === "string" && /^TauCetiProject\/[A-Za-z0-9_.-]+$/.test(repo);
export const directory = (repo: string, id: string) =>
  `${repo}/${BigInt(id) % 64n}`;
export async function digest(data: string | ArrayBuffer): Promise<string> {
  return [
    ...new Uint8Array(
      await crypto.subtle.digest(
        "SHA-256",
        typeof data === "string" ? new TextEncoder().encode(data) : data,
      ),
    ),
  ]
    .map((n) => n.toString(16).padStart(2, "0"))
    .join("");
}
export async function equal(a: string, b: string): Promise<boolean> {
  return !!a && !!b && (await digest(a)) === (await digest(b));
}
