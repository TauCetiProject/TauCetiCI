#!/usr/bin/env python3
"""Provision the isolated Cloudflare service and dedicated read-only GitHub App.

Secrets remain under ~/.config/tauceti-live-ci and are passed on stdin. The
registration page performs GitHub's interactive manifest flow; finish adopts
its credentials only after verifying the actual organisation installation.
"""

import argparse
import json
import os
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

SERVICE = Path(__file__).resolve().parents[1]
DIRECTORY = Path.home() / ".config/tauceti-live-ci"
URL = "https://ci-data.taucetiproject.org"
ACCOUNT = "ec2169bdf033f56b009956d4b64ba8ef"


def private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(value)
    path.chmod(0o600)


def run(*args, input=None, check=True):
    return subprocess.run(
        args, cwd=SERVICE, input=input, text=True, capture_output=True, check=check
    )


def http(path, token, data=None, app=False):
    req = urllib.request.Request(
        ("https://api.github.com/" if app else URL) + path,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "User-Agent": "TauCetiCI-setup/1.0",
            "Content-Type": "application/json",
        },
        data=json.dumps(data).encode() if data is not None else None,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise SystemExit(
            f"Request failed: HTTP {error.code}; credentials withheld"
        ) from None


def prepare():
    for name in ["admin-token", "ingest-token", "webhook-secret"]:
        if not (DIRECTORY / name).exists():
            private(DIRECTORY / name, secrets.token_urlsafe(40))


def finish():
    credentials = http(
        "/admin/registration", (DIRECTORY / "admin-token").read_text().strip()
    )
    if credentials.get("slug") != "tauceti-live-ci":
        raise SystemExit("Unexpected App identity")
    private(DIRECTORY / "github-app-credentials.json", json.dumps(credentials))
    converted = run(
        "node",
        "--input-type=module",
        "-e",
        "import {createPrivateKey} from 'node:crypto'; let s=''; for await(const c of process.stdin)s+=c; process.stdout.write(createPrivateKey(s).export({format:'pem',type:'pkcs8'}));",
        input=credentials["pem"],
    )
    private(DIRECTORY / "github-private-key.pem", converted.stdout)
    code = """import {readFileSync} from 'node:fs';import {sign} from 'node:crypto';
const d=process.argv[1],a=JSON.parse(readFileSync(d+'/github-app-credentials.json')),t=Math.floor(Date.now()/1000);
const e=v=>Buffer.from(JSON.stringify(v)).toString('base64url');const s=e({alg:'RS256',typ:'JWT'})+'.'+e({iat:t-60,exp:t+540,iss:String(a.id)});
process.stdout.write(s+'.'+sign('RSA-SHA256',Buffer.from(s),readFileSync(d+'/github-private-key.pem')).toString('base64url'));"""
    token = run("node", "--input-type=module", "-e", code, str(DIRECTORY)).stdout
    installations = http("app/installations?per_page=100", token, app=True)
    matches = [
        i
        for i in installations
        if i["account"]["login"] == "TauCetiProject" and not i.get("suspended_at")
    ]
    if len(matches) != 1 or any(
        v != "read" for v in matches[0].get("permissions", {}).values()
    ):
        raise SystemExit("Expected one active read-only organisation installation")
    installation = matches[0]
    grant = http(
        f"app/installations/{installation['id']}/access_tokens", token, {}, app=True
    )
    selected = http("installation/repositories?per_page=100", grant["token"], app=True)
    names = sorted(
        r["full_name"] for r in selected["repositories"] if not r.get("archived")
    )
    if selected["total_count"] > 100 or "TauCetiProject/TauCeti" not in names:
        raise SystemExit(
            "Unexpected repository selection; expected TauCeti and the fleet repositories"
        )
    private(
        DIRECTORY / "installation.json",
        json.dumps(
            {
                "app_id": str(credentials["id"]),
                "installation": str(installation["id"]),
                "repositories": names,
                "verified_at": time.time(),
            }
        ),
    )
    private(DIRECTORY / "webhook-secret", credentials["webhook_secret"])
    print(
        "Verified dedicated App and",
        len(names),
        "repositories. Run deploy to apply credentials.",
    )


def deploy():
    prepare()
    identity = run("npx", "--no-install", "wrangler", "whoami").stdout
    if ACCOUNT not in identity:
        raise SystemExit("TauCeti Cloudflare account is unavailable")
    config = json.loads((SERVICE / "wrangler.json").read_text())
    installation = (
        json.loads((DIRECTORY / "installation.json").read_text())
        if (DIRECTORY / "installation.json").exists()
        else None
    )
    if installation:
        config["vars"].update(
            GITHUB_APP_ID=installation["app_id"],
            GITHUB_INSTALLATION_ID=installation["installation"],
            ALLOWED_REPOSITORIES=json.dumps(installation["repositories"]),
        )
    else:
        result = run("gh", "api", "orgs/TauCetiProject/repos?per_page=100")
        config["vars"]["ALLOWED_REPOSITORIES"] = json.dumps(
            sorted(
                r["full_name"]
                for r in json.loads(result.stdout)
                if not r.get("archived")
            )
        )
    if (DIRECTORY / "publication-mode").exists():
        config["vars"]["PUBLICATION_MODE"] = (
            (DIRECTORY / "publication-mode").read_text().strip()
        )
    if config["vars"]["PUBLICATION_MODE"] == "live":
        health = http(
            "/admin/health", (DIRECTORY / "admin-token").read_text().strip()
        )
        if not health.get("github_configured") or not (
            0 <= time.time() - health.get("published", 0) < 300
        ):
            raise SystemExit(
                "Live publication requires a configured GitHub App and a report published within five minutes"
            )
    for queue in ["tauceti-live-ci-events-dlq", "tauceti-live-ci-events"]:
        result = run(
            "npx", "--no-install", "wrangler", "queues", "create", queue, check=False
        )
        if result.returncode and not any(
            s in (result.stderr + result.stdout).lower()
            for s in ["already exists", "already taken"]
        ):
            raise SystemExit("Queue provisioning failed: " + result.stderr)
    result = run(
        "npx",
        "--no-install",
        "wrangler",
        "r2",
        "bucket",
        "create",
        "tauceti-live-ci",
        check=False,
    )
    if result.returncode and "already" not in (result.stdout + result.stderr).lower():
        raise SystemExit("Bucket provisioning failed: " + result.stderr)
    secret_values = {
        key: (DIRECTORY / name).read_text().strip()
        for key, name in [
            ("ADMIN_TOKEN", "admin-token"),
            ("INGEST_TOKEN", "ingest-token"),
            ("WEBHOOK_SECRET", "webhook-secret"),
        ]
    }
    if installation:
        secret_values["GITHUB_PRIVATE_KEY"] = (
            DIRECTORY / "github-private-key.pem"
        ).read_text()
    fd, path = tempfile.mkstemp(prefix="deploy-", suffix=".json", dir=SERVICE)
    try:
        os.close(fd)
        private(Path(path), json.dumps(config))
        for args, stdin in [
            (["deploy"], None),
            (["secret", "bulk"], json.dumps(secret_values)),
        ]:
            result = run(
                "npx",
                "--no-install",
                "wrangler",
                *args,
                "--config",
                path,
                input=stdin,
                check=False,
            )
            if result.returncode:
                raise SystemExit("Deployment failed: " + result.stderr)
        # Bound replay/debug evidence and retired detail archives; keep compact summaries.
        rule = {"rules": [
            {"id": name, "enabled": True, "conditions": {"prefix": prefix},
             "deleteObjectsTransition": {"condition": {"type": "Age", "maxAge": days * 86400}}}
            for name, prefix, days in [
                ("delivery-retention", "deliveries/", 14),
                ("failed-retention", "failed/", 14),
                ("report-retention", "public/v1/ci/assets/", 14),
                ("validation-retention", "validation/", 30),
                ("retired-facts", "facts/", 7),
                ("retired-aggregates", "aggregates/", 7),
            ]
        ]}
        # Wrangler's lifecycle command reads structured JSON from a file.
        lifecycle = Path(path).with_suffix(".lifecycle.json")
        private(lifecycle, json.dumps(rule))
        try:
            result = run(
                "npx",
                "--no-install",
                "wrangler",
                "r2",
                "bucket",
                "lifecycle",
                "set",
                "tauceti-live-ci",
                "--file",
                str(lifecycle),
                "--force",
                check=False,
            )
            if result.returncode:
                raise SystemExit("Lifecycle configuration failed: " + result.stderr)
        finally:
            lifecycle.unlink(missing_ok=True)
    finally:
        Path(path).unlink(missing_ok=True)
    private(
        DIRECTORY / "deployment.json",
        json.dumps(
            {
                "url": URL,
                "account": ACCOUNT,
                "at": time.time(),
                "mode": config["vars"]["PUBLICATION_MODE"],
                "repositories": json.loads(config["vars"]["ALLOWED_REPOSITORIES"]),
            }
        ),
    )
    print("Deployed", config["vars"]["PUBLICATION_MODE"], "reporting:", URL)
    if not installation:
        print("Complete dedicated App registration:", URL + "/setup/register")


def watch():
    """Finish the one-time interactive installation without a further CLI step."""
    prepare()
    while True:
        try:
            finish()
            deploy()
            private(DIRECTORY / "installation-complete", str(time.time()))
            print("Dedicated App connected; shadow validation is running.", flush=True)
            return
        except (SystemExit, OSError, ValueError, subprocess.CalledProcessError):
            # A missing registration costs only a protected R2 read. Once the
            # App exists, installation discovery runs at most twelve times/hour.
            print(
                "Waiting for dedicated App registration and installation.", flush=True
            )
            time.sleep(300)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["prepare", "deploy", "finish", "watch"])
    a = p.parse_args()
    {"prepare": prepare, "deploy": deploy, "finish": finish, "watch": watch}[a.action]()
