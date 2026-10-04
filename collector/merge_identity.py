"""Merge-engine identity comes from the CI event and trusted harness metadata."""
import json
import re

SHA = re.compile(r"^[0-9a-f]{40}$")
TITLE = re.compile(r"^bors branch=([^ ]+) batch=(\d+) head=([0-9a-f]{40}) base=([0-9a-f]{40})$")


def apply_dispatch_title(record, title):
    if record.get("event") != "repository_dispatch" or record.get("workflow") != ".github/workflows/pr-build.yml":
        return
    # This workflow subscribes only to tauceti-bors-staging dispatches. Even
    # without artifacts, their consumed CI minutes belong to bors.
    record["trigger"] = "bors_unknown"
    record["tested"] = {"head_sha": None, "batch_members": []}
    record["merge_metadata_pending"] = True
    match = TITLE.fullmatch(title or "")
    if match:
        branch, batch, head, base = match.groups()
        record["trigger"] = "bors_staging" if branch == "main" else "bors_unknown" if branch == "unknown" else "bors_pilot"
        if branch not in ("main", "unknown"):
            record["merge_metadata_pending"] = False
        record["tested"].update(head_sha=head, base_sha=base, batch_id=int(batch))


def apply_telemetry(record, telemetry):
    meta = telemetry.get("meta") or {}
    if record.get("event") != "repository_dispatch" or meta.get("merge_engine") != "bors":
        return
    head, base = meta.get("head_sha", ""), meta.get("base_sha", "")
    if not SHA.fullmatch(head) or not SHA.fullmatch(base):
        return
    try:
        batch = int(meta["batch_id"])
        members = json.loads(meta["batch_members"])
        if batch <= 0 or not isinstance(members, list):
            return
        if any(type(m.get("pr")) is not int or m["pr"] <= 0
               or not SHA.fullmatch(m.get("head_sha") or "") for m in members):
            return
    except (KeyError, ValueError, TypeError, AttributeError):
        return
    record["trigger"] = "bors_staging"
    record["tested"] = {"head_sha": head, "base_sha": base, "batch_id": batch,
                        "batch_members": members}
    record["merge_metadata_pending"] = False
    # record.head_sha remains GitHub's workflow source commit for provenance.


def identity(record):
    tested = record.get("tested") or {}
    if record.get("trigger") == "merge_queue":
        return "queue", tested.get("head_sha") or record["head_sha"], tested.get("base_sha"), None, []
    if record.get("trigger") == "bors_staging":
        return "bors", tested.get("head_sha"), tested.get("base_sha"), tested.get("batch_id"), tested.get("batch_members", [])
    return None
