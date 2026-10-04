"""Merge-engine identity comes from the CI event and trusted harness metadata."""
import json
import re

SHA = re.compile(r"^[0-9a-f]{40}$")


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
    # record.head_sha remains GitHub's workflow source commit for provenance.


def identity(record):
    tested = record.get("tested") or {}
    if record.get("trigger") == "merge_queue":
        return "queue", tested.get("head_sha") or record["head_sha"], tested.get("base_sha"), None, []
    if record.get("trigger") == "bors_staging":
        return "bors", tested["head_sha"], tested.get("base_sha"), tested.get("batch_id"), tested.get("batch_members", [])
    return None
