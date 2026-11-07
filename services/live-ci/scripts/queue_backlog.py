#!/usr/bin/env python3
"""Reconstruct minute backlog samples from a complete, sanitized queue journal.

This prepares an admin-import document; it never opens or changes the queue DB.
The source needs inputs, members, batches, journal, captured_at, journal_bounds.
Export those in one read-only SQLite transaction. Journal detail needs only
number/input_id/members/withdrawn/against_main/key/value. Keep journal IDs and
recorded timestamps; approval ages can predate actual admission during imports.
"""
import argparse
import datetime
import hashlib
import json
import math
from pathlib import Path

REPO = "TauCetiProject/TauCeti"


def iso(at):
    return datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def reconstruct(source, until):
    bounds = source["journal_bounds"]
    if bounds["first"] != 1 or bounds["last"] != bounds["count"]:
        raise ValueError("The complete journal, starting at ID 1, is required")
    if any(row["repository"] != REPO for row in source["inputs"] + source["batches"]):
        raise ValueError("Mixed repository journals are not supported")
    journal = sorted(source["journal"], key=lambda row: row["id"])
    if len({row["id"] for row in journal}) != len(journal):
        raise ValueError("Duplicate journal identities")
    approvals = {row["detail"]["input_id"]: row["detail"]["number"] for row in journal if row["kind"] == "approved"}
    if approvals != {row["id"]: row["number"] for row in source["inputs"]}:
        raise ValueError("Every input must have its recorded approval")
    start = next((row["at"] for row in journal if row["kind"] == "setting" and
                  row["detail"].get("key") == "publication_enabled" and row["detail"].get("value") is True), None)
    if start is None or not start <= until <= source["captured_at"]:
        raise ValueError("Backfill must lie inside the recorded production window")
    live, changes, uncertain = set(), [], []
    last = 0
    for row in journal:
        kind, detail = row["kind"], row["detail"]
        remove = kind in ("withdrawn", "problem-anticipated", "probe-missing-source") or (
            kind == "probe-conflict" and detail.get("against_main"))
        if kind != "approved" and kind != "landed" and not remove:
            continue
        # Some probes record their start time after a newer approval commits.
        # Journal order owns state order. Bound those clock inversions and omit
        # any minute boundary whose state would depend on the ordering choice.
        at = max(last, row["at"])
        if at != row["at"]:
            uncertain.append((row["at"], at))
        last = at
        if kind == "approved":
            live.add(detail["number"])
        elif kind == "landed":
            live.difference_update(detail["members"])
            live.difference_update(detail.get("withdrawn", []))
        else:
            live.discard(detail["number"])
        changes.append((at, len(live)))
    batches = {row["id"]: row for row in source["batches"]}
    expected = {row["number"] for row in source["inputs"] if row["state"] in ("waiting", "reserved")}
    expected.update(row["number"] for row in source["members"] if not row["removed"] and not row["withdrawn"] and
                    batches[row["batch_id"]]["state"] in ("head", "open"))
    if live != expected:
        raise ValueError("Journal replay does not match the captured queue backlog")
    samples, index, count = [], 0, 0
    for at in range(math.ceil(start / 60) * 60, math.floor(until) + 1, 60):
        while index < len(changes) and changes[index][0] <= at:
            count = changes[index][1]
            index += 1
        complete = not any(lo <= at <= hi for lo, hi in uncertain)
        samples.append({"observed_at": iso(at), "eligible_count": count,
                        "candidate_scan_complete": complete,
                        "candidate_snapshot_at": iso(at) if complete else None,
                        "reconstructed": True})
    return {"schema": "tauceti-ci.queue-backfill/v1", "repo": REPO,
            "replayed_current_count": len(live), "uncertain_intervals": uncertain,
            "observations": samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--until", required=True, help="UTC time before the first direct queue sample")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    raw = args.source.read_bytes()
    until = datetime.datetime.fromisoformat(args.until.replace("Z", "+00:00")).timestamp()
    result = reconstruct(json.loads(raw), until)
    result["source_sha256"] = hashlib.sha256(raw).hexdigest()
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"samples": len(result["observations"]), "replayed_current_count": result["replayed_current_count"],
                      "source_sha256": result["source_sha256"]}))


if __name__ == "__main__":
    main()
