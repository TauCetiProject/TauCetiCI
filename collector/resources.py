"""Additive resource observations: never imported by merge or admission tooling."""
import io
import json
import zipfile

LIMIT = 1024 * 1024
ARTIFACTS = {
    'resources': ('ci-host-resources', 'ci-host-resources.json', 'tauceti-ci.host-resources/v1', 'host'),
    'publication': ('ci-publication', 'ci-publication.json', 'tauceti-ci.publication/v1', 'publisher-host'),
}


def observations(gh, repo, run):
    """Only attempt-named artifacts; absent, expired and malformed observations are gaps.

    Trust describes the producing host boundary, not proof correctness or policy
    authority. Artifact contents are data; collection never executes them.
    """
    result = {key: {'coverage': 'missing', 'payload': None} for key in ARTIFACTS}
    try:
        artifacts = gh.get(f"repos/TauCetiProject/{repo}/actions/runs/{run['id']}/artifacts")['artifacts']
    except Exception:
        return {key: {'coverage': 'unavailable', 'payload': None} for key in ARTIFACTS}
    for key, (prefix, filename, schema, trust) in ARTIFACTS.items():
        matches = [a for a in artifacts if a['name'] == f"{prefix}-{run.get('run_attempt', 1)}"]
        if len(matches) != 1:
            if matches:
                result[key]['coverage'] = 'ambiguous'
            continue
        artifact = matches[0]
        if artifact.get('expired'):
            result[key]['coverage'] = 'expired'
            continue
        try:
            if artifact.get('size_in_bytes', 0) > LIMIT:
                raise ValueError('oversized artifact')
            data = gh.raw(f"repos/TauCetiProject/{repo}/actions/artifacts/{artifact['id']}/zip")
            if len(data) > LIMIT:
                raise ValueError('oversized archive')
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                info = archive.getinfo(filename)
                if info.file_size > LIMIT:
                    raise ValueError('oversized observation')
                payload = json.loads(archive.read(info))
            if (payload.get('schema') != schema or payload.get('trust') != trust
                    or str(payload.get('run_id')) != str(run['id'])
                    or payload.get('run_attempt') != run.get('run_attempt', 1)):
                raise ValueError('observation provenance mismatch')
            result[key] = {'coverage': 'present', 'artifact_id': artifact['id'], 'payload': payload}
        except Exception:
            result[key]['coverage'] = 'invalid-or-unavailable'
    return result


def cohort(record):
    """Cache temperature is never inferred from success or a dependency change."""
    payload = ((record.get('observations') or {}).get('resources') or {}).get('payload') or {}
    bump = payload.get('dependency_bump')
    if bump == '1':
        return 'dependency-bump (warmth unknown)'
    if bump == '0':
        return 'ordinary'
    diff = (record.get('tested') or {}).get('diff') or {}
    paths = diff.get('paths')
    if paths is None:
        return 'unknown'
    if any(path in ('lean-toolchain', 'lake-manifest.json') for path in paths):
        return 'dependency-bump (warmth unknown)'
    return 'unknown' if diff.get('files_truncated') else 'ordinary'


def phase(name):
    name = name.lower()
    if any(part in name for part in ('publish', 'publication', 'confirm the exact', 'terminal merge-group', 'report build', 'report whether')):
        return 'publication/finalization'
    if any(part in name for part in ('hand the merge-group', 'cache artifact metadata', 'stage')):
        return 'cache staging'
    if any(part in name for part in ('lint', 'audit', 'shim', 'source option', 'meta-import', 'dot-notation')):
        return 'audits/lints'
    if any(part in name for part in ('build exact candidate', 'restore', 'cache get', 'fetch')):
        return 'compile/restore'
    if any(part in name for part in ('telemetry', 'observations')):
        return 'observability'
    return 'setup'
