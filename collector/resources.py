"""Additive resource observations: never imported by merge or admission tooling."""
import io
import json
import math
import re
import zipfile

LIMIT = 1024 * 1024
ARTIFACTS = {
    'resources': ('ci-host-resources', 'ci-host-resources.json', 'tauceti-ci.host-resources/v1', 'host'),
    'publication': ('ci-publication', 'ci-publication.json', 'tauceti-ci.publication/v1', 'publisher-host'),
}


def producer_sha(repo, run):
    """Require API provenance, independently of artifact-controlled labels.

    For target PRs, API head fields describe the candidate, not the harness.
    Require the API's pinned main reusable-workflow references in that context.
    The same check handles stored records without trusting payload assertions.
    """
    workflow = run.get('path') or run.get('workflow')
    events = {
        '.github/workflows/pr-build.yml': ('repository_dispatch', 'workflow_dispatch'),
        '.github/workflows/ci.yml': ('push', 'workflow_dispatch'),
    }
    repository = (run.get('repository') or {}).get('full_name') or run.get('workflow_repository')
    if repo != 'TauCeti' or repository not in (None, 'TauCetiProject/TauCeti'):
        return None
    if workflow == '.github/workflows/pr-build.yml' and run.get('event') == 'pull_request_target':
        if repository != 'TauCetiProject/TauCeti':
            return None
        sources = set()
        for reference in run.get('referenced_workflows') or []:
            sha = reference.get('sha')
            path = reference.get('path', '')
            if path.split('@')[0] not in (
                    'TauCetiProject/TauCeti/.github/workflows/pick-runner.yml',
                    'TauCetiProject/TauCeti/.github/workflows/publish-lake-cache.yml'):
                continue
            if (reference.get('ref') != 'refs/heads/main' or not isinstance(sha, str)
                    or re.fullmatch(r'[0-9a-f]{40}', sha) is None or not path.endswith('@' + sha)):
                return None
            sources.add(sha)
        return next(iter(sources)) if len(sources) == 1 else None
    head_repo = (run.get('head_repository') or {}).get('full_name') or run.get('head_repo')
    if (head_repo == 'TauCetiProject/TauCeti'
            and run.get('head_branch') == 'main'
            and run.get('event') in events.get(workflow, ())
            and isinstance(run.get('head_sha'), str)
            and re.fullmatch(r'[0-9a-f]{40}', run['head_sha']) is not None):
        return run['head_sha']
    return None


def verified_observation(repo, run, kind, observation):
    """Recheck stored observations too, so rebuilding removes old self-label trust."""
    if observation.get('coverage') != 'present':
        return observation
    payload = observation.get('payload') or {}
    source = producer_sha(repo, run)
    if not source or (kind == 'resources' and payload.get('workflow_sha') != source):
        return {**observation, 'coverage': 'unverified-producer', 'payload': None}
    return observation


def validate(payload, kind):
    if kind == 'resources':
        if 'missing' in payload and (not isinstance(payload['missing'], list)
                or any(not isinstance(item, str) for item in payload['missing'])):
            raise ValueError('invalid missing-counter coverage')
        for field in ('host_pressure_stall_us', 'host_device_io_bytes'):
            counters = payload.get(field, {})
            if not isinstance(counters, dict):
                raise ValueError('invalid host counters')
            for name, values in counters.items():
                if not isinstance(name, str) or not isinstance(values, dict):
                    raise ValueError('invalid counter shape')
                for value in values.values():
                    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                        raise ValueError('invalid counter value')
    else:
        for field in ('complete', 'uploaded', 'confirmed'):
            if type(payload.get(field)) is not bool:
                raise ValueError('invalid publication outcome')
        for field in ('staged_bytes', 'staged_objects'):
            if type(payload.get(field)) is not int or payload[field] < 0:
                raise ValueError('invalid publication size')


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
        if not producer_sha(repo, run):
            result[key] = {'coverage': 'unverified-producer',
                           'artifact_id': artifact['id'], 'payload': None}
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
                    or type(payload.get('run_attempt')) is not int
                    or payload.get('run_attempt') != run.get('run_attempt', 1)):
                raise ValueError('observation provenance mismatch')
            validate(payload, key)
            result[key] = verified_observation(repo, run, key, {
                'coverage': 'present', 'artifact_id': artifact['id'], 'payload': payload})
        except Exception:
            result[key]['coverage'] = 'invalid-or-unavailable'
    return result


def cohort(record):
    """Cache temperature is never inferred from success or a dependency change."""
    observation = (record.get('observations') or {}).get('resources') or {}
    observation = verified_observation(record.get('repo'), record, 'resources', observation)
    payload = (observation.get('payload') or {}) if observation.get('coverage') == 'present' else {}
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
