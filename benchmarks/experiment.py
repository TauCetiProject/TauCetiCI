"""One immutable, read-only experiment. Results never authorize a merge or publication."""
import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess

PLAN = Path(__file__).with_name('plan.json')


def plan():
    return json.loads(PLAN.read_text())


def slots(p):
    return [dict(workload=w, shape=s, repetition=r, slot=f'{w}-{s}-{r}')
            for w in p['workloads'] for r in range(1, p['repetitions'] + 1)
            for s in p['shapes']]


def authorize(runs, run_id, attempt, branch):
    """Only the earliest dispatch may execute; reruns and other refs fail closed.

    Run history must never be deleted for this workflow. A later dispatch, including
    concurrent dispatch, cannot acquire another set of slots. Every worker also
    checks run_attempt, because rerun-failed can reuse successful gate outputs.
    """
    if attempt != 1 or branch != 'refs/heads/main':
        raise ValueError('benchmarks require first attempt on main')
    if not runs or run_id != min(int(r['id']) for r in runs):
        raise ValueError('the experiment already has an owner dispatch')


def inventory(root):
    """Content identity for the verified root cache, not a proof of correctness."""
    rows = []
    for name in ('outputs', 'artifacts'):
        for path in sorted((root / name).rglob('*')):
            if path.is_file() and not path.name.endswith('.hash'):
                with path.open('rb') as handle:
                    rows.append([str(path.relative_to(root)), hashlib.file_digest(handle, 'sha256').hexdigest()])
    if not rows:
        raise ValueError('warm root cache is missing; refusing a cold substitution')
    return hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest(), len(rows)


def read_results(directory, p):
    results = []
    expected = {s['slot'] for s in slots(p)}
    for f in directory.rglob('result.json'):
        try:
            row = json.loads(f.read_text())
            if not isinstance(row, dict) or row.get('slot') not in expected:
                raise ValueError('invalid slot record')
        except (ValueError, OSError) as error:
            slot = f.parent.name.removeprefix('benchmark-')
            if slot not in expected:
                raise ValueError(f'cannot attribute corrupt result: {f}') from error
            row = dict(slot=slot, status='corrupt-result', error=str(error))
        results.append(row)
    return results


def attach_jobs(rows, jobs, p):
    for row in rows:
        matches = [j for j in jobs if row['slot'] in j.get('name', '')]
        if len(matches) != 1:
            row['job_coverage'] = 'missing-or-ambiguous'
            continue
        job = matches[0]
        row['job'] = job
        row['job_coverage'] = job['status']
        if row['status'] == 'success' and (job['status'] != 'completed' or job.get('conclusion') != 'success'):
            row['status'] = 'failed-job'
        if job.get('started_at') and job.get('completed_at'):
            seconds = (datetime.fromisoformat(job['completed_at'].replace('Z', '+00:00')) -
                       datetime.fromisoformat(job['started_at'].replace('Z', '+00:00'))).total_seconds()
            row['job_elapsed_s'] = seconds
            row['job_estimated_unit_minutes'] = seconds / 60 * p['shapes'][row['shape']]['units_per_minute']


def summarize(p, results, jobs=None):
    expected = slots(p)
    by_slot = {}
    for item in results:
        key = item['slot']
        if key in by_slot or key not in {s['slot'] for s in expected}:
            raise ValueError('duplicate or unexpected slot')
        by_slot[key] = item
    rows = [{**s, **by_slot.get(s['slot'], {'status': 'missing'})} for s in expected]
    if jobs is not None:
        attach_jobs(rows, jobs, p)
    # A changed cache invalidates the whole workload comparison, even if all builds passed.
    for w in p['workloads']:
        group = [r for r in rows if r['workload'] == w]
        identities = {r.get('cache_identity') for r in group if r['status'] == 'success'}
        if len(identities) != 1 or None in identities:
            for row in group:
                if row['status'] == 'success':
                    row['status'] = 'incomparable-cache'
    groups = []
    for w in p['workloads']:
        for shape, spec in p['shapes'].items():
            group = [r for r in rows if r['workload'] == w and r['shape'] == shape]
            good = [r for r in group if r['status'] == 'success']
            groups.append(dict(workload=w, shape=shape, successful=len(good), planned=len(group),
                               median_elapsed_s=statistics.median(r['elapsed_s'] for r in good) if good else None,
                               median_job_elapsed_s=statistics.median(r['job_elapsed_s'] for r in good) if good and all('job_elapsed_s' in r for r in good) else None,
                               median_sandbox_cpu_s=statistics.median(r['phases']['sandbox']['command_cpu_user_s'] + r['phases']['sandbox']['command_cpu_system_s'] for r in good) if good and all('sandbox' in r.get('phases', {}) for r in good) else None,
                               largest_process_rss_bytes=max(r['phases']['sandbox']['command_max_process_rss_bytes'] for r in good) if good and all('sandbox' in r.get('phases', {}) for r in good) else None,
                               median_unit_minutes=statistics.median(r.get('job_estimated_unit_minutes', r['elapsed_s'] / 60 * spec['units_per_minute']) for r in good) if good else None))
    recommendations = []
    for w in p['workloads']:
        choices = [g for g in groups if g['workload'] == w]
        baseline = choices[0]
        eligible = [g for g in choices[1:] if g['successful'] == baseline['successful'] == 2
                    and (g['median_job_elapsed_s'] or g['median_elapsed_s']) <= .90 * (baseline['median_job_elapsed_s'] or baseline['median_elapsed_s'])
                    and g['median_unit_minutes'] <= 1.10 * baseline['median_unit_minutes']]
        winner = min(eligible, key=lambda g: g['median_elapsed_s']) if eligible else baseline
        recommendations.append(dict(workload=w, shape=winner['shape'],
                                    reason='At least 10% faster at no more than 10% extra estimated compute cost'
                                    if eligible else 'Retain baseline; no complete larger-shape pair meets benefit/cost threshold'))
    complete = all(r['status'] == 'success' for r in rows)
    return dict(experiment=p['experiment'], complete=complete, slots=rows, groups=groups, recommendations=recommendations,
                recommendation='Review per-workload recommendations against whole-job costs and resource coverage before changing production; no automatic runner change.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['gate', 'matrix', 'inventory', 'report'])
    parser.add_argument('--jobs', type=Path)
    parser.add_argument('--directory', type=Path, default=Path('results'))
    args = parser.parse_args()
    p = plan()
    if len(slots(p)) != p['cap'] or p['cap'] != 18:
        raise ValueError('invalid experiment budget')
    if args.command == 'matrix':
        print(json.dumps({'include': slots(p)}))
    elif args.command == 'gate':
        raw = subprocess.check_output(['gh', 'api', '--paginate', '--slurp',
            'repos/TauCetiProject/TauCetiCI/actions/workflows/runner-benchmark.yml/runs?event=workflow_dispatch&per_page=100'])
        runs = [r for page in json.loads(raw) for r in page['workflow_runs']]
        authorize(runs, int(os.environ['GITHUB_RUN_ID']), int(os.environ['GITHUB_RUN_ATTEMPT']), os.environ['GITHUB_REF'])
        print('Authorized 18 slots; reruns and later dispatches are disabled.')
    elif args.command == 'inventory':
        digest, count = inventory(args.directory)
        print(json.dumps({'sha256': digest, 'files': count}))
    else:
        results = read_results(args.directory, p)
        pages = json.loads(args.jobs.read_text()) if args.jobs else None
        jobs = [j for page in pages for j in page['jobs']] if pages is not None else None
        report = summarize(p, results, jobs)
        args.directory.mkdir(exist_ok=True, parents=True)
        (args.directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        lines = ['# Runner experiment 16245', '', '| Workload | Shape | Success/planned | Median command seconds | Median whole-job seconds | Estimated compute unit-minutes |', '|---|---|---:|---:|---:|---:|']
        for g in report['groups']:
            lines.append(f"| {g['workload']} | {g['shape']} | {g['successful']}/{g['planned']} | {g['median_elapsed_s']} | {g['median_job_elapsed_s']} | {g['median_unit_minutes']} |")
        lines += ['', '| Workload | Shape | Median sandbox CPU seconds | Largest process RSS bytes |', '|---|---|---:|---:|', *[f"| {g['workload']} | {g['shape']} | {g['median_sandbox_cpu_s']} | {g['largest_process_rss_bytes']} |" for g in report['groups']]]
        lines += ['', *[f"{r['workload']}: {r['shape']}. {r['reason']}." for r in report['recommendations']], '', report['recommendation'], '', 'Costs use whole-job API durations when present, otherwise command durations excluding checkout/provisioning/artifact overhead. Use jobs API duration for whole-job estimates; provider invoice remains authoritative. Missing, failed, partial and cache-incomparable slots stay in report.json. Full phase logs are untrusted statistics; host sampler records have separate scope.']
        (args.directory / 'REPORT.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
