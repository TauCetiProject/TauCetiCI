"""Record a slot even if setup or compilation fails. Never retry a benchmark."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from .experiment import plan, slots


def main():
    p = plan()
    cell = next(s for s in slots(p) if s['slot'] == os.environ['SLOT'])
    spec = p['shapes'][cell['shape']]
    out = Path('results')
    out.mkdir(exist_ok=True)
    start = time.monotonic()
    record = dict(cell, experiment=p['experiment'], run_id=os.environ['GITHUB_RUN_ID'],
                  run_attempt=int(os.environ['GITHUB_RUN_ATTEMPT']), harness_sha=os.environ['HARNESS_SHA'],
                  tooling_sha=p['tooling_sha'], head_sha=p['head_sha'], status='failed')
    try:
        if record['run_attempt'] != 1:
            raise ValueError('reruns cannot execute benchmark slots')
        cpus = len(os.sched_getaffinity(0))
        mem = int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemTotal:'))) * 1024
        record.update(cpu_count=cpus, host_memory_bytes=mem,
                      cpu_model=next((l.split(':', 1)[1].strip() for l in Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('model name')), None))
        # Reject an incorrectly provisioned runner instead of comparing wrong shapes.
        if cpus != spec['cpu'] or not .85 * spec['memory_gb'] * 10**9 <= mem <= 1.15 * spec['memory_gb'] * 2**30:
            raise ValueError('actual runner CPU/memory does not match requested shape')
        for path in ('cpu.max', 'memory.max'):
            f = Path('/sys/fs/cgroup') / path
            record[path] = f.read_text().strip() if f.exists() else None
        manifest = hashlib.sha256(Path('pr/lake-manifest.json').read_bytes()).hexdigest()
        if manifest != p['manifest_sha256'] or Path('pr/lean-toolchain').read_text().strip() != p['toolchain']:
            raise ValueError('unexpected workload pins')
        record['manifest_sha256'] = manifest
        env = {**os.environ, 'HEAD_SHA': p['head_sha'],
               'CACHE_SHA': p['workloads'][cell['workload']]['cache_sha'] or 'cold'}
        # No caller credentials in dependency tooling; bwrap has its own empty environment.
        for name in list(env):
            if 'TOKEN' in name or 'SECRET' in name or 'PASSWORD' in name or name.startswith('ACTIONS_'):
                del env[name]
        code = subprocess.call(['bash', 'benchmarks/run.sh'], env=env)
        record['exit_code'] = code
        record['status'] = 'success' if code == 0 else 'failed'
    except Exception as error:
        record['error'] = str(error)
    finally:
        record['elapsed_s'] = time.monotonic() - start
        record['estimated_unit_minutes'] = record['elapsed_s'] / 60 * spec['units_per_minute']
        record['estimated_prepaid_usd'] = record['estimated_unit_minutes'] * p['pricing']['prepaid_usd_per_unit_minute']
        record['estimated_overage_usd'] = record['estimated_unit_minutes'] * p['pricing']['overage_usd_per_unit_minute']
        cache = out / 'cache.json'
        telemetry_errors = []
        if cache.exists():
            try:
                record['cache_identity'] = json.loads(cache.read_text())['sha256']
            except (ValueError, KeyError) as error:
                telemetry_errors.append('cache: ' + str(error))
        phases = {}
        for f in out.glob('*.json'):
            if f.name in ('result.json', 'cache.json'):
                continue
            try:
                phases[f.stem] = json.loads(f.read_text())
            except ValueError as error:
                telemetry_errors.append(f.name + ': ' + str(error))
        record['phases'] = phases
        record['telemetry_errors'] = telemetry_errors
        if record['status'] == 'success' and (telemetry_errors or 'cache_identity' not in record or set(phases) != {'setup', 'mathlib', 'root-cache', 'trusted-drivers', 'sandbox'} or any(v.get('command_accounting_scope') != 'reaped-descendants' for v in phases.values())):
            record['status'] = 'partial-telemetry'
        (out / 'result.json').write_text(json.dumps(record, indent=2) + '\n')
    return 0 if record['status'] == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
