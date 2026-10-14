"""Readiness check only: prepare the pinned inputs, never build TauCeti."""
import os
import subprocess
from .experiment import plan
from .worker import without_credentials


def main():
    p = plan()
    workload = p['workloads']['warm-heavy']
    environment = without_credentials({**os.environ, 'HEAD_SHA': workload['head_sha'],
        'CACHE_SHA': workload['cache_sha'], 'CACHE_MAP_URL': workload['cache_map_url'],
        'CACHE_MAP_SHA256': workload['cache_map_sha256']})
    subprocess.run(['bash', 'benchmarks/run.sh', 'prepare-only'], env=environment, check=True)


if __name__ == '__main__':
    main()
