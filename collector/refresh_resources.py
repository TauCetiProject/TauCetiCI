"""Bounded recent resource collection, independent of the full-history cursor.

Lists at most 100 recent completed runs per instrumented workflow, records at most
20 new trusted attempts, and never waits across an API quota reset. This is a
recent sample, not exhaustive/random coverage. Receipts preserve its limits.
"""
import argparse
import datetime as dt
import json
import os
import re
import time

from . import collect
from .gh import GitHub, RateLimitUnavailable
from .resources import producer_sha


class RefreshBudgetExceeded(RuntimeError):
    pass


class RefreshGitHub(GitHub):
    def __init__(self, max_calls=150, max_seconds=300):
        super().__init__(wait_for_rate_limit=False)
        self.max_calls = max_calls
        self.deadline = time.monotonic() + max_seconds
        self.started_requests = 0

    def check_budget(self):
        if self.started_requests >= self.max_calls or time.monotonic() >= self.deadline:
            raise RefreshBudgetExceeded('resource refresh API/time budget reached')

    def before_request(self):
        self.check_budget()
        self.started_requests += 1


def refresh(gh, run_ids=(), max_runs=20, now=None):
    now = now or dt.datetime.now(collect.UTC)
    since = now - dt.timedelta(hours=6)
    stamp = now.strftime('%Y%m%dT%H%M%SZ') + '-resources'
    scope = {'mode': 'selected-runs' if run_ids else 'bounded-recent-resources',
             'since': None if run_ids else collect.iso(since), 'until': collect.iso(now),
             'list_limit_per_workflow': 100, 'max_runs': max_runs,
             'api_limit': gh.max_calls, 'request_start_budget_seconds': 300}
    receipt = {**scope, 'explicit_run_ids': list(run_ids), 'listings': [],
               'attempted': 0, 'recorded': 0, 'without_artifacts': 0,
               'already_recorded': 0, 'unverified_producers': 0, 'deferred': None,
               'gaps': [], 'error': None}
    enricher = collect.Enricher(gh, 'TauCeti')
    try:
        if run_ids:
            candidates = [gh.get(f'repos/TauCetiProject/TauCeti/actions/runs/{n}') for n in run_ids]
            have = set()  # Explicit verification can refresh an existing snapshot.
        else:
            candidates = []
            for workflow in ('pr-build.yml', 'ci.yml'):
                value = gh.get(f'repos/TauCetiProject/TauCeti/actions/workflows/{workflow}/runs',
                               status='completed', created=f'{collect.iso(since)}..{collect.iso(now)}',
                               per_page=100)
                items = value['workflow_runs']
                candidates.extend(items)
                receipt['listings'].append({'workflow': workflow, 'returned': len(items),
                                            'total_count': value['total_count'],
                                            'truncated': value['total_count'] > len(items)})
            have = collect.seen_keys('runs', 'TauCeti', since, now,
                                     lambda r: (r['run_id'], r['run_attempt']))
        candidates.sort(key=lambda r: (r['created_at'], r['id']), reverse=True)
        visited = set()
        for run in candidates:
            gh.check_budget()
            key = (run['id'], run.get('run_attempt', 1))
            if key in visited:
                continue
            visited.add(key)
            if key in have:
                receipt['already_recorded'] += 1
                continue
            if run.get('status') != 'completed' or not producer_sha('TauCeti', run):
                receipt['unverified_producers'] += 1
                continue
            if receipt['attempted'] >= max_runs:
                receipt['deferred'] = 'run limit reached'
                break
            receipt['attempted'] += 1
            # This path budgets GitHub requests. Defer the separate Bors API
            # fallback to broad collection rather than bypassing that budget.
            record = collect.run_record(gh, 'TauCeti', run, enricher, 'all', batch_fallback=False)
            if not any(o.get('coverage') == 'present' for o in record.get('observations', {}).values()):
                receipt['without_artifacts'] += 1
                receipt['gaps'].append({'run_id': key[0], 'run_attempt': key[1],
                                        'coverage': {k: v.get('coverage') for k, v in
                                                     record.get('observations', {}).items()}})
                continue
            record['collection_scope'] = scope
            # Persist each completed snapshot; a later request cannot discard it.
            collect.write_records('runs', 'TauCeti', [record], 'created_at', stamp)
            receipt['recorded'] += 1
    except (RefreshBudgetExceeded, RateLimitUnavailable) as e:
        receipt['deferred'] = str(e)
    except Exception as e:
        receipt['error'] = type(e).__name__
        raise
    finally:
        receipt['api_calls'] = gh.calls
        receipt['request_attempts'] = gh.started_requests
        directory = collect.RECORDS / 'resource-refresh' / now.strftime('%Y/%m/%d')
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f'{stamp}.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        print(json.dumps(receipt, sort_keys=True), flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runs', default=os.environ.get('RESOURCE_RUN_IDS', ''),
                        help='optional comma-separated completed run IDs for selected verification')
    args = parser.parse_args()
    if args.runs and not re.fullmatch(r'[0-9]+(?:,[0-9]+)*', args.runs):
        parser.error('runs must be comma-separated positive integer IDs')
    ids = list(dict.fromkeys(int(n) for n in args.runs.split(',') if n))
    if len(ids) > 20 or any(n < 1 for n in ids):
        parser.error('at most 20 positive run IDs are allowed')
    refresh(RefreshGitHub(), ids)


if __name__ == '__main__':
    main()
