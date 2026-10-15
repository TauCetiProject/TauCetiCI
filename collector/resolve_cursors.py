"""Resolve only collector cursor conflicts, preserving conservative coverage.

Records remain append-only. An older overlapping writer must not lose its
observations because another writer advanced the JSON cursor. Keep the earlier
shared watermark and let the next collection resume/deduplicate. Unknown file
conflicts are never resolved automatically.
"""
import datetime as dt
import json
from pathlib import Path
import subprocess

ALLOWED = {'state/cursor.json', 'state/observations-cursor.json',
           'state/inventory-cursor.json', 'state/build-cursor.json', 'state/main-cursor.json', 'state/details-cursor.json'}


def merge(left, right):
    for data in (left, right):
        if not isinstance(data, dict):
            raise ValueError('cursor must be an object')
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError('invalid cursor entry')
            when = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
            if when.tzinfo is None:
                raise ValueError('cursor timestamp must be timezone-aware')
    result = {**left, **right}
    for key in left.keys() & right.keys():
        result[key] = min((left[key], right[key]),
                          key=lambda value: dt.datetime.fromisoformat(value.replace('Z', '+00:00')))
    return result


def resolve():
    paths = subprocess.check_output(['git', 'diff', '--name-only', '--diff-filter=U', '-z']).decode().strip('\0').split('\0')
    if not paths or not set(paths) <= ALLOWED:
        raise ValueError('only collector cursor conflicts can be resolved')
    # Validate every conflict before changing anything.
    resolved = {}
    for path in paths:
        left = json.loads(subprocess.check_output(['git', 'show', ':2:' + path]))
        right = json.loads(subprocess.check_output(['git', 'show', ':3:' + path]))
        resolved[path] = merge(left, right)
    for path, value in resolved.items():
        Path(path).write_text(json.dumps(value, indent=1, sort_keys=True) + '\n')
    subprocess.run(['git', 'add', '--', *paths], check=True)


if __name__ == '__main__':
    resolve()
