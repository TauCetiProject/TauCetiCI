"""Publish one completed collector checkpoint, keeping concurrent append-only writers."""
import datetime as dt
import subprocess


def git(*args, check=True):
    return subprocess.run(['git', *args], check=check, timeout=120)


def paths(*args):
    return subprocess.check_output(['git', *args], timeout=120).decode().splitlines()


def records_only(changed):
    if any(not p.startswith(('records/', 'state/')) for p in changed):
        raise RuntimeError('checkpoint may publish only records/ and state/')


def main():
    git('config', 'user.name', 'tauceti-ci-collector')
    git('config', 'user.email', 'tauceti-ci-collector@users.noreply.github.com')
    records_only(paths('diff', '--name-only', 'origin/main...HEAD'))
    git('add', '--', 'records', 'state')
    records_only(paths('diff', '--cached', '--name-only'))
    if git('diff', '--cached', '--quiet', check=False).returncode:
        git('commit', '-qm', f'records: incremental checkpoint {dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%MZ}')
    elif not paths('rev-list', 'origin/main..HEAD'):
        return
    # A previous push may have failed after committing. Retry those commits
    # even when there are no newly staged files.
    for _ in range(5):
        git('fetch', '-q', 'origin', 'main')
        if git('rebase', 'origin/main', check=False).returncode:
            resolved = subprocess.run(['python3', '-m', 'collector.resolve_cursors'], timeout=120)
            if resolved.returncode or git('-c', 'core.editor=true', 'rebase', '--continue', check=False).returncode:
                git('rebase', '--abort', check=False)
                raise RuntimeError('checkpoint conflict requires attention; records retained locally')
        if git('push', '-q', 'origin', 'HEAD:main', check=False).returncode == 0:
            return
    raise RuntimeError('checkpoint push failed after concurrent updates; records retained locally')


if __name__ == '__main__':
    main()
