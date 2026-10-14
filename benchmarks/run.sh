#!/usr/bin/env bash
# Invoked by the main-defined benchmark workflow; never by candidate code.
set -euo pipefail
export PATH="$HOME/.elan/bin:/usr/bin:/bin"
export MATHLIB_CACHE_DIR="$RUNNER_TEMP/benchmark-mathlib"
export LAKE_CACHE_DIR="$PWD/pr/.lake/cache"
export LAKE_ARTIFACT_CACHE=true LAKE_RESTORE_ARTIFACTS=true LAKE_NO_CACHE=true
export CI_RESOURCE_WORKFLOW_SHA="$HARNESS_SHA"
export CI_RESOURCE_TESTED_HEAD="$HEAD_SHA" CI_RESOURCE_TESTED_BASE="$CACHE_SHA"
export CI_RESOURCE_DEPENDENCY_BUMP=unknown
mkdir -p results "$MATHLIB_CACHE_DIR"
# Each phase receives host-side resource accounting, including nonzero exits.
phase() {
  python3 gate/scripts/ci_host_resources.py --out "$PWD/results/$1.json" -- "${@:2}"
}
setup() {
  curl -fsSL "https://github.com/leanprover/elan/releases/download/v4.2.3/elan-x86_64-unknown-linux-gnu.tar.gz" -o "$RUNNER_TEMP/elan.tar.gz"
  echo "df0b2b3a439961ffcbb3985214365ffe40f49bc871df04dff268c7d8e21ca8b2  $RUNNER_TEMP/elan.tar.gz" | sha256sum -c -
  tar -xzf "$RUNNER_TEMP/elan.tar.gz" -C "$RUNNER_TEMP" elan-init
  "$RUNNER_TEMP/elan-init" -y --default-toolchain none
  (cd pr && elan toolchain install "$(cat lean-toolchain)")
  curl -fsSL https://archive.ubuntu.com/ubuntu/pool/main/b/bubblewrap/bubblewrap_0.9.0-1build1_amd64.deb -o "$RUNNER_TEMP/bwrap.deb"
  echo "dde30d1f24da50446d647ed504ec8dc5a714f171974d054c70a589b48ba38b48  $RUNNER_TEMP/bwrap.deb" | sha256sum -c -
  sudo dpkg -i "$RUNNER_TEMP/bwrap.deb" >/dev/null
  echo '3e33f8255e1411ada346db27028c25ef5ba4fa3327754a2321ea4c8732a3bc10  /usr/bin/bwrap' | sha256sum -c -
  if [ "$(sysctl -n kernel.apparmor_restrict_unprivileged_userns 2>/dev/null || echo 0)" = 1 ]; then
    printf '%s\n' 'abi <abi/4.0>,' 'include <tunables/global>' 'profile bwrap /usr/bin/bwrap flags=(unconfined) {' '  userns,' '}' | sudo tee /etc/apparmor.d/bwrap >/dev/null
    sudo apparmor_parser -r /etc/apparmor.d/bwrap
  fi
  sudo -n sysctl -w vm.max_map_count=1048576 >/dev/null
}
mathlib() {
  # All cells start from empty job-local dependency/download directories. Only
  # official dependency code from the immutable, already-landed pins runs here.
  (cd pr && lake exe cache get) 2>&1 | tee results/mathlib.log
  ! grep -qF 'some files were not found in the cache' results/mathlib.log
  mkdir -p pr/.lake/tmp
}
root_cache() {
  if [ "$CACHE_SHA" = cold ]; then
    rm -rf pr/.lake/cache pr/.lake/build
    printf '{"sha256":"cold-empty-root","files":0}\n' > results/cache.json
  else
    # The checkout points at the exact published parent; no ancestor fallback.
    git -C pr diff --exit-code "$CACHE_SHA" "$HEAD_SHA" -- lakefile.toml lean-toolchain lake-manifest.json
    git -C pr checkout --detach "$CACHE_SHA"
    LAKE_CACHE_MAX_REVS=1 bash gate/scripts/lake-cache-get.sh pr
    python3 -m benchmarks.experiment inventory --directory pr/.lake/cache > results/cache.json
  fi
  git -C pr checkout --detach "$HEAD_SHA"
}
drivers() {
  real_lean=$(cd pr && elan which lean)
  bash gate/scripts/perf/prepare-watchdog-toolchain.sh "$RUNNER_TEMP/watchdog" "$real_lean"
  bash gate/scripts/build-lint-driver.sh pr gate/scripts/LintEnvDriver.lean "$RUNNER_TEMP/drivers"
  bash gate/scripts/build-audit-driver.sh pr gate/scripts "$RUNNER_TEMP/drivers"
}
export -f setup mathlib root_cache drivers
if [ "${1:-}" = phase ]; then "$2"; exit; fi
rm -rf pr/.lake
mkdir -p pr/.lake
phase setup bash "$0" phase setup
phase mathlib bash "$0" phase mathlib
phase root-cache bash "$0" phase root_cache
phase trusted-drivers bash "$0" phase drivers
# Exactly the same mount policy runs the canary and complete trusted build/audits.
policy=(env -i /usr/bin/bwrap --tmpfs /
  --ro-bind /usr /usr --ro-bind /etc /etc
  --symlink usr/bin /bin --symlink usr/sbin /sbin
  --symlink usr/lib /lib --symlink usr/lib64 /lib64
  --dev /dev --proc /proc --tmpfs /tmp
  --ro-bind "$HOME/.elan" "$HOME/.elan"
  --ro-bind "$RUNNER_TEMP/watchdog" "$RUNNER_TEMP/watchdog"
  --ro-bind "$RUNNER_TEMP/drivers" "$RUNNER_TEMP/drivers"
  --ro-bind "$PWD/gate" "$PWD/gate"
  --ro-bind "$PWD/pr" "$PWD/pr" --bind "$PWD/pr/.lake" "$PWD/pr/.lake"
  --remount-ro /
  --setenv PATH "$HOME/.elan/bin:/usr/bin:/bin" --setenv HOME "$HOME"
  --setenv CI true --setenv LAKE_NO_CACHE true
  --setenv HOST_CACHE "$MATHLIB_CACHE_DIR"
  --setenv LAKE_ARTIFACT_CACHE "$LAKE_ARTIFACT_CACHE"
  --setenv LAKE_RESTORE_ARTIFACTS "$LAKE_RESTORE_ARTIFACTS"
  --setenv LAKE_CACHE_DIR "$LAKE_CACHE_DIR"
  --setenv WATCHDOG_TOOLCHAIN "$RUNNER_TEMP/watchdog"
  --setenv LINT_DRIVER_EXE "$RUNNER_TEMP/drivers/lint-env-driver"
  --setenv IS_BATCH true --setenv STAGE_LAKE_CACHE 0
  --setenv TAUCETI_PROJECT_ROOT "$PWD/pr"
  --setenv TAUCETI_TRUSTED_SCRIPTS "$PWD/gate/scripts"
  --chdir "$PWD/pr"
  --unshare-user --unshare-ipc --unshare-pid --unshare-net --unshare-uts
  --unshare-cgroup --disable-userns --die-with-parent --new-session)
"${policy[@]}" -- bash -euo pipefail -c '
  test -z "${GH_TOKEN:-}${GITHUB_TOKEN:-}${ACTIONS_RUNTIME_TOKEN:-}"
  test "$(tr "\0" "\n" < /proc/1/environ | grep -c . || true)" = 0
  if touch "$HOME/.elan/benchmark-poison"; then exit 1; fi
  if touch ../gate/benchmark-poison; then exit 1; fi
  if unshare --user --map-root-user true; then exit 1; fi
  if curl -fsS --max-time 5 https://example.com >/dev/null; then exit 1; fi
  test ! -e "$HOST_CACHE"
  touch .lake/sandbox-canary
'
phase sandbox "${policy[@]}" -- bash ../gate/scripts/sandbox-build.sh
# Candidate-written statistics stay distinct from the host observations.
cp pr/.lake/telemetry/phases.tsv results/candidate-phases.tsv
cp pr/.lake/telemetry/lake-build.log results/lake-build.log
