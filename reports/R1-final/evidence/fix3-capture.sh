#!/usr/bin/env bash
# Run a verification command, preserve its full combined output and real exit.
# Usage: bash reports/R1-final/evidence/fix3-capture.sh LOG COMMAND [ARG...]
set -uo pipefail
log=$1
shift
if [[ -e "$log" ]]; then
  echo "Refusing to overwrite existing evidence: $log" >&2
  exit 2
fi
{
  printf 'START '; date -Iseconds
  printf 'PWD '; pwd
  printf 'HEAD '; git rev-parse HEAD
  printf 'HEAD_TREE '; git rev-parse 'HEAD^{tree}'
  printf 'CMD '; printf '%q ' "$@"; printf '\n'
  printf 'CARGO_TARGET_DIR=%s\nSAKURAPOOL_RUST_WORKER=%s\nPYTHONPATH=%s\n' \
    "${CARGO_TARGET_DIR-}" "${SAKURAPOOL_RUST_WORKER-}" "${PYTHONPATH-<unset>}"
  "$@"
  rc=$?
  printf '\nEXIT_CODE=%d\nEND ' "$rc"; date -Iseconds
  exit "$rc"
} 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
