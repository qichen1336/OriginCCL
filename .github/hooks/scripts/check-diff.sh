#!/usr/bin/env bash
#
# Stop hook: surface whitespace problems reported by `git diff --check`.
#
# Exits 1 when problems are found, which the hook contract treats as a non-blocking
# warning. It never exits 2, so a dirty tree cannot trap the agent in a retry loop.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

cd "${repo_root}" 2>/dev/null || exit 0
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0

if ! output="$(git diff --check 2>&1)"; then
    printf 'git diff --check reported whitespace problems:\n%s\n' "${output}" >&2
    exit 1
fi

exit 0
