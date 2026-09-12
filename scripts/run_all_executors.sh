#!/usr/bin/env bash
#
# Run the full 1/2/4-rank verification matrix once per executor strategy. The executor
# is a compile-time choice (OCCL_EXECUTOR), so each strategy gets its own build directory
# and its own invocation of run_tests.sh.
#
# Usage:
#   scripts/run_all_executors.sh [extra run_tests.sh options...]
#
# Any extra arguments are forwarded to every run_tests.sh invocation (e.g. --timeout).
# Exit code is 0 only if every executor passes every tier.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

executors=(multi_thread epoll polling)
failed=0

for executor in "${executors[@]}"; do
    echo
    echo "###################################################################"
    echo "### executor: $executor"
    echo "###################################################################"
    if ! "$repo_root/scripts/run_tests.sh" --build-dir "$repo_root/build-$executor" --executor "$executor" "$@"; then
        echo ">>> FAIL: executor $executor" >&2
        failed=1
    fi
done

echo
if [[ $failed -eq 0 ]]; then
    echo ">>> ALL EXECUTORS PASSED: ${executors[*]}"
else
    echo ">>> SOME EXECUTORS FAILED" >&2
fi
exit "$failed"
