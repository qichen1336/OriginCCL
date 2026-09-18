#!/usr/bin/env bash
#
# Stop hook: run clang-format on the C/C++ file(s) an agent changed.
#
# A Stop payload carries no edited-file list, so the files come from git: the diff
# against HEAD plus untracked files. clang-format is idempotent, so already-formatted
# files are rewritten unchanged. This script always exits 0 and never blocks: a
# missing or failing formatter must never trap the agent.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

cd "${repo_root}" 2>/dev/null || exit 0
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0

# Keep the most recent payload around, so the real stdin schema can be inspected.
payload="$(cat)"
printf '%s' "${payload}" >/tmp/occl-hook-last.json

# Escape a string for embedding in a JSON systemMessage.
escape_json() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e ':a;N;$!ba;s/\n/\\n/g'
}

# Set when the agent is already running because of a previous stop hook. Never
# re-enter, or the session can never end.
if printf '%s' "${payload}" \
    | grep -qE '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then
    exit 0
fi

files="$( {
    git diff --name-only HEAD 2>/dev/null
    git ls-files --others --exclude-standard 2>/dev/null
} | sort -u | grep -E '\.(cpp|cc|cxx|h|hpp|hh)$' || true )"

[ -n "${files}" ] || exit 0

if ! command -v clang-format >/dev/null 2>&1; then
    printf '{"systemMessage":"%s"}\n' \
        "$(escape_json "clang-format is not installed; skipped formatting changed sources.")"
    exit 0
fi

formatted=""
messages=""
while IFS= read -r file_path; do
    [ -f "${file_path}" ] || continue

    before="$(cksum <"${file_path}" 2>/dev/null)"
    if ! error="$(clang-format -i "${file_path}" 2>&1)"; then
        messages="${messages}clang-format failed on ${file_path}: ${error}
"
        continue
    fi
    [ "${before}" = "$(cksum <"${file_path}" 2>/dev/null)" ] || formatted="${formatted}${file_path}
"
done <<<"${files}"

[ -n "${formatted}" ] && messages="clang-format rewrote:
${formatted}${messages}"

if [ -n "${messages}" ]; then
    printf '{"systemMessage": "%s"}\n' "$(escape_json "${messages}")"
fi

exit 0
