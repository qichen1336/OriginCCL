#!/usr/bin/env bash
#
# PostToolUse hook: run clang-format on the file(s) an agent just edited.
#
# The hook payload arrives as JSON on stdin. The exact field name is not part of the
# documented contract, so several spellings are tried, and anything that does not
# resolve to an existing C/C++ file is ignored. This script always exits 0: a missing
# or failing formatter must never block the agent.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

# Keep the most recent payload around, so the real stdin schema can be inspected.
payload="$(cat)"
printf '%s' "${payload}" >/tmp/occl-hook-last.json

# Escape a string for embedding in a JSON systemMessage.
escape_json() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e ':a;N;$!ba;s/\n/\\n/g'
}

# Collect candidate paths: the likely keys first, then any generic "path"-like value.
candidates=""
if command -v jq >/dev/null 2>&1; then
    candidates="$(printf '%s' "${payload}" | jq -r '
        .tool_input.file_path, .tool_input.filePath, .tool_input.path,
        .file_path, .filePath, .path
        | select(type == "string" and . != "" and . != "null")' 2>/dev/null)"
fi

if [ -z "${candidates}" ]; then
    candidates="$(printf '%s' "${payload}" \
        | grep -oE '"(file_path|filePath|path)"[[:space:]]*:[[:space:]]*"[^"]+"' \
        | sed -E 's/^"[^"]+"[[:space:]]*:[[:space:]]*"([^"]+)"$/\1/')"
fi

[ -n "${candidates}" ] || exit 0

messages=""
while IFS= read -r file_path; do
    [ -n "${file_path}" ] || continue

    case "${file_path}" in
        /*) abs="${file_path}" ;;
        *) abs="${repo_root}/${file_path}" ;;
    esac

    # Only existing C/C++ sources and headers are formatted.
    [ -f "${abs}" ] || continue
    case "${abs}" in
        *.cpp|*.cc|*.cxx|*.h|*.hpp|*.hh) ;;
        *) continue ;;
    esac

    if ! command -v clang-format >/dev/null 2>&1; then
        messages="${messages}clang-format is not installed; skipped formatting ${file_path}
"
        break
    fi

    if ! error="$(clang-format -i "${abs}" 2>&1)"; then
        messages="${messages}clang-format failed on ${file_path}: ${error}
"
    fi
done <<<"${candidates}"

if [ -n "${messages}" ]; then
    printf '{"systemMessage": "%s"}\n' "$(escape_json "${messages}")"
fi

exit 0
