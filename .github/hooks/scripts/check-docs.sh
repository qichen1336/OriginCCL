#!/usr/bin/env bash
#
# Stop hook: keep the agent from finishing once after a large code change that
# never touched docs/.
#
# AGENTS.md requires the matching layer doc to be updated in the same change
# whenever an architecture boundary, a build command, or the way tests are run
# changes. This hook measures added plus deleted lines under src/; at or above
# OCCL_DOCS_SYNC_MIN_LINES (default 500) with docs/ untouched, it blocks the stop
# and names the layer docs to review. Any change under docs/ is enough to pass.
#
# The agent is blocked at most once per distinct change set. stop_hook_active covers
# the immediate continuation, and a fingerprint of the diff keeps a later stop in the
# same session from repeating the same nudge. Editing docs/ clears the fingerprint.
#
# Exit 0 with the JSON below is the only way this script blocks. It never exits 2,
# and every unexpected condition fails open: a blocking exit would trap the agent in
# a retry loop and spend turns on a hook that cannot help.
set -uo pipefail

kLineThreshold="${OCCL_DOCS_SYNC_MIN_LINES:-500}"
base_ref="${OCCL_DOCS_SYNC_BASE:-HEAD}"
code_scope=(src)
doc_scope=(docs)

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

cd "${repo_root}" 2>/dev/null || exit 0
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0

payload="$(cat)"

# Set when the agent is already running because of a previous stop hook. Never block
# twice in a row, or the session can never end.
if printf '%s' "${payload}" \
    | grep -qE '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then
    exit 0
fi

case "${kLineThreshold}" in
    '' | *[!0-9]*) kLineThreshold=500 ;;
esac

git cat-file -e "${base_ref}^{commit}" 2>/dev/null || exit 0

git_dir="$(git rev-parse --absolute-git-dir 2>/dev/null || printf '')"
[ -n "${git_dir}" ] || exit 0
state_file="${git_dir}/occl-docs-nudge"

# Uncommitted change under the code scope, tracked and untracked.
numstat="$(git diff --numstat "${base_ref}" -- "${code_scope[@]}" 2>/dev/null)"
changed_paths() {
    {
        git diff --name-only "${base_ref}" -- "$@" 2>/dev/null
        git ls-files --others --exclude-standard -- "$@" 2>/dev/null
    } | sort -u
}

# Added plus deleted lines, with untracked files counted by their length since they
# are absent from `git diff`.
code_lines="$(printf '%s\n' "${numstat}" \
    | awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { total += $1 + $2 } END { print total + 0 }')"
while IFS= read -r file; do
    [ -n "${file}" ] || continue
    [ -f "${file}" ] || continue
    lines="$(wc -l <"${file}" 2>/dev/null | tr -d '[:space:]')"
    case "${lines}" in '' | *[!0-9]*) continue ;; esac
    code_lines=$((code_lines + lines))
done < <(git ls-files --others --exclude-standard -- "${code_scope[@]}" 2>/dev/null)

# docs/ moved: the change satisfied the rule, so retire the reminder.
if [ -n "$(changed_paths "${doc_scope[@]}")" ]; then
    rm -f "${state_file}" 2>/dev/null
    exit 0
fi

if [ "${code_lines}" -lt "${kLineThreshold}" ]; then
    rm -f "${state_file}" 2>/dev/null
    exit 0
fi

fingerprint="$(printf '%s\n' "${numstat}" | sha1sum | cut -c1-16)"
if [ -f "${state_file}" ] && [ "$(cat "${state_file}" 2>/dev/null)" = "${fingerprint}" ]; then
    exit 0
fi
printf '%s\n' "${fingerprint}" >"${state_file}" 2>/dev/null

files="$(changed_paths "${code_scope[@]}" | head -n 10 | paste -sd', ' -)"
[ -n "${files}" ] || files="(unknown)"

reason="$(printf '%s\n' \
    "代码改动 ${code_lines} 行（阈值 ${kLineThreshold}），但 docs/ 未同步。改动文件：${files}" \
    "若改动涉及架构边界、构建命令或测试运行方式，请在同一次改动中更新 docs/layers/ 对应文档：executor.md / topology.md / transport.md / planner.md / communicator.md（AGENTS.md Commit Conventions）。" \
    "若确认无需更新文档，说明理由后即可结束。")"

escape_json() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e ':a;N;$!ba;s/\n/\\n/g'
}

printf '{"systemMessage":"%s","hookSpecificOutput":{"hookEventName":"Stop","decision":"block","reason":"%s"}}\n' \
    "$(escape_json "检测到大量代码改动（${code_lines} 行）未同步 docs/，已阻止收工一次。")" \
    "$(escape_json "${reason}")"

exit 0
