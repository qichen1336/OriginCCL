#!/usr/bin/env bash
#
# SessionStart + PostToolUse hook: remind the agent to keep the skill knowledge base
# in sync when it makes large code changes without touching .github/skills/.
#
# SessionStart records a baseline commit, the change volume already present at that
# commit, and how many skill files were already dirty.
# PostToolUse measures the same two numbers relative to that baseline. Once the agent
# has written at least OCCL_SKILL_SYNC_LINES (default 100) lines -- added plus deleted --
# under the code/build/test scope without editing .github/skills/, extra context is
# injected into the conversation naming the reference file for each layer that changed.
#
# The counter resets whenever the skill tree is edited, and after each reminder, so the
# agent is nudged roughly once per threshold-sized batch rather than on every edit.
#
# This script always exits 0. additionalContext is only honoured on a zero exit, and a
# non-zero exit would only trap the agent in a retry loop, so every failure stays silent.
set -uo pipefail

kLineThreshold="${OCCL_SKILL_SYNC_LINES:-100}"
state_dir="${TMPDIR:-/tmp}/occl-skill-sync"
scope=(src include tests CMakeLists.txt scripts)

# Tools that can plausibly write into the repository. Anything else is skipped early.
mutating_tools=" create_file replace_string_in_file multi_replace_string_in_file
 insert_edit_into_file edit_notebook_file editFiles apply_patch
 run_in_terminal create_and_run_task run_notebook_cell "

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

cd "${repo_root}" 2>/dev/null || exit 0
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0

payload="$(cat)"
[ -n "${payload}" ] || exit 0

have_jq=0
command -v jq >/dev/null 2>&1 && have_jq=1

num() {
    case "${1:-}" in
        '' | *[!0-9]*) printf '0' ;;
        *) printf '%s' "$1" ;;
    esac
}

json_field() {
    [ "${have_jq}" -eq 1 ] || return 0
    printf '%s' "${payload}" | jq -r "$1 // empty" 2>/dev/null
}

raw_field() {
    printf '%s' "${payload}" \
        | grep -oE "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" \
        | head -n1 \
        | sed -E 's/^"[^"]*"[[:space:]]*:[[:space:]]*"(.*)"$/\1/'
}

field() {
    local value
    value="$(json_field "$1")"
    [ -n "${value}" ] || value="$(raw_field "$2")"
    printf '%s' "${value}"
}

tool_paths() {
    if [ "${have_jq}" -eq 1 ]; then
        printf '%s' "${payload}" | jq -r '
            (.tool_input.filePath?, .tool_input.file_path?, .tool_input.path?,
             ((.tool_input.files? // [])[]), ((.tool_input.paths? // [])[]))
            | select(type == "string")' 2>/dev/null
    else
        printf '%s' "${payload}" \
            | grep -oE '"[^"]*\.(cpp|cc|cxx|h|hpp|hh|txt|sh)"' \
            | tr -d '"'
    fi
}

rel_path() {
    case "$1" in
        "${repo_root}"/*) printf '%s' "${1#"${repo_root}"/}" ;;
        *) printf '%s' "$1" ;;
    esac
}

in_scope() {
    case "$1" in
        src/* | include/* | tests/* | scripts/* | CMakeLists.txt) return 0 ;;
        *) return 1 ;;
    esac
}

# Added plus deleted lines under the code/build/test scope, relative to a commit.
# Untracked files are not part of `git diff`, so their line count is added by hand.
code_lines() {
    local base="$1" total=0 add del path file lines
    while read -r add del path; do
        [ -n "${add:-}" ] || continue
        case "${add}" in -) continue ;; esac
        total=$((total + add + del))
    done < <(git diff --numstat "${base}" -- "${scope[@]}" 2>/dev/null)

    while IFS= read -r file; do
        [ -n "${file}" ] || continue
        in_scope "${file}" || continue
        [ -f "${file}" ] || continue
        lines="$(wc -l <"${file}" 2>/dev/null | tr -d '[:space:]')"
        total=$((total + $(num "${lines}")))
    done < <(git ls-files --others --exclude-standard 2>/dev/null)

    printf '%s' "${total}"
}

# How many files under .github/skills/ differ from the baseline commit.
skills_files() {
    local base="$1" count
    count="$({
        git diff --name-only "${base}" 2>/dev/null
        git ls-files --others --exclude-standard 2>/dev/null
    } | grep -c '^\.github/skills/' 2>/dev/null)"
    num "${count}"
}

# In-scope paths that changed relative to a commit, tracked and untracked.
changed_paths() {
    local base="$1"
    {
        git diff --name-only "${base}" -- "${scope[@]}" 2>/dev/null
        git ls-files --others --exclude-standard 2>/dev/null
    } | sort -u
}

# Which knowledge base file owns a given source path.
doc_for_path() {
    case "$1" in
        *planner* | */types.h) printf 'reference/planner.md' ;;
        *executor*) printf 'reference/executor.md' ;;
        *topology*) printf 'reference/topology.md' ;;
        *transport*) printf 'reference/transport.md' ;;
        *communicator* | *bootstrap* | */channel.h) printf 'reference/communicator.md' ;;
        *) printf 'SKILL.md' ;;
    esac
}

state_key="$(printf '%s' "$(field '.session_id' 'session_id')" | tr -c 'A-Za-z0-9._-' '_')"
state_key="${state_key:-default}"
state_file="${state_dir}/${state_key}.state"

state_get() {
    local value=""
    if [ -f "${state_file}" ]; then
        value="$(grep -m1 "^$1=" "${state_file}" 2>/dev/null | cut -d= -f2- || true)"
    fi
    [ -n "${value}" ] || value="$2"
    printf '%s' "${value}"
}

state_put() {
    mkdir -p "${state_dir}" 2>/dev/null || return 0
    [ -f "${state_file}" ] || : >"${state_file}" 2>/dev/null || return 0
    local pair key value
    for pair in "$@"; do
        key="${pair%%=*}"
        value="${pair#*=}"
        if grep -q "^${key}=" "${state_file}" 2>/dev/null; then
            sed -i "s|^${key}=.*|${key}=${value}|" "${state_file}" 2>/dev/null
        else
            printf '%s=%s\n' "${key}" "${value}" >>"${state_file}" 2>/dev/null
        fi
    done
}

event="$(field '.hook_event_name' 'hook_event_name')"

if [ "${event}" = "SessionStart" ]; then
    baseline="$(git rev-parse HEAD 2>/dev/null || printf '')"
    [ -n "${baseline}" ] || exit 0
    state_put "baseline=${baseline}"
    state_put "ref_lines=$(code_lines "${baseline}")"
    state_put "skills_files=$(skills_files "${baseline}")"
    exit 0
fi

[ "${event}" = "PostToolUse" ] || exit 0

# Cheap gate: skip tools that cannot write the repository. Fail open when the tool name
# is missing, and let anything that mentions an in-scope path through.
tool_name="$(field '.tool_name' 'tool_name')"
gated=1
if [ -z "${tool_name}" ]; then
    gated=0
else
    case "${mutating_tools}" in *" ${tool_name} "*) gated=0 ;; esac
fi

if [ "${gated}" -eq 1 ]; then
    while IFS= read -r candidate; do
        [ -n "${candidate}" ] || continue
        if in_scope "$(rel_path "${candidate}")"; then
            gated=0
            break
        fi
    done < <(tool_paths)
fi

[ "${gated}" -eq 0 ] || exit 0

baseline="$(state_get baseline '')"
if [ -z "${baseline}" ] || ! git cat-file -e "${baseline}^{commit}" 2>/dev/null; then
    baseline="$(git rev-parse HEAD 2>/dev/null || printf '')"
    [ -n "${baseline}" ] || exit 0
    state_put "baseline=${baseline}"
fi

code="$(num "$(code_lines "${baseline}")")"
skills_now="$(num "$(skills_files "${baseline}")")"
ref="$(num "$(state_get ref_lines 0)")"
skills_before="$(num "$(state_get skills_files 0)")"

# The knowledge base was edited: restart the counter so a later large change still nudges.
if [ "${skills_now}" -gt "${skills_before}" ]; then
    state_put "ref_lines=${code}" "skills_files=${skills_now}"
    exit 0
fi

# Deletions can shrink the total; never leave the reference point above it.
[ "${code}" -ge "${ref}" ] || ref="${code}"

delta=$((code - ref))
[ "${delta}" -ge "${kLineThreshold}" ] || exit 0

targets="$(changed_paths "${baseline}" \
    | while IFS= read -r path; do doc_for_path "${path}"; done \
    | sort -u | paste -sd' ' -)"
[ -n "${targets}" ] || targets="SKILL.md"

message="$(printf '%s\n' \
    "[skill-sync] 本会话已累计改动 ${delta} 行代码（阈值 ${kLineThreshold}），但 .github/skills/ 尚未更新。" \
    "请按改动所在层更新对应知识库：${targets}" \
    "规则见 .github/skills/originccl-project/SKILL.md 的「使用后必须维护本 Skill」。")"

if [ "${have_jq}" -eq 1 ]; then
    jq -n --arg msg "${message}" \
        '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $msg}}'
else
    escaped="$(printf '%s' "${message}" \
        | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e ':a;N;$!ba;s/\n/\\n/g')"
    printf '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"%s"}}\n' \
        "${escaped}"
fi

state_put "ref_lines=${code}"

exit 0
