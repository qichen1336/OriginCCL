#!/usr/bin/env bash
# OriginCCL test entry point.
#
# One script drives the five test binaries across the two covered executors. It is the only
# documented entry point; the binaries take --level/--list-cases and are meant to be driven
# from here.
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEVEL=0
EXECUTORS="all"
SUITE="all"
BUILD_DIR=""
REPORT_DIR=""
TIMEOUT=600
JOBS=2
BUILD=true
DRY_RUN=false
ALLOW_SKIP=false
OVERSUBSCRIBE=false

EXIT_PASS=0
EXIT_FAIL=1
EXIT_SKIP=2
EXIT_USAGE=4

usage() {
    cat <<'EOF'
Usage: scripts/run_tests.sh [options]

Runs the OriginCCL test suite. The executables are built per executor into
build-tests/<executor> and each run gets its own report file.

Options:
  -h, --help              Show this help and exit.
  -l, --level N           0, 1 or 2 (default 0). See "Levels" below.
  -e, --executor LIST     all | polling | epoll (default all). Comma separated.
  -s, --suite LIST        all | single | multi | transport (default all).
  -d, --build-dir DIR     Build root (default <repo>/build-tests).
  -r, --report-dir DIR    Report root (default <repo>/test-reports).
  -t, --timeout SEC       Per-run timeout (default 600).
  -j, --jobs N            Build parallelism (default 2).
      --no-build          Reuse the existing build output.
      --dry-run           Print the planned commands and exit.
      --allow-skip        Let environment SKIPs keep the exit code at 0.
      --oversubscribe     Pass --oversubscribe to mpirun (fewer cores than ranks).
      --list-cases        Print the case matrix of the selected level and exit.

Levels (counts are elements per rank):
  0  single machine 4 ranks       multi machine 4 machines x 1 rank   counts 1 1024 8192
  1  single machine 8 ranks       multi machine 4 machines x 2 ranks  counts 8192 32768 65536
  2  single machine 32 ranks      multi machine 8 machines x 4 ranks  counts 8192 32768 65536
Level 0 samples every dtype/op/count; level 1 adds the pairwise combinations; level 2
runs the full legal core set. All suites use four channels.

The multi-machine suite makes one host look like several machines through a logical
hostname, so it never leaves the local node. Level 1 and 2 need enough cores or
--oversubscribe. RDMA without a device with an active port is reported as SKIP, never
as a pass. TSan is not offered: the covered executors are polled from one thread.

Exit codes:
  0  the requested scope passed
  1  a case failed or a run timed out
  2  only environment SKIPs remain (see --allow-skip)
  4  invalid arguments

Examples:
  scripts/run_tests.sh -h
  scripts/run_tests.sh --level 0
  scripts/run_tests.sh --level 0 --executor polling
  scripts/run_tests.sh --level 1 --executor all -j 4 --oversubscribe
  scripts/run_tests.sh --level 2 --suite transport
EOF
}

die_usage() {
    echo "error: $*" >&2
    echo "run 'scripts/run_tests.sh -h' for usage" >&2
    exit "${EXIT_USAGE}"
}

contains() {
    local needle="$1"
    local list="$2"
    case ",${list}," in
    *",${needle},"*) return 0 ;;
    *) return 1 ;;
    esac
}

# Validation must run in the caller's shell: a die_usage inside a command substitution only
# exits the subshell, which would silently turn a bad list into an empty one.
validate_list() {
    local value="$1"
    local allowed="$2"
    local name="$3"
    if [ "${value}" = "all" ]; then
        return 0
    fi
    if [ -z "${value}" ]; then
        die_usage "empty ${name}"
    fi
    local item
    IFS=',' read -r -a items <<<"${value}"
    for item in "${items[@]}"; do
        if [ -z "${item}" ]; then
            die_usage "empty entry in ${name} '${value}'"
        fi
        if ! contains "${item}" "${allowed}"; then
            die_usage "unknown ${name} '${item}' (allowed: all, ${allowed})"
        fi
    done
}

expand_list() {
    if [ "$1" = "all" ]; then
        echo "$2"
    else
        echo "$1"
    fi
}

while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        usage
        exit 0
        ;;
    -l | --level)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        LEVEL="$2"
        shift 2
        ;;
    -e | --executor)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        EXECUTORS="$2"
        shift 2
        ;;
    -s | --suite)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        SUITE="$2"
        shift 2
        ;;
    -d | --build-dir)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        BUILD_DIR="$2"
        shift 2
        ;;
    -r | --report-dir)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        REPORT_DIR="$2"
        shift 2
        ;;
    -t | --timeout)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        TIMEOUT="$2"
        shift 2
        ;;
    -j | --jobs)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        JOBS="$2"
        shift 2
        ;;
    --no-build)
        BUILD=false
        shift
        ;;
    --dry-run)
        DRY_RUN=true
        shift
        ;;
    --allow-skip)
        ALLOW_SKIP=true
        shift
        ;;
    --oversubscribe)
        OVERSUBSCRIBE=true
        shift
        ;;
    --list-cases)
        LIST_CASES=true
        shift
        ;;
    *)
        die_usage "unknown option '$1'"
        ;;
    esac
done

case "${LEVEL}" in
0 | 1 | 2) ;;
*) die_usage "invalid --level '${LEVEL}' (expected 0, 1 or 2)" ;;
esac
case "${TIMEOUT}" in
'' | *[!0-9]*) die_usage "invalid --timeout '${TIMEOUT}'" ;;
esac
case "${JOBS}" in
'' | *[!0-9]*) die_usage "invalid --jobs '${JOBS}'" ;;
esac

# Only the polling and epoll executors are covered; the other two are not built here.
validate_list "${EXECUTORS}" "polling,epoll" "--executor"
validate_list "${SUITE}" "single,multi,transport" "--suite"
EXECUTORS="$(expand_list "${EXECUTORS}" "polling,epoll")"
SUITES="$(expand_list "${SUITE}" "single,multi,transport")"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-tests}"
REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/test-reports}"

case "${LEVEL}" in
0) SINGLE_RANKS=4
   MULTI_RANKS=4
   MULTI_PER_MACHINE=1
   COUNTS="1 1024 8192"
   ;;
1) SINGLE_RANKS=8
   MULTI_RANKS=8
   MULTI_PER_MACHINE=2
   COUNTS="8192 32768 65536"
   ;;
2) SINGLE_RANKS=32
   MULTI_RANKS=32
   MULTI_PER_MACHINE=4
   COUNTS="8192 32768 65536"
   ;;
esac
MULTI_MACHINES=$((MULTI_RANKS / MULTI_PER_MACHINE))

if [ "${LIST_CASES:-false}" = true ]; then
    if [ "${BUILD}" = true ] && [ "${DRY_RUN}" = false ]; then
        echo "error: --list-cases needs an existing build; run once without it, or use --no-build" >&2
        exit "${EXIT_USAGE}"
    fi
    binary="${BUILD_DIR}/$(echo "${EXECUTORS}" | cut -d, -f1)/tests/test_single_machine"
    if [ ! -x "${binary}" ]; then
        echo "error: ${binary} is missing; build first" >&2
        exit "${EXIT_USAGE}"
    fi
    echo "single-machine and multi-machine case matrix for level ${LEVEL}:"
    "${binary}" --level "${LEVEL}" --list-cases
    exit 0
fi

MPIRUN="$(command -v mpirun || true)"
if [ -z "${MPIRUN}" ]; then
    echo "error: mpirun not found; the suites use MPI for rank, world size and the unique id" >&2
    exit "${EXIT_SKIP}"
fi

if [ "${OVERSUBSCRIBE}" = true ]; then
    MPIRUN_FLAGS="--oversubscribe"
else
    MPIRUN_FLAGS=""
fi

if [ "${DRY_RUN}" = true ]; then
    echo "level ${LEVEL}: single machine ${SINGLE_RANKS} ranks, multi machine ${MULTI_PER_MACHINE} per machine x ${MULTI_MACHINES} machines = ${MULTI_RANKS} ranks"
    echo "counts per rank: ${COUNTS}"
    echo "executors: ${EXECUTORS}"
    echo "suites:    ${SUITES}"
    echo "report:    ${REPORT_DIR}"
    echo
    echo "planned commands (details of the case matrix need a built binary, see --list-cases):"
    IFS=',' read -r -a exec_list <<<"${EXECUTORS}"
    for executor in "${exec_list[@]}"; do
        echo "  cmake -S ${ROOT_DIR} -B ${BUILD_DIR}/${executor} -DOCCL_EXECUTOR=${executor} -DCMAKE_BUILD_TYPE=Debug"
        echo "  cmake --build ${BUILD_DIR}/${executor} -j ${JOBS}"
    done
    exit 0
fi

mkdir -p "${REPORT_DIR}"
SUMMARY="${REPORT_DIR}/summary.txt"
: >"${SUMMARY}"

total_fail=0
total_skip=0
total_cases=0
case_per_rank=""

log_line() {
    echo "$@" | tee -a "${SUMMARY}"
}

build_executor() {
    local executor="$1"
    local dir="${BUILD_DIR}/${executor}"
    local -a args=(-S "${ROOT_DIR}" -B "${dir}" "-DOCCL_EXECUTOR=${executor}" -DCMAKE_BUILD_TYPE=Debug)
    if [ "${BUILD}" = true ]; then
        cmake "${args[@]}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
        cmake --build "${dir}" -j "${JOBS}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
    fi
    [ -x "${dir}/tests/test_single_machine" ] || return 1
    return 0
}

# run_one <executor> <suite> <ranks> <binary>
run_one() {
    local executor="$1"
    local suite="$2"
    local ranks="$3"
    local binary="$4"
    local dir="${BUILD_DIR}/${executor}"
    local report="${REPORT_DIR}/${executor}.${suite}.ranklog"
    local out="${REPORT_DIR}/${executor}.${suite}.out"
    local level_flag="--level ${LEVEL}"
    local -a extra_args=()
    if [ "${suite}" = "single" ] || [ "${suite}" = "multi" ]; then
        extra_args+=("--report" "${report}")
        # Each rank appends its result, so a leftover file would be counted into this run.
        rm -f "${report}" "${report}".*
    fi

    # A single suite is sub-second, so a whole-second clock rounds it to 0 or 1 at random.
    local start end elapsed_ms
    start="$(date +%s%3N)"
    # shellcheck disable=SC2086
    timeout "${TIMEOUT}" "${MPIRUN}" ${MPIRUN_FLAGS} -np "${ranks}" "${dir}/tests/${binary}" ${level_flag} "${extra_args[@]}" \
        >"${out}" 2>&1
    local code=$?
    end="$(date +%s%3N)"
    elapsed_ms=$((end - start))

    local status
    case "${code}" in
    0) status="PASS" ;;
    2) status="SKIP" ;;
    124) status="TIMEOUT" ;;
    *) status="FAIL" ;;
    esac

    local cases=0
    if [ "${suite}" = "single" ] || [ "${suite}" = "multi" ]; then
        # One reported result per rank per suite, but every case inside runs on every rank.
        cases=$((case_per_rank * ranks))
    fi
    total_cases=$((total_cases + cases))

    log_line "$(printf '%-8s %-10s %-5s ranks=%-3s cases=%-5s %sms' \
        "${executor}" "${suite}" "${status}" "${ranks}" "${cases}" "${elapsed_ms}")"

    case "${status}" in
    PASS) ;;
    SKIP) total_skip=$((total_skip + 1)) ;;
    *) total_fail=$((total_fail + 1)) ;;
    esac
    return 0
}

log_line "OriginCCL test run: level=${LEVEL} suites=${SUITES} executors=${EXECUTORS}"
log_line "single machine: ${SINGLE_RANKS} ranks   multi machine: ${MULTI_PER_MACHINE} per machine x ${MULTI_MACHINES} machines = ${MULTI_RANKS} ranks"
log_line "counts per rank: ${COUNTS}"
log_line ""

for executor in $(echo "${EXECUTORS}" | tr ',' ' '); do
    if ! build_executor "${executor}"; then
        log_line "$(printf '%-8s %-10s %-5s' "${executor}" "build" "FAIL")"
        total_fail=$((total_fail + 1))
        continue
    fi
    if [ "${case_per_rank}" = "" ]; then
        # The case matrix is shared by both collective suites, so one listing is enough.
        case_per_rank="$("${BUILD_DIR}/${executor}/tests/test_single_machine" --level "${LEVEL}" --list-cases 2>/dev/null |
            grep -o 'cases=[0-9]*' | cut -d= -f2 | awk '{ sum += $1 } END { print sum + 0 }')"
    fi
    for suite in $(echo "${SUITES}" | tr ',' ' '); do
        case "${suite}" in
        single) run_one "${executor}" "single" "${SINGLE_RANKS}" "test_single_machine" ;;
        multi) run_one "${executor}" "multi" "${MULTI_RANKS}" "test_multi_machine" ;;
        transport)
            for transport in tcp shm rdma; do
                run_one "${executor}" "transport-${transport}" 2 "test_transport_${transport}"
            done
            ;;
        esac
    done
done

log_line ""
if [ "${total_fail}" -gt 0 ]; then
    log_line "result: FAIL (${total_fail} failed runs, ${total_cases} reported rank-cases)"
    exit "${EXIT_FAIL}"
fi
if [ "${total_skip}" -gt 0 ]; then
    if [ "${ALLOW_SKIP}" = true ]; then
        log_line "result: SKIP accepted (${total_skip} skipped runs, ${total_cases} reported rank-cases)"
        exit "${EXIT_PASS}"
    fi
    log_line "result: SKIP (${total_skip} skipped runs; pass --allow-skip to accept)"
    exit "${EXIT_SKIP}"
fi
log_line "result: PASS (${total_cases} reported rank-cases)"
exit "${EXIT_PASS}"
