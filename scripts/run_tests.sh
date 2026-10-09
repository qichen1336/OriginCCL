#!/usr/bin/env bash
# OriginCCL test entry point.
#
# One script drives the five test binaries. It is the only documented entry point; the
# binaries take --level/--list-cases and are meant to be driven from here. The executor is
# chosen at runtime by the library, so one build covers every run.
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEVEL=0
SUITE="all"
BUILD_DIR=""
REPORT_DIR=""
TIMEOUT=600
JOBS=2
BUILD=true
DRY_RUN=false
ALLOW_SKIP=false
OVERSUBSCRIBE=false
FULL_DATA=false

EXIT_PASS=0
EXIT_FAIL=1
EXIT_SKIP=2
EXIT_USAGE=4

usage() {
    cat <<'EOF'
Usage: scripts/run_tests.sh [options]

Runs the OriginCCL test suite. The executables are built once into
build/ and each run gets its own report file.

Options:
  -h, --help              Show this help and exit.
  -l, --level N           0, 1 or 2 (default 0). See "Levels" below.
    -s, --suite LIST        all | single | multi | transport | p2p (default all).
  -d, --build-dir DIR     Build root (default <repo>/build).
  -r, --report-dir DIR    Report root (default <repo>/test-reports).
  -t, --timeout SEC       Per-run timeout (default 600).
  -j, --jobs N            Build parallelism (default 2).
      --no-build          Reuse the existing build output.
      --dry-run           Print the planned commands and exit.
      --allow-skip        Let environment SKIPs keep the exit code at 0.
      --oversubscribe     Pass --oversubscribe to mpirun (fewer cores than ranks).
      --full-data         Build and run with the production data sizes instead of the
                          small-test ones. Use this on a machine with real resources.
      --list-cases        Print the case matrix of the selected level and exit.

Levels (counts are elements per rank; small-test reduces them 256x):
  0  single machine 4 ranks       multi machine 4 machines x 1 rank   counts 32768 131072
  1  single machine 8 ranks       multi machine 4 machines x 2 ranks  counts 65536 262144
  2  single machine 32 ranks      multi machine 8 machines x 4 ranks  counts 262144 1048576
Level 0 samples every dtype/op/count; level 1 adds the pairwise combinations; level 2
runs the full legal core set. All suites use four channels.

By default the build is configured with -DOCCL_SMALL_TESTS=ON: the library's chunk
granularity and the collective counts shrink by the same 256x factor, so a small VM runs
the identical planner path with 256x less memory. --full-data drops the macro and uses
the production sizes. The transport suites are never scaled: their boundaries and stall
sizes must exceed the transport ring capacities to still cover backpressure. The transport
suite runs four two-process binaries: tcp, shm, rdma and rdma_zc (the zero-copy variant).
The p2p suite uses four ranks at every level, tests local and simulated cross-machine
Send/Recv, and exercises polling and epoll with unscaled payloads through 48 MiB.
Without RDMA, cross-machine rejection is checked and RDMA_ZC transfers are SKIP.

The multi-machine suite makes one host look like several machines through a logical
hostname, so it never leaves the local node. Level 1 and 2 need enough cores or
--oversubscribe. RDMA and the zero-copy RDMA variant without a device with an active
port are reported as SKIP, never as a pass. The executor is picked at runtime: polling
when the cores cover the local ranks, epoll when they do not.

Exit codes:
  0  the requested scope passed
  1  a case failed or a run timed out
  2  only environment SKIPs remain (see --allow-skip)
  4  invalid arguments

Examples:
  scripts/run_tests.sh -h
  scripts/run_tests.sh --level 0
  scripts/run_tests.sh --level 0 --suite single
  scripts/run_tests.sh --level 1 -j 4 --oversubscribe
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
    --full-data)
        FULL_DATA=true
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

validate_list "${SUITE}" "single,multi,transport,p2p" "--suite"
SUITES="$(expand_list "${SUITE}" "single,multi,transport,p2p")"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/test-reports}"

case "${LEVEL}" in
0) SINGLE_RANKS=4
   MULTI_RANKS=4
   MULTI_PER_MACHINE=1
   BASE_COUNTS="32768 131072"
   ;;
1) SINGLE_RANKS=8
   MULTI_RANKS=8
   MULTI_PER_MACHINE=2
   BASE_COUNTS="65536 262144"
   ;;
2) SINGLE_RANKS=32
   MULTI_RANKS=32
   MULTI_PER_MACHINE=4
   BASE_COUNTS="262144 1048576"
   ;;
esac

# The small-test build divides the counts by 256 inside the test binaries; the log line has
# to divide them here too, because the script only ever sees the base values.
if [ "${FULL_DATA}" = true ]; then
    SMALL_TESTS=OFF
    COUNTS="${BASE_COUNTS}"
else
    SMALL_TESTS=ON
    COUNTS=""
    for count in ${BASE_COUNTS}; do
        COUNTS="${COUNTS}${COUNTS:+ }$((count / 256))"
    done
fi
MULTI_MACHINES=$((MULTI_RANKS / MULTI_PER_MACHINE))

if [ "${LIST_CASES:-false}" = true ]; then
    if [ "${BUILD}" = true ] && [ "${DRY_RUN}" = false ]; then
        echo "error: --list-cases needs an existing build; run once without it, or use --no-build" >&2
        exit "${EXIT_USAGE}"
    fi
    binary="${BUILD_DIR}/tests/regression/test_single_machine"
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
    echo "suites:    ${SUITES}"
    echo "report:    ${REPORT_DIR}"
    echo
    echo "planned commands (details of the case matrix need a built binary, see --list-cases):"
    echo "  cmake -S ${ROOT_DIR} -B ${BUILD_DIR} -DOCCL_SMALL_TESTS=${SMALL_TESTS} -DCMAKE_BUILD_TYPE=Debug"
    echo "  cmake --build ${BUILD_DIR} -j ${JOBS}"
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

build() {
    local -a args=(-S "${ROOT_DIR}" -B "${BUILD_DIR}" "-DOCCL_SMALL_TESTS=${SMALL_TESTS}" -DCMAKE_BUILD_TYPE=Debug)
    if [ "${BUILD}" = true ]; then
        cmake "${args[@]}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
        cmake --build "${BUILD_DIR}" -j "${JOBS}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
    fi
    [ -x "${BUILD_DIR}/tests/regression/test_single_machine" ] || return 1
    return 0
}

# run_one <suite> <ranks> <binary>
run_one() {
    local suite="$1"
    local ranks="$2"
    local binary="$3"
    local report="${REPORT_DIR}/${suite}.ranklog"
    local out="${REPORT_DIR}/${suite}.out"
    local level_flag="--level ${LEVEL}"
    local -a extra_args=()
    if [[ "${suite}" == "p2p-rdma" ]]; then
        extra_args+=("--cross-machine")
    fi
    if [ "${suite}" = "single" ] || [ "${suite}" = "multi" ]; then
        extra_args+=("--report" "${report}")
        # Each rank appends its result, so a leftover file would be counted into this run.
        rm -f "${report}" "${report}".*
    fi

    # A single suite is sub-second, so a whole-second clock rounds it to 0 or 1 at random.
    local start end elapsed_ms
    start="$(date +%s%3N)"
    # shellcheck disable=SC2086
    timeout "${TIMEOUT}" "${MPIRUN}" ${MPIRUN_FLAGS} -np "${ranks}" "${BUILD_DIR}/tests/${binary}" ${level_flag} "${extra_args[@]}" \
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

    log_line "$(printf '%-16s %-5s ranks=%-3s cases=%-5s %sms' \
        "${suite}" "${status}" "${ranks}" "${cases}" "${elapsed_ms}")"

    case "${status}" in
    PASS) ;;
    SKIP) total_skip=$((total_skip + 1)) ;;
    *) total_fail=$((total_fail + 1)) ;;
    esac
    return 0
}

log_line "OriginCCL test run: level=${LEVEL} suites=${SUITES}"
log_line "single machine: ${SINGLE_RANKS} ranks   multi machine: ${MULTI_PER_MACHINE} per machine x ${MULTI_MACHINES} machines = ${MULTI_RANKS} ranks"
log_line "counts per rank: ${COUNTS}   data scale: $( [ "${FULL_DATA}" = true ] && echo 'full' || echo 'small (256x)' )"
log_line ""

if ! build; then
    log_line "$(printf '%-16s %-5s' "build" "FAIL")"
    total_fail=$((total_fail + 1))
fi

# The case matrix is shared by both collective suites, so one listing is enough.
if [ "${total_fail}" -eq 0 ]; then
    case_per_rank="$("${BUILD_DIR}/tests/regression/test_single_machine" --level "${LEVEL}" --list-cases 2>/dev/null |
        grep -o 'cases=[0-9]*' | cut -d= -f2 | awk '{ sum += $1 } END { print sum + 0 }')"
    for suite in $(echo "${SUITES}" | tr ',' ' '); do
        case "${suite}" in
        single) run_one "single" "${SINGLE_RANKS}" "regression/test_single_machine" ;;
        multi) run_one "multi" "${MULTI_RANKS}" "regression/test_multi_machine" ;;
        p2p)
            run_one "p2p-local" 4 "regression/test_p2p"
            run_one "p2p-rdma" 4 "regression/test_p2p"
            ;;
        transport)
            for transport in tcp shm rdma rdma_zc; do
                run_one "transport-${transport}" 2 "regression/test_transport_${transport}"
            done
            ;;
        esac
    done
fi

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
