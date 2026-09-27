#!/usr/bin/env bash
# OriginCCL test entry point.
#
# One script drives the five test binaries across the two executors and the profiles that
# matter: a plain build, an ASan+UBSan build, and a gcov coverage build. It is the only
# documented entry point; the binaries take --level/--list-cases and are meant to be driven
# from here.
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEVEL=0
EXECUTORS="all"
SUITE="all"
PROFILE="all"
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
EXIT_REPORT=3
EXIT_USAGE=4

usage() {
    cat <<'EOF'
Usage: scripts/run_tests.sh [options]

Runs the OriginCCL test suite. The executables are built per executor into
build-tests/<executor>/<profile> and each run gets its own report file.

Options:
  -h, --help              Show this help and exit.
  -l, --level N           0, 1 or 2 (default 0). See "Levels" below.
  -e, --executor LIST     all | polling | epoll (default all). Comma separated.
  -s, --suite LIST        all | single | multi | transport (default all).
  -p, --profile LIST      all | normal | asan-ubsan | coverage (default all).
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
  0  single machine 4 ranks       multi machine 4x1 ranks  counts 1 1024 10240
  1  single machine 8 ranks       multi machine 4x2 ranks  counts 1 1024 10240 65536
  2  single machine 32 ranks      multi machine 8x4 ranks  counts 1024 10240 65536
Level 0 samples every dtype/op/count; level 1 adds the pairwise combinations and the
n_channels=3 profile; level 2 runs the full legal core set plus n_channels=3 and 1.

The multi-machine suite makes one host look like several machines through a logical
hostname, so it never leaves the local node. Level 1 and 2 need enough cores or
--oversubscribe. RDMA without a device with an active port is reported as SKIP, never
as a pass. TSan is not offered: the covered executors are polled from one thread.

Profile details:
  normal      plain Debug build.
  asan-ubsan  AddressSanitizer + UndefinedBehaviorSanitizer (UBSan halts on the first
              error). Leak checking is disabled: a program that only calls
              MPI_Init/MPI_Finalize under this instrumentation leaks the same 15464
              bytes as the suites, with Open MPI frames unresolved, so that channel
              reports the runtime instead of the code under test.
  coverage    gcovr report (lcov+genhtml, then plain gcov text as fallbacks) under
              test-reports/coverage/. Each executor is reported on its own.

Exit codes:
  0  the requested scope passed
  1  a case failed, a sanitizer fired, or a run timed out
  2  only environment SKIPs remain (see --allow-skip)
  3  the coverage report could not be generated
  4  invalid arguments

Examples:
  scripts/run_tests.sh -h
  scripts/run_tests.sh --level 0
  scripts/run_tests.sh --level 0 --executor polling --profile asan-ubsan
  scripts/run_tests.sh --level 1 --executor all --profile all -j 4 --oversubscribe
  scripts/run_tests.sh --level 2 --profile coverage
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

coverage_flag() {
    if [ "$1" = "coverage" ]; then
        echo " -DOCCL_ENABLE_COVERAGE=ON"
    fi
    return 0
}

sanitizer_flags() {
    if [ "$1" = "asan-ubsan" ]; then
        echo "-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all -g -O1"
    fi
    return 0
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
    -p | --profile)
        [ $# -ge 2 ] || die_usage "$1 needs a value"
        PROFILE="$2"
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
validate_list "${PROFILE}" "normal,asan-ubsan,coverage" "--profile"
EXECUTORS="$(expand_list "${EXECUTORS}" "polling,epoll")"
SUITES="$(expand_list "${SUITE}" "single,multi,transport")"
PROFILES="$(expand_list "${PROFILE}" "normal,asan-ubsan,coverage")"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-tests}"
REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/test-reports}"

case "${LEVEL}" in
0) SINGLE_RANKS=4
   MULTI_RANKS=4
   MULTI_PER_MACHINE=1
   ;;
1) SINGLE_RANKS=8
   MULTI_RANKS=8
   MULTI_PER_MACHINE=2
   ;;
2) SINGLE_RANKS=32
   MULTI_RANKS=32
   MULTI_PER_MACHINE=4
   ;;
esac

if [ "${LIST_CASES:-false}" = true ]; then
    if [ "${BUILD}" = true ] && [ "${DRY_RUN}" = false ]; then
        echo "error: --list-cases needs an existing build; run once without it, or use --no-build" >&2
        exit "${EXIT_USAGE}"
    fi
    binary="${BUILD_DIR}/$(echo "${EXECUTORS}" | cut -d, -f1)/normal/tests/test_single_machine"
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
    echo "level ${LEVEL}: single machine ${SINGLE_RANKS} ranks, multi machine ${MULTI_PER_MACHINE} per machine x ${MULTI_RANKS} ranks"
    echo "executors: ${EXECUTORS}"
    echo "suites:    ${SUITES}"
    echo "profiles:  ${PROFILES}"
    echo "report:    ${REPORT_DIR}"
    echo
    echo "planned commands (details of the case matrix need a built binary, see --list-cases):"
    IFS=',' read -r -a exec_list <<<"${EXECUTORS}"
    IFS=',' read -r -a profile_list <<<"${PROFILES}"
    for executor in "${exec_list[@]}"; do
        for profile in "${profile_list[@]}"; do
            echo "  cmake -S ${ROOT_DIR} -B ${BUILD_DIR}/${executor}/${profile} -DOCCL_EXECUTOR=${executor} -DCMAKE_BUILD_TYPE=Debug$(coverage_flag "${profile}")"
            echo "  cmake --build ${BUILD_DIR}/${executor}/${profile} -j ${JOBS}"
        done
    done
    exit 0
fi

mkdir -p "${REPORT_DIR}"
SUMMARY="${REPORT_DIR}/summary.txt"
: >"${SUMMARY}"

total_fail=0
total_skip=0
total_cases=0

log_line() {
    echo "$@" | tee -a "${SUMMARY}"
}

build_profile() {
    local executor="$1"
    local profile="$2"
    local dir="${BUILD_DIR}/${executor}/${profile}"
    local -a args=(-S "${ROOT_DIR}" -B "${dir}" "-DOCCL_EXECUTOR=${executor}" -DCMAKE_BUILD_TYPE=Debug)
    if [ "${profile}" = "coverage" ]; then
        args+=(-DOCCL_ENABLE_COVERAGE=ON)
    fi
    local sanitize
    sanitize="$(sanitizer_flags "${profile}")"
    if [ -n "${sanitize}" ]; then
        # One -D value with spaces, so it must stay a single argument.
        args+=("-DCMAKE_CXX_FLAGS=${sanitize}")
        args+=("-DCMAKE_EXE_LINKER_FLAGS=${sanitize}")
        args+=("-DCMAKE_SHARED_LINKER_FLAGS=${sanitize}")
    fi
    if [ "${BUILD}" = true ]; then
        cmake "${args[@]}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
        cmake --build "${dir}" -j "${JOBS}" >>"${REPORT_DIR}/build.log" 2>&1 || return 1
    fi
    [ -x "${dir}/tests/test_single_machine" ] || return 1
    if [ "${profile}" = "coverage" ]; then
        # Counters accumulate across runs; drop them so the report describes this run only.
        find "${dir}" -name '*.gcda' -delete 2>/dev/null
    fi
    return 0
}

# run_one <executor> <profile> <suite> <ranks> <binary>
run_one() {
    local executor="$1"
    local profile="$2"
    local suite="$3"
    local ranks="$4"
    local binary="$5"
    local dir="${BUILD_DIR}/${executor}/${profile}"
    local report="${REPORT_DIR}/${executor}.${profile}.${suite}.ranklog"
    local out="${REPORT_DIR}/${executor}.${profile}.${suite}.out"
    local level_flag="--level ${LEVEL}"
    local -a extra_args=()
    if [ "${suite}" = "single" ] || [ "${suite}" = "multi" ]; then
        extra_args+=("--report" "${report}")
    fi

    local start end elapsed
    start="${SECONDS}"
    if [ "${profile}" = "asan-ubsan" ]; then
        # Leak checking is off, and this is a measured limitation, not a convenience: a program
        # that only calls MPI_Init/MPI_Finalize under this same instrumentation leaks the same
        # 15464 bytes in 35 allocations as the suites do, with every frame reported as
        # "<unknown module>" because Open MPI and its providers are loaded without frame
        # pointers. ASan's own diagnostics and UBSan still run, and the suites compare every
        # payload byte, so this only narrows the leak channel.
        export ASAN_OPTIONS="detect_leaks=0"
        export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
    else
        unset ASAN_OPTIONS
        unset UBSAN_OPTIONS
    fi
    # shellcheck disable=SC2086
    timeout "${TIMEOUT}" "${MPIRUN}" ${MPIRUN_FLAGS} -np "${ranks}" "${dir}/tests/${binary}" ${level_flag} "${extra_args[@]}" \
        >"${out}" 2>&1
    local code=$?
    unset ASAN_OPTIONS
    unset UBSAN_OPTIONS
    end="${SECONDS}"
    elapsed=$((end - start))

    local status
    case "${code}" in
    0) status="PASS" ;;
    2) status="SKIP" ;;
    124) status="TIMEOUT" ;;
    *) status="FAIL" ;;
    esac

    local cases=0
    if [ -f "${report}" ] || compgen -G "${report}.*" >/dev/null 2>&1; then
        # Each rank appends its own result line, so the total is the number of rank-cases
        # reported across the run.
        cases="$(cat "${report}" "${report}".* 2>/dev/null | wc -l | tr -d ' ')"
    fi
    total_cases=$((total_cases + cases))

    if [ "${status}" != "PASS" ]; then
        # A sanitizer report turns a plain failure into an explicit risk finding.
        if grep -qE "ERROR: (Address|Leak)Sanitizer|runtime error:" "${out}" 2>/dev/null; then
            status="SANITIZER"
        fi
    fi

    log_line "$(printf '%-8s %-8s %-10s %-5s ranks=%-3s cases=%-5s %ss' \
        "${executor}" "${profile}" "${suite}" "${status}" "${ranks}" "${cases}" "${elapsed}")"

    case "${status}" in
    PASS) ;;
    SKIP) total_skip=$((total_skip + 1)) ;;
    *) total_fail=$((total_fail + 1)) ;;
    esac
    return 0
}

log_line "OriginCCL test run: level=${LEVEL} suites=${SUITES} executors=${EXECUTORS} profiles=${PROFILES}"
log_line "single machine: ${SINGLE_RANKS} ranks   multi machine: ${MULTI_PER_MACHINE} per machine x ${MULTI_RANKS} ranks"
log_line ""

for executor in $(echo "${EXECUTORS}" | tr ',' ' '); do
    for profile in $(echo "${PROFILES}" | tr ',' ' '); do
        if ! build_profile "${executor}" "${profile}"; then
            log_line "$(printf '%-8s %-8s %-10s %-5s' "${executor}" "${profile}" "build" "FAIL")"
            total_fail=$((total_fail + 1))
            continue
        fi
        for suite in $(echo "${SUITES}" | tr ',' ' '); do
            case "${suite}" in
            single) run_one "${executor}" "${profile}" "single" "${SINGLE_RANKS}" "test_single_machine" ;;
            multi) run_one "${executor}" "${profile}" "multi" "${MULTI_RANKS}" "test_multi_machine" ;;
            transport)
                for transport in tcp shm rdma; do
                    run_one "${executor}" "${profile}" "transport-${transport}" 2 "test_transport_${transport}"
                done
                ;;
            esac
        done
    done
done

if contains "coverage" "${PROFILES}"; then
    log_line ""
    if ! "${ROOT_DIR}/scripts/coverage_report.sh" "${BUILD_DIR}" "${REPORT_DIR}"; then
        log_line "coverage report: FAIL"
        log_line "raw gcov output was kept under ${REPORT_DIR}/coverage"
        echo "coverage report failed" >&2
        exit "${EXIT_REPORT}"
    fi
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
