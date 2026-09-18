#!/usr/bin/env bash
#
# Verification matrix for OriginCCL. Runs every tier AGENTS.md documents, because
# they are different code paths and not just "more of the same":
#
#   tier 0  test_transport_shm    shared-memory transport, forked endpoint pair, no ranks
#   tier 1  <test>                single rank (MPI singleton), takes the no-data-plane path
#   tier 2  mpirun -np 2          shared memory, the ring degenerates, prev == next
#   tier 3  mpirun -np 4          shared memory, all four channels
#   tier 4  mpirun -np 2          OCCL_DISABLE_SHM=0, which must not force TCP
#   tier 5  mpirun -np 2          unusable rendezvous directory, which must fail initialization
#   tier 6  mpirun -np 2          OCCL_DISABLE_SHM=1, same-host edges forced onto TCP
#   tier 7  mpirun -np 4          OCCL_DISABLE_SHM=1, all four channels over TCP
#   tier 8  mpirun -np 2          OCCL_DISABLE_SHM=1, which must not bind a rendezvous path
#
# Tiers 2-5 exercise the library's own selection (shared memory for same-host edges),
# tiers 6-8 override it with TCP. The 2- and 4-rank matrix runs in both modes, because the
# transport selection must not depend on the executor or on the rank count. Every tier
# makes the same collective assertions, so a tier proves its code path ran end to end, not
# which transport was picked.
#
# Tiers 5 and 8 pin down the override semantics: a shared-memory failure is never quietly
# turned into a TCP fallback, and the override is a selection rather than a fallback. They
# obstruct the same rendezvous directory and demand opposite outcomes.
#
# Tier 0 covers the transport itself and tier 1 has no data plane, so neither depends on
# the mode: tier 0 runs in every mode, tier 1 runs once, in the shm pass. Tier 0 needs
# neither mpirun nor MPI at all.
#
# A missing mpirun is reported as SKIP and never as a pass: silently going green on
# a machine that only ran tier 1 is the failure mode this script exists to prevent.
#
# Usage:
#   scripts/run_tests.sh [options]
#
# Options:
#   --coverage         configure with -DOCCL_ENABLE_COVERAGE=ON, then report
#   --no-build         reuse the existing build directory as-is
#   --build-dir <dir>  build directory (default: <repo>/build)
#   --build-type <t>   CMAKE_BUILD_TYPE (coverage defaults to Debug, see below)
#   --executor <name>  OCCL_EXECUTOR: multi_thread, epoll, polling, or reactor (default multi_thread)
#   --transport <mode> all (default): tiers 0-8, both transport modes
#                      shm:           tiers 0-5, the library's own transport selection
#                      tcp:           tier 0 and tiers 6-8, OCCL_DISABLE_SHM=1
#   --timeout <secs>   per-tier timeout (default: 120)
#   --allow-skip       a missing mpirun is a warning, not a failure exit
#   -h, --help         this text
#
# --coverage pins CMAKE_BUILD_TYPE=Debug unless --build-type says otherwise: at
# -O3 the compiler inlines, merges and deletes lines, so the counters no longer
# describe the source you are looking at.
#
# Exit codes: 0 all tiers passed; 1 a tier failed; 2 a tier was skipped;
#             3 the coverage report could not be generated.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir="$repo_root/build"
build_type=""
timeout_s=120
do_build=1
coverage=0
allow_skip=0
executor="multi_thread"
transport="all"

# The header is the usage text: print its comment lines, stopping at the first command.
# Deriving it beats a line range, which goes stale every time the header grows.
usage() { awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --coverage) coverage=1 ;;
        --no-build) do_build=0 ;;
        --build-dir)
            build_dir="$2"
            shift
            ;;
        --build-type)
            build_type="$2"
            shift
            ;;
        --executor)
            executor="$2"
            shift
            ;;
        --transport)
            transport="$2"
            shift
            ;;
        --timeout)
            timeout_s="$2"
            shift
            ;;
        --allow-skip) allow_skip=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 64
            ;;
    esac
    shift
done

case "$transport" in
    all | shm | tcp) ;;
    *)
        echo "unknown transport: $transport (expected all, shm, or tcp)" >&2
        usage >&2
        exit 64
        ;;
esac

# A narrowed mode is a deliberate choice, but the script's whole point is that a pass
# means the full matrix ran, so say out loud when it did not.
if [[ $transport != "all" ]]; then
    echo "note: --transport $transport runs a subset of the tiers; only --transport all covers the full matrix" >&2
fi

# Absolute, so that --build-dir with a relative path still resolves from here.
build_dir=$(cd "$repo_root" && mkdir -p "$build_dir" && cd "$build_dir" && pwd)
test_bin="$build_dir/tests/test_allreduce"
shm_test_bin="$build_dir/tests/test_transport_shm"

n_cpu=$(nproc 2> /dev/null || getconf _NPROCESSORS_ONLN 2> /dev/null || echo 1)

failed=0
skipped=0

run_tier() {
    local label="$1"
    shift
    echo
    echo "--- $label"
    local rc=0
    timeout "$timeout_s" "$@" || rc=$?
    if [[ $rc -eq 0 ]]; then
        echo ">>> PASS: $label"
        return 0
    fi
    if [[ $rc -eq 124 ]]; then
        echo ">>> FAIL: $label timed out after ${timeout_s}s (bootstrap hang?)" >&2
    else
        echo ">>> FAIL: $label exited with $rc" >&2
    fi
    failed=1
    return 1
}

# mpirun needs one slot per rank; below that it refuses to start rather than
# reporting an error we can read. The matrix tops out at kMaxRanks, so the flag is decided
# once rather than per tier.
kMaxRanks=4
mpi_extra=()
if [[ "$n_cpu" -lt "$kMaxRanks" ]]; then
    mpi_extra+=(--oversubscribe)
fi
mpi_extra_label=""
if [[ ${#mpi_extra[@]} -gt 0 ]]; then
    mpi_extra_label=" (${mpi_extra[*]})"
fi

have_mpirun=0
if command -v mpirun > /dev/null 2>&1; then
    have_mpirun=1
fi

# Usage: run_mpi_tier <label> <np> [NAME=value | -u NAME ...]
# Trailing arguments go to env(1), so a tier can export the transport override or clear it.
run_mpi_tier() {
    local label="$1"
    local np="$2"
    shift 2
    local -a launcher=(mpirun "${mpi_extra[@]}" -np "$np")
    if [[ $# -gt 0 ]]; then
        launcher=(env "$@" "${launcher[@]}")
    fi
    run_tier "$label, mpirun -np $np$mpi_extra_label" "${launcher[@]}" "$test_bin"
}

# The rendezvous sockets live in one directory (/tmp/originccl), which makes the failure
# mode observable without knowing the port rank 0 picked at runtime: a regular file where
# that directory must be fails the directory creation on every rank. The same obstruction is
# used with both expectations: with shared memory enabled it must fail initialization instead
# of quietly falling back to TCP, and with OCCL_DISABLE_SHM=1 it must not affect the run at
# all, because nothing creates the directory.
#
# Usage: run_blocked_rendezvous_tier <label> <fail|ok> [NAME=value | -u NAME ...]
# Trailing arguments go to env(1).
run_blocked_rendezvous_tier() {
    local label="$1"
    local expect="$2"
    shift 2
    local log="$build_dir/blocked-rendezvous-$expect.log"
    local rendezvous_dir="/tmp/originccl"
    rm -rf "$rendezvous_dir"
    : > "$rendezvous_dir"

    local rc=0
    echo
    echo "--- $label"
    timeout "$timeout_s" env "$@" mpirun "${mpi_extra[@]}" -np 2 "$test_bin" > "$log" 2>&1 || rc=$?
    rm -f "$rendezvous_dir"

    if [[ "$expect" == ok ]]; then
        if [[ $rc -eq 0 ]]; then
            echo ">>> PASS: $label (exit 0 with the rendezvous directory unusable)"
            rm -f "$log"
            return 0
        fi
    elif [[ $rc -ne 0 && $rc -ne 124 ]] \
        && grep -q 'Failed to create the shared-memory rendezvous directory' "$log" \
        && ! grep -q 'Communicator init successfully' "$log"; then
        # Two-sided on purpose. A zero exit means the endpoint quietly went to TCP, and
        # "Communicator init successfully" means initialization passed and a later
        # assertion rejected the transport - a different bug that must not read as a
        # clean initialization failure.
        echo ">>> PASS: $label (init failed instead of falling back to TCP)"
        rm -f "$log"
        return 0
    fi

    echo ">>> FAIL: $label (exit $rc, full output: $log)" >&2
    tail -n 20 "$log" >&2
    failed=1
    return 1
}

if [[ $do_build -eq 1 ]]; then
    if [[ $coverage -eq 1 ]]; then
        cov_flag=ON
        : "${build_type:=Debug}"
    else
        cov_flag=OFF
    fi

    configure_args=(-DOCCL_ENABLE_COVERAGE="$cov_flag" -DOCCL_EXECUTOR="$executor")
    if [[ -n "$build_type" ]]; then
        configure_args+=(-DCMAKE_BUILD_TYPE="$build_type")
    fi

    echo "=== configuring (OCCL_EXECUTOR=$executor, OCCL_ENABLE_COVERAGE=$cov_flag${build_type:+, CMAKE_BUILD_TYPE=$build_type})"
    cmake -S "$repo_root" -B "$build_dir" "${configure_args[@]}" > /dev/null
    echo "=== building with -j$n_cpu"
    cmake --build "$build_dir" -j"$n_cpu"
elif [[ $coverage -eq 1 ]]; then
    echo "warning: --coverage with --no-build; assuming $build_dir is already instrumented" >&2
fi

if [[ ! -x "$test_bin" ]]; then
    echo "error: $test_bin not found; drop --no-build or build first" >&2
    exit 1
fi

# .gcda counters accumulate across runs, so a stale file from an earlier partial
# run would inflate the report. Start from zero.
if [[ $coverage -eq 1 ]]; then
    find "$build_dir" -name '*.gcda' -delete
fi

# Tier 0 is single-process and rank-free: the forked pair needs no launcher, so run it
# before anything that depends on mpirun being present.
run_tier "tier 0: shared-memory transport (forked pair)" "$shm_test_bin" || true

# Tiers 1-5 all run with shared memory enabled. The test asserts that a single-host run
# used shared memory for every edge unless OCCL_DISABLE_SHM was exactly "1", so clearing the
# variable (tiers 2-3) and setting it to "0" (tier 4) must both keep shared memory.
if [[ $transport != "tcp" ]]; then
    # Tier 1 must not inherit OMPI_COMM_WORLD_*: MPI_Init would otherwise join the enclosing
    # mpirun world instead of coming up as a singleton, silently turning tier 1 into ws=N.
    run_tier "tier 1: single rank (no data plane)" \
        env -u OMPI_COMM_WORLD_RANK -u OMPI_COMM_WORLD_SIZE "$test_bin" || true

    if [[ $have_mpirun -eq 1 ]]; then
        run_mpi_tier "tier 2: shared memory (default selection)" 2 -u OCCL_DISABLE_SHM || true
        run_mpi_tier "tier 3: shared memory (default selection)" 4 -u OCCL_DISABLE_SHM || true
        run_mpi_tier "tier 4: shared memory (OCCL_DISABLE_SHM=0)" 2 OCCL_DISABLE_SHM=0 || true
        run_blocked_rendezvous_tier "tier 5: unusable rendezvous directory must fail init" fail \
            -u OCCL_DISABLE_SHM || true
    else
        echo
        echo ">>> SKIP: mpirun not found, tiers 2-5 were NOT run" >&2
        skipped=1
    fi
fi

if [[ $transport != "shm" ]]; then
    if [[ $have_mpirun -eq 1 ]]; then
        run_mpi_tier "tier 6: TCP (OCCL_DISABLE_SHM=1)" 2 OCCL_DISABLE_SHM=1 || true
        run_mpi_tier "tier 7: TCP (OCCL_DISABLE_SHM=1)" 4 OCCL_DISABLE_SHM=1 || true
        run_blocked_rendezvous_tier "tier 8: TCP override must not touch the rendezvous directory" ok \
            OCCL_DISABLE_SHM=1 || true
    else
        echo
        echo ">>> SKIP: mpirun not found, tiers 6-8 were NOT run" >&2
        skipped=1
    fi
fi

report_coverage() {
    local out="$build_dir/coverage"
    mkdir -p "$out"

    if command -v gcovr > /dev/null 2>&1; then
        if gcovr --root "$repo_root" \
            --filter "$repo_root/src/" \
            --filter "$repo_root/include/" \
            --html-details "$out/index.html" \
            --txt --print-summary \
            "$build_dir"; then
            echo
            echo ">>> HTML report: $out/index.html"
            return 0
        fi
        echo "gcovr failed, falling back to lcov" >&2
    fi

    if command -v lcov > /dev/null 2>&1 && command -v genhtml > /dev/null 2>&1; then
        lcov --capture --directory "$build_dir" --output-file "$out/lcov.info" > /dev/null
        lcov --remove "$out/lcov.info" '/usr/*' '*/third_party/*' \
            --output-file "$out/lcov.info" > /dev/null
        genhtml "$out/lcov.info" --output-directory "$out/html"
        echo
        echo ">>> HTML report: $out/html/index.html"
        return 0
    fi

    # gcov is GCC's own profiler, so it is present whenever --coverage built at all.
    # It gives a text summary but no HTML; install lcov/gcovr to upgrade.
    echo >&2
    echo "note: neither gcovr nor lcov/genhtml is installed; reporting with raw gcov" >&2
    echo "      for an HTML report: sudo apt-get install -y lcov gcovr" >&2
    report_coverage_gcov "$out" || return 3
}

# One gcov run per translation unit, each in its own directory so the annotation files
# it writes cannot be mistaken for build output.
#
# The numbers come from gcov's own summary lines rather than from re-counting the
# annotation files. Recounting is tempting but wrong: gcov emits one record per basic
# block, so a source line can appear several times, and its branch accounting does not
# correspond to the `branch` lines at all (261 lines vs a reported 193 branches for
# utils.cpp). gcov's summary is the same source of truth gcovr and lcov consume, so the
# hit counts below are derived from its percentages - which can drift by a line or two
# across a large file, and is exactly why this falls back to it only when neither
# gcovr nor lcov is available.
report_coverage_gcov() {
    local out="$1"
    local tmp="$out/gcov"
    local rows="$out/.rows"
    local report="$out/coverage.txt"
    rm -rf "$tmp"
    mkdir -p "$tmp"
    : > "$rows"

    local gcno dir
    while IFS= read -r gcno; do
        dir="$tmp/$(basename "$gcno" .gcno)"
        mkdir -p "$dir"
        (cd "$dir" && gcov -b -c -p "$gcno" 2> /dev/null) |
            awk -v root="$repo_root/" '
                function flush() {
                    if (have && cur ~ "^(src|tests)/.*[.]cpp$")
                        printf "%-34s %6d %6d %6d %6d\n", cur, lh, lt, bh, bt
                    have = 0
                    bh = 0
                    bt = 0
                }
                /^File / {
                    flush()
                    cur = $0
                    sub(/^File ./, "", cur)
                    sub(/.$/, "", cur)
                    sub(root, "", cur)
                    next
                }
                /^Lines executed:/ {
                    split($0, a, "% of ")
                    p = a[1]
                    sub(/^Lines executed:/, "", p)
                    lt = a[2] + 0
                    lh = int(p * lt / 100 + 0.5)
                    have = 1
                    next
                }
                /^Taken at least once:/ {
                    split($0, a, "% of ")
                    p = a[1]
                    sub(/^Taken at least once:/, "", p)
                    bt = a[2] + 0
                    bh = int(p * bt / 100 + 0.5)
                    next
                }
                END { flush() }
            ' >> "$rows"
    done < <(find "$build_dir" -name '*.gcno' -print | sort)

    if [[ ! -s "$rows" ]]; then
        echo "error: no instrumented sources found under $build_dir" >&2
        echo "       is $build_dir built with -DOCCL_ENABLE_COVERAGE=ON?" >&2
        return 1
    fi

    awk '
        function pct(h, t) { return t > 0 ? sprintf("%.1f%%", 100 * h / t) : "n/a" }
        BEGIN {
            printf "%-34s %6s %6s %8s %8s %8s\n", "FILE", "LINES", "HIT", "LINE%", "BRANCH", "TAKEN%"
            print "--------------------------------------------------------------------------------"
        }
        {
            printf "%-34s %6d %6d %8s %8d %8s\n", $1, $3, $2, pct($2, $3), $5, pct($4, $5)
            if ($1 ~ /^src\//) { slh += $2; slt += $3; sbh += $4; sbt += $5 }
            alh += $2; alt += $3; abh += $4; abt += $5
        }
        END {
            print "--------------------------------------------------------------------------------"
            printf "%-34s %6d %6d %8s %8d %8s\n", "liboriginccl (src/ only)", slt, slh, pct(slh, slt), sbt, pct(sbh, sbt)
            printf "%-34s %6d %6d %8s %8d %8s\n", "ALL (src/ + tests/)", alt, alh, pct(alh, alt), abt, pct(abh, abt)
            print ""
            print "LINES and BRANCH are gcov totals; HIT was derived from gcov percentages,"
            print "so a figure can be off by one line. TAKEN% is gcov \"taken at least once\":"
            print "every outcome of the branch observed."
            print "include/ headers are excluded - each is compiled into every TU that includes"
            print "it, so summing those copies would count the same line more than once."
        }
    ' "$rows" | tee "$report"

    rm -f "$rows"
    echo
    echo ">>> text report: $report"
}


if [[ $coverage -eq 1 ]]; then
    report_coverage || exit 3
fi

echo
if [[ $failed -eq 1 ]]; then
    echo "RESULT: FAILED"
    exit 1
fi
if [[ $skipped -eq 1 ]]; then
    if [[ $allow_skip -eq 1 ]]; then
        echo "RESULT: PASSED (with tiers skipped)"
        exit 0
    fi
    echo "RESULT: INCOMPLETE - some tiers were skipped; not a pass." >&2
    echo "        re-run with --allow-skip if this environment has no MPI." >&2
    exit 2
fi
echo "RESULT: PASSED (all tiers)"
