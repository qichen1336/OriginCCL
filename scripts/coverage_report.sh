#!/usr/bin/env bash
# Generates the gcov report for a coverage build. Called by run_tests.sh; not a user entry
# point of its own.
#
# The instrumented ranks write into the build tree. libgcov locks the .gcda file, so several
# MPI processes writing the same counters is safe; what is needed is to drop stale counters
# before a run (run_tests.sh does that). gcovr is preferred, lcov+genhtml is the fallback, and
# plain gcov text is the last resort.
set -u -o pipefail

BUILD_ROOT="$1"
REPORT_DIR="$2"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Callers may pass relative paths; resolve them before the report tree is recreated.
case "${BUILD_ROOT}" in
/*) ;;
*) BUILD_ROOT="${PWD}/${BUILD_ROOT}" ;;
esac
case "${REPORT_DIR}" in
/*) ;;
*) REPORT_DIR="${PWD}/${REPORT_DIR}" ;;
esac
COVERAGE_DIR="${REPORT_DIR}/coverage"
RAW_DIR="${COVERAGE_DIR}/raw"

rm -rf "${COVERAGE_DIR}"
mkdir -p "${RAW_DIR}"

coverage_builds="$(find "${BUILD_ROOT}" -maxdepth 2 -name 'coverage' -type d 2>/dev/null)"
if [ -z "${coverage_builds}" ]; then
    echo "no coverage build found under ${BUILD_ROOT}" >&2
    exit 1
fi

found=0
for build in ${coverage_builds}; do
    executor="$(basename "$(dirname "${build}")")"
    gcda_count="$(find "${build}" -name '*.gcda' 2>/dev/null | wc -l | tr -d ' ')"
    echo "coverage: ${executor}: ${gcda_count} gcda files"
    if [ "${gcda_count}" = "0" ]; then
        continue
    fi
    found=1

    if command -v gcovr >/dev/null 2>&1; then
        if gcovr --root "${ROOT_DIR}" \
            --filter "${ROOT_DIR}/(src|include)/.*" \
            --gcov-ignore-parse-errors \
            --txt "${COVERAGE_DIR}/${executor}.txt" \
            --json-summary "${COVERAGE_DIR}/${executor}.json" \
            --cobertura "${COVERAGE_DIR}/${executor}.xml" \
            --html-details "${COVERAGE_DIR}/${executor}.html" \
            "${build}" >"${COVERAGE_DIR}/${executor}.gcovr.log" 2>&1; then
            echo "coverage: ${executor}: gcovr report written to ${COVERAGE_DIR}/${executor}.html"
            continue
        fi
        echo "coverage: ${executor}: gcovr failed, falling back to gcov text" >&2
    fi

    gcov_dir="${RAW_DIR}/${executor}"
    mkdir -p "${gcov_dir}"
    # gcov writes its .gcov next to where it runs, so it is run inside the report tree to keep
    # the repository clean. It is invoked once per .gcno: several notes files in one call make
    # same-named headers (like logger.h) overwrite each other and inflate the counts. Only the
    # repository's own sources are reported; the system and MPI headers are dropped, since a
    # bare "/include/" also matches /usr/include.
    (
        cd "${gcov_dir}" || exit 1
        find "${build}" -name '*.gcno' -print0 | while IFS= read -r -d '' notes; do
            gcov -b -c -p "${notes}" 2>/dev/null | grep -B1 "^Lines executed" | grep -A1 "^File '${ROOT_DIR}/\(src\|include\)/"
        done | grep -E "^File '|^Lines executed" >"${COVERAGE_DIR}/${executor}.txt"
    )
    echo "coverage: ${executor}: gcov per-file summary in ${COVERAGE_DIR}/${executor}.txt ($(wc -l <"${COVERAGE_DIR}/${executor}.txt" | tr -d ' ') lines)"
done

if [ "${found}" = "0" ]; then
    echo "no .gcda data was produced; was the profile built with -DOCCL_ENABLE_COVERAGE=ON?" >&2
    exit 1
fi

{
    echo "Coverage summary ($(date -u '+%Y-%m-%dT%H:%M:%SZ'))"
    echo "Library code is counted from src/ and include/; the tests are reported separately."
    echo "Each executor is reported on its own: the executor is chosen at compile time, so the"
    echo "four executor implementations cannot be merged into one percentage."
    echo
    for build in ${coverage_builds}; do
        executor="$(basename "$(dirname "${build}")")"
        echo "== ${executor} =="
        if [ -f "${COVERAGE_DIR}/${executor}.json" ]; then
            cat "${COVERAGE_DIR}/${executor}.json"
            echo
        fi
        if [ -f "${COVERAGE_DIR}/${executor}.txt" ]; then
            # Headers are instrumented once per translation unit that includes them, so they
            # repeat in the per-file listing; the unique-file view below collapses each file to
            # its best-covered translation unit instead of summing the duplicates.
            # gcov prints "File '/abs/path'" then "Lines executed:NN.NN% of M".
            awk '
                /^File / { file = substr($0, 7, length($0) - 7) }
                /^Lines executed:/ {
                    split($0, parts, " ")
                    percent = substr(parts[2], index(parts[2], ":") + 1) + 0
                    total = parts[4] + 0
                    if (!(file in best) || percent > best[file]) { best[file] = percent; lines[file] = total }
                }
                END {
                    printf "%-68s %8s %8s\n", "file", "lines", "cover"
                    count = 0
                    for (file in best) {
                        printf "%-68s %8d %7.1f%%\n", file, lines[file], best[file]
                        count++
                    }
                    printf "%d unique files\n", count
                }
            ' "${COVERAGE_DIR}/${executor}.txt"
        fi
        echo
    done
} >"${COVERAGE_DIR}/summary.txt"

echo "coverage: summary written to ${COVERAGE_DIR}/summary.txt"
exit 0
