#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

now_iso() {
  python3 - <<'PY'
import datetime
print(datetime.datetime.now(datetime.timezone.utc).isoformat())
PY
}

start_ns() {
  python3 - <<'PY'
import time
print(time.time_ns())
PY
}

ms_from_ns() {
  python3 - "$1" "$2" <<'PY'
import sys
start_ns = int(sys.argv[1])
end_ns = int(sys.argv[2])
print(f"{(end_ns - start_ns) / 1_000_000:.3f}")
PY
}

run_start_iso="$(now_iso)"
suite_run_id="$run_start_iso"
suite_start_ns="$(start_ns)"

tests=()

collect_tests_from_list() {
  local list_output="$1"
  local prefix="$2"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    case "$line" in
      *": "*)
        local name="${line%%:*}"
        local kind="${line#*: }"
        if [[ "$kind" == "test" ]]; then
          tests+=("${prefix}${name}")
        fi
        ;;
    esac
  done <<<"$list_output"
}

# Collect lib tests.
lib_list_output="$(cargo test --lib -- --list)"
lib_list_status=$?
if [[ $lib_list_status -ne 0 ]]; then
  echo "cargo test --lib --list reported failures" >&2
  exit $lib_list_status
fi
collect_tests_from_list "$lib_list_output" "lib::"

# Collect integration tests per test binary to avoid lib tests noise.
for test_file in tests/*.rs; do
  test_bin="$(basename "$test_file" .rs)"
  if [[ "$test_bin" == "perf_utils" ]]; then
    continue
  fi
  test_list_output="$(cargo test --test "$test_bin" -- --list)"
  list_status=$?
  if [[ $list_status -ne 0 ]]; then
    echo "cargo test --test $test_bin --list reported failures" >&2
    exit $list_status
  fi
  collect_tests_from_list "$test_list_output" "test::$test_bin::"
done

mkdir -p tests/perf
csv_path="tests/perf/test_times.csv"
if [[ ! -f "$csv_path" || ! -s "$csv_path" ]]; then
  printf '%s\n' "suite_run_id,run_start_iso,run_end_iso,test_name,status,duration_ms" >> "$csv_path"
fi

for test_full_name in "${tests[@]}"; do
  if [[ "$test_full_name" == *"perf_suite_runner"* ]]; then
    continue
  fi
  test_start_ns="$(start_ns)"
  case "$test_full_name" in
    lib::*)
      test_name="${test_full_name#lib::}"
      cargo test --lib "$test_name" -- --exact --quiet
      ;;
    test::*)
      # Split "test::<bin>::<name>" into binary + test name.
      trimmed="${test_full_name#test::}"
      test_bin="${trimmed%%::*}"
      test_name="${trimmed#*::}"
      cargo test --test "$test_bin" "$test_name" -- --exact --quiet
      ;;
    *)
      cargo test "$test_full_name" -- --exact --quiet
      ;;
  esac
  run_status=$?
  test_end_iso="$(now_iso)"
  test_end_ns="$(start_ns)"
  duration_ms="$(ms_from_ns "$test_start_ns" "$test_end_ns")"
  if [[ $run_status -eq 0 ]]; then
    status="ok"
  else
    status="failed"
  fi
  printf '%s,%s,%s,%s,%s,%s\n' "$suite_run_id" "$run_start_iso" "$test_end_iso" "$test_name" "$status" "$duration_ms" >> "$csv_path"
  if [[ $run_status -ne 0 ]]; then
    echo "Test failed: $test_name" >&2
  fi
done

suite_end_iso="$(now_iso)"
suite_end_ns="$(start_ns)"
suite_ms="$(ms_from_ns "$suite_start_ns" "$suite_end_ns")"
printf '%s,%s,%s,%s,%s,%s\n' "$suite_run_id" "$run_start_iso" "$suite_end_iso" "__suite_total__" "ok" "$suite_ms" >> "$csv_path"
