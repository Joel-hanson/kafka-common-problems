#!/usr/bin/env bash
# After hard reset: report every leader-epoch-checkpoint under the data dir.
set -euo pipefail

DATA_DIR="${DATA_DIR:-/var/lib/kafka/data}"
OUT="${OUT:-/tmp/lec-inspect-$(date -u +%Y%m%dT%H%M%SZ).txt}"

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "Missing ${DATA_DIR}" >&2
  exit 1
fi

bad=0
{
  echo "=== inspect $(date -u -Iseconds) data_dir=${DATA_DIR} ==="
  echo

  mapfile -t FILES < <(find "${DATA_DIR}" -name leader-epoch-checkpoint | sort)
  echo "Found ${#FILES[@]} leader-epoch-checkpoint file(s)"
  echo

  for f in "${FILES[@]}"; do
    echo "----- ${f} -----"
    if ! [[ -s "${f}" ]]; then
      echo "EMPTY or missing content"
      bad=$((bad + 1))
      echo
      continue
    fi
    wc -c "${f}"
    echo "--- raw ---"
    cat -A "${f}" || true
    echo "--- parse check ---"
    mapfile -t lines < <(grep -v '^$' "${f}" || true)
    if [[ ${#lines[@]} -lt 2 ]]; then
      echo "PARSE_FAIL: fewer than 2 lines"
      bad=$((bad + 1))
      echo
      continue
    fi
    ver="${lines[0]}"
    count="${lines[1]}"
    rest=$((${#lines[@]} - 2))
    echo "version=${ver} declared_count=${count} actual_entry_lines=${rest}"
    if ! [[ "${count}" =~ ^[0-9]+$ ]]; then
      echo "PARSE_FAIL: count is not an integer (possible torn write)"
      bad=$((bad + 1))
    elif [[ "${rest}" -ne "${count}" ]]; then
      echo "PARSE_FAIL: count mismatch (possible torn write)"
      bad=$((bad + 1))
    else
      ok=1
      for ((i = 2; i < ${#lines[@]}; i++)); do
        if ! [[ "${lines[$i]}" =~ ^[0-9]+[[:space:]]+[0-9]+$ ]]; then
          echo "PARSE_FAIL: bad entry line: ${lines[$i]}"
          ok=0
          bad=$((bad + 1))
          break
        fi
      done
      if [[ "${ok}" -eq 1 ]]; then
        echo "PARSE_OK"
      fi
    fi
    echo
  done

  echo "=== summary bad_files=${bad} ==="
} | tee "${OUT}"

# Recompute exit status from the summary line (pipe subshell)
if grep -q 'PARSE_FAIL\|EMPTY or missing' "${OUT}"; then
  echo "Wrote ${OUT} (failures detected)"
  exit 2
fi
echo "Wrote ${OUT}"
exit 0
