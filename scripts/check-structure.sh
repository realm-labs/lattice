#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
allowlist="$root/scripts/pub-use-allowlist.txt"
status=0

check_file() {
  file=$1
  relative=${file#"$root"/}
  lines=$(awk 'END { print NR }' "$file")
  if [ "$lines" -gt 1200 ]; then
    echo "$relative: $lines lines exceeds the 1200 line limit" >&2
    status=1
  fi

  if ! awk -v path="$relative" '
    /allow[[:space:]]*\([^]]*clippy::too_many_arguments/ {
      print path ":" NR ": bare allow for clippy::too_many_arguments is forbidden" > "/dev/stderr"
      failed = 1
    }
    /expect[[:space:]]*\([^]]*clippy::too_many_arguments/ &&
      $0 !~ /reason[[:space:]]*=[[:space:]]*"[^"]*[^"[:space:]][^"]*"/ {
      print path ":" NR ": clippy::too_many_arguments expectation requires a non-empty same-line reason" > "/dev/stderr"
      failed = 1
    }
    END { exit failed ? 1 : 0 }
  ' "$file"; then
    echo failed >"$failure_marker"
  fi

  awk '/^[[:space:]]*pub(\([^)]*\))?[[:space:]]+use[[:space:]]/ {
      print NR "\t" $0
    }' "$file" |
    while IFS="$(printf '\t')" read -r line declaration; do
      if ! awk -F "$(printf '\t')" \
        -v path="$relative" \
        -v declaration="$declaration" \
        '$1 == path && $2 == declaration && length($3) > 0 { found = 1 }
         END { exit found ? 0 : 1 }' "$allowlist"; then
        echo "$relative:$line: unapproved public re-export: $declaration" >&2
        echo failed >"$failure_marker"
      fi
    done
}

failure_marker=${TMPDIR:-/tmp}/lattice-structure-check-$$
trap 'rm -f "$failure_marker"' EXIT HUP INT TERM

find "$root" \
  \( -path "$root/target" -o -path "$root/.git" \) -prune -o \
  -type f -name '*.rs' -print |
while IFS= read -r file; do
  check_file "$file"
  if [ "$status" -ne 0 ]; then
    echo failed >"$failure_marker"
  fi
done

if [ -f "$failure_marker" ]; then
  exit 1
fi

echo "structure check passed"
