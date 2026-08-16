#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(readlink -f "$(dirname "$0")")
ORACLE_CONNECT_STRING="${ORACLE_CONNECT_STRING:-//${ORACLE_HOST:-localhost}:${ORACLE_PORT:-1521}/${ORACLE_PDB:-XEPDB1}}"
declare -a samples=("$@")

if [ -z "${SAMPLE_SCHEMAS_DIR-}" ]; then
  if [ -w "$SCRIPT_DIR" ]; then
    SAMPLE_SCHEMAS_DIR="$SCRIPT_DIR"
  else
    SAMPLE_SCHEMAS_DIR="/tmp"
  fi
fi

version="21.1"
tag="v$version.zip"
dir="$SAMPLE_SCHEMAS_DIR/db-sample-schemas-$version"
archive="$dir.zip"

if [ ! -d "$dir" ]; then
  if [ ! -f "$archive" ]; then
    echo "Downloading $(basename "$archive") to $SAMPLE_SCHEMAS_DIR..."
    if ! curl -s --fail --show-error -L "https://github.com/oracle-samples/db-sample-schemas/archive/refs/tags/$tag" -o "$archive"; then
      echo "Failed to download sample schemas" >&2
      rm -f "$archive"
      exit 1
    fi
  else
    echo "Samples already downloaded to $archive"
  fi

  echo "Unpacking to $dir..."
  unzip -qd "$SAMPLE_SCHEMAS_DIR" "$archive"
  echo "Patching paths..."
  if ! sed -i "s|__SUB__CWD__|$dir|g" "$dir"/*.sql "$dir"/*/*.sql "$dir"/*/*.dat; then
    rm -r "$dir"
    exit 1
  fi
else
  echo "Samples already unpacked in $dir"
fi

if [ "${#samples[@]}" -eq 0 ]; then
  samples=(human_resources customer_orders)
fi

cd "$dir"
for s in "${samples[@]}"; do
  case "$s" in
    hr | HR | human[_-]resources)
      echo "Creating Human Resources database (HR)"
      sqlplus "SYSTEM/$ORACLE_PWD@$ORACLE_CONNECT_STRING" \
              @human_resources/hr_main.sql \
              HR USERS TEMP "$ORACLE_PWD" "$SAMPLE_SCHEMAS_DIR/" "$ORACLE_CONNECT_STRING" </dev/null
      ;;
    co | CO | customer[_-]orders)
      echo "Creating Customer Orders database (CO)"
      sqlplus "SYSTEM/$ORACLE_PWD@$ORACLE_CONNECT_STRING" \
              @customer_orders/co_main.sql \
              CO "$ORACLE_CONNECT_STRING" USERS TEMP </dev/null
      ;;
    all)
      echo "Creating all sample databases"
      sqlplus "SYSTEM/$ORACLE_PWD@$ORACLE_CONNECT_STRING" \
              @mksample.sql "$ORACLE_PWD" "$ORACLE_PWD" HR OE PM IX SH BI USERS TEMP "$SAMPLE_SCHEMAS_DIR/" "$ORACLE_CONNECT_STRING" </dev/null
      ;;
    *)
      echo "$0: unsupported sample database: $s" >&2
      exit 1
  esac
done
