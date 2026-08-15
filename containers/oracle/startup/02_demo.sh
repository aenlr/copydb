#!/usr/bin/env bash

set -euo pipefail
cd /tmp
version="21.1"
tag="v$version.zip"
dir="$PWD/db-sample-schemas-$version"
archive="$dir.zip"
marker="$dir/.installed"

if [ -f "$marker" ]; then
  echo "Sample schemas already installed. Remove $marker to recreate."
  exit 0
fi

if [ ! -d "$dir" ]; then
  if [ ! -f "$archive" ]; then
    if ! curl --fail -L "https://github.com/oracle-samples/db-sample-schemas/archive/refs/tags/$tag" -o "$archive"; then
      echo "Failed to download sample schemas" >&2
      rm -f "$archive"
      exit 1
    fi
  fi

  unzip "$archive" || exit $?
  if ! sed -i "s|__SUB__CWD__|$dir|g" "$dir"/*.sql "$dir"/*/*.sql "$dir"/*/*.dat; then
    rm -r "$dir"
    exit 1
  fi
fi

cd "$dir"
echo "Creating Human Resources database (HR)"
"$ORACLE_HOME"/bin/sqlplus "system/$ORACLE_PWD@//localhost:1521/XEPDB1" \
              @human_resources/hr_main.sql \
              HR USERS TEMP "$ORACLE_PWD" /tmp/ "//localhost:1521/XEPDB1"

echo "Creating Customer Orders database (CO)"
"$ORACLE_HOME"/bin/sqlplus "system/$ORACLE_PWD@//localhost:1521/XEPDB1" \
              @customer_orders/co_main.sql \
              CO "//localhost:1521/XEPDB1" USERS TEMP

touch "$marker"

# "$ORACLE_HOME"/bin/sqlplus "system/$ORACLE_PWD@//localhost:1521/XEPDB1" \
  #               @mksample.sql "$ORACLE_PWD" "$ORACLE_PWD" \
  #               hr oe pm ix sh bi \
  #               USERS TEMP \
  #               /tmp/ //localhost:1521/XEPDB1
