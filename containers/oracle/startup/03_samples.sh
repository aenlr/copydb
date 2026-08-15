#!/usr/bin/env bash

if [ "$CREATE_SAMPLE_SCHEMAS" != Y ]; then
  return
fi

marker="$HOME/.samples"

version="21.1"
tag="v$version.zip"
dir="/tmp/db-sample-schemas-$version"
archive="$dir.zip"

if [ -f "$marker" ]; then
  echo "Sample schemas already installed. Remove $marker to recreate."
  return
fi

if [ ! -d "$dir" ]; then
  if [ ! -f "$archive" ]; then
    if ! curl --fail -L "https://github.com/oracle-samples/db-sample-schemas/archive/refs/tags/$tag" -o "$archive"; then
      echo "Failed to download sample schemas" >&2
      rm -f "$archive"
      return 1
    fi
  fi

  unzip -d /tmp "$archive" "db-sample-schemas-$version"/{human_resources,customer_orders}"/*" || return $?
  if ! sed -i "s|__SUB__CWD__|$dir|g" "$dir"/*/*.sql; then
    rm -r "$dir"
    return 1
  fi
fi

pushd "$dir" || return $?
echo "Creating Human Resources database (HR)"
"$ORACLE_HOME"/bin/sqlplus "SYSTEM/$ORACLE_PWD@//localhost:1521/XEPDB1" \
              @human_resources/hr_main.sql \
              HR USERS TEMP "$ORACLE_PWD" /tmp/ "//localhost:1521/XEPDB1" </dev/null

echo "Creating Customer Orders database (CO)"
"$ORACLE_HOME"/bin/sqlplus "SYSTEM/$ORACLE_PWD@//localhost:1521/XEPDB1" \
              @customer_orders/co_main.sql \
              CO "//localhost:1521/XEPDB1" USERS TEMP </dev/null
popd || return $?

touch "$marker"
