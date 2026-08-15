#!/usr/bin/env bash

if [ "${CREATE_SCOTT_SCHEMA:-Y}" != Y ]; then
  exit 0
fi

set -euo pipefail
marker="$PWD/.scott"

if [ -f "$marker" ]; then
  echo "SCOTT schema already installed. Remove $marker to recreate."
  exit 0
fi

echo "Creating SCOTT schema."
sed 's|^CONNECT SCOTT/tiger|CONNECT SCOTT/tiger@//localhost:1521/XEPDB1|' \
    <"$ORACLE_HOME/rdbms/admin/utlsampl.sql" >scott.tmp.sql
"$ORACLE_HOME"/bin/sqlplus "SYSTEM/$ORACLE_PWD@//localhost:1521/XEPDB1" "@scott.tmp.sql"

touch "$marker"
