#!/usr/bin/env bash

if [ "${CREATE_SCOTT_SCHEMA:-Y}" != Y ]; then
  return
fi

marker="$HOME/.scott"
if [ -f "$marker" ]; then
  echo "SCOTT schema already installed. Remove $marker to recreate."
  return
fi

echo "Creating SCOTT schema."
sed 's|^CONNECT SCOTT/tiger|CONNECT SCOTT/tiger@//localhost:1521/XEPDB1|' \
    <"$ORACLE_HOME/rdbms/admin/utlsampl.sql" >/tmp/scott.tmp.sql
"$ORACLE_HOME"/bin/sqlplus "SYSTEM/$ORACLE_PWD@//localhost:1521/XEPDB1" "@/tmp/scott.tmp.sql"

touch "$marker"
