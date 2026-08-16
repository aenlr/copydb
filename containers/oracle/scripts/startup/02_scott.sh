#!/usr/bin/env bash

ORACLE_CONNECT_STRING="${ORACLE_CONNECT_STRING:-//${ORACLE_HOST:-localhost}:${ORACLE_PORT:-1521}/${ORACLE_PDB:-XEPDB1}}"

if echo exit | sqlplus -S -L -NOLOGINTIME "SCOTT/tiger@$ORACLE_CONNECT_STRING" >/dev/null 2>&1; then
  if ! return 0 2>/dev/null; then
    echo "SCOTT schema already installed."
    echo "To remove: printf \"DROP USER SCOTT CASCADE;\nexit\" | sqlplus \"SYSTEM/\$ORACLE_PWD@$ORACLE_CONNECT_STRING\""
  fi
else
  ddl="$ORACLE_HOME/rdbms/admin/utlsampl.sql"
  if [ ! -e "$ddl"  ]; then
    ddl=/tmp/utlsampl.sql
    if [ ! -e "$ddl" ]; then
      echo "SCOTT schema definition not found: utlsampl.sql. Downloading..."
      url=https://raw.githubusercontent.com/oracle-samples/oracle-db-examples/refs/heads/main/text/oracle-text-scripts/dbms_search/utlsampl.sql
      if ! curl -s --fail --show-error -L "$url" -o "$ddl"; then
        echo "$0: download of $url failed" >&2
        [ "$0" = "${BASH_SOURCE[0]}" ] && return 1 || exit 1
      fi
    fi
  fi

  echo "Creating SCOTT schema."
  # utlsampl.sql doesn't handle portable databases
  sed "s|^CONNECT SCOTT/tiger|CONNECT SCOTT/tiger@$ORACLE_CONNECT_STRING|" <"$ddl" >/tmp/scott.tmp.sql
  sqlplus "SYSTEM/$ORACLE_PWD@$ORACLE_CONNECT_STRING" "@/tmp/scott.tmp.sql"
fi
