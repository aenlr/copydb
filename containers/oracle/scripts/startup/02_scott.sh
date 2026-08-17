#!/usr/bin/env bash

ORACLE_CONNECT_STRING="${ORACLE_CONNECT_STRING:-//${ORACLE_HOST:-localhost}:${ORACLE_PORT:-1521}/${ORACLE_PDB:-XEPDB1}}"

if echo exit | sqlplus -S -L -NOLOGINTIME "SCOTT/tiger@$ORACLE_CONNECT_STRING" >/dev/null 2>&1; then
  if ! return 0 2>/dev/null; then
    echo "SCOTT schema already installed."
    echo "To remove: printf \"DROP USER SCOTT CASCADE;\nexit\" | sqlplus \"SYSTEM/\$ORACLE_PWD@$ORACLE_CONNECT_STRING\""
  fi
else
  if [ -z "$COPYDB_SQL_ROOT" ]; then
    # SCRIPTS_ROOT is set by /opt/oracle/runUserScripts.sh
    if [ -z "$SCRIPTS_ROOT" ]; then
      SCRIPTS_ROOT=$(dirname "$0")
    fi
    COPYDB_SQL_ROOT=$(readlink -f "$SCRIPTS_ROOT/../sql")
  fi
  ddl=
  for f in "$ORACLE_HOME/rdbms/admin/utlsampl.sql" \
               "$COPYDB_SQL_ROOT/utlsampl.sql" \
               "$COPYDB_SQL_ROOT/scott.sql" \
               /tmp/utlsampl.sql
  do
    if [ -e "$f" ]; then
      ddl="$f"
      break
    fi
  done
  if [ -z "$ddl"  ]; then
    ddl=/tmp/utlsampl.sql
    echo "SCOTT schema definition not found: utlsampl.sql. Downloading..."
    url=https://raw.githubusercontent.com/oracle-samples/oracle-db-examples/refs/heads/main/text/oracle-text-scripts/dbms_search/utlsampl.sql
    if ! curl -s --fail --show-error -L "$url" -o "$ddl"; then
      echo "$0: download of $url failed" >&2
      # shellcheck disable=SC2317
      return 1 2>/dev/null || exit 1
    fi
  fi

  echo "Creating SCOTT schema."
  # utlsampl.sql doesn't handle portable databases
  sed "s|^CONNECT SCOTT/tiger|CONNECT SCOTT/tiger@$ORACLE_CONNECT_STRING|" <"$ddl" >/tmp/scott.tmp.sql
  sqlplus "SYSTEM/$ORACLE_PWD@$ORACLE_CONNECT_STRING" "@/tmp/scott.tmp.sql"
fi
