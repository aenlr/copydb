#!/usr/bin/env bash

ORACLE_CONNECT_STRING="${ORACLE_CONNECT_STRING:-//${ORACLE_HOST:-localhost}:${ORACLE_PORT:-1521}/${ORACLE_PDB:-XEPDB1}}"
if echo exit | sqlplus -S -L -NOLOGINTIME "ORATST/ORATST@$ORACLE_CONNECT_STRING" >/dev/null 2>&1; then
  if ! return 0 2>/dev/null; then
    echo "ORATST schema already installed."
    echo "To remove: printf \"DROP USER ORATST CASCADE;\nexit\" | sqlplus \"SYSTEM/\$ORACLE_PWD@$ORACLE_CONNECT_STRING\""
  fi
else
  if [ -z "$COPYDB_SQL_ROOT" ]; then
    # SCRIPTS_ROOT is set by /opt/oracle/runUserScripts.sh
    if [ -z "$SCRIPTS_ROOT" ]; then
      SCRIPTS_ROOT=$(dirname "$0")
    fi
    COPYDB_SQL_ROOT=$(readlink -f "$SCRIPTS_ROOT/../sql")
  fi
  echo "Creating ORATST schema."
  echo exit | sqlplus "SYS/$ORACLE_PWD@$ORACLE_CONNECT_STRING as sysdba" "@$COPYDB_SQL_ROOT/oratst.sql"
fi
