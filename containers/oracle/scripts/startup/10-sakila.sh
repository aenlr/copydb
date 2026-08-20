#!/usr/bin/env bash

if [ -z "$SAKILA" ]; then
  (return 2>/dev/null) && SAKILA=false || SAKILA=true
fi

if [ "$SAKILA" != true ]; then
  # shellcheck disable=SC2317
  return 2>/dev/null || exit
fi

ORACLE_CONNECT_STRING="${ORACLE_CONNECT_STRING:-//${ORACLE_HOST:-localhost}:${ORACLE_PORT:-1521}/${ORACLE_PDB:-XEPDB1}}"
if echo exit | sqlplus -S -L -NOLOGINTIME "SAKILA/sakila@$ORACLE_CONNECT_STRING" >/dev/null 2>&1; then
  echo "SAKILA schema already created."
  echo "To remove: printf \"DROP USER SAKILA CASCADE;\nexit\" | sqlplus \"SYSTEM/\$ORACLE_PWD@$ORACLE_CONNECT_STRING\""
else
  if [ -z "$COPYDB_SQL_ROOT" ]; then
    # SCRIPTS_ROOT is set by /opt/oracle/runUserScripts.sh
    if [ -z "$SCRIPTS_ROOT" ]; then
      SCRIPTS_ROOT=$(dirname "$0")
    fi
    COPYDB_SQL_ROOT=$(readlink -f "$SCRIPTS_ROOT/../sql")
  fi

  SAKILA_DIR=${SAKILA_DIR:-/home/oracle/sakila}

  echo "Creating SAKILA schema."
  if ! (echo exit | sqlplus "SYS/$ORACLE_PWD@$ORACLE_CONNECT_STRING as sysdba" "@$COPYDB_SQL_ROOT/sakila.sql" "@$ORACLE_CONNECT_STRING" "$SAKILA_DIR"); then
    echo "SAKILA schema creation failed"
    # shellcheck disable=SC2317
    return 1 2>/dev/null || exit 1
  fi
fi
