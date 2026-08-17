#!/usr/bin/env bash

# Determine which database we are using
ORACLE_SID=${ORACLE_SID:?}
ORACLE_PDB=${ORACLE_PDB:-${ORACLE_SID}PDB1}

# Locate the oracle/scripts/sql directory
if [ -z "$COPYDB_SQL_ROOT" ]; then
  # SCRIPTS_ROOT is set by /opt/oracle/runUserScripts.sh
  if [ -z "$SCRIPTS_ROOT" ]; then
    SCRIPTS_ROOT=$(dirname "$0")
  fi
  COPYDB_SQL_ROOT=$(readlink -f "$SCRIPTS_ROOT/../sql")
fi

# Ensure we have a USERS tablespace with ASSM enabled.
users_datafile="${ORACLE_BASE:?}/oradata/$ORACLE_SID/$ORACLE_PDB/users01.dbf"
if [ ! -e "$users_datafile"  ]; then
  echo "Creating USERS tablespace."
  echo exit | sqlplus "SYS/${ORACLE_PWD:?}@//localhost:1521/$ORACLE_PDB as sysdba" "@$COPYDB_SQL_ROOT/usrtbspc.sql" "$users_datafile" || exit $?
fi
