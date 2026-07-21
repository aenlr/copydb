# Database copying backed by Liquibase

`copydb` is a tool for copying databases between different database vendors
that utilizes [Liquibase](https://www.liquibase.com/) for schema management.

Drivers are included for
- H2
- Oracle
- MariaDB
- MySQL
- PostgreSQL
- SQLite
- SQL Server

## Examples

```shell
# Copy between H2 databases, initializing target with liquibase.
copydb --changelog=changelog.xml --truncate \
  -u sa -p sa jdbc:h2:~/one \
  -u sa -p sa jdbc:h2:~/two
```

```shell
# Copy specified tables and sequences.
# Same credentials for source and target
copydb --truncate --tables footab,bartab --sequences fooseq,barseq \
  -u sa -p sa jdbc:h2:~/one jdbc:h2:~/two
```

```shell
# Copy between Oracle and H2 databases, initializing target with liquibase.
# Passwords are read from environment variables.
export COPYDB_SOURCE_PASSWORD=...
export COPYDB_TARGET_PASSWORD=...
copydb --changelog=changelog.xml --truncate \
  -u ORAUSER jdbc:oracle:thin:@DBNAME -u H2USER jdbc:h2:~/two
```

## Conversions

The following conversions are supported

* boolean to numeric (true is converted to and false to 0)
* numeric to boolean (0 is converted to false and non-zero to true)
* UUID to Oracle RAW(16)
* UUID to VARCHAR/CHAR/TEXT (canonical representation)
* BINARY(16)/RAW(16) to UUID
* VARCHAR/CHAR(36) to UUID (canonical text representation)
* VARCHAR/CHAR(32) to UUID (plain hex format)
* BLOB/OID/LO to BINARY/BYTEA
* BINARY/BYTEA to BLOB/OID/LO
* VARCHAR to BINARY/BLOB/BYTEA/OID using UTF-8 encoding
* VARCHAR/CHAR/TEXT/CLOB to BINARY/BLOB/BYTEA/OID using UTF-8 encoding
* VARCHAR/CHAR/CLOB/BINARY/BLOB to PostgreSQL JSON/JSONB
* PostgreSQL JSON/JSONB to VARCHAR/CLOB/BINARY/BLOB
