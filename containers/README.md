# Test containers

Docker compose project to start various database services.

To conveniently choose which services to start by default
you may create a `docker-compose.override.yml` file:
```yaml
services:
  h2:
    profiles: [disabled] # disable
  ora21c:
    ports: !override
      - 2521:1521 # override host port
    profiles: !override [] # enable
```

## H2

Starts an H2 instance allowing creation of new databases in the home
directory.  The console is reachable on http://localhost:9093 and the
H2 server on port 9092.

## Oracle

All oracle instances creates two simple schemas by default
* ORATST/ORATST - an empty schema
* SCOTT/tiger - https://www.orafaq.com/wiki/SCOTT


To allow exposing multiple Oracle versions on localhost the ports are
mapped according to version:
- ora18c: 1518
- ora21c: 1521
- ora26ai: 1526

For example, to connect to the Oracle 26ai instance from outside the container:
```shell
sqlplus SCOTT/tiger@//localhost:1526/FREEPDB1
```

Use `docker-compose.override.yml` to override port mappings.

### Oracle 18c/21c

The `ora18c` and `ora21c` services are Oracle Database XE Release versions.

[Oracle Database Sample Schemas](https://github.com/oracle-samples/db-sample-schemas)
can be installed by running 
```shell
docker compose exec -u oracle ora21c /home/oracle/samples/install-sample-schemas.sh human_resources
```

Changing the argument to `all' will install all sample schemas (NOTE:
not all schemas have been tested if they work with this setup).

### Oracle 26ai (23ai)

The `ora26ai` service is an Oracle AI Database 26ai Free instance.
