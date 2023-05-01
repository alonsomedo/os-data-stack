#!/bin/bash
set -e
# Configuration required for lineage in openmetadata
echo "shared_preload_libraries = 'pg_stat_statements'" >> $PGDATA/postgresql.conf
echo "pg_stat_statements.max = 10000" >> $PGDATA/postgresql.conf
echo "pg_stat_statements.track = all" >> $PGDATA/postgresql.conf

psql -v ON_ERROR_STOP=1 --username "dwh" --dbname "dwh" <<-EOSQL
  CREATE SCHEMA AIRBYTE;
  CREATE SCHEMA BRONZE;
  CREATE SCHEMA SILVER;
  CREATE SCHEMA GOLD;
  CREATE EXTENSION pg_stat_statements;
EOSQL