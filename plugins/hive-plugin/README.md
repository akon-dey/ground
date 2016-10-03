## Ground Hive Plugin

Ground Hive Plugin implements Hive's Metastore interface (org.apache.hadoop.hive.metastore.RawStore) using ground-core APIs. Our goal is to provide versioning support for Hive.

## Current Status

This is a work in progress. Implemented APIs are in GroundStore.java and an abstract class GroundMetastore contains all unimplemented ones
Following APIs are implemented

createDatabase(Database db)
getDatabase(String dbName)
dropDatabase(String dbName)
createTable(Table table)
getTable(String tableName)
dropTable(String tableName)
addPartitions(Partition partition)

getAllDatabases()
getAllTables(String dbName)

## Getting Started

One has to setup postgres to test or develop. Then from ground checkout directory (root) do

(cd ground-core/scripts/postgres && python2.7 postgres_setup.py test default) ; dropdb test; createdb test; (cd ground-core/scripts/postgres && python2.7 postgres_setup.py test test) ;mvn clean package -Dtest=TestGroundMetastore

## API Reference

Please look at Ground core API getting started guide and hive Metastore APIs at https://hive.apache.org/javadocs/r0.13.1/api/metastore/org/apache/hadoop/hive/metastore/RawStore.html

## Tests

Currently unit tests are available in TestGroundMetastore.java and requires a Postgres server.

