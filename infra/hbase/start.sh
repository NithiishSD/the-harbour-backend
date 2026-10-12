#!/bin/sh
# Standalone HBase: one JVM runs master, region server and ZooKeeper. The REST gateway runs beside it.
set -e
hbase rest start -p 8080 > /opt/hbase/logs/rest.log 2>&1 &
exec hbase master start
