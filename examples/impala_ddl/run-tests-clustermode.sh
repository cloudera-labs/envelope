#!/usr/bin/env bash

ENV_JAR=$1
IMPALA_HOST=$2

if [[ -z ${IMPALA_HOST} || -z ${ENV_JAR} ]]; then
  echo "Usage: setup-tests.sh <env_jar> <impala_daemon>" >&2
  exit 1
fi

echo "Running Unpartitioned Example"
impala-shell -i ${IMPALA_HOST} -q "select * from example_output"
spark2-submit --files env.conf,simple-impala-unpart.conf --keytab user.kt --principal ian --deploy-mode client ${ENV_JAR} simple-impala-unpart.conf
impala-shell -i ${IMPALA_HOST} -q "select * from example_output"

echo "Running Partitioned Example"
impala-shell -i ${IMPALA_HOST} -q "show partitions example_output_part"
impala-shell -i ${IMPALA_HOST} -q "select * from example_output_part"
spark2-submit --files user.kt,env.conf,simple-impala-part.conf --deploy-mode cluster ${ENV_JAR} simple-impala-part.conf
impala-shell -i ${IMPALA_HOST} -q "show partitions example_output_part"
impala-shell -i ${IMPALA_HOST} -q "select * from example_output_part"

echo "Running Kudu Example"
impala-shell -i ${IMPALA_HOST} -q "show partitions example_output_kudu"
impala-shell -i ${IMPALA_HOST} -q "select * from example_output_kudu"
spark2-submit --files env.conf,simple-impala-kudu.conf --keytab user.kt --principal ian --deploy-mode cluster ${ENV_JAR} simple-impala-kudu.conf
impala-shell -i ${IMPALA_HOST} -q "show partitions example_output_kudu"
impala-shell -i ${IMPALA_HOST} -q "select * from example_output_kudu"
