#!/usr/bin/env bash

ENV_JAR=$1

if [[ -z ${ENV_JAR} ]]; then
  echo "Usage: setup-tests.sh <env_jar>" >&2
  exit 1
fi

source env.sh

echo "Running Unpartitioned Example"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output"
${SPARK_CMD} ${SPARK_EXTRA_ARGS} --files env.conf,simple-impala-unpart.conf --deploy-mode cluster ${ENV_JAR} simple-impala-unpart.conf
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output"

echo "Running Partitioned Example"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "show partitions example_output_part"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output_part"
${SPARK_CMD} ${SPARK_EXTRA_ARGS} --files env.conf,simple-impala-part.conf --deploy-mode cluster ${ENV_JAR} simple-impala-part.conf
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "show partitions example_output_part"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output_part"

echo "Running Kudu Example"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "show partitions example_output_kudu"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output_kudu"
${SPARK_CMD} ${SPARK_EXTRA_ARGS} --files env.conf,simple-impala-kudu.conf --deploy-mode cluster ${ENV_JAR} simple-impala-kudu.conf
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "show partitions example_output_kudu"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -q "select * from example_output_kudu"
