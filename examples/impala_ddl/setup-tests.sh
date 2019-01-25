#!/bin/bash

UP_HDFS_DIR='/tmp/example-output-unpartitioned'
P_HDFS_DIR='/tmp/example-output-partitioned'
INPUT_DIR='/tmp/example-input'

IMPALA_HOST=$1

if [[ -z ${IMPALA_HOST} ]]; then
  echo "Usage: setup-tests.sh <impala_daemon>" >&2
  exit 1
fi

echo "Cleaning HDFS directories"
hdfs dfs -rm -r -skipTrash ${UP_HDFS_DIR}
hdfs dfs -rm -r -skipTrash ${P_HDFS_DIR}
hdfs dfs -rm -r -skipTrash "${INPUT_DIR}"

echo "Creating tables"
impala-shell -i ${IMPALA_HOST} -f create_tables.sql

echo "Uploading HDFS data"
hdfs dfs -mkdir ${INPUT_DIR}
hdfs dfs -put -f example-input* ${INPUT_DIR}/
