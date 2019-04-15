#!/bin/bash

UP_HDFS_DIR='/tmp/example-output-unpartitioned'
P_HDFS_DIR='/tmp/example-output-partitioned'
INPUT_DIR='/tmp/example-input'

source env.sh

echo "Cleaning HDFS directories"
hdfs dfs -rm -r -skipTrash ${UP_HDFS_DIR}
hdfs dfs -rm -r -skipTrash ${P_HDFS_DIR}
hdfs dfs -rm -r -skipTrash "${INPUT_DIR}"

echo "Creating HDFS directories"
hdfs dfs -mkdir ${UP_HDFS_DIR}
hdfs dfs -mkdir ${P_HDFS_DIR}
hdfs dfs -mkdir "${INPUT_DIR}"

echo "Creating tables"
impala-shell -i ${IMPALA_HOST} ${IMPALA_SHELL_EXTRA_ARGS} -f create_tables.sql

echo "Uploading HDFS data"
hdfs dfs -put -f example-input* ${INPUT_DIR}/
