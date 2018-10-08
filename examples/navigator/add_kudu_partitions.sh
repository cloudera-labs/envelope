#!/bin/bash

DAYS=${1:-7}
START_DAY=${2:-$(date +%Y-%m-%d)}

DB_NAME=nav
TABLE_NAMES="hbase_events hdfs_events hive_events hue_events impala_events navms_events sentry_events solr_events"

for table in $TABLE_NAMES; do
  cnt=0
  while [ $cnt -lt $DAYS ]; do
    day=$(date --date="$START_DAY + $cnt days" +%Y-%m-%d)
    nextday=$(date --date="$START_DAY + $((cnt+1)) days" +%Y-%m-%d)
    echo "ALTER TABLE $DB_NAME.$table ADD IF NOT EXISTS RANGE PARTITION '$day' <= VALUES < '$nextday';"
    cnt=$((cnt+1))
  done
done

