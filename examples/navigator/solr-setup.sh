#!/bin/bash

COLLECTION_NAME=nav-audit
NUM_SHARDS=3

BASE_DIR=$( readlink -f $( dirname $0 ) )
COLLECTION_DIR=$BASE_DIR/nav-solr

cnt=$( solrctl collection --list | grep "^$COLLECTION_NAME " | wc -l )
if [ "$cnt" -gt "0" ]; then
  echo "Collection $COLLECTION_NAME already exists."
  echo -n "Delete it? (y/N) "
  read deleteit

  if [ "$( echo "$deleteit" | tr "y" "Y")" != "Y" ]; then
    echo "Bye."
    exit
  fi

  echo "Deleting Solr collection $COLLECTION_NAME"
  solrctl collection --delete $COLLECTION_NAME
fi

rm -rf $COLLECTION_DIR
echo "Generating local Solr configuration directory"
solrctl instancedir --generate $COLLECTION_DIR
echo "Updating schema.xml file in the configuration"
cp $BASE_DIR/schema.xml $COLLECTION_DIR/conf/
echo "Uploading configuration to ZooKeeper"
solrctl instancedir --create $COLLECTION_NAME $COLLECTION_DIR
echo "Creating collection $COLLECTION_NAME in Solr"
solrctl collection --create $COLLECTION_NAME -s $NUM_SHARDS -c $COLLECTION_NAME
