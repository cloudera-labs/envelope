#!/bin/bash

# Uncomment the below if necessary to use a non-default Java Home
#export JAVA_HOME=/usr/java/jdk1.8.0_171-amd64
#export PATH=$JAVA_HOME/bin:$PATH
BASE_DIR=$( readlink -f $( dirname $0 ) )
FILES=$BASE_DIR/nav-morphline.conf#nav-morphline.conf

spark2-submit \
  --files $FILES \
  $BASE_DIR/../../build/envelope/target/envelope-*.jar $BASE_DIR/nav-audit.hcon
