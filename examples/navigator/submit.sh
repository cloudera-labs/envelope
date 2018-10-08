#!/bin/bash

# Specify Kerberos credentials
KERBEROS_KEYTAB=KEYTABFILEPATH
KERBEROS_PRINCIPAL=KERBEROSPRINCIPALNAME
JKS_TRUSTSTORE=/opt/cloudera/security/jks/truststore.jks
# Uncomment the below if necessary to use a non-default Java Home
#export JAVA_HOME=/usr/java/jdk1.8.0_171-amd64
#export PATH=$JAVA_HOME/bin:$PATH

BASE_DIR=$( readlink -f $( dirname $0 ) )
FILES=$BASE_DIR/jaas.conf#jaas.conf,$BASE_DIR/nav-morphline.conf#nav-morphline.conf,$JKS_TRUSTSTORE#truststore.jks

export SPARK_KAFKA_VERSION=0.10

spark2-submit \
  --keytab $KERBEROS_KEYTAB \
  --principal $KERBEROS_PRINCIPAL \
  --files $FILES,<(cat $KERBEROS_KEYTAB)#$(basename $KERBEROS_KEYTAB) \
  --driver-java-options "-Djava.security.auth.login.config=jaas.conf -Djavax.net.ssl.trustStore=truststore.jks" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Djavax.net.ssl.trustStore=truststore.jks" \
  $BASE_DIR/../../build/envelope/target/envelope-*.jar $BASE_DIR/nav-audit.hcon
