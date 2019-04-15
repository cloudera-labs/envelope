#!/bin/bash

# Edit the variables below for your environment

# The security level of Impala (possible values are kerberos, ldap or none)
export IMPALA_SECURITY=kerberos
# If using LDAP you must supply the following values for user and pass
export IMPALA_USERNAME=REPLACEME
export IMPALA_PASSWORD=REPLACEME
# The FQDN of an Impala daemon or Impala LB
export IMPALA_HOST=REPLACEME
# The port on which the Impala daemon or LB listens for JDBC connections
export IMPALA_PORT=21050
# Whether SSL is set or not (true or false)
export IMPALA_SSL=false
# The JKS file with the CA certificate
export SSL_TRUSTSTORE_JKS=REPLACEME
# The password for the JKS CA certificate
export SSL_TRUSTSTORE_JKS_PASSWORD=REPLACEME
# The Kerberos principal to use
export IMPALA_KRB_PRINC=REPLACEME
# The Kerberos keytab with the above principal
export IMPALA_KRB_KEYTAB=REPLACEME
# The Kudu master nodes
export KUDU_MASTERS=REPLACEME

### DO NOT EDIT BELOW

IMPALA_SHELL_EXTRA_ARGS=""
if [[ ${IMPALA_SECURITY} == "kerberos" ]]; then
  IMPALA_SHELL_EXTRA_ARGS="${IMPALA_SHELL_EXTRA_ARGS} -k"
elif [[ ${IMPALA_SECURITY} == "ldap" ]]; then
  IMPALA_SHELL_EXTRA_ARGS="${IMPALA_SHELL_EXTRA_ARGS} -l -u ${IMPALA_USERNAME} --ldap_password_cmd='echo -n ${IMPALA_PASSWORD}'"
fi
if [[ ${IMPALA_SSL} == "true" ]]; then
  IMPALA_SHELL_EXTRA_ARGS="${IMPALA_SHELL_EXTRA_ARGS} --ssl"
elif [[ ${IMPALA_SECURITY} == "ldap" ]]; then
  IMPALA_SHELL_EXTRA_ARGS="${IMPALA_SHELL_EXTRA_ARGS} --auth_creds_ok_in_clear"
fi
export IMPALA_SHELL_EXTRA_ARGS

SPARK_CMD="spark-submit"
if which spark2-submit &>/dev/null; then
  SPARK_CMD="spark2-submit"
fi

# Write out env.conf file

cat >env.conf <<EOF
env {
EOF
# Write out Impala configuration
cat >>env.conf <<EOF
  impala {
    host = "${IMPALA_HOST}"
    port = ${IMPALA_PORT}
EOF
if [[ ${IMPALA_SECURITY} == "kerberos" ]]; then
cat >>env.conf <<EOF
    auth = kerberos
    krb {
      keytab = "${IMPALA_KRB_KEYTAB}"
      user-principal = "${IMPALA_KRB_PRINC}"
      debug = true
    }
EOF
elif [[ ${IMPALA_SECURITY} == "ldap" ]]; then
cat >>env.conf <<EOF
    auth = ldap
    username = ${IMPALA_USERNAME}
    password = ${IMPALA_PASSWORD}
EOF
fi
if [[ ${IMPALA_SSL} == "true" ]]; then
cat >>env.conf <<EOF
    ssl = true
    ssl-truststore = ${SSL_TRUSTSTORE_JKS}
    ssl-truststore-password = ${SSL_TRUSTSTORE_JKS_PASSWORD}
EOF
fi
cat >>env.conf <<EOF
  }
EOF

# Write out Kudu configuration
cat >>env.conf <<EOF
  kudu {
    connection = "${KUDU_MASTERS}"
  }
EOF

# Write footer
cat >>env.conf <<EOF
}
EOF
