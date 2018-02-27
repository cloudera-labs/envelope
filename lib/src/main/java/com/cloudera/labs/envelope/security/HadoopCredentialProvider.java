/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.security;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.typesafe.config.Config;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;

/**
 *
 * @author vijaykumarsingh
 */
public class HadoopCredentialProvider implements CredentialProvider, ProvidesAlias {

    public static final String HADOOP_CREDENTIAL_PROVIDER_TYPE = "hadoop";
    public static final String HADOOP_CREDENTIAL_PROVIDER_ALIAS = "alias";
    public static final String HADOOP_CREDENTIAL_PROVIDER_PATH = "provider";
    public static final String HADOOP_CREDENTIAL_PROVIDER_SECRET_PATH = "provider-secret-path";
    public static final String HADOOP_CREDENTIAL_PROVIDER_CONF_RESOURCE = "core-site.xml";

    private Config config;
    private String alias;
    private String provider;

    @Override
    public void configure(Config config) {
        this.config = config;
        if (!config.hasPath(HADOOP_CREDENTIAL_PROVIDER_ALIAS)) {
            throw new RuntimeException(HADOOP_CREDENTIAL_PROVIDER_TYPE
                + " requires '" + HADOOP_CREDENTIAL_PROVIDER_ALIAS + "' property");
        } else {
            alias = config.getString(HADOOP_CREDENTIAL_PROVIDER_ALIAS);
            if (alias.trim().isEmpty()) {
                throw new RuntimeException(HADOOP_CREDENTIAL_PROVIDER_ALIAS
                    + "cannot be whitespace.");
            }
        }
        if (!config.hasPath(HADOOP_CREDENTIAL_PROVIDER_PATH)) {
            LOG.debug(HADOOP_CREDENTIAL_PROVIDER_ALIAS + " is +'" + provider + "'");
            throw new RuntimeException(HADOOP_CREDENTIAL_PROVIDER_TYPE + " "
                + "requires '" + HADOOP_CREDENTIAL_PROVIDER_PATH + "' property");
        } else {
            provider = config.getString(HADOOP_CREDENTIAL_PROVIDER_PATH);
            if (provider.trim().isEmpty()) {
                LOG.debug(HADOOP_CREDENTIAL_PROVIDER_PATH + " is +'" + provider + "'");
                throw new RuntimeException(HADOOP_CREDENTIAL_PROVIDER_PATH
                    + "cannot be whitespace.");
            }
        }
    }

    /**
     *
     * @return @throws Exception
     */
    @Override
    public String getPassword() throws Exception {
        String password = null;
        Configuration coreConfiguration = new Configuration(false);
        coreConfiguration.addResource(HADOOP_CREDENTIAL_PROVIDER_CONF_RESOURCE);
        coreConfiguration.set(
            org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            provider);

        if (config.hasPath(HADOOP_CREDENTIAL_PROVIDER_SECRET_PATH)) {
            String passwordfile = config.getString(
                HADOOP_CREDENTIAL_PROVIDER_SECRET_PATH);
            if (null != passwordfile && (!passwordfile.trim().isEmpty())) {
                coreConfiguration.set(
                    AbstractJavaKeyStoreProvider.CREDENTIAL_PASSWORD_FILE_KEY,
                    passwordfile);
                 ClassLoader cl = Thread.currentThread().getContextClassLoader();
            }

        }

        List<org.apache.hadoop.security.alias.CredentialProvider> providers
            = org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(
                coreConfiguration);

        if (providers.isEmpty()) {
            throw new RuntimeException("Provider did not match configured "
                + "implementation");
        } else {
            for (org.apache.hadoop.security.alias.CredentialProvider item : providers) {

                org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry credentialEntry = item.getCredentialEntry(alias);
                if (credentialEntry != null) {
                    password = new String(credentialEntry.getCredential());
                    break;
                }
            }
        }
        return password;
    }

    @Override
    public String getAlias() {
        return HADOOP_CREDENTIAL_PROVIDER_TYPE;
    }

}
