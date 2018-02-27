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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.labs.envelope.security;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.cloudera.labs.envelope.output.Output;
import com.typesafe.config.Config;

/**
 *
 * @author vijaykumarsingh
 */
public class CredentialProviderFactory extends LoadableFactory<Output> {

    public static CredentialProvider create(Config config) {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("CredentialProvider type not specified");
        }
        Config credentialProviderConfig = extractCredentitalProviderConfig(config);
        String credentialProviderType = credentialProviderConfig.getString(TYPE_CONFIG_NAME);
        CredentialProvider credentialProvider = null;
        try {
            credentialProvider = loadImplementation(
                CredentialProvider.class, credentialProviderType);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        credentialProvider.configure(credentialProviderConfig);

        return credentialProvider;
    }

    private static Config extractCredentitalProviderConfig(Config config) {
        Config returnConfig = config;
        if (config.hasPath(CredentialProvider.CEDENTIAL_PROVIDER_CONF)) {
            returnConfig = config.getConfig(CredentialProvider.CEDENTIAL_PROVIDER_CONF);
        }
        return returnConfig;
    }
}
