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

public class BasicCredentialProvider implements CredentialProvider, ProvidesAlias {
    private Config config;
    public static final String BASIC_CREDENTIAL_PROVIDER_TYPE = "basic";
    public static final String BASIC_CEDENTIAL_PROVIDER_PASSWORD = "password";

    @Override
    public void configure(Config config) {
        this.config = config;
        if(!config.hasPath(BASIC_CEDENTIAL_PROVIDER_PASSWORD)) {
            throw new RuntimeException(BASIC_CREDENTIAL_PROVIDER_TYPE
                + " requires '" + BASIC_CEDENTIAL_PROVIDER_PASSWORD + "' property");
        }
    }
    
    @Override
    public String getPassword() throws Exception {
        return config.getString(BASIC_CEDENTIAL_PROVIDER_PASSWORD);
    }
    
    @Override
    public String getAlias() {
        return BASIC_CREDENTIAL_PROVIDER_TYPE;
    }
}
