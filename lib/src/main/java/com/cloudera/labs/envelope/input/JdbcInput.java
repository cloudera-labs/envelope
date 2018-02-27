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
package com.cloudera.labs.envelope.input;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.JdbcOutput;
import  com.cloudera.labs.envelope.security.CredentialProvider;
import com.cloudera.labs.envelope.security.CredentialProviderFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcInput  implements BatchInput, ProvidesAlias {

  public static final String JDBC_CONFIG_URL = "url";
  public static final String JDBC_CONFIG_TABLENAME = "tablename";
  public static final String JDBC_CONFIG_USERNAME = "username";
  public static final String JDBC_CONFIG_PASSWORD = "password";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(JDBC_CONFIG_URL)) {
      throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_URL + "' property");
    }

    if (!config.hasPath(JDBC_CONFIG_TABLENAME)) {
      throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_TABLENAME + "' property");
    }

    if (!config.hasPath(JDBC_CONFIG_USERNAME)) {
      throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_USERNAME + "' property");
    }

    if(!config.hasPath(CredentialProvider.CEDENTIAL_PROVIDER_CONF)) {
      if (!config.hasPath(JDBC_CONFIG_PASSWORD)) {
        throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_PASSWORD + "' property");
      }
    }
  }

  @Override
  public Dataset<Row> read() throws Exception {
    String url = config.getString(JDBC_CONFIG_URL);
    String tablename = config.getString(JDBC_CONFIG_TABLENAME);
    String username = config.getString(JDBC_CONFIG_USERNAME);

    Properties properties = new Properties();
    properties.put("user",username);
    if(config.hasPath(CredentialProvider.CEDENTIAL_PROVIDER_CONF)) {
        properties.put(JDBC_CONFIG_PASSWORD,
            CredentialProviderFactory.create(config).getPassword());
    } else {
      String password = config.getString(JDBC_CONFIG_PASSWORD);
      properties.put(JDBC_CONFIG_PASSWORD,password);
    }

    return Contexts.getSparkSession().read().jdbc(url,tablename,properties);
  }

  @Override
  public String getAlias() {
    return "jdbc";
  }
}
